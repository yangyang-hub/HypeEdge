//! Testnet-only, fill-aware single-venue funding-rate arbitrage runtime, port of
//! `src/hypeedge/strategy/funding_arb/runtime.py`.
//!
//! Owns one durable spot-long/perpetual-short cycle driven through
//! `ENTERING_SPOT → ENTERING_PERP → COMPENSATING_ENTRY → OPEN`, exiting through
//! `EXITING_PERP → EXITING_SPOT → CLOSED`, with `REBALANCING` and `FAULTED`
//! branches. The candidate-selection logic (`_candidate_plan`) is pure and
//! tested; the two-leg execution (`_execute_leg`/`_wait_authoritative_order`)
//! runs behind the [`ExecutionClient`] + [`AccountView`] boundaries.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::{Decimal, Size};
use hypeedge_domain::enums::{
    FundingArbCycleState, MarketMakerLifecycle, OrderStatus, OrderType, Side, TimeInForce,
};
use hypeedge_domain::models::{L2BookSnapshot, Order, OrderIntent, Position};
use hypeedge_domain::traits::ExecutionClient;
use tokio::sync::mpsc;

use super::models::{FundingArbCycle, FundingArbParams};
use super::scanner::{FundingArbMarketScanner, FundingArbMarketSnapshot};
use super::store::FundingArbCycleStore;
use crate::strategy::registry::{
    FaultedRuntimeHandle, StrategyConfigSnapshot, StrategyRuntimeHandle,
};

/// Instrument metadata the runtime needs (a subset of `InstrumentInfo`).
#[derive(Debug, Clone)]
pub struct InstrumentInfo {
    pub symbol: String,
    pub display_name: String,
    pub base_token: String,
    pub quote_token: String,
    pub is_spot: bool,
    pub tick_size: Decimal,
    pub lot_size: Decimal,
    pub min_size: Decimal,
    pub max_leverage: u32,
}

/// The account data the runtime reads (subset of `AccountTracker`).
pub trait FundingArbAccountView: Send + Sync {
    fn get_position(&self, symbol: &str) -> Option<Position>;
    fn get_spot_balance(&self, token: &str) -> Option<SpotBalanceView>;
    fn get_account_available_balance(&self) -> Option<Decimal>;
}

/// A minimal spot-balance view.
#[derive(Debug, Clone)]
pub struct SpotBalanceView {
    pub total: Decimal,
    pub hold: Decimal,
}

impl SpotBalanceView {
    pub fn available(&self) -> Decimal {
        (self.total - self.hold).max(Decimal::ZERO)
    }
}

/// Deployment-wide ceilings (subset of `FundingArbSettings`).
#[derive(Debug, Clone)]
pub struct FundingArbDeployment {
    pub max_notional_usd: Decimal,
    pub poll_interval_seconds: f64,
    pub order_status_poll_interval_seconds: f64,
    pub max_leg_attempts: u32,
    pub market_stale_seconds: f64,
    pub min_spot_24h_volume_usd: Decimal,
    pub min_perp_24h_volume_usd: Decimal,
    pub min_top_book_depth_usd: Decimal,
    pub max_combined_spread_bps: Decimal,
}

/// Instrument metadata lookup the runtime needs for floor/leverage checks.
pub trait FundingArbInstrumentMeta: Send + Sync {
    fn get(&self, symbol: &str) -> Option<InstrumentInfo>;
}

/// Live boundaries captured by the app for an enabled testnet deployment.
pub struct FundingArbRuntimeDependencies {
    pub execution: Arc<dyn ExecutionClient>,
    pub scanner: Arc<dyn FundingArbMarketScanner>,
    pub tracker: Arc<dyn FundingArbAccountView>,
    pub cycles: Arc<dyn FundingArbCycleStore>,
    pub meta: Arc<dyn FundingArbInstrumentMeta>,
    pub trading_ready: Box<dyn Fn() -> BoxFuture<'static, bool> + Send + Sync>,
    pub kill_switch_active: Box<dyn Fn() -> BoxFuture<'static, bool> + Send + Sync>,
    pub account_allows_risk_increase: Box<dyn Fn() -> BoxFuture<'static, bool> + Send + Sync>,
    pub reconcile: Box<dyn Fn() -> BoxFuture<'static, bool> + Send + Sync>,
    pub deployment: FundingArbDeployment,
    pub account_address: String,
}

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// A computed entry plan.
#[derive(Debug, Clone)]
pub struct EntryPlan {
    pub perp_symbol: String,
    pub spot_symbol: String,
    pub spot_display: String,
    pub base_token: String,
    pub quote_token: String,
    pub funding_rate: Decimal,
    pub basis_bps: Decimal,
    pub expected_edge_bps: Decimal,
    pub liquidity_volume_usd: Decimal,
    pub top_book_depth_usd: Decimal,
    pub perp_size: Decimal,
    pub spot_size: Decimal,
    /// Exchange lot size for the perp leg (floor step for post-fill sizing).
    pub perp_lot_size: Decimal,
    /// Exchange lot size for the spot leg.
    pub spot_lot_size: Decimal,
}

/// A leg-execution outcome.
#[derive(Debug, Clone)]
pub struct OrderOutcome {
    pub cloid: String,
    pub filled_size: Decimal,
    pub status: Option<String>,
    pub unknown: bool,
}

/// Parameters for one leg submission (mirrors the Python keyword args).
#[derive(Debug, Clone)]
pub struct LegRequest {
    pub symbol: String,
    pub side: Side,
    pub size: Decimal,
    pub cloid: String,
    pub is_spot: bool,
    pub reduce_only: bool,
    pub risk_reducing: bool,
}

/// The funding-arb runtime handle.
#[allow(dead_code)] // strategy_id/sub_account feed the live leg execution path
pub struct FundingArbRuntimeHandle {
    strategy_id: String,
    params: FundingArbParams,
    config_revision: u64,
    sub_account: String,
    deps: Option<Arc<FundingArbRuntimeDependencies>>,
    cycle: tokio::sync::Mutex<Option<FundingArbCycle>>,
    started: tokio::sync::Mutex<bool>,
    allow_entry: tokio::sync::Mutex<bool>,
    entry_block_reason: tokio::sync::Mutex<Option<String>>,
    entry_diagnostics: tokio::sync::Mutex<serde_json::Value>,
    candidate_count: tokio::sync::Mutex<usize>,
}

impl FundingArbRuntimeHandle {
    pub fn new(
        strategy_id: String,
        params: FundingArbParams,
        config_revision: u64,
        sub_account: String,
        deps: Option<Arc<FundingArbRuntimeDependencies>>,
    ) -> Result<Self, String> {
        params.validate()?;
        if let Some(deps) = &deps {
            if deps.account_address.is_empty() {
                return Err("funding-arb requires a configured testnet account".into());
            }
            if !sub_account
                .to_lowercase()
                .eq_ignore_ascii_case(&deps.account_address.to_lowercase())
            {
                return Err(
                    "funding-arb instance sub_account must match the routed exchange account"
                        .into(),
                );
            }
        }
        Ok(Self {
            strategy_id,
            params,
            config_revision,
            sub_account: sub_account.to_lowercase(),
            deps,
            cycle: tokio::sync::Mutex::new(None),
            started: tokio::sync::Mutex::new(false),
            allow_entry: tokio::sync::Mutex::new(false),
            entry_block_reason: tokio::sync::Mutex::new(Some("not_evaluated".into())),
            entry_diagnostics: tokio::sync::Mutex::new(serde_json::json!({})),
            candidate_count: tokio::sync::Mutex::new(0),
        })
    }

    pub fn live_enabled(&self) -> bool {
        self.deps.is_some()
    }

    pub async fn snapshot(&self) -> serde_json::Value {
        let cycle = self.cycle.lock().await.clone();
        serde_json::json!({
            "live_enabled": self.live_enabled(),
            "allow_entry": *self.allow_entry.lock().await,
            "cycle_id": cycle.as_ref().map(|c| c.cycle_id.to_string()),
            "cycle_state": cycle.as_ref().map(|c| c.state.as_str()),
            "selected_perp": cycle.as_ref().map(|c| c.perp_symbol.clone()),
            "selected_spot": cycle.as_ref().map(|c| c.spot_display.clone()),
            "candidate_count": *self.candidate_count.lock().await,
            "perp_open_size": cycle.as_ref().map(|c| c.perp_open_size.to_string()).unwrap_or("0".into()),
            "spot_open_size": cycle.as_ref().map(|c| c.spot_open_size.to_string()).unwrap_or("0".into()),
            "error_code": cycle.as_ref().and_then(|c| c.error_code.clone()),
            "error_message": cycle.as_ref().and_then(|c| c.error_message.clone()),
            "entry_block_reason": self.entry_block_reason.lock().await.clone(),
            "entry_diagnostics": self.entry_diagnostics.lock().await.clone(),
        })
    }

    // --- Pure candidate-selection logic (high parity value) ---

    /// Validate a book is non-crossed and fresh.
    pub fn valid_book(&self, book: &L2BookSnapshot, now: DateTime<Utc>) -> bool {
        if book.bids.is_empty()
            || book.asks.is_empty()
            || book.bids[0].price.inner() >= book.asks[0].price.inner()
        {
            return false;
        }
        let age = (now - book.local_ts).num_seconds() as f64;
        age <= self
            .deps
            .as_ref()
            .map(|d| d.deployment.market_stale_seconds)
            .unwrap_or(5.0)
    }

    /// Depth in USD within the slippage boundary of the best level.
    pub fn book_depth_usd(&self, book: &L2BookSnapshot, bids: bool) -> Decimal {
        let levels = if bids { &book.bids } else { &book.asks };
        let Some(best) = levels.first() else {
            return Decimal::ZERO;
        };
        let best_price = best.price.inner();
        let slippage = Decimal::from_i128(self.params.max_slippage_bps as i128)
            / Decimal::from_str_lenient("10000").unwrap();
        let boundary = if bids {
            best_price * (Decimal::ONE - slippage)
        } else {
            best_price * (Decimal::ONE + slippage)
        };
        let mut total = Decimal::ZERO;
        for level in levels {
            let price = level.price.inner();
            if (bids && price < boundary) || (!bids && price > boundary) {
                break;
            }
            total += price * level.size.inner();
        }
        total
    }

    /// `floor(value / step) * step` (ROUND_DOWN).
    pub fn floor(value: Decimal, step: Decimal) -> Decimal {
        value.floor_to_step(step)
    }

    /// Evaluate one candidate into an `EntryPlan` (or a rejection reason).
    pub fn candidate_plan(
        &self,
        candidate: &FundingArbMarketSnapshot,
        meta: &InstrumentInfo,
        spot_meta: &InstrumentInfo,
        now: DateTime<Utc>,
    ) -> Result<Option<EntryPlan>, (String, serde_json::Value)> {
        let market = serde_json::json!({
            "perp": candidate.perp_symbol,
            "spot": candidate.spot_display,
        });
        if !spot_meta.is_spot || meta.is_spot {
            return Err(("instrument_metadata_unavailable".into(), market));
        }
        if spot_meta.base_token != meta.symbol || spot_meta.quote_token != "USDC" {
            return Err((
                "instrument_pair_invalid".into(),
                serde_json::json!({ "error": "spot/perp risk units do not match" }),
            ));
        }
        let deps = self
            .deps
            .as_ref()
            .ok_or(("live_dependencies_unavailable".into(), market.clone()))?;
        if candidate.spot_24h_volume_usd < deps.deployment.min_spot_24h_volume_usd {
            return Err((
                "spot_volume_below_minimum".into(),
                serde_json::json!({ "spot_24h_volume_usd": candidate.spot_24h_volume_usd, "minimum": deps.deployment.min_spot_24h_volume_usd }),
            ));
        }
        if candidate.perp_24h_volume_usd < deps.deployment.min_perp_24h_volume_usd {
            return Err((
                "perp_volume_below_minimum".into(),
                serde_json::json!({ "perp_24h_volume_usd": candidate.perp_24h_volume_usd, "minimum": deps.deployment.min_perp_24h_volume_usd }),
            ));
        }
        if !self.valid_book(&candidate.perp_book, now) {
            return Err(("perp_book_invalid_or_stale".into(), market));
        }
        if !self.valid_book(&candidate.spot_book, now) {
            return Err(("spot_book_invalid_or_stale".into(), market));
        }
        if candidate.funding_rate < self.params.entry_funding_rate {
            return Err((
                "funding_below_entry_threshold".into(),
                serde_json::json!({ "funding_rate": candidate.funding_rate, "entry_funding_rate": self.params.entry_funding_rate }),
            ));
        }
        let perp_mid = mid(&candidate.perp_book);
        let spot_mid = mid(&candidate.spot_book);
        let basis_bps =
            (perp_mid - spot_mid).abs() / spot_mid * Decimal::from_str_lenient("10000").unwrap();
        if basis_bps > Decimal::from_i128(self.params.max_basis_bps as i128) {
            return Err((
                "basis_exceeds_limit".into(),
                serde_json::json!({ "basis_bps": basis_bps, "max_basis_bps": self.params.max_basis_bps }),
            ));
        }
        let spread_cost_bps = spread_cost(
            &candidate.perp_book,
            &candidate.spot_book,
            perp_mid,
            spot_mid,
        );
        if spread_cost_bps > deps.deployment.max_combined_spread_bps {
            return Err((
                "combined_spread_exceeds_limit".into(),
                serde_json::json!({ "combined_spread_bps": spread_cost_bps, "maximum": deps.deployment.max_combined_spread_bps }),
            ));
        }
        let top_book_depth = self
            .book_depth_usd(&candidate.perp_book, true)
            .min(self.book_depth_usd(&candidate.perp_book, false))
            .min(self.book_depth_usd(&candidate.spot_book, true))
            .min(self.book_depth_usd(&candidate.spot_book, false));
        let notional = self
            .params
            .max_notional_usd
            .min(deps.deployment.max_notional_usd);
        let required_depth = deps.deployment.min_top_book_depth_usd.max(notional);
        if top_book_depth < required_depth {
            return Err((
                "book_depth_below_minimum".into(),
                serde_json::json!({ "top_book_depth_usd": top_book_depth, "minimum": required_depth }),
            ));
        }
        let expected_edge = candidate.funding_rate
            * Decimal::from_i128(self.params.expected_hold_hours as i128)
            * Decimal::from_str_lenient("10000").unwrap()
            - self.params.round_trip_fee_bps
            - spread_cost_bps;
        if expected_edge < self.params.min_expected_edge_bps {
            return Err((
                "expected_edge_below_minimum".into(),
                serde_json::json!({ "expected_edge_bps": expected_edge, "min_expected_edge_bps": self.params.min_expected_edge_bps, "spread_cost_bps": spread_cost_bps }),
            ));
        }
        let perp_size = Self::floor(notional / perp_mid, meta.lot_size);
        let spot_size = Self::floor(perp_size * self.params.hedge_ratio, spot_meta.lot_size);
        if perp_size < meta.min_size || spot_size < spot_meta.min_size {
            return Err((
                "size_below_exchange_minimum".into(),
                serde_json::json!({ "perp_size": perp_size, "spot_size": spot_size }),
            ));
        }
        Ok(Some(EntryPlan {
            perp_symbol: candidate.perp_symbol.clone(),
            spot_symbol: candidate.spot_symbol.clone(),
            spot_display: candidate.spot_display.clone(),
            base_token: spot_meta.base_token.clone(),
            quote_token: spot_meta.quote_token.clone(),
            funding_rate: candidate.funding_rate,
            basis_bps,
            expected_edge_bps: expected_edge,
            liquidity_volume_usd: candidate
                .perp_24h_volume_usd
                .min(candidate.spot_24h_volume_usd),
            top_book_depth_usd: top_book_depth,
            perp_size,
            spot_size,
            perp_lot_size: meta.lot_size,
            spot_lot_size: spot_meta.lot_size,
        }))
    }

    // --- Lifecycle ---

    /// Bind a recovered cycle's instruments.
    #[allow(dead_code)] // used by the live recover path
    async fn bind_cycle_instruments(
        &self,
        cycle: &FundingArbCycle,
        meta: &InstrumentInfo,
        spot_meta: &InstrumentInfo,
    ) -> Result<(), String> {
        if cycle.perp_symbol != meta.symbol
            || !spot_meta.is_spot
            || cycle.spot_symbol != spot_meta.symbol
        {
            return Err(format!(
                "instrument metadata mismatch for cycle {}",
                cycle.cycle_id
            ));
        }
        Ok(())
    }

    /// Recover a cycle that was open when the process restarted (A19).
    pub async fn recover_active_cycle(&self) -> Result<(), String> {
        let Some(deps) = self.deps.clone() else {
            return Ok(());
        };
        let Some(cycle) = deps.cycles.get_active(&self.strategy_id).await? else {
            return Ok(());
        };
        match cycle.state {
            FundingArbCycleState::Open
            | FundingArbCycleState::Rebalancing
            | FundingArbCycleState::ExitingPerp
            | FundingArbCycleState::ExitingSpot => {
                tracing::info!(
                    strategy_id = %self.strategy_id,
                    cycle_id = %cycle.cycle_id,
                    state = cycle.state.as_str(),
                    "funding_arb_recovered_active_cycle"
                );
                *self.cycle.lock().await = Some(cycle);
            }
            _ => {
                tracing::error!(
                    strategy_id = %self.strategy_id,
                    cycle_state = cycle.state.as_str(),
                    "funding_arb_recovered_unresumable_cycle_faulting"
                );
                *self.cycle.lock().await = Some(cycle.clone());
                self.transition(
                    FundingArbCycleState::Faulted,
                    "cycle_recovery_faulted",
                    Some(serde_json::json!({
                        "error_code": "unresumable_cycle_state",
                        "error_message": format!(
                            "recovered cycle in {} state",
                            cycle.state.as_str()
                        ),
                    })),
                    serde_json::json!({
                        "error_code": "unresumable_cycle_state",
                    }),
                )
                .await?;
            }
        }
        Ok(())
    }

    /// One driver tick: entry scanning when flat; rebalance/exit when open.
    pub async fn tick(&self) -> Result<(), String> {
        let Some(deps) = self.deps.clone() else {
            return Ok(());
        };
        let cycle = self.cycle.lock().await.clone();
        let Some(cycle) = cycle else {
            if !*self.allow_entry.lock().await {
                *self.entry_block_reason.lock().await = Some("entry_disabled_by_lifecycle".into());
                return Ok(());
            }
            if (deps.kill_switch_active)().await
                || !(deps.trading_ready)().await
                || !(deps.account_allows_risk_increase)().await
            {
                *self.entry_block_reason.lock().await = Some("safety_gates_blocked".into());
                return Ok(());
            }
            *self.entry_block_reason.lock().await = None;
            let candidates = deps.scanner.scan().await?;
            for candidate in candidates {
                let Some(perp_meta) = deps.meta.get(&candidate.perp_symbol) else {
                    continue;
                };
                let spot_meta = deps
                    .meta
                    .get(&candidate.spot_symbol)
                    .or_else(|| deps.meta.get(&candidate.spot_display));
                let Some(spot_meta) = spot_meta else {
                    continue;
                };
                if let Ok(Some(plan)) =
                    self.candidate_plan(&candidate, &perp_meta, &spot_meta, Utc::now())
                {
                    self.open_cycle(&plan).await?;
                    break;
                }
            }
            return Ok(());
        };

        if (deps.kill_switch_active)().await {
            return self.close_cycle("kill_switch_active").await;
        }
        if matches!(
            cycle.state,
            FundingArbCycleState::Open | FundingArbCycleState::Rebalancing
        ) {
            if cycle.state == FundingArbCycleState::Open
                && let Ok(Some(snapshot)) = deps
                    .scanner
                    .get_market(&cycle.perp_symbol, &cycle.spot_symbol)
                    .await
                && snapshot.funding_rate <= self.params.exit_funding_rate
            {
                return self.close_cycle("funding_exit_threshold").await;
            }
            return self.rebalance_if_needed().await;
        }
        if matches!(
            cycle.state,
            FundingArbCycleState::ExitingPerp | FundingArbCycleState::ExitingSpot
        ) {
            return self.close_cycle("resume_interrupted_exit").await;
        }
        Ok(())
    }

    /// The hedge-matches check (pure).
    pub fn hedge_matches(&self, spot_size: Decimal, perp_size: Decimal, spot_lot: Decimal) -> bool {
        let expected_spot = Self::floor(perp_size * self.params.hedge_ratio, spot_lot);
        (spot_size - expected_spot).abs() <= spot_lot / Decimal::from_i128(2)
    }

    /// The actual exposure from account state (spot total minus baseline; perp short size).
    pub fn actual_cycle_exposure(
        &self,
        tracker: &dyn FundingArbAccountView,
        cycle: &FundingArbCycle,
    ) -> (Decimal, Decimal) {
        let spot_total = tracker
            .get_spot_balance(&cycle.base_token)
            .map(|b| b.total)
            .unwrap_or(Decimal::ZERO);
        let spot_size = (spot_total - cycle.baseline_spot_size).max(Decimal::ZERO);
        let perp_size = tracker
            .get_position(&cycle.perp_symbol)
            .map(|p| {
                if p.size.inner() < Decimal::ZERO {
                    -p.size.inner()
                } else {
                    Decimal::ZERO
                }
            })
            .unwrap_or(Decimal::ZERO);
        (spot_size, perp_size)
    }

    // --- Two-leg execution (design doc §7.2; mirrors `_execute_leg` … `_close_cycle`) ---

    fn require_deps(&self) -> Result<Arc<FundingArbRuntimeDependencies>, String> {
        self.deps
            .clone()
            .ok_or_else(|| "funding-arb live dependencies are unavailable".into())
    }

    /// Submit one MARKET/IOC leg and wait for its authoritative outcome.
    pub async fn execute_leg(&self, leg: &LegRequest) -> Result<OrderOutcome, String> {
        let deps = self.require_deps()?;
        let intent = OrderIntent {
            symbol: leg.symbol.clone(),
            side: leg.side,
            size: Size::new(leg.size),
            price: None,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::Ioc,
            strategy_id: Some(self.strategy_id.clone()),
            sub_account: Some(self.sub_account.clone()),
            reduce_only: leg.reduce_only,
            cloid: Some(leg.cloid.clone()),
            client_id: None,
            is_spot: leg.is_spot,
            risk_reducing: leg.risk_reducing,
            max_slippage_bps: self.params.max_slippage_bps.min(u32::from(u16::MAX)) as u16,
        };
        let order = deps
            .execution
            .submit_order(intent, None)
            .await
            .map_err(|e| format!("leg submit failed: {e}"))?;
        self.wait_authoritative_order(&order.cloid, self.params.max_unhedged_seconds)
            .await
    }

    /// Poll the durable projection until the outcome is authoritative: full
    /// fill, a settled terminal status, or the timeout → `unknown`.
    pub async fn wait_authoritative_order(
        &self,
        cloid: &str,
        timeout_seconds: u32,
    ) -> Result<OrderOutcome, String> {
        let deps = self.require_deps()?;
        let deadline = Instant::now() + Duration::from_secs(timeout_seconds as u64);
        let poll = deps.deployment.order_status_poll_interval_seconds.max(0.01);
        let settle_seconds = (poll * 2.0)
            .clamp(0.0, 1.0)
            .max((timeout_seconds as f64) / 4.0)
            .min(1.0);
        let mut terminal_since: Option<(Instant, Decimal)> = None;
        let mut last: Option<Order> = None;
        while Instant::now() < deadline {
            last = deps
                .execution
                .refresh_order_from_durable(cloid)
                .await
                .map_err(|e| format!("order status query failed: {e}"))?;
            if let Some(order) = &last {
                let filled = order.filled_size.inner();
                if filled >= order.size.inner() {
                    return Ok(OrderOutcome {
                        cloid: cloid.into(),
                        filled_size: filled,
                        status: Some(order.status.as_str().to_string()),
                        unknown: false,
                    });
                }
                // A18: an IOC order that the engine records as Filled is
                // terminal even on a partial fill — an IOC cannot fill further.
                // Treating a partial IOC fill as `unknown` faulted the cycle and
                // paid the spread twice (or left an unmanaged residual).
                if order.status == OrderStatus::Filled && order.time_in_force == TimeInForce::Ioc {
                    return Ok(OrderOutcome {
                        cloid: cloid.into(),
                        filled_size: filled,
                        status: Some("filled".into()),
                        unknown: false,
                    });
                }
                if matches!(
                    order.status,
                    OrderStatus::Cancelled | OrderStatus::Rejected | OrderStatus::Expired
                ) {
                    match &terminal_since {
                        Some((ts, prev_filled))
                            if *prev_filled == filled
                                && ts.elapsed().as_secs_f64() >= settle_seconds =>
                        {
                            return Ok(OrderOutcome {
                                cloid: cloid.into(),
                                filled_size: filled,
                                status: Some(order.status.as_str().to_string()),
                                unknown: false,
                            });
                        }
                        _ => terminal_since = Some((Instant::now(), filled)),
                    }
                } else {
                    terminal_since = None;
                }
            }
            tokio::time::sleep(Duration::from_secs_f64(poll)).await;
        }
        let filled = last
            .as_ref()
            .map(|o| o.filled_size.inner())
            .unwrap_or(Decimal::ZERO);
        let status = last.as_ref().map(|o| o.status.as_str().to_string());
        Ok(OrderOutcome {
            cloid: cloid.into(),
            filled_size: filled,
            status,
            unknown: true,
        })
    }

    /// Reduce a leg with retries; returns the total authenticated fill.
    pub async fn execute_reducing(
        &self,
        symbol: String,
        side: Side,
        size: Decimal,
        is_spot: bool,
        event_prefix: &str,
        reduce_only: bool,
    ) -> Result<Decimal, String> {
        let deps = self.require_deps()?;
        let mut remaining = size;
        let mut filled_total = Decimal::ZERO;
        for attempt in 1..=deps.deployment.max_leg_attempts {
            if remaining <= Decimal::ZERO {
                break;
            }
            let cloid = self.new_cloid();
            let field = Self::cloid_field(event_prefix);
            let mut updates = serde_json::json!({});
            if let Some(f) = field {
                updates[f] = serde_json::Value::String(cloid.clone());
            }
            let state = self
                .cycle
                .lock()
                .await
                .as_ref()
                .map(|c| c.state)
                .unwrap_or(FundingArbCycleState::Faulted);
            self.transition(
                state,
                &format!("{event_prefix}_attempt"),
                Some(serde_json::json!({
                    "attempt": attempt,
                    "cloid": cloid,
                    "size": remaining.to_string(),
                })),
                updates,
            )
            .await?;
            let outcome = self
                .execute_leg(&LegRequest {
                    symbol: symbol.clone(),
                    side,
                    size: remaining,
                    cloid,
                    is_spot,
                    reduce_only,
                    risk_reducing: true,
                })
                .await?;
            if outcome.unknown {
                self.fault(
                    &format!("{event_prefix}_unknown"),
                    &format!(
                        "risk-reducing order outcome is unresolved: cloid={}",
                        outcome.cloid
                    ),
                )
                .await?;
                return Ok(filled_total);
            }
            filled_total += outcome.filled_size;
            remaining = Self::floor(
                (size - filled_total).max(Decimal::ZERO),
                self.lot_size(is_spot).await,
            );
        }
        Ok(filled_total)
    }

    /// Open a full cycle: create the durable row, buy the spot leg, then sell
    /// the perp leg sized to the spot fill, and align both.
    pub async fn open_cycle(&self, plan: &EntryPlan) -> Result<(), String> {
        let deps = self.require_deps()?;
        if !self.refresh_authoritative_account().await {
            return Ok(()); // caller blocks entry; mirror Python's `_block_entry`
        }
        let spot_cloid = self.new_cloid();
        let baseline = self.spot_total(&plan.base_token);
        let cycle = FundingArbCycle {
            cycle_id: uuid::Uuid::new_v4(),
            strategy_id: self.strategy_id.clone(),
            config_revision: self.config_revision,
            sub_account: self.sub_account.clone(),
            perp_symbol: plan.perp_symbol.clone(),
            spot_symbol: plan.spot_symbol.clone(),
            spot_display: plan.spot_display.clone(),
            base_token: plan.base_token.clone(),
            quote_token: plan.quote_token.clone(),
            state: FundingArbCycleState::EnteringSpot,
            target_perp_size: plan.perp_size,
            target_spot_size: plan.spot_size,
            perp_open_size: Decimal::ZERO,
            spot_open_size: Decimal::ZERO,
            baseline_spot_size: baseline,
            entry_funding_rate: plan.funding_rate,
            entry_basis_bps: plan.basis_bps,
            revision: 0,
            spot_entry_cloid: Some(spot_cloid.clone()),
            perp_entry_cloid: None,
            compensation_cloid: None,
            perp_exit_cloid: None,
            spot_exit_cloid: None,
            error_code: None,
            error_message: None,
            opened_at: None,
            closed_at: None,
            created_at: None,
            updated_at: None,
        };
        let created = deps.cycles.create(&cycle).await?;
        *self.cycle.lock().await = Some(created);

        // A20: set the perp leverage *before* buying the spot leg. If the
        // leverage update failed after the spot buy, the account would hold a
        // naked spot long with a stuck cycle (and a retry would hit the active
        // unique index).
        let leverage_u32 = self.params.leverage.to_string().parse::<u32>().unwrap_or(1);
        deps.execution
            .update_leverage(&plan.perp_symbol, leverage_u32, false)
            .await
            .map_err(|e| format!("leverage update failed: {e}"))?;

        let spot_outcome = self
            .execute_leg(&LegRequest {
                symbol: plan.spot_symbol.clone(),
                side: Side::Buy,
                size: plan.spot_size,
                cloid: spot_cloid,
                is_spot: true,
                reduce_only: false,
                risk_reducing: false,
            })
            .await?;
        if spot_outcome.unknown {
            self.fault_after_unknown("spot_entry_unknown").await?;
            return Ok(());
        }
        if spot_outcome.filled_size <= Decimal::ZERO {
            self.transition(
                FundingArbCycleState::Closed,
                "spot_entry_no_fill",
                Some(serde_json::json!({
                    "error_code": "spot_entry_no_fill",
                    "error_message": "spot entry produced no authenticated fill",
                })),
                serde_json::json!({
                    "error_code": "spot_entry_no_fill",
                    "error_message": "spot entry produced no authenticated fill",
                }),
            )
            .await?;
            self.release_cycle_binding().await;
            return Ok(());
        }

        let perp_target = Self::floor(
            spot_outcome.filled_size / self.params.hedge_ratio,
            plan.perp_lot_size,
        );
        if perp_target <= Decimal::ZERO {
            self.compensate_spot(spot_outcome.filled_size, "spot_fill_below_perp_lot")
                .await?;
            return Ok(());
        }
        let perp_cloid = self.new_cloid();
        self.transition(
            FundingArbCycleState::EnteringPerp,
            "spot_entry_filled",
            Some(serde_json::json!({ "filled_size": spot_outcome.filled_size.to_string() })),
            serde_json::json!({
                "spot_open_size": spot_outcome.filled_size,
                "perp_entry_cloid": perp_cloid,
            }),
        )
        .await?;
        let perp_outcome = self
            .execute_leg(&LegRequest {
                symbol: plan.perp_symbol.clone(),
                side: Side::Sell,
                size: perp_target.min(plan.perp_size),
                cloid: perp_cloid,
                is_spot: false,
                reduce_only: false,
                risk_reducing: false,
            })
            .await?;
        if perp_outcome.unknown {
            self.fault_after_unknown("perp_entry_unknown").await?;
            return Ok(());
        }
        self.align_and_open(spot_outcome.filled_size, perp_outcome.filled_size)
            .await
    }

    /// Align the two legs after entry: refresh authoritative exposure, reduce
    /// the larger leg, and open when the hedge matches.
    pub async fn align_and_open(
        &self,
        spot_size: Decimal,
        perp_size: Decimal,
    ) -> Result<(), String> {
        if self.cycle.lock().await.is_none() {
            return Ok(());
        }
        self.transition(
            FundingArbCycleState::CompensatingEntry,
            "entry_alignment_started",
            None,
            serde_json::json!({ "spot_open_size": spot_size, "perp_open_size": perp_size }),
        )
        .await?;
        if !self.refresh_authoritative_account().await {
            self.fault(
                "entry_reconciliation_failed",
                "authoritative reconciliation failed before alignment",
            )
            .await?;
            return Ok(());
        }
        // Reduce whichever leg is oversized; the authoritative refresh below
        // re-reads the true exposure (the reduced values feed fault diagnostics).
        let (spot, perp) = self.actual_exposure().await;
        self.reduce_larger_leg(spot, perp, "entry").await?;
        if !self.refresh_authoritative_account().await {
            self.fault(
                "entry_reconciliation_failed",
                "authoritative reconciliation failed after alignment",
            )
            .await?;
            return Ok(());
        }
        let (spot, perp) = self.actual_exposure().await;
        let spot_lot = self.lot_size(true).await;
        if spot <= Decimal::ZERO && perp <= Decimal::ZERO {
            self.transition(
                FundingArbCycleState::Closed,
                "entry_compensated_flat",
                None,
                serde_json::json!({ "spot_open_size": Decimal::ZERO, "perp_open_size": Decimal::ZERO }),
            )
            .await?;
            self.release_cycle_binding().await;
            return Ok(());
        }
        if !self.hedge_matches(spot, perp, spot_lot) {
            self.fault(
                "entry_compensation_incomplete",
                "two legs could not be aligned",
            )
            .await?;
            return Ok(());
        }
        self.transition(
            FundingArbCycleState::Open,
            "cycle_opened",
            None,
            serde_json::json!({ "spot_open_size": spot, "perp_open_size": perp }),
        )
        .await
    }

    /// Sell back an oversized spot entry when the perp target is below its lot.
    pub async fn compensate_spot(&self, size: Decimal, reason: &str) -> Result<(), String> {
        self.transition(
            FundingArbCycleState::CompensatingEntry,
            reason,
            None,
            serde_json::json!({ "spot_open_size": size, "perp_open_size": Decimal::ZERO }),
        )
        .await?;
        if !self.refresh_authoritative_account().await {
            self.fault("compensation_reconciliation_failed", reason)
                .await?;
            return Ok(());
        }
        let (actual_spot, actual_perp) = self.actual_exposure().await;
        let perp_lot = self.lot_size(false).await;
        let spot_lot = self.lot_size(true).await;
        if actual_perp > perp_lot / Decimal::from_i128(2) {
            self.fault(
                "unexpected_perp_during_spot_compensation",
                &format!("perp={actual_perp}"),
            )
            .await?;
            return Ok(());
        }
        if actual_spot <= spot_lot / Decimal::from_i128(2) {
            self.transition(
                FundingArbCycleState::Closed,
                "spot_compensation_complete",
                None,
                serde_json::json!({ "spot_open_size": Decimal::ZERO, "perp_open_size": Decimal::ZERO }),
            )
            .await?;
            self.release_cycle_binding().await;
            return Ok(());
        }
        let spot_symbol = self
            .cycle
            .lock()
            .await
            .as_ref()
            .map(|c| c.spot_symbol.clone());
        let Some(spot_symbol) = spot_symbol else {
            return Ok(());
        };
        self.execute_reducing(
            spot_symbol,
            Side::Sell,
            actual_spot,
            true,
            "spot_compensation",
            false,
        )
        .await?;
        if !self.refresh_authoritative_account().await {
            self.fault("compensation_reconciliation_failed", reason)
                .await?;
            return Ok(());
        }
        let (remaining_spot, remaining_perp) = self.actual_exposure().await;
        let spot_lot = self.lot_size(true).await;
        let perp_lot = self.lot_size(false).await;
        if remaining_spot > spot_lot / Decimal::from_i128(2)
            || remaining_perp > perp_lot / Decimal::from_i128(2)
        {
            self.fault(
                "spot_compensation_incomplete",
                &format!("spot={remaining_spot} perp={remaining_perp}"),
            )
            .await?;
            return Ok(());
        }
        self.transition(
            FundingArbCycleState::Closed,
            "spot_compensation_complete",
            None,
            serde_json::json!({ "spot_open_size": Decimal::ZERO, "perp_open_size": Decimal::ZERO }),
        )
        .await?;
        self.release_cycle_binding().await;
        Ok(())
    }

    /// Flatten both legs: exit the perp (buy-to-close) then the spot (sell).
    pub async fn close_cycle(&self, reason: &str) -> Result<(), String> {
        let Some(cycle) = self.cycle.lock().await.clone() else {
            return Ok(());
        };
        if !self.refresh_authoritative_account().await {
            self.fault("exit_reconciliation_failed", reason).await?;
            return Ok(());
        }
        let (spot_size, perp_size) = self.actual_exposure().await;
        // A21: an inverted perp leg must fault before the spot is sold — the
        // pre-fix code read a long perp as zero exposure, sold the spot, and
        // left a naked long behind.
        if self.perp_leg_inverted(&cycle.perp_symbol) {
            return self
                .fault(
                    "inverted_perp_leg",
                    "perp position is long (expected short)",
                )
                .await;
        }
        self.transition(
            FundingArbCycleState::ExitingPerp,
            "exit_started",
            Some(serde_json::json!({ "reason": reason })),
            serde_json::json!({ "spot_open_size": spot_size, "perp_open_size": perp_size }),
        )
        .await?;
        let perp_symbol = cycle.perp_symbol.clone();
        if perp_size > Decimal::ZERO {
            // Perp remaining is whatever the authoritative refresh shows next.
            self.execute_reducing(perp_symbol, Side::Buy, perp_size, false, "perp_exit", true)
                .await?;
        }
        if !self.refresh_authoritative_account().await {
            self.fault("perp_exit_reconciliation_failed", reason)
                .await?;
            return Ok(());
        }
        let (spot_size, perp_size) = self.actual_exposure().await;
        let perp_lot = self.lot_size(false).await;
        if perp_size > perp_lot / Decimal::from_i128(2) {
            self.fault("perp_exit_incomplete", &format!("remaining={perp_size}"))
                .await?;
            return Ok(());
        }
        self.transition(
            FundingArbCycleState::ExitingSpot,
            "perp_exit_complete",
            None,
            serde_json::json!({ "perp_open_size": Decimal::ZERO }),
        )
        .await?;
        let spot_symbol = cycle.spot_symbol.clone();
        if spot_size > Decimal::ZERO {
            self.execute_reducing(spot_symbol, Side::Sell, spot_size, true, "spot_exit", false)
                .await?;
        }
        let spot_lot = self.lot_size(true).await;
        let (final_spot, final_perp) = self.actual_exposure().await;
        if final_spot > spot_lot / Decimal::from_i128(2)
            || final_perp > perp_lot / Decimal::from_i128(2)
        {
            self.fault(
                "final_exposure_not_flat",
                &format!("spot={final_spot} perp={final_perp}"),
            )
            .await?;
            return Ok(());
        }
        self.transition(
            FundingArbCycleState::Closed,
            "cycle_closed",
            None,
            serde_json::json!({ "spot_open_size": Decimal::ZERO, "perp_open_size": Decimal::ZERO }),
        )
        .await?;
        self.release_cycle_binding().await;
        Ok(())
    }

    /// Rebalance when the two legs drift past `rebalance_threshold_bps`.
    pub async fn rebalance_if_needed(&self) -> Result<(), String> {
        let Some(cycle) = self.cycle.lock().await.clone() else {
            return Ok(());
        };
        if !self.refresh_authoritative_account().await {
            return Ok(());
        }
        let (spot_size, perp_size) = self.actual_exposure().await;
        let spot_lot = self.lot_size(true).await;
        let denominator = (perp_size * self.params.hedge_ratio).max(spot_lot);
        if denominator <= Decimal::ZERO {
            return Ok(());
        }
        // A21: an inverted perp leg (long, expected short) is a critical
        // anomaly — fault rather than let rebalancing act on a phantom short.
        if self.perp_leg_inverted(&cycle.perp_symbol) {
            return self
                .fault(
                    "inverted_perp_leg",
                    "perp position is long (expected short)",
                )
                .await;
        }
        let deviation_bps = (spot_size - perp_size * self.params.hedge_ratio).abs() / denominator
            * Decimal::from_str_lenient("10000").unwrap();
        if deviation_bps <= Decimal::from_i128(self.params.rebalance_threshold_bps as i128) {
            return Ok(());
        }
        self.transition(
            FundingArbCycleState::Rebalancing,
            "rebalance_started",
            Some(serde_json::json!({ "deviation_bps": deviation_bps.to_string() })),
            serde_json::json!({ "spot_open_size": spot_size, "perp_open_size": perp_size }),
        )
        .await?;
        self.reduce_larger_leg(spot_size, perp_size, "rebalance")
            .await?;
        if !self.refresh_authoritative_account().await {
            self.fault(
                "rebalance_reconciliation_failed",
                "authoritative reconciliation failed after rebalance",
            )
            .await?;
            return Ok(());
        }
        let (spot_size, perp_size) = self.actual_exposure().await;
        let spot_lot = self.lot_size(true).await;
        if !self.hedge_matches(spot_size, perp_size, spot_lot) {
            self.fault(
                "rebalance_incomplete",
                &format!("spot={spot_size} perp={perp_size}"),
            )
            .await?;
            return Ok(());
        }
        self.transition(
            FundingArbCycleState::Open,
            "rebalance_complete",
            None,
            serde_json::json!({ "spot_open_size": spot_size, "perp_open_size": perp_size }),
        )
        .await
    }

    /// Reduce whichever leg is larger toward the hedge target.
    pub async fn reduce_larger_leg(
        &self,
        spot_size: Decimal,
        perp_size: Decimal,
        event_prefix: &str,
    ) -> Result<(Decimal, Decimal), String> {
        let spot_lot = self.lot_size(true).await;
        let perp_lot = self.lot_size(false).await;
        let target_spot = Self::floor(perp_size * self.params.hedge_ratio, spot_lot);
        let mut spot_size = spot_size;
        let mut perp_size = perp_size;
        if spot_size > target_spot {
            let excess = Self::floor(spot_size - target_spot, spot_lot);
            if excess > Decimal::ZERO {
                let spot_symbol = self
                    .cycle
                    .lock()
                    .await
                    .as_ref()
                    .map(|c| c.spot_symbol.clone());
                if let Some(sym) = spot_symbol {
                    let filled = self
                        .execute_reducing(
                            sym,
                            Side::Sell,
                            excess,
                            true,
                            &format!("{event_prefix}_spot_reduce"),
                            false,
                        )
                        .await?;
                    spot_size = (spot_size - filled).max(Decimal::ZERO);
                }
            }
        } else if spot_size < target_spot && self.params.hedge_ratio > Decimal::ZERO {
            let target_perp = Self::floor(spot_size / self.params.hedge_ratio, perp_lot);
            let excess = Self::floor(perp_size - target_perp, perp_lot);
            if excess > Decimal::ZERO {
                let perp_symbol = self
                    .cycle
                    .lock()
                    .await
                    .as_ref()
                    .map(|c| c.perp_symbol.clone());
                if let Some(sym) = perp_symbol {
                    let filled = self
                        .execute_reducing(
                            sym,
                            Side::Buy,
                            excess,
                            false,
                            &format!("{event_prefix}_perp_reduce"),
                            true,
                        )
                        .await?;
                    perp_size = (perp_size - filled).max(Decimal::ZERO);
                }
            }
        }
        Ok((spot_size, perp_size))
    }

    /// The lot size for a leg, resolved from instrument metadata by symbol.
    async fn lot_size(&self, is_spot: bool) -> Decimal {
        let symbol = {
            let cycle = self.cycle.lock().await.clone();
            match cycle.as_ref() {
                Some(c) if is_spot => c.spot_symbol.clone(),
                Some(c) => c.perp_symbol.clone(),
                None => String::new(),
            }
        };
        if symbol.is_empty() {
            return Decimal::ZERO;
        }
        self.deps
            .as_ref()
            .and_then(|d| d.meta.get(&symbol))
            .map(|i| i.lot_size)
            .unwrap_or(Decimal::ZERO)
    }

    async fn actual_exposure(&self) -> (Decimal, Decimal) {
        let deps = self.require_deps().ok();
        let cycle = self.cycle.lock().await.clone();
        let (Some(deps), Some(cycle)) = (deps, cycle) else {
            return (Decimal::ZERO, Decimal::ZERO);
        };
        self.actual_cycle_exposure(&*deps.tracker, &cycle)
    }

    /// Whether the perp leg is inverted (long when the strategy only ever
    /// shorts) — a critical anomaly that must fault rather than be silently
    /// read as flat exposure (A21).
    fn perp_leg_inverted(&self, symbol: &str) -> bool {
        self.deps
            .as_ref()
            .and_then(|d| d.tracker.get_position(symbol))
            .map(|p| p.size.inner() > Decimal::ZERO)
            .unwrap_or(false)
    }

    /// The current spot balance total for a token.
    fn spot_total(&self, token: &str) -> Decimal {
        self.deps
            .as_ref()
            .and_then(|d| d.tracker.get_spot_balance(token))
            .map(|b| b.total)
            .unwrap_or(Decimal::ZERO)
    }

    async fn refresh_authoritative_account(&self) -> bool {
        let Some(deps) = &self.deps else {
            return false;
        };
        (deps.reconcile)().await
    }

    /// Transition the durable cycle and refresh the in-memory binding.
    async fn transition(
        &self,
        state: FundingArbCycleState,
        event_type: &str,
        payload: Option<serde_json::Value>,
        updates: serde_json::Value,
    ) -> Result<(), String> {
        let cycle = self.cycle.lock().await.clone();
        let Some(cycle) = cycle else {
            return Err("funding-arb cycle is unavailable".into());
        };
        let deps = self.require_deps()?;
        let updated = deps
            .cycles
            .transition(&cycle, state, event_type, payload, updates)
            .await?;
        *self.cycle.lock().await = Some(updated);
        Ok(())
    }

    /// Mark the cycle FAULTED with an error code.
    async fn fault(&self, code: &str, message: &str) -> Result<(), String> {
        let cycle = self.cycle.lock().await.clone();
        let Some(cycle) = cycle else {
            return Ok(());
        };
        if cycle.state == FundingArbCycleState::Closed {
            return Ok(());
        }
        let deps = self.require_deps()?;
        let updated = deps
            .cycles
            .transition(
                &cycle,
                FundingArbCycleState::Faulted,
                "cycle_faulted",
                Some(serde_json::json!({ "error_code": code, "error_message": message })),
                serde_json::json!({ "error_code": code, "error_message": message }),
            )
            .await?;
        *self.cycle.lock().await = Some(updated);
        tracing::error!(
            error_code = code,
            error_message = message,
            "funding_arb_cycle_faulted"
        );
        Ok(())
    }

    /// After an unresolved leg outcome, reduce any open exposure then fault.
    async fn fault_after_unknown(&self, code: &str) -> Result<(), String> {
        self.refresh_authoritative_account().await;
        let (spot_size, perp_size) = self.actual_exposure().await;
        if spot_size > Decimal::ZERO || perp_size > Decimal::ZERO {
            self.reduce_larger_leg(spot_size, perp_size, "unknown_compensation")
                .await?;
        }
        self.fault(
            code,
            &format!("unresolved order outcome; spot={spot_size} perp={perp_size}"),
        )
        .await
    }

    async fn release_cycle_binding(&self) {
        *self.cycle.lock().await = None;
    }

    fn new_cloid(&self) -> String {
        crate::execution::cloid::CloidGenerator::to_hl_cloid(
            &crate::execution::cloid::CloidGenerator::generate(Some(&self.strategy_id)),
        )
    }

    /// The cycle field to track a leg cloid (compensation / perp-exit / spot-exit).
    fn cloid_field(event_prefix: &str) -> Option<&'static str> {
        if event_prefix.contains("compensation") || event_prefix.contains("spot_reduce") {
            return Some("compensation_cloid");
        }
        if event_prefix == "perp_exit" || event_prefix.contains("perp_reduce") {
            return Some("perp_exit_cloid");
        }
        if event_prefix == "spot_exit" {
            return Some("spot_exit_cloid");
        }
        None
    }
}

/// Mid price of a book.
pub fn mid(book: &L2BookSnapshot) -> Decimal {
    (book.bids[0].price.inner() + book.asks[0].price.inner()).div(Decimal::from_i128(2))
}

/// Combined spread cost in bps across both books.
pub fn spread_cost(
    perp_book: &L2BookSnapshot,
    spot_book: &L2BookSnapshot,
    perp_mid: Decimal,
    spot_mid: Decimal,
) -> Decimal {
    let perp_spread =
        (perp_book.asks[0].price.inner() - perp_book.bids[0].price.inner()) / perp_mid;
    let spot_spread =
        (spot_book.asks[0].price.inner() - spot_book.bids[0].price.inner()) / spot_mid;
    (perp_spread + spot_spread) * Decimal::from_str_lenient("10000").unwrap()
}

/// Decode a config snapshot into `FundingArbParams`.
pub fn decode_funding_arb_config(
    config: &StrategyConfigSnapshot,
) -> Result<FundingArbParams, String> {
    let v = &config.values;
    let get_d = |k: &str| -> Result<Decimal, String> {
        match v.get(k) {
            Some(serde_json::Value::String(s)) => {
                Decimal::from_str_lenient(s).map_err(|_| format!("invalid decimal for {k}"))
            }
            Some(serde_json::Value::Number(n)) => n
                .as_f64()
                .ok_or_else(|| format!("invalid number for {k}"))
                .and_then(|f| Decimal::from_f64(f).map_err(|_| format!("invalid decimal for {k}"))),
            Some(serde_json::Value::Null) | None => Err(format!("missing field {k}")),
            Some(_) => Err(format!("unexpected type for {k}")),
        }
    };
    let get_u = |k: &str| -> Result<u32, String> {
        v.get(k)
            .and_then(|x| x.as_u64())
            .map(|x| x as u32)
            .ok_or_else(|| format!("missing integer field {k}"))
    };
    let params = FundingArbParams {
        entry_funding_rate: get_d("entry_funding_rate")?,
        exit_funding_rate: get_d("exit_funding_rate")?,
        max_notional_usd: get_d("max_notional_usd")?,
        hedge_ratio: get_d("hedge_ratio")?,
        rebalance_threshold_bps: get_u("rebalance_threshold_bps")?,
        leverage: get_d("leverage")?,
        max_slippage_bps: get_u("max_slippage_bps")?,
        max_basis_bps: get_u("max_basis_bps")?,
        min_expected_edge_bps: get_d("min_expected_edge_bps")?,
        expected_hold_hours: get_u("expected_hold_hours")?,
        round_trip_fee_bps: get_d("round_trip_fee_bps")?,
        max_unhedged_seconds: get_u("max_unhedged_seconds")?,
    };
    params.validate()?;
    Ok(params)
}

/// Default funding-arb config.
pub fn default_funding_arb_config() -> serde_json::Value {
    let p = FundingArbParams::default();
    serde_json::json!({
        "entry_funding_rate": p.entry_funding_rate.to_string(),
        "exit_funding_rate": p.exit_funding_rate.to_string(),
        "max_notional_usd": p.max_notional_usd.to_string(),
        "hedge_ratio": p.hedge_ratio.to_string(),
        "rebalance_threshold_bps": p.rebalance_threshold_bps,
        "leverage": p.leverage.to_string(),
        "max_slippage_bps": p.max_slippage_bps,
        "max_basis_bps": p.max_basis_bps,
        "min_expected_edge_bps": p.min_expected_edge_bps.to_string(),
        "expected_hold_hours": p.expected_hold_hours,
        "round_trip_fee_bps": p.round_trip_fee_bps.to_string(),
        "max_unhedged_seconds": p.max_unhedged_seconds,
    })
}

/// Build the funding-arb plugin factory (the `StrategyTypePlugin` analog).
pub fn build_funding_arb_plugin(
    deps: Option<Arc<FundingArbRuntimeDependencies>>,
) -> crate::strategy::registry::StrategyTypePlugin {
    crate::strategy::registry::StrategyTypePlugin {
        strategy_type: "funding_arb".to_string(),
        capabilities: crate::strategy::registry::funding_arb_capabilities(),
        factory: Arc::new(
            move |ctx: &crate::strategy::registry::StrategyBuildContext| {
                let params = decode_funding_arb_config(&ctx.config).unwrap_or_default();
                let sub_account = ctx.instance.sub_account.clone();
                let strategy_id = ctx.instance.strategy_id.clone();
                let config_revision = ctx.config.revision;
                let deps = deps.clone();
                match FundingArbRuntimeHandle::new(
                    strategy_id,
                    params,
                    config_revision,
                    sub_account,
                    deps,
                ) {
                    Ok(handle) => Arc::new(FundingArbRuntimeAdapter {
                        inner: Arc::new(tokio::sync::Mutex::new(handle)),
                        stop_tx: tokio::sync::Mutex::new(None),
                        task: tokio::sync::Mutex::new(None),
                    }),
                    Err(e) => Arc::new(FaultedRuntimeHandle {
                        message: format!("funding-arb runtime construction failed: {e}"),
                    }),
                }
            },
        ),
    }
}

/// Adapter that maps `FundingArbRuntimeHandle` to `StrategyRuntimeHandle` and
/// drives the live scan/open/rebalance/close loop while the strategy runs.
pub struct FundingArbRuntimeAdapter {
    inner: Arc<tokio::sync::Mutex<FundingArbRuntimeHandle>>,
    stop_tx: tokio::sync::Mutex<Option<mpsc::Sender<()>>>,
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<(), String>>>>,
}

#[async_trait]
impl StrategyRuntimeHandle for FundingArbRuntimeAdapter {
    async fn start(&self) -> Result<(), String> {
        let handle = self.inner.clone();
        {
            let guard = self.inner.lock().await;
            if *guard.started.lock().await {
                return Ok(()); // idempotent
            }
            guard.recover_active_cycle().await?;
            *guard.started.lock().await = true;
        }
        let (stop_tx, stop_rx) = mpsc::channel(1);
        *self.stop_tx.lock().await = Some(stop_tx);
        let task = tokio::spawn(async move { Self::run_driver(handle, stop_rx).await });
        *self.task.lock().await = Some(task);
        Ok(())
    }
    async fn set_mode(&self, mode: MarketMakerLifecycle) -> Result<(), String> {
        let handle = self.inner.lock().await;
        match mode {
            MarketMakerLifecycle::Warming | MarketMakerLifecycle::Shadow => Ok(()),
            MarketMakerLifecycle::Running => {
                *handle.allow_entry.lock().await = true;
                *handle.entry_block_reason.lock().await = None;
                Ok(())
            }
            MarketMakerLifecycle::Paused | MarketMakerLifecycle::Faulted => {
                *handle.allow_entry.lock().await = false;
                Ok(())
            }
            MarketMakerLifecycle::Stopped | MarketMakerLifecycle::Draining => {
                *handle.allow_entry.lock().await = false;
                *handle.started.lock().await = false;
                Ok(())
            }
        }
    }
    async fn apply_config(&self, config: &StrategyConfigSnapshot) -> Result<(), String> {
        let mut handle = self.inner.lock().await;
        let cycle = handle.cycle.lock().await;
        if cycle.is_some() && config.revision != handle.config_revision {
            return Err("funding-arb config cannot change while a cycle is active".into());
        }
        drop(cycle);
        let params = decode_funding_arb_config(config)?;
        handle.params = params;
        handle.config_revision = config.revision;
        Ok(())
    }
    async fn stop(&self) -> Result<(), String> {
        {
            let handle = self.inner.lock().await;
            *handle.allow_entry.lock().await = false;
            *handle.started.lock().await = false;
        }
        if let Some(tx) = self.stop_tx.lock().await.take() {
            let _ = tx.send(()).await;
        }
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
        Ok(())
    }
}

impl FundingArbRuntimeAdapter {
    /// The live driver: scan candidates while flat, and rebalance/close while
    /// a cycle is open. Rebalancing and closing are never gated by `allow_entry`
    /// — safety exits must always work.
    async fn run_driver(
        handle: Arc<tokio::sync::Mutex<FundingArbRuntimeHandle>>,
        mut stop_rx: mpsc::Receiver<()>,
    ) -> Result<(), String> {
        let interval = handle
            .lock()
            .await
            .deps
            .as_ref()
            .map(|d| d.deployment.poll_interval_seconds)
            .unwrap_or(5.0)
            .max(0.5);
        loop {
            tokio::select! {
                _ = stop_rx.recv() => break,
                _ = tokio::time::sleep(Duration::from_secs_f64(interval)) => {}
            }
            let guard = handle.lock().await;
            if !*guard.started.lock().await {
                break;
            }
            if let Err(e) = guard.tick().await {
                tracing::error!(
                    strategy_id = %guard.strategy_id,
                    error = %e,
                    "funding_arb_driver_tick_error"
                );
            }
            drop(guard);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::decimal::{Price, Size};
    use hypeedge_domain::enums::FundingArbCycleState;
    use hypeedge_domain::models::L2Level;
    use hypeedge_domain::models::{Order, OrderIntent};

    fn book(
        bid: &str,
        bid_sz: &str,
        ask: &str,
        ask_sz: &str,
        now: DateTime<Utc>,
    ) -> L2BookSnapshot {
        L2BookSnapshot {
            symbol: "BTC".into(),
            bids: vec![L2Level {
                price: Price::new(Decimal::from_str_lenient(bid).unwrap()),
                size: Size::new(Decimal::from_str_lenient(bid_sz).unwrap()),
            }],
            asks: vec![L2Level {
                price: Price::new(Decimal::from_str_lenient(ask).unwrap()),
                size: Size::new(Decimal::from_str_lenient(ask_sz).unwrap()),
            }],
            timestamp: 0,
            local_ts: now,
            version: 1,
            connection_generation: 0,
        }
    }

    fn meta(symbol: &str, is_spot: bool) -> InstrumentInfo {
        InstrumentInfo {
            symbol: symbol.into(),
            display_name: symbol.into(),
            base_token: symbol.into(),
            quote_token: "USDC".into(),
            is_spot,
            tick_size: Decimal::from_str_lenient("0.1").unwrap(),
            lot_size: Decimal::from_str_lenient("0.001").unwrap(),
            min_size: Decimal::from_str_lenient("0.001").unwrap(),
            max_leverage: 5,
        }
    }

    fn deps() -> Arc<FundingArbRuntimeDependencies> {
        Arc::new(FundingArbRuntimeDependencies {
            execution: Arc::new(NoopExecution),
            scanner: Arc::new(NoopScanner),
            tracker: Arc::new(NoopTracker),
            cycles: Arc::new(NoopStore),
            meta: Arc::new(FakeMeta),
            trading_ready: Box::new(|| Box::pin(async { true })),
            kill_switch_active: Box::new(|| Box::pin(async { false })),
            account_allows_risk_increase: Box::new(|| Box::pin(async { true })),
            reconcile: Box::new(|| Box::pin(async { true })),
            deployment: FundingArbDeployment {
                max_notional_usd: Decimal::from_str_lenient("500").unwrap(),
                poll_interval_seconds: 5.0,
                order_status_poll_interval_seconds: 0.25,
                max_leg_attempts: 3,
                market_stale_seconds: 5.0,
                min_spot_24h_volume_usd: Decimal::from_str_lenient("1000").unwrap(),
                min_perp_24h_volume_usd: Decimal::from_str_lenient("10000").unwrap(),
                min_top_book_depth_usd: Decimal::from_str_lenient("100").unwrap(),
                max_combined_spread_bps: Decimal::from_str_lenient("100").unwrap(),
            },
            account_address: "0xabc".into(),
        })
    }

    struct FakeMeta;
    impl FundingArbInstrumentMeta for FakeMeta {
        fn get(&self, symbol: &str) -> Option<InstrumentInfo> {
            Some(meta(symbol, symbol == "SPOT-BTC"))
        }
    }

    struct NoopExecution;
    #[async_trait]
    impl ExecutionClient for NoopExecution {
        async fn submit_order(
            &self,
            _: OrderIntent,
            _: Option<bool>,
        ) -> Result<Order, hypeedge_domain::error::HypeEdgeError> {
            unimplemented!()
        }
        async fn cancel_order(
            &self,
            _: &str,
        ) -> Result<bool, hypeedge_domain::error::HypeEdgeError> {
            Ok(true)
        }
        async fn cancel_all_orders(
            &self,
            _: Option<&str>,
        ) -> Result<u64, hypeedge_domain::error::HypeEdgeError> {
            Ok(0)
        }
        async fn get_order(
            &self,
            _: &str,
        ) -> Result<Option<Order>, hypeedge_domain::error::HypeEdgeError> {
            Ok(None)
        }
        async fn get_open_orders(
            &self,
            _: Option<&str>,
        ) -> Result<Vec<Order>, hypeedge_domain::error::HypeEdgeError> {
            Ok(vec![])
        }
    }
    struct NoopScanner;
    #[async_trait]
    impl FundingArbMarketScanner for NoopScanner {
        async fn scan(&self) -> Result<Vec<FundingArbMarketSnapshot>, String> {
            Ok(vec![])
        }
        async fn get_market(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Option<FundingArbMarketSnapshot>, String> {
            Ok(None)
        }
    }
    struct NoopTracker;
    impl FundingArbAccountView for NoopTracker {
        fn get_position(&self, _: &str) -> Option<Position> {
            None
        }
        fn get_spot_balance(&self, _: &str) -> Option<SpotBalanceView> {
            Some(SpotBalanceView {
                total: Decimal::from_str_lenient("10000").unwrap(),
                hold: Decimal::ZERO,
            })
        }
        fn get_account_available_balance(&self) -> Option<Decimal> {
            Some(Decimal::from_str_lenient("10000").unwrap())
        }
    }
    struct NoopStore;
    #[async_trait]
    impl FundingArbCycleStore for NoopStore {
        async fn create(&self, _: &FundingArbCycle) -> Result<FundingArbCycle, String> {
            unimplemented!()
        }
        async fn get_active(&self, _: &str) -> Result<Option<FundingArbCycle>, String> {
            Ok(None)
        }
        async fn transition(
            &self,
            _: &FundingArbCycle,
            _: FundingArbCycleState,
            _: &str,
            _: Option<serde_json::Value>,
            _: serde_json::Value,
        ) -> Result<FundingArbCycle, String> {
            unimplemented!()
        }
    }

    fn runtime() -> FundingArbRuntimeHandle {
        FundingArbRuntimeHandle::new(
            "fa_1".into(),
            FundingArbParams::default(),
            1,
            "0xabc".into(),
            Some(deps()),
        )
        .unwrap()
    }

    #[test]
    fn candidate_plan_accepts_eligible_market() {
        let now = Utc::now();
        let handle = runtime();
        let candidate = FundingArbMarketSnapshot {
            perp_symbol: "BTC".into(),
            spot_symbol: "@1".into(),
            spot_display: "BTC/USDC".into(),
            funding_rate: Decimal::from_str_lenient("0.001").unwrap(), // 80 bps over 8h hold
            perp_24h_volume_usd: Decimal::from_str_lenient("100000").unwrap(),
            spot_24h_volume_usd: Decimal::from_str_lenient("50000").unwrap(),
            perp_book: book("99.9", "10", "100.1", "10", now),
            spot_book: book("99.9", "20", "100.1", "20", now),
        };
        let perp_meta = meta("BTC", false);
        let spot_meta = meta("BTC", true);
        let plan = handle
            .candidate_plan(&candidate, &perp_meta, &spot_meta, now)
            .unwrap();
        assert!(plan.is_some(), "eligible market should produce a plan");
        let plan = plan.unwrap();
        assert!(plan.expected_edge_bps > Decimal::ZERO);
        assert!(plan.perp_size > Decimal::ZERO);
    }

    #[test]
    fn candidate_plan_rejects_low_funding() {
        let now = Utc::now();
        let handle = runtime();
        let candidate = FundingArbMarketSnapshot {
            perp_symbol: "BTC".into(),
            spot_symbol: "@1".into(),
            spot_display: "BTC/USDC".into(),
            funding_rate: Decimal::from_str_lenient("0.00005").unwrap(), // < entry
            perp_24h_volume_usd: Decimal::from_str_lenient("100000").unwrap(),
            spot_24h_volume_usd: Decimal::from_str_lenient("50000").unwrap(),
            perp_book: book("99.9", "10", "100.1", "10", now),
            spot_book: book("99.9", "20", "100.1", "20", now),
        };
        let err = handle
            .candidate_plan(&candidate, &meta("BTC", false), &meta("BTC", true), now)
            .unwrap_err();
        assert_eq!(err.0, "funding_below_entry_threshold");
    }

    #[test]
    fn candidate_plan_rejects_crossed_book() {
        let now = Utc::now();
        let handle = runtime();
        let candidate = FundingArbMarketSnapshot {
            perp_symbol: "BTC".into(),
            spot_symbol: "@1".into(),
            spot_display: "BTC/USDC".into(),
            funding_rate: Decimal::from_str_lenient("0.0002").unwrap(),
            perp_24h_volume_usd: Decimal::from_str_lenient("100000").unwrap(),
            spot_24h_volume_usd: Decimal::from_str_lenient("50000").unwrap(),
            perp_book: book("100", "10", "99", "10", now), // crossed
            spot_book: book("99.4", "20", "100.4", "20", now),
        };
        let err = handle
            .candidate_plan(&candidate, &meta("BTC", false), &meta("BTC", true), now)
            .unwrap_err();
        assert_eq!(err.0, "perp_book_invalid_or_stale");
    }

    #[test]
    fn hedge_matches_uses_floor() {
        let handle = runtime();
        assert!(handle.hedge_matches(
            Decimal::from_str_lenient("1").unwrap(),
            Decimal::from_str_lenient("1").unwrap(),
            Decimal::from_str_lenient("0.001").unwrap()
        ));
        // hedge_ratio 1, perp 1 → expected spot 1, within lot/2.
        assert!(!handle.hedge_matches(
            Decimal::from_str_lenient("0.5").unwrap(),
            Decimal::from_str_lenient("1").unwrap(),
            Decimal::from_str_lenient("0.001").unwrap()
        ));
    }

    #[test]
    fn decode_config_roundtrip() {
        let config = StrategyConfigSnapshot {
            strategy_id: "fa_1".into(),
            revision: 1,
            values: default_funding_arb_config(),
        };
        let params = decode_funding_arb_config(&config).unwrap();
        assert!(params.validate().is_ok());
        assert_eq!(params.entry_funding_rate.to_string(), "0.0001");
    }

    // --- Two-leg execution ---

    /// A scripted environment: submits fill immediately, projects the tracker
    /// position/balance, and records cycle transitions.
    #[derive(Clone)]
    struct ScriptedEnv {
        state: Arc<std::sync::Mutex<ScriptedState>>,
    }

    struct ScriptedState {
        spot_total: Decimal,
        perp_position: Decimal, // negative = short
        orders: Vec<Order>,
        cycle_states: Vec<(FundingArbCycleState, String)>,
        current_cycle: Option<FundingArbCycle>,
        leverage_updated: bool,
        /// Operation sequence for ordering assertions (A20).
        ops: Vec<String>,
    }

    impl ScriptedEnv {
        fn new() -> Self {
            Self {
                state: Arc::new(std::sync::Mutex::new(ScriptedState {
                    spot_total: Decimal::ZERO,
                    perp_position: Decimal::ZERO,
                    orders: Vec::new(),
                    cycle_states: Vec::new(),
                    current_cycle: None,
                    leverage_updated: false,
                    ops: Vec::new(),
                })),
            }
        }

        fn plan() -> EntryPlan {
            EntryPlan {
                perp_symbol: "BTC".into(),
                spot_symbol: "@1".into(),
                spot_display: "BTC/USDC".into(),
                base_token: "BTC".into(),
                quote_token: "USDC".into(),
                funding_rate: Decimal::from_str_lenient("0.001").unwrap(),
                basis_bps: Decimal::from_str_lenient("10").unwrap(),
                expected_edge_bps: Decimal::from_str_lenient("50").unwrap(),
                liquidity_volume_usd: Decimal::from_str_lenient("100000").unwrap(),
                top_book_depth_usd: Decimal::from_str_lenient("500").unwrap(),
                perp_size: Decimal::from_str_lenient("1.0").unwrap(),
                spot_size: Decimal::from_str_lenient("1.0").unwrap(),
                perp_lot_size: Decimal::from_str_lenient("0.001").unwrap(),
                spot_lot_size: Decimal::from_str_lenient("0.001").unwrap(),
            }
        }
    }

    #[async_trait]
    impl ExecutionClient for ScriptedEnv {
        async fn submit_order(
            &self,
            intent: OrderIntent,
            _: Option<bool>,
        ) -> Result<Order, hypeedge_domain::error::HypeEdgeError> {
            let mut st = self.state.lock().unwrap();
            let filled = intent.size.inner();
            if intent.is_spot {
                st.ops.push("spot_leg".into());
                if intent.side == Side::Buy {
                    st.spot_total += filled;
                } else {
                    st.spot_total = st.spot_total - filled;
                }
            } else if intent.side == Side::Buy {
                st.ops.push("perp_buy".into());
                st.perp_position += filled;
            } else {
                st.ops.push("perp_sell".into());
                st.perp_position = st.perp_position - filled;
            }
            let cloid = intent.cloid.clone().unwrap_or_default();
            let mut order = Order::new(
                cloid.clone(),
                intent.symbol.clone(),
                intent.side,
                intent.size,
                intent.price,
                intent.order_type,
                intent.time_in_force,
            );
            order.status = OrderStatus::Filled;
            order.filled_size = intent.size;
            order.cloid = cloid;
            st.orders.push(order.clone());
            Ok(order)
        }
        async fn cancel_order(
            &self,
            _: &str,
        ) -> Result<bool, hypeedge_domain::error::HypeEdgeError> {
            Ok(true)
        }
        async fn cancel_all_orders(
            &self,
            _: Option<&str>,
        ) -> Result<u64, hypeedge_domain::error::HypeEdgeError> {
            Ok(0)
        }
        async fn get_order(
            &self,
            cloid: &str,
        ) -> Result<Option<Order>, hypeedge_domain::error::HypeEdgeError> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .orders
                .iter()
                .rev()
                .find(|o| o.cloid == cloid)
                .cloned())
        }
        async fn get_open_orders(
            &self,
            _: Option<&str>,
        ) -> Result<Vec<Order>, hypeedge_domain::error::HypeEdgeError> {
            Ok(vec![])
        }
        async fn refresh_order_from_durable(
            &self,
            cloid: &str,
        ) -> Result<Option<Order>, hypeedge_domain::error::HypeEdgeError> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .orders
                .iter()
                .rev()
                .find(|o| o.cloid == cloid)
                .cloned())
        }
        async fn update_leverage(
            &self,
            _: &str,
            _: u32,
            _: bool,
        ) -> Result<serde_json::Value, hypeedge_domain::error::HypeEdgeError> {
            let mut st = self.state.lock().unwrap();
            st.leverage_updated = true;
            st.ops.push("leverage".into());
            Ok(serde_json::json!({ "ok": true }))
        }
    }

    impl FundingArbAccountView for ScriptedEnv {
        fn get_position(&self, _symbol: &str) -> Option<Position> {
            let st = self.state.lock().unwrap();
            let size = st.perp_position;
            Some(Position {
                symbol: "BTC".into(),
                size: Size::new(size),
                entry_price: None,
                mark_price: None,
                unrealized_pnl: None,
                leverage: 0,
                liquidation_price: None,
                sub_account: None,
                strategy_id: None,
            })
        }
        fn get_spot_balance(&self, _token: &str) -> Option<SpotBalanceView> {
            let st = self.state.lock().unwrap();
            Some(SpotBalanceView {
                total: st.spot_total,
                hold: Decimal::ZERO,
            })
        }
        fn get_account_available_balance(&self) -> Option<Decimal> {
            Some(Decimal::from_str_lenient("100000").unwrap())
        }
    }

    #[async_trait]
    impl FundingArbCycleStore for ScriptedEnv {
        async fn create(&self, cycle: &FundingArbCycle) -> Result<FundingArbCycle, String> {
            let mut st = self.state.lock().unwrap();
            st.current_cycle = Some(cycle.clone());
            Ok(cycle.clone())
        }
        async fn get_active(&self, _: &str) -> Result<Option<FundingArbCycle>, String> {
            Ok(self.state.lock().unwrap().current_cycle.clone())
        }
        async fn transition(
            &self,
            cycle: &FundingArbCycle,
            state: FundingArbCycleState,
            event_type: &str,
            _payload: Option<serde_json::Value>,
            updates: serde_json::Value,
        ) -> Result<FundingArbCycle, String> {
            let mut st = self.state.lock().unwrap();
            st.cycle_states.push((state, event_type.to_string()));
            let mut updated = cycle.clone();
            updated.state = state;
            updated.revision += 1;
            if let Some(v) = updates.get("spot_open_size").and_then(|v| v.as_str()) {
                updated.spot_open_size = Decimal::from_str_lenient(v).unwrap_or(Decimal::ZERO);
            }
            if let Some(v) = updates.get("perp_open_size").and_then(|v| v.as_str()) {
                updated.perp_open_size = Decimal::from_str_lenient(v).unwrap_or(Decimal::ZERO);
            }
            if let Some(v) = updates.get("perp_entry_cloid").and_then(|v| v.as_str()) {
                updated.perp_entry_cloid = Some(v.to_string());
            }
            if let Some(v) = updates.get("error_code").and_then(|v| v.as_str()) {
                updated.error_code = Some(v.to_string());
            }
            if let Some(v) = updates.get("error_message").and_then(|v| v.as_str()) {
                updated.error_message = Some(v.to_string());
            }
            if updated.state == FundingArbCycleState::Open {
                updated.opened_at = Some(Utc::now());
            }
            if updated.state == FundingArbCycleState::Closed {
                updated.closed_at = Some(Utc::now());
            }
            st.current_cycle = Some(updated.clone());
            Ok(updated)
        }
    }

    impl FundingArbInstrumentMeta for ScriptedEnv {
        fn get(&self, symbol: &str) -> Option<InstrumentInfo> {
            Some(meta(symbol, symbol == "@1"))
        }
    }

    #[async_trait]
    impl FundingArbMarketScanner for ScriptedEnv {
        async fn scan(&self) -> Result<Vec<FundingArbMarketSnapshot>, String> {
            Ok(vec![])
        }
        async fn get_market(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Option<FundingArbMarketSnapshot>, String> {
            Ok(None)
        }
    }

    fn scripted_runtime(env: &ScriptedEnv) -> FundingArbRuntimeHandle {
        let deps = Arc::new(FundingArbRuntimeDependencies {
            execution: Arc::new(env.clone()),
            scanner: Arc::new(env.clone()),
            tracker: Arc::new(env.clone()),
            cycles: Arc::new(env.clone()),
            meta: Arc::new(env.clone()),
            trading_ready: Box::new(|| Box::pin(async { true })),
            kill_switch_active: Box::new(|| Box::pin(async { false })),
            account_allows_risk_increase: Box::new(|| Box::pin(async { true })),
            reconcile: Box::new(|| Box::pin(async { true })),
            deployment: FundingArbDeployment {
                max_notional_usd: Decimal::from_str_lenient("500").unwrap(),
                poll_interval_seconds: 5.0,
                order_status_poll_interval_seconds: 0.01,
                max_leg_attempts: 3,
                market_stale_seconds: 5.0,
                min_spot_24h_volume_usd: Decimal::from_str_lenient("1000").unwrap(),
                min_perp_24h_volume_usd: Decimal::from_str_lenient("10000").unwrap(),
                min_top_book_depth_usd: Decimal::from_str_lenient("100").unwrap(),
                max_combined_spread_bps: Decimal::from_str_lenient("100").unwrap(),
            },
            account_address: "0xabc".into(),
        });
        FundingArbRuntimeHandle::new(
            "fa_1".into(),
            FundingArbParams::default(),
            1,
            "0xabc".into(),
            Some(deps),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn open_cycle_buys_spot_sells_perp_and_opens() {
        let env = ScriptedEnv::new();
        let handle = scripted_runtime(&env);
        let plan = ScriptedEnv::plan();
        handle.open_cycle(&plan).await.unwrap();

        let st = env.state.lock().unwrap();
        // SPOT leg buy → spot total 1.0; PERP leg sell → short 1.0.
        assert_eq!(st.spot_total.to_string(), "1");
        assert_eq!(st.perp_position.to_string(), "-1");
        assert!(
            st.leverage_updated,
            "leverage must be set before the perp leg"
        );
        // Cycle ends OPEN.
        let last = st.cycle_states.last().cloned().unwrap();
        assert_eq!(
            last.0,
            FundingArbCycleState::Open,
            "states: {:?}",
            st.cycle_states
        );
        let cycle = st.current_cycle.as_ref().unwrap();
        assert_eq!(cycle.spot_open_size.to_string(), "1");
        assert_eq!(cycle.perp_open_size.to_string(), "1");
    }

    #[tokio::test]
    async fn close_cycle_flattens_both_legs() {
        let env = ScriptedEnv::new();
        let handle = scripted_runtime(&env);
        handle.open_cycle(&ScriptedEnv::plan()).await.unwrap();
        handle.close_cycle("funding_exit").await.unwrap();

        let st = env.state.lock().unwrap();
        let last = st.cycle_states.last().cloned().unwrap();
        assert_eq!(
            last.0,
            FundingArbCycleState::Closed,
            "states: {:?}",
            st.cycle_states
        );
        // Both legs flattened: spot back to baseline 0, perp position 0.
        assert_eq!(st.spot_total.to_string(), "0");
        assert_eq!(st.perp_position.to_string(), "0");
        // The durable cycle is retained in the store as CLOSED (the handle
        // binding is what gets released).
        let cycle = st.current_cycle.as_ref().unwrap();
        assert_eq!(cycle.state, FundingArbCycleState::Closed);
        assert!(cycle.closed_at.is_some());
    }

    #[tokio::test]
    async fn wait_authoritative_order_resolves_filled_and_timeout() {
        let env = ScriptedEnv::new();
        let handle = scripted_runtime(&env);
        // Immediate fill: the scripted env fills on submit.
        let outcome = handle
            .execute_leg(&LegRequest {
                symbol: "BTC".into(),
                side: Side::Buy,
                size: Decimal::ONE,
                cloid: "c_fill".into(),
                is_spot: false,
                reduce_only: false,
                risk_reducing: false,
            })
            .await
            .unwrap();
        assert!(!outcome.unknown);
        assert_eq!(outcome.filled_size.to_string(), "1");
        assert_eq!(outcome.status.as_deref(), Some("filled"));

        // Unknown order: no durable record → times out as unknown.
        let outcome = handle.wait_authoritative_order("c_ghost", 1).await.unwrap();
        assert!(outcome.unknown);
        assert_eq!(outcome.filled_size.to_string(), "0");
    }

    #[tokio::test]
    async fn ioc_partial_fill_is_settled_not_unknown() {
        // A18 regression: an IOC order the engine records as Filled is terminal
        // even on a partial fill — it must settle, not be treated as unknown
        // (which used to fault the cycle and pay the spread twice).
        let env = ScriptedEnv::new();
        let handle = scripted_runtime(&env);
        let mut order = Order::new(
            "c_partial".into(),
            "BTC".into(),
            Side::Buy,
            Size::new(Decimal::from_str_strict("2").unwrap()),
            None,
            OrderType::Market,
            TimeInForce::Ioc,
        );
        order.status = OrderStatus::Filled;
        order.filled_size = Size::new(Decimal::from_str_strict("1").unwrap());
        order.cloid = "c_partial".into();
        env.state.lock().unwrap().orders.push(order);

        let outcome = handle
            .wait_authoritative_order("c_partial", 2)
            .await
            .unwrap();
        assert!(!outcome.unknown, "IOC partial fill must be settled (A18)");
        assert_eq!(outcome.filled_size.to_string(), "1");
    }

    fn cycle_in_state(state: FundingArbCycleState) -> FundingArbCycle {
        FundingArbCycle {
            cycle_id: uuid::Uuid::new_v4(),
            strategy_id: "fa_1".into(),
            config_revision: 1,
            sub_account: "0xabc".into(),
            perp_symbol: "BTC".into(),
            spot_symbol: "@1".into(),
            spot_display: "BTC/USDC".into(),
            base_token: "BTC".into(),
            quote_token: "USDC".into(),
            state,
            target_perp_size: Decimal::from_str_strict("1").unwrap(),
            target_spot_size: Decimal::from_str_strict("1").unwrap(),
            perp_open_size: Decimal::from_str_strict("1").unwrap(),
            spot_open_size: Decimal::from_str_strict("1").unwrap(),
            baseline_spot_size: Decimal::ZERO,
            entry_funding_rate: Decimal::from_str_strict("0.001").unwrap(),
            entry_basis_bps: Decimal::from_str_strict("10").unwrap(),
            revision: 0,
            spot_entry_cloid: None,
            perp_entry_cloid: None,
            compensation_cloid: None,
            perp_exit_cloid: None,
            spot_exit_cloid: None,
            error_code: None,
            error_message: None,
            opened_at: None,
            closed_at: None,
            created_at: None,
            updated_at: None,
        }
    }

    #[tokio::test]
    async fn start_recovers_open_cycle_and_faults_unresumable() {
        // A19 regression: after a restart the in-memory cycle binding is empty;
        // start() must rebind a persisted Open/Rebalancing/Exiting cycle and
        // fault an unresumable entry-intermediate one.
        let env = ScriptedEnv::new();
        env.state.lock().unwrap().current_cycle = Some(cycle_in_state(FundingArbCycleState::Open));
        let handle = scripted_runtime(&env);
        let adapter = FundingArbRuntimeAdapter {
            inner: Arc::new(tokio::sync::Mutex::new(handle)),
            stop_tx: tokio::sync::Mutex::new(None),
            task: tokio::sync::Mutex::new(None),
        };
        adapter.start().await.unwrap();
        assert!(
            env.state.lock().unwrap().cycle_states.is_empty(),
            "a recoverable Open cycle must not be faulted on recovery (A19)"
        );

        let env2 = ScriptedEnv::new();
        env2.state.lock().unwrap().current_cycle =
            Some(cycle_in_state(FundingArbCycleState::EnteringSpot));
        let handle2 = scripted_runtime(&env2);
        let adapter2 = FundingArbRuntimeAdapter {
            inner: Arc::new(tokio::sync::Mutex::new(handle2)),
            stop_tx: tokio::sync::Mutex::new(None),
            task: tokio::sync::Mutex::new(None),
        };
        adapter2.start().await.unwrap();
        let states = env2.state.lock().unwrap().cycle_states.clone();
        assert!(
            states
                .iter()
                .any(|(s, _)| *s == FundingArbCycleState::Faulted),
            "an unresumable cycle state must be faulted on recovery (A19): {states:?}"
        );
    }

    #[tokio::test]
    async fn leverage_is_set_before_spot_leg() {
        // A20 regression: the perp leverage must be set before the spot leg is
        // bought, so a leverage failure can never leave a naked spot long.
        let env = ScriptedEnv::new();
        let handle = scripted_runtime(&env);
        handle.open_cycle(&ScriptedEnv::plan()).await.unwrap();

        let ops = env.state.lock().unwrap().ops.clone();
        let leverage_at = ops
            .iter()
            .position(|o| o == "leverage")
            .expect("leverage op");
        let spot_at = ops.iter().position(|o| o == "spot_leg").expect("spot op");
        assert!(
            leverage_at < spot_at,
            "leverage must be set before the spot leg (A20): {ops:?}"
        );
    }

    #[tokio::test]
    async fn close_cycle_faults_on_inverted_perp_leg() {
        // A21 regression: an inverted (long) perp leg must fault the cycle
        // instead of being silently read as flat (which sold the spot and left
        // a naked long).
        let env = ScriptedEnv::new();
        // Build an OPEN cycle with a long perp position.
        env.state.lock().unwrap().current_cycle = Some(cycle_in_state(FundingArbCycleState::Open));
        env.state.lock().unwrap().perp_position = Decimal::from_str_strict("1").unwrap(); // inverted long
        env.state.lock().unwrap().spot_total = Decimal::from_str_strict("1").unwrap();
        let handle = scripted_runtime(&env);
        // Bind the Open cycle into the handle (the env's get_active returns it).
        handle
            .cycle
            .lock()
            .await
            .replace(cycle_in_state(FundingArbCycleState::Open));
        handle.close_cycle("test").await.unwrap();
        let states = env.state.lock().unwrap().cycle_states.clone();
        assert!(
            states
                .iter()
                .any(|(s, _)| *s == FundingArbCycleState::Faulted),
            "inverted perp leg must fault the cycle (A21): {states:?}"
        );
    }
}
