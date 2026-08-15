//! Execution engine — the sole order submission outlet, port of
//! `src/hypeedge/execution/engine.py`.
//!
//! Implements [`hypeedge_domain::traits::ExecutionClient`]: every order
//! mutation funnels through the serial nonce queue (guaranteeing monotonic
//! nonces and a single signing hot path), the kill switch is checked before
//! every placement, and cloid idempotency makes replays return the original
//! order. Exchange outcomes are applied only from authoritative responses —
//! timeouts degrade to `SUBMIT_UNKNOWN`/`CANCEL_UNKNOWN` for reconciliation,
//! never a blind resend.
//!
//! Design doc §9: "The execution module is the sole signing outlet, responsible
//! for nonce serialization, cloid generation, order submission/cancel/replace,
//! retries."

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::{Decimal, Price, Size};
use hypeedge_domain::enums::{OrderStatus, OrderType, Side, TimeInForce};
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::events::{DomainEvent, Event};
use hypeedge_domain::models::{Order, OrderIntent, RiskCheckResult};
use hypeedge_domain::traits::{
    DurableExecutionCommand, DurableOrderStore, ExecutionClient, MarketDataProvider,
};
use serde_json::Value;

use super::durable_worker::DurableCommandDispatcher;
use tokio::sync::Mutex;

use crate::execution::cloid::CloidGenerator;
use crate::execution::exchange::{AssetIndexProvider, ExchangeClient};
use crate::execution::nonce::NonceQueue;
use crate::execution::normalizer::OrderNormalizer;
use crate::execution::order_state::OrderStateMachine;
use crate::execution::signing::{CancelByCloidWire, OrderTypeWire, OrderWire, TifWire};
use crate::market_data::rate_limiter::RateLimiter;
use crate::risk::checker::RiskChecker;
use crate::risk::kill_switch::KillSwitch;
use crate::risk::safety::SafetyController;
use hypeedge_infra::event_bus::EventBus;

/// Sentinel the serial worker returns when the placement preflight aborted
/// (kill switch / safety gate). The caller classifies this as a safety abort
/// (→ CANCELLED `dispatch_aborted`) rather than a transport failure.
const SAFETY_ABORT_PREFIX: &str = "__safety_abort:";

/// 3-second submission timeout (design doc §9.4).
const SUBMIT_TIMEOUT_MS: u64 = 3000;

/// Timeout for cloid order-status queries used to resolve uncertain outcomes
/// (P1-1/C3): a hanging `/info` lookup must not stall the caller.
const CLOID_QUERY_TIMEOUT_MS: u64 = 5000;

/// Bounded wait for the exchange-action IP weight limiter (P2-6/M-EX1): the
/// serial worker must never stall on a full rate window.
const RATE_LIMIT_ACQUIRE_TIMEOUT_MS: u64 = 2000;

/// Batch ceiling for `cancel_all_orders` (P2-6/H-EX6): each batch is one signed
/// `cancelByCloid` action with IP weight `1 + floor(N/40)`.
const CANCEL_ALL_BATCH_LIMIT: usize = 100;

/// A durable kill trigger (e.g. the Postgres safety latch). Returns whether the
/// kill was durably recorded. Mirrors `durable_kill_trigger` in engine.py.
#[async_trait]
pub trait DurableKillTrigger: Send + Sync {
    async fn trigger(&self, reason: &str) -> bool;
}

/// Post-send hook for the durable worker (fault injection / observability).
pub type AfterSendHook = dyn Fn(&DurableExecutionCommand) + Send;

/// How an exchange submission ended, for the caller's state-machine handling.
enum SubmitOutcome {
    /// Exchange response to feed to [`ExecutionEngine::handle_submit_response`].
    Response(Value),
    /// Kill switch / safety gate aborted the placement.
    SafetyAborted(HypeEdgeError),
    /// 3s timeout and the cloid query did not resolve.
    Timeout,
    /// The exchange rejected the placement or transport failed.
    Rejected(String),
}

/// How a `cancelByCloid` submission ended.
enum CancelSubmit {
    /// Exchange response to feed to [`ExecutionEngine::handle_cancel_response`].
    Response(Value),
    /// 3s timeout; the cancel outcome is unknown.
    Timeout,
    /// The nonce worker returned an error.
    Failed(String),
}

fn risk_pass() -> RiskCheckResult {
    RiskCheckResult {
        passed: true,
        reason: None,
        checked_limits: Vec::new(),
    }
}

fn risk_fail(reason: impl Into<String>) -> RiskCheckResult {
    RiskCheckResult {
        passed: false,
        reason: Some(reason.into()),
        checked_limits: Vec::new(),
    }
}

fn tif_wire(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::Gtc => "Gtc",
        TimeInForce::Ioc => "Ioc",
        TimeInForce::Alo => "Alo",
        TimeInForce::Gtx => "Gtx",
    }
}

/// Build the HL wire order. A free function so the serial worker can rebuild
/// the wire after an in-worker market-reference refresh (M-EX6).
fn build_order_wire(
    asset_index_provider: Option<&dyn AssetIndexProvider>,
    intent: &OrderIntent,
    cloid: &str,
    reference_price: Option<Decimal>,
) -> Result<OrderWire, HypeEdgeError> {
    let Some(index_provider) = asset_index_provider else {
        return Err(HypeEdgeError::Execution {
            message: "no asset index provider".into(),
        });
    };
    let asset =
        index_provider
            .asset_index(&intent.symbol)
            .ok_or_else(|| HypeEdgeError::OrderRejected {
                message: format!("unknown symbol {}", intent.symbol),
                cloid: Some(cloid.to_string()),
                reason: Some("instrument_meta_unavailable".to_string()),
            })?;

    let is_buy = intent.side == Side::Buy;
    let sz = intent.size.inner().to_exact_string();
    let (px, tif): (String, &'static str) = match intent.order_type {
        OrderType::Limit => {
            let p = intent.price.ok_or_else(|| {
                HypeEdgeError::order_rejected(
                    "limit order requires a price",
                    Some(cloid.to_string()),
                    Some("price_required".to_string()),
                )
            })?;
            (p.inner().to_exact_string(), tif_wire(intent.time_in_force))
        }
        OrderType::Market => {
            // Market order: aggressive IoC priced with slippage.
            let reference = reference_price.ok_or_else(|| {
                HypeEdgeError::order_rejected(
                    "market order requires a reference price",
                    Some(cloid.to_string()),
                    Some("market_price_not_available".to_string()),
                )
            })?;
            let slippage =
                Decimal::from_i128(intent.max_slippage_bps as i128).div(Decimal::from_i128(10_000));
            let aggressive = if is_buy {
                reference.mul(Decimal::ONE + slippage)
            } else {
                reference.mul(Decimal::ONE - slippage)
            };
            (aggressive.to_exact_string(), "Ioc")
        }
        _ => {
            return Err(HypeEdgeError::order_rejected(
                "stop orders are not supported by the execution engine",
                Some(cloid.to_string()),
                Some("unsupported_order_type".to_string()),
            ));
        }
    };

    Ok(OrderWire {
        a: asset,
        b: is_buy,
        p: px,
        s: sz,
        r: intent.reduce_only,
        t: OrderTypeWire {
            limit: TifWire { tif },
        },
        c: Some(CloidGenerator::to_hl_cloid(cloid)),
    })
}

/// Acquire Hyperliquid exchange-action IP weight (`1 + floor(N/40)`) before a
/// signed send (P2-6/M-EX1). Bounded by `timeout` so the serial worker never
/// stalls on a full rate window; on failure the caller treats the action as
/// not-sent.
async fn acquire_exchange_weight(
    rl: Option<Arc<RateLimiter>>,
    batch_length: u64,
    timeout: Duration,
) -> Result<(), String> {
    let Some(rl) = rl else {
        return Ok(());
    };
    match tokio::time::timeout(timeout, rl.acquire("exchange", batch_length, 0)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(format!("ip_rate_limit:{e}")),
        Err(_) => Err(format!(
            "ip_rate_limit:acquire_timeout_{}ms",
            timeout.as_millis()
        )),
    }
}

/// M-EX6: refresh the market reference inside the serial worker, replacing
/// `reference` with a fresh mid when available. `Err(reason)` fails closed —
/// a market order must never be priced from a stale/absent snapshot.
async fn refresh_market_reference(
    provider: &Option<Arc<dyn MarketDataProvider>>,
    symbol: &str,
    stale_seconds: f64,
    reference: &mut Option<Decimal>,
) -> Result<(), String> {
    let Some(p) = provider else {
        if reference.is_none() {
            return Err("market_price_not_available".into());
        }
        return Ok(());
    };
    match p.get_price_snapshot(symbol).await {
        Ok(Some(snap)) => {
            let fresh = DateTime::from_timestamp_millis(snap.timestamp).is_none_or(|observed| {
                (Utc::now() - observed).num_milliseconds() as f64 / 1000.0 <= stale_seconds
            });
            if fresh {
                *reference = Some(snap.price);
                Ok(())
            } else {
                Err("market_price_stale".into())
            }
        }
        Ok(None) => Err("market_price_not_available".into()),
        Err(e) => Err(format!("market_price_error:{e}")),
    }
}

/// Shared risk-rejection escalation (M-EX3): a risk timeout puts the system
/// cancel-only; hard failures (check error / drawdown breach) hit the kill
/// switch. Mirrors the admission path in `handle_risk_rejection`.
async fn escalate_risk_rejection(
    safety: Option<Arc<Mutex<SafetyController>>>,
    durable_kill_trigger: Option<Arc<dyn DurableKillTrigger>>,
    kill_switch: Arc<KillSwitch>,
    reason: &str,
) {
    if reason.starts_with("risk_check_timeout") {
        if let Some(safety) = &safety {
            safety.lock().await.enter_cancel_only(reason);
        }
        return;
    }
    if !(reason.starts_with("risk_check_error") || reason.starts_with("drawdown_exceeded")) {
        return;
    }
    if let Some(trigger) = &durable_kill_trigger {
        let _ = trigger.trigger(reason).await;
    } else {
        kill_switch.trigger(reason).await;
    }
}

/// Canonical identity of an order intent, used as the auto-cloid key (A3): two
/// intents are the same order iff they serialize to the same key. `is_spot` is
/// part of the identity (M-EX4) — a perp and a spot order for the same symbol
/// must never collide on one cloid.
fn intent_key(intent: &OrderIntent) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        intent.symbol,
        intent.side.as_str(),
        intent.size.inner().to_exact_string(),
        intent
            .price
            .map(|p| p.inner().to_exact_string())
            .unwrap_or_default(),
        intent.order_type.as_str(),
        intent.time_in_force.as_str(),
        intent.reduce_only,
        intent.risk_reducing,
        intent.max_slippage_bps,
        intent.is_spot,
    )
}

/// Real execution engine. Clonable handle (all state is shared behind Arcs).
#[derive(Clone)]
pub struct ExecutionEngine {
    nonce: Arc<NonceQueue>,
    event_bus: Arc<EventBus>,
    kill_switch: Arc<KillSwitch>,
    state_machine: Arc<OrderStateMachine>,
    exchange: Arc<dyn ExchangeClient>,
    account_address: String,
    safety: Option<Arc<Mutex<SafetyController>>>,
    risk_checker: Option<Arc<RiskChecker>>,
    rate_limiter: Option<Arc<RateLimiter>>,
    durable_store: Option<Arc<dyn DurableOrderStore>>,
    market_data_provider: Option<Arc<dyn MarketDataProvider>>,
    order_normalizer: Option<Arc<OrderNormalizer>>,
    asset_index_provider: Option<Arc<dyn AssetIndexProvider>>,
    deferred_execution: bool,
    market_price_stale_seconds: f64,
    durable_kill_trigger: Option<Arc<dyn DurableKillTrigger>>,
    orders: Arc<Mutex<HashMap<String, Order>>>,
    /// Address action-quota ledger (B3): when configured, every placement
    /// passes `permission()`, debits the ledger after a successful send, and
    /// credits organic fill volume.
    action_budget: Option<Arc<Mutex<crate::risk::ActionBudgetController>>>,
    /// Wall-clock timeouts for the signed path (C3). Defaults to the module
    /// constants; tests shrink them to keep the suite fast (private fields —
    /// the public [`ExecutionEngineConfig`] is unaffected).
    submit_timeout: Duration,
    cloid_query_timeout: Duration,
    rate_limit_acquire_timeout: Duration,
}

/// Configuration for [`ExecutionEngine::new`].
pub struct ExecutionEngineConfig {
    pub nonce: Arc<NonceQueue>,
    pub event_bus: Arc<EventBus>,
    pub kill_switch: Arc<KillSwitch>,
    pub exchange: Arc<dyn ExchangeClient>,
    pub account_address: String,
    pub safety: Option<Arc<Mutex<SafetyController>>>,
    pub risk_checker: Option<Arc<RiskChecker>>,
    pub rate_limiter: Option<Arc<RateLimiter>>,
    pub durable_store: Option<Arc<dyn DurableOrderStore>>,
    pub market_data_provider: Option<Arc<dyn MarketDataProvider>>,
    pub order_normalizer: Option<Arc<OrderNormalizer>>,
    pub asset_index_provider: Option<Arc<dyn AssetIndexProvider>>,
    pub deferred_execution: bool,
    pub market_price_stale_seconds: f64,
    pub durable_kill_trigger: Option<Arc<dyn DurableKillTrigger>>,
    /// Address action-quota ledger (B3).
    pub action_budget: Option<Arc<Mutex<crate::risk::ActionBudgetController>>>,
}

impl ExecutionEngine {
    pub fn new(config: ExecutionEngineConfig) -> Self {
        Self {
            nonce: config.nonce,
            event_bus: config.event_bus,
            kill_switch: config.kill_switch,
            state_machine: Arc::new(OrderStateMachine::new()),
            exchange: config.exchange,
            account_address: config.account_address,
            safety: config.safety,
            risk_checker: config.risk_checker,
            rate_limiter: config.rate_limiter,
            durable_store: config.durable_store,
            market_data_provider: config.market_data_provider,
            order_normalizer: config.order_normalizer,
            asset_index_provider: config.asset_index_provider,
            deferred_execution: config.deferred_execution,
            market_price_stale_seconds: config.market_price_stale_seconds,
            durable_kill_trigger: config.durable_kill_trigger,
            orders: Arc::new(Mutex::new(HashMap::new())),
            action_budget: config.action_budget,
            submit_timeout: Duration::from_millis(SUBMIT_TIMEOUT_MS),
            cloid_query_timeout: Duration::from_millis(CLOID_QUERY_TIMEOUT_MS),
            rate_limit_acquire_timeout: Duration::from_millis(RATE_LIMIT_ACQUIRE_TIMEOUT_MS),
        }
    }

    fn publish(&self, payload: DomainEvent, correlation_id: &str) {
        if let Err(e) = self.event_bus.publish_sync(Arc::new(
            Event::new(payload).with_correlation_id(correlation_id),
        )) {
            tracing::warn!(event_type = %e.event_type, "event_bus_publish_sync_backpressure");
        }
    }

    async fn store(&self, order: &Order) {
        self.orders
            .lock()
            .await
            .insert(order.cloid.clone(), order.clone());
    }

    async fn load(&self, cloid: &str) -> Option<Order> {
        self.orders.lock().await.get(cloid).cloned()
    }

    // --- Order submission (design doc §9.1, §9.4) ---

    /// Kill-switch + safety placement gate. Returns `Err` when the placement is
    /// blocked. (No `&mut` needed — safety state is behind a mutex.)
    async fn run_gates(&self, intent: &OrderIntent) -> Result<(), HypeEdgeError> {
        self.kill_switch.check().await?;
        if let Some(safety) = &self.safety {
            safety.lock().await.check_placement(intent)?;
        }
        Ok(())
    }

    /// The full submit gate sequence (mirrors `submit_order` in engine.py).
    /// Ordinary placements use the `Place` budget channel
    /// (`emergency: false`).
    pub async fn submit_order_impl(
        &self,
        intent: OrderIntent,
        deferred: Option<bool>,
    ) -> Result<Order, HypeEdgeError> {
        self.submit_order_inner(intent, deferred, false).await
    }

    /// Emergency-close entry point (kill-triggered flatten, CancelOnly
    /// convergence): routes the placement through the budget
    /// `Close + emergency` channel, which bypasses the budget mode gates while
    /// still requiring address + IP margin (action_budget.rs `permission`).
    ///
    /// Integration note: the engine's kill-switch / safety-mode gates still
    /// apply exactly as for ordinary placements — bypassing *those* for a
    /// post-kill flatten is orchestrated by the caller (e.g. the kill hook /
    /// emergency-cancel executor in `crates/app`), which decides when the
    /// system is allowed to place a closing order.
    pub async fn submit_emergency_close(
        &self,
        intent: OrderIntent,
    ) -> Result<Order, HypeEdgeError> {
        self.submit_order_inner(intent, None, true).await
    }

    async fn submit_order_inner(
        &self,
        intent: OrderIntent,
        deferred: Option<bool>,
        emergency_close: bool,
    ) -> Result<Order, HypeEdgeError> {
        // Spot validity gates (unconditional).
        if intent.is_spot && intent.reduce_only {
            return Err(HypeEdgeError::order_rejected(
                "Spot orders cannot use reduce_only",
                intent.cloid.clone(),
                Some("spot_reduce_only_invalid".to_string()),
            ));
        }
        if intent.risk_reducing
            && !(intent.reduce_only || (intent.is_spot && intent.side == Side::Sell))
        {
            return Err(HypeEdgeError::order_rejected(
                "risk_reducing is valid only for perp reduce-only or spot sell orders",
                intent.cloid.clone(),
                Some("invalid_risk_reducing_intent".to_string()),
            ));
        }

        let deferred_execution = self.deferred_execution || deferred.unwrap_or(false);

        // Normalize against instrument rules (quantizes size/price).
        let intent = self.normalize_intent(intent).await?;

        // Canonical cloid. Caller-supplied cloids pass through; auto cloids are
        // deterministic from the normalized intent (A3) so a crash/replay with
        // the same intent reuses the same cloid and idempotency returns the
        // original order instead of double-submitting.
        let raw_cloid = match intent.cloid.clone() {
            Some(c) => c,
            None => {
                CloidGenerator::deterministic(intent.strategy_id.as_deref(), &intent_key(&intent))
            }
        };
        let cloid = CloidGenerator::to_hl_cloid(&raw_cloid);

        let intent = OrderIntent {
            sub_account: intent.sub_account.or_else(|| {
                if self.account_address.is_empty() {
                    None
                } else {
                    Some(self.account_address.to_lowercase())
                }
            }),
            cloid: Some(cloid.clone()),
            ..intent
        };

        // Idempotency precedes every new-placement gate. P2-5/H-EX3: an
        // in-memory SUBMITTED order with no durable row is a ghost left behind
        // by a failed persist — a replay must error rather than return it.
        let existing = match self.load(&cloid).await {
            Some(o) => {
                if self.durable_store.is_some()
                    && o.status == OrderStatus::Submitted
                    && self.durable_get(&cloid).await?.is_none()
                {
                    return Err(HypeEdgeError::Execution {
                        message: format!(
                            "durable placement missing for {cloid}; refusing ghost replay"
                        ),
                    });
                }
                Some(o)
            }
            None => self.durable_get(&cloid).await?,
        };
        if let Some(existing) = existing {
            if Self::matches_intent(&existing, &intent) {
                tracing::info!(cloid = %cloid, status = existing.status.as_str(), "order_idempotent_replay");
                return Ok(existing);
            }
            return Err(HypeEdgeError::order_rejected(
                format!("Cloid {cloid} is already bound to a different order"),
                Some(cloid),
                Some("cloid_payload_conflict".to_string()),
            ));
        }

        // New-placement gates run only after canonical cloid deduplication.
        match self.run_gates(&intent).await {
            Ok(()) => {}
            Err(e) => {
                if !deferred_execution || self.durable_store.is_none() {
                    return Err(e);
                }
                let reason = e.code().to_string();
                let order = self.rejected_order(&intent, &reason);
                self.persist_placement(&order, &risk_fail(&reason), None, false, None)
                    .await?;
                return Ok(order);
            }
        }
        // Resolve the reference price (A1/A16). The provider snapshot is
        // fetched for every order — not just market orders — so the risk
        // checker's notional uses the live mid rather than the order's own
        // limit price. Market orders *require* a fresh mid (fail-closed when
        // the provider is absent or the price is stale). Limit orders also
        // fail-closed when a provider is configured (P1-6/A16 regression: the
        // old fallback to the order's own price let the order control the
        // number risk checks ran against); without a configured provider the
        // legacy fallback to the intent price is kept (dry-run/test mode).
        let market_snap = match &self.market_data_provider {
            Some(p) => p.get_price_snapshot(&intent.symbol).await?,
            None => None,
        };
        let stale = match &market_snap {
            Some(snap) => DateTime::from_timestamp_millis(snap.timestamp).is_some_and(|observed| {
                let age = (Utc::now() - observed).num_milliseconds() as f64 / 1000.0;
                age > self.market_price_stale_seconds
            }),
            None => false,
        };
        let reference_price = match &market_snap {
            Some(snap) if !stale => Some(snap.price),
            // P1-6: with a provider configured, a missing/stale snapshot must
            // not fall back to the order's own price — fail closed instead.
            _ if self.market_data_provider.is_some() => None,
            _ => intent.price.map(|p| p.inner()),
        };
        if self.market_data_provider.is_some() && reference_price.is_none() {
            // P1-6: a configured provider with a missing/stale snapshot means
            // the risk check cannot be priced from trustworthy market data —
            // fail closed for limit orders exactly like market orders.
            let reason = if market_snap.is_none() {
                "market_price_not_available"
            } else {
                "market_price_stale"
            };
            let risk = risk_fail(reason);
            let order = self.rejected_order(&intent, reason);
            self.persist_placement(&order, &risk, None, false, reference_price)
                .await?;
            return Ok(order);
        }
        if intent.order_type == OrderType::Market && reference_price.is_none() {
            let reason = if market_snap.is_none() {
                "market_price_not_available"
            } else {
                "market_price_stale"
            };
            let risk = risk_fail(reason);
            let order = self.rejected_order(&intent, reason);
            self.persist_placement(&order, &risk, None, false, reference_price)
                .await?;
            return Ok(order);
        }

        // Risk check (in-process, fail-safe timeout).
        let mut risk_result = risk_pass();
        if let Some(checker) = &self.risk_checker {
            risk_result = checker.check(&intent, reference_price).await;
            if !risk_result.passed {
                let reason = risk_result
                    .reason
                    .clone()
                    .unwrap_or_else(|| "risk_check_rejected".into());
                self.handle_risk_rejection(&reason).await;
                let order = self.rejected_order(&intent, &reason);
                self.persist_placement(&order, &risk_result, None, false, reference_price)
                    .await?;
                return Ok(order);
            }
        }

        // Action credits check.
        if self
            .rate_limiter
            .as_ref()
            .is_some_and(|rl| !rl.check_action_credits())
        {
            tracing::warn!(cloid = %cloid, "order_rejected_action_credits_low");
            let risk = risk_fail("action_credits_below_threshold");
            let order = self.rejected_order(&intent, "action_credits_below_threshold");
            self.persist_placement(&order, &risk, None, false, reference_price)
                .await?;
            return Ok(order);
        }

        // Create local Order (PENDING) and transition to SUBMITTED.
        let mut order = Order {
            cloid: cloid.clone(),
            symbol: intent.symbol.clone(),
            side: intent.side,
            size: intent.size,
            price: intent.price,
            order_type: intent.order_type,
            time_in_force: intent.time_in_force,
            status: OrderStatus::Pending,
            strategy_id: intent.strategy_id.clone(),
            sub_account: intent.sub_account.clone(),
            reduce_only: intent.reduce_only,
            is_spot: intent.is_spot,
            risk_reducing: intent.risk_reducing,
            max_slippage_bps: intent.max_slippage_bps,
            ..Order::new(
                "".into(),
                "".into(),
                Side::Buy,
                Size::ZERO,
                None,
                OrderType::Limit,
                TimeInForce::Gtc,
            )
        };
        // P2-5/H-EX3: persist the placement *before* exposing the order in
        // memory, so a persist failure can never leave a ghost SUBMITTED order
        // behind (the idempotent replay would otherwise return a phantom).
        self.state_machine
            .transition(&mut order, OrderStatus::Submitted, Some("submit_order"))?;
        order.submitted_at = Some(Utc::now());
        let command_id = uuid::Uuid::new_v4();
        let durable_risk = self
            .persist_placement(
                &order,
                &risk_result,
                Some(command_id),
                true,
                reference_price,
            )
            .await
            .map_err(|e| {
                tracing::error!(cloid = %cloid, error = %e, "order_persist_failed_no_ghost");
                e
            })?;
        self.store(&order).await;
        if let Some(risk) = durable_risk.filter(|r| !r.passed) {
            order.status = OrderStatus::Rejected;
            order.error_message = risk.reason.clone();
            self.store(&order).await;
            self.publish(DomainEvent::OrderRejected(order.clone()), &cloid);
            return Ok(order);
        }
        self.publish(DomainEvent::OrderSubmitted(order.clone()), &cloid);

        tracing::info!(
            cloid = %cloid,
            symbol = %intent.symbol,
            side = intent.side.as_str(),
            size = intent.size.to_string(),
            price = intent.price.map(|p| p.to_string()).unwrap_or_default(),
            order_type = intent.order_type.as_str(),
            "order_submitting"
        );

        if deferred_execution {
            return Ok(order);
        }

        // Submit to exchange through the serial nonce queue.
        let outcome = self
            .submit_to_exchange(&intent, &cloid, reference_price, emergency_close)
            .await;
        match outcome {
            SubmitOutcome::Response(resp) => {
                self.handle_submit_response(&mut order, resp, Some(command_id))
                    .await?;
            }
            SubmitOutcome::SafetyAborted(e) => {
                self.state_machine.transition(
                    &mut order,
                    OrderStatus::Cancelled,
                    Some("dispatch_aborted_by_safety_gate"),
                )?;
                order.error_message = Some(e.to_string());
                self.persist_transition(
                    &order,
                    "dispatch_aborted",
                    Some(command_id),
                    Some("cancelled"),
                )
                .await?;
                self.publish(DomainEvent::OrderCancelled(order.clone()), &cloid);
            }
            SubmitOutcome::Timeout => {
                self.state_machine.transition(
                    &mut order,
                    OrderStatus::SubmitUnknown,
                    Some("submit_timeout"),
                )?;
                order.error_message = Some("exchange_action_outcome_unknown".into());
                self.persist_transition(
                    &order,
                    "submit_unknown",
                    Some(command_id),
                    Some("unknown"),
                )
                .await?;
                tracing::error!(cloid = %cloid, "order_submit_unknown");
            }
            SubmitOutcome::Rejected(msg) => {
                self.state_machine
                    .transition(&mut order, OrderStatus::Rejected, Some(&msg))?;
                order.error_message = Some(msg.clone());
                self.persist_transition(&order, "rejected", Some(command_id), Some("failed"))
                    .await?;
                self.publish(DomainEvent::OrderRejected(order.clone()), &cloid);
                tracing::error!(cloid = %cloid, error = %msg, "order_rejected");
            }
        }
        self.store(&order).await;
        Ok(order)
    }

    async fn normalize_intent(&self, intent: OrderIntent) -> Result<OrderIntent, HypeEdgeError> {
        let Some(normalizer) = &self.order_normalizer else {
            return Ok(intent);
        };
        let (best_bid, best_ask) = match &self.market_data_provider {
            Some(p) => p
                .get_best_bid_ask(&intent.symbol)
                .await?
                .map(|(b, a)| (Some(b), Some(a)))
                .unwrap_or((None, None)),
            None => (None, None),
        };
        normalizer.normalize(&intent, best_bid, best_ask)
    }

    async fn durable_get(&self, cloid: &str) -> Result<Option<Order>, HypeEdgeError> {
        match &self.durable_store {
            Some(s) => s.get_order(cloid).await,
            None => Ok(None),
        }
    }

    /// The submit to the exchange: build the wire, run the placement preflight
    /// inside the serial worker (so queued work cannot survive a kill/safety
    /// transition), sign + send, and resolve timeouts by cloid query.
    ///
    /// `emergency_close` selects the budget `Close + emergency` channel
    /// (bypasses the budget mode gates; still gated on address + IP margin).
    async fn submit_to_exchange(
        &self,
        intent: &OrderIntent,
        cloid: &str,
        reference_price: Option<Decimal>,
        emergency_close: bool,
    ) -> SubmitOutcome {
        // Build the wire before enqueueing so payload errors surface promptly.
        let mut wire = match build_order_wire(
            self.asset_index_provider.as_deref(),
            intent,
            cloid,
            reference_price,
        ) {
            Ok(w) => w,
            Err(e) => return SubmitOutcome::Rejected(e.to_string()),
        };
        let hl_cloid = CloidGenerator::to_hl_cloid(cloid);
        let ks = self.kill_switch.clone();
        let safety = self.safety.clone();
        let rl = self.rate_limiter.clone();
        let exchange = self.exchange.clone();
        let risk_checker = self.risk_checker.clone();
        let intent_for_preflight = intent.clone();
        let action_budget = self.action_budget.clone();
        let cloid_owned = cloid.to_string();
        let durable_kill_trigger = self.durable_kill_trigger.clone();
        let market_data_provider = self.market_data_provider.clone();
        let asset_index_provider = self.asset_index_provider.clone();
        let stale_seconds = self.market_price_stale_seconds;
        let rate_limit_acquire_timeout = self.rate_limit_acquire_timeout;

        // Submit inside the serial nonce worker with a 3s timeout (design doc
        // §9.4). A timeout means the outcome is unknown: resolve it by cloid
        // query before classifying, never blind-resend.
        let submitted = tokio::time::timeout(
            self.submit_timeout,
            self.nonce.submit("order", move |nonce| {
                let ks = ks.clone();
                let safety = safety.clone();
                let rl = rl.clone();
                let exchange = exchange.clone();
                let risk_checker = risk_checker.clone();
                let action_budget = action_budget.clone();
                let intent_for_preflight = intent_for_preflight.clone();
                let durable_kill_trigger = durable_kill_trigger.clone();
                let market_data_provider = market_data_provider.clone();
                let asset_index_provider = asset_index_provider.clone();
                let rate_limit_acquire_timeout = rate_limit_acquire_timeout;
                let mut reference_price = reference_price;
                Box::pin(async move {
                    // Preflight inside the worker, immediately before signing.
                    if let Err(e) = ks.check().await {
                        return Err(format!("{SAFETY_ABORT_PREFIX}{e}"));
                    }
                    let placement = match &safety {
                        Some(s) => s.lock().await.check_placement(&intent_for_preflight),
                        None => Ok(()),
                    };
                    if let Err(e) = placement {
                        return Err(format!("{SAFETY_ABORT_PREFIX}{e}"));
                    }
                    if rl.as_ref().is_some_and(|rl| !rl.check_action_credits()) {
                        return Err(format!("{SAFETY_ABORT_PREFIX}action_credits_below_threshold"));
                    }
                    // M-EX6: re-resolve the market reference inside the worker
                    // so a queued market order never prices its aggressive IoC
                    // from a mid that went stale while it waited (fail-closed).
                    if intent_for_preflight.order_type == OrderType::Market {
                        if let Err(reason) = refresh_market_reference(
                            &market_data_provider,
                            &intent_for_preflight.symbol,
                            stale_seconds,
                            &mut reference_price,
                        )
                        .await
                        {
                            return Err(format!("{SAFETY_ABORT_PREFIX}{reason}"));
                        }
                        wire = match build_order_wire(
                            asset_index_provider.as_deref(),
                            &intent_for_preflight,
                            &cloid_owned,
                            reference_price,
                        ) {
                            Ok(w) => w,
                            Err(e) => return Err(format!("{SAFETY_ABORT_PREFIX}{e}")),
                        };
                    }
                    // M-EX1/P2-6: acquire exchange-action IP weight
                    // (1 + floor(N/40)) before the signed send.
                    if let Err(e) =
                        acquire_exchange_weight(rl.clone(), 1, rate_limit_acquire_timeout).await
                    {
                        return Err(format!("{SAFETY_ABORT_PREFIX}{e}"));
                    }
                    // Re-run the risk checker immediately before the signed send
                    // (B2): N queued orders must not all pass against the same
                    // pre-queue account state, and a drawdown/leverage breach
                    // that develops after admission must stop the placement.
                    if let Some(checker) = &risk_checker {
                        let result = checker.check(&intent_for_preflight, reference_price).await;
                        if !result.passed {
                            let reason = result
                                .reason
                                .unwrap_or_else(|| "risk_check_rejected".into());
                            // M-EX3: escalate inside the worker exactly like the
                            // admission path — a risk timeout puts the system
                            // cancel-only, hard failures hit the kill switch.
                            escalate_risk_rejection(
                                safety.clone(),
                                durable_kill_trigger.clone(),
                                ks.clone(),
                                &reason,
                            )
                            .await;
                            return Err(format!("{SAFETY_ABORT_PREFIX}{reason}"));
                        }
                    }
                    // Action-quota permission (B3): reject when the address
                    // budget does not permit a placement. Emergency closes use
                    // the `Close + emergency` channel (bypasses budget mode
                    // gates; still gated on address + IP margin).
                    if let Some(budget) = &action_budget {
                        let request = crate::risk::PermissionRequest {
                            action: if emergency_close {
                                crate::risk::BudgetAction::Close
                            } else {
                                crate::risk::BudgetAction::Place
                            },
                            strategy_id: intent_for_preflight.strategy_id.clone(),
                            symbol: intent_for_preflight
                                .strategy_id
                                .as_ref()
                                .map(|_| intent_for_preflight.symbol.clone()),
                            child_actions: 1,
                            ip_weight: 1,
                            risk_reducing: intent_for_preflight.reduce_only
                                || intent_for_preflight.risk_reducing,
                            emergency: emergency_close,
                        };
                        let permission = budget.lock().await.permission(&request);
                        if let Ok(permission) = permission
                            && !permission.allowed
                        {
                            return Err(format!(
                                "{SAFETY_ABORT_PREFIX}action_budget:{}",
                                permission.reason
                            ));
                        }
                    }
                    let result = exchange.order(vec![wire], nonce).await;
                    // Debit the address ledger for the placement (B3).
                    if let Some(budget) = &action_budget {
                        let debit = crate::risk::NetworkAttemptDebit {
                            attempt_id: cloid_owned.clone(),
                            child_actions: vec![if emergency_close {
                                crate::risk::BudgetAction::Close
                            } else {
                                crate::risk::BudgetAction::Place
                            }],
                            ip_weight: 1,
                            occurred_at: Utc::now(),
                            strategy_id: intent_for_preflight.strategy_id.clone(),
                            symbol: intent_for_preflight
                                .strategy_id
                                .as_ref()
                                .map(|_| intent_for_preflight.symbol.clone()),
                        };
                        if let Err(e) = budget.lock().await.debit_network_attempt(debit) {
                            tracing::error!(cloid = %cloid_owned, error = %e, "action_budget_debit_failed");
                        }
                    }
                    result
                })
            }),
        )
        .await;

        match submitted {
            Ok(Ok(value)) => SubmitOutcome::Response(value),
            Ok(Err(msg)) if msg.starts_with(SAFETY_ABORT_PREFIX) => {
                let rest = &msg[SAFETY_ABORT_PREFIX.len()..];
                SubmitOutcome::SafetyAborted(HypeEdgeError::Execution {
                    message: rest.to_string(),
                })
            }
            Ok(Err(msg)) => {
                // Transport/exchange failure (non-timeout). With a cloid the
                // Python nonce manager never blindly retries: it resolves by
                // cloid query. Found → apply the authoritative outcome; not
                // found → the outcome is genuinely unknown (OrderTimeoutError
                // in Python → SUBMIT_UNKNOWN), never a fabricated rejection.
                tracing::warn!(cloid = %hl_cloid, error = %msg, "exchange_action_transport_error");
                match self.query_order_by_cloid(&hl_cloid).await {
                    Ok(Some(resp)) => SubmitOutcome::Response(resp),
                    _ => SubmitOutcome::Timeout,
                }
            }
            Err(_) => {
                // 3s timeout: the action may still land on the exchange. Resolve
                // by cloid query; absence keeps it UNKNOWN for reconciliation.
                match self.query_order_by_cloid(&hl_cloid).await {
                    Ok(Some(resp)) => SubmitOutcome::Response(resp),
                    _ => SubmitOutcome::Timeout,
                }
            }
        }
    }

    /// Query the authoritative order status by cloid with a bounded timeout
    /// (P1-1/C3): a hanging `/info` lookup must not stall the caller.
    async fn query_order_by_cloid(&self, hl_cloid: &str) -> Result<Option<Value>, String> {
        match tokio::time::timeout(
            self.cloid_query_timeout,
            self.exchange.query_order_by_cloid(hl_cloid),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(format!(
                "cloid query timed out after {}ms",
                self.cloid_query_timeout.as_millis()
            )),
        }
    }

    /// Resolve the reference price for a durable replay (P1-6/A16). Limit
    /// orders use a fresh mid when a provider is configured (falling back to
    /// the order's own price only without a provider); market orders require a
    /// fresh mid. `None` means the caller must fail closed.
    async fn resolve_replay_reference(
        &self,
        order: &Order,
    ) -> Result<Option<Decimal>, HypeEdgeError> {
        let fallback = order.price.map(|p| p.inner());
        let Some(provider) = &self.market_data_provider else {
            return Ok(fallback);
        };
        match provider.get_price_snapshot(&order.symbol).await? {
            Some(snap) => {
                let fresh =
                    DateTime::from_timestamp_millis(snap.timestamp).is_none_or(|observed| {
                        (Utc::now() - observed).num_milliseconds() as f64 / 1000.0
                            <= self.market_price_stale_seconds
                    });
                Ok(if fresh { Some(snap.price) } else { None })
            }
            None => Ok(None),
        }
    }

    // --- Exchange response handling (design doc §9.4) ---

    async fn handle_submit_response(
        &self,
        order: &mut Order,
        response: Value,
        command_id: Option<uuid::Uuid>,
    ) -> Result<(), HypeEdgeError> {
        let cloid = order.cloid.clone();
        let is_object = response.is_object();

        let status = response.get("status").and_then(|s| s.as_str());
        match status {
            Some("ok") => {
                let statuses = response
                    .pointer("/response/data/statuses")
                    .and_then(|s| s.as_array());
                match statuses {
                    Some(statuses) if !statuses.is_empty() => {
                        let first = &statuses[0];
                        if let Some(resting) = first.get("resting") {
                            let oid = resting.get("oid").map(|o| o.to_string());
                            order.exchange_oid = oid;
                            // H-EX2: a resting response can carry a same-entry
                            // `filled` (partial or full fill alongside the ack).
                            // Merge it — never drop the fill — and publish the
                            // matching fill event + quota credit.
                            if let Some(fill) = first.get("filled") {
                                let filled_size = Size::new(
                                    fill.get("totalSz")
                                        .and_then(|v| v.as_str())
                                        .and_then(|s| Decimal::from_str_lenient(s).ok())
                                        .unwrap_or(order.size.inner()),
                                );
                                let filled_price = Price::new(
                                    fill.get("avgPx")
                                        .and_then(|v| v.as_str())
                                        .and_then(|s| Decimal::from_str_lenient(s).ok())
                                        .unwrap_or(Decimal::ZERO),
                                );
                                order.filled_size = filled_size;
                                order.avg_fill_price = Some(filled_price);
                                order.filled_at = Some(Utc::now());
                                self.record_fill(filled_size.inner() * filled_price.inner())
                                    .await;
                                if filled_size.inner() >= order.size.inner() {
                                    self.state_machine.transition(
                                        order,
                                        OrderStatus::Filled,
                                        Some("resting_filled"),
                                    )?;
                                    self.persist_transition(
                                        order,
                                        "filled",
                                        command_id,
                                        Some("succeeded"),
                                    )
                                    .await?;
                                    self.publish(DomainEvent::OrderFilled(order.clone()), &cloid);
                                } else {
                                    self.state_machine.transition(
                                        order,
                                        OrderStatus::PartialFill,
                                        Some("resting_partial_fill"),
                                    )?;
                                    self.persist_transition(
                                        order,
                                        "partial_fill",
                                        command_id,
                                        Some("succeeded"),
                                    )
                                    .await?;
                                    self.publish(
                                        DomainEvent::OrderPartialFill(order.clone()),
                                        &cloid,
                                    );
                                }
                            } else {
                                self.state_machine.transition(
                                    order,
                                    OrderStatus::Acknowledged,
                                    Some("exchange_ack"),
                                )?;
                                order.acknowledged_at = Some(Utc::now());
                                self.persist_transition(
                                    order,
                                    "acknowledged",
                                    command_id,
                                    Some("succeeded"),
                                )
                                .await?;
                                self.publish(DomainEvent::OrderAcknowledged(order.clone()), &cloid);
                            }
                        } else if first.get("filled").is_some() {
                            let fill = &first["filled"];
                            let provisional_size = Size::new(
                                fill.get("totalSz")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| Decimal::from_str_lenient(s).ok())
                                    .unwrap_or(order.size.inner()),
                            );
                            let provisional_price = Price::new(
                                fill.get("avgPx")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| Decimal::from_str_lenient(s).ok())
                                    .unwrap_or(Decimal::ZERO),
                            );
                            self.state_machine.transition(
                                order,
                                OrderStatus::Filled,
                                Some("immediate_fill"),
                            )?;
                            order.filled_at = Some(Utc::now());
                            order.exchange_oid = fill.get("oid").map(|o| o.to_string());
                            order.filled_size = provisional_size;
                            order.avg_fill_price = Some(provisional_price);
                            // Credit the address action quota with the organic
                            // fill volume (B3).
                            self.record_fill(provisional_size.inner() * provisional_price.inner())
                                .await;
                            self.persist_transition(order, "filled", command_id, Some("succeeded"))
                                .await?;
                            self.publish(DomainEvent::OrderFilled(order.clone()), &cloid);
                        } else if let Some(err) = first.get("error") {
                            let msg = err.as_str().unwrap_or("unknown_error").to_string();
                            self.state_machine.transition(
                                order,
                                OrderStatus::Rejected,
                                Some(&msg),
                            )?;
                            order.error_message = Some(msg.clone());
                            self.persist_transition(order, "rejected", command_id, Some("failed"))
                                .await?;
                            self.publish(DomainEvent::OrderRejected(order.clone()), &cloid);
                        }
                        // else: no resting/filled/error key — leave the order
                        // SUBMITTED (mirrors the Python no-op).
                    }
                    _ => {
                        // Accepted but no detailed status.
                        self.state_machine.transition(
                            order,
                            OrderStatus::Acknowledged,
                            Some("exchange_ack"),
                        )?;
                        order.acknowledged_at = Some(Utc::now());
                        self.persist_transition(
                            order,
                            "acknowledged",
                            command_id,
                            Some("succeeded"),
                        )
                        .await?;
                        self.publish(DomainEvent::OrderAcknowledged(order.clone()), &cloid);
                    }
                }
            }
            Some("err") => {
                let msg = response
                    .get("response")
                    .map(|r| r.to_string())
                    .unwrap_or_else(|| "unknown_error".into());
                self.state_machine.transition(
                    order,
                    OrderStatus::Rejected,
                    Some("exchange_err"),
                )?;
                order.error_message = Some(msg);
                self.persist_transition(order, "rejected", command_id, Some("failed"))
                    .await?;
                self.publish(DomainEvent::OrderRejected(order.clone()), &cloid);
            }
            Some("order") => {
                // orderStatus lookup after an uncertain submission.
                let status_data = response.get("order").cloned().unwrap_or(Value::Null);
                let exchange_status = status_data
                    .get("status")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_string();
                let terminal = Self::terminal_exchange_status(&exchange_status);
                match terminal {
                    Some(OrderStatus::Filled) => {
                        self.state_machine.transition(
                            order,
                            OrderStatus::Filled,
                            Some("status_query_filled"),
                        )?;
                        let order_payload = status_data
                            .get("order")
                            .filter(|v| v.is_object())
                            .unwrap_or(&status_data);
                        if let Some(total_sz) = order_payload
                            .get("totalSz")
                            .and_then(|v| v.as_str())
                            .and_then(|s| Decimal::from_str_lenient(s).ok())
                        {
                            order.filled_size = Size::new(total_sz.max(order.filled_size.inner()));
                        }
                        if let Some(avg_px) = order_payload
                            .get("avgPx")
                            .and_then(|v| v.as_str())
                            .and_then(|s| Decimal::from_str_lenient(s).ok())
                            .filter(|d| *d > Decimal::ZERO)
                        {
                            order.avg_fill_price = Some(Price::new(avg_px));
                        }
                        order.filled_at = Some(Utc::now());
                        self.record_fill(
                            order.filled_size.inner()
                                * order
                                    .avg_fill_price
                                    .map(|p| p.inner())
                                    .unwrap_or(Decimal::ZERO),
                        )
                        .await;
                        self.persist_transition(order, "filled", command_id, Some("succeeded"))
                            .await?;
                        self.publish(DomainEvent::OrderFilled(order.clone()), &cloid);
                    }
                    Some(OrderStatus::Cancelled) => {
                        self.state_machine.transition(
                            order,
                            OrderStatus::Cancelled,
                            Some("status_query_cancelled"),
                        )?;
                        self.persist_transition(order, "cancelled", command_id, Some("succeeded"))
                            .await?;
                        self.publish(DomainEvent::OrderCancelled(order.clone()), &cloid);
                    }
                    Some(OrderStatus::Rejected) => {
                        self.state_machine.transition(
                            order,
                            OrderStatus::Rejected,
                            Some("status_query_rejected"),
                        )?;
                        self.persist_transition(order, "rejected", command_id, Some("failed"))
                            .await?;
                        self.publish(DomainEvent::OrderRejected(order.clone()), &cloid);
                    }
                    Some(OrderStatus::Expired) => {
                        self.state_machine.transition(
                            order,
                            OrderStatus::Expired,
                            Some("status_query_expired"),
                        )?;
                        self.persist_transition(order, "expired", command_id, Some("failed"))
                            .await?;
                        self.publish(DomainEvent::OrderExpired(order.clone()), &cloid);
                    }
                    _ => {
                        self.state_machine.transition(
                            order,
                            OrderStatus::Acknowledged,
                            Some("status_query_open"),
                        )?;
                        order.acknowledged_at = Some(Utc::now());
                        self.persist_transition(
                            order,
                            "acknowledged",
                            command_id,
                            Some("succeeded"),
                        )
                        .await?;
                        self.publish(DomainEvent::OrderAcknowledged(order.clone()), &cloid);
                    }
                }
            }
            _ => {
                if is_object {
                    // Unknown response must not be treated as an acknowledgement.
                    self.state_machine.transition(
                        order,
                        OrderStatus::SubmitUnknown,
                        Some("unknown_response"),
                    )?;
                    order.error_message = Some("unknown_exchange_response".into());
                    self.persist_transition(order, "submit_unknown", command_id, Some("unknown"))
                        .await?;
                } else {
                    // Non-object response (e.g. raw market_open data).
                    self.state_machine.transition(
                        order,
                        OrderStatus::Acknowledged,
                        Some("exchange_ack"),
                    )?;
                    order.acknowledged_at = Some(Utc::now());
                    self.persist_transition(order, "acknowledged", command_id, Some("succeeded"))
                        .await?;
                    self.publish(DomainEvent::OrderAcknowledged(order.clone()), &cloid);
                }
            }
        }
        Ok(())
    }

    /// Classify documented Hyperliquid terminal order-status variants.
    fn terminal_exchange_status(raw: &str) -> Option<OrderStatus> {
        let value = raw.trim().to_lowercase().replace('_', "");
        if value == "filled" {
            return Some(OrderStatus::Filled);
        }
        if value == "margincanceled" || value == "rejected" || value.ends_with("rejected") {
            return Some(OrderStatus::Rejected);
        }
        if matches!(
            value.as_str(),
            "canceled" | "cancelled" | "ioccancel" | "scheduledcancel"
        ) || value.ends_with("canceled")
            || value.ends_with("cancelled")
        {
            return Some(OrderStatus::Cancelled);
        }
        if value == "expired" {
            return Some(OrderStatus::Expired);
        }
        None
    }

    // --- Cancellation ---

    async fn handle_cancel_response(
        &self,
        order: &mut Order,
        response: Value,
        command_id: Option<uuid::Uuid>,
    ) -> Result<bool, HypeEdgeError> {
        let cloid = order.cloid.clone();
        let is_object = response.is_object();
        if !is_object {
            self.mark_cancel_unknown(order, "invalid_cancel_response", command_id)
                .await;
            return Ok(false);
        }
        let top_status = response
            .get("status")
            .and_then(|s| s.as_str())
            .unwrap_or("");
        match top_status {
            "order" => {
                let status_data = response.get("order").cloned().unwrap_or(Value::Null);
                let exchange_status = status_data
                    .get("status")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_string();
                match Self::terminal_exchange_status(&exchange_status) {
                    Some(OrderStatus::Cancelled) => Ok(self
                        .mark_cancelled(order, "cancel_status_confirmed", command_id)
                        .await?),
                    Some(OrderStatus::Filled) => {
                        self.state_machine.transition(
                            order,
                            OrderStatus::Filled,
                            Some("cancel_status_filled"),
                        )?;
                        let order_payload = status_data
                            .get("order")
                            .filter(|v| v.is_object())
                            .unwrap_or(&status_data);
                        if let Some(total_sz) = order_payload
                            .get("totalSz")
                            .and_then(|v| v.as_str())
                            .and_then(|s| Decimal::from_str_lenient(s).ok())
                        {
                            order.filled_size = Size::new(total_sz.max(order.filled_size.inner()));
                        }
                        if let Some(avg_px) = order_payload
                            .get("avgPx")
                            .and_then(|v| v.as_str())
                            .and_then(|s| Decimal::from_str_lenient(s).ok())
                            .filter(|d| *d > Decimal::ZERO)
                        {
                            order.avg_fill_price = Some(Price::new(avg_px));
                        }
                        order.filled_at = Some(Utc::now());
                        self.record_fill(
                            order.filled_size.inner()
                                * order
                                    .avg_fill_price
                                    .map(|p| p.inner())
                                    .unwrap_or(Decimal::ZERO),
                        )
                        .await;
                        order.error_message = Some("cancel_not_applied_order_filled".into());
                        self.persist_transition(order, "filled", command_id, Some("failed"))
                            .await?;
                        tracing::warn!(cloid = %cloid, "cancel_order_already_filled");
                        Ok(false)
                    }
                    Some(OrderStatus::Rejected) | Some(OrderStatus::Expired) => {
                        self.state_machine.transition(
                            order,
                            OrderStatus::Rejected,
                            Some("cancel_status_rejected"),
                        )?;
                        order.error_message = Some("cancel_not_applied_order_rejected".into());
                        self.persist_transition(order, "rejected", command_id, Some("failed"))
                            .await?;
                        Ok(false)
                    }
                    _ => {
                        // Most commonly `open` from a post-timeout lookup; the
                        // original cancel may still arrive later.
                        let reason = format!("cancel_status_{}", exchange_status);
                        self.mark_cancel_unknown(order, &reason, command_id).await;
                        Ok(false)
                    }
                }
            }
            "ok" => {
                let statuses = response
                    .pointer("/response/data/statuses")
                    .and_then(|s| s.as_array())
                    .cloned()
                    .unwrap_or_default();
                if let Some(first) = statuses.first() {
                    if let Some(s) = first.as_str() {
                        if s.eq_ignore_ascii_case("success") {
                            return self
                                .mark_cancelled(order, "cancel_exchange_success", command_id)
                                .await;
                        }
                    } else if first.get("error").is_some() {
                        let msg = first["error"].to_string();
                        order.error_message = Some(msg);
                        self.persist_transition(order, "cancel_failed", command_id, Some("failed"))
                            .await?;
                        tracing::warn!(cloid = %cloid, "cancel_order_rejected");
                        return Ok(false);
                    }
                }
                self.mark_cancel_unknown(order, "unknown_cancel_response", command_id)
                    .await;
                Ok(false)
            }
            "err" => {
                let msg = response
                    .get("response")
                    .map(|r| r.to_string())
                    .unwrap_or_else(|| "cancel_rejected".into());
                order.error_message = Some(msg);
                self.persist_transition(order, "cancel_failed", command_id, Some("failed"))
                    .await?;
                tracing::warn!(cloid = %cloid, "cancel_order_rejected");
                Ok(false)
            }
            _ => {
                self.mark_cancel_unknown(order, "unknown_cancel_response", command_id)
                    .await;
                Ok(false)
            }
        }
    }

    async fn mark_cancelled(
        &self,
        order: &mut Order,
        reason: &str,
        command_id: Option<uuid::Uuid>,
    ) -> Result<bool, HypeEdgeError> {
        self.state_machine
            .transition(order, OrderStatus::Cancelled, Some(reason))?;
        order.error_message = None;
        self.persist_transition(order, "cancelled", command_id, Some("succeeded"))
            .await?;
        self.publish(DomainEvent::OrderCancelled(order.clone()), &order.cloid);
        tracing::info!(cloid = %order.cloid, reason, "order_cancelled");
        Ok(true)
    }

    async fn mark_cancel_unknown(
        &self,
        order: &mut Order,
        reason: &str,
        command_id: Option<uuid::Uuid>,
    ) {
        if order.status != OrderStatus::CancelUnknown {
            let _ = self
                .state_machine
                .transition(order, OrderStatus::CancelUnknown, Some(reason));
        }
        order.error_message = Some(reason.to_string());
        let _ = self
            .persist_transition(order, "cancel_unknown", command_id, Some("unknown"))
            .await;
    }

    /// Execute a cancel command claimed by the sole durable worker.
    pub async fn execute_durable_cancel_command(
        &self,
        command: &DurableExecutionCommand,
    ) -> Result<bool, HypeEdgeError> {
        let cloid = command
            .payload
            .get("cloid")
            .and_then(|c| c.as_str())
            .ok_or_else(|| HypeEdgeError::Execution {
                message: "cancel command missing cloid".into(),
            })?;
        let store = self
            .durable_store
            .as_ref()
            .ok_or_else(|| HypeEdgeError::Execution {
                message: "cancel command requires durable store".into(),
            })?;
        let mut order = store
            .get_order(cloid)
            .await?
            .ok_or_else(|| HypeEdgeError::Execution {
                message: format!("durable cancel order not found for {cloid}"),
            })?;
        self.store(&order).await;

        if order.is_terminal() {
            let status = if order.status == OrderStatus::Cancelled {
                "succeeded"
            } else {
                "failed"
            };
            self.persist_transition(
                &order,
                "cancel_recovered_terminal",
                Some(command.command_id),
                Some(status),
            )
            .await?;
            return Ok(true);
        }

        if command.requires_resolution {
            let hl = CloidGenerator::to_hl_cloid(&order.cloid);
            let resp = self
                .query_order_by_cloid(&hl)
                .await
                .map_err(|e| HypeEdgeError::Execution { message: e })?;
            match resp {
                Some(resp) => {
                    let status_data = resp.get("order").cloned().unwrap_or(Value::Null);
                    let exchange_status = status_data
                        .get("status")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string();
                    if matches!(exchange_status.as_str(), "open" | "resting" | "triggered") {
                        // Still live: proceed to send the cancel.
                    } else {
                        self.handle_cancel_response(&mut order, resp, Some(command.command_id))
                            .await?;
                        return Ok(order.status != OrderStatus::CancelUnknown);
                    }
                }
                None => {
                    self.mark_cancel_unknown(
                        &mut order,
                        "cancel_recovery_status_unknown",
                        Some(command.command_id),
                    )
                    .await;
                    return Ok(false);
                }
            }
        }

        // Send the cancel by cloid through the serial nonce queue.
        match self.submit_cancel_by_cloid(&order).await? {
            CancelSubmit::Response(value) => {
                self.handle_cancel_response(&mut order, value, Some(command.command_id))
                    .await
            }
            CancelSubmit::Failed(msg) => {
                self.mark_cancel_unknown(&mut order, &msg, Some(command.command_id))
                    .await;
                Ok(false)
            }
            CancelSubmit::Timeout => {
                self.mark_cancel_unknown(&mut order, "cancel_timeout", Some(command.command_id))
                    .await;
                Ok(false)
            }
        }
    }

    /// Sign + send `cancelByCloid` inside the serial worker. Resolves the asset
    /// index first (so an unknown symbol fails before signing).
    async fn submit_cancel_by_cloid(&self, order: &Order) -> Result<CancelSubmit, HypeEdgeError> {
        let asset = self.resolve_asset_index(&order.symbol, &order.cloid)?;
        let hl = CloidGenerator::to_hl_cloid(&order.cloid);
        let exchange = self.exchange.clone();
        let nonce = self.nonce.clone();
        let rl = self.rate_limiter.clone();
        let rate_limit_acquire_timeout = self.rate_limit_acquire_timeout;
        let timeout = tokio::time::timeout(
            self.submit_timeout,
            nonce.submit("cancel_order", move |nonce| {
                let exchange = exchange.clone();
                let hl = hl.clone();
                let rl = rl.clone();
                Box::pin(async move {
                    // M-EX1/P2-6: exchange actions count against IP weight.
                    acquire_exchange_weight(rl, 1, rate_limit_acquire_timeout).await?;
                    exchange
                        .cancel_by_cloid(vec![CancelByCloidWire { asset, cloid: hl }], nonce)
                        .await
                })
            }),
        )
        .await;
        Ok(match timeout {
            Ok(Ok(value)) => CancelSubmit::Response(value),
            Ok(Err(msg)) => CancelSubmit::Failed(msg),
            Err(_) => CancelSubmit::Timeout,
        })
    }

    fn resolve_asset_index(&self, symbol: &str, cloid: &str) -> Result<i64, HypeEdgeError> {
        let Some(index_provider) = &self.asset_index_provider else {
            return Err(HypeEdgeError::Execution {
                message: "no asset index provider".into(),
            });
        };
        index_provider.asset_index(symbol).ok_or_else(|| {
            HypeEdgeError::order_rejected(
                format!("unknown symbol {symbol}"),
                Some(cloid.to_string()),
                Some("instrument_meta_unavailable".to_string()),
            )
        })
    }

    // --- Persistence helpers ---

    async fn persist_placement(
        &self,
        order: &Order,
        risk_result: &RiskCheckResult,
        command_id: Option<uuid::Uuid>,
        dispatch: bool,
        reference_price: Option<Decimal>,
    ) -> Result<Option<RiskCheckResult>, HypeEdgeError> {
        let Some(store) = &self.durable_store else {
            return Ok(None);
        };
        Ok(Some(
            store
                .persist_placement(
                    order,
                    risk_result,
                    command_id.unwrap_or_else(uuid::Uuid::new_v4),
                    dispatch,
                    reference_price,
                )
                .await?,
        ))
    }

    async fn persist_transition(
        &self,
        order: &Order,
        event_type: &str,
        command_id: Option<uuid::Uuid>,
        command_status: Option<&str>,
    ) -> Result<(), HypeEdgeError> {
        let Some(store) = &self.durable_store else {
            return Ok(());
        };
        store
            .persist_transition(order, event_type, command_id, command_status)
            .await
    }

    /// Credit the address action quota with organic fill volume (B3,
    /// P1-8/H-EX5b). Every authoritative fill path (submit ack, status query,
    /// cancel-conflict) records exactly once.
    async fn record_fill(&self, volume_usdc: Decimal) {
        if let Some(budget) = &self.action_budget {
            budget
                .lock()
                .await
                .record_fill(volume_usdc, Some(Utc::now()));
        }
    }

    /// Handle a risk rejection: escalate to cancel-only on timeout, kill switch
    /// on hard risk failures. Shared with the in-worker escalation (M-EX3).
    async fn handle_risk_rejection(&self, reason: &str) {
        escalate_risk_rejection(
            self.safety.clone(),
            self.durable_kill_trigger.clone(),
            self.kill_switch.clone(),
            reason,
        )
        .await
    }

    fn rejected_order(&self, intent: &OrderIntent, reason: &str) -> Order {
        let cloid = intent.cloid.clone().unwrap_or_else(|| {
            CloidGenerator::deterministic(intent.strategy_id.as_deref(), &intent_key(intent))
        });
        let order = Order {
            cloid,
            symbol: intent.symbol.clone(),
            side: intent.side,
            size: intent.size,
            price: intent.price,
            order_type: intent.order_type,
            time_in_force: intent.time_in_force,
            status: OrderStatus::Rejected,
            strategy_id: intent.strategy_id.clone(),
            sub_account: intent.sub_account.clone(),
            reduce_only: intent.reduce_only,
            is_spot: intent.is_spot,
            risk_reducing: intent.risk_reducing,
            max_slippage_bps: intent.max_slippage_bps,
            error_message: Some(reason.to_string()),
            ..Order::new(
                "".into(),
                "".into(),
                Side::Buy,
                Size::ZERO,
                None,
                OrderType::Limit,
                TimeInForce::Gtc,
            )
        };
        self.publish(DomainEvent::OrderRejected(order.clone()), &order.cloid);
        order
    }

    fn matches_intent(order: &Order, intent: &OrderIntent) -> bool {
        order.symbol == intent.symbol
            && order.side == intent.side
            && order.size == intent.size
            && order.price == intent.price
            && order.order_type == intent.order_type
            && order.time_in_force == intent.time_in_force
            && order.strategy_id == intent.strategy_id
            && order.sub_account == intent.sub_account
            && order.reduce_only == intent.reduce_only
            && order.is_spot == intent.is_spot
            && order.risk_reducing == intent.risk_reducing
            && order.max_slippage_bps == intent.max_slippage_bps
    }

    /// Rebuild an `OrderIntent` from a persisted `Order` (durable re-dispatch).
    fn intent_from_order(order: &Order) -> OrderIntent {
        OrderIntent {
            symbol: order.symbol.clone(),
            side: order.side,
            size: order.size,
            price: order.price,
            order_type: order.order_type,
            time_in_force: order.time_in_force,
            strategy_id: order.strategy_id.clone(),
            sub_account: order.sub_account.clone(),
            reduce_only: order.reduce_only,
            cloid: Some(order.cloid.clone()),
            client_id: None,
            is_spot: order.is_spot,
            risk_reducing: order.risk_reducing,
            max_slippage_bps: order.max_slippage_bps,
        }
    }

    // --- Durable command execution ---

    /// Execute or resolve a command claimed by the sole durable worker.
    pub async fn execute_durable_command(
        &self,
        command: &DurableExecutionCommand,
        after_send_hook: Option<Box<AfterSendHook>>,
    ) -> Result<bool, HypeEdgeError> {
        let cloid = command
            .payload
            .get("cloid")
            .and_then(|c| c.as_str())
            .ok_or_else(|| HypeEdgeError::Execution {
                message: "durable command missing cloid".into(),
            })?;
        let store = self
            .durable_store
            .as_ref()
            .ok_or_else(|| HypeEdgeError::Execution {
                message: "durable command requires durable store".into(),
            })?;
        let mut order = store
            .get_order(cloid)
            .await?
            .ok_or_else(|| HypeEdgeError::Execution {
                message: format!("durable order not found for command {}", command.command_id),
            })?;
        self.store(&order).await;
        if order.is_terminal() {
            return Ok(true);
        }

        if command.requires_resolution {
            let hl = CloidGenerator::to_hl_cloid(cloid);
            let response = self
                .query_order_by_cloid(&hl)
                .await
                .map_err(|e| HypeEdgeError::Execution { message: e })?;
            let Some(response) = response else {
                if order.status != OrderStatus::SubmitUnknown {
                    self.state_machine.transition(
                        &mut order,
                        OrderStatus::SubmitUnknown,
                        Some("lease_recovery_unknown"),
                    )?;
                }
                order.error_message =
                    Some("exchange_order_not_found_after_ambiguous_submission".into());
                self.persist_transition(
                    &order,
                    "submit_unknown",
                    Some(command.command_id),
                    Some("unknown"),
                )
                .await?;
                return Ok(false);
            };
            self.handle_submit_response(&mut order, response, Some(command.command_id))
                .await?;
            return Ok(order.status != OrderStatus::SubmitUnknown);
        }

        let intent = Self::intent_from_order(&order);
        // Re-run the gates before dispatch.
        if let Err(e) = self.run_gates(&intent).await {
            self.state_machine.transition(
                &mut order,
                OrderStatus::Cancelled,
                Some("dispatch_aborted_by_safety_gate"),
            )?;
            order.error_message = Some(e.to_string());
            self.persist_transition(
                &order,
                "dispatch_aborted",
                Some(command.command_id),
                Some("cancelled"),
            )
            .await?;
            self.publish(DomainEvent::OrderCancelled(order.clone()), &order.cloid);
            return Ok(true);
        }
        // P1-6/A16: resolve the reference price *before* the risk re-check.
        // A replayed market order has no persisted limit price, so the risk
        // check must see the fresh mid — otherwise the reference-less checker
        // fails (`market_price_not_available`) and mis-kills the replay. Limit
        // replays use the fresh mid too; with a configured provider a missing/
        // stale snapshot fails closed instead of trusting the order's own price.
        let reference_price = self.resolve_replay_reference(&order).await?;
        if self.market_data_provider.is_some() && reference_price.is_none() {
            let reason = if order.price.is_some() {
                "market_price_stale"
            } else {
                "market_price_not_available"
            };
            self.state_machine.transition(
                &mut order,
                OrderStatus::Cancelled,
                Some("dispatch_aborted_by_reference_unavailable"),
            )?;
            order.error_message = Some(reason.to_string());
            self.persist_transition(
                &order,
                "dispatch_aborted",
                Some(command.command_id),
                Some("cancelled"),
            )
            .await?;
            self.publish(DomainEvent::OrderCancelled(order.clone()), &order.cloid);
            return Ok(true);
        }
        // B2: a durable command replayed after a crash/lease-loss is re-sent —
        // it must re-pass the risk check, not sail through on the state that
        // existed when it was originally admitted.
        if let Some(checker) = &self.risk_checker {
            let result = checker.check(&intent, reference_price).await;
            if !result.passed {
                let reason = result
                    .reason
                    .unwrap_or_else(|| "risk_check_rejected".into());
                self.state_machine.transition(
                    &mut order,
                    OrderStatus::Cancelled,
                    Some("dispatch_aborted_by_risk"),
                )?;
                order.error_message = Some(reason.clone());
                self.persist_transition(
                    &order,
                    "dispatch_aborted",
                    Some(command.command_id),
                    Some("cancelled"),
                )
                .await?;
                self.publish(DomainEvent::OrderCancelled(order.clone()), &order.cloid);
                return Ok(true);
            }
        }
        if self
            .rate_limiter
            .as_ref()
            .is_some_and(|rl| !rl.check_action_credits())
        {
            self.state_machine.transition(
                &mut order,
                OrderStatus::Cancelled,
                Some("dispatch_aborted_by_safety_gate"),
            )?;
            order.error_message = Some("action_credits_below_threshold".into());
            self.persist_transition(
                &order,
                "dispatch_aborted",
                Some(command.command_id),
                Some("cancelled"),
            )
            .await?;
            self.publish(DomainEvent::OrderCancelled(order.clone()), &order.cloid);
            return Ok(true);
        }

        let outcome = self
            .submit_to_exchange(&intent, cloid, reference_price, false)
            .await;
        match outcome {
            SubmitOutcome::Response(resp) => {
                if let Some(hook) = &after_send_hook {
                    hook(command);
                }
                self.handle_submit_response(&mut order, resp, Some(command.command_id))
                    .await?;
                Ok(order.status != OrderStatus::SubmitUnknown)
            }
            SubmitOutcome::SafetyAborted(e) => {
                self.state_machine.transition(
                    &mut order,
                    OrderStatus::Cancelled,
                    Some("dispatch_aborted_by_safety_gate"),
                )?;
                order.error_message = Some(e.to_string());
                self.persist_transition(
                    &order,
                    "dispatch_aborted",
                    Some(command.command_id),
                    Some("cancelled"),
                )
                .await?;
                self.publish(DomainEvent::OrderCancelled(order.clone()), &order.cloid);
                Ok(true)
            }
            SubmitOutcome::Timeout => {
                self.state_machine.transition(
                    &mut order,
                    OrderStatus::SubmitUnknown,
                    Some("submit_timeout"),
                )?;
                order.error_message = Some("exchange_action_outcome_unknown".into());
                self.persist_transition(
                    &order,
                    "submit_unknown",
                    Some(command.command_id),
                    Some("unknown"),
                )
                .await?;
                Ok(false)
            }
            SubmitOutcome::Rejected(msg) => {
                self.state_machine.transition(
                    &mut order,
                    OrderStatus::Rejected,
                    Some("exchange_err"),
                )?;
                order.error_message = Some(msg);
                self.persist_transition(
                    &order,
                    "rejected",
                    Some(command.command_id),
                    Some("failed"),
                )
                .await?;
                self.publish(DomainEvent::OrderRejected(order.clone()), &order.cloid);
                Ok(true)
            }
        }
    }

    /// Recover open orders from the durable store (startup).
    pub async fn recover_open_orders(&self) -> Result<usize, HypeEdgeError> {
        let Some(store) = &self.durable_store else {
            return Ok(0);
        };
        let orders = store.load_open_orders().await?;
        for order in &orders {
            self.store(order).await;
        }
        tracing::info!(count = orders.len(), "execution_orders_recovered");
        Ok(orders.len())
    }

    /// Import an exchange-authoritative order discovered by reconciliation.
    pub async fn import_exchange_order(&self, order: Order) {
        self.store(&order).await;
    }

    /// Durably import exchange truth before it can be cancelled locally (port
    /// of `import_exchange_order_authoritative`).
    pub async fn import_exchange_order_authoritative(
        &self,
        order: Order,
    ) -> Result<(), HypeEdgeError> {
        if let Some(store) = &self.durable_store {
            store.persist_reconciled_order(&order).await?;
        }
        self.store(&order).await;
        Ok(())
    }

    /// Refresh one committed exchange projection into process memory.
    pub async fn refresh_order_from_durable(
        &self,
        cloid: &str,
    ) -> Result<Option<Order>, HypeEdgeError> {
        match &self.durable_store {
            Some(store) => {
                let order = store.get_order(cloid).await?;
                if let Some(order) = &order {
                    self.store(order).await;
                }
                Ok(order)
            }
            None => Ok(self.load(cloid).await),
        }
    }

    /// Serialize a leverage update and re-check global safety before signing.
    /// Wrapped in the same 3s caller timeout as order submission (P1-1/C3): a
    /// hung leverage action must return an error, never block the queue.
    pub async fn update_leverage(
        &self,
        symbol: &str,
        leverage: u32,
        is_cross: bool,
    ) -> Result<serde_json::Value, HypeEdgeError> {
        if leverage == 0 {
            return Err(HypeEdgeError::Execution {
                message: "leverage must be positive".into(),
            });
        }
        let asset = self.resolve_asset_index(symbol, "")?;
        let exchange = self.exchange.clone();
        let ks = self.kill_switch.clone();
        let rl = self.rate_limiter.clone();
        let rate_limit_acquire_timeout = self.rate_limit_acquire_timeout;
        let result = match tokio::time::timeout(
            self.submit_timeout,
            self.nonce.submit("update_leverage", move |nonce| {
                let exchange = exchange.clone();
                let ks = ks.clone();
                let rl = rl.clone();
                Box::pin(async move {
                    // Preflight inside the serial worker (mirrors engine.py):
                    // queued leverage changes cannot survive a kill/safety move.
                    if let Err(e) = ks.check().await {
                        return Err(e.to_string());
                    }
                    // M-EX1/P2-6: exchange actions count against IP weight.
                    acquire_exchange_weight(rl, 1, rate_limit_acquire_timeout).await?;
                    exchange
                        .update_leverage(asset, is_cross, leverage as i64, nonce)
                        .await
                })
            }),
        )
        .await
        {
            Ok(Ok(value)) => value,
            Ok(Err(e)) => return Err(HypeEdgeError::Execution { message: e }),
            Err(_) => {
                return Err(HypeEdgeError::Execution {
                    message: format!(
                        "update_leverage timed out after {}ms",
                        self.submit_timeout.as_millis()
                    ),
                });
            }
        };
        Ok(result)
    }

    /// The number of open (non-terminal) orders tracked in memory.
    pub async fn open_order_count(&self) -> usize {
        self.orders
            .lock()
            .await
            .values()
            .filter(|o| !o.is_terminal())
            .count()
    }
}

#[async_trait]
impl ExecutionClient for ExecutionEngine {
    async fn submit_order(
        &self,
        intent: OrderIntent,
        deferred: Option<bool>,
    ) -> Result<Order, HypeEdgeError> {
        self.submit_order_impl(intent, deferred).await
    }

    async fn cancel_order(&self, cloid: &str) -> Result<bool, HypeEdgeError> {
        let Some(mut order) = self.load(cloid).await else {
            tracing::warn!(cloid, "cancel_order_not_found");
            return Ok(false);
        };
        if order.is_terminal() {
            tracing::warn!(
                cloid,
                status = order.status.as_str(),
                "cancel_order_already_terminal"
            );
            return Ok(false);
        }

        let command_id = uuid::Uuid::new_v4();
        if let Some(store) = &self.durable_store {
            store.persist_cancel_requested(&order, command_id).await?;
        }

        // Send the cancel by cloid through the serial nonce queue.
        match self.submit_cancel_by_cloid(&order).await? {
            CancelSubmit::Response(value) => {
                let accepted = self
                    .handle_cancel_response(&mut order, value, Some(command_id))
                    .await?;
                self.store(&order).await;
                Ok(accepted)
            }
            CancelSubmit::Failed(msg) => {
                self.mark_cancel_unknown(&mut order, &msg, Some(command_id))
                    .await;
                self.store(&order).await;
                tracing::warn!(cloid, error = %msg, "cancel_order_unknown");
                Ok(false)
            }
            CancelSubmit::Timeout => {
                // H-EX9: a timed-out cancel may still have landed on the
                // exchange — resolve the terminal state by cloid query before
                // degrading to CANCEL_UNKNOWN (mirrors the submit path). Only a
                // failed resolution keeps the order CANCEL_UNKNOWN.
                let hl = CloidGenerator::to_hl_cloid(&order.cloid);
                match self.query_order_by_cloid(&hl).await {
                    Ok(Some(resp)) => {
                        let accepted = self
                            .handle_cancel_response(&mut order, resp, Some(command_id))
                            .await?;
                        self.store(&order).await;
                        Ok(accepted)
                    }
                    _ => {
                        self.mark_cancel_unknown(&mut order, "cancel_timeout", Some(command_id))
                            .await;
                        self.store(&order).await;
                        tracing::warn!(cloid, "cancel_order_unknown_timeout");
                        Ok(false)
                    }
                }
            }
        }
    }

    /// Cancel every open order in one or more batched `cancelByCloid` actions
    /// (P2-6/H-EX6) — a single signed action packs up to
    /// [`CANCEL_ALL_BATCH_LIMIT`] target cloids (IP weight `1 + floor(N/40)`)
    /// instead of one action per order. Fault tolerance is per target: an
    /// unresolvable asset or a failing status entry never aborts the batch,
    /// and the count of unresolved targets is logged (reconciliation recovers).
    async fn cancel_all_orders(&self, symbol: Option<&str>) -> Result<u64, HypeEdgeError> {
        let open: Vec<Order> = self
            .orders
            .lock()
            .await
            .values()
            .filter(|o| !o.is_terminal())
            .filter(|o| symbol.is_none_or(|s| o.symbol == s))
            .cloned()
            .collect();
        if open.is_empty() {
            return Ok(0);
        }
        let mut cancelled = 0u64;
        let mut unresolved: Vec<String> = Vec::new();
        for chunk in open.chunks(CANCEL_ALL_BATCH_LIMIT) {
            // Per-target asset resolution: an unresolvable symbol faults only
            // its own target, never the whole batch.
            let mut wires = Vec::with_capacity(chunk.len());
            for order in chunk {
                match self.resolve_asset_index(&order.symbol, &order.cloid) {
                    Ok(asset) => wires.push(CancelByCloidWire {
                        asset,
                        cloid: CloidGenerator::to_hl_cloid(&order.cloid),
                    }),
                    Err(e) => {
                        tracing::warn!(cloid = %order.cloid, error = %e, "cancel_all_asset_unresolved");
                        unresolved.push(order.cloid.clone());
                    }
                }
            }
            if wires.is_empty() {
                continue;
            }
            let exchange = self.exchange.clone();
            let nonce = self.nonce.clone();
            let rl = self.rate_limiter.clone();
            let rate_limit_acquire_timeout = self.rate_limit_acquire_timeout;
            let batch_len = wires.len() as u64;
            let result = tokio::time::timeout(
                self.submit_timeout,
                nonce.submit("cancel_all_orders", move |nonce| {
                    let exchange = exchange.clone();
                    let rl = rl.clone();
                    Box::pin(async move {
                        // M-EX1/P2-6: exchange actions count against IP weight
                        // (1 + floor(N/40) for the packed batch).
                        acquire_exchange_weight(rl, batch_len, rate_limit_acquire_timeout).await?;
                        exchange.cancel_by_cloid(wires, nonce).await
                    })
                }),
            )
            .await;

            match result {
                Ok(Ok(value)) => {
                    // statuses[i] is the outcome for wires[i]; apply each
                    // target independently so one failure never aborts the rest.
                    let statuses = value
                        .pointer("/response/data/statuses")
                        .and_then(|s| s.as_array())
                        .cloned()
                        .unwrap_or_default();
                    for (i, order) in chunk.iter().enumerate() {
                        let entry = statuses.get(i).cloned().unwrap_or(Value::Null);
                        let per_target = serde_json::json!({
                            "status": "ok",
                            "response": {"data": {"statuses": [entry]}}
                        });
                        let mut order = order.clone();
                        match self
                            .handle_cancel_response(&mut order, per_target, None)
                            .await
                        {
                            Ok(true) => cancelled += 1,
                            Ok(false) => {
                                unresolved.push(order.cloid.clone());
                            }
                            Err(e) => {
                                tracing::warn!(cloid = %order.cloid, error = %e, "cancel_all_target_unresolved");
                                unresolved.push(order.cloid.clone());
                            }
                        }
                        self.store(&order).await;
                    }
                }
                Ok(Err(msg)) => {
                    tracing::error!(error = %msg, batch = batch_len, "cancel_all_transport_failed");
                    for order in chunk {
                        let mut order = order.clone();
                        self.mark_cancel_unknown(&mut order, "cancel_all_transport_failed", None)
                            .await;
                        self.store(&order).await;
                        unresolved.push(order.cloid.clone());
                    }
                }
                Err(_) => {
                    tracing::error!(batch = batch_len, "cancel_all_timeout");
                    for order in chunk {
                        let mut order = order.clone();
                        self.mark_cancel_unknown(&mut order, "cancel_all_timeout", None)
                            .await;
                        self.store(&order).await;
                        unresolved.push(order.cloid.clone());
                    }
                }
            }
        }
        tracing::warn!(
            cancelled,
            unresolved = unresolved.len(),
            "cancel_all_finished"
        );
        Ok(cancelled)
    }

    async fn get_order(&self, cloid: &str) -> Result<Option<Order>, HypeEdgeError> {
        Ok(self.load(cloid).await)
    }

    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>, HypeEdgeError> {
        Ok(self
            .orders
            .lock()
            .await
            .values()
            .filter(|o| !o.is_terminal())
            .filter(|o| symbol.is_none_or(|s| o.symbol == s))
            .cloned()
            .collect())
    }
}

#[async_trait]
impl DurableCommandDispatcher for ExecutionEngine {
    async fn execute_durable_cancel_command(
        &self,
        command: &DurableExecutionCommand,
    ) -> Result<bool, HypeEdgeError> {
        ExecutionEngine::execute_durable_cancel_command(self, command).await
    }

    async fn execute_durable_command(
        &self,
        command: &DurableExecutionCommand,
        after_send_hook: Option<Box<AfterSendHook>>,
    ) -> Result<bool, HypeEdgeError> {
        ExecutionEngine::execute_durable_command(self, command, after_send_hook).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::exchange::ExchangeClient;
    use crate::risk::action_budget::{
        ActionBudgetController, ActionBudgetSettings, BudgetAllocation, CancelHeadroomSnapshot,
        RemoteActionSnapshot,
    };
    use async_trait::async_trait;
    use hypeedge_domain::decimal::{Price, Size};
    use hypeedge_domain::models::{AccountState, MidPrice, Position};
    use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
    use uuid::Uuid;

    struct MockExchange {
        responses: Arc<Mutex<VecDeque<Value>>>,
        submitted: Arc<AtomicU32>,
        query_result: Option<Value>,
        order_error: bool,
        hang_order: bool,
        hang_cancel: bool,
        hang_leverage: bool,
        hang_query: bool,
        /// Number of cloids in the last `cancel_by_cloid` batch (H-EX6).
        last_cancel_batch_size: Arc<AtomicU32>,
        /// Price string of the last placed order wire (M-EX6 freshness check).
        last_order_price: Arc<std::sync::Mutex<Option<String>>>,
    }

    impl MockExchange {
        fn new(responses: Vec<Value>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses.into())),
                submitted: Arc::new(AtomicU32::new(0)),
                query_result: None,
                order_error: false,
                hang_order: false,
                hang_cancel: false,
                hang_leverage: false,
                hang_query: false,
                last_cancel_batch_size: Arc::new(AtomicU32::new(0)),
                last_order_price: Arc::new(std::sync::Mutex::new(None)),
            }
        }
        fn with_query(mut self, query: Value) -> Self {
            self.query_result = Some(query);
            self
        }
        fn with_order_error(mut self) -> Self {
            self.order_error = true;
            self
        }
        fn with_hang_order(mut self) -> Self {
            self.hang_order = true;
            self
        }
        fn with_hang_cancel(mut self) -> Self {
            self.hang_cancel = true;
            self
        }
        fn with_hang_leverage(mut self) -> Self {
            self.hang_leverage = true;
            self
        }
        fn with_hang_query(mut self) -> Self {
            self.hang_query = true;
            self
        }
        fn last_cancel_batch_size(&self) -> u32 {
            self.last_cancel_batch_size.load(AtomicOrdering::SeqCst)
        }
        fn last_price(&self) -> Option<String> {
            self.last_order_price.lock().unwrap().clone()
        }
    }

    use std::collections::VecDeque;

    #[async_trait]
    impl ExchangeClient for MockExchange {
        async fn order(&self, orders: Vec<OrderWire>, _nonce: u64) -> Result<Value, String> {
            self.submitted.fetch_add(1, AtomicOrdering::SeqCst);
            if let Some(wire) = orders.first() {
                *self.last_order_price.lock().unwrap() = Some(wire.p.clone());
            }
            if self.hang_order {
                // Simulates a dead socket: never completes on its own.
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
            if self.order_error {
                return Err("transport_error".into());
            }
            Ok(self
                .responses
                .lock()
                .await
                .pop_front()
                .unwrap_or(Value::Null))
        }
        async fn cancel(
            &self,
            _cancels: Vec<crate::execution::signing::CancelWire>,
            _nonce: u64,
        ) -> Result<Value, String> {
            self.submitted.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(Value::Null)
        }
        async fn cancel_by_cloid(
            &self,
            cancels: Vec<crate::execution::signing::CancelByCloidWire>,
            _nonce: u64,
        ) -> Result<Value, String> {
            self.submitted.fetch_add(1, AtomicOrdering::SeqCst);
            self.last_cancel_batch_size
                .store(cancels.len() as u32, AtomicOrdering::SeqCst);
            if self.hang_cancel {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
            Ok(self
                .responses
                .lock()
                .await
                .pop_front()
                .unwrap_or(Value::Null))
        }
        async fn update_leverage(
            &self,
            _asset: i64,
            _is_cross: bool,
            _leverage: i64,
            _nonce: u64,
        ) -> Result<Value, String> {
            if self.hang_leverage {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
            Ok(Value::Null)
        }
        async fn query_order_by_cloid(&self, _cloid: &str) -> Result<Option<Value>, String> {
            if self.hang_query {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
            Ok(self.query_result.clone())
        }
    }

    struct FakeAssetIndex;
    impl AssetIndexProvider for FakeAssetIndex {
        fn asset_index(&self, _symbol: &str) -> Option<i64> {
            Some(0)
        }
    }

    struct FakeMarketDataProvider {
        mid: Option<MidPrice>,
    }
    #[async_trait]
    impl MarketDataProvider for FakeMarketDataProvider {
        async fn get_price_snapshot(
            &self,
            _symbol: &str,
        ) -> Result<Option<MidPrice>, HypeEdgeError> {
            Ok(self.mid.clone())
        }
        async fn get_best_bid_ask(
            &self,
            _symbol: &str,
        ) -> Result<Option<(Decimal, Decimal)>, HypeEdgeError> {
            Ok(None)
        }
    }

    /// Provider that returns a scripted sequence of snapshots, one per call —
    /// lets tests stage an admission mid that differs from the in-worker mid.
    struct QueuedMarketDataProvider {
        snaps: std::sync::Mutex<VecDeque<Option<MidPrice>>>,
    }
    impl QueuedMarketDataProvider {
        fn new(snaps: Vec<Option<MidPrice>>) -> Self {
            Self {
                snaps: std::sync::Mutex::new(snaps.into()),
            }
        }
    }
    #[async_trait]
    impl MarketDataProvider for QueuedMarketDataProvider {
        async fn get_price_snapshot(
            &self,
            _symbol: &str,
        ) -> Result<Option<MidPrice>, HypeEdgeError> {
            Ok(self.snaps.lock().unwrap().pop_front().unwrap_or(None))
        }
        async fn get_best_bid_ask(
            &self,
            _symbol: &str,
        ) -> Result<Option<(Decimal, Decimal)>, HypeEdgeError> {
            Ok(None)
        }
    }

    /// In-memory durable store with a persist-failure switch (H-EX3).
    struct FakeDurableStore {
        orders: std::sync::Mutex<HashMap<String, Order>>,
        persist_placement_fails: bool,
    }
    impl FakeDurableStore {
        fn new() -> Self {
            Self {
                orders: std::sync::Mutex::new(HashMap::new()),
                persist_placement_fails: false,
            }
        }
        fn fail_persist_placement(&mut self) {
            self.persist_placement_fails = true;
        }
        fn has(&self, cloid: &str) -> bool {
            self.orders.lock().unwrap().contains_key(cloid)
        }
    }
    #[async_trait]
    impl DurableOrderStore for FakeDurableStore {
        async fn persist_placement(
            &self,
            order: &Order,
            risk_result: &RiskCheckResult,
            _command_id: Uuid,
            _dispatch: bool,
            _reference_price: Option<Decimal>,
        ) -> Result<RiskCheckResult, HypeEdgeError> {
            if self.persist_placement_fails {
                return Err(HypeEdgeError::Postgres {
                    message: "persist_placement boom".into(),
                });
            }
            self.orders
                .lock()
                .unwrap()
                .insert(order.cloid.clone(), order.clone());
            Ok(risk_result.clone())
        }
        async fn persist_transition(
            &self,
            order: &Order,
            _event_type: &str,
            _command_id: Option<Uuid>,
            _command_status: Option<&str>,
        ) -> Result<(), HypeEdgeError> {
            self.orders
                .lock()
                .unwrap()
                .insert(order.cloid.clone(), order.clone());
            Ok(())
        }
        async fn persist_cancel_requested(
            &self,
            _order: &Order,
            _command_id: Uuid,
        ) -> Result<(), HypeEdgeError> {
            Ok(())
        }
        async fn persist_reconciled_order(&self, order: &Order) -> Result<(), HypeEdgeError> {
            self.orders
                .lock()
                .unwrap()
                .insert(order.cloid.clone(), order.clone());
            Ok(())
        }
        async fn load_open_orders(&self) -> Result<Vec<Order>, HypeEdgeError> {
            Ok(self
                .orders
                .lock()
                .unwrap()
                .values()
                .filter(|o| !o.is_terminal())
                .cloned()
                .collect())
        }
        async fn get_order(&self, cloid: &str) -> Result<Option<Order>, HypeEdgeError> {
            Ok(self.orders.lock().unwrap().get(cloid).cloned())
        }
    }

    /// An account facade with nothing available — the risk checker must reject.
    struct NoAccountView;
    impl crate::risk::checker::AccountView for NoAccountView {
        fn get_account_state(&self) -> Option<AccountState> {
            None
        }
        fn get_position(&self, _symbol: &str) -> Option<Position> {
            None
        }
        fn last_update_ts(&self) -> Option<DateTime<Utc>> {
            None
        }
    }

    /// An account facade that is healthy on the first read (so the pre-queue
    /// risk check passes) and unavailable on later reads (so the in-worker
    /// re-check rejects) — deterministic way to exercise the B2 worker path.
    struct FlippingAccount {
        reads: std::sync::atomic::AtomicU32,
    }
    impl crate::risk::checker::AccountView for FlippingAccount {
        fn get_account_state(&self) -> Option<AccountState> {
            let n = self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 0 {
                let eq = hypeedge_domain::Usd::new(Decimal::from_str_strict("1000000").unwrap());
                Some(AccountState {
                    equity: eq,
                    available_balance: eq,
                    total_margin_used: hypeedge_domain::Usd::ZERO,
                    total_unrealized_pnl: hypeedge_domain::Usd::ZERO,
                    peak_equity: eq,
                    sub_account: None,
                })
            } else {
                None
            }
        }
        fn get_position(&self, _symbol: &str) -> Option<Position> {
            None
        }
        fn last_update_ts(&self) -> Option<DateTime<Utc>> {
            Some(Utc::now())
        }
    }

    fn rate_limiter() -> RateLimiter {
        let rl = RateLimiter::new(1200, 1000);
        rl.update_action_credits(5000);
        rl
    }

    fn base_engine(exchange: Arc<dyn ExchangeClient>) -> ExecutionEngine {
        base_engine_with_provider(exchange, None)
    }

    fn limit_intent() -> OrderIntent {
        OrderIntent {
            symbol: "BTC".into(),
            side: Side::Buy,
            size: Size::new(Decimal::from_str_strict("1.0").unwrap()),
            price: Some(Price::new(Decimal::from_str_strict("50000").unwrap())),
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            strategy_id: Some("test".into()),
            sub_account: None,
            reduce_only: false,
            cloid: None,
            client_id: None,
            is_spot: false,
            risk_reducing: false,
            max_slippage_bps: 50,
        }
    }

    fn market_intent() -> OrderIntent {
        OrderIntent {
            symbol: "BTC".into(),
            side: Side::Buy,
            size: Size::new(Decimal::from_str_strict("1.0").unwrap()),
            price: None,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::Ioc,
            strategy_id: Some("test".into()),
            sub_account: None,
            reduce_only: false,
            cloid: None,
            client_id: None,
            is_spot: false,
            risk_reducing: false,
            max_slippage_bps: 50,
        }
    }

    fn base_engine_with_provider(
        exchange: Arc<dyn ExchangeClient>,
        provider: Option<Arc<dyn MarketDataProvider>>,
    ) -> ExecutionEngine {
        let bus = Arc::new(EventBus::new(256));
        let ks = Arc::new(KillSwitch::new(bus.clone(), true));
        ExecutionEngine::new(ExecutionEngineConfig {
            nonce: Arc::new(NonceQueue::new()),
            event_bus: bus,
            kill_switch: ks,
            exchange,
            account_address: "0xabc".into(),
            safety: None,
            risk_checker: None,
            rate_limiter: Some(Arc::new(rate_limiter())),
            durable_store: None,
            market_data_provider: provider,
            order_normalizer: None,
            asset_index_provider: Some(Arc::new(FakeAssetIndex)),
            deferred_execution: false,
            market_price_stale_seconds: 5.0,
            durable_kill_trigger: None,
            action_budget: None,
        })
    }

    fn engine_with_store_and_provider(
        exchange: Arc<dyn ExchangeClient>,
        store: Arc<dyn DurableOrderStore>,
        provider: Option<Arc<dyn MarketDataProvider>>,
    ) -> ExecutionEngine {
        let bus = Arc::new(EventBus::new(256));
        ExecutionEngine::new(ExecutionEngineConfig {
            nonce: Arc::new(NonceQueue::new()),
            event_bus: bus,
            kill_switch: Arc::new(KillSwitch::new(Arc::new(EventBus::new(256)), true)),
            exchange,
            account_address: "0xabc".into(),
            safety: None,
            risk_checker: None,
            rate_limiter: Some(Arc::new(rate_limiter())),
            durable_store: Some(store),
            market_data_provider: provider,
            order_normalizer: None,
            asset_index_provider: Some(Arc::new(FakeAssetIndex)),
            deferred_execution: false,
            market_price_stale_seconds: 5.0,
            durable_kill_trigger: None,
            action_budget: None,
        })
    }

    fn sell_limit_intent() -> OrderIntent {
        let mut intent = limit_intent();
        intent.side = Side::Sell;
        intent
    }

    /// A marketable limit sell: price well below the (stale) market mid.
    fn sell_limit_intent_marketable() -> OrderIntent {
        let mut intent = sell_limit_intent();
        intent.price = Some(Price::new(Decimal::from_str_strict("40000").unwrap()));
        intent
    }

    fn fresh_mid(price: &str) -> MidPrice {
        MidPrice {
            symbol: "BTC".into(),
            price: Decimal::from_str_strict(price).unwrap(),
            timestamp: Utc::now().timestamp_millis(),
        }
    }

    fn stale_mid(price: &str) -> MidPrice {
        let mut mid = fresh_mid(price);
        mid.timestamp = Utc::now().timestamp_millis() - 60_000; // 60s old, stale limit 5s
        mid
    }

    /// An account facade healthy on the first read and deep in drawdown on
    /// later reads — drives the in-worker risk failure → kill switch (M-EX3).
    struct DrawdownFlippingAccount {
        reads: AtomicU32,
    }
    impl crate::risk::checker::AccountView for DrawdownFlippingAccount {
        fn get_account_state(&self) -> Option<AccountState> {
            let n = self.reads.fetch_add(1, AtomicOrdering::SeqCst);
            let peak = hypeedge_domain::Usd::new(Decimal::from_str_strict("1000000").unwrap());
            let equity = if n == 0 {
                peak
            } else {
                // 90% drawdown from peak — breaches the 10% max_drawdown_pct.
                hypeedge_domain::Usd::new(Decimal::from_str_strict("100000").unwrap())
            };
            Some(AccountState {
                equity,
                available_balance: equity,
                total_margin_used: hypeedge_domain::Usd::ZERO,
                total_unrealized_pnl: hypeedge_domain::Usd::ZERO,
                peak_equity: peak,
                sub_account: None,
            })
        }
        fn get_position(&self, _symbol: &str) -> Option<Position> {
            None
        }
        fn last_update_ts(&self) -> Option<DateTime<Utc>> {
            Some(Utc::now())
        }
    }

    #[tokio::test]
    async fn submit_acknowledges_resting_order() {
        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 123}}]}}}),
        ]));
        let engine = base_engine(exchange);
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(order.status, OrderStatus::Acknowledged);
        assert_eq!(order.exchange_oid.as_deref(), Some("123"));
    }

    #[tokio::test]
    async fn submit_immediate_fill() {
        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [{"filled": {"oid": 9, "totalSz": "1.0", "avgPx": "49950", "fee": "0.5"}}]}}}),
        ]));
        let engine = base_engine(exchange);
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.avg_fill_price.unwrap().to_string(), "49950");
    }

    #[tokio::test]
    async fn submit_exchange_error_rejects() {
        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [{"error": "Invalid price"}]}}}),
        ]));
        let engine = base_engine(exchange);
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(order.status, OrderStatus::Rejected);
        assert!(
            order
                .error_message
                .as_deref()
                .unwrap()
                .contains("Invalid price")
        );
    }

    #[tokio::test]
    async fn market_order_with_fresh_mid_succeeds() {
        // A1 regression: a market order with a fresh provider mid must build
        // the aggressive IoC wire and reach the exchange (previously rejected
        // as `market_price_not_available`).
        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 7}}]}}}),
        ]));
        let provider = Some(Arc::new(FakeMarketDataProvider {
            mid: Some(MidPrice {
                symbol: "BTC".into(),
                price: Decimal::from_str_strict("50000").unwrap(),
                timestamp: chrono::Utc::now().timestamp_millis(),
            }),
        }) as Arc<dyn MarketDataProvider>);
        let engine = base_engine_with_provider(exchange, provider);
        let order = engine.submit_order(market_intent(), None).await.unwrap();
        assert_eq!(order.status, OrderStatus::Acknowledged);
    }

    #[tokio::test]
    async fn market_order_without_provider_rejected_fail_closed() {
        // A1 fail-closed: no provider -> market order rejected, never sent.
        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![]));
        let engine = base_engine(exchange); // provider: None
        let order = engine.submit_order(market_intent(), None).await.unwrap();
        assert_eq!(order.status, OrderStatus::Rejected);
        assert_eq!(
            order.error_message.as_deref(),
            Some("market_price_not_available")
        );
    }

    #[tokio::test]
    async fn market_order_with_stale_mid_rejected_fail_closed() {
        // A1 fail-closed: a stale mid (older than market_price_stale_seconds)
        // must not be used for a market order.
        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![]));
        let stale_ts = chrono::Utc::now().timestamp_millis() - 60_000; // 60s old, stale limit 5s
        let provider = Some(Arc::new(FakeMarketDataProvider {
            mid: Some(MidPrice {
                symbol: "BTC".into(),
                price: Decimal::from_str_strict("50000").unwrap(),
                timestamp: stale_ts,
            }),
        }) as Arc<dyn MarketDataProvider>);
        let engine = base_engine_with_provider(exchange, provider);
        let order = engine.submit_order(market_intent(), None).await.unwrap();
        assert_eq!(order.status, OrderStatus::Rejected);
        assert_eq!(order.error_message.as_deref(), Some("market_price_stale"));
    }

    #[tokio::test]
    async fn kill_switch_propagates_on_non_deferred() {
        let bus = Arc::new(EventBus::new(256));
        let ks = Arc::new(KillSwitch::new(bus.clone(), true));
        let engine = ExecutionEngine::new(ExecutionEngineConfig {
            nonce: Arc::new(NonceQueue::new()),
            event_bus: bus,
            kill_switch: ks.clone(),
            exchange: Arc::new(MockExchange::new(vec![])),
            account_address: "0xabc".into(),
            safety: None,
            risk_checker: None,
            rate_limiter: Some(Arc::new(rate_limiter())),
            durable_store: None,
            market_data_provider: None,
            order_normalizer: None,
            asset_index_provider: Some(Arc::new(FakeAssetIndex)),
            deferred_execution: false,
            market_price_stale_seconds: 5.0,
            durable_kill_trigger: None,
            action_budget: None,
        });
        ks.trigger("test").await;
        let err = engine.submit_order(limit_intent(), None).await.unwrap_err();
        assert!(matches!(err, HypeEdgeError::KillSwitchTriggered { .. }));
    }

    #[tokio::test]
    async fn risk_rejection_returns_rejected_order() {
        let tracker = Arc::new(NoAccountView);
        let checker = Arc::new(RiskChecker::new(tracker, Default::default()));
        let engine = ExecutionEngine::new(ExecutionEngineConfig {
            nonce: Arc::new(NonceQueue::new()),
            event_bus: Arc::new(EventBus::new(256)),
            kill_switch: Arc::new(KillSwitch::new(Arc::new(EventBus::new(256)), true)),
            exchange: Arc::new(MockExchange::new(vec![])),
            account_address: "0xabc".into(),
            safety: None,
            risk_checker: Some(checker),
            rate_limiter: Some(Arc::new(rate_limiter())),
            durable_store: None,
            market_data_provider: None,
            order_normalizer: None,
            asset_index_provider: Some(Arc::new(FakeAssetIndex)),
            deferred_execution: false,
            market_price_stale_seconds: 5.0,
            durable_kill_trigger: None,
            action_budget: None,
        });
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(order.status, OrderStatus::Rejected);
        assert_eq!(
            order.error_message.as_deref(),
            Some("account_state_not_available")
        );
    }

    #[tokio::test]
    async fn risk_failure_inside_worker_aborts_placement() {
        // B2 regression: the risk check is re-run inside the serial worker
        // immediately before signing. A rejection there (account state became
        // unavailable between the pre-queue check and the send) must abort the
        // placement via the safety-abort path, never reach the exchange.
        let checker = Arc::new(RiskChecker::new(
            Arc::new(FlippingAccount {
                reads: std::sync::atomic::AtomicU32::new(0),
            }),
            Default::default(),
        ));
        let engine = ExecutionEngine::new(ExecutionEngineConfig {
            nonce: Arc::new(NonceQueue::new()),
            event_bus: Arc::new(EventBus::new(256)),
            kill_switch: Arc::new(KillSwitch::new(Arc::new(EventBus::new(256)), true)),
            exchange: Arc::new(MockExchange::new(vec![])),
            account_address: "0xabc".into(),
            safety: None,
            risk_checker: Some(checker),
            rate_limiter: Some(Arc::new(rate_limiter())),
            durable_store: None,
            market_data_provider: None,
            order_normalizer: None,
            asset_index_provider: Some(Arc::new(FakeAssetIndex)),
            deferred_execution: false,
            market_price_stale_seconds: 5.0,
            durable_kill_trigger: None,
            action_budget: None,
        });
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        // Outer risk check passes (fresh read #1); the in-worker re-check (read
        // #2) sees no account -> SafetyAborted -> order recorded as Cancelled.
        assert_eq!(
            order.status,
            OrderStatus::Cancelled,
            "in-worker risk failure must abort the placement (B2)"
        );
    }

    #[tokio::test]
    async fn action_budget_consulted_debit_and_credit_fills() {
        // B3 regression: when an ActionBudgetController is configured, the
        // engine passes permission() before sending, debits the ledger after a
        // successful send, and credits organic fill volume.
        use crate::risk::action_budget::{
            ActionBudgetController, ActionBudgetSettings, BudgetAllocation, CancelHeadroomSnapshot,
            RemoteActionSnapshot,
        };
        let owner = "0x1111111111111111111111111111111111111111";
        let budget = Arc::new(Mutex::new(
            ActionBudgetController::new(owner, ActionBudgetSettings::default()).unwrap(),
        ));
        // Reconcile so the controller leaves forced CancelOnly.
        budget
            .lock()
            .await
            .reconcile_remote(RemoteActionSnapshot {
                quota_owner_address: owner.into(),
                cap: 10_000,
                used: 0,
                observed_at: Utc::now(),
            })
            .expect("reconcile");
        budget
            .lock()
            .await
            .reconcile_cancel_headroom(CancelHeadroomSnapshot {
                cap: 10_000,
                used: 0,
                observed_at: Utc::now(),
            });
        // `limit_intent()` carries a strategy_id; permission requires an
        // allocation for (strategy, symbol).
        budget.lock().await.set_allocation(BudgetAllocation {
            strategy_id: "test".into(),
            symbol: "BTC".into(),
            soft_limit: 100,
            hard_limit: 100,
        });

        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [{"filled": {"oid": 1, "totalSz": "1.0", "avgPx": "100", "fee": "0"}}]}}}),
        ]));
        let engine = ExecutionEngine::new(ExecutionEngineConfig {
            nonce: Arc::new(NonceQueue::new()),
            event_bus: Arc::new(EventBus::new(256)),
            kill_switch: Arc::new(KillSwitch::new(Arc::new(EventBus::new(256)), true)),
            exchange,
            account_address: "0xabc".into(),
            safety: None,
            risk_checker: None,
            rate_limiter: Some(Arc::new(rate_limiter())),
            durable_store: None,
            market_data_provider: None,
            order_normalizer: None,
            asset_index_provider: Some(Arc::new(FakeAssetIndex)),
            deferred_execution: false,
            market_price_stale_seconds: 5.0,
            durable_kill_trigger: None,
            action_budget: Some(budget.clone()),
        });

        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(order.status, OrderStatus::Filled);

        let state = budget.lock().await.export_recovery_state();
        assert_eq!(
            state.attempts_after_snapshot.len(),
            1,
            "placement must debit the action ledger (B3)"
        );
        assert_eq!(
            state.fills.len(),
            1,
            "immediate fill must credit quota (B3)"
        );
        assert_eq!(state.fills[0].volume_usdc.to_string(), "100");
    }

    #[tokio::test]
    async fn idempotent_replay_returns_original() {
        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 1}}]}}}),
        ]));
        let engine = base_engine(exchange);
        let mut intent = limit_intent();
        intent.cloid = Some("mm_1_1".into());
        let first = engine.submit_order(intent.clone(), None).await.unwrap();
        let second = engine.submit_order(intent, None).await.unwrap();
        assert_eq!(first.status, second.status);
        assert_eq!(first.cloid, second.cloid);
    }

    #[tokio::test]
    async fn cancel_success_moves_to_cancelled() {
        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 1}}]}}}),
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": ["success"]}}}),
        ]));
        let engine = base_engine(exchange);
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(order.status, OrderStatus::Acknowledged);
        let ok = engine.cancel_order(&order.cloid).await.unwrap();
        assert!(ok);
        let after = engine.get_order(&order.cloid).await.unwrap().unwrap();
        assert_eq!(after.status, OrderStatus::Cancelled);
    }

    #[tokio::test]
    async fn cancel_rejected_by_exchange_returns_false_and_keeps_order_open() {
        // Python parity: an "err" cancel response returns False and the order
        // stays ACKNOWLEDGED (only a status lookup proving "open" degrades to
        // CANCEL_UNKNOWN for reconciliation).
        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 1}}]}}}),
            serde_json::json!({"status": "err", "response": "oops"}),
        ]));
        let engine = base_engine(exchange);
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        let ok = engine.cancel_order(&order.cloid).await.unwrap();
        assert!(!ok);
        let after = engine.get_order(&order.cloid).await.unwrap().unwrap();
        assert_eq!(after.status, OrderStatus::Acknowledged);
    }

    #[tokio::test]
    async fn cancel_timeout_lookup_open_degrades_to_cancel_unknown() {
        // A post-timeout status lookup reporting the order still open means the
        // original cancel may still land — retain CANCEL_UNKNOWN.
        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 1}}]}}}),
            serde_json::json!({"status": "order", "order": {"status": "open"}}),
        ]));
        let engine = base_engine(exchange);
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        let ok = engine.cancel_order(&order.cloid).await.unwrap();
        assert!(!ok);
        let after = engine.get_order(&order.cloid).await.unwrap().unwrap();
        assert_eq!(after.status, OrderStatus::CancelUnknown);
    }

    #[tokio::test]
    async fn transport_error_resolved_by_cloid_query_marks_filled() {
        // A transport failure on `/exchange` is not proof of non-execution: the
        // engine resolves the outcome by cloid query. The query reporting
        // "filled" must move the order to FILLED (no blind resend).
        let exchange: Arc<dyn ExchangeClient> = Arc::new(
            MockExchange::new(vec![])
                .with_order_error()
                .with_query(serde_json::json!({
                    "status": "order",
                    "order": {"status": "filled", "totalSz": "1.0", "avgPx": "49950"}
                })),
        );
        let engine = base_engine(exchange);
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(
            order.status,
            OrderStatus::Filled,
            "cloid resolution must apply the authoritative outcome"
        );
        assert!(order.filled_at.is_some());
        assert_eq!(order.filled_size.to_string(), "1");
        assert_eq!(order.avg_fill_price.unwrap().to_string(), "49950");
    }

    #[tokio::test]
    async fn transport_error_with_unknown_cloid_degrades_to_submit_unknown() {
        // If the cloid query finds nothing, the outcome is genuinely unknown —
        // SUBMIT_UNKNOWN for reconciliation, never a fabricated ack.
        let exchange: Arc<dyn ExchangeClient> = Arc::new(
            MockExchange::new(vec![]).with_order_error(), // no query result
        );
        let engine = base_engine(exchange);
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(order.status, OrderStatus::SubmitUnknown);
    }

    // --- P1-1/C3: zero-timeout signature path ---

    #[tokio::test]
    async fn hanging_exchange_does_not_block_subsequent_actions() {
        // C3 regression: a hung `/exchange` POST must not permanently block
        // trading. Every caller behind the hung action hits its own submit
        // timeout (→ SUBMIT_UNKNOWN, resolved by cloid query) and the nonce
        // worker's per-action ceiling drains the queue — no blind resend, no
        // permanent stall.
        let mock = Arc::new(
            MockExchange::new(vec![serde_json::json!({
                "status": "ok",
                "response": {"data": {"statuses": [{"resting": {"oid": 1}}]}}
            })])
            .with_hang_order(),
        );
        let engine = fast_engine(mock.clone());
        let mut i1 = limit_intent();
        i1.cloid = Some("hang_1".into());
        let mut i2 = limit_intent();
        i2.cloid = Some("hang_2".into());
        let e1 = engine.clone();
        let e2 = engine.clone();
        let f1 = tokio::spawn(async move { e1.submit_order(i1, None).await });
        let f2 = tokio::spawn(async move { e2.submit_order(i2, None).await });

        // Both callers time out (the first send hangs; the second is queued
        // behind it); the cloid query finds nothing.
        let first = f1.await.unwrap().unwrap();
        let second = f2.await.unwrap().unwrap();
        assert_eq!(
            first.status,
            OrderStatus::SubmitUnknown,
            "hung send must degrade to SUBMIT_UNKNOWN, never block"
        );
        assert_eq!(
            second.status,
            OrderStatus::SubmitUnknown,
            "a queued caller must time out too, never hang forever"
        );
        assert_eq!(mock.submitted.load(AtomicOrdering::SeqCst), 1);

        // The worker's 100ms action ceiling then drains the hung action and
        // the queued order #2 is processed (its reply is dropped — the caller
        // already degraded to UNKNOWN — but the queue keeps draining).
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            mock.submitted.load(AtomicOrdering::SeqCst),
            2,
            "worker must continue processing after a hung action"
        );
    }

    #[tokio::test]
    async fn update_leverage_timeout_returns_error_and_queue_recovers() {
        // C3: update_leverage must have the same caller timeout as submit — a
        // hung leverage action returns an error instead of blocking forever.
        let mock = Arc::new(
            MockExchange::new(vec![serde_json::json!({
                "status": "ok",
                "response": {"data": {"statuses": [{"resting": {"oid": 2}}]}}
            })])
            .with_hang_leverage(),
        );
        let engine = fast_engine(mock.clone());
        let f = tokio::spawn({
            let e = engine.clone();
            async move { e.update_leverage("BTC", 5, true).await }
        });
        let err = f.await.unwrap().unwrap_err();
        assert!(
            err.to_string().contains("timed out"),
            "leverage timeout must surface as an error: {err}"
        );

        // The queue must recover: once the worker's per-action ceiling drains
        // the hung action, a follow-up order goes through cleanly.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(
            order.status,
            OrderStatus::Acknowledged,
            "serial queue must recover after a hung leverage action"
        );
    }

    #[tokio::test]
    async fn cloid_query_timeout_degrades_to_submit_unknown() {
        // P1-1: the post-timeout cloid resolution itself is bounded — a hung
        // `/info` lookup must not stall the caller beyond the query timeout.
        let exchange: Arc<dyn ExchangeClient> = Arc::new(
            MockExchange::new(vec![])
                .with_order_error()
                .with_hang_query(),
        );
        let engine = fast_engine(exchange);
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(
            order.status,
            OrderStatus::SubmitUnknown,
            "a hung cloid query must degrade to SUBMIT_UNKNOWN after its timeout"
        );
    }

    // --- P1-6/A16: reference-price fail-closed + durable replay order ---

    #[tokio::test]
    async fn stale_snapshot_rejects_marketable_limit_sell_fail_closed() {
        // P1-6 regression: a limit order must not fall back to its own price
        // when the reference snapshot is stale — the order would control the
        // number the risk checks run against. Fail closed like market orders.
        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![]));
        let provider = Some(Arc::new(FakeMarketDataProvider {
            mid: Some(stale_mid("50000")),
        }) as Arc<dyn MarketDataProvider>);
        let engine = base_engine_with_provider(exchange, provider);
        let order = engine
            .submit_order(sell_limit_intent_marketable(), None)
            .await
            .unwrap();
        assert_eq!(order.status, OrderStatus::Rejected);
        assert_eq!(order.error_message.as_deref(), Some("market_price_stale"));
    }

    #[tokio::test]
    async fn missing_snapshot_rejects_limit_order_fail_closed() {
        // P1-6: a configured provider that returns no snapshot fails closed for
        // limit orders too (previously it silently trusted the intent price).
        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![]));
        let provider =
            Some(Arc::new(FakeMarketDataProvider { mid: None }) as Arc<dyn MarketDataProvider>);
        let engine = base_engine_with_provider(exchange, provider);
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(order.status, OrderStatus::Rejected);
        assert_eq!(
            order.error_message.as_deref(),
            Some("market_price_not_available")
        );
    }

    #[tokio::test]
    async fn durable_replay_market_order_with_fresh_mid_not_killed() {
        // P1-6: the durable replay must resolve the reference price *before*
        // the risk re-check — a replayed market order with a fresh mid must not
        // be mis-killed by a reference-less risk check.
        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 42}}]}}}),
        ]));
        let provider = Some(Arc::new(FakeMarketDataProvider {
            mid: Some(fresh_mid("50000")),
        }) as Arc<dyn MarketDataProvider>);
        let store = Arc::new(FakeDurableStore::new());
        let engine = engine_with_store_and_provider(exchange, store.clone(), provider);

        // A market order persisted as SUBMITTED (crash before dispatch).
        let mut order = Order::new(
            "mm_replay".into(),
            "BTC".into(),
            Side::Buy,
            Size::new(Decimal::from_str_strict("1.0").unwrap()),
            None,
            OrderType::Market,
            TimeInForce::Ioc,
        );
        order.status = OrderStatus::Submitted;
        store
            .orders
            .lock()
            .unwrap()
            .insert(order.cloid.clone(), order.clone());
        let command = DurableExecutionCommand {
            command_id: uuid::Uuid::new_v4(),
            command_type: "place_order".into(),
            payload: serde_json::json!({"cloid": "mm_replay"}),
            attempt_count: 1,
            requires_resolution: false,
        };
        let resolved = engine
            .execute_durable_command(&command, None)
            .await
            .unwrap();
        assert!(resolved);
        // The authoritative projection lives in the durable store (the
        // in-memory map is refreshed by reconciliation / refresh_order_from_durable).
        let after = store.get_order("mm_replay").await.unwrap().unwrap();
        assert_eq!(
            after.status,
            OrderStatus::Acknowledged,
            "replay market order with a fresh mid must not be mis-killed"
        );
    }

    // --- P1-7/H-EX9: cancel timeout resolves by cloid query ---

    #[tokio::test]
    async fn cancel_timeout_resolved_to_cancelled_by_cloid_query() {
        // H-EX9 regression: a timed-out cancel is resolved by cloid query —
        // the exchange reporting the order cancelled must move it to CANCELLED,
        // not leave it stuck CANCEL_UNKNOWN forever.
        let mock = Arc::new(
            MockExchange::new(vec![serde_json::json!({
                "status": "ok",
                "response": {"data": {"statuses": [{"resting": {"oid": 1}}]}}
            })])
            .with_hang_cancel()
            .with_query(serde_json::json!({
                "status": "order",
                "order": {"status": "cancelled"}
            })),
        );
        let engine = fast_engine(mock.clone());
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(order.status, OrderStatus::Acknowledged);
        let ok = engine.cancel_order(&order.cloid).await.unwrap();
        assert!(ok, "cancel resolved to cancelled must report success");
        let after = engine.get_order(&order.cloid).await.unwrap().unwrap();
        assert_eq!(after.status, OrderStatus::Cancelled);
    }

    #[tokio::test]
    async fn cancel_timeout_with_unresolved_query_stays_cancel_unknown() {
        // H-EX9: only a failed resolution keeps CANCEL_UNKNOWN (recovery path).
        let mock = Arc::new(
            MockExchange::new(vec![serde_json::json!({
                "status": "ok",
                "response": {"data": {"statuses": [{"resting": {"oid": 1}}]}}
            })])
            .with_hang_cancel()
            .with_hang_query(),
        );
        let engine = fast_engine(mock.clone());
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        let ok = engine.cancel_order(&order.cloid).await.unwrap();
        assert!(!ok);
        let after = engine.get_order(&order.cloid).await.unwrap().unwrap();
        assert_eq!(after.status, OrderStatus::CancelUnknown);
    }

    // --- P1-8/H-EX2 + H-EX5b: resting+filled merge, record_fill everywhere ---

    /// Engine with shrunk wall-clock timeouts and a fast nonce action ceiling
    /// so the C3 timeout tests stay fast with real time.
    fn fast_engine(exchange: Arc<dyn ExchangeClient>) -> ExecutionEngine {
        let mut engine = base_engine(exchange);
        engine.nonce = Arc::new(NonceQueue::with_action_timeout(
            64,
            Duration::from_millis(100),
        ));
        engine.submit_timeout = Duration::from_millis(50);
        engine.cloid_query_timeout = Duration::from_millis(50);
        engine.rate_limit_acquire_timeout = Duration::from_millis(50);
        engine
    }

    async fn budget_engine(
        exchange: Arc<dyn ExchangeClient>,
    ) -> (ExecutionEngine, Arc<Mutex<ActionBudgetController>>) {
        let owner = "0x1111111111111111111111111111111111111111";
        let budget = Arc::new(Mutex::new(
            ActionBudgetController::new(owner, ActionBudgetSettings::default()).unwrap(),
        ));
        // Reconcile so the controller leaves forced CancelOnly (B3 setup).
        budget
            .lock()
            .await
            .reconcile_remote(RemoteActionSnapshot {
                quota_owner_address: owner.into(),
                cap: 10_000,
                used: 0,
                observed_at: Utc::now(),
            })
            .expect("reconcile");
        budget
            .lock()
            .await
            .reconcile_cancel_headroom(CancelHeadroomSnapshot {
                cap: 10_000,
                used: 0,
                observed_at: Utc::now(),
            });
        // `limit_intent()` carries strategy_id "test"; permission requires an
        // allocation for (strategy, symbol).
        budget.lock().await.set_allocation(BudgetAllocation {
            strategy_id: "test".into(),
            symbol: "BTC".into(),
            soft_limit: 100,
            hard_limit: 100,
        });
        let engine = ExecutionEngine::new(ExecutionEngineConfig {
            nonce: Arc::new(NonceQueue::new()),
            event_bus: Arc::new(EventBus::new(256)),
            kill_switch: Arc::new(KillSwitch::new(Arc::new(EventBus::new(256)), true)),
            exchange,
            account_address: "0xabc".into(),
            safety: None,
            risk_checker: None,
            rate_limiter: Some(Arc::new(rate_limiter())),
            durable_store: None,
            market_data_provider: None,
            order_normalizer: None,
            asset_index_provider: Some(Arc::new(FakeAssetIndex)),
            deferred_execution: false,
            market_price_stale_seconds: 5.0,
            durable_kill_trigger: None,
            action_budget: Some(budget.clone()),
        });
        (engine, budget)
    }

    #[tokio::test]
    async fn resting_with_partial_fill_merges_fill_and_credits_quota() {
        // H-EX2 regression: a resting response carrying a same-entry `filled`
        // must merge the fill (size/price, PARTIAL_FILL event, quota credit)
        // instead of dropping it.
        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [
                {"resting": {"oid": 5}, "filled": {"oid": 5, "totalSz": "0.4", "avgPx": "49900"}}
            ]}}}),
        ]));
        let (engine, budget) = budget_engine(exchange).await;
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(
            order.status,
            OrderStatus::PartialFill,
            "resting + partial fill must transition to PARTIAL_FILL"
        );
        assert_eq!(order.filled_size.to_string(), "0.4");
        assert_eq!(order.avg_fill_price.unwrap().to_string(), "49900");
        let state = budget.lock().await.export_recovery_state();
        assert_eq!(
            state.fills.len(),
            1,
            "resting partial fill must credit the action quota"
        );
        assert_eq!(state.fills[0].volume_usdc.to_string(), "19960");
    }

    #[tokio::test]
    async fn resting_with_full_fill_transitions_to_filled() {
        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [
                {"resting": {"oid": 6}, "filled": {"oid": 6, "totalSz": "1.0", "avgPx": "49800"}}
            ]}}}),
        ]));
        let (engine, budget) = budget_engine(exchange).await;
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.filled_size.to_string(), "1");
        assert_eq!(budget.lock().await.export_recovery_state().fills.len(), 1);
    }

    #[tokio::test]
    async fn status_query_fill_records_budget_credit() {
        // H-EX5b: the status-query fill path must credit the action quota like
        // the immediate-fill path (it previously did not).
        let exchange: Arc<dyn ExchangeClient> = Arc::new(
            MockExchange::new(vec![])
                .with_order_error()
                .with_query(serde_json::json!({
                    "status": "order",
                    "order": {"status": "filled", "totalSz": "1.0", "avgPx": "100"}
                })),
        );
        let (engine, budget) = budget_engine(exchange).await;
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(order.status, OrderStatus::Filled);
        let state = budget.lock().await.export_recovery_state();
        assert_eq!(
            state.fills.len(),
            1,
            "status-query fill must credit quota (H-EX5b)"
        );
        assert_eq!(state.fills[0].volume_usdc.to_string(), "100");
    }

    // --- P2-5/H-EX3: persist-before-memory, no ghost orders ---

    #[tokio::test]
    async fn persist_failure_leaves_no_memory_ghost() {
        let mut store = FakeDurableStore::new();
        store.fail_persist_placement();
        let store = Arc::new(store);
        let engine = engine_with_store_and_provider(
            Arc::new(MockExchange::new(vec![])),
            store.clone(),
            None,
        );
        let mut intent = limit_intent();
        intent.cloid = Some("pf_1".into());
        let err = engine.submit_order(intent, None).await.unwrap_err();
        assert!(
            err.to_string().contains("persist_placement boom"),
            "persist failure must surface: {err}"
        );
        assert!(
            engine.get_order("pf_1").await.unwrap().is_none(),
            "no memory ghost after a persist failure (H-EX3)"
        );
        assert!(!store.has("pf_1"), "no durable row after a persist failure");
    }

    #[tokio::test]
    async fn ghost_submitted_order_replay_refused() {
        // H-EX3: an in-memory SUBMITTED order with no durable row is a ghost —
        // the idempotent replay must error rather than return the phantom.
        let store = Arc::new(FakeDurableStore::new());
        let engine =
            engine_with_store_and_provider(Arc::new(MockExchange::new(vec![])), store, None);
        let mut intent = limit_intent();
        intent.cloid = Some("ghost_1".into());
        // The engine keys memory by the HL-transformed cloid.
        let hl_cloid = CloidGenerator::to_hl_cloid("ghost_1");
        let mut ghost = Order::new(
            hl_cloid.clone(),
            "BTC".into(),
            Side::Buy,
            intent.size,
            intent.price,
            OrderType::Limit,
            TimeInForce::Gtc,
        );
        ghost.status = OrderStatus::Submitted;
        engine.store(&ghost).await;
        let err = engine.submit_order(intent, None).await.unwrap_err();
        assert!(
            err.to_string().contains("ghost"),
            "replay must refuse the ghost order: {err}"
        );
    }

    // --- P2-6/H-EX6: cancel_all batch + per-target tolerance ---

    #[tokio::test]
    async fn cancel_all_batches_one_action_and_cancels_all() {
        let mock = Arc::new(MockExchange::new(vec![
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 1}}]}}}),
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 2}}]}}}),
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 3}}]}}}),
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": ["success", "success", "success"]}}}),
        ]));
        let engine = base_engine(mock.clone());
        let mut cloids = Vec::new();
        for i in 0..3 {
            let mut intent = limit_intent();
            intent.cloid = Some(format!("ca_{i}"));
            let order = engine.submit_order(intent, None).await.unwrap();
            cloids.push(order.cloid.clone());
        }
        let cancelled = engine.cancel_all_orders(None).await.unwrap();
        assert_eq!(cancelled, 3);
        assert_eq!(
            mock.last_cancel_batch_size(),
            3,
            "cancel_all must pack all target cloids into one signed action (H-EX6)"
        );
        for c in &cloids {
            let order = engine.get_order(c).await.unwrap().unwrap();
            assert_eq!(order.status, OrderStatus::Cancelled);
        }
    }

    #[tokio::test]
    async fn cancel_all_per_target_fault_tolerance() {
        // One failing target in the batch must not abort the rest.
        let mock = Arc::new(MockExchange::new(vec![
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 1}}]}}}),
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 2}}]}}}),
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 3}}]}}}),
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": ["success", {"error": "oops"}, "success"]}}}),
        ]));
        let engine = base_engine(mock.clone());
        let mut cloids = Vec::new();
        for i in 0..3 {
            let mut intent = limit_intent();
            intent.cloid = Some(format!("ft_{i}"));
            let order = engine.submit_order(intent, None).await.unwrap();
            cloids.push(order.cloid.clone());
        }
        let cancelled = engine.cancel_all_orders(None).await.unwrap();
        assert_eq!(cancelled, 2, "one failing target must not abort the batch");
        // The error entry is per-position in the batch (HashMap iteration order
        // is unspecified), so assert the aggregate: exactly two targets end up
        // CANCELLED and the failing one stays open for reconciliation.
        let mut cancelled_count = 0;
        let mut open_count = 0;
        for c in &cloids {
            match engine.get_order(c).await.unwrap().unwrap().status {
                OrderStatus::Cancelled => cancelled_count += 1,
                OrderStatus::Acknowledged => open_count += 1,
                other => panic!("unexpected post-batch status: {other:?}"),
            }
        }
        assert_eq!(cancelled_count, 2, "two targets must be cancelled");
        assert_eq!(
            open_count, 1,
            "the failing target stays open for reconciliation"
        );
    }

    // --- M-EX3: worker-side risk escalation ---

    #[tokio::test]
    async fn worker_risk_failure_escalates_to_kill_switch() {
        // M-EX3: the in-worker risk re-check failure must escalate exactly like
        // the admission path — a hard failure (drawdown breach) trips the kill
        // switch instead of being silently swallowed.
        let checker = Arc::new(RiskChecker::new(
            Arc::new(DrawdownFlippingAccount {
                reads: AtomicU32::new(0),
            }),
            Default::default(),
        ));
        let bus = Arc::new(EventBus::new(256));
        let ks = Arc::new(KillSwitch::new(bus.clone(), true));
        let engine = ExecutionEngine::new(ExecutionEngineConfig {
            nonce: Arc::new(NonceQueue::new()),
            event_bus: bus,
            kill_switch: ks.clone(),
            exchange: Arc::new(MockExchange::new(vec![])),
            account_address: "0xabc".into(),
            safety: None,
            risk_checker: Some(checker),
            rate_limiter: Some(Arc::new(rate_limiter())),
            durable_store: None,
            market_data_provider: None,
            order_normalizer: None,
            asset_index_provider: Some(Arc::new(FakeAssetIndex)),
            deferred_execution: false,
            market_price_stale_seconds: 5.0,
            durable_kill_trigger: None,
            action_budget: None,
        });
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(
            order.status,
            OrderStatus::Cancelled,
            "in-worker risk failure must abort the placement"
        );
        assert!(
            ks.is_active().await,
            "hard risk failure inside the worker must trip the kill switch (M-EX3)"
        );
    }

    #[tokio::test]
    async fn risk_timeout_reason_enters_cancel_only() {
        // M-EX3 escalation unit: a risk-check timeout puts the system
        // cancel-only (the kill switch stays untouched).
        let safety = Arc::new(Mutex::new(SafetyController::new(
            hypeedge_domain::enums::SafetyMode::Normal,
        )));
        let ks = Arc::new(KillSwitch::new(Arc::new(EventBus::new(256)), true));
        escalate_risk_rejection(Some(safety.clone()), None, ks, "risk_check_timeout").await;
        assert_eq!(
            safety.lock().await.mode(),
            hypeedge_domain::enums::SafetyMode::CancelOnly
        );
    }

    // --- Budget `Close + emergency` channel (risk-agent completed side) ---

    #[tokio::test]
    async fn emergency_close_bypasses_budget_mode_gate() {
        // The budget's `Close + emergency` channel bypasses the CancelOnly mode
        // gate while ordinary placements stay blocked — still gated on address
        // + IP margin. `submit_emergency_close` must feed the real flag.
        let owner = "0x1111111111111111111111111111111111111111";
        let budget = Arc::new(Mutex::new(
            ActionBudgetController::new(owner, ActionBudgetSettings::default()).unwrap(),
        ));
        // used=9994 → address_remaining 6 ≤ required_cancel(10) + close
        // reserve(5) → CancelOnly, but ≥ 1 so the emergency channel can pass.
        budget
            .lock()
            .await
            .reconcile_remote(RemoteActionSnapshot {
                quota_owner_address: owner.into(),
                cap: 10_000,
                used: 9_994,
                observed_at: Utc::now(),
            })
            .expect("reconcile");
        budget
            .lock()
            .await
            .reconcile_cancel_headroom(CancelHeadroomSnapshot {
                cap: 10_000,
                used: 0,
                observed_at: Utc::now(),
            });
        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![
            serde_json::json!({"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 9}}]}}}),
        ]));
        let engine = ExecutionEngine::new(ExecutionEngineConfig {
            nonce: Arc::new(NonceQueue::new()),
            event_bus: Arc::new(EventBus::new(256)),
            kill_switch: Arc::new(KillSwitch::new(Arc::new(EventBus::new(256)), true)),
            exchange,
            account_address: "0xabc".into(),
            safety: None,
            risk_checker: None,
            rate_limiter: Some(Arc::new(rate_limiter())),
            durable_store: None,
            market_data_provider: None,
            order_normalizer: None,
            asset_index_provider: Some(Arc::new(FakeAssetIndex)),
            deferred_execution: false,
            market_price_stale_seconds: 5.0,
            durable_kill_trigger: None,
            action_budget: Some(budget.clone()),
        });

        // Ordinary placement: blocked by the CancelOnly mode gate.
        let blocked = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(blocked.status, OrderStatus::Cancelled);
        assert!(
            blocked
                .error_message
                .as_deref()
                .unwrap_or_default()
                .contains("action_budget"),
            "ordinary placement must be blocked under CancelOnly: {:?}",
            blocked.error_message
        );

        // Emergency close: allowed through the Close + emergency channel.
        let mut close_intent = limit_intent();
        close_intent.reduce_only = true;
        close_intent.risk_reducing = true;
        let order = engine.submit_emergency_close(close_intent).await.unwrap();
        assert_eq!(
            order.status,
            OrderStatus::Acknowledged,
            "emergency close must pass the CancelOnly budget gate"
        );

        let state = budget.lock().await.export_recovery_state();
        assert_eq!(state.attempts_after_snapshot.len(), 1);
        assert_eq!(
            state.attempts_after_snapshot[0].child_actions,
            vec![crate::risk::BudgetAction::Close],
            "emergency close must debit the Close action class"
        );
    }

    // --- M-EX4: intent_key must distinguish spot and perp ---

    #[test]
    fn intent_key_distinguishes_spot_and_perp() {
        let perp = limit_intent();
        let mut spot = limit_intent();
        spot.is_spot = true;
        assert_ne!(
            intent_key(&perp),
            intent_key(&spot),
            "perp and spot intents for the same symbol must not share a cloid key"
        );
        // Same market still collides (idempotency preserved).
        assert_eq!(intent_key(&perp), intent_key(&perp));
    }

    // --- M-EX6: in-worker market reference refresh ---

    #[tokio::test]
    async fn market_order_uses_fresh_in_worker_mid() {
        // M-EX6: the aggressive IoC price must be built from the mid re-fetched
        // inside the worker (51000), not the admission-time mid (50000).
        let mock = Arc::new(MockExchange::new(vec![serde_json::json!({
            "status": "ok",
            "response": {"data": {"statuses": [{"resting": {"oid": 7}}]}}
        })]));
        let provider = Some(Arc::new(QueuedMarketDataProvider::new(vec![
            Some(fresh_mid("50000")), // admission
            Some(fresh_mid("51000")), // in-worker refresh
        ])) as Arc<dyn MarketDataProvider>);
        let engine = base_engine_with_provider(mock.clone(), provider);
        let order = engine.submit_order(market_intent(), None).await.unwrap();
        assert_eq!(order.status, OrderStatus::Acknowledged);
        // 51000 × (1 + 50bps) = 51255.
        assert_eq!(mock.last_price().as_deref(), Some("51255"));
    }

    #[tokio::test]
    async fn market_order_with_stale_in_worker_mid_aborts() {
        // M-EX6 fail-closed: if the in-worker refresh finds the mid stale, the
        // placement is aborted rather than priced from old data.
        let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange::new(vec![]));
        let provider = Some(Arc::new(QueuedMarketDataProvider::new(vec![
            Some(fresh_mid("50000")), // admission
            Some(stale_mid("51000")), // in-worker refresh: 60s old
        ])) as Arc<dyn MarketDataProvider>);
        let engine = base_engine_with_provider(exchange, provider);
        let order = engine.submit_order(market_intent(), None).await.unwrap();
        assert_eq!(
            order.status,
            OrderStatus::Cancelled,
            "stale in-worker mid must abort the market placement (M-EX6)"
        );
    }
}
