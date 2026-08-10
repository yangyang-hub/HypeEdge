//! Event-driven runtime boundary for the pure market-maker policy, port of
//! `src/hypeedge/strategy/market_maker/runtime.py`.
//!
//! Coalesces market-data events (latest book/trade), reliable lifecycle facts,
//! and markout samples into a serialized `_cycle` that builds features, quotes,
//! coordinates, and — only when `RUNNING` and unfenced — submits a durable
//! quote plan. In `SHADOW` mode it drives a virtual [`ShadowOrderState`] and
//! never writes live trading facts.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use hypeedge_domain::decimal::{Decimal, Size, Usd};
use hypeedge_domain::enums::{MarketMakerLifecycle, QuoteDecision};
use hypeedge_domain::events::{DomainEvent, Event, EventType};
use hypeedge_domain::models::L2BookSnapshot;
use hypeedge_infra::event_bus::{BoundedMailbox, EventBus};
use tokio::sync::mpsc;

use super::estimators::{AdverseMarkoutEstimator, DecisionLatencyEstimator};
use super::models::{ActionBudgetSnapshot, InventorySnapshot, MarketFeatures, MarketMakerConfig};
use super::policy::MarketMakerPolicy;
use super::shadow::{ShadowActionEstimate, ShadowOrderState};
use crate::market_data::features::MarketFeatureEngine;
use crate::strategy::registry::{StrategyConfigSnapshot, StrategyRuntimeHandle};
use crate::trading::quote_coordinator::QuoteCoordinator;
use crate::trading::quotes::{DesiredQuoteSet, QuotePlan, QuoteSlotView};

type Mailbox = Arc<BoundedMailbox<Arc<Event>>>;

/// Provider protocols (the Rust analogs of the Python `Protocol`s).
pub trait InventorySnapshotProvider: Send + Sync {
    fn get_inventory(&self, sub_account: &str, symbol: &str) -> InventorySnapshot;
}

pub trait ActionBudgetSnapshotProvider: Send + Sync {
    fn get_action_budget(&self, strategy_id: &str, symbol: &str) -> ActionBudgetSnapshot;
}

pub trait AccountHealthProvider: Send + Sync {
    fn allows_risk_increase(&self) -> bool;
}

pub trait QuoteSlotProvider: Send + Sync {
    fn get_quote_slots(
        &self,
        strategy_id: &str,
        symbol: &str,
    ) -> Result<(QuoteSlotView, QuoteSlotView), String>;
}

pub trait FundingSnapshotProvider: Send + Sync {
    fn get_funding(&self, symbol: &str) -> Option<(f64, i64)>;
}

/// A cancel request for the durable command client.
#[derive(Debug, Clone)]
pub struct QuoteCancelRequest {
    pub strategy_id: String,
    pub session_id: String,
    pub symbol: String,
    pub config_version: Option<u64>,
    pub revision: i64,
    pub reason: String,
    pub requested_at: DateTime<Utc>,
}

/// Only durable quote-set commands cross the live execution boundary.
#[async_trait]
pub trait QuotePlanCommandClient: Send + Sync {
    async fn submit_quote_plan(&self, plan: &QuotePlan) -> Result<(), String>;
    async fn cancel_strategy_quotes(&self, request: &QuoteCancelRequest) -> Result<(), String>;
}

/// A runtime snapshot for monitoring.
#[derive(Debug, Clone)]
pub struct MarketMakerRuntimeSnapshot {
    pub strategy_id: String,
    pub session_id: String,
    pub symbol: String,
    pub mode: MarketMakerLifecycle,
    pub config_version: Option<u64>,
    pub quote_revision: i64,
    pub market_version: Option<i64>,
    pub connection_generation: Option<i64>,
    pub last_cycle_at: Option<DateTime<Utc>>,
    pub last_reason: Option<String>,
    pub desired: Option<DesiredQuoteSet>,
    pub plan: Option<QuotePlan>,
    pub features: Option<MarketFeatures>,
}

impl MarketMakerRuntimeSnapshot {
    /// Build the `fair_value` frame the market-making WS sends (port of the
    /// Python `market_making_ws.py` latest-value frame).
    pub fn fair_value_frame(&self, sequence: u64) -> serde_json::Value {
        let desired = self.desired.as_ref();
        let features = self.features.as_ref();
        let external_reference = features.filter(|f| f.external_source.is_some()).map(|f| {
            serde_json::json!({
                "source": f.external_source,
                "symbol": f.external_symbol,
                "raw_price": f.external_raw_price.map(|p| p.to_string()),
                "adjusted_price": f.external_adjusted_price.map(|p| p.to_string()),
                "basis_bps": f.external_basis_bps.to_string(),
                "effective_weight": f.external_effective_weight.to_string(),
                "confidence": f.external_confidence.to_string(),
                "age_ms": f.external_age_ms,
                "quality": f.external_quality,
                "observed_at": f.external_observed_at.map(|t| t.to_rfc3339()),
            })
        });
        serde_json::json!({
            "schema_version": 1,
            "sequence": sequence,
            "type": "fair_value",
            "strategy_id": self.strategy_id,
            "runtime_revision": self.quote_revision,
            "market_revision": self.market_version,
            "observed_at": chrono::Utc::now().to_rfc3339(),
            "fair_price": desired.map(|d| d.fair_price.to_string()),
            "reservation_price": desired.map(|d| d.reservation_price.to_string()),
            "best_bid": features.map(|f| f.best_bid.to_string()),
            "best_ask": features.map(|f| f.best_ask.to_string()),
            "external_reference": external_reference,
        })
    }
}

const RELIABLE_EVENTS: &[EventType] = &[
    EventType::OrderFilled,
    EventType::OrderPartialFill,
    EventType::PositionChanged,
    EventType::AccountStateUpdate,
    EventType::ActionCreditsLow,
    EventType::ReconciliationComplete,
    EventType::WsConnected,
    EventType::WsDisconnected,
];

/// The market-maker runtime.
pub struct MarketMakerRuntime {
    strategy_id: String,
    session_id: String,
    sub_account: String,
    symbol: String,
    bus: Arc<EventBus>,
    feature_engine: Arc<tokio::sync::Mutex<MarketFeatureEngine>>,
    policy: MarketMakerPolicy,
    coordinator: QuoteCoordinator,
    inventory: Arc<dyn InventorySnapshotProvider>,
    budget: Arc<dyn ActionBudgetSnapshotProvider>,
    account_health: Arc<dyn AccountHealthProvider>,
    slots: Arc<dyn QuoteSlotProvider>,
    commands: Arc<dyn QuotePlanCommandClient>,
    funding: Option<Arc<dyn FundingSnapshotProvider>>,
    latency_estimator: tokio::sync::Mutex<DecisionLatencyEstimator>,
    markout_estimator: tokio::sync::Mutex<AdverseMarkoutEstimator>,
    market_stale_after: Duration,

    mode: tokio::sync::Mutex<MarketMakerLifecycle>,
    config: tokio::sync::Mutex<Option<MarketMakerConfig>>,
    book: tokio::sync::Mutex<Option<L2BookSnapshot>>,
    revision: tokio::sync::Mutex<i64>,
    last_cycle_at: tokio::sync::Mutex<Option<DateTime<Utc>>>,
    last_reason: tokio::sync::Mutex<String>,
    last_desired: tokio::sync::Mutex<Option<DesiredQuoteSet>>,
    last_plan: tokio::sync::Mutex<Option<QuotePlan>>,
    last_features: tokio::sync::Mutex<Option<MarketFeatures>>,
    shadow: tokio::sync::Mutex<ShadowOrderState>,
    running: tokio::sync::Mutex<bool>,
}

#[allow(clippy::too_many_arguments)]
impl MarketMakerRuntime {
    pub fn new(
        strategy_id: String,
        session_id: String,
        sub_account: String,
        symbol: String,
        bus: Arc<EventBus>,
        feature_engine: Arc<tokio::sync::Mutex<MarketFeatureEngine>>,
        policy: MarketMakerPolicy,
        coordinator: QuoteCoordinator,
        inventory: Arc<dyn InventorySnapshotProvider>,
        budget: Arc<dyn ActionBudgetSnapshotProvider>,
        account_health: Arc<dyn AccountHealthProvider>,
        slots: Arc<dyn QuoteSlotProvider>,
        commands: Arc<dyn QuotePlanCommandClient>,
        funding: Option<Arc<dyn FundingSnapshotProvider>>,
        market_stale_after: Duration,
    ) -> Result<Self, String> {
        if session_id.is_empty() {
            return Err("session_id is required".into());
        }
        if market_stale_after <= Duration::zero() {
            return Err("market_stale_after must be positive".into());
        }
        Ok(Self {
            strategy_id,
            session_id,
            sub_account,
            symbol,
            bus,
            feature_engine,
            policy,
            coordinator,
            inventory,
            budget,
            account_health,
            slots,
            commands,
            funding,
            latency_estimator: tokio::sync::Mutex::new(
                DecisionLatencyEstimator::new(
                    hypeedge_domain::decimal::Decimal::from_str_lenient("0.2").unwrap(),
                    hypeedge_domain::decimal::Decimal::from_str_lenient("0.1").unwrap(),
                    5,
                )
                .unwrap(),
            ),
            markout_estimator: tokio::sync::Mutex::new(
                AdverseMarkoutEstimator::new(
                    20,
                    500,
                    hypeedge_domain::decimal::Decimal::from_str_lenient("1").unwrap(),
                )
                .unwrap(),
            ),
            market_stale_after,
            mode: tokio::sync::Mutex::new(MarketMakerLifecycle::Warming),
            config: tokio::sync::Mutex::new(None),
            book: tokio::sync::Mutex::new(None),
            revision: tokio::sync::Mutex::new(0),
            last_cycle_at: tokio::sync::Mutex::new(None),
            last_reason: tokio::sync::Mutex::new("not_started".into()),
            last_desired: tokio::sync::Mutex::new(None),
            last_plan: tokio::sync::Mutex::new(None),
            last_features: tokio::sync::Mutex::new(None),
            shadow: tokio::sync::Mutex::new(ShadowOrderState::new()),
            running: tokio::sync::Mutex::new(false),
        })
    }

    async fn cancel_live_quotes(&self, reason: &str) -> Result<(), String> {
        let mut revision = self.revision.lock().await;
        *revision += 1;
        let config_version = self.config.lock().await.as_ref().map(|c| c.version);
        let request = QuoteCancelRequest {
            strategy_id: self.strategy_id.clone(),
            session_id: self.session_id.clone(),
            symbol: self.symbol.clone(),
            config_version,
            revision: *revision,
            reason: reason.to_string(),
            requested_at: Utc::now(),
        };
        self.commands.cancel_strategy_quotes(&request).await?;
        *self.last_reason.lock().await = reason.to_string();
        Ok(())
    }

    fn book_is_healthy(&self, book: &L2BookSnapshot, now: DateTime<Utc>) -> bool {
        book.bids
            .first()
            .zip(book.asks.first())
            .map(|(bid, ask)| {
                bid.price.inner() < ask.price.inner()
                    && now - book.local_ts <= self.market_stale_after
                    && now >= book.local_ts
            })
            .unwrap_or(false)
    }

    fn matches_order(&self, order: &hypeedge_domain::models::Order) -> bool {
        order.symbol == self.symbol
            && (order.strategy_id.is_none()
                || order.strategy_id.as_deref() == Some(self.strategy_id.as_str()))
            && (order.sub_account.is_none()
                || order.sub_account.as_deref() == Some(self.sub_account.as_str()))
    }

    fn matches_position(&self, position: &hypeedge_domain::models::Position) -> bool {
        position.symbol == self.symbol
            && (position.strategy_id.is_none()
                || position.strategy_id.as_deref() == Some(self.strategy_id.as_str()))
            && (position.sub_account.is_none()
                || position.sub_account.as_deref() == Some(self.sub_account.as_str()))
    }

    /// The coalesced cycle: build features, quote, coordinate, act.
    async fn cycle(&self, reason: &str) -> Result<(), String> {
        let config = self.config.lock().await.clone();
        let book = self.book.lock().await.clone();
        let (Some(config), Some(book)) = (config, book) else {
            *self.last_reason.lock().await = "waiting_for_config_or_book".to_string();
            return Ok(());
        };
        let now = Utc::now();
        let book_healthy =
            self.book_is_healthy(&book, now) && self.account_health.allows_risk_increase();

        let receipt_latency = (now - book.local_ts).num_milliseconds().max(0) as f64 / 1000.0;
        self.latency_estimator.lock().await.observe(
            hypeedge_domain::decimal::Decimal::from_f64(receipt_latency).unwrap_or_default(),
        );
        let latency = {
            let est = self.latency_estimator.lock().await;
            (est.seconds(), est.quality())
        };
        let markout = {
            let est = self.markout_estimator.lock().await;
            est.estimate(
                &self.strategy_id,
                &self.symbol,
                Some(config.min_markout_samples as usize),
                Some(config.conservative_markout_bps),
            )
            .unwrap_or(crate::market_maker::estimators::MarkoutEstimate {
                adverse_bps: config.conservative_markout_bps,
                quality: "conservative_default".into(),
                sample_count: 0,
            })
        };
        let funding_rate = self
            .funding
            .as_ref()
            .and_then(|f| f.get_funding(&self.symbol))
            .map(|(rate, _)| hypeedge_domain::decimal::Decimal::from_f64(rate).unwrap_or_default())
            .unwrap_or_default();

        let features = {
            let mut engine = self.feature_engine.lock().await;
            match engine.build(
                &book,
                book_healthy,
                funding_rate,
                markout.adverse_bps,
                Some(latency.0),
                &latency.1,
                &markout.quality,
                None,
                Some(&config),
                now,
            ) {
                Ok(f) => f,
                Err(_) => {
                    self.cancel_live_quotes("invalid_market_snapshot").await?;
                    return Ok(());
                }
            }
        };
        let inventory = self
            .inventory
            .get_inventory(&self.sub_account, &self.symbol);
        let budget = self
            .budget
            .get_action_budget(&self.strategy_id, &self.symbol);
        let (bid_view, ask_view) = self.views().await?;
        let mut revision = self.revision.lock().await;
        *revision += 1;
        let desired = self.policy.quote(
            &self.strategy_id,
            &self.session_id,
            *revision,
            bid_view.revision.max(ask_view.revision),
            &features,
            &inventory,
            &budget,
            &config,
        )?;
        let mode = *self.mode.lock().await;
        let desired = if !matches!(
            mode,
            MarketMakerLifecycle::Running | MarketMakerLifecycle::Shadow
        ) {
            as_no_quote(desired, format!("lifecycle_{}", mode.as_str()))
        } else {
            desired
        };
        let plan =
            self.coordinator
                .coordinate(&desired, &bid_view, &ask_view, config.tick_size, now)?;

        let _shadow_actions: Option<ShadowActionEstimate> = None;
        if mode == MarketMakerLifecycle::Shadow {
            let _shadow_actions = Some(self.shadow.lock().await.apply(&plan, now)?);
        } else if mode == MarketMakerLifecycle::Running
            && !plan.fenced
            && plan
                .diffs
                .iter()
                .any(|d| d.estimated_incremental_actions() > 0)
        {
            self.commands.submit_quote_plan(&plan).await?;
        }

        *self.last_cycle_at.lock().await = Some(now);
        *self.last_reason.lock().await = if plan.fenced {
            plan.fence_reason
                .clone()
                .unwrap_or_else(|| reason.to_string())
        } else {
            reason.to_string()
        };
        *self.last_desired.lock().await = Some(desired);
        *self.last_plan.lock().await = Some(plan);
        *self.last_features.lock().await = Some(features);
        Ok(())
    }

    async fn views(&self) -> Result<(QuoteSlotView, QuoteSlotView), String> {
        if *self.mode.lock().await == MarketMakerLifecycle::Shadow {
            let mut shadow = self.shadow.lock().await;
            Ok(shadow.views(&self.strategy_id, &self.symbol))
        } else {
            self.slots.get_quote_slots(&self.strategy_id, &self.symbol)
        }
    }

    async fn handle_event(&self, event: &Event) -> Result<(), String> {
        match &event.payload {
            DomainEvent::L2BookUpdate(book) => {
                if book.symbol != self.symbol {
                    return Ok(());
                }
                let mut current = self.book.lock().await;
                let stale = if let Some(prev) = current.as_ref() {
                    book.connection_generation < prev.connection_generation
                        || (book.connection_generation == prev.connection_generation
                            && book.version <= prev.version)
                } else {
                    false
                };
                if stale {
                    *self.last_reason.lock().await = "stale_market_event_fenced".to_string();
                    return Ok(());
                }
                *current = Some(book.clone());
                drop(current);
                self.cycle("book_update").await
            }
            DomainEvent::TradeUpdate(trade) => {
                if trade.symbol == self.symbol {
                    self.feature_engine.lock().await.observe_trade(trade);
                    self.cycle("trade_update").await?;
                }
                Ok(())
            }
            DomainEvent::OrderFilled(order) | DomainEvent::OrderPartialFill(order) => {
                if self.matches_order(order) {
                    if *self.mode.lock().await == MarketMakerLifecycle::Shadow {
                        self.shadow
                            .lock()
                            .await
                            .simulate_fill_by_cloid(&order.cloid, order.filled_size);
                    }
                    self.cycle("fill_update").await?;
                }
                Ok(())
            }
            DomainEvent::PositionChanged(position) => {
                if self.matches_position(position) {
                    self.cycle("position_update").await?;
                }
                Ok(())
            }
            // Account, budget, reconciliation and connection events are reliable
            // invalidation signals; providers remain authoritative.
            _ => {
                self.cycle(&event.event_type().as_str().to_lowercase())
                    .await
            }
        }
    }

    /// The coalesced event loop: subscribe to the book (latest), trade
    /// (latest), reliable, and markout mailboxes; dispatch events serially.
    pub async fn run_event_loop(&self, mut stop_rx: mpsc::Receiver<()>) -> Result<(), String> {
        let market_mailbox = self.bus.subscribe_maxsize(EventType::L2BookUpdate, 1);
        let trade_mailbox = self.bus.subscribe_maxsize(EventType::TradeUpdate, 1);
        let reliable_mailbox = self.bus.subscribe_many(RELIABLE_EVENTS);
        let markout_mailbox = self.bus.subscribe_maxsize(EventType::MmFillMarkout, 256);

        loop {
            tokio::select! {
                _ = stop_rx.recv() => break,
                maybe = recv_first(&market_mailbox, &trade_mailbox, &reliable_mailbox, &markout_mailbox) => {
                    match maybe {
                        Some(event) => {
                            if let Err(e) = self.handle_event(&event).await {
                                // A9: a transient cycle failure (stale inventory,
                                // provider hiccup, exchange rate limit) must not
                                // kill the event loop — that leaves resting quotes
                                // live while the strategy is dead. Log the reason
                                // and keep listening; the next book update re-runs
                                // the cycle.
                                tracing::error!(error = %e, "market_maker_cycle_error_continuing");
                                *self.last_reason.lock().await =
                                    format!("cycle_error: {e}");
                            }
                        }
                        None => break,
                    }
                }
            }
        }
        self.bus
            .unsubscribe(EventType::L2BookUpdate, &market_mailbox);
        self.bus.unsubscribe(EventType::TradeUpdate, &trade_mailbox);
        self.bus
            .unsubscribe_many(RELIABLE_EVENTS, &reliable_mailbox);
        self.bus
            .unsubscribe(EventType::MmFillMarkout, &markout_mailbox);
        Ok(())
    }
}

/// Await the first ready event across the four mailboxes.
async fn recv_first(
    market: &Mailbox,
    trade: &Mailbox,
    reliable: &Mailbox,
    markout: &Mailbox,
) -> Option<Arc<Event>> {
    // Prefer any already-queued event, reliable facts before lossy.
    if let Some(ev) = reliable.try_recv() {
        return Some(ev);
    }
    if let Some(ev) = market.try_recv() {
        return Some(ev);
    }
    if let Some(ev) = trade.try_recv() {
        return Some(ev);
    }
    if let Some(ev) = markout.try_recv() {
        return Some(ev);
    }
    #[allow(clippy::type_complexity)]
    let futs: Vec<
        std::pin::Pin<Box<dyn std::future::Future<Output = Option<Arc<Event>>> + Send>>,
    > = vec![
        Box::pin(reliable.recv()),
        Box::pin(market.recv()),
        Box::pin(trade.recv()),
        Box::pin(markout.recv()),
    ];
    let (res, _, _) = futures::future::select_all(futs).await;
    res
}

fn as_no_quote(mut desired: DesiredQuoteSet, reason: String) -> DesiredQuoteSet {
    desired.bid.decision = QuoteDecision::NoQuote;
    desired.bid.price = None;
    desired.bid.size = None;
    desired.bid.reason = reason.clone();
    desired.ask.decision = QuoteDecision::NoQuote;
    desired.ask.price = None;
    desired.ask.size = None;
    desired.ask.reason = reason;
    desired
}

/// A `StrategyRuntimeHandle` adapter for the market-maker runtime. Because the
/// runtime keeps its own state behind tokio locks, we drive it from the
/// supervisor; the handle forwards lifecycle calls and spawns the coalesced
/// event loop on `start`.
pub struct MarketMakerRuntimeHandle {
    runtime: Arc<MarketMakerRuntime>,
    stop_tx: tokio::sync::Mutex<Option<mpsc::Sender<()>>>,
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<(), String>>>>,
}

impl MarketMakerRuntimeHandle {
    pub fn new(runtime: Arc<MarketMakerRuntime>) -> Self {
        Self {
            runtime,
            stop_tx: tokio::sync::Mutex::new(None),
            task: tokio::sync::Mutex::new(None),
        }
    }
}

#[async_trait]
impl StrategyRuntimeHandle for MarketMakerRuntimeHandle {
    async fn start(&self) -> Result<(), String> {
        let mut running = self.runtime.running.lock().await;
        if !*running {
            *running = true;
            *self.runtime.last_reason.lock().await = "warming".to_string();
            // Spawn the coalesced event loop.
            let (stop_tx, stop_rx) = mpsc::channel(1);
            *self.stop_tx.lock().await = Some(stop_tx);
            let runtime = self.runtime.clone();
            let task = tokio::spawn(async move { runtime.run_event_loop(stop_rx).await });
            *self.task.lock().await = Some(task);
        }
        Ok(())
    }

    async fn set_mode(&self, mode: MarketMakerLifecycle) -> Result<(), String> {
        let mut current = self.runtime.mode.lock().await;
        let previous = *current;
        *current = mode;
        let cancel_modes = matches!(
            mode,
            MarketMakerLifecycle::Paused
                | MarketMakerLifecycle::Draining
                | MarketMakerLifecycle::Faulted
                | MarketMakerLifecycle::Stopped
        );
        if cancel_modes
            || (mode == MarketMakerLifecycle::Shadow && previous == MarketMakerLifecycle::Running)
        {
            drop(current);
            if cancel_modes {
                self.runtime
                    .cycle(&format!("lifecycle_{}", mode.as_str()))
                    .await?;
            }
            self.runtime
                .cancel_live_quotes(&format!("lifecycle_{}", mode.as_str()))
                .await?;
        } else if matches!(
            mode,
            MarketMakerLifecycle::Shadow | MarketMakerLifecycle::Running
        ) {
            drop(current);
            self.runtime.cycle("lifecycle_change").await?;
        }
        Ok(())
    }

    async fn apply_config(&self, config: &StrategyConfigSnapshot) -> Result<(), String> {
        if config.strategy_id != self.runtime.strategy_id {
            return Err("configuration belongs to another strategy".into());
        }
        let decoded = decode_market_maker_config(config)?;
        // A8: never admit a degenerate config (zero notionals / zero quote size
        // would divide by zero in inventory sizing).
        decoded.validate()?;
        if decoded.version != config.revision {
            return Err("decoded configuration version does not match registry revision".into());
        }
        *self.runtime.config.lock().await = Some(decoded);
        self.runtime.cycle("config_applied").await
    }

    async fn stop(&self) -> Result<(), String> {
        self.set_mode(MarketMakerLifecycle::Stopped).await?;
        *self.runtime.running.lock().await = false;
        if let Some(tx) = self.stop_tx.lock().await.take() {
            let _ = tx.send(()).await;
        }
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
        Ok(())
    }
}

/// Decode a config snapshot into `MarketMakerConfig` (A8). The previous
/// "default adapter" returned `default_with(...)`, whose notionals and quote
/// size are all zero — `inventory.calculate` then divided by zero and the
/// event loop panicked. Decode the operator's real values with non-zero
/// fallbacks, and validate before returning.
pub fn decode_market_maker_config(
    config: &StrategyConfigSnapshot,
) -> Result<MarketMakerConfig, String> {
    let defaults = MarketMakerConfig::default_with(
        config.revision,
        hypeedge_domain::decimal::Decimal::from_str_lenient("0.1").unwrap(),
        hypeedge_domain::decimal::Decimal::from_str_lenient("0.001").unwrap(),
        hypeedge_domain::decimal::Decimal::from_str_lenient("0.001").unwrap(),
    );
    let v = &config.values;
    let get_dec = |k: &str, fallback: &str| -> Result<Decimal, String> {
        match v.get(k) {
            Some(serde_json::Value::String(s)) => {
                Decimal::from_str_lenient(s).map_err(|_| format!("invalid decimal for {k}"))
            }
            Some(serde_json::Value::Number(n)) => n
                .as_f64()
                .ok_or_else(|| format!("invalid number for {k}"))
                .and_then(|f| Decimal::from_f64(f).map_err(|_| format!("invalid decimal for {k}"))),
            Some(_) => Err(format!("unexpected type for {k}")),
            None => Ok(Decimal::from_str_lenient(fallback).unwrap_or(Decimal::ZERO)),
        }
    };
    let cfg = MarketMakerConfig {
        soft_inventory_notional: Usd::new(get_dec("soft_inventory_notional", "100")?),
        hard_inventory_notional: Usd::new(get_dec("hard_inventory_notional", "150")?),
        emergency_inventory_notional: Usd::new(get_dec("emergency_inventory_notional", "200")?),
        quote_size: Size::new(get_dec("quote_size", "0.001")?),
        max_quote_lifetime_seconds: get_dec("max_quote_lifetime_seconds", "10")?,
        horizon_seconds: get_dec("horizon_seconds", "5")?,
        inventory_skew_bps: get_dec("inventory_skew_bps", "5")?,
        inventory_gamma_bps: get_dec("inventory_gamma_bps", "1")?,
        max_inventory_shift_bps: get_dec("max_inventory_shift_bps", "20")?,
        min_half_spread_bps: get_dec("min_half_spread_bps", "1")?,
        toxicity_spread_bps: get_dec("toxicity_spread_bps", "10")?,
        max_depth_participation: get_dec("max_depth_participation", "0.05")?,
        signed_maker_fee_rate: get_dec("signed_maker_fee_rate", "-0.0002")?,
        expected_fill_probability: get_dec("expected_fill_probability", "0.10")?,
        min_expected_pnl_usdc: Usd::new(get_dec("min_expected_pnl_usdc", "0")?),
        ..defaults
    };
    cfg.validate()?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_maker::models::{InventorySnapshot, MarketMakerConfig};
    use crate::trading::quote_coordinator::QuoteCoordinatorConfig;
    use hypeedge_domain::decimal::{Price, Size, Usd};
    use hypeedge_domain::models::L2Level;

    struct FakeInventory(Arc<std::sync::Mutex<Size>>);
    impl InventorySnapshotProvider for FakeInventory {
        fn get_inventory(&self, _: &str, _: &str) -> InventorySnapshot {
            InventorySnapshot {
                position_size: *self.0.lock().unwrap(),
                equity: Usd::new(hypeedge_domain::Decimal::from_str_lenient("10000").unwrap()),
                available_balance: Usd::ZERO,
                margin_used: Usd::ZERO,
                observed_at: Utc::now(),
                healthy: true,
            }
        }
    }
    struct FakeBudget;
    impl ActionBudgetSnapshotProvider for FakeBudget {
        fn get_action_budget(&self, _: &str, _: &str) -> ActionBudgetSnapshot {
            ActionBudgetSnapshot {
                mode: hypeedge_domain::enums::ActionBudgetMode::Normal,
                address_actions_remaining: 10000,
                cancel_headroom: 100,
                ip_weight_remaining: 1200,
                action_shadow_cost_usdc: Usd::ZERO,
                observed_at: Utc::now(),
                healthy: true,
            }
        }
    }
    struct FakeHealth;
    impl AccountHealthProvider for FakeHealth {
        fn allows_risk_increase(&self) -> bool {
            true
        }
    }
    struct FakeSlots;
    impl QuoteSlotProvider for FakeSlots {
        fn get_quote_slots(
            &self,
            _: &str,
            _: &str,
        ) -> Result<(QuoteSlotView, QuoteSlotView), String> {
            let bid = QuoteSlotView {
                key: crate::trading::quotes::QuoteSlotKey {
                    strategy_id: "mm_1".into(),
                    symbol: "BTC".into(),
                    side: hypeedge_domain::enums::Side::Buy,
                    level: 0,
                },
                revision: 0,
                plan_revision: 0,
                owners: vec![],
                last_transition_at: None,
            };
            let ask = QuoteSlotView {
                key: crate::trading::quotes::QuoteSlotKey {
                    strategy_id: "mm_1".into(),
                    symbol: "BTC".into(),
                    side: hypeedge_domain::enums::Side::Sell,
                    level: 0,
                },
                revision: 0,
                plan_revision: 0,
                owners: vec![],
                last_transition_at: None,
            };
            Ok((bid, ask))
        }
    }
    struct FakeCommands {
        plans: Arc<tokio::sync::Mutex<usize>>,
    }
    #[async_trait]
    impl QuotePlanCommandClient for FakeCommands {
        async fn submit_quote_plan(&self, _: &QuotePlan) -> Result<(), String> {
            *self.plans.lock().await += 1;
            Ok(())
        }
        async fn cancel_strategy_quotes(&self, _: &QuoteCancelRequest) -> Result<(), String> {
            Ok(())
        }
    }

    fn valid_config() -> MarketMakerConfig {
        let mut c = MarketMakerConfig::default_with(
            1,
            hypeedge_domain::Decimal::from_str_lenient("0.01").unwrap(),
            hypeedge_domain::Decimal::from_str_lenient("0.001").unwrap(),
            hypeedge_domain::Decimal::from_str_lenient("0.001").unwrap(),
        );
        c.soft_inventory_notional =
            Usd::new(hypeedge_domain::Decimal::from_str_lenient("1000").unwrap());
        c.hard_inventory_notional =
            Usd::new(hypeedge_domain::Decimal::from_str_lenient("2000").unwrap());
        c.emergency_inventory_notional =
            Usd::new(hypeedge_domain::Decimal::from_str_lenient("3000").unwrap());
        c.quote_size = Size::new(hypeedge_domain::Decimal::ONE);
        c
    }

    async fn make_runtime(commands: Arc<dyn QuotePlanCommandClient>) -> Arc<MarketMakerRuntime> {
        let bus = Arc::new(EventBus::new(16));
        let engine = Arc::new(tokio::sync::Mutex::new(
            MarketFeatureEngine::new(5, 5.0, 2048).unwrap(),
        ));
        let policy = MarketMakerPolicy::new();
        let coordinator = QuoteCoordinator::new(QuoteCoordinatorConfig::default()).unwrap();
        let inventory: Arc<dyn InventorySnapshotProvider> =
            Arc::new(FakeInventory(Arc::new(std::sync::Mutex::new(Size::ZERO))));
        let budget: Arc<dyn ActionBudgetSnapshotProvider> = Arc::new(FakeBudget);
        let health: Arc<dyn AccountHealthProvider> = Arc::new(FakeHealth);
        let slots: Arc<dyn QuoteSlotProvider> = Arc::new(FakeSlots);
        let runtime = MarketMakerRuntime::new(
            "mm_1".into(),
            "s1".into(),
            "sub1".into(),
            "BTC".into(),
            bus,
            engine,
            policy,
            coordinator,
            inventory,
            budget,
            health,
            slots,
            commands,
            None,
            Duration::seconds(2),
        )
        .unwrap();
        Arc::new(runtime)
    }

    async fn seed_book(runtime: &MarketMakerRuntime) {
        *runtime.book.lock().await = Some(L2BookSnapshot {
            symbol: "BTC".into(),
            bids: vec![L2Level {
                price: Price::new(hypeedge_domain::Decimal::from_str_lenient("99.5").unwrap()),
                size: Size::new(hypeedge_domain::Decimal::from_str_lenient("5").unwrap()),
            }],
            asks: vec![L2Level {
                price: Price::new(hypeedge_domain::Decimal::from_str_lenient("100.5").unwrap()),
                size: Size::new(hypeedge_domain::Decimal::from_str_lenient("5").unwrap()),
            }],
            timestamp: 1700000000000,
            local_ts: Utc::now(),
            version: 1,
            connection_generation: 0,
        });
    }

    #[tokio::test]
    async fn running_cycle_submits_quote_plan() {
        let plans = Arc::new(tokio::sync::Mutex::new(0usize));
        let commands: Arc<dyn QuotePlanCommandClient> = Arc::new(FakeCommands {
            plans: plans.clone(),
        });
        let runtime = make_runtime(commands).await;

        // Set mode + config so the cycle can run.
        *runtime.mode.lock().await = MarketMakerLifecycle::Running;
        *runtime.config.lock().await = Some(valid_config());

        seed_book(&runtime).await;
        runtime.cycle("book_update").await.unwrap();
        assert!(
            *plans.lock().await > 0,
            "running mode must submit a quote plan"
        );
        let reason = runtime.last_reason.lock().await.clone();
        assert_eq!(reason, "book_update");
        let snapshot_plan = runtime.last_plan.lock().await.clone().unwrap();
        assert!(!snapshot_plan.fenced);
        assert!(!snapshot_plan.diffs.is_empty());
    }

    #[tokio::test]
    async fn shadow_mode_does_not_submit_live() {
        let plans = Arc::new(tokio::sync::Mutex::new(0usize));
        let commands: Arc<dyn QuotePlanCommandClient> = Arc::new(FakeCommands {
            plans: plans.clone(),
        });
        let runtime = make_runtime(commands).await;
        *runtime.mode.lock().await = MarketMakerLifecycle::Shadow;
        *runtime.config.lock().await = Some(valid_config());
        seed_book(&runtime).await;
        runtime.cycle("book_update").await.unwrap();
        assert_eq!(
            *plans.lock().await,
            0,
            "shadow mode must not submit live plans"
        );
    }

    #[tokio::test]
    async fn lifecycle_not_running_forces_no_quote() {
        let plans = Arc::new(tokio::sync::Mutex::new(0usize));
        let commands: Arc<dyn QuotePlanCommandClient> = Arc::new(FakeCommands {
            plans: plans.clone(),
        });
        let runtime = make_runtime(commands).await;
        *runtime.mode.lock().await = MarketMakerLifecycle::Paused;
        *runtime.config.lock().await = Some(valid_config());
        seed_book(&runtime).await;
        runtime.cycle("lifecycle_paused").await.unwrap();
        let desired = runtime.last_desired.lock().await.clone().unwrap();
        assert_eq!(desired.bid.decision, QuoteDecision::NoQuote);
        assert_eq!(desired.bid.reason, "lifecycle_paused");
        assert_eq!(*plans.lock().await, 0);
    }

    #[test]
    fn decode_market_maker_config_reads_real_values_and_validates() {
        // A8 regression: the decoder must read the operator's values with
        // non-zero fallbacks and validate, not return the all-zero `default_with`
        // (which used to divide by zero in inventory sizing).
        let config = StrategyConfigSnapshot {
            strategy_id: "mm_1".into(),
            revision: 3,
            values: serde_json::json!({
                "soft_inventory_notional": "250",
                "hard_inventory_notional": "400",
                "emergency_inventory_notional": "600",
                "quote_size": "0.005",
            }),
        };
        let decoded = decode_market_maker_config(&config).unwrap();
        assert_eq!(decoded.version, 3);
        assert_eq!(decoded.soft_inventory_notional.to_string(), "250");
        assert_eq!(decoded.hard_inventory_notional.to_string(), "400");
        assert_eq!(decoded.emergency_inventory_notional.to_string(), "600");
        assert_eq!(decoded.quote_size.to_string(), "0.005");
        assert!(decoded.validate().is_ok(), "decoded config must validate");

        // A missing/invalid config must error, not panic.
        let bad = StrategyConfigSnapshot {
            strategy_id: "mm_1".into(),
            revision: 4,
            values: serde_json::json!({ "soft_inventory_notional": "-5" }),
        };
        assert!(
            decode_market_maker_config(&bad).is_err(),
            "invalid notionals must fail decode/validate"
        );
    }
}
