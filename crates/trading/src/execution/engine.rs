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

/// Canonical identity of an order intent, used as the auto-cloid key (A3): two
/// intents are the same order iff they serialize to the same key.
fn intent_key(intent: &OrderIntent) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}",
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
        self.orders.lock().await.insert(order.cloid.clone(), order.clone());
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
    pub async fn submit_order_impl(
        &self,
        intent: OrderIntent,
        deferred: Option<bool>,
    ) -> Result<Order, HypeEdgeError> {
        // Spot validity gates (unconditional).
        if intent.is_spot {
            return Err(HypeEdgeError::order_rejected(
                "Hyperliquid spot execution is not enabled for this deployment",
                intent.cloid.clone(),
                Some("spot_execution_not_enabled".to_string()),
            ));
        }
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
            None => CloidGenerator::deterministic(intent.strategy_id.as_deref(), &intent_key(&intent)),
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

        // Idempotency precedes every new-placement gate.
        let existing = match self.load(&cloid).await {
            Some(o) => Some(o),
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
        // the provider is absent or the price is stale); limit orders fall
        // back to their own price.
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
            _ => intent.price.map(|p| p.inner()),
        };
        if intent.order_type == OrderType::Market && reference_price.is_none() {
            let reason = if market_snap.is_none() {
                "market_price_not_available"
            } else {
                "market_price_stale"
            };
            let risk = risk_fail(reason);
            let order = self.rejected_order(&intent, reason);
            self.persist_placement(&order, &risk, None, false, reference_price).await?;
            return Ok(order);
        }

        // Risk check (in-process, fail-safe timeout).
        let mut risk_result = risk_pass();
        if let Some(checker) = &self.risk_checker {
            risk_result = checker.check(&intent, reference_price).await;
            if !risk_result.passed {
                let reason = risk_result.reason.clone().unwrap_or_else(|| "risk_check_rejected".into());
                self.handle_risk_rejection(&reason).await;
                let order = self.rejected_order(&intent, &reason);
                self.persist_placement(&order, &risk_result, None, false, reference_price)
                    .await?;
                return Ok(order);
            }
        }

        // Action credits check.
        if self.rate_limiter.as_ref().is_some_and(|rl| !rl.check_action_credits()) {
            tracing::warn!(cloid = %cloid, "order_rejected_action_credits_low");
            let risk = risk_fail("action_credits_below_threshold");
            let order = self.rejected_order(&intent, "action_credits_below_threshold");
            self.persist_placement(&order, &risk, None, false, reference_price).await?;
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
            ..Order::new("".into(), "".into(), Side::Buy, Size::ZERO, None, OrderType::Limit, TimeInForce::Gtc)
        };
        self.store(&order).await;
        self.state_machine
            .transition(&mut order, OrderStatus::Submitted, Some("submit_order"))?;
        order.submitted_at = Some(Utc::now());
        let command_id = uuid::Uuid::new_v4();
        let durable_risk = self
            .persist_placement(&order, &risk_result, Some(command_id), true, reference_price)
            .await?;
        if let Some(risk) = durable_risk.filter(|r| !r.passed) {
            order.status = OrderStatus::Rejected;
            order.error_message = risk.reason.clone();
            self.store(&order).await;
            self.publish(DomainEvent::OrderRejected(order.clone()), &cloid);
            return Ok(order);
        }
        self.store(&order).await;
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
        let outcome = self.submit_to_exchange(&intent, &cloid, reference_price).await;
        match outcome {
            SubmitOutcome::Response(resp) => {
                self.handle_submit_response(&mut order, resp, Some(command_id)).await?;
            }
            SubmitOutcome::SafetyAborted(e) => {
                self.state_machine
                    .transition(&mut order, OrderStatus::Cancelled, Some("dispatch_aborted_by_safety_gate"))?;
                order.error_message = Some(e.to_string());
                self.persist_transition(&order, "dispatch_aborted", Some(command_id), Some("cancelled"))
                    .await?;
                self.publish(DomainEvent::OrderCancelled(order.clone()), &cloid);
            }
            SubmitOutcome::Timeout => {
                self.state_machine
                    .transition(&mut order, OrderStatus::SubmitUnknown, Some("submit_timeout"))?;
                order.error_message = Some("exchange_action_outcome_unknown".into());
                self.persist_transition(&order, "submit_unknown", Some(command_id), Some("unknown"))
                    .await?;
                tracing::error!(cloid = %cloid, "order_submit_unknown");
            }
            SubmitOutcome::Rejected(msg) => {
                self.state_machine
                    .transition(&mut order, OrderStatus::Rejected, Some(&msg))?;
                order.error_message = Some(msg.clone());
                self.persist_transition(&order, "rejected", Some(command_id), Some("failed")).await?;
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
    async fn submit_to_exchange(
        &self,
        intent: &OrderIntent,
        cloid: &str,
        reference_price: Option<Decimal>,
    ) -> SubmitOutcome {
        // Build the wire before enqueueing so payload errors surface promptly.
        let wire = match self.build_order_wire(intent, cloid, reference_price) {
            Ok(w) => w,
            Err(e) => return SubmitOutcome::Rejected(e.to_string()),
        };
        let hl_cloid = CloidGenerator::to_hl_cloid(cloid);
        let ks = self.kill_switch.clone();
        let safety = self.safety.clone();
        let rl = self.rate_limiter.clone();
        let exchange = self.exchange.clone();
        let intent_for_preflight = intent.clone();

        // Submit inside the serial nonce worker with a 3s timeout (design doc
        // §9.4). A timeout means the outcome is unknown: resolve it by cloid
        // query before classifying, never blind-resend.
        let submitted = tokio::time::timeout(
            Duration::from_millis(SUBMIT_TIMEOUT_MS),
            self.nonce.submit("order", move |nonce| {
                let ks = ks.clone();
                let safety = safety.clone();
                let rl = rl.clone();
                let exchange = exchange.clone();
                let intent_for_preflight = intent_for_preflight.clone();
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
                    exchange.order(vec![wire], nonce).await
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
                match self.exchange.query_order_by_cloid(&hl_cloid).await {
                    Ok(Some(resp)) => SubmitOutcome::Response(resp),
                    _ => SubmitOutcome::Timeout,
                }
            }
            Err(_) => {
                // 3s timeout: the action may still land on the exchange. Resolve
                // by cloid query; absence keeps it UNKNOWN for reconciliation.
                match self.exchange.query_order_by_cloid(&hl_cloid).await {
                    Ok(Some(resp)) => SubmitOutcome::Response(resp),
                    _ => SubmitOutcome::Timeout,
                }
            }
        }
    }

    fn build_order_wire(
        &self,
        intent: &OrderIntent,
        cloid: &str,
        reference_price: Option<Decimal>,
    ) -> Result<OrderWire, HypeEdgeError> {
        let Some(index_provider) = &self.asset_index_provider else {
            return Err(HypeEdgeError::Execution {
                message: "no asset index provider".into(),
            });
        };
        let asset = index_provider
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
                let p = intent.price.ok_or_else(|| HypeEdgeError::order_rejected(
                    "limit order requires a price",
                    Some(cloid.to_string()),
                    Some("price_required".to_string()),
                ))?;
                (p.inner().to_exact_string(), tif_wire(intent.time_in_force))
            }
            OrderType::Market => {
                // Market order: aggressive IoC priced with slippage.
                let reference = reference_price.ok_or_else(|| HypeEdgeError::order_rejected(
                    "market order requires a reference price",
                    Some(cloid.to_string()),
                    Some("market_price_not_available".to_string()),
                ))?;
                let slippage = Decimal::from_i128(intent.max_slippage_bps as i128)
                    .div(Decimal::from_i128(10_000));
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
                ))
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
                            self.state_machine
                                .transition(order, OrderStatus::Acknowledged, Some("exchange_ack"))?;
                            order.exchange_oid = oid;
                            order.acknowledged_at = Some(Utc::now());
                            self.persist_transition(order, "acknowledged", command_id, Some("succeeded"))
                                .await?;
                            self.publish(DomainEvent::OrderAcknowledged(order.clone()), &cloid);
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
                            self.state_machine
                                .transition(order, OrderStatus::Filled, Some("immediate_fill"))?;
                            order.filled_at = Some(Utc::now());
                            order.exchange_oid = fill.get("oid").map(|o| o.to_string());
                            order.filled_size = provisional_size;
                            order.avg_fill_price = Some(provisional_price);
                            self.persist_transition(order, "filled", command_id, Some("succeeded"))
                                .await?;
                            self.publish(DomainEvent::OrderFilled(order.clone()), &cloid);
                        } else if let Some(err) = first.get("error") {
                            let msg = err.as_str().unwrap_or("unknown_error").to_string();
                            self.state_machine
                                .transition(order, OrderStatus::Rejected, Some(&msg))?;
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
                        self.state_machine
                            .transition(order, OrderStatus::Acknowledged, Some("exchange_ack"))?;
                        order.acknowledged_at = Some(Utc::now());
                        self.persist_transition(order, "acknowledged", command_id, Some("succeeded"))
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
                self.state_machine.transition(order, OrderStatus::Rejected, Some("exchange_err"))?;
                order.error_message = Some(msg);
                self.persist_transition(order, "rejected", command_id, Some("failed")).await?;
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
                        self.state_machine
                            .transition(order, OrderStatus::Filled, Some("status_query_filled"))?;
                        order.filled_at = Some(Utc::now());
                        self.persist_transition(order, "filled", command_id, Some("succeeded")).await?;
                        self.publish(DomainEvent::OrderFilled(order.clone()), &cloid);
                    }
                    Some(OrderStatus::Cancelled) => {
                        self.state_machine
                            .transition(order, OrderStatus::Cancelled, Some("status_query_cancelled"))?;
                        self.persist_transition(order, "cancelled", command_id, Some("succeeded")).await?;
                        self.publish(DomainEvent::OrderCancelled(order.clone()), &cloid);
                    }
                    Some(OrderStatus::Rejected) => {
                        self.state_machine
                            .transition(order, OrderStatus::Rejected, Some("status_query_rejected"))?;
                        self.persist_transition(order, "rejected", command_id, Some("failed")).await?;
                        self.publish(DomainEvent::OrderRejected(order.clone()), &cloid);
                    }
                    Some(OrderStatus::Expired) => {
                        self.state_machine
                            .transition(order, OrderStatus::Expired, Some("status_query_expired"))?;
                        self.persist_transition(order, "expired", command_id, Some("failed")).await?;
                        self.publish(DomainEvent::OrderExpired(order.clone()), &cloid);
                    }
                    _ => {
                        self.state_machine
                            .transition(order, OrderStatus::Acknowledged, Some("status_query_open"))?;
                        order.acknowledged_at = Some(Utc::now());
                        self.persist_transition(order, "acknowledged", command_id, Some("succeeded")).await?;
                        self.publish(DomainEvent::OrderAcknowledged(order.clone()), &cloid);
                    }
                }
            }
            _ => {
                if is_object {
                    // Unknown response must not be treated as an acknowledgement.
                    self.state_machine
                        .transition(order, OrderStatus::SubmitUnknown, Some("unknown_response"))?;
                    order.error_message = Some("unknown_exchange_response".into());
                    self.persist_transition(order, "submit_unknown", command_id, Some("unknown"))
                        .await?;
                } else {
                    // Non-object response (e.g. raw market_open data).
                    self.state_machine
                        .transition(order, OrderStatus::Acknowledged, Some("exchange_ack"))?;
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
        if matches!(value.as_str(), "canceled" | "cancelled" | "ioccancel" | "scheduledcancel")
            || value.ends_with("canceled")
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
            self.mark_cancel_unknown(order, "invalid_cancel_response", command_id).await;
            return Ok(false);
        }
        let top_status = response.get("status").and_then(|s| s.as_str()).unwrap_or("");
        match top_status {
            "order" => {
                let status_data = response.get("order").cloned().unwrap_or(Value::Null);
                let exchange_status = status_data
                    .get("status")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_string();
                match Self::terminal_exchange_status(&exchange_status) {
                    Some(OrderStatus::Cancelled) => {
                        Ok(self.mark_cancelled(order, "cancel_status_confirmed", command_id).await?)
                    }
                    Some(OrderStatus::Filled) => {
                        self.state_machine
                            .transition(order, OrderStatus::Filled, Some("cancel_status_filled"))?;
                        order.filled_at = Some(Utc::now());
                        order.error_message = Some("cancel_not_applied_order_filled".into());
                        self.persist_transition(order, "filled", command_id, Some("failed")).await?;
                        tracing::warn!(cloid = %cloid, "cancel_order_already_filled");
                        Ok(false)
                    }
                    Some(OrderStatus::Rejected) | Some(OrderStatus::Expired) => {
                        self.state_machine
                            .transition(order, OrderStatus::Rejected, Some("cancel_status_rejected"))?;
                        order.error_message = Some("cancel_not_applied_order_rejected".into());
                        self.persist_transition(order, "rejected", command_id, Some("failed")).await?;
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
                            return self.mark_cancelled(order, "cancel_exchange_success", command_id).await;
                        }
                    } else if first.get("error").is_some() {
                        let msg = first["error"].to_string();
                        order.error_message = Some(msg);
                        self.persist_transition(order, "cancel_failed", command_id, Some("failed")).await?;
                        tracing::warn!(cloid = %cloid, "cancel_order_rejected");
                        return Ok(false);
                    }
                }
                self.mark_cancel_unknown(order, "unknown_cancel_response", command_id).await;
                Ok(false)
            }
            "err" => {
                let msg = response
                    .get("response")
                    .map(|r| r.to_string())
                    .unwrap_or_else(|| "cancel_rejected".into());
                order.error_message = Some(msg);
                self.persist_transition(order, "cancel_failed", command_id, Some("failed")).await?;
                tracing::warn!(cloid = %cloid, "cancel_order_rejected");
                Ok(false)
            }
            _ => {
                self.mark_cancel_unknown(order, "unknown_cancel_response", command_id).await;
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
        self.state_machine.transition(order, OrderStatus::Cancelled, Some(reason))?;
        order.error_message = None;
        self.persist_transition(order, "cancelled", command_id, Some("succeeded")).await?;
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
        let store = self.durable_store.as_ref().ok_or_else(|| HypeEdgeError::Execution {
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
            self.persist_transition(&order, "cancel_recovered_terminal", Some(command.command_id), Some(status))
                .await?;
            return Ok(true);
        }

        if command.requires_resolution {
            let hl = CloidGenerator::to_hl_cloid(&order.cloid);
            let resp = self
                .exchange
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
                        self.handle_cancel_response(&mut order, resp, Some(command.command_id)).await?;
                        return Ok(order.status != OrderStatus::CancelUnknown);
                    }
                }
                None => {
                    self.mark_cancel_unknown(&mut order, "cancel_recovery_status_unknown", Some(command.command_id))
                        .await;
                    return Ok(false);
                }
            }
        }

        // Send the cancel by cloid through the serial nonce queue.
        match self.submit_cancel_by_cloid(&order).await? {
            CancelSubmit::Response(value) => {
                self.handle_cancel_response(&mut order, value, Some(command.command_id)).await
            }
            CancelSubmit::Failed(msg) => {
                self.mark_cancel_unknown(&mut order, &msg, Some(command.command_id)).await;
                Ok(false)
            }
            CancelSubmit::Timeout => {
                self.mark_cancel_unknown(&mut order, "cancel_timeout", Some(command.command_id)).await;
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
        let timeout = tokio::time::timeout(
            Duration::from_millis(SUBMIT_TIMEOUT_MS),
            nonce.submit("cancel_order", move |nonce| {
                let exchange = exchange.clone();
                let hl = hl.clone();
                Box::pin(async move {
                    exchange.cancel_by_cloid(vec![CancelByCloidWire { asset, cloid: hl }], nonce).await
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
        index_provider.asset_index(symbol).ok_or_else(|| HypeEdgeError::order_rejected(
            format!("unknown symbol {symbol}"),
            Some(cloid.to_string()),
            Some("instrument_meta_unavailable".to_string()),
        ))
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

    /// Handle a risk rejection: escalate to cancel-only on timeout, kill switch
    /// on hard risk failures.
    async fn handle_risk_rejection(&self, reason: &str) {
        if reason.starts_with("risk_check_timeout") {
            if let Some(safety) = &self.safety {
                safety.lock().await.enter_cancel_only(reason);
            }
            return;
        }
        if !(reason.starts_with("risk_check_error") || reason.starts_with("drawdown_exceeded")) {
            return;
        }
        if let Some(trigger) = &self.durable_kill_trigger {
            let _ = trigger.trigger(reason).await;
        } else {
            self.kill_switch.trigger(reason).await;
        }
    }

    fn rejected_order(&self, intent: &OrderIntent, reason: &str) -> Order {
        let cloid = intent
            .cloid
            .clone()
            .unwrap_or_else(|| CloidGenerator::deterministic(intent.strategy_id.as_deref(), &intent_key(intent)));
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
            ..Order::new("".into(), "".into(), Side::Buy, Size::ZERO, None, OrderType::Limit, TimeInForce::Gtc)
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
        let store = self.durable_store.as_ref().ok_or_else(|| HypeEdgeError::Execution {
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
                .exchange
                .query_order_by_cloid(&hl)
                .await
                .map_err(|e| HypeEdgeError::Execution { message: e })?;
            let Some(response) = response else {
                if order.status != OrderStatus::SubmitUnknown {
                    self.state_machine
                        .transition(&mut order, OrderStatus::SubmitUnknown, Some("lease_recovery_unknown"))?;
                }
                order.error_message = Some("exchange_order_not_found_after_ambiguous_submission".into());
                self.persist_transition(&order, "submit_unknown", Some(command.command_id), Some("unknown"))
                    .await?;
                return Ok(false);
            };
            self.handle_submit_response(&mut order, response, Some(command.command_id)).await?;
            return Ok(order.status != OrderStatus::SubmitUnknown);
        }

        let intent = Self::intent_from_order(&order);
        // Re-run the gates before dispatch.
        if let Err(e) = self.run_gates(&intent).await {
            self.state_machine
                .transition(&mut order, OrderStatus::Cancelled, Some("dispatch_aborted_by_safety_gate"))?;
            order.error_message = Some(e.to_string());
            self.persist_transition(&order, "dispatch_aborted", Some(command.command_id), Some("cancelled"))
                .await?;
            self.publish(DomainEvent::OrderCancelled(order.clone()), &order.cloid);
            return Ok(true);
        }
        if self.rate_limiter.as_ref().is_some_and(|rl| !rl.check_action_credits()) {
            self.state_machine
                .transition(&mut order, OrderStatus::Cancelled, Some("dispatch_aborted_by_safety_gate"))?;
            order.error_message = Some("action_credits_below_threshold".into());
            self.persist_transition(&order, "dispatch_aborted", Some(command.command_id), Some("cancelled"))
                .await?;
            self.publish(DomainEvent::OrderCancelled(order.clone()), &order.cloid);
            return Ok(true);
        }

        // Resolve a market reference for market orders on replay (A1): the
        // persisted order has no limit price, so a market order needs a fresh
        // mid to build the aggressive IoC wire.
        let mut reference_price = order.price.map(|p| p.inner());
        if reference_price.is_none()
            && let Some(snap) = match &self.market_data_provider {
                Some(p) => p.get_price_snapshot(&order.symbol).await?,
                None => None,
            }
        {
            let fresh = DateTime::from_timestamp_millis(snap.timestamp).is_none_or(|observed| {
                (Utc::now() - observed).num_milliseconds() as f64 / 1000.0
                    <= self.market_price_stale_seconds
            });
            if fresh {
                reference_price = Some(snap.price);
            }
        }
        let outcome = self.submit_to_exchange(&intent, cloid, reference_price).await;
        match outcome {
            SubmitOutcome::Response(resp) => {
                if let Some(hook) = &after_send_hook {
                    hook(command);
                }
                self.handle_submit_response(&mut order, resp, Some(command.command_id)).await?;
                Ok(order.status != OrderStatus::SubmitUnknown)
            }
            SubmitOutcome::SafetyAborted(e) => {
                self.state_machine
                    .transition(&mut order, OrderStatus::Cancelled, Some("dispatch_aborted_by_safety_gate"))?;
                order.error_message = Some(e.to_string());
                self.persist_transition(&order, "dispatch_aborted", Some(command.command_id), Some("cancelled"))
                    .await?;
                self.publish(DomainEvent::OrderCancelled(order.clone()), &order.cloid);
                Ok(true)
            }
            SubmitOutcome::Timeout => {
                self.state_machine
                    .transition(&mut order, OrderStatus::SubmitUnknown, Some("submit_timeout"))?;
                order.error_message = Some("exchange_action_outcome_unknown".into());
                self.persist_transition(&order, "submit_unknown", Some(command.command_id), Some("unknown"))
                    .await?;
                Ok(false)
            }
            SubmitOutcome::Rejected(msg) => {
                self.state_machine.transition(&mut order, OrderStatus::Rejected, Some("exchange_err"))?;
                order.error_message = Some(msg);
                self.persist_transition(&order, "rejected", Some(command.command_id), Some("failed")).await?;
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
    pub async fn import_exchange_order_authoritative(&self, order: Order) -> Result<(), HypeEdgeError> {
        if let Some(store) = &self.durable_store {
            store.persist_reconciled_order(&order).await?;
        }
        self.store(&order).await;
        Ok(())
    }

    /// Refresh one committed exchange projection into process memory.
    pub async fn refresh_order_from_durable(&self, cloid: &str) -> Result<Option<Order>, HypeEdgeError> {
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
        let result = self
            .nonce
            .submit("update_leverage", move |nonce| {
                let exchange = exchange.clone();
                let ks = ks.clone();
                Box::pin(async move {
                    // Preflight inside the serial worker (mirrors engine.py):
                    // queued leverage changes cannot survive a kill/safety move.
                    if let Err(e) = ks.check().await {
                        return Err(e.to_string());
                    }
                    exchange
                        .update_leverage(asset, is_cross, leverage as i64, nonce)
                        .await
                })
            })
            .await
            .map_err(|e| HypeEdgeError::Execution { message: e })?;
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
            tracing::warn!(cloid, status = order.status.as_str(), "cancel_order_already_terminal");
            return Ok(false);
        }

        let command_id = uuid::Uuid::new_v4();
        if let Some(store) = &self.durable_store {
            store.persist_cancel_requested(&order, command_id).await?;
        }

        // Send the cancel by cloid through the serial nonce queue.
        match self.submit_cancel_by_cloid(&order).await? {
            CancelSubmit::Response(value) => {
                let accepted = self.handle_cancel_response(&mut order, value, Some(command_id)).await?;
                self.store(&order).await;
                Ok(accepted)
            }
            CancelSubmit::Failed(msg) => {
                self.mark_cancel_unknown(&mut order, &msg, Some(command_id)).await;
                self.store(&order).await;
                tracing::warn!(cloid, error = %msg, "cancel_order_unknown");
                Ok(false)
            }
            CancelSubmit::Timeout => {
                self.mark_cancel_unknown(&mut order, "cancel_timeout", Some(command_id)).await;
                self.store(&order).await;
                tracing::warn!(cloid, "cancel_order_unknown_timeout");
                Ok(false)
            }
        }
    }

    async fn cancel_all_orders(&self, symbol: Option<&str>) -> Result<u64, HypeEdgeError> {
        let open: Vec<String> = self
            .orders
            .lock()
            .await
            .values()
            .filter(|o| !o.is_terminal())
            .filter(|o| symbol.is_none_or(|s| o.symbol == s))
            .map(|o| o.cloid.clone())
            .collect();
        let mut cancelled = 0u64;
        for cloid in open {
            if self.cancel_order(&cloid).await? {
                cancelled += 1;
            }
        }
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
    use async_trait::async_trait;
    use hypeedge_domain::decimal::{Price, Size};
    use hypeedge_domain::models::{AccountState, MidPrice, Position};
    use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

    struct MockExchange {
        responses: Arc<Mutex<VecDeque<Value>>>,
        submitted: Arc<AtomicU32>,
        query_result: Option<Value>,
        order_error: bool,
    }

    impl MockExchange {
        fn new(responses: Vec<Value>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses.into())),
                submitted: Arc::new(AtomicU32::new(0)),
                query_result: None,
                order_error: false,
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
    }

    use std::collections::VecDeque;

    #[async_trait]
    impl ExchangeClient for MockExchange {
        async fn order(&self, _orders: Vec<OrderWire>, _nonce: u64) -> Result<Value, String> {
            self.submitted.fetch_add(1, AtomicOrdering::SeqCst);
            if self.order_error {
                return Err("transport_error".into());
            }
            Ok(self.responses.lock().await.pop_front().unwrap_or(Value::Null))
        }
        async fn cancel(&self, _cancels: Vec<crate::execution::signing::CancelWire>, _nonce: u64) -> Result<Value, String> {
            self.submitted.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(Value::Null)
        }
        async fn cancel_by_cloid(
            &self,
            _cancels: Vec<crate::execution::signing::CancelByCloidWire>,
            _nonce: u64,
        ) -> Result<Value, String> {
            self.submitted.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(self.responses.lock().await.pop_front().unwrap_or(Value::Null))
        }
        async fn update_leverage(
            &self,
            _asset: i64,
            _is_cross: bool,
            _leverage: i64,
            _nonce: u64,
        ) -> Result<Value, String> {
            Ok(Value::Null)
        }
        async fn query_order_by_cloid(&self, _cloid: &str) -> Result<Option<Value>, String> {
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
        })
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
        assert!(order.error_message.as_deref().unwrap().contains("Invalid price"));
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
        });
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(order.status, OrderStatus::Rejected);
        assert_eq!(order.error_message.as_deref(), Some("account_state_not_available"));
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
                .with_query(serde_json::json!({"status": "order", "order": {"status": "filled"}})),
        );
        let engine = base_engine(exchange);
        let order = engine.submit_order(limit_intent(), None).await.unwrap();
        assert_eq!(order.status, OrderStatus::Filled, "cloid resolution must apply the authoritative outcome");
        assert!(order.filled_at.is_some());
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
}
