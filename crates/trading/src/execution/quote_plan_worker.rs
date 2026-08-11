//! Single-worker durable execution for market-maker quote-plan children, port
//! of `src/hypeedge/execution/quote_plan_worker.py`.
//!
//! The worker claims children with a SKIP LOCKED lease, runs the fail-closed
//! dispatch guard for placements, sends the action through the injected
//! executor (the sole nonce outlet), and records the attempt. It never retries
//! an ambiguous child: a replacement placement is deliberately not claimable
//! until the cancel child for the same plan item is durably succeeded.
//!
//! This crate stays DB-free: the claim/record boundary is the
//! [`QuotePlanStore`] trait, implemented in the storage crate.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::{Price, Size};
use hypeedge_domain::enums::{OrderStatus, OrderType, Side, TimeInForce};
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::models::{Order, OrderIntent};
use hypeedge_infra::sha256_hex;
use uuid::Uuid;

use super::batch::{ChildActionType, DispatchGuardContext, GuardDecision, evaluate_dispatch_guard};
use crate::risk::action_budget::{ActionBudgetController, BudgetAction, NetworkAttemptDebit};

/// A child action claimed by the quote-plan worker.
#[derive(Debug, Clone, PartialEq)]
pub struct QuoteDispatchChild {
    pub item_id: i64,
    pub command_id: Uuid,
    pub action: ChildActionType,
    pub attempt: u32,
    pub plan_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub runtime_session_id: String,
    pub config_version: i64,
    pub plan_revision: i64,
    pub market_version: i64,
    pub connection_generation: i64,
    pub valid_until: DateTime<Utc>,
    pub source_cloid: Option<String>,
    pub target_cloid: Option<String>,
    pub side: Side,
    pub level: u32,
    pub price: Option<Price>,
    pub size: Option<Size>,
    pub sub_account: Option<String>,
}

impl QuoteDispatchChild {
    /// Canonical request payload used for the attempt-hash fingerprint.
    pub fn request_payload(&self) -> Vec<u8> {
        let mut fields: Vec<(String, String)> = vec![
            ("action".into(), self.action.as_str().to_string()),
            (
                "source_cloid".into(),
                self.source_cloid.clone().unwrap_or_default(),
            ),
            (
                "target_cloid".into(),
                self.target_cloid.clone().unwrap_or_default(),
            ),
            ("symbol".into(), self.symbol.clone()),
            ("side".into(), self.side.as_str().to_string()),
            (
                "price".into(),
                self.price.map(|p| p.to_string()).unwrap_or_default(),
            ),
            (
                "size".into(),
                self.size.map(|s| s.to_string()).unwrap_or_default(),
            ),
        ];
        fields.sort_by(|a, b| a.0.cmp(&b.0));
        let mut out = String::new();
        for (k, v) in fields {
            out.push_str(&k);
            out.push('=');
            out.push_str(&v);
            out.push(';');
        }
        out.into_bytes()
    }

    /// Build the placement intent for a place child.
    pub fn intent(&self) -> Result<OrderIntent, HypeEdgeError> {
        let (Some(cloid), Some(price), Some(size)) = (&self.target_cloid, self.price, self.size)
        else {
            return Err(HypeEdgeError::TradingCommand {
                message: "placement child lacks a complete durable order".into(),
            });
        };
        Ok(OrderIntent {
            symbol: self.symbol.clone(),
            side: self.side,
            size,
            price: Some(price),
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Alo,
            strategy_id: Some(self.strategy_id.clone()),
            sub_account: self.sub_account.clone(),
            cloid: Some(cloid.clone()),
            client_id: None,
            is_spot: false,
            reduce_only: false,
            risk_reducing: false,
            max_slippage_bps: 50,
        })
    }
}

/// The executor injected by the application (the ExecutionEngine).
#[async_trait]
pub trait QuoteActionExecutor: Send + Sync {
    async fn submit_order(&self, intent: OrderIntent) -> Result<Order, HypeEdgeError>;
    async fn cancel_order(&self, cloid: &str) -> Result<bool, HypeEdgeError>;
}

/// The claimed-child storage boundary (SKIP LOCKED claim + attempt record).
#[async_trait]
pub trait QuotePlanStore: Send + Sync {
    /// Claim one pending child with a lease. `None` when nothing is claimable.
    async fn claim_child(
        &self,
        worker_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<QuoteDispatchChild>, HypeEdgeError>;

    /// Record a dispatch attempt outcome and settle the item/slot state.
    #[allow(clippy::too_many_arguments)]
    async fn record_attempt(
        &self,
        child: &QuoteDispatchChild,
        request_hash: &str,
        sent_at: DateTime<Utc>,
        responded_at: DateTime<Utc>,
        outcome: &str,
        status: &str,
        resolution: Option<&str>,
    ) -> Result<bool, HypeEdgeError>;

    /// Mark a child finished without sending (guard superseded/expired/blocked).
    async fn finish_without_send(
        &self,
        child: &QuoteDispatchChild,
        decision: GuardDecision,
        completed_at: DateTime<Utc>,
    ) -> Result<(), HypeEdgeError>;
}

/// Rebuild every placement gate from current authoritative application state.
#[async_trait]
pub trait QuoteDispatchGuardProvider: Send + Sync {
    async fn context(&self, child: &QuoteDispatchChild) -> DispatchGuardContext;
}

/// The worker: claim → guard → dispatch → record, with a bounded poll loop.
pub struct QuotePlanWorker {
    store: Arc<dyn QuotePlanStore>,
    executor: Arc<dyn QuoteActionExecutor>,
    guards: Arc<dyn QuoteDispatchGuardProvider>,
    budget: Arc<std::sync::Mutex<ActionBudgetController>>,
    poll_interval: Duration,
    worker_id: String,
    stopped: std::sync::atomic::AtomicBool,
}

impl QuotePlanWorker {
    pub fn new(
        store: Arc<dyn QuotePlanStore>,
        executor: Arc<dyn QuoteActionExecutor>,
        guards: Arc<dyn QuoteDispatchGuardProvider>,
        budget: Arc<std::sync::Mutex<ActionBudgetController>>,
        poll_interval_seconds: f64,
        worker_id: Option<String>,
    ) -> Self {
        Self {
            store,
            executor,
            guards,
            budget,
            poll_interval: Duration::from_secs_f64(poll_interval_seconds),
            worker_id: worker_id.unwrap_or_else(|| "quote-plan-worker".into()),
            stopped: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Run the claim→dispatch loop until stopped.
    pub async fn run(&self) -> Result<(), HypeEdgeError> {
        tracing::info!(worker_id = %self.worker_id, "quote_plan_worker_started");
        while !self.stopped.load(std::sync::atomic::Ordering::Relaxed) {
            match self.claim_one().await? {
                Some(child) => self.dispatch(child).await?,
                None => {
                    tokio::select! {
                        _ = tokio::time::sleep(self.poll_interval) => {}
                        _ = tokio::task::yield_now() => {
                            // Re-check the stop flag promptly.
                        }
                    }
                }
            }
        }
        tracing::info!(worker_id = %self.worker_id, "quote_plan_worker_stopped");
        Ok(())
    }

    pub fn stop(&self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Claim one child from the store.
    pub async fn claim_one(&self) -> Result<Option<QuoteDispatchChild>, HypeEdgeError> {
        self.store.claim_child(&self.worker_id, Utc::now()).await
    }

    /// Dispatch one claimed child and record its outcome.
    pub async fn dispatch(&self, child: QuoteDispatchChild) -> Result<(), HypeEdgeError> {
        if child.action == ChildActionType::Place {
            let decision =
                evaluate_dispatch_guard(child.action, &self.guards.context(&child).await);
            if decision != GuardDecision::Allow {
                self.store
                    .finish_without_send(&child, decision, Utc::now())
                    .await?;
                return Ok(());
            }
        }

        let sent_at = Utc::now();
        let request_hash = sha256_hex(&child.request_payload());
        let (outcome, status, resolution) = match child.action {
            ChildActionType::Cancel => match &child.source_cloid {
                None => (
                    "rejected".to_string(),
                    "failed".to_string(),
                    Some("missing_source_cloid".to_string()),
                ),
                Some(cloid) => match self.executor.cancel_order(cloid).await {
                    Ok(true) => ("succeeded".to_string(), "succeeded".to_string(), None),
                    Ok(false) => (
                        "unknown".to_string(),
                        "unknown".to_string(),
                        Some("cancel_result_not_authoritative".to_string()),
                    ),
                    Err(e) => (
                        "transport_error".to_string(),
                        "unknown".to_string(),
                        Some(e.to_string()),
                    ),
                },
            },
            ChildActionType::Place => match self.executor.submit_order(child.intent()?).await {
                Ok(order) => placement_outcome(&order),
                Err(e) => (
                    "transport_error".to_string(),
                    "unknown".to_string(),
                    Some(e.to_string()),
                ),
            },
            ChildActionType::Modify => (
                "rejected".to_string(),
                "failed".to_string(),
                Some("modify_not_supported_by_worker".to_string()),
            ),
        };

        let responded_at = Utc::now();
        let inserted = self
            .store
            .record_attempt(
                &child,
                &request_hash,
                sent_at,
                responded_at,
                &outcome,
                &status,
                resolution.as_deref(),
            )
            .await?;
        if inserted {
            let debit = NetworkAttemptDebit {
                attempt_id: format!("quote-item:{}:{}", child.item_id, child.attempt),
                child_actions: vec![budget_action(child.action)],
                ip_weight: 1,
                occurred_at: sent_at,
                strategy_id: Some(child.strategy_id.clone()),
                symbol: Some(child.symbol.clone()),
            };
            let mut budget = self.budget.lock().unwrap();
            let _ = budget.debit_network_attempt(debit);
        }
        Ok(())
    }
}

fn budget_action(action: ChildActionType) -> BudgetAction {
    match action {
        ChildActionType::Place => BudgetAction::Place,
        ChildActionType::Cancel => BudgetAction::Cancel,
        ChildActionType::Modify => BudgetAction::Modify,
    }
}

/// Map a placement order's authoritative status to (outcome, status, resolution).
fn placement_outcome(order: &Order) -> (String, String, Option<String>) {
    match order.status {
        OrderStatus::Acknowledged | OrderStatus::PartialFill | OrderStatus::Filled => {
            ("succeeded".to_string(), "succeeded".to_string(), None)
        }
        OrderStatus::SubmitUnknown => (
            "unknown".to_string(),
            "unknown".to_string(),
            order.error_message.clone(),
        ),
        OrderStatus::Rejected | OrderStatus::Cancelled | OrderStatus::Expired => (
            "rejected".to_string(),
            "failed".to_string(),
            order.error_message.clone(),
        ),
        other => (
            "unknown".to_string(),
            "unknown".to_string(),
            Some(format!("non_authoritative_order_status:{}", other.as_str())),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;

    fn child() -> QuoteDispatchChild {
        QuoteDispatchChild {
            item_id: 1,
            command_id: Uuid::new_v4(),
            action: ChildActionType::Place,
            attempt: 1,
            plan_id: Uuid::new_v4(),
            strategy_id: "mm-btc".into(),
            symbol: "BTC".into(),
            runtime_session_id: "s1".into(),
            config_version: 1,
            plan_revision: 2,
            market_version: 3,
            connection_generation: 4,
            valid_until: Utc::now() + chrono::Duration::seconds(10),
            source_cloid: None,
            target_cloid: Some("c1".into()),
            side: Side::Buy,
            level: 1,
            price: Some(Price::new(hypeedge_domain::Decimal::from_scaled(50000, 0))),
            size: Some(Size::new(hypeedge_domain::Decimal::from_scaled(1, 0))),
            sub_account: None,
        }
    }

    #[test]
    fn request_payload_is_canonical() {
        let a = child();
        let b = child();
        assert_eq!(a.request_payload(), b.request_payload());
        // Different price changes the fingerprint.
        let mut c = child();
        c.price = Some(Price::new(hypeedge_domain::Decimal::from_scaled(50001, 0)));
        assert_ne!(a.request_payload(), c.request_payload());
    }

    #[test]
    fn intent_builds_limit_alo() {
        let c = child();
        let intent = c.intent().unwrap();
        assert_eq!(intent.symbol, "BTC");
        assert_eq!(intent.order_type, OrderType::Limit);
        assert_eq!(intent.time_in_force, TimeInForce::Alo);
        assert_eq!(intent.cloid.as_deref(), Some("c1"));
    }

    #[test]
    fn intent_requires_complete_order() {
        let mut c = child();
        c.price = None;
        assert!(c.intent().is_err());
    }

    #[test]
    fn placement_outcome_maps_authoritative_status() {
        let mut order = Order::new(
            "c1".into(),
            "BTC".into(),
            Side::Buy,
            Size::new(hypeedge_domain::Decimal::ONE),
            None,
            OrderType::Limit,
            TimeInForce::Gtc,
        );
        order.status = OrderStatus::Acknowledged;
        assert_eq!(placement_outcome(&order).0, "succeeded");
        order.status = OrderStatus::SubmitUnknown;
        assert_eq!(placement_outcome(&order).0, "unknown");
        order.status = OrderStatus::Rejected;
        assert_eq!(placement_outcome(&order).0, "rejected");
    }

    #[test]
    fn request_payload_includes_action_and_side() {
        let mut c = child();
        let payload = String::from_utf8(c.request_payload()).unwrap();
        assert!(payload.contains("place"));
        assert!(payload.contains("buy"));
        c.action = ChildActionType::Cancel;
        assert!(
            String::from_utf8(c.request_payload())
                .unwrap()
                .contains("cancel")
        );
    }

    // --- Worker dispatch tests with fakes ---

    /// One recorded attempt: `(item_id, outcome, status, resolution)`.
    type RecordedAttempt = (i64, String, String, Option<String>);

    struct FakeStore {
        claims: StdMutex<VecDeque<Option<QuoteDispatchChild>>>,
        recorded: StdMutex<Vec<RecordedAttempt>>,
        finished: StdMutex<Vec<(i64, String)>>,
    }

    impl FakeStore {
        fn new(claims: Vec<Option<QuoteDispatchChild>>) -> Self {
            Self {
                claims: StdMutex::new(claims.into()),
                recorded: StdMutex::new(Vec::new()),
                finished: StdMutex::new(Vec::new()),
            }
        }
        fn recorded(&self) -> Vec<RecordedAttempt> {
            self.recorded.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl QuotePlanStore for FakeStore {
        async fn claim_child(
            &self,
            _worker_id: &str,
            _now: DateTime<Utc>,
        ) -> Result<Option<QuoteDispatchChild>, HypeEdgeError> {
            Ok(self.claims.lock().unwrap().pop_front().unwrap_or(None))
        }
        async fn record_attempt(
            &self,
            child: &QuoteDispatchChild,
            _request_hash: &str,
            _sent_at: DateTime<Utc>,
            _responded_at: DateTime<Utc>,
            outcome: &str,
            status: &str,
            resolution: Option<&str>,
        ) -> Result<bool, HypeEdgeError> {
            self.recorded.lock().unwrap().push((
                child.item_id,
                outcome.to_string(),
                status.to_string(),
                resolution.map(|s| s.to_string()),
            ));
            Ok(true)
        }
        async fn finish_without_send(
            &self,
            child: &QuoteDispatchChild,
            decision: GuardDecision,
            _completed_at: DateTime<Utc>,
        ) -> Result<(), HypeEdgeError> {
            self.finished
                .lock()
                .unwrap()
                .push((child.item_id, decision.as_str().to_string()));
            Ok(())
        }
    }

    struct AllowGuards;

    #[async_trait]
    impl QuoteDispatchGuardProvider for AllowGuards {
        async fn context(&self, _child: &QuoteDispatchChild) -> DispatchGuardContext {
            let now = Utc::now();
            DispatchGuardContext {
                now,
                deadline: now + chrono::Duration::seconds(10),
                expected_session_id: "s1".into(),
                active_session_id: "s1".into(),
                expected_config_version: 1,
                active_config_version: 1,
                expected_plan_revision: 2,
                active_plan_revision: 2,
                expected_connection_generation: 4,
                active_connection_generation: 4,
                market_fresh: true,
                account_fresh: true,
                user_stream_fresh: true,
                postgres_fresh: true,
                safety_allows_place: true,
                lifecycle_allows_place: true,
                budget_allows_place: true,
                reservation_valid: true,
                alo_valid: true,
            }
        }
    }

    struct FakeExecutor {
        submitted: StdMutex<Vec<String>>,
        cancelled: StdMutex<Vec<String>>,
    }

    impl FakeExecutor {
        fn new() -> Self {
            Self {
                submitted: StdMutex::new(Vec::new()),
                cancelled: StdMutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl QuoteActionExecutor for FakeExecutor {
        async fn submit_order(&self, intent: OrderIntent) -> Result<Order, HypeEdgeError> {
            self.submitted
                .lock()
                .unwrap()
                .push(intent.cloid.clone().unwrap_or_default());
            let mut order = Order::new(
                intent.cloid.clone().unwrap_or_default(),
                intent.symbol,
                intent.side,
                intent.size,
                intent.price,
                intent.order_type,
                intent.time_in_force,
            );
            order.status = OrderStatus::Acknowledged;
            Ok(order)
        }
        async fn cancel_order(&self, cloid: &str) -> Result<bool, HypeEdgeError> {
            self.cancelled.lock().unwrap().push(cloid.to_string());
            Ok(true)
        }
    }

    fn budget_controller() -> Arc<std::sync::Mutex<ActionBudgetController>> {
        Arc::new(std::sync::Mutex::new(
            ActionBudgetController::new(
                "0x1111111111111111111111111111111111111111",
                crate::risk::action_budget::ActionBudgetSettings::default(),
            )
            .unwrap(),
        ))
    }

    #[tokio::test]
    async fn place_child_dispatches_and_records() {
        let store = Arc::new(FakeStore::new(vec![Some(child())]));
        let executor = Arc::new(FakeExecutor::new());
        let worker = QuotePlanWorker::new(
            store.clone(),
            executor.clone(),
            Arc::new(AllowGuards),
            budget_controller(),
            0.05,
            None,
        );
        worker.dispatch(child()).await.unwrap();
        let recorded = store.recorded();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, 1);
        assert_eq!(recorded[0].1, "succeeded");
        assert_eq!(executor.submitted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn cancel_child_dispatches_without_guard() {
        let mut c = child();
        c.action = ChildActionType::Cancel;
        c.source_cloid = Some("c0".into());
        let store = Arc::new(FakeStore::new(vec![Some(c.clone())]));
        let executor = Arc::new(FakeExecutor::new());
        let worker = QuotePlanWorker::new(
            store.clone(),
            executor.clone(),
            Arc::new(AllowGuards),
            budget_controller(),
            0.05,
            None,
        );
        worker.dispatch(c).await.unwrap();
        assert_eq!(
            executor.cancelled.lock().unwrap().clone(),
            vec!["c0".to_string()]
        );
        let recorded = store.recorded();
        assert_eq!(recorded[0].1, "succeeded");
    }

    #[tokio::test]
    async fn claim_none_returns_none() {
        let store = Arc::new(FakeStore::new(vec![None]));
        let worker = QuotePlanWorker::new(
            store,
            Arc::new(FakeExecutor::new()),
            Arc::new(AllowGuards),
            budget_controller(),
            0.05,
            None,
        );
        assert!(worker.claim_one().await.unwrap().is_none());
    }

    #[test]
    fn sha256_fingerprint_stable() {
        assert_eq!(sha256_hex(b"payload"), sha256_hex(b"payload"));
        assert_ne!(sha256_hex(b"a"), sha256_hex(b"b"));
    }

    #[test]
    fn worker_id_defaults() {
        let store = Arc::new(FakeStore::new(vec![]));
        let worker = QuotePlanWorker::new(
            store,
            Arc::new(FakeExecutor::new()),
            Arc::new(AllowGuards),
            budget_controller(),
            0.05,
            None,
        );
        assert_eq!(worker.worker_id, "quote-plan-worker");
    }

    #[test]
    fn stop_flag_interrupts_run_loop() {
        let store = Arc::new(FakeStore::new(vec![]));
        let worker = QuotePlanWorker::new(
            store,
            Arc::new(FakeExecutor::new()),
            Arc::new(AllowGuards),
            budget_controller(),
            0.05,
            None,
        );
        worker.stop();
        assert!(worker.stopped.load(std::sync::atomic::Ordering::Relaxed));
    }
}
