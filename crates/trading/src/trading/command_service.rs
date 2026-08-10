//! Unified, fail-closed admission boundary for all trading commands, port of
//! `src/hypeedge/trading/command_service.py`.
//!
//! [`TradingCommandService`] admits placements in the one authorized order
//! (safety → data health → risk → action budget → normalize) and persists the
//! canonical [`TradingCommand`] through a durable sink. A false or unknown
//! gate decision always means rejection; every rejection is itself persisted.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::Decimal;
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::models::{OrderIntent, RiskCheckResult};
use uuid::Uuid;

use crate::execution::normalizer::OrderNormalizer;
use crate::risk::action_budget::{ActionBudgetController, BudgetAction, PermissionRequest};
use crate::risk::safety::SafetyController;

/// The kind of trading command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingCommandKind {
    Place,
    Cancel,
    CancelAll,
}

impl TradingCommandKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TradingCommandKind::Place => "place",
            TradingCommandKind::Cancel => "cancel",
            TradingCommandKind::CancelAll => "cancel_all",
        }
    }
}

/// Command admission outcome. `ACCEPTED` means queued, not exchange-acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingCommandStatus {
    Accepted,
    Rejected,
}

impl TradingCommandStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            TradingCommandStatus::Accepted => "accepted",
            TradingCommandStatus::Rejected => "rejected",
        }
    }
}

/// A named admission decision; false or unknown always means rejection.
#[derive(Debug, Clone, PartialEq)]
pub struct GateDecision {
    pub allowed: bool,
    pub reason: Option<String>,
}

impl GateDecision {
    pub fn allow() -> Self {
        Self {
            allowed: true,
            reason: None,
        }
    }

    pub fn reject(reason: impl Into<String>) -> Self {
        Self {
            allowed: false,
            reason: Some(reason.into()),
        }
    }
}

/// Data-health decision plus the immutable market context used downstream.
#[derive(Debug, Clone, PartialEq)]
pub struct DataHealthDecision {
    pub allowed: bool,
    pub reason: Option<String>,
    pub reference_price: Option<Decimal>,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub market_version: Option<i64>,
    pub connection_generation: Option<i64>,
}

impl DataHealthDecision {
    pub fn reject(reason: impl Into<String>) -> Self {
        Self {
            allowed: false,
            reason: Some(reason.into()),
            reference_price: None,
            best_bid: None,
            best_ask: None,
            market_version: None,
            connection_generation: None,
        }
    }
}

/// Canonical command persisted before any signed side effect.
#[derive(Debug, Clone, PartialEq)]
pub struct TradingCommand {
    pub command_id: Uuid,
    pub kind: TradingCommandKind,
    pub status: TradingCommandStatus,
    pub created_at: DateTime<Utc>,
    pub intent: Option<OrderIntent>,
    pub target_cloid: Option<String>,
    pub symbol: Option<String>,
    pub strategy_id: Option<String>,
    pub rejection_gate: Option<String>,
    pub rejection_reason: Option<String>,
    pub risk_result: Option<RiskCheckResult>,
    pub market_version: Option<i64>,
    pub connection_generation: Option<i64>,
}

/// Durable acknowledgement. `ACCEPTED` means queued, not exchange-acknowledged.
#[derive(Debug, Clone, PartialEq)]
pub struct TradingCommandReceipt {
    pub command_id: Uuid,
    pub kind: TradingCommandKind,
    pub status: TradingCommandStatus,
    pub created_at: DateTime<Utc>,
    pub intent: Option<OrderIntent>,
    pub target_cloid: Option<String>,
    pub symbol: Option<String>,
    pub rejection_gate: Option<String>,
    pub rejection_reason: Option<String>,
}

impl TradingCommandReceipt {
    pub fn accepted(&self) -> bool {
        self.status == TradingCommandStatus::Accepted
    }
}

// --- Gate boundaries (implemented by real components and faked in tests) ---

/// Fail-closed placement gate that raises on rejection (e.g. safety mode).
pub trait SafetyPlacementGate: Send + Sync {
    fn check_placement(&self, intent: &OrderIntent) -> Result<(), HypeEdgeError>;
}

/// Async data-health gate producing the market context used downstream.
#[async_trait]
pub trait DataHealthGate: Send + Sync {
    async fn check_placement(
        &self,
        intent: &OrderIntent,
    ) -> Result<DataHealthDecision, HypeEdgeError>;
}

/// Risk admission gate (fail-safe: any error is a rejection).
#[async_trait]
pub trait RiskAdmissionGate: Send + Sync {
    async fn check(
        &self,
        intent: &OrderIntent,
        reference_price: Option<Decimal>,
    ) -> RiskCheckResult;
}

/// Action-budget placement gate.
#[async_trait]
pub trait ActionBudgetAdmissionGate: Send + Sync {
    async fn check_placement(&self, intent: &OrderIntent) -> Result<GateDecision, HypeEdgeError>;
}

/// Order-intent normalizer (quantizes size/price to instrument rules).
pub trait OrderIntentNormalizer: Send + Sync {
    fn normalize(
        &self,
        intent: &OrderIntent,
        best_bid: Option<Decimal>,
        best_ask: Option<Decimal>,
    ) -> Result<OrderIntent, HypeEdgeError>;
}

/// Durable sink for the canonical command.
#[async_trait]
pub trait DurableTradingCommandSink: Send + Sync {
    async fn persist(
        &self,
        command: &TradingCommand,
    ) -> Result<TradingCommandReceipt, HypeEdgeError>;
}

/// Admit placements in the one authorized order, then persist them.
pub struct TradingCommandService {
    safety: Arc<dyn SafetyPlacementGate>,
    data_health: Arc<dyn DataHealthGate>,
    risk: Arc<dyn RiskAdmissionGate>,
    action_budget: Arc<dyn ActionBudgetAdmissionGate>,
    normalizer: Arc<dyn OrderIntentNormalizer>,
    sink: Arc<dyn DurableTradingCommandSink>,
}

impl TradingCommandService {
    pub fn new(
        safety: Arc<dyn SafetyPlacementGate>,
        data_health: Arc<dyn DataHealthGate>,
        risk: Arc<dyn RiskAdmissionGate>,
        action_budget: Arc<dyn ActionBudgetAdmissionGate>,
        normalizer: Arc<dyn OrderIntentNormalizer>,
        sink: Arc<dyn DurableTradingCommandSink>,
    ) -> Self {
        Self {
            safety,
            data_health,
            risk,
            action_budget,
            normalizer,
            sink,
        }
    }

    /// Admit and persist a placement intent.
    pub async fn submit_order(
        &self,
        intent: OrderIntent,
        command_id: Option<Uuid>,
    ) -> Result<TradingCommandReceipt, HypeEdgeError> {
        let command_id = command_id.unwrap_or_else(Uuid::new_v4);
        let created_at = Utc::now();

        // 1. Safety.
        if let Err(e) = self.safety.check_placement(&intent) {
            return self
                .persist_rejection(
                    command_id,
                    created_at,
                    &intent,
                    "safety",
                    &e.to_string(),
                    None,
                    None,
                )
                .await;
        }

        // 2. Data health.
        let data = match self.data_health.check_placement(&intent).await {
            Ok(data) => data,
            Err(e) => {
                return self
                    .persist_rejection(
                        command_id,
                        created_at,
                        &intent,
                        "data_health",
                        &e.to_string(),
                        None,
                        None,
                    )
                    .await;
            }
        };
        if !data.allowed {
            let reason = data
                .reason
                .clone()
                .unwrap_or_else(|| "data_health_rejected".into());
            return self
                .persist_rejection(
                    command_id,
                    created_at,
                    &intent,
                    "data_health",
                    &reason,
                    None,
                    Some(&data),
                )
                .await;
        }

        // 3. Risk.
        let reference_price = data.reference_price;
        let risk_result = self.risk.check(&intent, reference_price).await;
        if !risk_result.passed {
            let reason = risk_result
                .reason
                .clone()
                .unwrap_or_else(|| "risk_rejected".into());
            return self
                .persist_rejection(
                    command_id,
                    created_at,
                    &intent,
                    "risk",
                    &reason,
                    Some(&risk_result),
                    Some(&data),
                )
                .await;
        }

        // 4. Action budget.
        let budget = match self.action_budget.check_placement(&intent).await {
            Ok(budget) => budget,
            Err(e) => {
                return self
                    .persist_rejection(
                        command_id,
                        created_at,
                        &intent,
                        "action_budget",
                        &e.to_string(),
                        Some(&risk_result),
                        Some(&data),
                    )
                    .await;
            }
        };
        if !budget.allowed {
            let reason = budget
                .reason
                .clone()
                .unwrap_or_else(|| "action_budget_rejected".into());
            return self
                .persist_rejection(
                    command_id,
                    created_at,
                    &intent,
                    "action_budget",
                    &reason,
                    Some(&risk_result),
                    Some(&data),
                )
                .await;
        }

        // 5. Normalize.
        let normalized = match self
            .normalizer
            .normalize(&intent, data.best_bid, data.best_ask)
        {
            Ok(normalized) => normalized,
            Err(e) => {
                return self
                    .persist_rejection(
                        command_id,
                        created_at,
                        &intent,
                        "normalize",
                        &e.to_string(),
                        Some(&risk_result),
                        Some(&data),
                    )
                    .await;
            }
        };

        self.persist(TradingCommand {
            command_id,
            kind: TradingCommandKind::Place,
            status: TradingCommandStatus::Accepted,
            created_at,
            intent: Some(normalized.clone()),
            target_cloid: None,
            symbol: Some(normalized.symbol.clone()),
            strategy_id: normalized.strategy_id.clone(),
            rejection_gate: None,
            rejection_reason: None,
            risk_result: Some(risk_result),
            market_version: data.market_version,
            connection_generation: data.connection_generation,
        })
        .await
    }

    /// Submit a placement (alias used by strategies).
    pub async fn submit_placement(
        &self,
        intent: OrderIntent,
        command_id: Option<Uuid>,
    ) -> Result<TradingCommandReceipt, HypeEdgeError> {
        self.submit_order(intent, command_id).await
    }

    /// Persist a cancel-order command.
    pub async fn cancel_order(
        &self,
        cloid: &str,
        strategy_id: Option<String>,
        command_id: Option<Uuid>,
    ) -> Result<TradingCommandReceipt, HypeEdgeError> {
        if cloid.is_empty() {
            return Err(HypeEdgeError::TradingCommand {
                message: "cloid is required".into(),
            });
        }
        self.persist(TradingCommand {
            command_id: command_id.unwrap_or_else(Uuid::new_v4),
            kind: TradingCommandKind::Cancel,
            status: TradingCommandStatus::Accepted,
            created_at: Utc::now(),
            intent: None,
            target_cloid: Some(cloid.to_string()),
            symbol: None,
            strategy_id,
            rejection_gate: None,
            rejection_reason: None,
            risk_result: None,
            market_version: None,
            connection_generation: None,
        })
        .await
    }

    /// Persist a cancel-all command.
    pub async fn cancel_all_orders(
        &self,
        symbol: Option<String>,
        strategy_id: Option<String>,
        command_id: Option<Uuid>,
    ) -> Result<TradingCommandReceipt, HypeEdgeError> {
        self.persist(TradingCommand {
            command_id: command_id.unwrap_or_else(Uuid::new_v4),
            kind: TradingCommandKind::CancelAll,
            status: TradingCommandStatus::Accepted,
            created_at: Utc::now(),
            intent: None,
            target_cloid: None,
            symbol,
            strategy_id,
            rejection_gate: None,
            rejection_reason: None,
            risk_result: None,
            market_version: None,
            connection_generation: None,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist_rejection(
        &self,
        command_id: Uuid,
        created_at: DateTime<Utc>,
        intent: &OrderIntent,
        gate: &str,
        reason: &str,
        risk_result: Option<&RiskCheckResult>,
        data: Option<&DataHealthDecision>,
    ) -> Result<TradingCommandReceipt, HypeEdgeError> {
        let reason = safe_reason(reason);
        tracing::warn!(
            command_id = %command_id,
            gate,
            reason = %reason,
            strategy_id = ?intent.strategy_id,
            symbol = %intent.symbol,
            "trading_command_rejected"
        );
        self.persist(TradingCommand {
            command_id,
            kind: TradingCommandKind::Place,
            status: TradingCommandStatus::Rejected,
            created_at,
            intent: Some(intent.clone()),
            target_cloid: None,
            symbol: Some(intent.symbol.clone()),
            strategy_id: intent.strategy_id.clone(),
            rejection_gate: Some(gate.to_string()),
            rejection_reason: Some(reason),
            risk_result: risk_result.cloned(),
            market_version: data.and_then(|d| d.market_version),
            connection_generation: data.and_then(|d| d.connection_generation),
        })
        .await
    }

    async fn persist(
        &self,
        command: TradingCommand,
    ) -> Result<TradingCommandReceipt, HypeEdgeError> {
        self.sink.persist(&command).await.map_err(|e| match e {
            HypeEdgeError::TradingCommandConflict { .. } => e,
            _ => HypeEdgeError::TradingCommandPersistence {
                message: format!(
                    "Durable command persistence failed: command_id={} kind={}",
                    command.command_id,
                    command.kind.as_str()
                ),
            },
        })
    }
}

/// Expose the synchronous scope controller as the command-service gate.
pub struct ActionBudgetControllerAdapter {
    controller: ActionBudgetController,
}

impl ActionBudgetControllerAdapter {
    pub fn new(controller: ActionBudgetController) -> Self {
        Self { controller }
    }
}

#[async_trait]
impl ActionBudgetAdmissionGate for ActionBudgetControllerAdapter {
    async fn check_placement(&self, intent: &OrderIntent) -> Result<GateDecision, HypeEdgeError> {
        let request = PermissionRequest {
            action: BudgetAction::Place,
            strategy_id: intent.strategy_id.clone(),
            symbol: if intent.strategy_id.is_some() {
                Some(intent.symbol.clone())
            } else {
                None
            },
            child_actions: 1,
            ip_weight: 1,
            risk_reducing: intent.reduce_only || intent.risk_reducing,
            emergency: false,
        };
        match self.controller.permission(&request) {
            Ok(permission) => Ok(GateDecision {
                allowed: permission.allowed,
                reason: Some(permission.reason),
            }),
            Err(e) => Err(HypeEdgeError::TradingCommand { message: e }),
        }
    }
}

/// Deterministic idempotent sink for unit tests and non-production simulations.
pub struct InMemoryTradingCommandSink {
    commands: std::sync::Mutex<std::collections::HashMap<Uuid, (String, TradingCommandReceipt)>>,
}

impl Default for InMemoryTradingCommandSink {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryTradingCommandSink {
    pub fn new() -> Self {
        Self {
            commands: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub fn receipts(&self) -> Vec<TradingCommandReceipt> {
        let guard = self.commands.lock().unwrap();
        guard.values().map(|(_, receipt)| receipt.clone()).collect()
    }
}

#[async_trait]
impl DurableTradingCommandSink for InMemoryTradingCommandSink {
    async fn persist(
        &self,
        command: &TradingCommand,
    ) -> Result<TradingCommandReceipt, HypeEdgeError> {
        let fingerprint = fingerprint(command);
        let mut guard = self.commands.lock().unwrap();
        if let Some((existing_fingerprint, receipt)) = guard.get(&command.command_id) {
            if existing_fingerprint != &fingerprint {
                return Err(HypeEdgeError::TradingCommandConflict {
                    message: format!(
                        "Command id {} was reused with a different payload",
                        command.command_id
                    ),
                });
            }
            return Ok(receipt.clone());
        }
        let receipt = TradingCommandReceipt {
            command_id: command.command_id,
            kind: command.kind,
            status: command.status,
            created_at: command.created_at,
            intent: command.intent.clone(),
            target_cloid: command.target_cloid.clone(),
            symbol: command.symbol.clone(),
            rejection_gate: command.rejection_gate.clone(),
            rejection_reason: command.rejection_reason.clone(),
        };
        guard.insert(command.command_id, (fingerprint, receipt.clone()));
        Ok(receipt)
    }
}

/// A stable, order-independent semantic fingerprint of a command.
fn fingerprint(command: &TradingCommand) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    let mut push = |key: &str, value: String| {
        use std::io::Write;
        let _ = hasher.write_all(key.as_bytes());
        let _ = hasher.write_all(b"=");
        let _ = hasher.write_all(value.as_bytes());
        let _ = hasher.write_all(b";");
    };
    push("kind", command.kind.as_str().to_string());
    push("status", command.status.as_str().to_string());
    push(
        "target_cloid",
        command.target_cloid.clone().unwrap_or_default(),
    );
    push("symbol", command.symbol.clone().unwrap_or_default());
    push(
        "strategy_id",
        command.strategy_id.clone().unwrap_or_default(),
    );
    push(
        "rejection_gate",
        command.rejection_gate.clone().unwrap_or_default(),
    );
    push(
        "rejection_reason",
        command.rejection_reason.clone().unwrap_or_default(),
    );
    if let Some(intent) = &command.intent {
        push("intent.symbol", intent.symbol.clone());
        push("intent.side", intent.side.as_str().to_string());
        push("intent.size", intent.size.to_string());
        push(
            "intent.price",
            intent.price.map(|p| p.to_string()).unwrap_or_default(),
        );
        push("intent.order_type", intent.order_type.as_str().to_string());
        push(
            "intent.time_in_force",
            intent.time_in_force.as_str().to_string(),
        );
        push(
            "intent.strategy_id",
            intent.strategy_id.clone().unwrap_or_default(),
        );
        push(
            "intent.sub_account",
            intent.sub_account.clone().unwrap_or_default(),
        );
        push("intent.reduce_only", intent.reduce_only.to_string());
        push("intent.cloid", intent.cloid.clone().unwrap_or_default());
        push(
            "intent.client_id",
            intent.client_id.clone().unwrap_or_default(),
        );
    }
    format!("{:x}", hasher.finalize())
}

/// Strip whitespace; fall back to a stable default for empty reasons.
fn safe_reason(reason: &str) -> String {
    let trimmed = reason.trim();
    if trimmed.is_empty() {
        "rejected".to_string()
    } else {
        trimmed.to_string()
    }
}

// Adapter wiring helpers so the concrete gates can be built from the real
// components without duplicated plumbing in the app crate.

/// Wire the safety gate from a `SafetyController`.
impl SafetyPlacementGate for SafetyController {
    fn check_placement(&self, intent: &OrderIntent) -> Result<(), HypeEdgeError> {
        SafetyController::check_placement(self, intent)
    }
}

/// Wire the risk gate from the `RiskChecker`.
#[async_trait]
impl RiskAdmissionGate for crate::risk::checker::RiskChecker {
    async fn check(
        &self,
        intent: &OrderIntent,
        reference_price: Option<Decimal>,
    ) -> RiskCheckResult {
        crate::risk::checker::RiskChecker::check(self, intent, reference_price).await
    }
}

/// Wire the normalizer gate from `OrderNormalizer`.
impl OrderIntentNormalizer for OrderNormalizer {
    fn normalize(
        &self,
        intent: &OrderIntent,
        best_bid: Option<Decimal>,
        best_ask: Option<Decimal>,
    ) -> Result<OrderIntent, HypeEdgeError> {
        OrderNormalizer::normalize(self, intent, best_bid, best_ask)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::decimal::{Price, Size};
    use hypeedge_domain::enums::{OrderType, Side, TimeInForce};

    fn intent() -> OrderIntent {
        OrderIntent {
            symbol: "BTC".into(),
            side: Side::Buy,
            size: Size::new(Decimal::ONE),
            price: Some(Price::new(Decimal::from_scaled(50000, 0))),
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            strategy_id: Some("mm-btc".into()),
            sub_account: None,
            reduce_only: false,
            cloid: Some("c1".into()),
            client_id: None,
            is_spot: false,
            risk_reducing: false,
            max_slippage_bps: 50,
        }
    }

    // --- Mock gates ---

    struct AllowAllSafety;

    impl SafetyPlacementGate for AllowAllSafety {
        fn check_placement(&self, _intent: &OrderIntent) -> Result<(), HypeEdgeError> {
            Ok(())
        }
    }

    struct RejectSafety(String);

    impl SafetyPlacementGate for RejectSafety {
        fn check_placement(&self, _intent: &OrderIntent) -> Result<(), HypeEdgeError> {
            Err(HypeEdgeError::order_rejected(
                self.0.clone(),
                None,
                Some("safety_mode_test".to_string()),
            ))
        }
    }

    struct StaticDataHealth {
        decision: DataHealthDecision,
    }

    impl StaticDataHealth {
        fn allowed() -> Self {
            Self {
                decision: DataHealthDecision {
                    allowed: true,
                    reason: None,
                    reference_price: Some(Decimal::from_scaled(50000, 0)),
                    best_bid: Some(Decimal::from_scaled(49950, 0)),
                    best_ask: Some(Decimal::from_scaled(50050, 0)),
                    market_version: Some(7),
                    connection_generation: Some(3),
                },
            }
        }
        fn rejected(reason: &str) -> Self {
            Self {
                decision: DataHealthDecision::reject(reason),
            }
        }
    }

    #[async_trait]
    impl DataHealthGate for StaticDataHealth {
        async fn check_placement(
            &self,
            _intent: &OrderIntent,
        ) -> Result<DataHealthDecision, HypeEdgeError> {
            Ok(self.decision.clone())
        }
    }

    struct FailingDataHealth(String);

    #[async_trait]
    impl DataHealthGate for FailingDataHealth {
        async fn check_placement(
            &self,
            _intent: &OrderIntent,
        ) -> Result<DataHealthDecision, HypeEdgeError> {
            Err(HypeEdgeError::MarketData(self.0.clone()))
        }
    }

    struct StaticRisk {
        result: RiskCheckResult,
    }

    impl StaticRisk {
        fn allowed() -> Self {
            Self {
                result: RiskCheckResult {
                    passed: true,
                    reason: None,
                    checked_limits: vec!["test".into()],
                },
            }
        }
        fn rejected(reason: &str) -> Self {
            Self {
                result: RiskCheckResult {
                    passed: false,
                    reason: Some(reason.into()),
                    checked_limits: vec!["test".into()],
                },
            }
        }
    }

    #[async_trait]
    impl RiskAdmissionGate for StaticRisk {
        async fn check(
            &self,
            _intent: &OrderIntent,
            _reference_price: Option<Decimal>,
        ) -> RiskCheckResult {
            self.result.clone()
        }
    }

    struct StaticBudget {
        decision: GateDecision,
    }

    impl StaticBudget {
        fn allowed() -> Self {
            Self {
                decision: GateDecision::allow(),
            }
        }
        fn rejected(reason: &str) -> Self {
            Self {
                decision: GateDecision::reject(reason),
            }
        }
    }

    #[async_trait]
    impl ActionBudgetAdmissionGate for StaticBudget {
        async fn check_placement(
            &self,
            _intent: &OrderIntent,
        ) -> Result<GateDecision, HypeEdgeError> {
            Ok(self.decision.clone())
        }
    }

    struct PassThroughNormalizer;

    impl OrderIntentNormalizer for PassThroughNormalizer {
        fn normalize(
            &self,
            intent: &OrderIntent,
            _best_bid: Option<Decimal>,
            _best_ask: Option<Decimal>,
        ) -> Result<OrderIntent, HypeEdgeError> {
            Ok(intent.clone())
        }
    }

    struct FailingNormalizer(String);

    impl OrderIntentNormalizer for FailingNormalizer {
        fn normalize(
            &self,
            _intent: &OrderIntent,
            _best_bid: Option<Decimal>,
            _best_ask: Option<Decimal>,
        ) -> Result<OrderIntent, HypeEdgeError> {
            Err(HypeEdgeError::OrderNormalization {
                message: self.0.clone(),
                symbol: "BTC".into(),
                reason: "test".into(),
            })
        }
    }

    type AllowSet = (
        Arc<dyn SafetyPlacementGate>,
        Arc<dyn DataHealthGate>,
        Arc<dyn RiskAdmissionGate>,
        Arc<dyn ActionBudgetAdmissionGate>,
        Arc<dyn OrderIntentNormalizer>,
    );

    fn all_allow() -> AllowSet {
        (
            Arc::new(AllowAllSafety),
            Arc::new(StaticDataHealth::allowed()),
            Arc::new(StaticRisk::allowed()),
            Arc::new(StaticBudget::allowed()),
            Arc::new(PassThroughNormalizer),
        )
    }

    fn service(
        safety: Arc<dyn SafetyPlacementGate>,
        data_health: Arc<dyn DataHealthGate>,
        risk: Arc<dyn RiskAdmissionGate>,
        budget: Arc<dyn ActionBudgetAdmissionGate>,
        normalizer: Arc<dyn OrderIntentNormalizer>,
        sink: Arc<InMemoryTradingCommandSink>,
    ) -> TradingCommandService {
        TradingCommandService::new(safety, data_health, risk, budget, normalizer, sink)
    }

    #[tokio::test]
    async fn accepted_placement_passes_all_gates_and_normalizes() {
        let sink = Arc::new(InMemoryTradingCommandSink::new());
        let (s, dh, r, b, n) = all_allow();
        let svc = service(s, dh, r, b, n, sink.clone());
        let receipt = svc.submit_order(intent(), None).await.unwrap();
        assert!(receipt.accepted());
        assert_eq!(receipt.kind, TradingCommandKind::Place);
        assert!(receipt.intent.is_some());
        assert_eq!(receipt.rejection_gate, None);
        assert_eq!(sink.receipts().len(), 1);
    }

    #[tokio::test]
    async fn safety_rejection_is_persisted() {
        let sink = Arc::new(InMemoryTradingCommandSink::new());
        let (_, dh, r, b, n) = all_allow();
        let svc = service(
            Arc::new(RejectSafety("halting".into())),
            dh,
            r,
            b,
            n,
            sink.clone(),
        );
        let receipt = svc.submit_order(intent(), None).await.unwrap();
        assert!(!receipt.accepted());
        assert_eq!(receipt.status, TradingCommandStatus::Rejected);
        assert_eq!(receipt.rejection_gate.as_deref(), Some("safety"));
        assert_eq!(sink.receipts().len(), 1);
    }

    #[tokio::test]
    async fn data_health_rejection_records_gate() {
        let sink = Arc::new(InMemoryTradingCommandSink::new());
        let (s, _, r, b, n) = all_allow();
        let svc = service(
            s,
            Arc::new(StaticDataHealth::rejected("stale_market_data")),
            r,
            b,
            n,
            sink.clone(),
        );
        let receipt = svc.submit_order(intent(), None).await.unwrap();
        assert!(!receipt.accepted());
        assert_eq!(receipt.rejection_gate.as_deref(), Some("data_health"));
        assert_eq!(
            receipt.rejection_reason.as_deref(),
            Some("stale_market_data")
        );
    }

    #[tokio::test]
    async fn data_health_error_is_fail_closed() {
        let sink = Arc::new(InMemoryTradingCommandSink::new());
        let (s, _, r, b, n) = all_allow();
        let svc = service(
            s,
            Arc::new(FailingDataHealth("boom".into())),
            r,
            b,
            n,
            sink.clone(),
        );
        let receipt = svc.submit_order(intent(), None).await.unwrap();
        assert!(!receipt.accepted());
        assert_eq!(receipt.rejection_gate.as_deref(), Some("data_health"));
        assert!(
            receipt
                .rejection_reason
                .as_deref()
                .unwrap()
                .contains("boom")
        );
    }

    #[tokio::test]
    async fn risk_rejection_records_reason_and_result() {
        let sink = Arc::new(InMemoryTradingCommandSink::new());
        let (s, dh, _, b, n) = all_allow();
        let svc = service(
            s,
            dh,
            Arc::new(StaticRisk::rejected("max_position_exceeded")),
            b,
            n,
            sink.clone(),
        );
        let receipt = svc.submit_order(intent(), None).await.unwrap();
        assert!(!receipt.accepted());
        assert_eq!(receipt.rejection_gate.as_deref(), Some("risk"));
        assert_eq!(
            receipt.rejection_reason.as_deref(),
            Some("max_position_exceeded")
        );
    }

    #[tokio::test]
    async fn budget_rejection_records_gate() {
        let sink = Arc::new(InMemoryTradingCommandSink::new());
        let (s, dh, r, _, n) = all_allow();
        let svc = service(
            s,
            dh,
            r,
            Arc::new(StaticBudget::rejected("quota_exhausted")),
            n,
            sink.clone(),
        );
        let receipt = svc.submit_order(intent(), None).await.unwrap();
        assert!(!receipt.accepted());
        assert_eq!(receipt.rejection_gate.as_deref(), Some("action_budget"));
        assert_eq!(receipt.rejection_reason.as_deref(), Some("quota_exhausted"));
    }

    #[tokio::test]
    async fn normalizer_failure_is_fail_closed() {
        let sink = Arc::new(InMemoryTradingCommandSink::new());
        let (s, dh, r, b, _) = all_allow();
        let svc = service(
            s,
            dh,
            r,
            b,
            Arc::new(FailingNormalizer("bad_size".into())),
            sink.clone(),
        );
        let receipt = svc.submit_order(intent(), None).await.unwrap();
        assert!(!receipt.accepted());
        assert_eq!(receipt.rejection_gate.as_deref(), Some("normalize"));
    }

    #[tokio::test]
    async fn cancel_order_persists_accepted_command() {
        let sink = Arc::new(InMemoryTradingCommandSink::new());
        let (s, dh, r, b, n) = all_allow();
        let svc = service(s, dh, r, b, n, sink.clone());
        let receipt = svc
            .cancel_order("c1", Some("mm-btc".into()), None)
            .await
            .unwrap();
        assert!(receipt.accepted());
        assert_eq!(receipt.kind, TradingCommandKind::Cancel);
        assert_eq!(receipt.target_cloid.as_deref(), Some("c1"));
        assert_eq!(sink.receipts().len(), 1);
    }

    #[tokio::test]
    async fn cancel_all_persists_symbol_filter() {
        let sink = Arc::new(InMemoryTradingCommandSink::new());
        let (s, dh, r, b, n) = all_allow();
        let svc = service(s, dh, r, b, n, sink.clone());
        let receipt = svc
            .cancel_all_orders(Some("ETH".into()), None, None)
            .await
            .unwrap();
        assert!(receipt.accepted());
        assert_eq!(receipt.kind, TradingCommandKind::CancelAll);
        assert_eq!(receipt.symbol.as_deref(), Some("ETH"));
    }

    #[tokio::test]
    async fn empty_cloid_is_rejected() {
        let sink = Arc::new(InMemoryTradingCommandSink::new());
        let (s, dh, r, b, n) = all_allow();
        let svc = service(s, dh, r, b, n, sink);
        assert!(svc.cancel_order("", None, None).await.is_err());
    }

    #[tokio::test]
    async fn sink_is_idempotent_by_command_id() {
        let sink = InMemoryTradingCommandSink::new();
        let command = TradingCommand {
            command_id: Uuid::new_v4(),
            kind: TradingCommandKind::Place,
            status: TradingCommandStatus::Accepted,
            created_at: Utc::now(),
            intent: Some(intent()),
            target_cloid: None,
            symbol: Some("BTC".into()),
            strategy_id: Some("mm-btc".into()),
            rejection_gate: None,
            rejection_reason: None,
            risk_result: None,
            market_version: Some(1),
            connection_generation: Some(2),
        };
        let first = sink.persist(&command).await.unwrap();
        let second = sink.persist(&command).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(sink.receipts().len(), 1);
    }

    #[tokio::test]
    async fn sink_rejects_conflicting_payload_for_same_id() {
        let sink = InMemoryTradingCommandSink::new();
        let mut command = TradingCommand {
            command_id: Uuid::new_v4(),
            kind: TradingCommandKind::Place,
            status: TradingCommandStatus::Accepted,
            created_at: Utc::now(),
            intent: Some(intent()),
            target_cloid: None,
            symbol: Some("BTC".into()),
            strategy_id: Some("mm-btc".into()),
            rejection_gate: None,
            rejection_reason: None,
            risk_result: None,
            market_version: Some(1),
            connection_generation: Some(2),
        };
        sink.persist(&command).await.unwrap();
        command.status = TradingCommandStatus::Rejected;
        assert!(matches!(
            sink.persist(&command).await,
            Err(HypeEdgeError::TradingCommandConflict { .. })
        ));
    }

    #[tokio::test]
    async fn accepted_placement_uses_normalized_intent() {
        let sink = Arc::new(InMemoryTradingCommandSink::new());
        let (s, dh, r, b, _) = all_allow();
        let normalizer = Arc::new(PassThroughNormalizer);
        let svc = service(s, dh, r, b, normalizer.clone(), sink.clone());
        let receipt = svc.submit_order(intent(), None).await.unwrap();
        // The persisted command carries the normalized intent's market context.
        assert_eq!(receipt.intent.unwrap().cloid.as_deref(), Some("c1"));
    }
}
