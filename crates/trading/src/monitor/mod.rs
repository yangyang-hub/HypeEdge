//! Bounded-cardinality operational projection for market-making health, port
//! of `src/hypeedge/monitor/market_making.py`, plus structured alerts
//! (`src/hypeedge/monitor/alerts.py`).
//!
//! The metrics here are operational projections only. Callers must derive
//! values from authoritative runtime/Postgres facts; Prometheus and ClickHouse
//! are never used as order, PnL, quota, or configuration truth. This crate
//! stays prometheus-free: [`MarketMakingMetrics`] is the write interface, and
//! an in-memory implementation records observations for tests and wiring.

pub mod alerts;

use hypeedge_domain::decimal::Decimal;

pub use alerts::{AlertDispatcher, AlertPayload, AlertSeverity, LogAlertDispatcher};

/// The freshness source for one market-making fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FreshnessSource {
    Feed,
    UserStream,
    Account,
    Credit,
    ExternalReference,
}

impl FreshnessSource {
    pub fn as_str(self) -> &'static str {
        match self {
            FreshnessSource::Feed => "feed",
            FreshnessSource::UserStream => "user_stream",
            FreshnessSource::Account => "account",
            FreshnessSource::Credit => "credit",
            FreshnessSource::ExternalReference => "external_reference",
        }
    }
}

/// Quality of an external reference price source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExternalReferenceQuality {
    Healthy,
    Degraded,
    Stale,
    Disabled,
}

impl ExternalReferenceQuality {
    pub fn as_str(self) -> &'static str {
        match self {
            ExternalReferenceQuality::Healthy => "healthy",
            ExternalReferenceQuality::Degraded => "degraded",
            ExternalReferenceQuality::Stale => "stale",
            ExternalReferenceQuality::Disabled => "disabled",
        }
    }
}

/// Inventory position band relative to the configured soft/hard/emergency
/// notional limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InventoryBand {
    Normal,
    Soft,
    Hard,
    Emergency,
}

impl InventoryBand {
    pub fn as_str(self) -> &'static str {
        match self {
            InventoryBand::Normal => "normal",
            InventoryBand::Soft => "soft",
            InventoryBand::Hard => "hard",
            InventoryBand::Emergency => "emergency",
        }
    }
}

/// The execution outcome class recorded for telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionOutcome {
    Submit,
    Cancel,
    Modify,
    Reject,
    Unknown,
    BatchPartial,
}

impl ExecutionOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            ExecutionOutcome::Submit => "submit",
            ExecutionOutcome::Cancel => "cancel",
            ExecutionOutcome::Modify => "modify",
            ExecutionOutcome::Reject => "reject",
            ExecutionOutcome::Unknown => "unknown",
            ExecutionOutcome::BatchPartial => "batch_partial",
        }
    }
}

/// The latency stage measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LatencyStage {
    ReceiptToDecision,
    DecisionToSend,
    Ack,
    Cancel,
    EventLoopLag,
}

impl LatencyStage {
    pub fn as_str(self) -> &'static str {
        match self {
            LatencyStage::ReceiptToDecision => "receipt_to_decision",
            LatencyStage::DecisionToSend => "decision_to_send",
            LatencyStage::Ack => "ack",
            LatencyStage::Cancel => "cancel",
            LatencyStage::EventLoopLag => "event_loop_lag",
        }
    }
}

/// A single external-reference observation to project.
#[derive(Debug, Clone, PartialEq)]
pub struct ExternalReferenceObservation {
    pub strategy_id: String,
    pub symbol: String,
    pub source: String,
    pub quality: ExternalReferenceQuality,
    pub raw_price: Option<Decimal>,
    pub basis_adjusted_price: Option<Decimal>,
    pub basis_bps: Option<Decimal>,
    pub basis_limit_bps: Option<Decimal>,
    pub divergence_bps: Option<Decimal>,
    pub divergence_limit_bps: Option<Decimal>,
    pub weight: Option<Decimal>,
    pub age_seconds: Option<f64>,
}

/// One quote-level observation.
#[derive(Debug, Clone, PartialEq)]
pub struct QuoteObservation {
    pub strategy_id: String,
    pub symbol: String,
    pub side: String,
    pub level: u32,
    pub state: String,
    pub price: Option<Decimal>,
    pub size: Option<Decimal>,
    pub open_ms: Option<f64>,
}

/// One inventory observation.
#[derive(Debug, Clone, PartialEq)]
pub struct InventoryObservation {
    pub strategy_id: String,
    pub symbol: String,
    pub notional: Decimal,
    pub band: InventoryBand,
    pub soft: Option<Decimal>,
    pub hard: Option<Decimal>,
    pub emergency: Option<Decimal>,
}

/// One action-budget observation.
#[derive(Debug, Clone, PartialEq)]
pub struct ActionBudgetObservation {
    pub strategy_id: String,
    pub symbol: Option<String>,
    pub mode: String,
    pub address_remaining: i64,
    pub cancel_remaining: i64,
    pub ip_remaining: i64,
    /// `(strategy_id, symbol, soft_limit, hard_limit)` allocation rows.
    pub allocations: Vec<BudgetAllocationRow>,
}

/// One per-strategy allocation row in an [`ActionBudgetObservation`].
#[derive(Debug, Clone, PartialEq)]
pub struct BudgetAllocationRow {
    pub strategy_id: String,
    pub symbol: String,
    pub soft_limit: i64,
    pub hard_limit: i64,
}

/// Explicit write interface for market-making operational telemetry.
pub trait MarketMakingMetrics: Send + Sync {
    /// Record the age of the latest authoritative fact for a dimension.
    fn observe_freshness(
        &self,
        strategy_id: &str,
        symbol: &str,
        source: FreshnessSource,
        age_seconds: f64,
        max_age_seconds: f64,
        healthy: bool,
    );

    /// Set fair / reservation reference prices. Both must be positive.
    fn set_reference_prices(
        &self,
        strategy_id: &str,
        symbol: &str,
        fair: Decimal,
        reservation: Decimal,
    ) -> Result<(), String>;

    /// Project one external-reference observation (never an order/oracle fact).
    fn set_external_reference(&self, observation: &ExternalReferenceObservation);

    /// Set the current quote state for a slot.
    fn set_quote(&self, observation: &QuoteObservation);

    /// Set the quote uptime ratio for a window; must be in [0, 1].
    fn set_quote_uptime(
        &self,
        strategy_id: &str,
        symbol: &str,
        window: &str,
        ratio: Decimal,
    ) -> Result<(), String>;

    /// Set the inventory projection for a symbol.
    fn set_inventory(&self, observation: &InventoryObservation);

    /// Set the action-budget projection.
    fn set_action_budget(&self, observation: &ActionBudgetObservation);

    /// Record one execution outcome.
    fn record_execution_outcome(
        &self,
        strategy_id: &str,
        symbol: &str,
        outcome: ExecutionOutcome,
        count: u64,
    );

    /// Set the count of orders in an unknown/exchange-ambiguous state.
    fn set_unknown_orders(&self, strategy_id: &str, symbol: &str, count: u64);

    /// Observe one latency sample.
    fn observe_latency(&self, strategy_id: &str, symbol: &str, stage: LatencyStage, seconds: f64);

    /// Set the reconciliation diff count for a severity.
    fn set_reconciliation_diff(&self, strategy_id: &str, symbol: &str, severity: &str, count: u64);

    /// Set the strategy runtime state and config version.
    fn set_runtime(&self, strategy_id: &str, symbol: &str, state: &str, config_version: i64);

    /// Set the canary directive state.
    fn set_canary_directive(&self, strategy_id: &str, symbol: &str, directive: &str, enabled: bool);

    /// Whether Postgres is available for authoritative reads.
    fn set_postgres_available(&self, available: bool);

    /// Record an emergency-cancel failure for a sub-account.
    fn record_emergency_cancel_failure(&self, sub_account: &str);
}

/// One recorded freshness observation.
type FreshnessRecord = (String, String, FreshnessSource, f64, f64, bool);
/// One recorded reference-price observation.
type ReferencePriceRecord = (String, String, Decimal, Decimal);
/// One recorded quote-uptime observation.
type QuoteUptimeRecord = (String, String, String, Decimal);

/// In-memory [`MarketMakingMetrics`] that records observations in bounded
/// maps — deterministic, testable, and the default when no Prometheus registry
/// is wired.
#[derive(Debug, Default)]
pub struct InMemoryMarketMakingMetrics {
    freshness: std::sync::Mutex<Vec<FreshnessRecord>>,
    reference_prices: std::sync::Mutex<Vec<ReferencePriceRecord>>,
    external: std::sync::Mutex<Vec<ExternalReferenceObservation>>,
    quotes: std::sync::Mutex<Vec<QuoteObservation>>,
    quote_uptime: std::sync::Mutex<Vec<QuoteUptimeRecord>>,
    inventory: std::sync::Mutex<Vec<InventoryObservation>>,
    budget: std::sync::Mutex<Vec<ActionBudgetObservation>>,
    outcomes: std::sync::Mutex<Vec<(String, String, ExecutionOutcome, u64)>>,
    unknown_orders: std::sync::Mutex<Vec<(String, String, u64)>>,
    latency: std::sync::Mutex<Vec<(String, String, LatencyStage, f64)>>,
    reconciliation: std::sync::Mutex<Vec<(String, String, String, u64)>>,
    runtime: std::sync::Mutex<Vec<(String, String, String, i64)>>,
    canary: std::sync::Mutex<Vec<(String, String, String, bool)>>,
    postgres_available: std::sync::Mutex<Vec<bool>>,
    emergency_failures: std::sync::Mutex<Vec<String>>,
}

impl InMemoryMarketMakingMetrics {
    pub fn new() -> Self {
        Self::default()
    }
}

impl MarketMakingMetrics for InMemoryMarketMakingMetrics {
    fn observe_freshness(
        &self,
        strategy_id: &str,
        symbol: &str,
        source: FreshnessSource,
        age_seconds: f64,
        max_age_seconds: f64,
        healthy: bool,
    ) {
        self.freshness.lock().unwrap().push((
            strategy_id.to_string(),
            symbol.to_string(),
            source,
            age_seconds,
            max_age_seconds,
            healthy,
        ));
    }

    fn set_reference_prices(
        &self,
        strategy_id: &str,
        symbol: &str,
        fair: Decimal,
        reservation: Decimal,
    ) -> Result<(), String> {
        if fair <= Decimal::ZERO || reservation <= Decimal::ZERO {
            return Err("reference prices must be positive".into());
        }
        self.reference_prices.lock().unwrap().push((
            strategy_id.to_string(),
            symbol.to_string(),
            fair,
            reservation,
        ));
        Ok(())
    }

    fn set_external_reference(&self, observation: &ExternalReferenceObservation) {
        self.external.lock().unwrap().push(observation.clone());
    }

    fn set_quote(&self, observation: &QuoteObservation) {
        self.quotes.lock().unwrap().push(observation.clone());
    }

    fn set_quote_uptime(
        &self,
        strategy_id: &str,
        symbol: &str,
        window: &str,
        ratio: Decimal,
    ) -> Result<(), String> {
        if ratio < Decimal::ZERO || ratio > Decimal::ONE {
            return Err("quote uptime ratio must be in [0, 1]".into());
        }
        self.quote_uptime.lock().unwrap().push((
            strategy_id.to_string(),
            symbol.to_string(),
            window.to_string(),
            ratio,
        ));
        Ok(())
    }

    fn set_inventory(&self, observation: &InventoryObservation) {
        self.inventory.lock().unwrap().push(observation.clone());
    }

    fn set_action_budget(&self, observation: &ActionBudgetObservation) {
        self.budget.lock().unwrap().push(observation.clone());
    }

    fn record_execution_outcome(
        &self,
        strategy_id: &str,
        symbol: &str,
        outcome: ExecutionOutcome,
        count: u64,
    ) {
        self.outcomes.lock().unwrap().push((
            strategy_id.to_string(),
            symbol.to_string(),
            outcome,
            count,
        ));
    }

    fn set_unknown_orders(&self, strategy_id: &str, symbol: &str, count: u64) {
        self.unknown_orders.lock().unwrap().push((
            strategy_id.to_string(),
            symbol.to_string(),
            count,
        ));
    }

    fn observe_latency(&self, strategy_id: &str, symbol: &str, stage: LatencyStage, seconds: f64) {
        self.latency.lock().unwrap().push((
            strategy_id.to_string(),
            symbol.to_string(),
            stage,
            seconds,
        ));
    }

    fn set_reconciliation_diff(&self, strategy_id: &str, symbol: &str, severity: &str, count: u64) {
        self.reconciliation.lock().unwrap().push((
            strategy_id.to_string(),
            symbol.to_string(),
            severity.to_string(),
            count,
        ));
    }

    fn set_runtime(&self, strategy_id: &str, symbol: &str, state: &str, config_version: i64) {
        self.runtime.lock().unwrap().push((
            strategy_id.to_string(),
            symbol.to_string(),
            state.to_string(),
            config_version,
        ));
    }

    fn set_canary_directive(
        &self,
        strategy_id: &str,
        symbol: &str,
        directive: &str,
        enabled: bool,
    ) {
        self.canary.lock().unwrap().push((
            strategy_id.to_string(),
            symbol.to_string(),
            directive.to_string(),
            enabled,
        ));
    }

    fn set_postgres_available(&self, available: bool) {
        self.postgres_available.lock().unwrap().push(available);
    }

    fn record_emergency_cancel_failure(&self, sub_account: &str) {
        self.emergency_failures
            .lock()
            .unwrap()
            .push(sub_account.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enums_serialize_to_snake_case() {
        assert_eq!(
            FreshnessSource::ExternalReference.as_str(),
            "external_reference"
        );
        assert_eq!(InventoryBand::Emergency.as_str(), "emergency");
        assert_eq!(ExecutionOutcome::BatchPartial.as_str(), "batch_partial");
        assert_eq!(
            LatencyStage::ReceiptToDecision.as_str(),
            "receipt_to_decision"
        );
        assert_eq!(ExternalReferenceQuality::Degraded.as_str(), "degraded");
    }

    #[test]
    fn reference_prices_must_be_positive() {
        let m = InMemoryMarketMakingMetrics::new();
        assert!(
            m.set_reference_prices("s", "BTC", Decimal::ZERO, Decimal::ONE)
                .is_err()
        );
        assert!(
            m.set_reference_prices("s", "BTC", Decimal::ONE, Decimal::ZERO)
                .is_err()
        );
        assert!(
            m.set_reference_prices("s", "BTC", Decimal::ONE, Decimal::ONE)
                .is_ok()
        );
    }

    #[test]
    fn quote_uptime_must_be_in_unit_interval() {
        let m = InMemoryMarketMakingMetrics::new();
        assert!(
            m.set_quote_uptime("s", "BTC", "5m", Decimal::from_scaled(150, 0))
                .is_err()
        );
        assert!(
            m.set_quote_uptime("s", "BTC", "5m", Decimal::from_scaled(50, 2))
                .is_ok()
        );
        assert!(m.set_quote_uptime("s", "BTC", "5m", Decimal::ONE).is_ok());
    }

    #[test]
    fn observations_are_recorded_in_order() {
        let m = InMemoryMarketMakingMetrics::new();
        m.observe_freshness("s1", "BTC", FreshnessSource::Account, 0.5, 6.0, true);
        m.record_execution_outcome("s1", "BTC", ExecutionOutcome::Reject, 2);
        m.set_unknown_orders("s1", "BTC", 1);
        m.set_postgres_available(false);
        m.record_emergency_cancel_failure("0xabc");

        assert_eq!(m.freshness.lock().unwrap().len(), 1);
        assert_eq!(m.outcomes.lock().unwrap()[0].2, ExecutionOutcome::Reject);
        assert_eq!(m.unknown_orders.lock().unwrap()[0].2, 1);
        assert!(!*m.postgres_available.lock().unwrap().last().unwrap());
        assert_eq!(m.emergency_failures.lock().unwrap()[0], "0xabc");
    }

    #[test]
    fn inventory_and_budget_observations() {
        let m = InMemoryMarketMakingMetrics::new();
        m.set_inventory(&InventoryObservation {
            strategy_id: "s".into(),
            symbol: "BTC".into(),
            notional: Decimal::ONE,
            band: InventoryBand::Soft,
            soft: Some(Decimal::ONE),
            hard: None,
            emergency: None,
        });
        m.set_action_budget(&ActionBudgetObservation {
            strategy_id: "s".into(),
            symbol: None,
            mode: "normal".into(),
            address_remaining: 100,
            cancel_remaining: 50,
            ip_remaining: 1000,
            allocations: vec![],
        });
        assert_eq!(m.inventory.lock().unwrap()[0].band, InventoryBand::Soft);
        assert_eq!(m.budget.lock().unwrap()[0].mode, "normal");
    }
}
