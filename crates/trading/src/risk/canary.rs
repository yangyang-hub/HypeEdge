//! Versioned shadow, testnet, and mainnet-canary release gates, port of
//! `src/hypeedge/risk/canary.py`.
//!
//! A pure, fail-closed evaluator: the same decision code serves the API,
//! the strategy supervisor, and deployment checks. Every gate returns a
//! [`GateDecision`] with the concrete reasons that failed — no side effects.

use std::time::Duration;

use hypeedge_domain::decimal::{Decimal, Usd};

/// Most permissive runtime directive allowed by the current evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CanaryDirective {
    Running,
    Paused,
    CancelOnly,
    Halted,
}

impl CanaryDirective {
    pub fn as_str(self) -> &'static str {
        match self {
            CanaryDirective::Running => "running",
            CanaryDirective::Paused => "paused",
            CanaryDirective::CancelOnly => "cancel_only",
            CanaryDirective::Halted => "halted",
        }
    }
}

/// Immutable, versioned limits activated before a mainnet canary starts.
#[derive(Debug, Clone, PartialEq)]
pub struct CanaryRiskEnvelope {
    pub version: i32,
    pub max_deployed_equity: Usd,
    pub max_quote_notional: Usd,
    pub max_daily_loss: Usd,
    pub max_cumulative_loss: Usd,
    pub max_daily_actions: i64,
    pub max_total_actions: i64,
    pub min_action_credits: i64,
    pub min_cancel_headroom: i64,
    pub max_forced_flatten_count: i64,
    pub max_forced_flatten_cost: Usd,
    pub unknown_sla: Duration,
    pub max_duration: Duration,
    pub max_filled_volume: Usd,
}

impl CanaryRiskEnvelope {
    pub fn validate(&self) -> Result<(), String> {
        if self.version <= 0 {
            return Err("canary envelope version must be positive".into());
        }
        let monetary = [
            self.max_deployed_equity.inner(),
            self.max_quote_notional.inner(),
            self.max_daily_loss.inner(),
            self.max_cumulative_loss.inner(),
            self.max_forced_flatten_cost.inner(),
            self.max_filled_volume.inner(),
        ];
        if monetary.iter().any(|d| *d < Decimal::ZERO) {
            return Err("canary monetary limits cannot be negative".into());
        }
        let counts = [
            self.max_daily_actions,
            self.max_total_actions,
            self.min_action_credits,
            self.min_cancel_headroom,
            self.max_forced_flatten_count,
        ];
        if counts.iter().any(|c| *c < 0) {
            return Err("canary count limits cannot be negative".into());
        }
        if self.max_daily_actions > self.max_total_actions {
            return Err("daily actions cannot exceed total actions".into());
        }
        if self.unknown_sla.is_zero() || self.max_duration.is_zero() {
            return Err("canary time limits must be positive".into());
        }
        Ok(())
    }
}

/// Auditable evidence required before moving between deployment stages.
#[derive(Debug, Clone, PartialEq)]
pub struct ReleaseEvidence {
    pub shadow_complete_utc_days: i64,
    pub testnet_clean_utc_days: i64,
    pub reconciliation_diff_count: u64,
    pub duplicate_order_count: u64,
    pub risk_bypass_count: u64,
    pub hard_inventory_breach_count: u64,
    pub unresolved_unknown_count: u64,
    pub pessimistic_net_edge_usdc: Usd,
    pub projected_runway_hours: Decimal,
    pub required_runway_hours: Decimal,
}

/// Authoritative Postgres-derived live canary state.
#[derive(Debug, Clone, PartialEq)]
pub struct CanaryObservation {
    pub observed_at: i64, // unix millis
    pub started_at: i64,  // unix millis
    pub deployed_equity: Usd,
    pub live_quote_notional: Usd,
    pub daily_pnl: Usd,
    pub cumulative_pnl: Usd,
    pub daily_actions: i64,
    pub total_actions: i64,
    pub action_credits: i64,
    pub cancel_headroom: i64,
    pub forced_flatten_count: i64,
    pub forced_flatten_cost: Usd,
    pub oldest_unknown_age: Option<Duration>,
    pub filled_volume: Usd,
    pub reconciliation_healthy: bool,
    pub market_data_healthy: bool,
    pub account_healthy: bool,
}

/// Statistical and operational evidence required before risk expansion.
#[derive(Debug, Clone, PartialEq)]
pub struct ExpansionEvidence {
    pub complete_utc_days: i64,
    pub independent_inventory_episodes: u64,
    pub regime_coverage_complete: bool,
    pub accounting_edge_ci95_lower: Usd,
    pub marginal_usdc_per_action: Usd,
    pub critical_reconciliation_diff_count: u64,
    pub duplicate_order_count: u64,
    pub hard_inventory_breach_count: u64,
    pub unknown_with_terminal_fact_count: u64,
    pub unknown_total_count: u64,
    pub directional_concentration: Decimal,
}

/// The decision of one gate: allowed + the most permissive directive + reasons.
#[derive(Debug, Clone, PartialEq)]
pub struct GateDecision {
    pub allowed: bool,
    pub directive: CanaryDirective,
    pub reasons: Vec<String>,
}

/// The pure fail-closed evaluator.
#[derive(Debug, Clone)]
pub struct CanaryGateEvaluator {
    shadow_min_days: i64,
    testnet_min_days: i64,
    expansion_min_days: i64,
    expansion_min_episodes: u64,
    max_directional_concentration: Decimal,
}

impl Default for CanaryGateEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

impl CanaryGateEvaluator {
    pub fn new() -> Self {
        Self {
            shadow_min_days: 14,
            testnet_min_days: 14,
            expansion_min_days: 30,
            expansion_min_episodes: 30,
            max_directional_concentration: Decimal::from_str_strict("0.50").unwrap(),
        }
    }

    /// Constructor with explicit thresholds; validates the sample minimums and
    /// the directional-concentration bound.
    pub fn with_limits(
        shadow_min_days: i64,
        testnet_min_days: i64,
        expansion_min_days: i64,
        expansion_min_episodes: u64,
        max_directional_concentration: Decimal,
    ) -> Result<Self, String> {
        if shadow_min_days <= 0 || testnet_min_days <= 0 || expansion_min_days <= 0 || expansion_min_episodes == 0 {
            return Err("release-gate sample minimums must be positive".into());
        }
        if max_directional_concentration <= Decimal::ZERO
            || max_directional_concentration > Decimal::ONE
        {
            return Err("directional concentration limit must be in (0, 1]".into());
        }
        Ok(Self {
            shadow_min_days,
            testnet_min_days,
            expansion_min_days,
            expansion_min_episodes,
            max_directional_concentration,
        })
    }

    /// Whether a mainnet canary may start given the release evidence.
    pub fn can_start_canary(&self, evidence: &ReleaseEvidence) -> GateDecision {
        let mut reasons: Vec<String> = Vec::new();
        if evidence.shadow_complete_utc_days < self.shadow_min_days {
            reasons.push("shadow_observation_incomplete".into());
        }
        if evidence.testnet_clean_utc_days < self.testnet_min_days {
            reasons.push("testnet_soak_incomplete".into());
        }
        if evidence.reconciliation_diff_count > 0 {
            reasons.push("reconciliation_diff_present".into());
        }
        if evidence.duplicate_order_count > 0 {
            reasons.push("duplicate_orders_present".into());
        }
        if evidence.risk_bypass_count > 0 {
            reasons.push("risk_bypass_present".into());
        }
        if evidence.hard_inventory_breach_count > 0 {
            reasons.push("hard_inventory_breach_present".into());
        }
        if evidence.unresolved_unknown_count > 0 {
            reasons.push("unresolved_unknown_present".into());
        }
        if evidence.pessimistic_net_edge_usdc.inner() < Decimal::ZERO {
            reasons.push("pessimistic_edge_negative".into());
        }
        if evidence.projected_runway_hours < evidence.required_runway_hours {
            reasons.push("action_runway_insufficient".into());
        }
        let directive = if reasons.is_empty() {
            CanaryDirective::Running
        } else {
            CanaryDirective::Halted
        };
        GateDecision {
            allowed: reasons.is_empty(),
            directive,
            reasons,
        }
    }

    /// Evaluate one live canary snapshot against its envelope.
    pub fn evaluate_live(&self, envelope: &CanaryRiskEnvelope, observation: &CanaryObservation) -> GateDecision {
        let mut halted: Vec<String> = Vec::new();
        let mut cancel_only: Vec<String> = Vec::new();
        let mut paused: Vec<String> = Vec::new();

        if !observation.reconciliation_healthy {
            halted.push("reconciliation_unhealthy".into());
        }
        if observation.deployed_equity.inner() > envelope.max_deployed_equity.inner() {
            halted.push("deployed_equity_limit".into());
        }
        if -observation.cumulative_pnl.inner() > envelope.max_cumulative_loss.inner() {
            halted.push("cumulative_loss_limit".into());
        }
        if observation.total_actions > envelope.max_total_actions {
            halted.push("total_action_limit".into());
        }
        let elapsed = observation.observed_at - observation.started_at;
        if elapsed > envelope.max_duration.as_millis() as i64 {
            halted.push("maximum_duration".into());
        }
        if observation.filled_volume.inner() > envelope.max_filled_volume.inner() {
            halted.push("filled_volume_limit".into());
        }
        if observation.forced_flatten_count > envelope.max_forced_flatten_count {
            halted.push("forced_flatten_count_limit".into());
        }
        if observation.forced_flatten_cost.inner() > envelope.max_forced_flatten_cost.inner() {
            halted.push("forced_flatten_cost_limit".into());
        }

        if observation.oldest_unknown_age.is_some_and(|age| age > envelope.unknown_sla) {
            cancel_only.push("unknown_sla_exceeded".into());
        }
        if observation.action_credits < envelope.min_action_credits {
            cancel_only.push("action_credits_below_minimum".into());
        }
        if observation.cancel_headroom < envelope.min_cancel_headroom {
            cancel_only.push("cancel_headroom_below_minimum".into());
        }
        if !observation.market_data_healthy || !observation.account_healthy {
            cancel_only.push("runtime_data_unhealthy".into());
        }

        if observation.live_quote_notional.inner() > envelope.max_quote_notional.inner() {
            paused.push("quote_notional_limit".into());
        }
        if -observation.daily_pnl.inner() > envelope.max_daily_loss.inner() {
            paused.push("daily_loss_limit".into());
        }
        if observation.daily_actions > envelope.max_daily_actions {
            paused.push("daily_action_limit".into());
        }

        if !halted.is_empty() {
            let mut reasons = halted;
            reasons.extend(cancel_only);
            reasons.extend(paused);
            return GateDecision {
                allowed: false,
                directive: CanaryDirective::Halted,
                reasons,
            };
        }
        if !cancel_only.is_empty() {
            let mut reasons = cancel_only;
            reasons.extend(paused);
            return GateDecision {
                allowed: false,
                directive: CanaryDirective::CancelOnly,
                reasons,
            };
        }
        if !paused.is_empty() {
            return GateDecision {
                allowed: false,
                directive: CanaryDirective::Paused,
                reasons: paused,
            };
        }
        GateDecision {
            allowed: true,
            directive: CanaryDirective::Running,
            reasons: Vec::new(),
        }
    }

    /// Whether risk may be expanded beyond the canary envelope.
    pub fn can_expand(&self, evidence: &ExpansionEvidence) -> GateDecision {
        let mut reasons: Vec<String> = Vec::new();
        if evidence.complete_utc_days < self.expansion_min_days {
            reasons.push("observation_window_incomplete".into());
        }
        if evidence.independent_inventory_episodes < self.expansion_min_episodes {
            reasons.push("inventory_episode_sample_insufficient".into());
        }
        if !evidence.regime_coverage_complete {
            reasons.push("regime_coverage_incomplete".into());
        }
        if evidence.accounting_edge_ci95_lower.inner() <= Decimal::ZERO {
            reasons.push("accounting_edge_ci_not_positive".into());
        }
        if evidence.marginal_usdc_per_action.inner() < Decimal::from_str_strict("1.25").unwrap() {
            reasons.push("marginal_usdc_per_action_below_gate".into());
        }
        if evidence.critical_reconciliation_diff_count > 0 {
            reasons.push("critical_reconciliation_diff_present".into());
        }
        if evidence.duplicate_order_count > 0 {
            reasons.push("duplicate_orders_present".into());
        }
        if evidence.hard_inventory_breach_count > 0 {
            reasons.push("hard_inventory_breach_present".into());
        }
        if evidence.unknown_with_terminal_fact_count != evidence.unknown_total_count {
            reasons.push("unknown_without_terminal_fact".into());
        }
        if evidence.directional_concentration > self.max_directional_concentration {
            reasons.push("directional_pnl_concentration".into());
        }
        let directive = if reasons.is_empty() {
            CanaryDirective::Running
        } else {
            CanaryDirective::Halted
        };
        GateDecision {
            allowed: reasons.is_empty(),
            directive,
            reasons,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usd(v: &str) -> Usd {
        Usd::new(Decimal::from_str_strict(v).unwrap())
    }

    fn dec(v: &str) -> Decimal {
        Decimal::from_str_strict(v).unwrap()
    }

    fn valid_envelope() -> CanaryRiskEnvelope {
        let e = CanaryRiskEnvelope {
            version: 1,
            max_deployed_equity: usd("100000"),
            max_quote_notional: usd("50000"),
            max_daily_loss: usd("500"),
            max_cumulative_loss: usd("1500"),
            max_daily_actions: 100,
            max_total_actions: 500,
            min_action_credits: 1000,
            min_cancel_headroom: 50,
            max_forced_flatten_count: 3,
            max_forced_flatten_cost: usd("200"),
            unknown_sla: Duration::from_secs(300),
            max_duration: Duration::from_secs(30 * 24 * 3600),
            max_filled_volume: usd("2000000"),
        };
        e.validate().unwrap();
        e
    }

    fn healthy_observation() -> CanaryObservation {
        CanaryObservation {
            observed_at: 1_700_000_000_000,
            started_at: 1_699_000_000_000, // ~2.8h ago, well under the 30d max
            deployed_equity: usd("50000"),
            live_quote_notional: usd("10000"),
            daily_pnl: usd("100"),
            cumulative_pnl: usd("400"),
            daily_actions: 20,
            total_actions: 80,
            action_credits: 9000,
            cancel_headroom: 500,
            forced_flatten_count: 0,
            forced_flatten_cost: usd("0"),
            oldest_unknown_age: None,
            filled_volume: usd("100000"),
            reconciliation_healthy: true,
            market_data_healthy: true,
            account_healthy: true,
        }
    }

    #[test]
    fn envelope_validates_limits() {
        // Invalid version.
        let mut e = valid_envelope();
        e.version = 0;
        assert!(e.validate().is_err());
        // daily > total.
        let mut e = valid_envelope();
        e.max_daily_actions = 1000;
        assert!(e.validate().is_err());
        // Negative monetary.
        let mut e = valid_envelope();
        e.max_daily_loss = usd("-1");
        assert!(e.validate().is_err());
        // Zero duration.
        let mut e = valid_envelope();
        e.unknown_sla = Duration::ZERO;
        assert!(e.validate().is_err());
        // Valid passes.
        assert!(valid_envelope().validate().is_ok());
    }

    #[test]
    fn evaluator_constructor_validates() {
        assert!(CanaryGateEvaluator::with_limits(0, 14, 30, 30, dec("0.5")).is_err());
        assert!(CanaryGateEvaluator::with_limits(14, 14, 30, 30, dec("0")).is_err());
        assert!(CanaryGateEvaluator::with_limits(14, 14, 30, 30, dec("1.5")).is_err());
        assert!(CanaryGateEvaluator::with_limits(14, 14, 30, 30, dec("0.5")).is_ok());
    }

    #[test]
    fn can_start_canary_passes_when_evidence_clean() {
        let evaluator = CanaryGateEvaluator::new();
        let evidence = ReleaseEvidence {
            shadow_complete_utc_days: 14,
            testnet_clean_utc_days: 14,
            reconciliation_diff_count: 0,
            duplicate_order_count: 0,
            risk_bypass_count: 0,
            hard_inventory_breach_count: 0,
            unresolved_unknown_count: 0,
            pessimistic_net_edge_usdc: usd("100"),
            projected_runway_hours: dec("48"),
            required_runway_hours: dec("24"),
        };
        let decision = evaluator.can_start_canary(&evidence);
        assert!(decision.allowed);
        assert_eq!(decision.directive, CanaryDirective::Running);
        assert!(decision.reasons.is_empty());
    }

    #[test]
    fn can_start_canary_halts_on_shortfalls() {
        let evaluator = CanaryGateEvaluator::new();
        let evidence = ReleaseEvidence {
            shadow_complete_utc_days: 7,   // < 14
            testnet_clean_utc_days: 14,
            reconciliation_diff_count: 1,  // diff present
            duplicate_order_count: 0,
            risk_bypass_count: 1,          // bypass present
            hard_inventory_breach_count: 0,
            unresolved_unknown_count: 2,   // unknown present
            pessimistic_net_edge_usdc: usd("-5"), // negative edge
            projected_runway_hours: dec("12"),
            required_runway_hours: dec("24"), // insufficient runway
        };
        let decision = evaluator.can_start_canary(&evidence);
        assert!(!decision.allowed);
        assert_eq!(decision.directive, CanaryDirective::Halted);
        for reason in ["shadow_observation_incomplete", "reconciliation_diff_present", "risk_bypass_present", "unresolved_unknown_present", "pessimistic_edge_negative", "action_runway_insufficient"] {
            assert!(decision.reasons.iter().any(|r| r == reason), "missing reason {reason}: {:?}", decision.reasons);
        }
    }

    #[test]
    fn evaluate_live_running_when_within_envelope() {
        let evaluator = CanaryGateEvaluator::new();
        let decision = evaluator.evaluate_live(&valid_envelope(), &healthy_observation());
        assert!(decision.allowed);
        assert_eq!(decision.directive, CanaryDirective::Running);
        assert!(decision.reasons.is_empty());
    }

    #[test]
    fn evaluate_live_halts_on_hard_limits() {
        let evaluator = CanaryGateEvaluator::new();
        let mut obs = healthy_observation();
        obs.cumulative_pnl = usd("-2000"); // > 1500 cumulative loss
        obs.total_actions = 600;           // > 500 total actions
        let decision = evaluator.evaluate_live(&valid_envelope(), &obs);
        assert_eq!(decision.directive, CanaryDirective::Halted);
        assert!(decision.reasons.iter().any(|r| r == "cumulative_loss_limit"));
        assert!(decision.reasons.iter().any(|r| r == "total_action_limit"));
    }

    #[test]
    fn evaluate_live_cancel_only_on_sla_and_credits() {
        let evaluator = CanaryGateEvaluator::new();
        let mut obs = healthy_observation();
        obs.oldest_unknown_age = Some(Duration::from_secs(600)); // > 300s SLA
        obs.action_credits = 100;                                 // < 1000
        obs.cancel_headroom = 10;                                 // < 50
        obs.market_data_healthy = false;
        let decision = evaluator.evaluate_live(&valid_envelope(), &obs);
        assert_eq!(decision.directive, CanaryDirective::CancelOnly);
        for reason in ["unknown_sla_exceeded", "action_credits_below_minimum", "cancel_headroom_below_minimum", "runtime_data_unhealthy"] {
            assert!(decision.reasons.iter().any(|r| r == reason), "missing {reason}");
        }
    }

    #[test]
    fn evaluate_live_paused_on_soft_limits() {
        let evaluator = CanaryGateEvaluator::new();
        let mut obs = healthy_observation();
        obs.live_quote_notional = usd("60000"); // > 50000
        obs.daily_pnl = usd("-600");            // > -500 daily loss
        obs.daily_actions = 150;                // > 100
        let decision = evaluator.evaluate_live(&valid_envelope(), &obs);
        assert_eq!(decision.directive, CanaryDirective::Paused);
        for reason in ["quote_notional_limit", "daily_loss_limit", "daily_action_limit"] {
            assert!(decision.reasons.iter().any(|r| r == reason), "missing {reason}");
        }
    }

    #[test]
    fn evaluate_live_halts_takes_precedence_over_paused() {
        let evaluator = CanaryGateEvaluator::new();
        let mut obs = healthy_observation();
        obs.daily_pnl = usd("-600"); // paused-level
        obs.cumulative_pnl = usd("-2000"); // halted-level
        let decision = evaluator.evaluate_live(&valid_envelope(), &obs);
        assert_eq!(decision.directive, CanaryDirective::Halted);
        assert!(decision.reasons.iter().any(|r| r == "cumulative_loss_limit"));
        assert!(decision.reasons.iter().any(|r| r == "daily_loss_limit"));
    }

    #[test]
    fn evaluate_live_max_duration_elapsed_halts() {
        let evaluator = CanaryGateEvaluator::new();
        let mut obs = healthy_observation();
        // Canary started 31 days ago; max_duration is 30 days.
        obs.started_at = obs.observed_at - (31 * 24 * 3600 * 1000);
        let decision = evaluator.evaluate_live(&valid_envelope(), &obs);
        assert_eq!(decision.directive, CanaryDirective::Halted);
        assert!(decision.reasons.iter().any(|r| r == "maximum_duration"));
    }

    #[test]
    fn can_expand_passes_with_strong_evidence() {
        let evaluator = CanaryGateEvaluator::new();
        let evidence = ExpansionEvidence {
            complete_utc_days: 30,
            independent_inventory_episodes: 30,
            regime_coverage_complete: true,
            accounting_edge_ci95_lower: usd("2"),
            marginal_usdc_per_action: usd("1.5"),
            critical_reconciliation_diff_count: 0,
            duplicate_order_count: 0,
            hard_inventory_breach_count: 0,
            unknown_with_terminal_fact_count: 0,
            unknown_total_count: 0,
            directional_concentration: dec("0.3"),
        };
        let decision = evaluator.can_expand(&evidence);
        assert!(decision.allowed);
        assert_eq!(decision.directive, CanaryDirective::Running);
    }

    #[test]
    fn can_expand_halts_on_weak_evidence() {
        let evaluator = CanaryGateEvaluator::new();
        let evidence = ExpansionEvidence {
            complete_utc_days: 10,   // < 30
            independent_inventory_episodes: 5, // < 30
            regime_coverage_complete: false,
            accounting_edge_ci95_lower: usd("0"), // not positive
            marginal_usdc_per_action: usd("1.0"), // < 1.25
            critical_reconciliation_diff_count: 1,
            duplicate_order_count: 0,
            hard_inventory_breach_count: 0,
            unknown_with_terminal_fact_count: 1, // != total 2
            unknown_total_count: 2,
            directional_concentration: dec("0.6"), // > 0.5
        };
        let decision = evaluator.can_expand(&evidence);
        assert!(!decision.allowed);
        assert_eq!(decision.directive, CanaryDirective::Halted);
        for reason in ["observation_window_incomplete", "inventory_episode_sample_insufficient", "regime_coverage_incomplete", "accounting_edge_ci_not_positive", "marginal_usdc_per_action_below_gate", "critical_reconciliation_diff_present", "unknown_without_terminal_fact", "directional_pnl_concentration"] {
            assert!(decision.reasons.iter().any(|r| r == reason), "missing {reason}");
        }
    }

    #[test]
    fn directive_as_str() {
        assert_eq!(CanaryDirective::Running.as_str(), "running");
        assert_eq!(CanaryDirective::Paused.as_str(), "paused");
        assert_eq!(CanaryDirective::CancelOnly.as_str(), "cancel_only");
        assert_eq!(CanaryDirective::Halted.as_str(), "halted");
    }
}
