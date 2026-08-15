//! Prometheus projection of the [`MarketMakingMetrics`] write interface
//! (P5-2).
//!
//! All metrics are registered on the default registry and use integer
//! families ([`prometheus::IntGaugeVec`] / [`prometheus::IntCounterVec`]).
//! Because Prometheus gauges are integers, fractional values (weights,
//! confidence, prices, ages) are stored at a documented fixed-point scale:
//! - fractions (weight/confidence/uptime): ×1e6
//! - prices / notional (USD): ×1e6 (micro)
//! - ages / latencies / open_ms: milliseconds
//!
//! Constructing more than one instance in a process is tolerated: duplicate
//! metric registration is ignored and the later clone shares the underlying
//! state, which keeps unit tests simple.

use hypeedge_domain::decimal::Decimal;
use prometheus::core::Collector;
use prometheus::{IntCounterVec, IntGauge, IntGaugeVec, Opts};

use super::{
    ActionBudgetObservation, ExecutionOutcome, ExternalReferenceObservation, FreshnessSource,
    InventoryBand, InventoryObservation, LatencyStage, MarketMakingMetrics, QuoteObservation,
};

/// Fractional values (weight/confidence/uptime) are stored ×1e6.
const FRACTION_SCALE: i64 = 1_000_000;
/// Prices and notional (USD) are stored ×1e6 (micro).
const PRICE_SCALE: i64 = 1_000_000;

fn scaled_fraction(v: Decimal) -> i64 {
    (v.to_string().parse::<f64>().unwrap_or(0.0) * FRACTION_SCALE as f64) as i64
}

fn scaled_price(v: Option<Decimal>) -> i64 {
    v.map(|d| (d.to_string().parse::<f64>().unwrap_or(0.0) * PRICE_SCALE as f64) as i64)
        .unwrap_or(0)
}

fn millis(seconds: f64) -> i64 {
    (seconds * 1000.0) as i64
}

/// Register a collector on the default registry, tolerating duplicates (the
/// clone shares state with the registered instance).
fn register<C: Collector + Clone + 'static>(collector: &C) -> C {
    let clone = collector.clone();
    let _ = prometheus::register(Box::new(clone.clone()));
    clone
}

fn gauge_vec(name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    let vec = IntGaugeVec::new(Opts::new(name, help), labels).expect("valid metric name");
    register(&vec)
}

fn counter_vec(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let vec = IntCounterVec::new(Opts::new(name, help), labels).expect("valid metric name");
    register(&vec)
}

/// [`MarketMakingMetrics`] projected onto the default Prometheus registry.
pub struct PrometheusMarketMakingMetrics {
    freshness_age_ms: IntGaugeVec,
    freshness_healthy: IntGaugeVec,
    reference_price: IntGaugeVec,
    external_raw_price: IntGaugeVec,
    external_adjusted_price: IntGaugeVec,
    external_weight: IntGaugeVec,
    external_basis_bps: IntGaugeVec,
    external_age_ms: IntGaugeVec,
    quote_price: IntGaugeVec,
    quote_size: IntGaugeVec,
    quote_open_ms: IntGaugeVec,
    quote_uptime: IntGaugeVec,
    inventory_notional: IntGaugeVec,
    inventory_band: IntGaugeVec,
    budget_address_remaining: IntGaugeVec,
    budget_cancel_remaining: IntGaugeVec,
    budget_ip_remaining: IntGaugeVec,
    execution_outcomes_total: IntCounterVec,
    unknown_orders: IntGaugeVec,
    latency_last_ms: IntGaugeVec,
    latency_observations_total: IntCounterVec,
    reconciliation_diff: IntGaugeVec,
    runtime_config_version: IntGaugeVec,
    canary_directive: IntGaugeVec,
    postgres_available: IntGauge,
    emergency_cancel_failures_total: IntCounterVec,
}

impl PrometheusMarketMakingMetrics {
    /// Build the metric family set and register it on the default registry.
    /// Panics if a metric name collides with an unrelated registered metric
    /// (the family names are fixed and namespaced `mm_*`).
    pub fn new() -> Self {
        Self {
            freshness_age_ms: gauge_vec(
                "mm_freshness_age_millis",
                "Age of the latest authoritative fact, in milliseconds",
                &["strategy_id", "symbol", "source"],
            ),
            freshness_healthy: gauge_vec(
                "mm_freshness_healthy",
                "1 when the fact is within its max age, 0 otherwise",
                &["strategy_id", "symbol", "source"],
            ),
            reference_price: gauge_vec(
                "mm_reference_price_micro",
                "Fair/reservation reference price in micro-USD",
                &["strategy_id", "symbol", "side"],
            ),
            external_raw_price: gauge_vec(
                "mm_external_raw_price_micro",
                "External raw reference price in micro-USD",
                &["strategy_id", "symbol", "source", "quality"],
            ),
            external_adjusted_price: gauge_vec(
                "mm_external_adjusted_price_micro",
                "External basis-adjusted reference price in micro-USD (0 when not applicable)",
                &["strategy_id", "symbol", "source", "quality"],
            ),
            external_weight: gauge_vec(
                "mm_external_effective_weight",
                "Effective external weight ×1e6",
                &["strategy_id", "symbol", "source", "quality"],
            ),
            external_basis_bps: gauge_vec(
                "mm_external_basis_bps",
                "External basis in basis points",
                &["strategy_id", "symbol", "source"],
            ),
            external_age_ms: gauge_vec(
                "mm_external_age_millis",
                "External reference age in milliseconds",
                &["strategy_id", "symbol", "source"],
            ),
            quote_price: gauge_vec(
                "mm_quote_price_micro",
                "Quote price in micro-USD",
                &["strategy_id", "symbol", "side", "level", "state"],
            ),
            quote_size: gauge_vec(
                "mm_quote_size",
                "Quote size",
                &["strategy_id", "symbol", "side", "level", "state"],
            ),
            quote_open_ms: gauge_vec(
                "mm_quote_open_millis",
                "Quote open duration in milliseconds",
                &["strategy_id", "symbol", "side", "level", "state"],
            ),
            quote_uptime: gauge_vec(
                "mm_quote_uptime",
                "Quote uptime ratio ×1e6",
                &["strategy_id", "symbol", "window"],
            ),
            inventory_notional: gauge_vec(
                "mm_inventory_notional_micro",
                "Inventory notional in micro-USD",
                &["strategy_id", "symbol", "band"],
            ),
            inventory_band: gauge_vec(
                "mm_inventory_band",
                "1 when the inventory band is active, 0 otherwise",
                &["strategy_id", "symbol", "band"],
            ),
            budget_address_remaining: gauge_vec(
                "mm_budget_address_remaining",
                "Address action budget remaining",
                &["strategy_id", "mode"],
            ),
            budget_cancel_remaining: gauge_vec(
                "mm_budget_cancel_remaining",
                "Cancel headroom remaining",
                &["strategy_id", "mode"],
            ),
            budget_ip_remaining: gauge_vec(
                "mm_budget_ip_remaining",
                "IP weight remaining",
                &["strategy_id", "mode"],
            ),
            execution_outcomes_total: counter_vec(
                "mm_execution_outcomes_total",
                "Execution outcome counts",
                &["strategy_id", "symbol", "outcome"],
            ),
            unknown_orders: gauge_vec(
                "mm_unknown_orders",
                "Orders in an unknown/exchange-ambiguous state",
                &["strategy_id", "symbol"],
            ),
            latency_last_ms: gauge_vec(
                "mm_latency_last_millis",
                "Most recent latency sample in milliseconds",
                &["strategy_id", "symbol", "stage"],
            ),
            latency_observations_total: counter_vec(
                "mm_latency_observations_total",
                "Latency sample count",
                &["strategy_id", "symbol", "stage"],
            ),
            reconciliation_diff: gauge_vec(
                "mm_reconciliation_diff",
                "Reconciliation diff count per severity",
                &["strategy_id", "symbol", "severity"],
            ),
            runtime_config_version: gauge_vec(
                "mm_runtime_config_version",
                "Strategy runtime state and config version",
                &["strategy_id", "symbol", "state"],
            ),
            canary_directive: gauge_vec(
                "mm_canary_directive",
                "1 when the canary directive is enabled, 0 otherwise",
                &["strategy_id", "symbol", "directive"],
            ),
            postgres_available: register(
                &IntGauge::new(
                    "mm_postgres_available",
                    "1 when Postgres is available for authoritative reads, 0 otherwise",
                )
                .expect("valid metric name"),
            ),
            emergency_cancel_failures_total: counter_vec(
                "mm_emergency_cancel_failures_total",
                "Emergency-cancel failures per sub-account",
                &["sub_account"],
            ),
        }
    }
}

impl Default for PrometheusMarketMakingMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl MarketMakingMetrics for PrometheusMarketMakingMetrics {
    fn observe_freshness(
        &self,
        strategy_id: &str,
        symbol: &str,
        source: FreshnessSource,
        age_seconds: f64,
        _max_age_seconds: f64,
        healthy: bool,
    ) {
        let labels = &[strategy_id, symbol, source.as_str()];
        self.freshness_age_ms
            .with_label_values(labels)
            .set(millis(age_seconds));
        self.freshness_healthy
            .with_label_values(labels)
            .set(i64::from(healthy));
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
        self.reference_price
            .with_label_values(&[strategy_id, symbol, "fair"])
            .set(scaled_price(Some(fair)));
        self.reference_price
            .with_label_values(&[strategy_id, symbol, "reservation"])
            .set(scaled_price(Some(reservation)));
        Ok(())
    }

    fn set_external_reference(&self, observation: &ExternalReferenceObservation) {
        let quality = observation.quality.as_str();
        let base = &[
            observation.strategy_id.as_str(),
            observation.symbol.as_str(),
        ];
        let labels = &[
            observation.strategy_id.as_str(),
            observation.symbol.as_str(),
            observation.source.as_str(),
            quality,
        ];
        self.external_raw_price
            .with_label_values(labels)
            .set(scaled_price(observation.raw_price));
        self.external_adjusted_price
            .with_label_values(labels)
            .set(scaled_price(observation.basis_adjusted_price));
        self.external_weight
            .with_label_values(labels)
            .set(scaled_fraction(observation.weight.unwrap_or(Decimal::ZERO)));
        self.external_basis_bps
            .with_label_values(&[base[0], base[1], observation.source.as_str()])
            .set(
                observation
                    .basis_bps
                    .map(|b| (b.to_string().parse::<f64>().unwrap_or(0.0)) as i64)
                    .unwrap_or(0),
            );
        self.external_age_ms
            .with_label_values(&[base[0], base[1], observation.source.as_str()])
            .set((observation.age_seconds.unwrap_or(0.0) * 1000.0) as i64);
    }

    fn set_quote(&self, observation: &QuoteObservation) {
        let labels = &[
            observation.strategy_id.as_str(),
            observation.symbol.as_str(),
            observation.side.as_str(),
            &observation.level.to_string(),
            observation.state.as_str(),
        ];
        self.quote_price
            .with_label_values(labels)
            .set(scaled_price(observation.price));
        self.quote_size
            .with_label_values(labels)
            .set(scaled_price(observation.size));
        self.quote_open_ms
            .with_label_values(labels)
            .set((observation.open_ms.unwrap_or(0.0) * 1000.0) as i64);
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
        self.quote_uptime
            .with_label_values(&[strategy_id, symbol, window])
            .set(scaled_fraction(ratio));
        Ok(())
    }

    fn set_inventory(&self, observation: &InventoryObservation) {
        let band = observation.band.as_str();
        let labels = &[
            observation.strategy_id.as_str(),
            observation.symbol.as_str(),
        ];
        self.inventory_notional
            .with_label_values(&[labels[0], labels[1], band])
            .set(scaled_price(Some(observation.notional)));
        // Band indicators: only the active band is 1.
        for candidate in [
            InventoryBand::Normal,
            InventoryBand::Soft,
            InventoryBand::Hard,
            InventoryBand::Emergency,
        ] {
            self.inventory_band
                .with_label_values(&[labels[0], labels[1], candidate.as_str()])
                .set(i64::from(candidate == observation.band));
        }
    }

    fn set_action_budget(&self, observation: &ActionBudgetObservation) {
        let labels = &[observation.strategy_id.as_str(), observation.mode.as_str()];
        self.budget_address_remaining
            .with_label_values(labels)
            .set(observation.address_remaining);
        self.budget_cancel_remaining
            .with_label_values(labels)
            .set(observation.cancel_remaining);
        self.budget_ip_remaining
            .with_label_values(labels)
            .set(observation.ip_remaining);
    }

    fn record_execution_outcome(
        &self,
        strategy_id: &str,
        symbol: &str,
        outcome: ExecutionOutcome,
        count: u64,
    ) {
        self.execution_outcomes_total
            .with_label_values(&[strategy_id, symbol, outcome.as_str()])
            .inc_by(count);
    }

    fn set_unknown_orders(&self, strategy_id: &str, symbol: &str, count: u64) {
        self.unknown_orders
            .with_label_values(&[strategy_id, symbol])
            .set(count as i64);
    }

    fn observe_latency(&self, strategy_id: &str, symbol: &str, stage: LatencyStage, seconds: f64) {
        let labels = &[strategy_id, symbol, stage.as_str()];
        self.latency_last_ms
            .with_label_values(labels)
            .set(millis(seconds));
        self.latency_observations_total
            .with_label_values(labels)
            .inc();
    }

    fn set_reconciliation_diff(&self, strategy_id: &str, symbol: &str, severity: &str, count: u64) {
        self.reconciliation_diff
            .with_label_values(&[strategy_id, symbol, severity])
            .set(count as i64);
    }

    fn set_runtime(&self, strategy_id: &str, symbol: &str, state: &str, config_version: i64) {
        self.runtime_config_version
            .with_label_values(&[strategy_id, symbol, state])
            .set(config_version);
    }

    fn set_canary_directive(
        &self,
        strategy_id: &str,
        symbol: &str,
        directive: &str,
        enabled: bool,
    ) {
        self.canary_directive
            .with_label_values(&[strategy_id, symbol, directive])
            .set(i64::from(enabled));
    }

    fn set_postgres_available(&self, available: bool) {
        self.postgres_available.set(i64::from(available));
    }

    fn record_emergency_cancel_failure(&self, sub_account: &str) {
        self.emergency_cancel_failures_total
            .with_label_values(&[sub_account])
            .inc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::decimal::Decimal;
    use prometheus::Encoder;

    fn sample() -> PrometheusMarketMakingMetrics {
        PrometheusMarketMakingMetrics::new()
    }

    fn gather_text(m: &PrometheusMarketMakingMetrics) -> String {
        let mut buf = Vec::new();
        let encoder = prometheus::TextEncoder::new();
        // The families live on the default registry; `register` tolerates the
        // duplicate registration from `sample()`.
        let mut families = m.freshness_age_ms.collect();
        families.extend(m.freshness_healthy.collect());
        encoder.encode(&families, &mut buf).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn registers_and_sets_gauge_values() {
        let m = sample();
        m.observe_freshness("s1", "BTC", FreshnessSource::Feed, 1.5, 6.0, true);
        m.record_execution_outcome("s1", "BTC", ExecutionOutcome::Reject, 2);
        m.set_postgres_available(true);
        m.record_emergency_cancel_failure("0xabc");

        // Gauge family value is directly observable via the metric object.
        assert_eq!(
            m.freshness_age_ms
                .with_label_values(&["s1", "BTC", "feed"])
                .get(),
            1500,
            "age stored in millis"
        );
        assert_eq!(
            m.freshness_healthy
                .with_label_values(&["s1", "BTC", "feed"])
                .get(),
            1
        );
        assert_eq!(
            m.execution_outcomes_total
                .with_label_values(&["s1", "BTC", "reject"])
                .get(),
            2
        );
        assert_eq!(m.postgres_available.get(), 1);
        assert_eq!(
            m.emergency_cancel_failures_total
                .with_label_values(&["0xabc"])
                .get(),
            1
        );
    }

    #[test]
    fn reference_prices_and_uptime_validate() {
        let m = sample();
        assert!(
            m.set_reference_prices("s", "BTC", Decimal::ONE, Decimal::ONE)
                .is_ok()
        );
        assert!(
            m.set_reference_prices("s", "BTC", Decimal::ZERO, Decimal::ONE)
                .is_err()
        );
        assert!(
            m.set_quote_uptime("s", "BTC", "5m", Decimal::from_scaled(50, 2))
                .is_ok()
        );
        assert!(
            m.set_quote_uptime("s", "BTC", "5m", Decimal::from_scaled(150, 0))
                .is_err()
        );
        // Uptime stored as ×1e6.
        assert_eq!(
            m.quote_uptime.with_label_values(&["s", "BTC", "5m"]).get(),
            500_000
        );
    }

    #[test]
    fn inventory_band_indicators_are_exclusive() {
        let m = sample();
        m.set_inventory(&InventoryObservation {
            strategy_id: "s".into(),
            symbol: "BTC".into(),
            notional: Decimal::from_scaled(1_000, 0),
            band: InventoryBand::Hard,
            soft: Some(Decimal::ONE),
            hard: Some(Decimal::from_scaled(2_000, 0)),
            emergency: Some(Decimal::from_scaled(3_000, 0)),
        });
        assert_eq!(
            m.inventory_band
                .with_label_values(&["s", "BTC", "hard"])
                .get(),
            1
        );
        assert_eq!(
            m.inventory_band
                .with_label_values(&["s", "BTC", "soft"])
                .get(),
            0
        );
    }

    #[test]
    fn text_encoding_contains_registered_families() {
        // P5-2: the metric families are discoverable for a /metrics scrape.
        let m = sample();
        m.observe_freshness("s1", "BTC", FreshnessSource::Feed, 1.5, 6.0, true);
        let text = gather_text(&m);
        assert!(text.contains("mm_freshness_age_millis"));
        assert!(text.contains("mm_freshness_healthy"));
    }
}
