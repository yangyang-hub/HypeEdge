//! Pure market-maker inputs and versioned configuration, port of
//! `src/hypeedge/strategy/market_maker/models.py`.

use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::{Decimal, Price, Size, Usd};
use hypeedge_domain::enums::ActionBudgetMode;

/// Market features at a decision point.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketFeatures {
    pub symbol: String,
    pub market_version: i64,
    pub connection_generation: i64,
    pub exchange_ts: i64,
    pub received_at: DateTime<Utc>,
    pub healthy: bool,
    pub best_bid: Price,
    pub best_ask: Price,
    pub best_bid_size: Size,
    pub best_ask_size: Size,
    pub microprice: Price,
    pub normalized_ofi: Decimal,
    pub trade_flow: Decimal,
    pub short_return: Decimal,
    pub return_variance_per_second: Decimal,
    pub expected_adverse_markout_bps: Decimal,
    pub latency_buffer_bps: Decimal,
    pub toxicity: Decimal,
    pub funding_rate: Decimal,
    pub external_source: Option<String>,
    pub external_symbol: Option<String>,
    pub external_raw_price: Option<Price>,
    pub external_adjusted_price: Option<Price>,
    pub external_basis_bps: Decimal,
    pub external_effective_weight: Decimal,
    pub external_confidence: Decimal,
    pub external_age_ms: Option<i64>,
    pub external_quality: String,
    pub external_observed_at: Option<DateTime<Utc>>,
    pub latency_quality: String,
    pub markout_quality: String,
}

impl MarketFeatures {
    pub fn mid_price(&self) -> Price {
        Price::new((self.best_bid.inner() + self.best_ask.inner()).div(Decimal::from_i128(2)))
    }

    /// Validate invariants (mirrors `__post_init__`).
    pub fn validate(&self) -> Result<(), String> {
        if self.market_version < 0 || self.connection_generation < 0 {
            return Err("market versions cannot be negative".into());
        }
        if self.best_bid.inner() <= Decimal::ZERO || self.best_ask.inner() <= self.best_bid.inner()
        {
            return Err("market features require a positive non-crossed book".into());
        }
        if self.best_bid_size.inner() <= Decimal::ZERO
            || self.best_ask_size.inner() <= Decimal::ZERO
        {
            return Err("top-of-book sizes must be positive".into());
        }
        if !(Decimal::ZERO <= self.toxicity && self.toxicity <= Decimal::ONE) {
            return Err("toxicity must be in [0, 1]".into());
        }
        if self.return_variance_per_second < Decimal::ZERO {
            return Err("return variance cannot be negative".into());
        }
        if !(Decimal::ZERO <= self.external_effective_weight
            && self.external_effective_weight <= Decimal::ONE)
        {
            return Err("external effective weight must be in [0, 1]".into());
        }
        if !(Decimal::ZERO <= self.external_confidence && self.external_confidence <= Decimal::ONE)
        {
            return Err("external confidence must be in [0, 1]".into());
        }
        if let Some(age) = self.external_age_ms
            && age < 0
        {
            return Err("external age cannot be negative".into());
        }
        Ok(())
    }
}

/// A minimal external-reference snapshot consumed by the feature engine
/// (the fields the engine actually reads from `ExternalReferenceSnapshot`).
#[derive(Debug, Clone, PartialEq)]
pub struct ExternalReferenceInput {
    pub source: String,
    pub symbol: String,
    pub raw_price: Option<Price>,
    pub adjusted_price: Option<Price>,
    pub basis_bps: Decimal,
    pub effective_weight: Decimal,
    pub confidence: Decimal,
    pub quality: String,
    pub observed_at: DateTime<Utc>,
}

/// Inventory snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct InventorySnapshot {
    pub position_size: Size,
    pub equity: Usd,
    pub available_balance: Usd,
    pub margin_used: Usd,
    pub observed_at: DateTime<Utc>,
    pub healthy: bool,
}

/// Action-budget snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct ActionBudgetSnapshot {
    pub mode: ActionBudgetMode,
    pub address_actions_remaining: i64,
    pub cancel_headroom: i64,
    pub ip_weight_remaining: i64,
    pub action_shadow_cost_usdc: Usd,
    pub observed_at: DateTime<Utc>,
    pub healthy: bool,
}

/// Versioned market-maker configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketMakerConfig {
    pub version: u64,
    pub model_version: String,
    pub tick_size: Decimal,
    pub lot_size: Decimal,
    pub min_size: Decimal,
    pub soft_inventory_notional: Usd,
    pub hard_inventory_notional: Usd,
    pub emergency_inventory_notional: Usd,
    pub quote_size: Size,
    pub max_depth_participation: Decimal,
    pub beta_microprice: Decimal,
    pub beta_ofi_ticks: Decimal,
    pub beta_trade_flow_ticks: Decimal,
    pub beta_short_return_ticks: Decimal,
    pub max_fair_shift_ticks: Decimal,
    pub external_reference_weight: Decimal,
    pub external_basis_alpha: Decimal,
    pub external_max_age_seconds: Decimal,
    pub external_outlier_bps: Decimal,
    pub max_external_shift_ticks: Decimal,
    pub max_total_fair_shift_ticks: Decimal,
    pub latency_risk_multiplier: Decimal,
    pub conservative_latency_seconds: Decimal,
    pub conservative_markout_bps: Decimal,
    pub min_markout_samples: u32,
    pub inventory_skew_bps: Decimal,
    pub inventory_gamma_bps: Decimal,
    pub max_inventory_shift_bps: Decimal,
    pub horizon_seconds: Decimal,
    pub min_half_spread_bps: Decimal,
    pub toxicity_spread_bps: Decimal,
    pub signed_maker_fee_rate: Decimal,
    pub expected_fill_probability: Decimal,
    pub min_expected_pnl_usdc: Usd,
    pub max_quote_lifetime_seconds: Decimal,
}

impl MarketMakerConfig {
    /// The defaults from `models.py`.
    pub fn default_with(
        version: u64,
        tick_size: Decimal,
        lot_size: Decimal,
        min_size: Decimal,
    ) -> Self {
        Self {
            version,
            model_version: "v1".into(),
            tick_size,
            lot_size,
            min_size,
            soft_inventory_notional: Usd::ZERO,
            hard_inventory_notional: Usd::ZERO,
            emergency_inventory_notional: Usd::ZERO,
            quote_size: Size::ZERO,
            max_depth_participation: Decimal::from_str_lenient("0.05").unwrap(),
            beta_microprice: Decimal::from_str_lenient("0.5").unwrap(),
            beta_ofi_ticks: Decimal::from_str_lenient("0.25").unwrap(),
            beta_trade_flow_ticks: Decimal::from_str_lenient("0.25").unwrap(),
            beta_short_return_ticks: Decimal::from_str_lenient("0.25").unwrap(),
            max_fair_shift_ticks: Decimal::from_str_lenient("2").unwrap(),
            external_reference_weight: Decimal::from_str_lenient("0.25").unwrap(),
            external_basis_alpha: Decimal::from_str_lenient("0.02").unwrap(),
            external_max_age_seconds: Decimal::from_str_lenient("0.5").unwrap(),
            external_outlier_bps: Decimal::from_str_lenient("75").unwrap(),
            max_external_shift_ticks: Decimal::from_str_lenient("2").unwrap(),
            max_total_fair_shift_ticks: Decimal::from_str_lenient("3").unwrap(),
            latency_risk_multiplier: Decimal::ONE,
            conservative_latency_seconds: Decimal::from_str_lenient("0.1").unwrap(),
            conservative_markout_bps: Decimal::ONE,
            min_markout_samples: 20,
            inventory_skew_bps: Decimal::from_str_lenient("5").unwrap(),
            inventory_gamma_bps: Decimal::ONE,
            max_inventory_shift_bps: Decimal::from_str_lenient("20").unwrap(),
            horizon_seconds: Decimal::from_str_lenient("5").unwrap(),
            min_half_spread_bps: Decimal::ONE,
            toxicity_spread_bps: Decimal::from_str_lenient("10").unwrap(),
            signed_maker_fee_rate: Decimal::from_str_lenient("-0.0002").unwrap(),
            expected_fill_probability: Decimal::from_str_lenient("0.10").unwrap(),
            min_expected_pnl_usdc: Usd::ZERO,
            max_quote_lifetime_seconds: Decimal::from_str_lenient("10").unwrap(),
        }
    }

    /// Validate invariants (mirrors `__post_init__`).
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("version", Decimal::from_i128(self.version as i128)),
            ("tick_size", self.tick_size),
            ("lot_size", self.lot_size),
            ("min_size", self.min_size),
            (
                "soft_inventory_notional",
                self.soft_inventory_notional.inner(),
            ),
            (
                "hard_inventory_notional",
                self.hard_inventory_notional.inner(),
            ),
            (
                "emergency_inventory_notional",
                self.emergency_inventory_notional.inner(),
            ),
            ("quote_size", self.quote_size.inner()),
            ("horizon_seconds", self.horizon_seconds),
            (
                "max_quote_lifetime_seconds",
                self.max_quote_lifetime_seconds,
            ),
        ] {
            if value <= Decimal::ZERO {
                return Err(format!("{name} must be positive"));
            }
        }
        if !(self.soft_inventory_notional.inner() < self.hard_inventory_notional.inner()
            && self.hard_inventory_notional.inner() < self.emergency_inventory_notional.inner())
        {
            return Err("inventory limits must satisfy soft < hard < emergency".into());
        }
        for (name, value) in [
            ("max_depth_participation", self.max_depth_participation),
            ("expected_fill_probability", self.expected_fill_probability),
        ] {
            if !(Decimal::ZERO < value && value <= Decimal::ONE) {
                return Err(format!("{name} must be in (0, 1]"));
            }
        }
        if self.max_fair_shift_ticks < Decimal::ZERO
            || self.max_external_shift_ticks < Decimal::ZERO
            || self.max_total_fair_shift_ticks < Decimal::ZERO
            || self.max_inventory_shift_bps < Decimal::ZERO
        {
            return Err("fair and inventory shift caps cannot be negative".into());
        }
        for (name, value) in [
            ("external_reference_weight", self.external_reference_weight),
            ("external_basis_alpha", self.external_basis_alpha),
        ] {
            if !(Decimal::ZERO <= value && value <= Decimal::ONE) {
                return Err(format!("{name} must be in [0, 1]"));
            }
        }
        if self.external_max_age_seconds <= Decimal::ZERO
            || self.external_outlier_bps <= Decimal::ZERO
        {
            return Err("external age and outlier limits must be positive".into());
        }
        if self.latency_risk_multiplier < Decimal::ZERO
            || self.conservative_latency_seconds < Decimal::ZERO
        {
            return Err("latency configuration cannot be negative".into());
        }
        if self.conservative_markout_bps < Decimal::ZERO || self.min_markout_samples == 0 {
            return Err(
                "markout configuration must be non-negative with a positive sample count".into(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> MarketMakerConfig {
        let mut c = MarketMakerConfig::default_with(
            1,
            Decimal::from_str_lenient("0.1").unwrap(),
            Decimal::from_str_lenient("0.001").unwrap(),
            Decimal::from_str_lenient("0.001").unwrap(),
        );
        c.soft_inventory_notional = Usd::new(Decimal::from_str_lenient("1000").unwrap());
        c.hard_inventory_notional = Usd::new(Decimal::from_str_lenient("2000").unwrap());
        c.emergency_inventory_notional = Usd::new(Decimal::from_str_lenient("3000").unwrap());
        c.quote_size = Size::new(Decimal::from_str_lenient("1").unwrap());
        c
    }

    #[test]
    fn config_validates() {
        valid_config().validate().unwrap();
        let mut bad = valid_config();
        bad.hard_inventory_notional = Usd::new(Decimal::from_str_lenient("500").unwrap()); // < soft
        assert!(bad.validate().is_err());
    }

    #[test]
    fn features_validate_crossed_book() {
        let mut f = MarketFeatures {
            symbol: "BTC".into(),
            market_version: 1,
            connection_generation: 0,
            exchange_ts: 0,
            received_at: Utc::now(),
            healthy: true,
            best_bid: Price::new(Decimal::from_str_lenient("100").unwrap()),
            best_ask: Price::new(Decimal::from_str_lenient("101").unwrap()),
            best_bid_size: Size::new(Decimal::ONE),
            best_ask_size: Size::new(Decimal::ONE),
            microprice: Price::new(Decimal::from_str_lenient("100.5").unwrap()),
            normalized_ofi: Decimal::ZERO,
            trade_flow: Decimal::ZERO,
            short_return: Decimal::ZERO,
            return_variance_per_second: Decimal::ZERO,
            expected_adverse_markout_bps: Decimal::ZERO,
            latency_buffer_bps: Decimal::ZERO,
            toxicity: Decimal::ZERO,
            funding_rate: Decimal::ZERO,
            external_source: None,
            external_symbol: None,
            external_raw_price: None,
            external_adjusted_price: None,
            external_basis_bps: Decimal::ZERO,
            external_effective_weight: Decimal::ZERO,
            external_confidence: Decimal::ZERO,
            external_age_ms: None,
            external_quality: "unavailable".into(),
            external_observed_at: None,
            latency_quality: "configured".into(),
            markout_quality: "configured".into(),
        };
        f.validate().unwrap();
        f.best_ask = Price::new(Decimal::from_str_lenient("99").unwrap()); // crossed
        assert!(f.validate().is_err());
        assert_eq!(f.mid_price().to_string(), "99.5");
    }
}
