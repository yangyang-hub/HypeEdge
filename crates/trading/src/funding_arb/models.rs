//! Funding-rate arbitrage parameters and durable cycle state, port of
//! `src/hypeedge/strategy/funding_arb/models.py`.

use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::Decimal;
use hypeedge_domain::enums::FundingArbCycleState;
use uuid::Uuid;

/// Funding-rate arbitrage parameters (mirrors `funding_arb_config_versions`).
#[derive(Debug, Clone, PartialEq)]
pub struct FundingArbParams {
    pub entry_funding_rate: Decimal,
    pub exit_funding_rate: Decimal,
    pub max_notional_usd: Decimal,
    pub hedge_ratio: Decimal,
    pub rebalance_threshold_bps: u32,
    pub leverage: Decimal,
    pub max_slippage_bps: u32,
    pub max_basis_bps: u32,
    pub min_expected_edge_bps: Decimal,
    pub expected_hold_hours: u32,
    pub round_trip_fee_bps: Decimal,
    pub max_unhedged_seconds: u32,
    /// M-FA7: hard ceiling on how long an open cycle may be held. The tick
    /// driver force-closes the cycle when `opened_at` is older than this.
    /// Default 168h (7 days) — a safety valve, not the exit trigger.
    pub max_hold_hours: u32,
}

impl Default for FundingArbParams {
    fn default() -> Self {
        Self {
            entry_funding_rate: Decimal::from_str_lenient("0.0001").unwrap(),
            exit_funding_rate: Decimal::ZERO,
            max_notional_usd: Decimal::from_str_lenient("1000").unwrap(),
            hedge_ratio: Decimal::ONE,
            rebalance_threshold_bps: 50,
            leverage: Decimal::ONE,
            max_slippage_bps: 50,
            max_basis_bps: 500,
            min_expected_edge_bps: Decimal::from_str_lenient("5").unwrap(),
            expected_hold_hours: 8,
            round_trip_fee_bps: Decimal::from_str_lenient("20").unwrap(),
            max_unhedged_seconds: 15,
            max_hold_hours: 168,
        }
    }
}

impl FundingArbParams {
    /// Validate constraints (mirror Postgres CHECKs / `__post_init__`).
    pub fn validate(&self) -> Result<(), String> {
        let mut errors = Vec::new();
        if self.entry_funding_rate <= Decimal::ZERO {
            errors.push(format!(
                "entry_funding_rate must be > 0, got {}",
                self.entry_funding_rate
            ));
        }
        if self.exit_funding_rate < Decimal::ZERO {
            errors.push(format!(
                "exit_funding_rate must be >= 0, got {}",
                self.exit_funding_rate
            ));
        }
        if self.exit_funding_rate >= self.entry_funding_rate {
            errors.push(format!(
                "exit_funding_rate must be < entry_funding_rate, got exit={} entry={}",
                self.exit_funding_rate, self.entry_funding_rate
            ));
        }
        if self.max_notional_usd <= Decimal::ZERO {
            errors.push(format!(
                "max_notional_usd must be > 0, got {}",
                self.max_notional_usd
            ));
        }
        if !(Decimal::ZERO < self.hedge_ratio && self.hedge_ratio <= Decimal::ONE) {
            errors.push(format!(
                "hedge_ratio must be in (0, 1], got {}",
                self.hedge_ratio
            ));
        }
        if self.rebalance_threshold_bps == 0 {
            errors.push(format!(
                "rebalance_threshold_bps must be > 0, got {}",
                self.rebalance_threshold_bps
            ));
        }
        if self.leverage <= Decimal::ZERO {
            errors.push(format!("leverage must be > 0, got {}", self.leverage));
        }
        if self.leverage != self.leverage.round_to_places(0) {
            errors.push(format!(
                "leverage must be an integer, got {}",
                self.leverage
            ));
        }
        if !(1..=500).contains(&self.max_slippage_bps) {
            errors.push(format!(
                "max_slippage_bps must be in [1, 500], got {}",
                self.max_slippage_bps
            ));
        }
        if self.max_basis_bps == 0 {
            errors.push(format!(
                "max_basis_bps must be > 0, got {}",
                self.max_basis_bps
            ));
        }
        if self.min_expected_edge_bps < Decimal::ZERO {
            errors.push(format!(
                "min_expected_edge_bps must be >= 0, got {}",
                self.min_expected_edge_bps
            ));
        }
        if !(1..=168).contains(&self.expected_hold_hours) {
            errors.push(format!(
                "expected_hold_hours must be in [1, 168], got {}",
                self.expected_hold_hours
            ));
        }
        if self.round_trip_fee_bps < Decimal::ZERO {
            errors.push(format!(
                "round_trip_fee_bps must be >= 0, got {}",
                self.round_trip_fee_bps
            ));
        }
        if !(1..=60).contains(&self.max_unhedged_seconds) {
            errors.push(format!(
                "max_unhedged_seconds must be in [1, 60], got {}",
                self.max_unhedged_seconds
            ));
        }
        if !(1..=8760).contains(&self.max_hold_hours) {
            errors.push(format!(
                "max_hold_hours must be in [1, 8760], got {}",
                self.max_hold_hours
            ));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    /// `leverage` as an integer (validated integral).
    pub fn leverage_int(&self) -> i64 {
        self.leverage.to_string().parse::<i64>().unwrap_or(1)
    }
}

/// Durable state of one spot/perpetual hedge lifecycle.
#[derive(Debug, Clone, PartialEq)]
pub struct FundingArbCycle {
    pub cycle_id: Uuid,
    pub strategy_id: String,
    pub config_revision: u64,
    pub sub_account: String,
    pub perp_symbol: String,
    pub spot_symbol: String,
    pub spot_display: String,
    pub base_token: String,
    pub quote_token: String,
    pub state: FundingArbCycleState,
    pub target_perp_size: Decimal,
    pub target_spot_size: Decimal,
    pub perp_open_size: Decimal,
    pub spot_open_size: Decimal,
    pub baseline_spot_size: Decimal,
    pub entry_funding_rate: Decimal,
    pub entry_basis_bps: Decimal,
    pub revision: u64,
    pub spot_entry_cloid: Option<String>,
    pub perp_entry_cloid: Option<String>,
    pub compensation_cloid: Option<String>,
    pub perp_exit_cloid: Option<String>,
    pub spot_exit_cloid: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub opened_at: Option<DateTime<Utc>>,
    pub closed_at: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_validate() {
        FundingArbParams::default().validate().unwrap();
    }

    #[test]
    fn rejects_exit_gte_entry() {
        let entry = FundingArbParams::default().entry_funding_rate;
        let p = FundingArbParams {
            exit_funding_rate: entry,
            ..FundingArbParams::default()
        };
        assert!(p.validate().is_err());
    }

    #[test]
    fn rejects_non_integer_leverage() {
        let p = FundingArbParams {
            leverage: Decimal::from_str_lenient("1.5").unwrap(),
            ..FundingArbParams::default()
        };
        assert!(p.validate().is_err());
        let p = FundingArbParams {
            leverage: Decimal::from_str_lenient("1").unwrap(),
            ..FundingArbParams::default()
        };
        assert!(p.validate().is_ok());
    }

    #[test]
    fn rejects_bad_slippage_and_hold() {
        let p = FundingArbParams {
            max_slippage_bps: 0,
            ..FundingArbParams::default()
        };
        assert!(p.validate().is_err());
        let p = FundingArbParams {
            expected_hold_hours: 200,
            ..FundingArbParams::default()
        };
        assert!(p.validate().is_err());
        let p = FundingArbParams {
            max_unhedged_seconds: 0,
            ..FundingArbParams::default()
        };
        assert!(p.validate().is_err());
    }
}
