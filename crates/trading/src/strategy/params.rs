//! Strategy parameter models, port of `src/hypeedge/strategy/params.py`.

/// Trend-following strategy parameters. Validation mirrors `TrendParams.__post_init__`.
#[derive(Debug, Clone, PartialEq)]
pub struct TrendParams {
    pub symbol: String,
    /// Candle interval this strategy trades on (e.g. "1m"). The persisted
    /// typed config has no interval column, so this defaults to "1m" (the
    /// runtime's market-data default) and is only overridable via the raw
    /// snapshot in `decode_trend_follow_config`.
    pub interval: String,
    pub fast_ema_period: usize,
    pub slow_ema_period: usize,
    pub signal_ema_period: usize,
    pub momentum_period: usize,
    pub momentum_threshold: f64,
    pub atr_period: usize,
    pub atr_position_multiplier: f64,
    pub max_position_pct: f64,
    pub risk_per_trade_pct: f64,
    pub atr_stop_multiplier: f64,
    pub macd_cross_threshold: f64,
}

impl Default for TrendParams {
    fn default() -> Self {
        Self {
            symbol: "BTC".into(),
            interval: "1m".into(),
            fast_ema_period: 12,
            slow_ema_period: 26,
            signal_ema_period: 9,
            momentum_period: 10,
            momentum_threshold: 0.0,
            atr_period: 14,
            atr_position_multiplier: 0.5,
            max_position_pct: 0.15,
            risk_per_trade_pct: 0.01,
            atr_stop_multiplier: 2.0,
            macd_cross_threshold: 0.0,
        }
    }
}

impl TrendParams {
    /// Upper bound for indicator periods (mirrors the config-normalize range).
    pub const MAX_PERIOD: usize = 500;

    /// Validate parameter constraints (port of `__post_init__`).
    pub fn validate(&self) -> Result<(), String> {
        let mut errors = Vec::new();
        let period_error = |name: &str, value: usize| {
            if !(1..=Self::MAX_PERIOD).contains(&value) {
                Some(format!(
                    "{name} must be in [1, {}], got {value}",
                    Self::MAX_PERIOD
                ))
            } else {
                None
            }
        };
        if let Some(e) = period_error("fast_ema_period", self.fast_ema_period) {
            errors.push(e);
        }
        if let Some(e) = period_error("slow_ema_period", self.slow_ema_period) {
            errors.push(e);
        }
        if self.fast_ema_period >= self.slow_ema_period {
            errors.push(format!(
                "fast_ema_period ({}) must be < slow_ema_period ({})",
                self.fast_ema_period, self.slow_ema_period
            ));
        }
        if let Some(e) = period_error("signal_ema_period", self.signal_ema_period) {
            errors.push(e);
        }
        if let Some(e) = period_error("momentum_period", self.momentum_period) {
            errors.push(e);
        }
        if let Some(e) = period_error("atr_period", self.atr_period) {
            errors.push(e);
        }
        if self.atr_position_multiplier <= 0.0 {
            errors.push(format!(
                "atr_position_multiplier must be > 0, got {}",
                self.atr_position_multiplier
            ));
        }
        if !(0.0 < self.max_position_pct && self.max_position_pct <= 1.0) {
            errors.push(format!(
                "max_position_pct must be in (0, 1.0], got {}",
                self.max_position_pct
            ));
        }
        if !(0.0 < self.risk_per_trade_pct && self.risk_per_trade_pct <= 1.0) {
            errors.push(format!(
                "risk_per_trade_pct must be in (0, 1.0], got {}",
                self.risk_per_trade_pct
            ));
        }
        if self.atr_stop_multiplier <= 0.0 {
            errors.push(format!(
                "atr_stop_multiplier must be > 0, got {}",
                self.atr_stop_multiplier
            ));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(format!("Invalid TrendParams: {}", errors.join("; ")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_validate() {
        TrendParams::default().validate().unwrap();
    }

    #[test]
    fn rejects_fast_gte_slow() {
        let p = TrendParams {
            fast_ema_period: 26,
            slow_ema_period: 12,
            ..TrendParams::default()
        };
        assert!(p.validate().is_err());
    }

    #[test]
    fn rejects_bad_fractions() {
        let p = TrendParams {
            max_position_pct: 0.0,
            ..TrendParams::default()
        };
        assert!(p.validate().is_err());
        let p = TrendParams {
            max_position_pct: 1.5,
            ..TrendParams::default()
        };
        assert!(p.validate().is_err());
    }

    #[test]
    fn rejects_zero_and_huge_periods() {
        // M-ST3: a zero period must be rejected (not just fast >= slow).
        let p = TrendParams {
            fast_ema_period: 0,
            slow_ema_period: 26,
            ..TrendParams::default()
        };
        assert!(p.validate().is_err());
        let p = TrendParams {
            atr_period: 0,
            ..TrendParams::default()
        };
        assert!(p.validate().is_err());
        // Upper bound (500) is enforced as well.
        let p = TrendParams {
            slow_ema_period: 501,
            ..TrendParams::default()
        };
        assert!(p.validate().is_err());
    }

    #[test]
    fn interval_defaults_to_one_minute() {
        assert_eq!(TrendParams::default().interval, "1m");
    }
}
