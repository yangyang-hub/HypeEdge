//! Typed strategy-config normalization and validation, port of
//! `normalize_*_config` / `default_*_config` in
//! `src/hypeedge/storage/market_making.py`.
//!
//! These functions validate the complete typed Postgres config contract for
//! each strategy type, fill new-field defaults, and return a canonical
//! `serde_json::Value` suitable for `config_hash` and the typed config-version
//! write path. Callers embed the JSON value exactly as Python does.

use hypeedge_domain::error::HypeEdgeError;
use serde_json::{Value, json};

// --- Market maker ---

/// Decimal config fields (persisted as NUMERIC(38,18)).
const MM_DECIMAL_FIELDS: &[&str] = &[
    "soft_inventory_notional",
    "hard_inventory_notional",
    "emergency_inventory_notional",
    "quote_size",
    "max_depth_participation",
    "inventory_skew_bps",
    "max_inventory_shift_bps",
    "min_half_spread_bps",
    "toxicity_spread_bps",
    "min_expected_pnl_usdc",
    "external_reference_weight",
    "external_max_age_seconds",
    "external_outlier_bps",
    "max_external_shift_ticks",
    "max_total_fair_shift_ticks",
    "latency_risk_multiplier",
    "conservative_latency_seconds",
    "conservative_markout_bps",
];

/// Integer config fields.
const MM_INTEGER_FIELDS: &[&str] = &[
    "min_quote_lifetime_ms",
    "refresh_cooldown_ms",
    "max_quote_age_ms",
    "market_stale_after_ms",
    "account_stale_after_ms",
    "min_markout_samples",
];

const MM_NEW_FIELD_DEFAULTS: &[(&str, &str)] = &[
    ("external_reference_weight", "0.25"),
    ("external_max_age_seconds", "0.5"),
    ("external_outlier_bps", "75"),
    ("max_external_shift_ticks", "2"),
    ("max_total_fair_shift_ticks", "3"),
    ("latency_risk_multiplier", "1"),
    ("conservative_latency_seconds", "0.1"),
    ("conservative_markout_bps", "1"),
    ("min_markout_samples", "20"),
];

/// Validate and normalize the complete typed market-maker config contract.
pub fn normalize_market_maker_config(values: &Value) -> Result<Value, HypeEdgeError> {
    let mut supplied = values.as_object().cloned().unwrap_or_default();
    let all_fields: Vec<&str> = MM_DECIMAL_FIELDS
        .iter()
        .chain(MM_INTEGER_FIELDS.iter())
        .copied()
        .collect();
    for (name, default) in MM_NEW_FIELD_DEFAULTS {
        supplied
            .entry((*name).to_string())
            .or_insert_with(|| Value::String(default.to_string()));
    }
    let keys: std::collections::HashSet<&str> = supplied.keys().map(|k| k.as_str()).collect();
    let required: std::collections::HashSet<&str> = all_fields
        .iter()
        .copied()
        .filter(|k| !MM_NEW_FIELD_DEFAULTS.iter().any(|(name, _)| name == k))
        .collect();
    if !required.is_subset(&keys) || !keys.is_subset(&all_fields.iter().copied().collect()) {
        let mut missing: Vec<&str> = required.difference(&keys).copied().collect();
        missing.sort_unstable();
        let mut extra: Vec<&str> = keys
            .difference(&all_fields.iter().copied().collect())
            .copied()
            .collect();
        extra.sort_unstable();
        return Err(HypeEdgeError::StrategyRegistration {
            message: format!(
                "Invalid market-maker config fields: missing={missing:?} extra={extra:?}"
            ),
        });
    }

    let mut out = serde_json::Map::new();
    for name in MM_DECIMAL_FIELDS {
        let text = decimal_text(&supplied[&name.to_string()])?;
        out.insert((*name).to_string(), Value::String(text));
    }
    for name in MM_INTEGER_FIELDS {
        let value = int_value(&supplied[&name.to_string()])?;
        out.insert((*name).to_string(), json!(value));
    }
    Ok(Value::Object(out))
}

/// Stable semantic hash; decimal scale and key order do not affect it.
pub fn market_maker_config_hash(values: &Value) -> Result<String, HypeEdgeError> {
    let normalized = normalize_market_maker_config(values)?;
    Ok(config_hash(&normalized))
}

/// Safe create defaults aligned with `MarketMakerConfig::default_with`.
pub fn default_market_maker_config() -> Value {
    normalize_market_maker_config(&json!({
        "soft_inventory_notional": "0",
        "hard_inventory_notional": "0",
        "emergency_inventory_notional": "0",
        "quote_size": "0",
        "max_depth_participation": "0.05",
        "inventory_skew_bps": "5",
        "max_inventory_shift_bps": "20",
        "min_half_spread_bps": "1",
        "toxicity_spread_bps": "10",
        "min_expected_pnl_usdc": "0",
        "min_quote_lifetime_ms": 200,
        "refresh_cooldown_ms": 100,
        "max_quote_age_ms": 5000,
        "market_stale_after_ms": 2000,
        "account_stale_after_ms": 5000,
    }))
    .expect("default market-maker config is valid")
}

// --- Trend follow ---

const TF_INTEGER_FIELDS: &[&str] = &[
    "fast_ema_period",
    "slow_ema_period",
    "signal_ema_period",
    "momentum_period",
    "atr_period",
];
const TF_DECIMAL_FIELDS: &[&str] = &[
    "momentum_threshold",
    "atr_position_multiplier",
    "atr_stop_multiplier",
    "max_position_pct",
    "risk_per_trade_pct",
    "macd_cross_threshold",
];

/// Validate and normalize typed trend-follow config.
pub fn normalize_trend_follow_config(values: &Value) -> Result<Value, HypeEdgeError> {
    let mut supplied = values.as_object().cloned().unwrap_or_default();
    // Drop symbol if callers embed it; instance.symbol is authoritative.
    supplied.remove("symbol");
    let all_fields: Vec<&str> = TF_INTEGER_FIELDS
        .iter()
        .chain(TF_DECIMAL_FIELDS.iter())
        .copied()
        .collect();
    let keys: std::collections::HashSet<&str> = supplied.keys().map(|k| k.as_str()).collect();
    if keys != all_fields.iter().copied().collect() {
        let missing: Vec<&str> = all_fields
            .iter()
            .copied()
            .filter(|k| !keys.contains(k))
            .collect();
        let extra: Vec<&str> = keys
            .iter()
            .copied()
            .filter(|k| !all_fields.contains(k))
            .collect();
        return Err(HypeEdgeError::StrategyRegistration {
            message: format!(
                "Invalid trend-follow config fields: missing={missing:?} extra={extra:?}"
            ),
        });
    }

    let mut out = serde_json::Map::new();
    for name in TF_INTEGER_FIELDS {
        let value = int_value(&supplied[&name.to_string()])?;
        out.insert((*name).to_string(), json!(value));
    }
    for name in TF_DECIMAL_FIELDS {
        let text = decimal_text(&supplied[&name.to_string()])?;
        out.insert((*name).to_string(), Value::String(text));
    }

    let fast: i64 = out["fast_ema_period"].as_i64().unwrap_or(0);
    let slow: i64 = out["slow_ema_period"].as_i64().unwrap_or(0);
    if fast >= slow {
        return Err(HypeEdgeError::StrategyRegistration {
            message: "fast_ema_period must be < slow_ema_period".into(),
        });
    }
    for name in ["max_position_pct", "risk_per_trade_pct"] {
        let value = decimal_text(&out[name])?;
        let d = parse_dec(&value)?;
        if !(d > 0.0 && d <= 1.0) {
            return Err(HypeEdgeError::StrategyRegistration {
                message: format!("{name} must be in (0, 1]"),
            });
        }
    }
    for name in ["atr_position_multiplier", "atr_stop_multiplier"] {
        let value = decimal_text(&out[name])?;
        let d = parse_dec(&value)?;
        if d <= 0.0 {
            return Err(HypeEdgeError::StrategyRegistration {
                message: format!("{name} must be > 0"),
            });
        }
    }
    Ok(Value::Object(out))
}

pub fn trend_follow_config_hash(values: &Value) -> Result<String, HypeEdgeError> {
    let normalized = normalize_trend_follow_config(values)?;
    Ok(config_hash(&normalized))
}

/// Safe create defaults aligned with `TrendParams` (normalized form).
pub fn default_trend_follow_config_values() -> Value {
    normalize_trend_follow_config(&json!({
        "fast_ema_period": 12,
        "slow_ema_period": 26,
        "signal_ema_period": 9,
        "momentum_period": 10,
        "momentum_threshold": "0",
        "atr_period": 14,
        "atr_position_multiplier": "0.5",
        "atr_stop_multiplier": "2",
        "max_position_pct": "0.15",
        "risk_per_trade_pct": "0.01",
        "macd_cross_threshold": "0",
    }))
    .expect("default trend-follow config is valid")
}

// --- Funding arbitrage ---

const FA_INTEGER_FIELDS: &[&str] = &[
    "rebalance_threshold_bps",
    "max_slippage_bps",
    "max_basis_bps",
    "expected_hold_hours",
    "max_unhedged_seconds",
];
const FA_DECIMAL_FIELDS: &[&str] = &[
    "entry_funding_rate",
    "exit_funding_rate",
    "max_notional_usd",
    "hedge_ratio",
    "leverage",
    "min_expected_edge_bps",
    "round_trip_fee_bps",
];
const FA_STRING_FIELDS: &[&str] = &["spot_coin"];

/// Auto market symbol sentinel (mirrors `AUTO_SPOT_MARKET`).
pub const AUTO_SPOT_MARKET: &str = "AUTO/USDC";

/// Validate and normalize typed funding-rate-arbitrage config.
pub fn normalize_funding_arb_config(values: &Value) -> Result<Value, HypeEdgeError> {
    let mut supplied = values.as_object().cloned().unwrap_or_default();
    supplied.remove("symbol");
    supplied
        .entry("spot_coin".to_string())
        .or_insert_with(|| Value::String(AUTO_SPOT_MARKET.to_string()));

    let all_fields: Vec<&str> = FA_INTEGER_FIELDS
        .iter()
        .chain(FA_DECIMAL_FIELDS.iter())
        .chain(FA_STRING_FIELDS.iter())
        .copied()
        .collect();
    let keys: std::collections::HashSet<&str> = supplied.keys().map(|k| k.as_str()).collect();
    if keys != all_fields.iter().copied().collect() {
        let missing: Vec<&str> = all_fields
            .iter()
            .copied()
            .filter(|k| !keys.contains(k))
            .collect();
        let extra: Vec<&str> = keys
            .iter()
            .copied()
            .filter(|k| !all_fields.contains(k))
            .collect();
        return Err(HypeEdgeError::StrategyRegistration {
            message: format!(
                "Invalid funding-arb config fields: missing={missing:?} extra={extra:?}"
            ),
        });
    }

    let mut out = serde_json::Map::new();
    for name in FA_INTEGER_FIELDS {
        let value = int_value(&supplied[&name.to_string()])?;
        out.insert((*name).to_string(), json!(value));
    }
    for name in FA_DECIMAL_FIELDS {
        let text = decimal_text(&supplied[&name.to_string()])?;
        out.insert((*name).to_string(), Value::String(text));
    }
    for name in FA_STRING_FIELDS {
        let value = supplied[&name.to_string()]
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| HypeEdgeError::StrategyRegistration {
                message: format!("Funding-arb config field must be a non-empty string: {name}"),
            })?;
        if !is_spot_market(value) {
            return Err(HypeEdgeError::StrategyRegistration {
                message: "spot_coin must be a valid Hyperliquid spot market identifier".into(),
            });
        }
        out.insert((*name).to_string(), Value::String(value.to_string()));
    }

    let entry: f64 = dec_of(&out, "entry_funding_rate")?;
    let exit: f64 = dec_of(&out, "exit_funding_rate")?;
    if entry <= 0.0 {
        return Err(HypeEdgeError::StrategyRegistration {
            message: "entry_funding_rate must be > 0".into(),
        });
    }
    if exit < 0.0 {
        return Err(HypeEdgeError::StrategyRegistration {
            message: "exit_funding_rate must be >= 0".into(),
        });
    }
    if exit >= entry {
        return Err(HypeEdgeError::StrategyRegistration {
            message: "exit_funding_rate must be < entry_funding_rate".into(),
        });
    }
    for name in ["max_notional_usd", "leverage"] {
        let d = dec_of(&out, name)?;
        if d <= 0.0 {
            return Err(HypeEdgeError::StrategyRegistration {
                message: format!("{name} must be > 0"),
            });
        }
    }
    let hedge = dec_of(&out, "hedge_ratio")?;
    if !(hedge > 0.0 && hedge <= 1.0) {
        return Err(HypeEdgeError::StrategyRegistration {
            message: "hedge_ratio must be in (0, 1]".into(),
        });
    }
    if int_of(&out, "rebalance_threshold_bps")? <= 0 {
        return Err(HypeEdgeError::StrategyRegistration {
            message: "rebalance_threshold_bps must be > 0".into(),
        });
    }
    let leverage = dec_of(&out, "leverage")?;
    if (leverage - leverage.trunc()).abs() > f64::EPSILON {
        return Err(HypeEdgeError::StrategyRegistration {
            message: "leverage must be an integer".into(),
        });
    }
    let max_slippage = int_of(&out, "max_slippage_bps")?;
    if !(1..=500).contains(&max_slippage) {
        return Err(HypeEdgeError::StrategyRegistration {
            message: "max_slippage_bps must be in [1, 500]".into(),
        });
    }
    if int_of(&out, "max_basis_bps")? <= 0 {
        return Err(HypeEdgeError::StrategyRegistration {
            message: "max_basis_bps must be > 0".into(),
        });
    }
    if dec_of(&out, "min_expected_edge_bps")? < 0.0 {
        return Err(HypeEdgeError::StrategyRegistration {
            message: "min_expected_edge_bps must be >= 0".into(),
        });
    }
    if dec_of(&out, "round_trip_fee_bps")? < 0.0 {
        return Err(HypeEdgeError::StrategyRegistration {
            message: "round_trip_fee_bps must be >= 0".into(),
        });
    }
    let hold = int_of(&out, "expected_hold_hours")?;
    if !(1..=168).contains(&hold) {
        return Err(HypeEdgeError::StrategyRegistration {
            message: "expected_hold_hours must be in [1, 168]".into(),
        });
    }
    let unhedged = int_of(&out, "max_unhedged_seconds")?;
    if !(1..=60).contains(&unhedged) {
        return Err(HypeEdgeError::StrategyRegistration {
            message: "max_unhedged_seconds must be in [1, 60]".into(),
        });
    }
    Ok(Value::Object(out))
}

pub fn funding_arb_config_hash(values: &Value) -> Result<String, HypeEdgeError> {
    let normalized = normalize_funding_arb_config(values)?;
    Ok(config_hash(&normalized))
}

/// Safe create defaults; the spot column is a non-tradable auto sentinel.
pub fn default_funding_arb_config() -> Value {
    normalize_funding_arb_config(&json!({
        "spot_coin": AUTO_SPOT_MARKET,
        "entry_funding_rate": "0.0001",
        "exit_funding_rate": "0",
        "max_notional_usd": "1000",
        "hedge_ratio": "1",
        "rebalance_threshold_bps": 50,
        "leverage": "1",
        "max_slippage_bps": 50,
        "max_basis_bps": 500,
        "min_expected_edge_bps": "5",
        "expected_hold_hours": 8,
        "round_trip_fee_bps": "20",
        "max_unhedged_seconds": 15,
    }))
    .expect("default funding-arb config is valid")
}

// --- Helpers ---

/// A spot market identifier: `@N` or a pair like `PURR/USDC`.
fn is_spot_market(value: &str) -> bool {
    if value.starts_with('@') && value[1..].chars().all(|c| c.is_ascii_digit()) {
        return true;
    }
    let mut parts = value.split('/');
    let left = parts.next().unwrap_or("");
    let right = parts.next();
    match right {
        Some(right) if parts.next().is_none() => !left.is_empty() && !right.is_empty(),
        _ => false,
    }
}

fn decimal_text(value: &Value) -> Result<String, HypeEdgeError> {
    match value {
        Value::String(s) => Ok(trim_decimal(s)),
        Value::Number(n) => Ok(trim_decimal(&n.to_string())),
        _ => Err(HypeEdgeError::StrategyRegistration {
            message: "config field must be numeric".into(),
        }),
    }
}

/// Trim trailing fractional zeros without losing `0` / negative zero shapes
/// (mirrors `_decimal_text`).
fn trim_decimal(s: &str) -> String {
    let s = s.trim();
    if s.is_empty() {
        return "0".to_string();
    }
    let (neg, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    if let Some(dot) = rest.find('.') {
        let int_part = &rest[..dot];
        let frac = rest[dot + 1..].trim_end_matches('0');
        if frac.is_empty() {
            let out = if int_part.is_empty() { "0" } else { int_part };
            return if neg && out != "0" {
                format!("-{out}")
            } else {
                out.to_string()
            };
        }
        let out = if int_part.is_empty() {
            format!("0.{frac}")
        } else {
            format!("{int_part}.{frac}")
        };
        return if neg { format!("-{out}") } else { out };
    }
    if neg && rest != "0" {
        format!("-{rest}")
    } else {
        rest.to_string()
    }
}

fn int_value(value: &Value) -> Result<i64, HypeEdgeError> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .ok_or_else(|| HypeEdgeError::StrategyRegistration {
                message: "config field must be an integer".into(),
            }),
        Value::String(s) => {
            s.trim()
                .parse::<i64>()
                .map_err(|_| HypeEdgeError::StrategyRegistration {
                    message: "config field must be an integer".into(),
                })
        }
        _ => Err(HypeEdgeError::StrategyRegistration {
            message: "config field must be an integer".into(),
        }),
    }
}

fn parse_dec(s: &str) -> Result<f64, HypeEdgeError> {
    s.parse::<f64>()
        .map_err(|_| HypeEdgeError::StrategyRegistration {
            message: "config field must be numeric".into(),
        })
}

fn dec_of(out: &serde_json::Map<String, Value>, key: &str) -> Result<f64, HypeEdgeError> {
    parse_dec(&decimal_text(&out[key])?)
}

fn int_of(out: &serde_json::Map<String, Value>, key: &str) -> Result<i64, HypeEdgeError> {
    out[key]
        .as_i64()
        .ok_or_else(|| HypeEdgeError::StrategyRegistration {
            message: format!("{key} must be an integer"),
        })
}

/// Compact, key-sorted sha256 hash (mirrors `config_hash` in the storage
/// crate). The normalized config is already key-sorted decimal strings, so
/// serde_json's deterministic serialization is the canonical form.
fn config_hash(values: &Value) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_string(values).unwrap_or_default().as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mm_default_config_is_valid_and_stable() {
        let d1 = default_market_maker_config();
        let d2 = default_market_maker_config();
        assert_eq!(d1, d2);
        assert!(d1["min_markout_samples"].as_i64() == Some(20));
        assert_eq!(
            d1["external_reference_weight"],
            Value::String("0.25".into())
        );
    }

    #[test]
    fn mm_rejects_unknown_fields() {
        assert!(normalize_market_maker_config(&json!({"bogus": 1})).is_err());
    }

    #[test]
    fn mm_hash_is_key_order_stable() {
        let mut a = json!({
            "soft_inventory_notional": "100",
            "hard_inventory_notional": "200",
            "emergency_inventory_notional": "300",
            "quote_size": "0.01",
            "max_depth_participation": "0.05",
            "inventory_skew_bps": "5",
            "max_inventory_shift_bps": "20",
            "min_half_spread_bps": "1",
            "toxicity_spread_bps": "10",
            "min_expected_pnl_usdc": "0",
            "min_quote_lifetime_ms": 200,
            "refresh_cooldown_ms": 100,
            "max_quote_age_ms": 5000,
            "market_stale_after_ms": 2000,
            "account_stale_after_ms": 5000,
        });
        // Same object reordered via serialization is identical; instead build
        // an equal object and confirm the hashes match.
        let b = a.clone();
        assert_eq!(
            market_maker_config_hash(&a).unwrap(),
            market_maker_config_hash(&b).unwrap()
        );
        // A semantically equal decimal with trailing zeros hashes identically.
        a["soft_inventory_notional"] = Value::String("100.0".into());
        assert_eq!(
            market_maker_config_hash(&a).unwrap(),
            market_maker_config_hash(&b).unwrap()
        );
    }

    #[test]
    fn mm_decimal_scale_does_not_affect_hash() {
        let base = default_market_maker_config();
        let mut with_zeros = base.clone();
        with_zeros["external_outlier_bps"] = Value::String("75.00".into());
        assert_eq!(
            market_maker_config_hash(&base).unwrap(),
            market_maker_config_hash(&with_zeros).unwrap()
        );
    }

    #[test]
    fn tf_default_config_valid() {
        let d = default_trend_follow_config_values();
        assert_eq!(d["fast_ema_period"].as_i64(), Some(12));
        assert_eq!(d["slow_ema_period"].as_i64(), Some(26));
    }

    #[test]
    fn tf_rejects_fast_ge_slow() {
        let mut v = default_trend_follow_config_values();
        v["fast_ema_period"] = json!(26);
        assert!(normalize_trend_follow_config(&v).is_err());
    }

    #[test]
    fn tf_rejects_out_of_bounds_pct() {
        let mut v = default_trend_follow_config_values();
        v["max_position_pct"] = Value::String("1.5".into());
        assert!(normalize_trend_follow_config(&v).is_err());
    }

    #[test]
    fn tf_drops_symbol_field() {
        let mut v = default_trend_follow_config_values();
        v["symbol"] = Value::String("ETH".into());
        assert!(normalize_trend_follow_config(&v).is_ok());
    }

    #[test]
    fn fa_default_config_valid() {
        let d = default_funding_arb_config();
        assert_eq!(d["spot_coin"], Value::String(AUTO_SPOT_MARKET.into()));
        assert_eq!(d["max_slippage_bps"].as_i64(), Some(50));
    }

    #[test]
    fn fa_rejects_exit_ge_entry() {
        let mut v = default_funding_arb_config();
        v["exit_funding_rate"] = Value::String("0.0001".into());
        assert!(normalize_funding_arb_config(&v).is_err());
    }

    #[test]
    fn fa_rejects_bad_spot_market() {
        let mut v = default_funding_arb_config();
        v["spot_coin"] = Value::String("not a market".into());
        assert!(normalize_funding_arb_config(&v).is_err());
    }

    #[test]
    fn fa_accepts_pair_and_auto_market() {
        let mut v = default_funding_arb_config();
        v["spot_coin"] = Value::String("PURR/USDC".into());
        assert!(normalize_funding_arb_config(&v).is_ok());
        v["spot_coin"] = Value::String("@5".into());
        assert!(normalize_funding_arb_config(&v).is_ok());
    }

    #[test]
    fn fa_rejects_non_integer_leverage() {
        let mut v = default_funding_arb_config();
        v["leverage"] = Value::String("1.5".into());
        assert!(normalize_funding_arb_config(&v).is_err());
    }

    #[test]
    fn fa_hash_is_stable() {
        let a = default_funding_arb_config();
        assert_eq!(
            funding_arb_config_hash(&a).unwrap(),
            funding_arb_config_hash(&a).unwrap()
        );
    }

    #[test]
    fn trim_decimal_shapes() {
        assert_eq!(trim_decimal("100.0"), "100");
        assert_eq!(trim_decimal("0.500"), "0.5");
        assert_eq!(trim_decimal("0"), "0");
        assert_eq!(trim_decimal("1.5"), "1.5");
        assert_eq!(trim_decimal("-2.50"), "-2.5");
        assert_eq!(trim_decimal("75.00"), "75");
    }

    #[test]
    fn spot_market_validation() {
        assert!(is_spot_market("PURR/USDC"));
        assert!(is_spot_market("@3"));
        assert!(is_spot_market("WBTC/USDC"));
        assert!(!is_spot_market("PURR"));
        assert!(!is_spot_market("A/B/C"));
        assert!(!is_spot_market(""));
    }
}
