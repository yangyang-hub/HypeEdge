//! Durable strategy config-version repository, port of the `_repository`
//! config-version methods in `src/hypeedge/api/routes/market_making.py` and
//! `MarketMakingRepository` in `src/hypeedge/storage/market_making.py`.
//!
//! Config versions are immutable snapshots: one `strategy_config_versions`
//! meta row (strategy_id, version, config_hash, created_by) plus a typed
//! strategy-specific row (`trend_follow_config_versions` /
//! `market_maker_config_versions` / `funding_arb_config_versions`) joined on
//! `config_version_id`. Creation is idempotent by semantic hash; the strategy
//! instance revision is bumped under `FOR UPDATE` as an optimistic lock.
//!
//! The Postgres implementation lives in [`crate::config_version_pg`].

use async_trait::async_trait;
use hypeedge_domain::error::HypeEdgeError;

/// A config-version record with its typed values (for the API payload).
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigVersionRecord {
    pub version: u64,
    pub config_hash: String,
    pub created_by: Option<String>,
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
    pub values: serde_json::Value,
}

/// The durable config-version repository boundary.
#[async_trait]
pub trait ConfigVersionStore: Send + Sync {
    /// List all config versions for a strategy, ordered by version.
    async fn list_config_versions(
        &self,
        strategy_id: &str,
    ) -> Result<Vec<ConfigVersionRecord>, HypeEdgeError>;

    /// Create (or return the existing hash-identical) config version for a
    /// strategy. `expected_revision` is the optimistic-lock guard on the
    /// strategy instance; `None` skips the check.
    async fn create_config_version(
        &self,
        strategy_id: &str,
        strategy_type: &str,
        values: &serde_json::Value,
        created_by: &str,
        expected_revision: Option<u64>,
    ) -> Result<ConfigVersionRecord, HypeEdgeError>;

    /// The strategy type for an instance (drives which typed table to use).
    async fn strategy_type(&self, strategy_id: &str) -> Result<Option<String>, HypeEdgeError>;
}

/// Canonicalize a config's values for hashing: normalize decimal fields to
/// trimmed strings, sort keys, compact separators, then sha256 hex. Mirrors
/// `*_config_hash`.
pub fn config_hash(values: &serde_json::Value) -> String {
    hypeedge_infra::sha256_hex(canonical_json(values).as_bytes())
}

/// Compact, key-sorted, decimal-normalized JSON (the hash input).
pub fn canonical_json(values: &serde_json::Value) -> String {
    let normalized = normalize_decimal_strings(values);
    serde_json::to_string(&normalized).unwrap_or_default()
}

/// Recursively normalize decimal strings/numbers to trimmed decimal strings and
/// sort object keys — mirrors Python's `_decimal_text` + `sort_keys`.
fn normalize_decimal_strings(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::new();
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for k in keys {
                out.insert(k.clone(), normalize_decimal_strings(&map[k]));
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Number(n) => {
            let s = if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(f) = n.as_f64() {
                trim_decimal(&format!("{f}"))
            } else {
                n.to_string()
            };
            serde_json::Value::String(s)
        }
        serde_json::Value::String(s) if looks_numeric(s) => {
            serde_json::Value::String(trim_decimal(s))
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(normalize_decimal_strings).collect())
        }
        other => other.clone(),
    }
}

/// Trim trailing fractional zeros (and a trailing dot) — `_decimal_text`.
fn trim_decimal(s: &str) -> String {
    let (int_part, frac_part) = match s.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (s, None),
    };
    match frac_part {
        Some(f) => {
            let trimmed = f.trim_end_matches('0');
            if trimmed.is_empty() {
                int_part.to_string()
            } else {
                format!("{int_part}.{trimmed}")
            }
        }
        None => int_part.to_string(),
    }
}

fn looks_numeric(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_digit() || b == b'.' || b == b'-' || b == b'+')
        && s.bytes().any(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_hash_is_stable_across_key_order() {
        let a = serde_json::json!({"fast_ema_period": 12, "slow_ema_period": 26});
        let b = serde_json::json!({"slow_ema_period": 26, "fast_ema_period": 12});
        assert_eq!(config_hash(&a), config_hash(&b));
        assert_eq!(config_hash(&a).len(), 64);
    }

    #[test]
    fn config_hash_normalizes_decimal_scale() {
        let a = serde_json::json!({"quote_size": "0.100"});
        let b = serde_json::json!({"quote_size": 0.1});
        assert_eq!(
            config_hash(&a),
            config_hash(&b),
            "trailing-zero scale must not affect the hash"
        );
    }

    #[test]
    fn config_hash_distinguishes_values() {
        let a = serde_json::json!({"fast_ema_period": 12});
        let b = serde_json::json!({"fast_ema_period": 13});
        assert_ne!(config_hash(&a), config_hash(&b));
    }
}
