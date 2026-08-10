//! Client order ID (cloid) generator, port of
//! `src/hypeedge/execution/cloid.py`.

use sha2::{Digest, Sha256};

/// HL cloid format: `0x` + 32 hex chars (16 bytes).
const HL_CLOID_LEN: usize = 34;

/// Generates unique client order IDs for idempotent order submission.
///
/// Internal format: `{strategy}_{timestamp_ms}_{short_uuid}` (human-readable).
/// HL exchange format: `0x` + 32 hex chars (via `to_hl_cloid`).
pub struct CloidGenerator;

impl CloidGenerator {
    /// Generate a new unique cloid.
    pub fn generate(strategy_id: Option<&str>) -> String {
        let ts = chrono::Utc::now().timestamp_millis();
        let short_id = uuid_short();
        let prefix = strategy_id.map(|s| &s[..s.len().min(20)]).unwrap_or("sys");
        let mut cloid = format!("{prefix}_{ts}_{short_id}");
        if cloid.len() > 64 {
            cloid.truncate(64);
        }
        cloid
    }

    /// Deterministic cloid for a strategy sequence number: the same
    /// `(strategy_id, seq)` always yields the same cloid, so a caller using it
    /// for replay/idempotency can rely on it across restarts. No timestamp.
    pub fn generate_for_strategy(strategy_id: &str, seq: u32) -> String {
        let mut cloid = format!("{strategy_id}_{seq:04}");
        if cloid.len() > 64 {
            cloid.truncate(64);
        }
        cloid
    }

    /// Deterministic cloid for a strategy intent (A3): same normalized intent
    /// key → same cloid (idempotent replay returns the original order); a
    /// different intent → a different cloid. Auto-generated cloids must not
    /// embed a random suffix, or a crash between submission and persistence
    /// would make a restart double-submit.
    pub fn deterministic(strategy_id: Option<&str>, intent_key: &str) -> String {
        let prefix = strategy_id.map(|s| &s[..s.len().min(20)]).unwrap_or("sys");
        let digest = Sha256::digest(intent_key.as_bytes());
        let hex = hex::encode(&digest[..12]);
        let mut cloid = format!("{prefix}_{hex}");
        if cloid.len() > 64 {
            cloid.truncate(64);
        }
        cloid
    }

    /// Convert an internal cloid to the HL `0x` + 32 hex format. Values already
    /// in that format pass through lowercased.
    pub fn to_hl_cloid(cloid: &str) -> String {
        if cloid.len() == HL_CLOID_LEN
            && cloid.starts_with("0x")
            && cloid[2..].bytes().all(|b| b.is_ascii_hexdigit())
        {
            return cloid.to_lowercase();
        }
        let digest = Sha256::digest(cloid.as_bytes());
        let hex = hex::encode(&digest[..16]);
        format!("0x{hex}")
    }

    /// Validate that a cloid is non-empty and within the 64-char limit.
    pub fn validate(cloid: &str) -> bool {
        !cloid.trim().is_empty() && cloid.len() <= 64
    }
}

/// Eight hex chars of a random UUID (uuid v4).
fn uuid_short() -> String {
    let u = uuid::Uuid::new_v4();
    u.simple().to_string()[..8].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_is_unique_and_bounded() {
        let a = CloidGenerator::generate(Some("trend_follow"));
        let b = CloidGenerator::generate(Some("trend_follow"));
        assert_ne!(a, b);
        assert!(CloidGenerator::validate(&a));
        assert!(a.len() <= 64);
        assert!(a.starts_with("trend_follow_"));
    }

    #[test]
    fn to_hl_cloid_hashes_internal() {
        let raw = "trend_follow_1700000000000_abcdef12";
        let hl = CloidGenerator::to_hl_cloid(raw);
        assert_eq!(hl.len(), 34);
        assert!(hl.starts_with("0x"));
        assert!(hl[2..].bytes().all(|b| b.is_ascii_hexdigit()));
    }

    #[test]
    fn to_hl_cloid_passthrough_existing_format() {
        // 0x + exactly 32 hex chars.
        let hl = "0xabcdef0123456789abcdef0123456789";
        let out = CloidGenerator::to_hl_cloid(hl);
        assert_eq!(out, hl.to_lowercase());
        // Uppercase passes through lowercased.
        let upper = "0xABCDEF0123456789ABCDEF0123456789";
        assert_eq!(CloidGenerator::to_hl_cloid(upper), hl);
    }

    #[test]
    fn validate_rejects_empty_and_long() {
        assert!(!CloidGenerator::validate(""));
        assert!(!CloidGenerator::validate("   "));
        let long = "x".repeat(65);
        assert!(!CloidGenerator::validate(&long));
    }

    #[test]
    fn deterministic_is_stable_per_intent_and_differs_across_intents() {
        let key = "BTC|buy|1|100|limit|Gtc|false|false|50";
        let a = CloidGenerator::deterministic(Some("tf_1"), key);
        let b = CloidGenerator::deterministic(Some("tf_1"), key);
        assert_eq!(a, b, "same intent key -> same cloid (idempotent replay)");
        let c = CloidGenerator::deterministic(Some("tf_1"), "ETH|buy|1|100|limit|Gtc|false|false|50");
        assert_ne!(a, c, "different intent -> different cloid");
        assert!(CloidGenerator::validate(&a));
        assert!(a.len() <= 64);
    }

    #[test]
    fn generate_for_strategy_is_deterministic() {
        // D1: no timestamp — the same (strategy_id, seq) always yields the same cloid.
        let a = CloidGenerator::generate_for_strategy("tf_1", 7);
        let b = CloidGenerator::generate_for_strategy("tf_1", 7);
        assert_eq!(a, b);
        assert_ne!(a, CloidGenerator::generate_for_strategy("tf_1", 8));
    }
}
