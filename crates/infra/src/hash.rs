//! Shared hash helpers for canonical payloads and idempotency keys.

use sha2::{Digest, Sha256};

/// Hex-encoded SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_hex_is_stable_and_sensitive() {
        assert_eq!(sha256_hex(b"payload"), sha256_hex(b"payload"));
        assert_ne!(sha256_hex(b"a"), sha256_hex(b"b"));
    }
}
