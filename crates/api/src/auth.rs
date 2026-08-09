//! API bearer-token authentication and role-based authorization, port of
//! `src/hypeedge/api/auth.py`.

use std::sync::Arc;

use hypeedge_config::settings::ApiSettings;
use sha2::{Digest, Sha256};

/// API role ranking (viewer < operator < admin).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ApiRole {
    Viewer,
    Operator,
    Admin,
}

impl ApiRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            ApiRole::Viewer => "viewer",
            ApiRole::Operator => "operator",
            ApiRole::Admin => "admin",
        }
    }
}

/// The authenticated principal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub actor_id: String,
    pub role: ApiRole,
}

/// The configured role tokens, with the legacy `auth_token` treated as admin.
#[derive(Clone)]
pub struct RoleTokens {
    tokens: Vec<(String, ApiRole)>,
}

impl RoleTokens {
    pub fn from_settings(settings: &ApiSettings) -> Self {
        let mut tokens = Vec::new();
        for (token, role) in [
            (&settings.viewer_token, ApiRole::Viewer),
            (&settings.operator_token, ApiRole::Operator),
            (&settings.admin_token, ApiRole::Admin),
            (&settings.auth_token, ApiRole::Admin),
        ] {
            if !token.is_empty() {
                tokens.push((token.clone(), role));
            }
        }
        Self { tokens }
    }

    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    /// Authenticate a bearer header. Returns the highest-ranked matching role.
    pub fn authenticate(&self, authorization: &str) -> Option<Principal> {
        let (scheme, supplied) = match authorization.split_once(' ') {
            Some((s, r)) if !r.is_empty() => (s, r),
            _ => return None,
        };
        if !scheme.eq_ignore_ascii_case("bearer") {
            return None;
        }
        let mut matched_role: Option<ApiRole> = None;
        for (configured, role) in &self.tokens {
            if constant_time_eq(supplied.as_bytes(), configured.as_bytes())
                && (matched_role.is_none() || *role > matched_role.unwrap())
            {
                matched_role = Some(*role);
            }
        }
        let role = matched_role?;
        let digest = Sha256::digest(supplied.as_bytes());
        let actor_id = format!("api-token:{}", hex::encode(&digest[..12]));
        Some(Principal { actor_id, role })
    }
}

/// Whether this method is a mutation (requires idempotency key + higher roles).
pub fn is_mutation(method: &str) -> bool {
    !matches!(method, "GET" | "HEAD" | "OPTIONS")
}

/// Constant-time comparison (avoids early-return token enumeration).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Test-accessible helper.
pub fn _constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    constant_time_eq(a, b)
}

/// An `Arc<RoleTokens>` for injection into handlers.
pub type RoleTokensRef = Arc<RoleTokens>;

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_config::settings::ApiSettings;

    fn settings() -> ApiSettings {
        ApiSettings {
            viewer_token: "viewer-token-1234567890123456".into(),
            admin_token: "admin-token-1234567890123456".into(),
            ..ApiSettings::default()
        }
    }

    #[test]
    fn bearer_auth_selects_highest_role() {
        let tokens = RoleTokens::from_settings(&settings());
        let p = tokens
            .authenticate("Bearer admin-token-1234567890123456")
            .unwrap();
        assert_eq!(p.role, ApiRole::Admin);
        let v = tokens
            .authenticate("Bearer viewer-token-1234567890123456")
            .unwrap();
        assert_eq!(v.role, ApiRole::Viewer);
    }

    #[test]
    fn bad_or_missing_token_fails() {
        let tokens = RoleTokens::from_settings(&settings());
        assert!(tokens.authenticate("").is_none());
        assert!(tokens.authenticate("Basic abc").is_none());
        assert!(tokens.authenticate("Bearer wrong-token").is_none());
    }

    #[test]
    fn empty_tokens_mean_open() {
        let tokens = RoleTokens::from_settings(&ApiSettings::default());
        assert!(tokens.is_empty());
    }

    #[test]
    fn constant_time_eq_matches() {
        assert!(_constant_time_eq(b"abc", b"abc"));
        assert!(!_constant_time_eq(b"abc", b"abd"));
        assert!(!_constant_time_eq(b"abc", b"abcd"));
    }
}
