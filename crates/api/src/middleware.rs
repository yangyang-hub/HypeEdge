//! HTTP middleware port of the FastAPI `request_security` middleware in
//! `src/hypeedge/api/app.py`.
//!
//! Applies request-id, security headers, bearer auth, sliding-window rate
//! limits, and idempotency-key enforcement in the same order as Python.

use axum::extract::connect_info::ConnectInfo;
use axum::extract::{Request, State};
use axum::http::header;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::net::SocketAddr;
use uuid::Uuid;

use crate::auth::{ApiRole, is_mutation};
use crate::state::AppState;

/// The authenticated role carried through the request (via extensions).
#[derive(Clone)]
pub struct RoleGuard(pub ApiRole);

/// Whether the request path is API-protected.
fn is_protected(path: &str) -> bool {
    path.starts_with("/api")
}

/// Whether the request path is a v1 mutation (idempotency required).
fn is_v1_mutation(path: &str) -> bool {
    path.starts_with("/api/v1/")
}

/// The unified security middleware.
pub async fn security(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    // H-AP1: the real peer address is authoritative for rate limiting. The
    // `ConnectInfo` extension is present only when the server was started with
    // `into_make_service_with_connect_info` (see `HypeEdgeApp::serve`); it is
    // set by the transport and cannot be spoofed, so `x-forwarded-for` is
    // NEVER trusted. Without it (unit tests / misconfigured server) every
    // request falls back to a single shared key and a warning is logged.
    let client_ip = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip().to_string())
        .or_else(|| {
            tracing::warn!(
                "client_ip_fallback: ConnectInfo missing (server must use \
                 into_make_service_with_connect_info); rate limiting shared"
            );
            Some("unknown".to_string())
        })
        .unwrap();

    // Request id.
    let request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or(&Uuid::new_v4().to_string())
        .to_string();

    let mut request = request;

    // --- Global request rate limit.
    let request_limit = state.settings.api.request_rate_limit_per_minute as u64;
    if !state
        .request_limiter
        .allow(&format!("request:{client_ip}"), request_limit)
        .await
    {
        return problem(
            429,
            "RATE_LIMIT_EXCEEDED",
            "Too many API requests; retry later",
            true,
            &request_id,
        );
    }

    // --- Authenticate.
    let mut actor_role: Option<ApiRole> = None;
    let protected = is_protected(&path);
    if protected && !state.role_tokens.is_empty() {
        match state.role_tokens.authenticate(&auth_header) {
            Some(principal) => {
                actor_role = Some(principal.role);
            }
            None => {
                let auth_limit = state.settings.api.auth_failure_limit_per_minute as u64;
                let allowed = state
                    .auth_failure_limiter
                    .allow(&format!("auth:{client_ip}"), auth_limit)
                    .await;
                return problem(
                    if allowed { 401 } else { 429 },
                    if allowed {
                        "AUTHENTICATION_REQUIRED"
                    } else {
                        "AUTH_RATE_LIMIT_EXCEEDED"
                    },
                    if allowed {
                        "A valid Bearer token is required"
                    } else {
                        "Too many failed authentication attempts"
                    },
                    !allowed,
                    &request_id,
                );
            }
        }
    } else if protected {
        // H-AP3: no tokens configured → the least-privilege `viewer` role, so
        // unauthenticated requests can read but every mutation is rejected by
        // the handler-level `authorize` gates (A23). (Was `Admin`.)
        actor_role = Some(ApiRole::Viewer);
    }

    // --- Mutation rate limit (per actor).
    let mutation = is_mutation(&method);
    if protected && mutation {
        let actor_key = format!("mutation:{client_ip}");
        let mutation_limit = state.settings.api.mutation_rate_limit_per_minute as u64;
        if !state
            .mutation_limiter
            .allow(&actor_key, mutation_limit)
            .await
        {
            return problem(
                429,
                "MUTATION_RATE_LIMIT_EXCEEDED",
                "Too many mutation requests; retry later",
                true,
                &request_id,
            );
        }
    }

    // --- Idempotency key on v1 mutations.
    if mutation && is_v1_mutation(&path) {
        let key = request
            .headers()
            .get("idempotency-key")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if key.is_empty() || key.len() > 128 {
            return problem(
                400,
                "IDEMPOTENCY_KEY_REQUIRED",
                "A valid Idempotency-Key header is required",
                false,
                &request_id,
            );
        }
    }

    // Attach the role for handlers.
    if let Some(role) = actor_role {
        request.extensions_mut().insert(RoleGuard(role));
    }

    let mut response = next.run(request).await;

    // Security headers.
    response.headers_mut().insert(
        "x-request-id",
        request_id.parse().unwrap_or_else(|_| "".parse().unwrap()),
    );
    response
        .headers_mut()
        .insert("x-content-type-options", "nosniff".parse().unwrap());
    response
        .headers_mut()
        .insert("x-frame-options", "DENY".parse().unwrap());
    response
        .headers_mut()
        .insert("referrer-policy", "no-referrer".parse().unwrap());
    response.headers_mut().insert(
        "permissions-policy",
        "camera=(), microphone=(), geolocation=()".parse().unwrap(),
    );
    if response.status().as_u16() == 429 {
        response
            .headers_mut()
            .insert("retry-after", "60".parse().unwrap());
    }
    response
}

/// Helper to build a problem+json response.
fn problem(status: u16, code: &str, detail: &str, retryable: bool, request_id: &str) -> Response {
    let body = serde_json::json!({
        "type": format!("https://hypeedge.local/problems/{}", code.to_lowercase()),
        "title": code,
        "status": status,
        "code": code,
        "detail": detail,
        "request_id": request_id,
        "retryable": retryable,
        "context": {},
    });
    let mut response = (
        axum::http::StatusCode::from_u16(status)
            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
        axum::Json(body),
    )
        .into_response();
    response.headers_mut().insert(
        "x-request-id",
        request_id
            .parse()
            .unwrap_or_else(|_| axum::http::HeaderValue::from_static("")),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::sync::Arc;
    use tower::ServiceExt;

    fn test_state(rate_limit: u32) -> AppState {
        let mut settings = hypeedge_config::settings::AppSettings::default();
        settings.api.request_rate_limit_per_minute = rate_limit;
        settings.api.mutation_rate_limit_per_minute = rate_limit;
        let settings = Arc::new(settings);
        let bus = Arc::new(hypeedge_infra::event_bus::EventBus::new(64));
        let ks = Arc::new(hypeedge_trading::risk::KillSwitch::new(bus.clone(), false));
        AppState::new(
            settings,
            ks,
            bus,
            Arc::new(tokio::sync::Mutex::new(
                hypeedge_trading::market_data::BookManager::new(20),
            )),
        )
    }

    fn request(uri: &str, method: &str, ip: std::net::IpAddr) -> Request<Body> {
        request_with_body(uri, method, ip, "{}")
    }

    fn request_with_body(uri: &str, method: &str, ip: std::net::IpAddr, body: &str) -> Request<Body> {
        let builder = Request::builder()
            .method(method)
            .uri(uri)
            .header("idempotency-key", "test-key-0001")
            .header("content-type", "application/json");
        let mut req = builder.body(Body::from(body.to_string())).unwrap();
        req.extensions_mut().insert(ConnectInfo(SocketAddr::new(ip, 54321)));
        req
    }

    /// H-AP1: a spoofed `x-forwarded-for` must not let a client exceed its
    /// rate limit — the real `ConnectInfo` peer address wins.
    #[tokio::test]
    async fn spoofed_xff_cannot_bypass_rate_limit() {
        let router = crate::build_router(test_state(2));
        let real_ip = "203.0.113.7".parse().unwrap();
        // Two requests from the same real peer, each lying with a different XFF.
        let mut r1 = request("/api/v1/strategies", "GET", real_ip);
        r1.headers_mut()
            .insert("x-forwarded-for", "1.2.3.4".parse().unwrap());
        assert_eq!(router.clone().oneshot(r1).await.unwrap().status(), StatusCode::OK);
        let mut r2 = request("/api/v1/strategies", "GET", real_ip);
        r2.headers_mut()
            .insert("x-forwarded-for", "5.6.7.8".parse().unwrap());
        assert_eq!(router.clone().oneshot(r2).await.unwrap().status(), StatusCode::OK);
        // Third request from the same peer — blocked despite yet another XFF.
        let mut r3 = request("/api/v1/strategies", "GET", real_ip);
        r3.headers_mut()
            .insert("x-forwarded-for", "9.9.9.9".parse().unwrap());
        let resp = router.oneshot(r3).await.unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("RATE_LIMIT_EXCEEDED"));
    }

    /// H-AP3: with no tokens configured, an unauthenticated mutation is denied
    /// (empty token no longer maps to Admin).
    #[tokio::test]
    async fn empty_token_mutation_is_forbidden() {
        let router = crate::build_router(test_state(100));
        let req = request(
            "/api/v1/strategies",
            "POST",
            "203.0.113.9".parse().unwrap(),
        );
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("FORBIDDEN"));
    }

    /// H-AP3: reads still work without a token (Viewer).
    #[tokio::test]
    async fn empty_token_reads_are_allowed() {
        let router = crate::build_router(test_state(100));
        let req = request("/api/v1/strategies", "GET", "203.0.113.9".parse().unwrap());
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// A configured operator token passes the mutation gate (regression guard
    /// for the Viewer default).
    #[tokio::test]
    async fn operator_token_passes_mutation_gate() {
        let mut settings = hypeedge_config::settings::AppSettings::default();
        settings.api.operator_token = "op-token-1234567890123456".into();
        settings.api.request_rate_limit_per_minute = 100;
        let settings = Arc::new(settings);
        let bus = Arc::new(hypeedge_infra::event_bus::EventBus::new(64));
        let ks = Arc::new(hypeedge_trading::risk::KillSwitch::new(bus.clone(), false));
        let state = AppState::new(
            settings,
            ks,
            bus,
            Arc::new(tokio::sync::Mutex::new(
                hypeedge_trading::market_data::BookManager::new(20),
            )),
        );
        let router = crate::build_router(state);
        let mut req = request("/api/v1/strategies", "POST", "203.0.113.10".parse().unwrap());
        req.headers_mut().insert(
            header::AUTHORIZATION,
            "Bearer op-token-1234567890123456".parse().unwrap(),
        );
        // Handler runs (validation error 422 for an empty body), not 403/401.
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }
}
