//! HTTP middleware port of the FastAPI `request_security` middleware in
//! `src/hypeedge/api/app.py`.
//!
//! Applies request-id, security headers, bearer auth, sliding-window rate
//! limits, and idempotency-key enforcement in the same order as Python.

use axum::extract::{Request, State};
use axum::http::header;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
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
    let client_ip = request
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .unwrap_or("unknown")
        .trim()
        .to_string();

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
        // No tokens configured → local-admin.
        actor_role = Some(ApiRole::Admin);
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
