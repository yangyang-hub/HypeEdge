//! Stable RFC 9457-style API errors, port of `src/hypeedge/api/errors.py`.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde_json::Value;

/// A safe, stable API error that can be returned to clients.
#[derive(Debug, Clone, Serialize)]
pub struct ApiProblem {
    pub status: u16,
    pub code: String,
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retryable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Value>,
}

impl ApiProblem {
    pub fn new(status: u16, code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            detail: detail.into(),
            retryable: None,
            context: None,
        }
    }

    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = Some(retryable);
        self
    }

    pub fn with_context(mut self, context: Value) -> Self {
        self.context = Some(context);
        self
    }
}

/// The full problem+json body (mirrors `problem_response`).
#[derive(Debug, Serialize)]
pub struct ProblemBody {
    #[serde(rename = "type")]
    pub type_: String,
    pub title: String,
    pub status: u16,
    pub code: String,
    pub detail: String,
    pub request_id: String,
    pub retryable: bool,
    pub context: Value,
}

impl IntoResponse for ApiProblem {
    fn into_response(self) -> Response {
        let body = ProblemBody {
            type_: format!(
                "https://hypeedge.local/problems/{}",
                self.code.to_lowercase()
            ),
            title: self.code.clone(),
            status: self.status,
            code: self.code.clone(),
            detail: self.detail.clone(),
            request_id: String::new(), // filled by the middleware response header
            retryable: self.retryable.unwrap_or(false),
            context: self.context.unwrap_or(Value::Object(Default::default())),
        };
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (status, Json(body)).into_response()
    }
}

/// Build a problem body with the request id filled in.
pub fn problem_response(status: u16, code: &str, detail: &str, retryable: bool) -> Response {
    ApiProblem::new(status, code, detail)
        .with_retryable(retryable)
        .into_response()
}

/// The `{ok: true, data}` envelope used by the frontend.
pub fn ok(data: Value) -> Response {
    (
        StatusCode::OK,
        Json(serde_json::json!({ "ok": true, "data": data })),
    )
        .into_response()
}
