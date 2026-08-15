//! Strategy control-plane routes, port of `src/hypeedge/api/routes/market_making.py`.
//!
//! Drives the [`StrategySupervisor`] (Phase 5) behind the frontend's
//! `StrategyInstance` discriminated-union contract. The in-memory state store
//! holds instances/configs; a Postgres-backed store replaces it when the app
//! wiring lands.

use axum::extract::{Extension, Path, State};
use axum::http::HeaderMap;
use axum::response::Response;

use crate::auth::{ApiRole, authorize};
use crate::errors::{ApiProblem, ok};
use crate::middleware::RoleGuard;
use crate::state::AppState;
use axum::response::IntoResponse;
use hypeedge_trading::strategy::{
    default_funding_arb_config, default_market_maker_config, default_trend_follow_config_values,
    normalize_funding_arb_config, normalize_market_maker_config, normalize_trend_follow_config,
    StrategyInstanceDefinition,
};

/// The `AUTO` market symbol (funding-arb scopes the whole account).
const AUTO_MARKET_SYMBOL: &str = "AUTO";

/// Parse an `If-Match` header into an expected revision (P4-5 / M-AP2).
///
/// The frontend sends `If-Match: "\"17\""` — an RFC 9110 quoted ETag — which a
/// bare `parse::<u64>()` rejects, silently disabling the optimistic lock. Strip
/// surrounding quotes and the `W/` weak-validator prefix before parsing. Also
/// accepts a bare number for legacy clients.
fn parse_etag(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("if-match")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_etag_value)
}

fn parse_etag_value(value: &str) -> Option<u64> {
    let trimmed = value.trim();
    let inner = trimmed
        .strip_prefix("W/")
        .or_else(|| trimmed.strip_prefix("w/"))
        .unwrap_or(trimmed)
        .trim();
    // Strip RFC 9110 quotes when present; a bare number is also accepted for
    // legacy clients.
    let inner = if inner.len() >= 2 && inner.starts_with('"') && inner.ends_with('"') {
        &inner[1..inner.len() - 1]
    } else {
        inner
    };
    inner.trim().parse::<u64>().ok()
}

/// Fetch an instance and enforce an If-Match optimistic-lock guard against its
/// revision. Returns `Ok(Some(instance))` when the guard matches (or no guard
/// was supplied), `Ok(None)` when the strategy does not exist, and
/// `Err(response)` with 409/404 on a conflict.
async fn guarded_instance(
    state: &AppState,
    strategy_id: &str,
    expected: Option<u64>,
) -> Result<Option<StrategyInstanceDefinition>, Response> {
    let Some(instance) = state
        .strategies
        .state_store
        .get_instance(strategy_id)
        .await
        .ok()
        .flatten()
    else {
        return Ok(None);
    };
    if let Some(expected) = expected
        && instance.revision != expected
    {
        return Err(ApiProblem::new(
            409,
            "CONFIG_VERSION_CONFLICT",
            format!(
                "Strategy revision changed (expected {expected}, current {}); reload and retry",
                instance.revision
            ),
        )
        .into_response());
    }
    Ok(Some(instance))
}

/// Build a `StrategyInstance`-shaped payload from a definition + runtime.
fn strategy_payload(
    instance: &hypeedge_trading::strategy::StrategyInstanceDefinition,
) -> serde_json::Value {
    serde_json::json!({
        "strategy_id": instance.strategy_id,
        "strategy_type": instance.strategy_type,
        "sub_account": instance.sub_account,
        "symbol": instance.symbol,
        "desired_state": instance.desired_state.as_str(),
        "actual_state": instance.desired_state.as_str(),
        "runtime_reason": null,
        "desired_config_version": instance.desired_config_revision,
        "desired_config_version_id": instance.desired_config_revision,
        "effective_config_version_id": null,
        "revision": instance.revision,
        "archived_at": null,
        "created_at": null,
        "updated_at": null,
    })
}

/// `GET /api/v1/strategies` — list instances (managed control plane).
pub async fn list_strategies(State(state): State<AppState>) -> Response {
    let instances = match state.strategies.state_store.list_instances().await {
        Ok(i) => i,
        Err(e) => {
            // M1: do not leak the store error into the response body.
            tracing::warn!(error = %e, "list_instances_failed");
            return ApiProblem::new(
                503,
                "MARKET_MAKING_STORE_UNAVAILABLE",
                "Strategy state store is unavailable",
            )
            .with_retryable(true)
            .into_response();
        }
    };
    let payloads: Vec<serde_json::Value> = instances.iter().map(strategy_payload).collect();
    ok(serde_json::json!(payloads))
}

/// `POST /api/v1/strategies` — create an instance with an initial config.
pub async fn create_strategy(
    State(state): State<AppState>,
    Extension(guard): Extension<RoleGuard>,
    headers: HeaderMap,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    // A23: strategy mutations require Operator.
    if let Err(resp) = authorize(guard.0, ApiRole::Operator) {
        return *resp;
    }
    // P4-5: an If-Match guard makes creation optimistic — the instance must
    // already exist at the expected revision, otherwise 409/404.
    let strategy_id = body
        .get("strategy_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if !strategy_id.is_empty() {
        let expected = parse_etag(&headers);
        match guarded_instance(&state, &strategy_id, expected).await {
            Ok(Some(_)) => {}
            Ok(None) => {
                if expected.is_some() {
                    return ApiProblem::new(
                        404,
                        "STRATEGY_NOT_FOUND",
                        "Strategy was not found",
                    )
                    .into_response();
                }
            }
            Err(resp) => return resp,
        }
    }
    let strategy_type = body
        .get("strategy_type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let symbol = body
        .get("symbol")
        .and_then(|v| v.as_str())
        .map(|s| {
            if strategy_type == "funding_arb" {
                AUTO_MARKET_SYMBOL
            } else {
                s
            }
        })
        .unwrap_or("")
        .to_string();
    if strategy_id.is_empty() || strategy_type.is_empty() || symbol.is_empty() {
        return ApiProblem::new(
            422,
            "REQUEST_VALIDATION_FAILED",
            "strategy_id, strategy_type, and symbol are required",
        )
        .into_response();
    }
    let config = body
        .get("initial_config")
        .cloned()
        .unwrap_or(serde_json::json!({}));
    let config = merge_config_defaults(&strategy_type, config);
    let config = match strategy_type.as_str() {
        "trend_follow" => normalize_trend_follow_config(&config),
        "market_maker" => normalize_market_maker_config(&config),
        "funding_arb" => normalize_funding_arb_config(&config),
        _ => Err(
            hypeedge_domain::error::HypeEdgeError::StrategyRegistration {
                message: format!("unknown strategy_type {strategy_type}"),
            },
        ),
    };
    let config = match config {
        Ok(c) => c,
        Err(e) => {
            return ApiProblem::new(
                422,
                "REQUEST_VALIDATION_FAILED",
                format!("invalid strategy config: {e}"),
            )
            .into_response();
        }
    };
    let sub_account = if state.settings.exchange.account_address.is_empty() {
        "0x0000000000000000000000000000000000000000".to_string()
    } else {
        state.settings.exchange.account_address.to_lowercase()
    };
    let instance = hypeedge_trading::strategy::StrategyInstanceDefinition {
        strategy_id,
        strategy_type,
        sub_account,
        symbol,
        desired_state: hypeedge_domain::enums::MarketMakerLifecycle::Stopped,
        desired_config_revision: 1,
        revision: 0,
    };
    if let Err(e) = state
        .strategies
        .state_store
        .upsert_instance(&instance)
        .await
    {
        // M1: do not leak the store error into the response body.
        tracing::warn!(error = %e, strategy_id = %instance.strategy_id, "upsert_instance_failed");
        return ApiProblem::new(409, "STRATEGY_CREATE_FAILED", "Failed to persist the strategy")
            .into_response();
    }
    let config_snapshot = hypeedge_trading::strategy::StrategyConfigSnapshot {
        strategy_id: instance.strategy_id.clone(),
        revision: 1,
        values: config,
    };
    if let Err(e) = state
        .strategies
        .state_store
        .upsert_config(&config_snapshot)
        .await
    {
        tracing::warn!(error = %e, strategy_id = %instance.strategy_id, "upsert_config_failed");
        return ApiProblem::new(
            409,
            "STRATEGY_CREATE_FAILED",
            "Failed to persist the strategy config",
        )
        .into_response();
    }
    ok(strategy_payload(&instance))
}

/// `GET /api/v1/strategies/{strategy_id}`.
pub async fn get_strategy(
    State(state): State<AppState>,
    Path(strategy_id): Path<String>,
) -> Response {
    match state
        .strategies
        .state_store
        .get_instance(&strategy_id)
        .await
    {
        Ok(Some(instance)) => ok(strategy_payload(&instance)),
        Ok(None) => {
            ApiProblem::new(404, "STRATEGY_NOT_FOUND", "Strategy was not found").into_response()
        }
        Err(e) => {
            tracing::warn!(error = %e, strategy_id = %strategy_id, "get_instance_failed");
            ApiProblem::new(
                503,
                "MARKET_MAKING_STORE_UNAVAILABLE",
                "Strategy state store is unavailable",
            )
            .into_response()
        }
    }
}

/// `POST /api/v1/strategies/{strategy_id}/actions/{action}` — start/pause/resume/drain/stop.
pub async fn strategy_action(
    State(state): State<AppState>,
    Extension(guard): Extension<RoleGuard>,
    headers: HeaderMap,
    Path((strategy_id, action)): Path<(String, String)>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    // A23: strategy lifecycle mutations require Operator.
    if let Err(resp) = authorize(guard.0, ApiRole::Operator) {
        return *resp;
    }
    // P4-5: If-Match guards the lifecycle transition (the supervisor ignores
    // expected_revision on start, so enforce it here against the instance).
    if let Err(resp) = guarded_instance(&state, &strategy_id, parse_etag(&headers)).await {
        return resp;
    }
    use hypeedge_domain::enums::MarketMakerLifecycle;
    if !matches!(
        action.as_str(),
        "start" | "pause" | "resume" | "drain" | "stop"
    ) {
        return ApiProblem::new(
            404,
            "STRATEGY_ACTION_NOT_FOUND",
            "Unsupported strategy lifecycle action",
        )
        .into_response();
    }
    let target_str = body
        .get("target")
        .or_else(|| body.get("target_state"))
        .and_then(|v| v.as_str());
    let target = match target_str {
        Some("shadow") => MarketMakerLifecycle::Shadow,
        Some("running") | None => MarketMakerLifecycle::Running,
        Some(other) => {
            return ApiProblem::new(
                400,
                "INVALID_TARGET",
                format!("unsupported target: {other}"),
            )
            .into_response();
        }
    };
    let supervisor = state.strategies.supervisor.clone();
    let result = match action.as_str() {
        "start" => supervisor.start(&strategy_id, target, None).await,
        "resume" => supervisor.resume(&strategy_id, target).await,
        "pause" => supervisor.pause(&strategy_id).await,
        "drain" => supervisor.drain(&strategy_id).await,
        "stop" => match supervisor.stop(&strategy_id).await {
            Ok(()) => {
                return ok(
                    serde_json::json!({ "strategy_id": strategy_id, "actual_state": "stopped" }),
                );
            }
            Err(e) => Err(e),
        },
        _ => unreachable!(),
    };
    match result {
        Ok(runtime) => ok(serde_json::json!({
            "strategy_id": runtime.strategy_id,
            "actual_state": runtime.actual_state.as_str(),
            "reason": runtime.reason,
            "revision": runtime.revision,
        })),
        Err(e) => ApiProblem::new(409, "STRATEGY_LIFECYCLE_CONFLICT", e).into_response(),
    }
}

/// Build the frontend config-version payload (mirrors `_config_payload`).
fn config_version_payload(record: &hypeedge_storage::ConfigVersionRecord) -> serde_json::Value {
    let mut public_values = record.values.clone();
    // funding_arb config carries the internal spot_coin key; the frontend
    // contract strips it (the strategy id already scopes the market).
    if let Some(obj) = public_values.as_object_mut() {
        obj.remove("spot_coin");
    }
    serde_json::json!({
        "id": record.version,
        "version": record.version,
        "config_hash": record.config_hash,
        "config": public_values,
        "created_by": record.created_by,
        "created_at": record.created_at.map(|t| t.to_rfc3339()),
        "approved_by": null,
        "approved_at": null,
        "shadow_preview": null,
    })
}

/// `GET /api/v1/strategies/{strategy_id}/config-versions`
pub async fn list_config_versions(
    State(state): State<AppState>,
    Path(strategy_id): Path<String>,
) -> Response {
    let Some(store) = &state.config_versions else {
        return ApiProblem::new(
            503,
            "MARKET_MAKING_STORE_UNAVAILABLE",
            "Config versions are unavailable",
        )
        .with_retryable(true)
        .into_response();
    };
    match store.list_config_versions(&strategy_id).await {
        Ok(versions) => {
            let payloads: Vec<serde_json::Value> =
                versions.iter().map(config_version_payload).collect();
            ok(serde_json::json!(payloads))
        }
        Err(e) => {
            // M1: do not leak the store error into the response body.
            tracing::warn!(error = %e, strategy_id = %strategy_id, "list_config_versions_failed");
            ApiProblem::new(
                503,
                "MARKET_MAKING_STORE_UNAVAILABLE",
                "Config versions are unavailable",
            )
            .with_retryable(true)
            .into_response()
        }
    }
}

/// `POST /api/v1/strategies/{strategy_id}/config-versions`
///
/// Body: `{ "strategy_type": "...", "config": {...} }`. Optional `If-Match`
/// header carries the expected instance revision for the optimistic lock.
pub async fn create_config_version(
    State(state): State<AppState>,
    Extension(guard): Extension<RoleGuard>,
    headers: HeaderMap,
    Path(strategy_id): Path<String>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    // A23: config mutations require Operator.
    if let Err(resp) = authorize(guard.0, ApiRole::Operator) {
        return *resp;
    }
    let Some(store) = &state.config_versions else {
        return ApiProblem::new(
            503,
            "MARKET_MAKING_STORE_UNAVAILABLE",
            "Config creation is unavailable",
        )
        .with_retryable(true)
        .into_response();
    };
    let strategy_type = body
        .get("strategy_type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let config = body.get("config").cloned().unwrap_or(serde_json::json!({}));
    let config = merge_config_defaults(&strategy_type, config);
    if strategy_type.is_empty() {
        return ApiProblem::new(
            422,
            "REQUEST_VALIDATION_FAILED",
            "strategy_type is required",
        )
        .into_response();
    }
    let config = match strategy_type.as_str() {
        "trend_follow" => normalize_trend_follow_config(&config),
        "market_maker" => normalize_market_maker_config(&config),
        "funding_arb" => normalize_funding_arb_config(&config),
        _ => Err(
            hypeedge_domain::error::HypeEdgeError::StrategyRegistration {
                message: format!("unknown strategy_type {strategy_type}"),
            },
        ),
    };
    let config = match config {
        Ok(c) => c,
        Err(e) => {
            return ApiProblem::new(
                422,
                "REQUEST_VALIDATION_FAILED",
                format!("invalid strategy config: {e}"),
            )
            .into_response();
        }
    };
    // P4-5: parse the quoted If-Match ETag (`"17"`) so the optimistic lock
    // actually engages instead of silently degrading to `None`.
    let expected_revision = parse_etag(&headers);
    match store
        .create_config_version(
            &strategy_id,
            &strategy_type,
            &config,
            "api",
            expected_revision,
        )
        .await
    {
        Ok(record) => ok(serde_json::json!({ "data": config_version_payload(&record) })),
        Err(e) => {
            // M1: log the store error; keep a fixed conflict message.
            tracing::warn!(error = %e, strategy_id = %strategy_id, "create_config_version_failed");
            ApiProblem::new(
                409,
                "CONFIG_VERSION_CONFLICT",
                "Config version conflict; reload and retry",
            )
            .into_response()
        }
    }
}

fn merge_config_defaults(strategy_type: &str, config: serde_json::Value) -> serde_json::Value {
    let defaults = match strategy_type {
        "trend_follow" => default_trend_follow_config_values(),
        "market_maker" => default_market_maker_config(),
        "funding_arb" => default_funding_arb_config(),
        _ => serde_json::json!({}),
    };
    let mut merged = defaults.as_object().cloned().unwrap_or_default();
    if let Some(supplied) = config.as_object() {
        for (k, v) in supplied {
            merged.insert(k.clone(), v.clone());
        }
    }
    serde_json::Value::Object(merged)
}

/// `POST /api/v1/strategies/{strategy_id}/config-versions/{version}/activate`.
pub async fn activate_config_version(
    State(state): State<AppState>,
    Extension(guard): Extension<RoleGuard>,
    headers: HeaderMap,
    Path((strategy_id, version)): Path<(String, u64)>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    // A23: config activation requires Operator.
    if let Err(resp) = authorize(guard.0, ApiRole::Operator) {
        return *resp;
    }
    let expected_revision = parse_etag(&headers)
        .or_else(|| body.get("expected_revision").and_then(|v| v.as_u64()));
    activate_version(&state, &strategy_id, version, expected_revision, "activated").await
}

/// `POST /api/v1/strategies/{strategy_id}/config-versions/{version}/rollback`
/// (P4-1 / M3).
///
/// Rolls the effective config back to `version` — the frontend picks the
/// target version explicitly (see `rollbackMarketMakerConfig`), so this is
/// activation of that version, i.e. the same optimistic-lock path. If the
/// intended semantics were "previous relative to the active version", the
/// config-version store would resolve that; the client-driven target keeps
/// the API contract stable and undo-safe.
pub async fn rollback_config_version(
    State(state): State<AppState>,
    Extension(guard): Extension<RoleGuard>,
    headers: HeaderMap,
    Path((strategy_id, version)): Path<(String, u64)>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    // A23: config rollback requires Operator.
    if let Err(resp) = authorize(guard.0, ApiRole::Operator) {
        return *resp;
    }
    let expected_revision = parse_etag(&headers)
        .or_else(|| body.get("expected_revision").and_then(|v| v.as_u64()));
    activate_version(&state, &strategy_id, version, expected_revision, "rolled_back").await
}

/// Shared activate/rollback: drive the supervisor and shape the response.
async fn activate_version(
    state: &AppState,
    strategy_id: &str,
    version: u64,
    expected_revision: Option<u64>,
    verb: &'static str,
) -> Response {
    match state
        .strategies
        .supervisor
        .activate_config(strategy_id, version, expected_revision)
        .await
    {
        Ok(()) => {
            // Dynamic key (`activated_config_version` / `rolled_back_config_version`)
            // needs an explicit map — the `json!` macro only takes literal keys.
            let mut payload = serde_json::Map::new();
            payload.insert("strategy_id".into(), serde_json::json!(strategy_id));
            payload.insert(
                format!("{verb}_config_version"),
                serde_json::json!(version),
            );
            ok(serde_json::Value::Object(payload))
        }
        Err(e) => {
            tracing::warn!(error = %e, strategy_id = %strategy_id, version, "activate_config_failed");
            ApiProblem::new(
                409,
                "CONFIG_VERSION_CONFLICT",
                "Config version conflict; reload and retry",
            )
            .into_response()
        }
    }
}

/// `POST /api/v1/strategies/{strategy_id}/archive` (P4-1).
///
/// The supervisor has no dedicated archive operation, so archiving a fully
/// stopped strategy is modelled as `stop` semantics (desired → stopped,
/// runtime handle released, allocation released) while the instance and its
/// config/history stay in the store — which is what "archive" means to the
/// frontend (`archiveStrategy` keeps trading and audit history).
pub async fn archive_strategy(
    State(state): State<AppState>,
    Extension(guard): Extension<RoleGuard>,
    headers: HeaderMap,
    Path(strategy_id): Path<String>,
) -> Response {
    // A23: strategy lifecycle mutations require Operator.
    if let Err(resp) = authorize(guard.0, ApiRole::Operator) {
        return *resp;
    }
    // P4-5: the frontend sends the instance revision as If-Match.
    let expected = parse_etag(&headers);
    let instance = match guarded_instance(&state, &strategy_id, expected).await {
        Ok(Some(i)) => i,
        Ok(None) => {
            return ApiProblem::new(404, "STRATEGY_NOT_FOUND", "Strategy was not found")
                .into_response();
        }
        Err(resp) => return resp,
    };
    if let Err(e) = state.strategies.supervisor.stop(&strategy_id).await {
        tracing::warn!(error = %e, strategy_id = %strategy_id, "archive_stop_failed");
        return ApiProblem::new(409, "STRATEGY_LIFECYCLE_CONFLICT", e).into_response();
    }
    // Reload the instance after stop (revision bumped by set_desired).
    let archived = state
        .strategies
        .state_store
        .get_instance(&strategy_id)
        .await
        .ok()
        .flatten()
        .unwrap_or(instance);
    ok(serde_json::json!({
        "strategy_id": archived.strategy_id,
        "strategy_type": archived.strategy_type,
        "symbol": archived.symbol,
        "sub_account": archived.sub_account,
        "desired_state": archived.desired_state.as_str(),
        "actual_state": "stopped",
        "revision": archived.revision,
        "archived_at": chrono::Utc::now().to_rfc3339(),
    }))
}

#[cfg(test)]
mod config_version_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::sync::Arc;
    use tower::ServiceExt;

    fn test_state() -> AppState {
        let settings = Arc::new(hypeedge_config::settings::AppSettings::default());
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

    #[tokio::test]
    async fn config_versions_return_503_without_store() {
        let state = test_state();
        // No config_versions wired → 503 MARKET_MAKING_STORE_UNAVAILABLE.
        let resp = list_config_versions(State(state), Path("tf_1".into())).await;
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&body).to_string();
        assert!(
            text.contains("MARKET_MAKING_STORE_UNAVAILABLE"),
            "body: {text}"
        );
    }

    #[tokio::test]
    async fn create_config_version_returns_503_without_store() {
        let state = test_state();
        let resp = create_config_version(
            State(state),
            Extension(RoleGuard(crate::auth::ApiRole::Admin)),
            HeaderMap::new(),
            Path("tf_1".into()),
            axum::Json(serde_json::json!({
                "strategy_type": "trend_follow",
                "config": {"fast_ema_period": 12}
            })),
        )
        .await;
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&body).to_string();
        assert!(
            text.contains("MARKET_MAKING_STORE_UNAVAILABLE"),
            "body: {text}"
        );
    }

    #[tokio::test]
    async fn config_version_route_denies_unauthenticated_mutation() {
        // P5-1: with no tokens configured the empty-token role is Viewer, so
        // the mutation is rejected with 403 before reaching the store check
        // (the route still returns 503 for an authenticated Operator).
        let router = crate::build_router(test_state());
        let resp = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .header("content-type", "application/json")
                    .uri("/api/v1/strategies/tf_1/config-versions")
                    .header("idempotency-key", "test-key-0001")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // --- P4-5: If-Match optimistic lock ---

    #[test]
    fn parse_etag_accepts_quoted_weak_and_bare_values() {
        let mut headers = HeaderMap::new();
        headers.insert("if-match", "\"17\"".parse().unwrap());
        assert_eq!(parse_etag(&headers), Some(17));
        headers.insert("if-match", "W/\"42\"".parse().unwrap());
        assert_eq!(parse_etag(&headers), Some(42));
        headers.insert("if-match", "17".parse().unwrap());
        assert_eq!(parse_etag(&headers), Some(17));
        headers.insert("if-match", "\"abc\"".parse().unwrap());
        assert_eq!(parse_etag(&headers), None);
        headers.insert("if-match", "\"1\", \"2\"".parse().unwrap());
        assert_eq!(parse_etag(&headers), None);
        let empty = HeaderMap::new();
        assert_eq!(parse_etag(&empty), None);
    }

    #[tokio::test]
    async fn stale_if_match_etag_yields_409_on_activate() {
        // End-to-end through the router: create → start (bumps revisions) →
        // activate with the stale quoted ETag → 409 CONFIG_VERSION_CONFLICT.
        let mut settings = hypeedge_config::settings::AppSettings::default();
        settings.api.operator_token = "op-token-1234567890123456".into();
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
        let auth = "Bearer op-token-1234567890123456";

        // Create (instance revision 0, config revision 1).
        let create = Request::builder()
            .method("POST")
            .header("content-type", "application/json")
            .uri("/api/v1/strategies")
            .header("idempotency-key", "test-key-0001")
            .header("authorization", auth)
            .body(Body::from(
                serde_json::json!({
                    "strategy_id": "tf_lock_1",
                    "strategy_type": "trend_follow",
                    "symbol": "BTC",
                    "initial_config": {}
                })
                .to_string(),
            ))
            .unwrap();
        let resp = router.clone().oneshot(create).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Start (set_desired/set_runtime bump revisions past 0).
        let start = Request::builder()
            .method("POST")
            .header("content-type", "application/json")
            .uri("/api/v1/strategies/tf_lock_1/actions/start")
            .header("idempotency-key", "test-key-0002")
            .header("authorization", auth)
            .header("if-match", "\"0\"")
            .body(Body::from(r#"{"target":"running"}"#))
            .unwrap();
        let resp = router.clone().oneshot(start).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "start must succeed");

        // Activate version 1 with the now-stale quoted ETag "0" → 409.
        let activate = Request::builder()
            .method("POST")
            .header("content-type", "application/json")
            .uri("/api/v1/strategies/tf_lock_1/config-versions/1/activate")
            .header("idempotency-key", "test-key-0003")
            .header("authorization", auth)
            .header("if-match", "\"0\"")
            .body(Body::from(r#"{"confirmation":"activate"}"#))
            .unwrap();
        let resp = router.clone().oneshot(activate).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("CONFIG_VERSION_CONFLICT"));

        // A fresh If-Match (current revision) succeeds.
        let current = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/strategies/tf_lock_1")
                    .header("authorization", auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(current.into_body(), 64 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let revision = json["data"]["revision"].as_u64().unwrap();
        let activate_ok = Request::builder()
            .method("POST")
            .header("content-type", "application/json")
            .uri("/api/v1/strategies/tf_lock_1/config-versions/1/activate")
            .header("idempotency-key", "test-key-0004")
            .header("authorization", auth)
            .header("if-match", format!("\"{revision}\""))
            .body(Body::from(r#"{"confirmation":"activate"}"#))
            .unwrap();
        let resp = router.clone().oneshot(activate_ok).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "fresh ETag must succeed");
    }

    #[tokio::test]
    async fn rollback_route_activates_target_version() {
        let mut settings = hypeedge_config::settings::AppSettings::default();
        settings.api.operator_token = "op-token-1234567890123456".into();
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
        let auth = "Bearer op-token-1234567890123456";
        // Create + start so a runtime exists for activate.
        let create = Request::builder()
            .method("POST")
            .header("content-type", "application/json")
            .uri("/api/v1/strategies")
            .header("idempotency-key", "test-key-0001")
            .header("authorization", auth)
            .body(Body::from(
                serde_json::json!({
                    "strategy_id": "tf_rb_1",
                    "strategy_type": "trend_follow",
                    "symbol": "BTC",
                    "initial_config": {}
                })
                .to_string(),
            ))
            .unwrap();
        assert_eq!(router.clone().oneshot(create).await.unwrap().status(), StatusCode::OK);
        let start = Request::builder()
            .method("POST")
            .header("content-type", "application/json")
            .uri("/api/v1/strategies/tf_rb_1/actions/start")
            .header("idempotency-key", "test-key-0002")
            .header("authorization", auth)
            .body(Body::from(r#"{"target":"running"}"#))
            .unwrap();
        assert_eq!(router.clone().oneshot(start).await.unwrap().status(), StatusCode::OK);

        let rollback = Request::builder()
            .method("POST")
            .header("content-type", "application/json")
            .uri("/api/v1/strategies/tf_rb_1/config-versions/1/rollback")
            .header("idempotency-key", "test-key-0003")
            .header("authorization", auth)
            .body(Body::from(r#"{"confirmation":"rollback"}"#))
            .unwrap();
        let resp = router.oneshot(rollback).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["rolled_back_config_version"], 1);
    }

    #[tokio::test]
    async fn archive_route_stops_and_reports_revision() {
        let mut settings = hypeedge_config::settings::AppSettings::default();
        settings.api.operator_token = "op-token-1234567890123456".into();
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
        let auth = "Bearer op-token-1234567890123456";
        let create = Request::builder()
            .method("POST")
            .header("content-type", "application/json")
            .uri("/api/v1/strategies")
            .header("idempotency-key", "test-key-0001")
            .header("authorization", auth)
            .body(Body::from(
                serde_json::json!({
                    "strategy_id": "tf_arc_1",
                    "strategy_type": "trend_follow",
                    "symbol": "BTC",
                    "initial_config": {}
                })
                .to_string(),
            ))
            .unwrap();
        assert_eq!(router.clone().oneshot(create).await.unwrap().status(), StatusCode::OK);

        let archive = Request::builder()
            .method("POST")
            .header("content-type", "application/json")
            .uri("/api/v1/strategies/tf_arc_1/archive")
            .header("idempotency-key", "test-key-0002")
            .header("authorization", auth)
            .header("if-match", "\"0\"")
            .body(Body::from("{}"))
            .unwrap();
        let resp = router.clone().oneshot(archive).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["strategy_id"], "tf_arc_1");
        assert_eq!(json["data"]["actual_state"], "stopped");
        assert!(!json["data"]["archived_at"].is_null());

        // A stale If-Match after the revision bumped → 409.
        let archive_again = Request::builder()
            .method("POST")
            .header("content-type", "application/json")
            .uri("/api/v1/strategies/tf_arc_1/archive")
            .header("idempotency-key", "test-key-0003")
            .header("authorization", auth)
            .header("if-match", "\"0\"")
            .body(Body::from("{}"))
            .unwrap();
        let resp = router.oneshot(archive_again).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }
}
