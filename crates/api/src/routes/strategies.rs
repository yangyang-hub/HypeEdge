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
};

/// The `AUTO` market symbol (funding-arb scopes the whole account).
const AUTO_MARKET_SYMBOL: &str = "AUTO";

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
            return ApiProblem::new(
                503,
                "MARKET_MAKING_STORE_UNAVAILABLE",
                format!("state store unavailable: {e}"),
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
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    // A23: strategy mutations require Operator.
    if let Err(resp) = authorize(guard.0, ApiRole::Operator) {
        return *resp;
    }
    let strategy_id = body
        .get("strategy_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
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
        return ApiProblem::new(
            409,
            "STRATEGY_CREATE_FAILED",
            format!("failed to persist strategy: {e}"),
        )
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
        return ApiProblem::new(
            409,
            "STRATEGY_CREATE_FAILED",
            format!("failed to persist strategy config: {e}"),
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
        Err(e) => ApiProblem::new(
            503,
            "MARKET_MAKING_STORE_UNAVAILABLE",
            format!("store unavailable: {e}"),
        )
        .into_response(),
    }
}

/// `POST /api/v1/strategies/{strategy_id}/actions/{action}` — start/pause/resume/drain/stop.
pub async fn strategy_action(
    State(state): State<AppState>,
    Extension(guard): Extension<RoleGuard>,
    Path((strategy_id, action)): Path<(String, String)>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    // A23: strategy lifecycle mutations require Operator.
    if let Err(resp) = authorize(guard.0, ApiRole::Operator) {
        return *resp;
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
        Err(e) => ApiProblem::new(
            503,
            "MARKET_MAKING_STORE_UNAVAILABLE",
            format!("config versions unavailable: {e}"),
        )
        .with_retryable(true)
        .into_response(),
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
    let expected_revision = headers
        .get("if-match")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());
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
        Err(e) => ApiProblem::new(409, "CONFIG_VERSION_CONFLICT", e.to_string()).into_response(),
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
    let expected_revision = headers
        .get("if-match")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .or_else(|| body.get("expected_revision").and_then(|v| v.as_u64()));
    match state
        .strategies
        .supervisor
        .activate_config(&strategy_id, version, expected_revision)
        .await
    {
        Ok(()) => ok(serde_json::json!({
            "strategy_id": strategy_id,
            "activated_config_version": version,
        })),
        Err(e) => ApiProblem::new(409, "CONFIG_VERSION_CONFLICT", e).into_response(),
    }
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
    async fn config_version_route_returns_503_when_unwired() {
        let router = crate::build_router(test_state());
        let resp = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/strategies/tf_1/config-versions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
