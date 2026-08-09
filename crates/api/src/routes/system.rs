//! System and bootstrap routes, port of `src/hypeedge/api/routes/system.py`.

use axum::extract::State;
use axum::response::Response;

use crate::errors::ok;
use crate::state::AppState;

/// `GET /health` — liveness + trading gate.
pub async fn health(State(state): State<AppState>) -> Response {
    ok(serde_json::json!({
        "status": "ok",
        "trading_enabled": *state.trading_enabled.read().await,
        "environment": state.environment(),
        "api_v1_enabled": true,
    }))
}

/// `GET /api/v1/system/status`.
pub async fn system_status(State(state): State<AppState>) -> Response {
    let kill_active = state.kill_switch.is_active().await;
    let kill_reason = state.kill_switch.reason().await;
    let safety_mode = state.safety_mode.read().await.clone();
    let features = &state.settings.features;
    ok(serde_json::json!({
        "environment": state.environment(),
        "trading_enabled": *state.trading_enabled.read().await,
        "kill_switch_active": kill_active,
        "kill_switch_reason": kill_reason,
        "safety_mode": safety_mode,
        "safety_reason": safety_mode,
        "shutting_down": false,
        "meta_loaded": false,
        "features": {
            "durable_ledger_v2": features.durable_ledger_v2,
            "execution_v2": features.execution_v2,
            "user_stream_v2": features.user_stream_v2,
            "reconciliation_v2": features.reconciliation_v2,
            "api_v1": true,
            "strategy_runner_v2": features.strategy_runner_v2,
        },
        "canary": {
            "evaluator_ready": true,
            "directive": "halted",
            "reasons": ["canary_not_activated"],
        },
    }))
}

/// `GET /api/v1/bootstrap`.
pub async fn bootstrap(State(state): State<AppState>) -> Response {
    let now = chrono::Utc::now();
    ok(serde_json::json!({
        "system": {
            "environment": state.environment(),
            "trading_enabled": *state.trading_enabled.read().await,
            "kill_switch_active": state.kill_switch.is_active().await,
            "kill_switch_reason": state.kill_switch.reason().await,
            "safety_mode": *state.safety_mode.read().await,
            "safety_reason": *state.safety_mode.read().await,
            "shutting_down": false,
            "meta_loaded": false,
        },
        "positions": [],
        "server_time": now.to_rfc3339(),
    }))
}
