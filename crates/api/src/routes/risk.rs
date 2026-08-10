//! Risk and kill-switch routes, port of `src/hypeedge/api/routes/risk.py`.

use axum::extract::Extension;
use axum::extract::State;
use axum::response::Response;

use crate::auth::{ApiRole, authorize};
use crate::errors::{ApiProblem, ok};
use crate::middleware::RoleGuard;
use crate::state::AppState;
use axum::response::IntoResponse;

/// `GET /api/v1/risk/status`.
pub async fn risk_status(State(state): State<AppState>) -> Response {
    let risk = &state.settings.risk;
    let safety_mode = state.safety_mode.read().await.clone();
    let tracker = &state.account_tracker;
    let drawdown = tracker.drawdown_pct();
    let leverage = tracker.get_leverage();
    let action_credits = match &state.action_budget {
        Some(budget) => {
            let guard = budget.lock().await;
            guard.snapshot().address_remaining.max(0)
        }
        None => 0,
    };
    ok(serde_json::json!({
        "kill_switch_active": state.kill_switch.is_active().await,
        "kill_switch_reason": state.kill_switch.reason().await,
        "safety_mode": safety_mode,
        "safety_reason": safety_mode,
        "limits": [
            {
                "name": "总回撤",
                "current": format!("{:.4}", drawdown),
                "limit": format!("{:.4}", risk.max_drawdown_pct),
                "unit": "%",
                "pct_used": if risk.max_drawdown_pct > 0.0 {
                    format!("{:.2}", drawdown / risk.max_drawdown_pct * 100.0)
                } else {
                    "0".into()
                },
            },
            {
                "name": "最大杠杆",
                "current": format!("{:.2}", leverage),
                "limit": format!("{}", risk.max_leverage),
                "unit": "x",
                "pct_used": if risk.max_leverage > 0 {
                    format!("{:.2}", leverage / risk.max_leverage as f64 * 100.0)
                } else {
                    "0".into()
                },
            },
        ],
        "check_stats": {},
        "strategy_pnl": {},
        "action_credits_remaining": action_credits,
    }))
}

/// `POST /api/v1/kill-switch` — body `{action: "trigger"|"reset", reason?}`.
pub async fn kill_switch(
    State(state): State<AppState>,
    Extension(guard): Extension<RoleGuard>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    // A23: kill-switch is admin-only.
    if let Err(resp) = authorize(guard.0, ApiRole::Admin) {
        return *resp;
    }
    let action = body.get("action").and_then(|a| a.as_str()).unwrap_or("");
    match action {
        "trigger" => {
            let reason = body
                .get("reason")
                .and_then(|r| r.as_str())
                .unwrap_or("manual_trigger_via_api")
                .to_string();
            state.kill_switch.trigger(&reason).await;
            ok(serde_json::json!({ "action": "triggered", "reason": reason }))
        }
        "reset" => {
            if !state.kill_switch.is_active().await {
                return ApiProblem::new(409, "KILL_SWITCH_NOT_ACTIVE", "Kill switch is not active")
                    .into_response();
            }
            // A22: actually clear the latch (was a no-op that reported success).
            state.kill_switch.reset().await;
            ok(serde_json::json!({
                "action": "reset",
                "trading_enabled": !state.kill_switch.is_active().await,
            }))
        }
        other => ApiProblem::new(
            400,
            "INVALID_KILL_SWITCH_ACTION",
            format!("Unknown action: {other}"),
        )
        .into_response(),
    }
}
