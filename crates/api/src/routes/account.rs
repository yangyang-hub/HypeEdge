//! Account / positions / orders routes.
//!
//! The `/account` and `/positions` routes read the live [`AccountTracker`]
//! (updated by clearinghouse polling and fill processing). When no account
//! state has been observed yet, an empty snapshot is returned so the dashboard
//! renders quietly instead of retry-spamming. All money/price fields are
//! decimal strings (frontend contract).

use axum::extract::{Extension, State};
use axum::response::Response;

use crate::auth::{authorize, ApiRole};
use crate::middleware::RoleGuard;
use crate::errors::{ApiProblem, ok};
use crate::state::AppState;
use axum::response::IntoResponse;
use hypeedge_domain::traits::ExecutionClient;

/// `GET /api/v1/account`.
pub async fn account(State(state): State<AppState>) -> Response {
    let tracker = &state.account_tracker;
    let trading_enabled = *state.trading_enabled.read().await;

    let acct = tracker.get_account_state();
    let (equity, available, margin_used, unrealized, peak, drawdown_pct, leverage, last_update) =
        match &acct {
            Some(acct) => (
                acct.equity.to_string(),
                acct.available_balance.to_string(),
                acct.total_margin_used.to_string(),
                acct.total_unrealized_pnl.to_string(),
                acct.peak_equity.to_string(),
                format!("{:.18}", acct.drawdown_pct()),
                format!("{:.6}", tracker.get_leverage()),
                tracker.last_update_ts().map(|t| t.to_rfc3339()),
            ),
            None => (
                "0".into(),
                "0".into(),
                "0".into(),
                "0".into(),
                "0".into(),
                "0".into(),
                "0".into(),
                None,
            ),
        };

    ok(serde_json::json!({
        "equity": equity,
        "available_balance": available,
        "total_margin_used": margin_used,
        "total_unrealized_pnl": unrealized,
        "peak_equity": peak,
        "drawdown_pct": drawdown_pct,
        "leverage": leverage,
        "total_fees": tracker.total_fees().to_string(),
        "total_funding": tracker.total_funding().to_string(),
        "fill_count": tracker.fill_count(),
        "position_count": tracker.get_all_positions().len(),
        "last_update": last_update,
        "trading_enabled": trading_enabled,
    }))
}

/// `GET /api/v1/account/equity-curve?days=30`.
pub async fn equity_curve(State(state): State<AppState>) -> Response {
    let equity = state.account_tracker.current_equity();
    let now_ms = chrono::Utc::now().timestamp_millis();
    ok(serde_json::json!([
        { "timestamp": now_ms, "equity": equity.to_string() }
    ]))
}

/// `GET /api/v1/positions`.
pub async fn positions(State(state): State<AppState>) -> Response {
    let positions: Vec<serde_json::Value> = state
        .account_tracker
        .get_all_positions()
        .into_iter()
        .map(|p| {
            serde_json::json!({
                "symbol": p.symbol,
                "size": p.size.to_string(),
                "entry_price": p.entry_price.map(|v| v.to_string()),
                "mark_price": p.mark_price.map(|v| v.to_string()),
                "unrealized_pnl": p.unrealized_pnl.map(|v| v.to_string()),
                "leverage": p.leverage,
                "liquidation_price": p.liquidation_price.map(|v| v.to_string()),
                "is_long": p.is_long(),
                "is_short": p.is_short(),
                // B12: the frontend keys on `side`; derive it from the size sign.
                "side": if p.is_long() { "long" } else if p.is_short() { "short" } else { "flat" },
            })
        })
        .collect();
    ok(serde_json::json!(positions))
}

/// `POST /api/v1/positions/{symbol}/close` — 202 Accepted.
pub async fn close_position(
    State(state): State<AppState>,
    Extension(guard): Extension<RoleGuard>,
    axum::extract::Path(symbol): axum::extract::Path<String>,
) -> Response {
    // A23: trading mutations require Operator.
    if let Err(resp) = authorize(guard.0, ApiRole::Operator) {
        return *resp;
    }
    // 6e: close the position via the execution engine (reduce-only market order
    // sized to the current position).
    let Some(execution) = &state.execution else {
        return ApiProblem::new(
            503,
            "EXECUTION_UNAVAILABLE",
            "Execution engine is not wired",
        )
        .into_response();
    };
    let position = state.account_tracker.get_position(&symbol);
    let Some(position) = position else {
        return ApiProblem::new(404, "POSITION_NOT_FOUND", "Position was not found")
            .into_response();
    };
    let intent = hypeedge_domain::models::OrderIntent {
        symbol: position.symbol.clone(),
        side: if position.is_long() {
            hypeedge_domain::enums::Side::Sell
        } else {
            hypeedge_domain::enums::Side::Buy
        },
        size: hypeedge_domain::decimal::Size::new(position.size.inner().abs()),
        price: None,
        order_type: hypeedge_domain::enums::OrderType::Market,
        time_in_force: hypeedge_domain::enums::TimeInForce::Ioc,
        strategy_id: None,
        sub_account: None,
        reduce_only: true,
        cloid: None,
        client_id: None,
        is_spot: false,
        risk_reducing: false,
        max_slippage_bps: 50,
    };
    match execution.submit_order(intent, None).await {
        Ok(order) => ok(serde_json::json!({
            "accepted": true,
            "cloid": order.cloid,
            "status": order.status.as_str(),
        })),
        Err(e) => ApiProblem::new(
            502,
            "ORDER_SUBMIT_FAILED",
            format!("close failed: {e}"),
        )
        .into_response(),
    }
}

/// `GET /api/v1/orders?status=active`.
pub async fn orders(State(state): State<AppState>) -> Response {
    let Some(execution) = &state.execution else {
        return ok(serde_json::json!([]));
    };
    match execution.get_open_orders(None).await {
        Ok(orders) => ok(serde_json::json!(orders
            .into_iter()
            .map(order_payload)
            .collect::<Vec<_>>())),
        Err(e) => ApiProblem::new(
            502,
            "ORDER_QUERY_FAILED",
            format!("orders failed: {e}"),
        )
        .into_response(),
    }
}

/// `POST /api/v1/orders` — submit an order through the execution engine.
pub async fn submit_order(
    State(state): State<AppState>,
    Extension(guard): Extension<RoleGuard>,
    body: axum::Json<serde_json::Value>,
) -> Response {
    // A23: trading mutations require Operator.
    if let Err(resp) = authorize(guard.0, ApiRole::Operator) {
        return *resp;
    }
    let Some(execution) = &state.execution else {
        return ApiProblem::new(
            503,
            "EXECUTION_UNAVAILABLE",
            "Execution engine is not wired",
        )
        .into_response();
    };
    let intent = match intent_from_json(&body) {
        Ok(i) => i,
        Err(msg) => {
            return ApiProblem::new(422, "REQUEST_VALIDATION_FAILED", msg).into_response();
        }
    };
    match execution.submit_order(intent, None).await {
        Ok(order) => ok(serde_json::json!({
            "cloid": order.cloid,
            "status": order.status.as_str(),
            "symbol": order.symbol,
        })),
        Err(e) => ApiProblem::new(
            502,
            "ORDER_SUBMIT_FAILED",
            format!("submit failed: {e}"),
        )
        .into_response(),
    }
}

/// `POST /api/v1/orders/{cloid}/cancel` — 202 Accepted.
pub async fn cancel_order(
    State(state): State<AppState>,
    Extension(guard): Extension<RoleGuard>,
    axum::extract::Path(cloid): axum::extract::Path<String>,
) -> Response {
    // A23: trading mutations require Operator.
    if let Err(resp) = authorize(guard.0, ApiRole::Operator) {
        return *resp;
    }
    let Some(execution) = &state.execution else {
        return ApiProblem::new(
            503,
            "EXECUTION_UNAVAILABLE",
            "Execution engine is not wired",
        )
        .into_response();
    };
    match execution.cancel_order(&cloid).await {
        Ok(accepted) => ok(serde_json::json!({ "accepted": accepted, "cloid": cloid })),
        Err(e) => ApiProblem::new(
            502,
            "ORDER_CANCEL_FAILED",
            format!("cancel failed: {e}"),
        )
        .into_response(),
    }
}

/// Build an `OrderIntent` from the JSON body contract:
/// `{ symbol, side, size, price?, order_type, time_in_force? }`.
fn intent_from_json(body: &serde_json::Value) -> Result<hypeedge_domain::models::OrderIntent, String> {
    use hypeedge_domain::decimal::{Price, Size};
    use hypeedge_domain::enums::{OrderType, Side, TimeInForce};
    let symbol = body
        .get("symbol")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if symbol.is_empty() {
        return Err(String::from("symbol is required"));
    }
    let side = match body.get("side").and_then(|v| v.as_str()) {
        Some("buy") => Side::Buy,
        Some("sell") => Side::Sell,
        _ => return Err(String::from("side must be buy or sell")),
    };
    let size = body
        .get("size")
        .and_then(|v| v.as_str())
        .and_then(|s| hypeedge_domain::decimal::Decimal::from_str_lenient(s).ok())
        .map(Size::new)
        .ok_or_else(|| String::from("size must be a decimal string"))?;
    let price = body
        .get("price")
        .and_then(|v| v.as_str())
        .and_then(|s| hypeedge_domain::decimal::Decimal::from_str_lenient(s).ok())
        .map(Price::new);
    let order_type = match body.get("order_type").and_then(|v| v.as_str()) {
        Some("market") => OrderType::Market,
        Some("limit") => OrderType::Limit,
        _ => return Err(String::from("order_type must be market or limit")),
    };
    let time_in_force = match body.get("time_in_force").and_then(|v| v.as_str()) {
        Some("ioc") => TimeInForce::Ioc,
        Some("alo") => TimeInForce::Alo,
        _ => TimeInForce::Gtc,
    };
    Ok(hypeedge_domain::models::OrderIntent {
        symbol,
        side,
        size,
        price,
        order_type,
        time_in_force,
        strategy_id: None,
        sub_account: None,
        reduce_only: false,
        cloid: None,
        client_id: None,
        is_spot: false,
        risk_reducing: false,
        max_slippage_bps: 50,
    })
}

/// Serialize an order for the frontend contract.
fn order_payload(order: hypeedge_domain::models::Order) -> serde_json::Value {
    serde_json::json!({
        "cloid": order.cloid,
        "symbol": order.symbol,
        "side": order.side.as_str(),
        "size": order.size.to_string(),
        "price": order.price.map(|p| p.to_string()),
        "order_type": order.order_type.as_str(),
        "status": order.status.as_str(),
        "filled_size": order.filled_size.to_string(),
        "avg_fill_price": order.avg_fill_price.map(|p| p.to_string()),
        "strategy_id": order.strategy_id,
        "error_message": order.error_message,
        "created_at": order.created_at.to_rfc3339(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use hypeedge_domain::decimal::{Decimal as D, Price, Size, Usd};
    use hypeedge_domain::enums::Side;
    use hypeedge_domain::models::{AccountState, Fill, Position};

    fn make_state() -> AppState {
        let settings = Arc::new(hypeedge_config::settings::AppSettings::default());
        let bus = Arc::new(hypeedge_infra::event_bus::EventBus::new(64));
        let ks = Arc::new(hypeedge_trading::risk::KillSwitch::new(bus.clone(), false));
        AppState::new(
            settings,
            ks,
            bus,
            Arc::new(tokio::sync::Mutex::new(hypeedge_trading::market_data::BookManager::new(20))),
        )
    }

    fn seed_tracker(state: &AppState) {
        let tracker = &state.account_tracker;
        tracker.update_account_state(&AccountState {
            equity: Usd::new(D::from_str_strict("10000").unwrap()),
            available_balance: Usd::new(D::from_str_strict("9000").unwrap()),
            total_margin_used: Usd::new(D::from_str_strict("500").unwrap()),
            total_unrealized_pnl: Usd::new(D::from_str_strict("100").unwrap()),
            peak_equity: Usd::ZERO, // normalized to running peak by the tracker
            sub_account: None,
        });
        tracker.update_position_from_exchange(
            "BTC",
            Position {
                symbol: "BTC".into(),
                size: Size::new(D::from_str_strict("0.5").unwrap()),
                entry_price: Some(Price::new(D::from_str_strict("50000").unwrap())),
                mark_price: Some(Price::new(D::from_str_strict("51000").unwrap())),
                unrealized_pnl: Some(Usd::new(D::from_str_strict("500").unwrap())),
                leverage: 3,
                liquidation_price: None,
                sub_account: None,
                strategy_id: None,
            },
        );
        // A spot fill exercises fee/count accounting without creating a perp
        // position, so the BTC position assertions below stay exact.
        tracker.update_fill(
            &Fill {
                cloid: "c1".into(),
                exchange_oid: "o1".into(),
                symbol: "USDC".into(),
                side: Side::Buy,
                price: Price::new(D::from_str_strict("1").unwrap()),
                size: Size::new(D::from_str_strict("100").unwrap()),
                fee: Usd::new(D::from_str_strict("0.25").unwrap()),
                is_maker: false,
                timestamp: chrono::Utc::now().timestamp_millis(),
                strategy_id: None,
                sub_account: None,
                is_spot: true,
            },
            false,
        );
    }

    #[tokio::test]
    async fn account_returns_live_tracker_values() {
        let state = make_state();
        seed_tracker(&state);
        *state.trading_enabled.write().await = true;
        let body = account(State(state)).await;
        let body = axum::body::to_bytes(body.into_body(), 64 * 1024).await.unwrap();
        let text = String::from_utf8_lossy(&body).to_string();
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        let data = &json["data"];
        assert_eq!(data["equity"], "10000");
        assert_eq!(data["available_balance"], "9000");
        assert_eq!(data["peak_equity"], "10000");
        assert_eq!(data["total_fees"], "0.25");
        assert_eq!(data["fill_count"], 1);
        assert_eq!(data["position_count"], 1);
        assert_eq!(data["trading_enabled"], true);
    }

    #[tokio::test]
    async fn account_returns_zeros_when_empty() {
        let state = make_state();
        let body = account(State(state)).await;
        let body = axum::body::to_bytes(body.into_body(), 64 * 1024).await.unwrap();
        let text = String::from_utf8_lossy(&body).to_string();
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        let data = &json["data"];
        assert_eq!(data["equity"], "0");
        assert_eq!(data["position_count"], 0);
        assert_eq!(data["trading_enabled"], false);
    }

    #[tokio::test]
    async fn positions_returns_live_positions() {
        let state = make_state();
        seed_tracker(&state);
        let body = positions(State(state)).await;
        let body = axum::body::to_bytes(body.into_body(), 64 * 1024).await.unwrap();
        let text = String::from_utf8_lossy(&body).to_string();
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        let data = json["data"].as_array().unwrap();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["symbol"], "BTC");
        assert_eq!(data[0]["size"], "0.5");
        assert_eq!(data[0]["leverage"], 3);
        assert_eq!(data[0]["is_long"], true);
    }
}
