//! HypeEdge HTTP API layer: axum router + middleware + routes + SSE + WS.
//!
//! Ports `src/hypeedge/api/`. The [`build_router`] factory wires the axum
//! [`Router`] with the security middleware, the V1 REST routes, the SSE event
//! stream, and the market WebSocket.

pub mod auth;
pub mod errors;
pub mod middleware;
pub mod routes;
pub mod sse;
pub mod sse_broker;
pub mod sse_durable;
pub mod state;
pub mod ws_market;
pub mod ws_market_making;

use std::sync::Arc;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::middleware::from_fn_with_state;
use axum::routing::{get, post};

pub use state::AppState;

/// Build the axum router for the given shared state.
pub fn build_router(state: AppState) -> Router {
    let state = Arc::new(state);

    let api = Router::new()
        .route("/system/status", get(routes::system::system_status))
        .route("/bootstrap", get(routes::system::bootstrap))
        .route("/risk/status", get(routes::risk::risk_status))
        .route("/kill-switch", post(routes::risk::kill_switch))
        .route("/market/{symbol}/book", get(routes::market::book))
        .route("/market/{symbol}/meta", get(routes::market::meta))
        .route("/market/{symbol}/funding", get(routes::market::funding))
        .route("/market/{symbol}/candles", get(routes::market::candles))
        .route("/account", get(routes::account::account))
        .route("/account/equity-curve", get(routes::account::equity_curve))
        .route("/positions", get(routes::account::positions))
        .route(
            "/positions/{symbol}/close",
            post(routes::account::close_position),
        )
        .route("/orders", get(routes::account::orders))
        .route("/orders", post(routes::account::submit_order))
        .route(
            "/orders/{cloid}/cancel",
            post(routes::account::cancel_order),
        )
        .route("/events", get(sse_durable::durable_events))
        .route("/strategies", get(routes::strategies::list_strategies))
        .route("/strategies", post(routes::strategies::create_strategy))
        .route(
            "/strategies/{strategy_id}",
            get(routes::strategies::get_strategy),
        )
        .route(
            "/strategies/{strategy_id}/actions/{action}",
            post(routes::strategies::strategy_action),
        )
        .route(
            "/strategies/{strategy_id}/config-versions",
            get(routes::strategies::list_config_versions),
        )
        .route(
            "/strategies/{strategy_id}/config-versions",
            post(routes::strategies::create_config_version),
        )
        .route(
            "/strategies/{strategy_id}/config-versions/{version}/activate",
            post(routes::strategies::activate_config_version),
        );

    Router::new()
        .route("/health", get(routes::system::health))
        .route("/ws/v1/market", get(ws_market::market_ws))
        .route(
            "/ws/v1/market-making",
            get(ws_market_making::market_making_ws),
        )
        .nest("/api/v1", api)
        .layer(from_fn_with_state(
            (*state).clone(),
            crate::middleware::security,
        ))
        .layer(DefaultBodyLimit::max(1 << 20))
        .with_state((*state).clone())
}
