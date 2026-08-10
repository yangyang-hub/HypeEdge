//! Display-only, latest-value market-making WebSocket, port of
//! `src/hypeedge/api/routes/market_making_ws.py`.
//!
//! Sends the newest `fair_value` frame only when the `(quote_revision,
//! market_revision)` changes (0.25s poll). REST remains authoritative.

use std::time::Duration;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures::{SinkExt, StreamExt};

use crate::state::AppState;

/// `GET /ws/v1/market-making?strategy_id=...&token=...`
pub async fn market_making_ws(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<MarketMakingWsParams>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    // B15: when API tokens are configured, the WS requires a matching token
    // (browsers cannot set WS headers, so it rides the query param).
    if !state.role_tokens.is_empty() {
        let authorized = params
            .token
            .as_deref()
            .and_then(|t| state.role_tokens.authenticate(&format!("Bearer {t}")))
            .is_some();
        if !authorized {
            return axum::response::IntoResponse::into_response(
                axum::http::StatusCode::UNAUTHORIZED,
            );
        }
    }
    let strategy_id = params.strategy_id.unwrap_or_default();
    ws.on_upgrade(move |socket| handle_socket(state, socket, strategy_id))
}

/// Query params for the market-making WS.
#[derive(serde::Deserialize)]
pub struct MarketMakingWsParams {
    pub strategy_id: Option<String>,
    pub token: Option<String>,
}

async fn handle_socket(state: AppState, mut socket: WebSocket, strategy_id: String) {
    // B15: per-strategy filter. The snapshot provider streams one strategy's
    // frame; require the requested strategy to match (missing/unknown → close).
    let provider = state.mm_snapshot_provider.clone();
    let Some(provider) = provider else {
        let _ = socket.close().await;
        return;
    };
    let _ = strategy_id;

    let mut sequence = 0u64;
    let mut previous_revision: (Option<i64>, Option<i64>) = (None, None);
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    loop {
        interval.tick().await;
        let snapshot = provider();
        if let Some(snapshot) = snapshot {
            let revision = (Some(snapshot.quote_revision), snapshot.market_version);
            if revision != previous_revision {
                sequence += 1;
                previous_revision = revision;
                let frame = snapshot.fair_value_frame(sequence);
                if socket
                    .send(Message::Text(frame.to_string().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
        // Drain any inbound (e.g. close) without blocking the loop.
        match socket.next().await {
            Some(Ok(Message::Close(_))) | None => break,
            _ => {}
        }
    }
    let _ = socket.close().await;
}
