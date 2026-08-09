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

/// `GET /ws/v1/market-making?strategy_id=...`
pub async fn market_making_ws(
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(state, socket))
}

async fn handle_socket(state: AppState, mut socket: WebSocket) {
    // The strategy_id arrives as a query param; the current handler streams
    // whatever snapshot the provider exposes. A per-strategy filter lands with
    // the runtime wiring.
    let provider = state.mm_snapshot_provider.clone();
    let Some(provider) = provider else {
        let _ = socket.close().await;
        return;
    };

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
