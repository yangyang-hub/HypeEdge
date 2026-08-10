//! Market data WebSocket, port of `src/hypeedge/api/routes/market_ws.py`.
//!
//! Sends a `snapshot` frame on connect, then `book`/`heartbeat` frames as the
//! in-memory book updates. Frame envelope: `{schema_version, sequence, type,
//! symbol, data}` — the Phase-6 contract.

use std::time::Duration;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures::StreamExt;
use serde_json::json;

use crate::state::AppState;

/// `GET /ws/v1/market?symbol=BTC`.
pub async fn market_ws(State(state): State<AppState>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(state, socket))
}

async fn handle_socket(state: AppState, mut socket: WebSocket) {
    let symbol = "BTC"; // single-symbol v1; the query param filters in a later increment.
    let mut sequence = 0u64;

    // Snapshot frame on connect.
    let snapshot = state.books.lock().await.get_snapshot(symbol);
    let frame = json!({
        "schema_version": 1,
        "sequence": sequence,
        "type": "snapshot",
        "symbol": symbol,
        "data": snapshot.as_ref().map(|s| json!({
            "bids": s.bids.iter().map(|l| json!([l.price.to_string(), l.size.to_string()])).collect::<Vec<_>>(),
            "asks": s.asks.iter().map(|l| json!([l.price.to_string(), l.size.to_string()])).collect::<Vec<_>>(),
        })).unwrap_or(json!({})),
    });
    if socket
        .send(Message::Text(frame.to_string().into()))
        .await
        .is_err()
    {
        return;
    }
    sequence += 1;

    // Heartbeat loop: emit a heartbeat every 5s and any book changes.
    let bus = state.event_bus.clone();
    let mailbox = bus.subscribe_maxsize(hypeedge_domain::events::EventType::L2BookUpdate, 1);
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if socket
                    .send(Message::Text(json!({
                        "schema_version": 1, "sequence": sequence, "type": "heartbeat", "symbol": symbol, "data": {}
                    }).to_string().into()))
                    .await
                    .is_err()
                {
                    break;
                }
                sequence += 1;
            }
            maybe = mailbox.recv() => {
                let Some(event) = maybe else { break };
                let hypeedge_domain::events::DomainEvent::L2BookUpdate(book) = &event.payload else { continue };
                if book.symbol != symbol { continue; }
                let frame = json!({
                    "schema_version": 1,
                    "sequence": sequence,
                    "type": "book",
                    "symbol": symbol,
                    "data": {
                        "bids": book.bids.iter().map(|l| json!([l.price.to_string(), l.size.to_string()])).collect::<Vec<_>>(),
                        "asks": book.asks.iter().map(|l| json!([l.price.to_string(), l.size.to_string()])).collect::<Vec<_>>(),
                    },
                });
                if socket.send(Message::Text(frame.to_string().into())).await.is_err() {
                    break;
                }
                sequence += 1;
            }
            msg = socket.next() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {} // ignore pings / other
                }
            }
        }
    }
    // C3: drop the bus subscription when the connection closes, so the event
    // bus does not accumulate a stale mailbox per disconnected browser.
    bus.unsubscribe(hypeedge_domain::events::EventType::L2BookUpdate, &mailbox);
}
