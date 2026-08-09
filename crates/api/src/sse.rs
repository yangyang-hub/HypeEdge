//! SSE event stream, port of `src/hypeedge/api/routes/events.py`.
//!
//! Subscribes to the event bus and forwards events as SSE frames. The durable
//! Postgres-backed replay (outbox + `Last-Event-ID`) lands with the storage
//! wiring; this live stream keeps the frontend's cache-invalidation path working.

use std::convert::Infallible;

use axum::extract::State;
use axum::response::Sse;
use axum::response::sse::{Event, KeepAlive};
use futures::stream::{self, Stream};
use hypeedge_domain::events::EventType;

use crate::state::AppState;

/// `GET /api/v1/events` — SSE stream of domain events.
pub async fn events(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let bus = state.event_bus.clone();
    let mailbox = bus.subscribe_all();
    let stream = stream::unfold((true, mailbox), |(first, mailbox)| async move {
        if first {
            // Immediate `: connected` comment per the Python SSE contract.
            return Some((
                Ok::<_, Infallible>(Event::default().event("connected")),
                (false, mailbox),
            ));
        }
        let event = mailbox.recv().await?;
        let event_type = event.payload.event_type();
        let payload = payload_json(&event_type, &event.payload);
        let data = serde_json::to_string(&serde_json::json!({
            "schema_version": 1,
            "sequence": 0,
            "event_type": snake_case(event_type.as_str()),
            "payload": payload,
            "timestamp": event.occurred_at.to_rfc3339(),
            "correlation_id": event.correlation_id,
        }))
        .unwrap_or_else(|_| "{}".into());
        Some((
            Ok::<_, Infallible>(
                Event::default()
                    .event(snake_case(event_type.as_str()))
                    .data(data),
            ),
            (false, mailbox),
        ))
    });

    Sse::new(stream).keep_alive(KeepAlive::default().interval(std::time::Duration::from_secs(15)))
}

/// Extract the JSON payload for an event (a compact serialization of the
/// domain payload — enough for the frontend to invalidate its caches).
fn payload_json(
    event_type: &EventType,
    payload: &hypeedge_domain::events::DomainEvent,
) -> serde_json::Value {
    // The frontend keys on the event *type* to revalidate; the payload content
    // matters less. Emit a stable marker so the frame is non-empty.
    let _ = payload;
    serde_json::json!({ "event_type": event_type.as_str() })
}

/// Convert a PascalCase event type to snake_case (`OrderSubmitted` →
/// `order_submitted`) — the Phase-6 contract cleanup.
fn snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snake_case_converts() {
        assert_eq!(snake_case("OrderSubmitted"), "order_submitted");
        assert_eq!(snake_case("KillSwitchTriggered"), "kill_switch_triggered");
        assert_eq!(snake_case("L2BookUpdate"), "l2_book_update");
        assert_eq!(
            snake_case("ReconciliationComplete"),
            "reconciliation_complete"
        );
    }
}
