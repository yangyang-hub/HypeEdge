//! Durable SSE endpoint using the outbox-backed [`SseBroker`], port of the
//! durable path in `src/hypeedge/api/routes/events.py`.
//!
//! On connect: sends `: connected`, then replays the outbox from
//! `Last-Event-ID` (or `StreamResyncRequired` on a retention gap), then streams
//! live committed events. The broker dedups a crash-retried sequence.

use std::collections::VecDeque;
use std::convert::Infallible;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Sse;
use axum::response::sse::{Event, KeepAlive};
use futures::stream::{self};

use crate::sse_broker::{BufferedEvent, SseBroker};
use crate::state::AppState;

struct StreamState {
    first: bool,
    broker: std::sync::Arc<SseBroker>,
    mailbox: crate::sse_broker::ClientMailbox,
    after: Option<i64>,
    last_sent: i64,
    pending_replay: VecDeque<BufferedEvent>,
}

/// `GET /api/v1/events` — durable SSE stream with `Last-Event-ID` replay.
pub async fn durable_events(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> axum::response::Response {
    let after_sequence = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|n| *n >= 0);
    let (mailbox, _replay) = state.sse_broker.subscribe(after_sequence);
    let initial = StreamState {
        first: true,
        broker: state.sse_broker.clone(),
        mailbox,
        after: after_sequence,
        last_sent: 0,
        pending_replay: VecDeque::new(),
    };

    let stream = stream::unfold(initial, |mut st| async move {
        if st.first {
            st.first = false;
            // Immediate flush so reverse proxies don't buffer.
            return Some((Ok::<_, Infallible>(Event::default().event("connected")), st));
        }
        // Durable replay: populate pending on the first live pass.
        if st.pending_replay.is_empty() && st.last_sent == 0 && st.broker.has_durable_store() {
            match st.broker.durable_replay(st.after).await {
                Ok(events) => {
                    st.pending_replay = events.into();
                }
                Err(_) => {
                    return Some((
                        Ok::<_, Infallible>(
                            Event::default()
                                .event("error")
                                .data(r#"{"detail":"sse_replay_unavailable"}"#),
                        ),
                        st,
                    ));
                }
            }
        }
        // Emit pending replay events first.
        if let Some(buffered) = st.pending_replay.pop_front() {
            st.last_sent = buffered.sequence;
            let ev = Event::default()
                .event(&buffered.event_type)
                .id(buffered.sequence.to_string())
                .data(buffered.data);
            return Some((Ok::<_, Infallible>(ev), st));
        }
        // Live events.
        let event = st.mailbox.recv().await?;
        if event.sequence <= st.last_sent {
            st.last_sent = event.sequence;
            return Some((
                Ok::<_, Infallible>(Event::default().event("skip").data("")),
                st,
            ));
        }
        st.last_sent = event.sequence;
        let ev = Event::default()
            .event(&event.event_type)
            .id(event.sequence.to_string())
            .data(event.data);
        Some((Ok::<_, Infallible>(ev), st))
    });

    let sse = Sse::new(stream)
        .keep_alive(KeepAlive::default().interval(std::time::Duration::from_secs(15)));
    let mut response = axum::response::IntoResponse::into_response(sse);
    response
        .headers_mut()
        .insert("cache-control", "no-cache, no-transform".parse().unwrap());
    response
        .headers_mut()
        .insert("x-accel-buffering", "no".parse().unwrap());
    response
}
