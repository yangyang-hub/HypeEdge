//! Display-only, latest-value market-making WebSocket, port of
//! `src/hypeedge/api/routes/market_making_ws.py`.
//!
//! Sends the newest `fair_value` frame only when the `(quote_revision,
//! market_revision)` changes (0.25s poll). REST remains authoritative.
//!
//! P4-3 / H-AP2: the poll loop previously awaited `socket.next()` after every
//! tick, which blocks on a silent client (browsers never send frames), so only
//! one frame was ever delivered. The loop now uses `tokio::select!` over the
//! poll interval and inbound messages, mirroring [`crate::ws_market`]. Unknown
//! strategies get an explicit error frame instead of a silent hang, and the
//! connection requires an Operator/Admin token when tokens are configured
//! (M11).

use std::time::Duration;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use serde_json::json;

use crate::auth::ApiRole;
use crate::state::{AppState, MmSnapshotProvider};

/// `GET /ws/v1/market-making?strategy_id=...&token=...`
pub async fn market_making_ws(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<MarketMakingWsParams>,
    ws: WebSocketUpgrade,
) -> Response {
    // B15: when API tokens are configured, the WS requires a token (browsers
    // cannot set WS headers, so it rides the query param). M11: the market-
    // making stream is operator-facing — Viewer tokens are rejected.
    if !state.role_tokens.is_empty() {
        let principal = params
            .token
            .as_deref()
            .and_then(|t| state.role_tokens.authenticate(&format!("Bearer {t}")));
        match principal {
            Some(p) if p.role >= ApiRole::Operator => {}
            Some(_) => {
                return crate::errors::ApiProblem::new(
                    403,
                    "FORBIDDEN",
                    "Market-making stream requires an operator or admin token",
                )
                .into_response();
            }
            None => {
                return crate::errors::ApiProblem::new(
                    401,
                    "AUTHENTICATION_REQUIRED",
                    "A valid Bearer token is required",
                )
                .into_response();
            }
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

/// Minimal frame-socket abstraction so the poll loop is unit-testable without
/// a live WebSocket upgrade (P4-3 test: a silent client must keep receiving
/// frames).
#[async_trait::async_trait]
trait FrameSocket: Send {
    async fn send_text(&mut self, frame: String) -> Result<(), ()>;
    async fn recv(&mut self) -> Option<Result<Message, axum::Error>>;
    async fn close(&mut self);
}

#[async_trait::async_trait]
impl FrameSocket for WebSocket {
    async fn send_text(&mut self, frame: String) -> Result<(), ()> {
        self.send(Message::Text(frame.into())).await.map_err(|_| ())
    }
    async fn recv(&mut self) -> Option<Result<Message, axum::Error>> {
        self.next().await
    }
    async fn close(&mut self) {
        // axum's `WebSocket` has no inherent `close`; it implements
        // `futures::Sink<Message>`, so the Sink close sends the close frame.
        // Fully-qualified to avoid recursing into `FrameSocket::close`.
        let _ = futures::SinkExt::close(self).await;
    }
}

async fn handle_socket(state: AppState, socket: WebSocket, strategy_id: String) {
    run_socket(state.mm_snapshot_provider.clone(), strategy_id, socket).await
}

/// The poll loop, parameterized over a [`FrameSocket`]. Sends an explicit
/// error frame (and closes) when the provider is missing or the strategy has
/// no live runtime, then streams `fair_value` frames on revision changes.
async fn run_socket<S: FrameSocket>(
    provider: Option<MmSnapshotProvider>,
    strategy_id: String,
    mut socket: S,
) {
    let Some(provider) = provider else {
        let _ = socket
            .send_text(
                json!({
                    "schema_version": 1, "sequence": 0, "type": "error",
                    "strategy_id": strategy_id,
                    "data": { "code": "SNAPSHOT_PROVIDER_UNAVAILABLE", "message": "market-making snapshot provider is not wired" },
                })
                .to_string(),
            )
            .await;
        socket.close().await;
        return;
    };
    // Unknown strategy → explicit error frame, then close (no silent hang).
    if provider(&strategy_id).await.is_none() {
        let _ = socket
            .send_text(
                json!({
                    "schema_version": 1, "sequence": 0, "type": "error",
                    "strategy_id": strategy_id,
                    "data": { "code": "STRATEGY_NOT_FOUND", "message": "no live market-making runtime for this strategy" },
                })
                .to_string(),
            )
            .await;
        socket.close().await;
        return;
    }

    let mut sequence = 0u64;
    let mut previous_revision: (Option<i64>, Option<i64>) = (None, None);
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let snapshot = provider(&strategy_id).await;
                if let Some(snapshot) = snapshot {
                    let revision = (Some(snapshot.quote_revision), snapshot.market_version);
                    if revision != previous_revision {
                        sequence += 1;
                        previous_revision = revision;
                        let frame = snapshot.fair_value_frame(sequence);
                        if socket.send_text(frame.to_string()).await.is_err() {
                            break;
                        }
                    }
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {} // ignore pings / other inbound
                }
            }
        }
    }
    socket.close().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    /// A fake snapshot whose revision advances on each poll so the loop emits
    /// a new frame every tick.
    fn advancing_provider() -> MmSnapshotProvider {
        let revision = Arc::new(std::sync::atomic::AtomicI64::new(0));
        Arc::new(move |id: &str| {
            let id = id.to_string();
            let revision = revision.clone();
            Box::pin(async move {
                if id != "mm_1" {
                    return None;
                }
                let r = revision.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Some(snapshot_for_test(r, "mm_1"))
            })
        })
    }

    fn static_provider() -> MmSnapshotProvider {
        Arc::new(move |id: &str| {
            let id = id.to_string();
            Box::pin(async move {
                if id != "mm_1" {
                    return None;
                }
                Some(snapshot_for_test(1, "mm_1"))
            })
        })
    }

    fn snapshot_for_test(
        revision: i64,
        strategy_id: &str,
    ) -> hypeedge_trading::market_maker::MarketMakerRuntimeSnapshot {
        use hypeedge_domain::enums::MarketMakerLifecycle;
        hypeedge_trading::market_maker::MarketMakerRuntimeSnapshot {
            strategy_id: strategy_id.to_string(),
            session_id: format!("sess-{strategy_id}"),
            symbol: "BTC".into(),
            mode: MarketMakerLifecycle::Shadow,
            config_version: Some(1),
            quote_revision: revision,
            market_version: Some(revision),
            connection_generation: Some(1),
            last_cycle_at: Some(chrono::Utc::now()),
            last_reason: Some("cycled".into()),
            desired: None,
            plan: None,
            features: None,
        }
    }

    /// The half of the harness the loop drives: frames flow to the test via
    /// `out_tx`; control messages (Close) arrive from the test via `in_rx`.
    struct LoopSocket {
        out_tx: mpsc::UnboundedSender<String>,
        in_rx: mpsc::UnboundedReceiver<Message>,
    }

    #[async_trait::async_trait]
    impl FrameSocket for LoopSocket {
        async fn send_text(&mut self, frame: String) -> Result<(), ()> {
            self.out_tx.send(frame).map_err(|_| ())
        }
        async fn recv(&mut self) -> Option<Result<Message, axum::Error>> {
            self.in_rx.recv().await.map(Ok)
        }
        async fn close(&mut self) {}
    }

    /// The test side of the harness: reads emitted frames, sends Close.
    struct Harness {
        out_rx: mpsc::UnboundedReceiver<String>,
        in_tx: mpsc::UnboundedSender<Message>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Harness {
        fn spawn(provider: Option<MmSnapshotProvider>, strategy_id: &str) -> Self {
            let (out_tx, out_rx) = mpsc::unbounded_channel();
            let (in_tx, in_rx) = mpsc::unbounded_channel();
            let socket = LoopSocket { out_tx, in_rx };
            let task = tokio::spawn(run_socket(provider, strategy_id.to_string(), socket));
            Self {
                out_rx,
                in_tx,
                task,
            }
        }

        async fn next_frame(&mut self) -> String {
            tokio::time::timeout(Duration::from_secs(2), self.out_rx.recv())
                .await
                .expect("frame timeout — poll loop stalled")
                .expect("channel closed")
        }

        fn close(&self) {
            let _ = self.in_tx.send(Message::Close(None));
        }
    }

    #[tokio::test]
    async fn silent_client_receives_multiple_frames() {
        // P4-3: a client that never sends must still receive a new frame each
        // poll tick (the old `socket.next()` after every tick blocked here).
        let mut harness = Harness::spawn(Some(advancing_provider()), "mm_1");
        let f1 = harness.next_frame().await;
        let v1: serde_json::Value = serde_json::from_str(&f1).unwrap();
        assert_eq!(v1["type"], "fair_value");
        assert_eq!(v1["sequence"], 1);
        let _f2 = harness.next_frame().await;
        let _f3 = harness.next_frame().await;
        harness.close();
        let _ = tokio::time::timeout(Duration::from_secs(2), harness.task)
            .await
            .expect("loop must exit after close");
    }

    #[tokio::test]
    async fn unknown_strategy_gets_error_frame_then_close() {
        let mut harness = Harness::spawn(Some(static_provider()), "ghost");
        let frame = harness.next_frame().await;
        let v: serde_json::Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "error");
        assert_eq!(v["data"]["code"], "STRATEGY_NOT_FOUND");
        harness.close();
        let _ = tokio::time::timeout(Duration::from_secs(2), harness.task)
            .await
            .expect("loop must exit after error close");
    }

    #[tokio::test]
    async fn missing_provider_gets_error_frame_then_close() {
        let mut harness = Harness::spawn(None, "mm_1");
        let frame = harness.next_frame().await;
        let v: serde_json::Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["data"]["code"], "SNAPSHOT_PROVIDER_UNAVAILABLE");
        harness.close();
        let _ = tokio::time::timeout(Duration::from_secs(2), harness.task)
            .await
            .expect("loop must exit after error close");
    }
}
