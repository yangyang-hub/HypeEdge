//! Durable Postgres-backed SSE replay and isolated live fan-out, port of
//! `src/hypeedge/api/routes/events.py`.
//!
//! The broker fans out only *committed* durable events (from the outbox). A
//! crash retry of the same sequence is ignored via a bounded `seen_sequences`
//! ring. When no outbox store is configured (monitor-only/test deployments),
//! it falls back to the legacy in-process event bus fan-out.

use std::collections::VecDeque;
use std::sync::Arc;

use hypeedge_domain::events::DomainEvent;
use hypeedge_infra::event_bus::{BoundedMailbox, EventBus};
use hypeedge_storage::outbox::PostgresOutboxStore;
use sqlx::PgPool;

/// A buffered, encoded SSE event.
#[derive(Debug, Clone)]
pub struct BufferedEvent {
    pub sequence: i64,
    pub event_type: String,
    pub data: String,
}

impl BufferedEvent {
    pub fn encode(&self) -> String {
        format!(
            "id: {}\nevent: {}\nretry: 3000\ndata: {}\n\n",
            self.sequence, self.event_type, self.data
        )
    }
}

/// Which legacy event types fan out when no outbox is present.
const LEGACY_SSE_EVENT_TYPES: &[&str] = &[
    "OrderSubmitted",
    "OrderAcknowledged",
    "OrderFilled",
    "OrderPartialFill",
    "OrderCancelled",
    "OrderRejected",
    "PositionChanged",
    "BalanceChanged",
    "AccountStateUpdate",
    "SignalGenerated",
    "RiskCheckPassed",
    "RiskCheckFailed",
    "KillSwitchTriggered",
    "ReconciliationComplete",
    "ActionCreditsLow",
    "WsConnected",
    "WsDisconnected",
];

/// A client subscription queue.
pub type ClientMailbox = Arc<BoundedMailbox<BufferedEvent>>;

/// The SSE broker.
pub struct SseBroker {
    event_bus: Arc<EventBus>,
    outbox: Option<Arc<PostgresOutboxStore>>,
    pool: Option<PgPool>,
    client_queue_size: usize,
    replay_size: usize,
    clients: std::sync::Mutex<Vec<(ClientMailbox, i64)>>,
    replay: std::sync::Mutex<VecDeque<BufferedEvent>>,
    seen_sequences: std::sync::Mutex<(VecDeque<i64>, std::collections::HashSet<i64>)>,
    sequence: std::sync::Mutex<i64>,
}

impl SseBroker {
    pub fn new(
        event_bus: Arc<EventBus>,
        outbox: Option<Arc<PostgresOutboxStore>>,
        pool: Option<PgPool>,
        replay_size: usize,
        client_queue_size: usize,
    ) -> Self {
        Self {
            event_bus,
            outbox,
            pool,
            client_queue_size,
            replay_size,
            clients: std::sync::Mutex::new(Vec::new()),
            replay: std::sync::Mutex::new(VecDeque::with_capacity(replay_size)),
            seen_sequences: std::sync::Mutex::new((
                VecDeque::new(),
                std::collections::HashSet::new(),
            )),
            sequence: std::sync::Mutex::new(0),
        }
    }

    /// Append to the in-memory replay ring, trimming to `replay_size` (C4).
    fn push_replay(&self, event: BufferedEvent) {
        let mut replay = self.replay.lock().unwrap();
        replay.push_back(event);
        while replay.len() > self.replay_size {
            replay.pop_front();
        }
    }

    pub fn has_durable_store(&self) -> bool {
        self.outbox.is_some() && self.pool.is_some()
    }

    /// Subscribe a client at `after_sequence`. Returns the mailbox + the
    /// in-memory replay (for the no-store path).
    pub fn subscribe(&self, after_sequence: Option<i64>) -> (ClientMailbox, Vec<BufferedEvent>) {
        let mailbox = Arc::new(BoundedMailbox::new(self.client_queue_size));
        let cursor = after_sequence.unwrap_or(0);
        self.clients.lock().unwrap().push((mailbox.clone(), cursor));
        let replay = if self.has_durable_store() {
            vec![]
        } else {
            let replay_guard = self.replay.lock().unwrap();
            replay_guard
                .iter()
                .filter(|e| after_sequence.is_none() || e.sequence > after_sequence.unwrap())
                .cloned()
                .collect()
        };
        (mailbox, replay)
    }

    pub fn unsubscribe(&self, mailbox: &ClientMailbox) {
        self.clients
            .lock()
            .unwrap()
            .retain(|(mb, _)| !Arc::ptr_eq(mb, mailbox));
    }

    pub fn is_subscribed(&self, mailbox: &ClientMailbox) -> bool {
        self.clients
            .lock()
            .unwrap()
            .iter()
            .any(|(mb, _)| Arc::ptr_eq(mb, mailbox))
    }

    /// Advance a client cursor to `sequence`.
    pub fn advance_client(&self, mailbox: &ClientMailbox, sequence: i64) {
        let mut clients = self.clients.lock().unwrap();
        if let Some((_, cursor)) = clients.iter_mut().find(|(mb, _)| Arc::ptr_eq(mb, mailbox)) {
            *cursor = (*cursor).max(sequence);
        }
    }

    /// Publish one committed durable sequence; a crash retry is ignored.
    pub async fn publish(&self, event: &hypeedge_domain::traits::DurableEvent) {
        {
            let mut guard = self.seen_sequences.lock().unwrap();
            let (order, seen) = &mut *guard;
            if seen.contains(&event.sequence) {
                return;
            }
            if order.len() >= 2000
                && let Some(oldest) = order.pop_front()
            {
                seen.remove(&oldest);
            }
            order.push_back(event.sequence);
            seen.insert(event.sequence);
        }
        let mut seq = self.sequence.lock().unwrap();
        *seq = (*seq).max(event.sequence);
        let buffered = from_durable(event);
        self.push_replay(buffered.clone());
        self.fan_out(buffered);
    }

    fn fan_out(&self, event: BufferedEvent) {
        let mut clients = self.clients.lock().unwrap();
        for (mailbox, cursor) in clients.iter_mut() {
            if event.sequence <= *cursor {
                continue;
            }
            if mailbox.len() >= self.client_queue_size {
                // Closing only this subscription makes the browser reconnect
                // with its last durable sequence; other clients are unaffected.
                mailbox.close();
                continue;
            }
            mailbox.put_lossy(event.clone());
            *cursor = event.sequence;
        }
        clients.retain(|(mb, _)| mb.len() < self.client_queue_size || mb.is_empty());
        // Do not close the mailbox here; the reader loop drops when closed.
    }

    /// The legacy bus fan-out path (no outbox store): consumes reliable events
    /// and broadcasts them. Runs until the mailbox closes.
    pub async fn run_legacy(&self, stop_rx: tokio::sync::mpsc::Receiver<()>) {
        let mailbox = self.event_bus.subscribe_all();
        let mut stop_rx = stop_rx;
        loop {
            tokio::select! {
                _ = stop_rx.recv() => break,
                maybe = mailbox.recv() => {
                    let Some(event) = maybe else { break };
                    let event_type = event.event_type().as_str();
                    if !LEGACY_SSE_EVENT_TYPES.contains(&event_type) {
                        continue;
                    }
                    let mut seq = self.sequence.lock().unwrap();
                    *seq += 1;
                    let payload = payload_for_legacy(&event.payload);
                    let body = serde_json::to_string(&serde_json::json!({
                        "schema_version": 1,
                        "sequence": *seq,
                        "event_type": event_type,
                        "payload": payload,
                        "timestamp": event.occurred_at.to_rfc3339(),
                        "correlation_id": event.correlation_id,
                    }))
                    .unwrap_or_else(|_| "{}".into());
                    let buffered = BufferedEvent { sequence: *seq, event_type: event_type.to_string(), data: body };
                    drop(seq);
                    self.push_replay(buffered.clone());
                    self.fan_out(buffered);
                }
            }
        }
    }

    /// Replay the durable outbox after `after_sequence`, yielding encoded
    /// events or a `StreamResyncRequired` on a retention gap.
    pub async fn durable_replay(
        &self,
        after_sequence: Option<i64>,
    ) -> Result<Vec<BufferedEvent>, String> {
        let (Some(outbox), Some(pool)) = (&self.outbox, &self.pool) else {
            return Ok(vec![]);
        };
        let (earliest, latest) = outbox
            .replay_bounds(pool)
            .await
            .map_err(|e| e.to_string())?;
        let latest = latest.unwrap_or(0);
        let Some(after) = after_sequence else {
            return Ok(vec![]); // fresh client: no replay needed
        };
        if after > latest || (after > 0 && earliest.is_some() && after < earliest.unwrap() - 1) {
            let body = serde_json::to_string(&serde_json::json!({
                "schema_version": 1,
                "sequence": latest,
                "event_type": "StreamResyncRequired",
                "payload": {
                    "reason": "retention_gap",
                    "requested_after": after,
                    "earliest_available": earliest,
                    "latest_available": latest,
                },
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "correlation_id": null,
            }))
            .unwrap_or_else(|_| "{}".into());
            return Ok(vec![BufferedEvent {
                sequence: latest,
                event_type: "StreamResyncRequired".into(),
                data: body,
            }]);
        }
        let mut out = Vec::new();
        let mut cursor = after;
        while cursor < latest {
            let page = outbox
                .read_after(pool, cursor, latest, 500)
                .await
                .map_err(|e| e.to_string())?;
            if page.is_empty() {
                break;
            }
            for event in page {
                cursor = event.sequence;
                out.push(from_durable(&event));
            }
        }
        Ok(out)
    }
}

/// Encode a durable event into a buffered SSE frame.
fn from_durable(event: &hypeedge_domain::traits::DurableEvent) -> BufferedEvent {
    let body = serde_json::to_string(&serde_json::json!({
        "schema_version": event.schema_version,
        "sequence": event.sequence,
        "event_id": event.event_id.to_string(),
        "event_type": event.event_type,
        "payload": precise_payload(&event.payload),
        "timestamp": event.occurred_at.to_rfc3339(),
        "correlation_id": event.correlation_id,
    }))
    .unwrap_or_else(|_| "{}".into());
    BufferedEvent {
        sequence: event.sequence,
        event_type: event.event_type.clone(),
        data: body,
    }
}

/// Recursively convert float/Decimal values to decimal strings.
fn precise_payload(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                serde_json::Value::String(
                    hypeedge_domain::Decimal::from_f64(f)
                        .unwrap_or_default()
                        .to_string(),
                )
            } else {
                value.clone()
            }
        }
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), precise_payload(v)))
                .collect(),
        ),
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(precise_payload).collect())
        }
        _ => value.clone(),
    }
}

/// A minimal payload for the legacy bus path (best-effort serialization).
fn payload_for_legacy(payload: &DomainEvent) -> serde_json::Value {
    // The frontend keys on the event type; emit a stable marker with the
    // correlation id so the frame is non-empty.
    serde_json::json!({ "event_type": payload.event_type().as_str() })
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::traits::DurableEvent;
    use uuid::Uuid;

    fn durable(sequence: i64) -> DurableEvent {
        DurableEvent {
            sequence,
            event_id: Uuid::new_v4(),
            event_type: "order.submitted".into(),
            schema_version: 1,
            aggregate_type: "order".into(),
            aggregate_id: "o1".into(),
            aggregate_revision: 1,
            correlation_id: None,
            payload: serde_json::json!({ "cloid": "c1", "price": 100.5 }),
            occurred_at: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn publish_dedups_replayed_sequence() {
        let bus = Arc::new(EventBus::new(16));
        let broker = SseBroker::new(bus, None, None, 1000, 256);
        let (mailbox, _) = broker.subscribe(None);
        broker.publish(&durable(5)).await;
        broker.publish(&durable(5)).await; // duplicate
        broker.publish(&durable(6)).await;
        // Client cursor advanced to 6; no event should be buffered for a fresh
        // client that subscribed before any publish (cursor 0).
        let _ = mailbox;
        // Check the replay ring has both distinct events.
        assert_eq!(broker.replay.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn fan_out_skips_caught_up_clients() {
        let bus = Arc::new(EventBus::new(16));
        let broker = SseBroker::new(bus, None, None, 1000, 256);
        let (mailbox, _) = broker.subscribe(Some(6)); // caught up to 6
        broker.publish(&durable(5)).await; // already seen
        broker.publish(&durable(7)).await; // new
        assert_eq!(mailbox.len(), 1, "only sequence 7 delivered");
        let event = mailbox.try_recv().unwrap();
        assert_eq!(event.sequence, 7);
    }

    #[test]
    fn buffered_event_encode_shape() {
        let e = BufferedEvent {
            sequence: 3,
            event_type: "order.submitted".into(),
            data: "{\"x\":1}".into(),
        };
        let s = e.encode();
        assert!(s.starts_with("id: 3\nevent: order.submitted\nretry: 3000\ndata: {"));
        assert!(s.ends_with("\n\n"));
    }

    #[tokio::test]
    async fn replay_ring_is_bounded() {
        // C4 regression: the in-memory replay ring must trim to replay_size,
        // not grow without bound.
        let bus = Arc::new(EventBus::new(16));
        let broker = SseBroker::new(bus, None, None, 5, 16);
        for seq in 0..20 {
            broker.publish(&durable(seq)).await;
        }
        assert_eq!(broker.replay.lock().unwrap().len(), 5, "replay ring bounded (C4)");
    }

    #[tokio::test]
    async fn legacy_bus_path_delivers_events_to_subscribers() {
        // C2 regression: the no-store deployment must deliver SSE events through
        // the legacy bus fan-out (AppState now spawns run_legacy).
        let bus = Arc::new(EventBus::new(16));
        let broker = Arc::new(SseBroker::new(bus.clone(), None, None, 1000, 16));
        let (stop_tx, stop_rx) = tokio::sync::mpsc::channel(1);
        {
            let broker = broker.clone();
            tokio::spawn(async move { broker.run_legacy(stop_rx).await });
        }
        let (mailbox, _) = broker.subscribe(None);
        // Let run_legacy subscribe to the bus before publishing.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let order = hypeedge_domain::models::Order::new(
            "c1".into(),
            "BTC".into(),
            hypeedge_domain::enums::Side::Buy,
            hypeedge_domain::decimal::Size::new(hypeedge_domain::decimal::Decimal::ONE),
            None,
            hypeedge_domain::enums::OrderType::Limit,
            hypeedge_domain::enums::TimeInForce::Gtc,
        );
        bus.publish_sync(Arc::new(
            hypeedge_domain::events::Event::new(
                hypeedge_domain::events::DomainEvent::OrderSubmitted(order),
            ),
        ))
        .expect("publish");

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            !mailbox.is_empty(),
            "legacy bus path must deliver events (C2)"
        );
        let _ = stop_tx;
    }
}
