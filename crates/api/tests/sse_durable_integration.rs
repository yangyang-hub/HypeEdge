//! Integration test: the durable SSE broker replay against a real Postgres
//! outbox. Requires the PG test container (same env gate as the storage suite).

use std::sync::Arc;

use hypeedge_api::sse_broker::SseBroker;
use hypeedge_domain::traits::DurableEvent;
use hypeedge_infra::event_bus::EventBus;
use hypeedge_storage::outbox::PostgresOutboxStore;
use sqlx::PgPool;
use uuid::Uuid;

fn test_pg_url() -> String {
    std::env::var("HYPE_TEST_PG_URL").unwrap_or_else(|_| {
        "postgres://postgres:testpass@localhost:55432/hypeedge_test".to_string()
    })
}

/// Serializes tests that share the outbox table.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn try_pool() -> Option<PgPool> {
    let opts = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(std::time::Duration::from_secs(2));
    match opts.connect(&test_pg_url()).await {
        Ok(p) => Some(p),
        Err(e) => {
            eprintln!("SKIP: Postgres unreachable ({e})");
            None
        }
    }
}

fn durable(sequence: i64, event_type: &str) -> DurableEvent {
    DurableEvent {
        sequence,
        event_id: Uuid::new_v4(),
        event_type: event_type.to_string(),
        schema_version: 1,
        aggregate_type: "order".into(),
        aggregate_id: "o1".into(),
        aggregate_revision: sequence,
        correlation_id: Some("c1".into()),
        payload: serde_json::json!({ "cloid": format!("c{sequence}"), "price": 100.5 }),
        occurred_at: chrono::Utc::now(),
    }
}

#[tokio::test]
async fn broker_replays_durable_outbox_from_last_event_id() {
    let _guard = SERIAL.lock().await;
    let Some(pool) = try_pool().await else { return };

    // Seed the outbox with a few events; capture the actual sequences (the
    // identity is not reset by DELETE, so sequences continue from prior runs).
    sqlx::query("DELETE FROM outbox_events")
        .execute(&pool)
        .await
        .unwrap();
    let mut sequences = Vec::new();
    for i in 1..=5i64 {
        let seq: i64 = sqlx::query_scalar(
            "INSERT INTO outbox_events (event_id, event_type, aggregate_type, aggregate_id, aggregate_revision, payload) VALUES ($1,$2,'order', $3, $4, $5) RETURNING sequence",
        )
        .bind(Uuid::new_v4())
        .bind(format!("order.submitted.{i}"))
        .bind(format!("o{i}"))
        .bind(i)
        .bind(serde_json::json!({"n": i}))
        .fetch_one(&pool)
        .await
        .unwrap();
        sequences.push(seq);
    }
    let first = sequences[0];
    let last = *sequences.last().unwrap();

    let outbox = Arc::new(PostgresOutboxStore::default());
    let bus = Arc::new(EventBus::new(16));
    let broker = SseBroker::new(bus, Some(outbox.clone()), Some(pool.clone()), 1000, 256);
    assert!(broker.has_durable_store());

    // Replay after the first sequence → the remaining 4 events.
    let replay = broker.durable_replay(Some(first)).await.unwrap();
    assert_eq!(
        replay.len(),
        4,
        "events after {first} replayed, got: {replay:?}"
    );
    assert_eq!(replay[0].sequence, sequences[1]);
    assert_eq!(replay[3].sequence, last);
    assert!(
        replay[0]
            .encode()
            .starts_with(&format!("id: {}\n", sequences[1]))
    );
    // Each frame has the id + event + data.
    assert!(replay[0].encode().contains("event: order.submitted.2"));

    // Replay after the last → none.
    let empty = broker.durable_replay(Some(last)).await.unwrap();
    assert!(empty.is_empty());

    // Bounds reflect the min/max.
    let (earliest, latest) = outbox.replay_bounds(&pool).await.unwrap();
    assert_eq!(earliest, Some(first));
    assert_eq!(latest, Some(last));
}

#[tokio::test]
async fn broker_emits_resync_on_retention_gap() {
    let _guard = SERIAL.lock().await;
    let Some(pool) = try_pool().await else { return };
    sqlx::query("DELETE FROM outbox_events")
        .execute(&pool)
        .await
        .unwrap();
    // Only one event exists at the current identity sequence.
    let seq: i64 = sqlx::query_scalar(
        "INSERT INTO outbox_events (event_id, event_type, aggregate_type, aggregate_id, aggregate_revision, payload) VALUES ($1,'order.submitted','order','o10',10,'{}') RETURNING sequence",
    )
    .bind(Uuid::new_v4())
    .fetch_one(&pool)
    .await
    .unwrap();

    let outbox = Arc::new(PostgresOutboxStore::default());
    let bus = Arc::new(EventBus::new(16));
    let broker = SseBroker::new(bus, Some(outbox.clone()), Some(pool.clone()), 1000, 256);

    // Requesting after seq-2 (earlier than the earliest available) → gap.
    let requested = seq - 2;
    let replay = broker.durable_replay(Some(requested)).await.unwrap();
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].event_type, "StreamResyncRequired");
    assert!(replay[0].data.contains("retention_gap"));
    assert!(
        replay[0]
            .data
            .contains(&format!("\"requested_after\":{requested}"))
    );
}

#[tokio::test]
async fn broker_fanout_delivers_only_new_sequences() {
    let Some(_pool) = try_pool().await else {
        return;
    };
    let bus = Arc::new(EventBus::new(16));
    let broker = SseBroker::new(bus, None, None, 1000, 256); // no store → in-memory path
    let (mailbox, _replay) = broker.subscribe(None);
    broker.publish(&durable(1, "order.submitted")).await;
    broker.publish(&durable(1, "order.submitted")).await; // dup
    broker.publish(&durable(2, "order.filled")).await;
    assert_eq!(mailbox.len(), 2);
    assert_eq!(mailbox.try_recv().unwrap().sequence, 1);
    assert_eq!(mailbox.try_recv().unwrap().sequence, 2);
}
