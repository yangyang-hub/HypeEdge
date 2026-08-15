//! Integration test for the exchange-fact projector against a real Postgres
//! (the schema from `migrations/0001_create_all.sql`).
//!
//! Requires `HYPE_TEST_PG_URL` (default `postgres://postgres:testpass@localhost:55432/hypeedge_test`).
//! Skips (rather than fails) when Postgres is unreachable so `cargo test
//! --workspace` stays green on machines without the container.

use hypeedge_storage::exchange_ingestor_store::PostgresExchangeFactProjector;
use hypeedge_trading::account::exchange_ingestor::ExchangeFactProjector;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

fn test_pg_url() -> String {
    std::env::var("HYPE_TEST_PG_URL").unwrap_or_else(|_| {
        "postgres://postgres:testpass@localhost:55432/hypeedge_test".to_string()
    })
}

async fn try_pool() -> Option<PgPool> {
    let opts = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(std::time::Duration::from_secs(2));
    match opts.connect(&test_pg_url()).await {
        Ok(p) => Some(p),
        Err(e) => {
            eprintln!(
                "SKIP: Postgres unreachable at {} ({e}); integration test skipped",
                test_pg_url()
            );
            None
        }
    }
}

const ACCOUNT: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// A valid HL cloid: `0x` + 32 lowercase hex (matches the CHECK constraint).
fn hl_cloid(n: u64) -> String {
    format!("0x{n:032x}")
}

/// Cross-binary serialization: every integration test binary shares one DB, so
/// each test holds a Postgres advisory lock to stop concurrent `clean_tables`
/// calls from clobbering another test's rows. The connection is returned; the
/// lock is released when it drops.
async fn acquire_test_lock(pool: &PgPool) -> sqlx::pool::PoolConnection<sqlx::Postgres> {
    let mut conn = pool.acquire().await.expect("acquire test connection");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(0x48455045i64) // "HYPE"
        .execute(&mut *conn)
        .await
        .expect("advisory lock");
    conn
}

async fn clean_tables(pool: &PgPool) {
    for t in [
        "funding_payments",
        "ledger_entries",
        "fills",
        "order_events",
        "outbox_events",
        "risk_reservations",
        "risk_events",
        "execution_command_items",
        "execution_commands",
        "quote_plan_items",
        "quote_slots",
        "positions",
        "inbox_events",
        "exchange_sync_cursors",
        "orders",
    ] {
        sqlx::query(&format!("DELETE FROM {t}"))
            .execute(pool)
            .await
            .ok();
    }
}

fn fill_payload() -> serde_json::Value {
    json!({
        "tid": 42,
        "oid": "12345",
        "cloid": hl_cloid(1),
        "coin": "BTC",
        "side": "B",
        "px": "50000.5",
        "sz": "1.25",
        "fee": "0.15",
        "closedPnl": "0",
        "crossed": false,
        "startPosition": "0",
        "time": 1_700_000_000_000i64
    })
}

#[tokio::test]
async fn ingest_fill_creates_fact_chain() {
    let Some(pool) = try_pool().await else { return };
    let _guard = acquire_test_lock(&pool).await;
    clean_tables(&pool).await;
    let projector = PostgresExchangeFactProjector::new(pool.clone(), ACCOUNT);

    let result = projector
        .ingest_fill(&fill_payload())
        .await
        .expect("ingest fill");
    assert!(result.processed);
    let projection = result.fill_projection.expect("fill projection");
    assert_eq!(projection.cloid, hl_cloid(1));
    assert_eq!(projection.symbol, "BTC");
    assert_eq!(projection.price.to_string(), "50000.5");
    assert_eq!(projection.size.to_string(), "1.25");
    assert_eq!(projection.fee.to_string(), "0.15");
    assert_eq!(projection.position_size.unwrap().to_string(), "1.25");
    assert_eq!(projection.order_status, "filled");

    // The fact chain: order + fill + position + 2 ledger entries + outbox.
    let (orders,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM orders WHERE exchange_oid = '12345'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(orders, 1);
    let (fills,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM fills WHERE exchange_fill_id = 'fill:42'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(fills, 1);
    let (positions,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM positions WHERE symbol = 'BTC'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(positions, 1);
    let (ledger,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM ledger_entries WHERE fill_id = $1")
            .bind(projection_cloid_fill(&pool).await)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(ledger, 2, "realized_pnl + fee ledger entries");
    let (outbox,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM outbox_events WHERE event_type = 'exchange.fill.ingested'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(outbox, 1);
    // Cursor advanced.
    let cursor = projector.cursor("fills").await.unwrap();
    assert_eq!(cursor, 1_700_000_000_000);
}

async fn projection_cloid_fill(pool: &PgPool) -> Uuid {
    let (fill_id,): (Uuid,) = sqlx::query_as("SELECT fill_id FROM fills LIMIT 1")
        .fetch_one(pool)
        .await
        .unwrap();
    fill_id
}

#[tokio::test]
async fn duplicate_fill_is_idempotent() {
    let Some(pool) = try_pool().await else { return };
    let _guard = acquire_test_lock(&pool).await;
    clean_tables(&pool).await;
    let projector = PostgresExchangeFactProjector::new(pool.clone(), ACCOUNT);

    let first = projector.ingest_fill(&fill_payload()).await.unwrap();
    let second = projector.ingest_fill(&fill_payload()).await.unwrap();
    assert!(first.processed);
    assert!(
        !second.processed,
        "same external id must dedupe via the inbox"
    );
    let (fills,): (i64,) = sqlx::query_as("SELECT count(*) FROM fills")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(fills, 1);
}

#[tokio::test]
async fn ingest_order_update_creates_projection() {
    let Some(pool) = try_pool().await else { return };
    let _guard = acquire_test_lock(&pool).await;
    clean_tables(&pool).await;
    let projector = PostgresExchangeFactProjector::new(pool.clone(), ACCOUNT);

    let update = json!({
        "channel": "orderUpdates",
        "order": {
            "oid": "98765",
            "cloid": hl_cloid(2),
            "coin": "ETH",
            "side": "A",
            "sz": "2.0",
            "limitPx": "3000",
            "status": "open"
        },
        "status": "open",
        "statusTimestamp": 1_700_000_000_500i64
    });
    let result = projector.ingest_order_update(&update).await.unwrap();
    assert!(result.processed);
    assert_eq!(
        result.external_event_id,
        "order:98765:acknowledged:1700000000500"
    );

    // Order bound + cursor.
    let (status, side): (String, String) =
        sqlx::query_as("SELECT status, side FROM orders WHERE exchange_oid = '98765'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "acknowledged");
    assert_eq!(side, "sell");
    let cursor = projector.cursor("orders").await.unwrap();
    assert_eq!(cursor, 1_700_000_000_500);
}

#[tokio::test]
async fn order_update_terminal_releases_reservation() {
    let Some(pool) = try_pool().await else { return };
    let _guard = acquire_test_lock(&pool).await;
    clean_tables(&pool).await;
    let projector = PostgresExchangeFactProjector::new(pool.clone(), ACCOUNT);

    // Seed an active reservation for the order we will mark cancelled.
    let order_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO orders (order_id, cloid, exchange_oid, symbol, side, order_type, time_in_force,
                            size, status, sub_account, is_spot, revision)
        VALUES ($1, $2, '555', 'BTC', 'buy', 'limit', 'Gtc', 1, 'acknowledged', $3, false, 0)
        "#,
    )
    .bind(order_id)
    .bind(hl_cloid(3))
    .bind(ACCOUNT)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO risk_reservations (reservation_id, command_id, order_id, symbol, side,
                                       reserved_size, reserved_notional, status, expires_at)
        VALUES ($1, $2, $3, 'BTC', 'buy', 1, 50000, 'active', now() + interval '1 day')
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(order_id)
    .execute(&pool)
    .await
    .unwrap();

    let update = json!({
        "order": {"oid": "555", "coin": "BTC", "side": "B", "sz": "1.0", "status": "cancelled"},
        "status": "cancelled",
        "statusTimestamp": 1_700_000_001_000i64
    });
    projector.ingest_order_update(&update).await.unwrap();

    let (status,): (String,) =
        sqlx::query_as("SELECT status FROM risk_reservations WHERE order_id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        status, "released",
        "terminal order update must release the reservation"
    );
}

#[tokio::test]
async fn filled_order_does_not_regress_on_late_cancelled_update() {
    // C7 regression: a stale/racing cancelled snapshot (WS vs REST reordering)
    // must not regress an order that was already filled.
    let Some(pool) = try_pool().await else { return };
    let _guard = acquire_test_lock(&pool).await;
    clean_tables(&pool).await;
    let projector = PostgresExchangeFactProjector::new(pool.clone(), ACCOUNT);

    let order_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO orders (order_id, cloid, exchange_oid, symbol, side, order_type, time_in_force,
                            size, status, sub_account, is_spot, revision)
        VALUES ($1, $2, '777', 'BTC', 'buy', 'limit', 'Gtc', 1, 'acknowledged', $3, false, 0)
        "#,
    )
    .bind(order_id)
    .bind(hl_cloid(7))
    .bind(ACCOUNT)
    .execute(&pool)
    .await
    .unwrap();

    // The fill arrives first.
    projector
        .ingest_order_update(&json!({
            "order": {"oid": "777", "coin": "BTC", "side": "B", "sz": "1.0", "status": "filled"},
            "status": "filled",
            "statusTimestamp": 1_700_000_003_000i64
        }))
        .await
        .unwrap();

    // A stale cancelled snapshot with an *older* timestamp lands afterwards.
    projector
        .ingest_order_update(&json!({
            "order": {"oid": "777", "coin": "BTC", "side": "B", "sz": "1.0", "status": "cancelled"},
            "status": "cancelled",
            "statusTimestamp": 1_700_000_001_000i64
        }))
        .await
        .unwrap();

    let (status,): (String,) = sqlx::query_as("SELECT status FROM orders WHERE order_id = $1")
        .bind(order_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        status, "filled",
        "a late cancelled snapshot must not regress a filled order (C7)"
    );
}

#[tokio::test]
async fn ingest_funding_records_payment_flat_shape() {
    // H-CH1 regression: the real REST `userFunding` payload is flat
    // (`{time, coin, usdc, ...}` with no `delta` and no `type`); the old
    // `delta.type == "funding"` validation rejected it. The flat shape must
    // ingest identically.
    let Some(pool) = try_pool().await else { return };
    let _guard = acquire_test_lock(&pool).await;
    clean_tables(&pool).await;
    let projector = PostgresExchangeFactProjector::new(pool.clone(), ACCOUNT);

    let update = json!({
        "time": 1_700_000_002_000i64,
        "coin": "BTC",
        "usdc": "-1.2345",
        "fundingRate": "0.00001",
        "szi": "1.25"
    });
    let result = projector.ingest_funding(&update).await.unwrap();
    assert!(result.processed);
    assert_eq!(result.funding_amount.unwrap().to_string(), "-1.2345");

    let (amount,): (bigdecimal::BigDecimal,) =
        sqlx::query_as("SELECT amount FROM funding_payments LIMIT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(amount.to_string(), "-1.2345");
    let cursor = projector.cursor("funding").await.unwrap();
    assert_eq!(cursor, 1_700_000_002_000);
}

#[tokio::test]
async fn funding_ws_and_rest_shapes_dedup_on_time_coin_usdc() {
    // H-CH1 regression: the same funding settlement arriving once as a WS
    // delta frame and once as a flat REST history item must collapse to one
    // `funding_payments` row. The external id is `(time, coin, usdc)` —
    // excluding the exchange hash, which WS frames do not carry.
    let Some(pool) = try_pool().await else { return };
    let _guard = acquire_test_lock(&pool).await;
    clean_tables(&pool).await;
    let projector = PostgresExchangeFactProjector::new(pool.clone(), ACCOUNT);

    let ws_delta = json!({
        "time": 1_700_000_004_000i64,
        "hash": "0xws1",
        "delta": {"coin": "BTC", "usdc": "-2.5", "fundingRate": "0.00002", "szi": "1.0"}
    });
    let rest_flat = json!({
        "time": 1_700_000_004_000i64,
        "hash": "0xrest1",
        "coin": "BTC",
        "usdc": "-2.5",
        "fundingRate": "0.00002",
        "szi": "1.0"
    });
    let first = projector.ingest_funding(&ws_delta).await.unwrap();
    assert!(first.processed);
    let second = projector.ingest_funding(&rest_flat).await.unwrap();
    assert!(
        !second.processed,
        "same (time, coin, usdc) must dedup: {:?}",
        second
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM funding_payments")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1, "one funding payment across WS + REST paths");

    // A genuinely different settlement still ingests.
    let next = json!({
        "time": 1_700_000_005_000i64,
        "coin": "BTC",
        "usdc": "-2.6",
        "fundingRate": "0.00002",
        "szi": "1.0"
    });
    let third = projector.ingest_funding(&next).await.unwrap();
    assert!(third.processed);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM funding_payments")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn synthetic_cloid_used_for_unknown_oid() {
    let Some(pool) = try_pool().await else { return };
    let _guard = acquire_test_lock(&pool).await;
    clean_tables(&pool).await;
    let projector = PostgresExchangeFactProjector::new(pool.clone(), ACCOUNT);

    // No cloid in the payload → synthetic cloid (0x + md5 of "exchange-order:999").
    let fill = json!({
        "tid": 99, "oid": "999", "coin": "BTC", "side": "B",
        "px": "50000", "sz": "0.5", "fee": "0", "time": 1_700_000_003_000i64
    });
    let result = projector.ingest_fill(&fill).await.unwrap();
    assert!(result.processed);
    let cloid = result.fill_projection.unwrap().cloid;
    assert!(cloid.starts_with("0x"), "synthetic cloid: {cloid}");
    assert_eq!(cloid.len(), 34);
}
