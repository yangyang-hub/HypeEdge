//! Integration test for the durable-order transaction boundary against a real
//! Postgres (the schema from `migrations/0001_create_all.sql`).
//!
//! Requires `HYPE_TEST_PG_URL` (default `postgres://postgres:testpass@localhost:55432/hypeedge_test`).
//! The test assumes the schema is already applied (see `docker exec` path in the
//! Phase-1 work; `0001_create_all.sql` is applied by `pg.rs` migrations at
//! connect time).

use chrono::Utc;
use hypeedge_domain::decimal::Decimal;
use hypeedge_domain::enums::{OrderStatus, OrderType, Side, TimeInForce};
use hypeedge_domain::models::{Order, RiskCheckResult};
use hypeedge_storage::durable_order_store::PostgresDurableOrderStore;
use hypeedge_storage::outbox::PostgresOutboxStore;
use sqlx::PgPool;
use uuid::Uuid;

fn test_pg_url() -> String {
    std::env::var("HYPE_TEST_PG_URL").unwrap_or_else(|_| {
        "postgres://postgres:testpass@localhost:55432/hypeedge_test".to_string()
    })
}

/// Serializes tests that share the (default NULL sub-account) account scope.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Connect, or `None` if Postgres is unreachable (tests skip rather than fail
/// so `cargo test --workspace` stays green on machines without the container).
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

/// The `orders.cloid` column is `0x` + 32 lowercase hex (CHECK constraint).
fn valid_cloid(n: u64) -> String {
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
    // Children first — `fills`, `ledger_entries`, `order_events` all hold FKs
    // into `orders`, so `orders` must be deleted last.
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
        "account_state",
    ] {
        sqlx::query(&format!("DELETE FROM {t}"))
            .execute(pool)
            .await
            .unwrap();
    }
}

fn make_order(cloid: &str, side: Side, size: &str, price: Option<&str>) -> Order {
    Order {
        cloid: cloid.to_string(),
        symbol: "BTC".to_string(),
        side,
        size: hypeedge_domain::Size::new(Decimal::from_str_strict(size).unwrap()),
        price: price.map(|p| hypeedge_domain::Price::new(Decimal::from_str_strict(p).unwrap())),
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::Gtc,
        status: OrderStatus::Pending,
        strategy_id: Some("test_strat".to_string()),
        sub_account: None,
        reduce_only: false,
        is_spot: false,
        risk_reducing: false,
        max_slippage_bps: 50,
        exchange_oid: None,
        filled_size: hypeedge_domain::Size::ZERO,
        avg_fill_price: None,
        submitted_at: None,
        acknowledged_at: None,
        filled_at: None,
        error_message: None,
        created_at: Utc::now(),
    }
}

fn passing_risk() -> RiskCheckResult {
    RiskCheckResult {
        passed: true,
        reason: None,
        checked_limits: vec!["test".into()],
    }
}

#[tokio::test]
async fn persist_placement_writes_all_rows_in_one_tx() {
    let _guard = SERIAL.lock().await;
    let Some(pool) = try_pool().await else { return };
    let _test_lock = acquire_test_lock(&pool).await;
    clean_tables(&pool).await;

    // Seed account + position so the DB scope check passes.
    sqlx::query(
        "INSERT INTO account_state (sub_account, equity, available_balance, total_margin_used, total_unrealized_pnl, peak_equity, action_credits_remaining, exchange_updated_at, revision, updated_at) VALUES (NULL, 10000, 10000, 0, 0, 10000, 10000, now(), 1, now())",
    )
    .execute(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO positions (position_id, sub_account, symbol, size, mark_price, leverage, exchange_updated_at, revision, created_at, updated_at) VALUES ($1, NULL, 'BTC', 0, 100, 5, now(), 1, now(), now())",
    )
    .bind(Uuid::new_v4())
    .execute(&pool).await.unwrap();

    let store = PostgresDurableOrderStore::default();
    let cloid = valid_cloid(1);
    let mut order = make_order(&cloid, Side::Buy, "0.1", Some("100"));
    order.status = OrderStatus::Submitted; // engine sets this before persisting
    let command_id = Uuid::new_v4();

    let effective = store
        .persist_placement(&pool, &mut order, &passing_risk(), command_id, true, None)
        .await
        .expect("placement persists");

    assert!(
        effective.passed,
        "DB scope check should pass: {effective:?}"
    );

    // All rows present.
    let orders: i64 = sqlx::query_scalar("SELECT count(*) FROM orders WHERE cloid=$1")
        .bind(&cloid)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(orders, 1);
    let risk_events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM risk_events WHERE command_id=$1")
            .bind(command_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(risk_events, 1);
    let commands: i64 =
        sqlx::query_scalar("SELECT count(*) FROM execution_commands WHERE command_id=$1")
            .bind(command_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(commands, 1);
    let reservations: i64 =
        sqlx::query_scalar("SELECT count(*) FROM risk_reservations WHERE command_id=$1")
            .bind(command_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(reservations, 1);
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM order_events WHERE order_id=(SELECT order_id FROM orders WHERE cloid=$1)")
        .bind(&cloid)
        .fetch_one(&pool).await.unwrap();
    assert_eq!(events, 1);
    let outbox: i64 =
        sqlx::query_scalar("SELECT count(*) FROM outbox_events WHERE event_type='order.submitted'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(outbox, 1);

    // The order status reflects dispatch (unchanged from the engine's value).
    assert_eq!(
        order.status,
        OrderStatus::Submitted,
        "in-memory order status reflects dispatch"
    );
}

#[tokio::test]
async fn risk_scope_rejects_when_limits_exceeded() {
    let _guard = SERIAL.lock().await;
    let Some(pool) = try_pool().await else { return };
    let _test_lock = acquire_test_lock(&pool).await;
    clean_tables(&pool).await;
    sqlx::query(
        "INSERT INTO account_state (sub_account, equity, available_balance, total_margin_used, total_unrealized_pnl, peak_equity, action_credits_remaining, exchange_updated_at, revision, updated_at) VALUES (NULL, 100, 100, 0, 0, 100, 10000, now(), 1, now())",
    )
    .execute(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO positions (position_id, sub_account, symbol, size, mark_price, leverage, exchange_updated_at, revision, created_at, updated_at) VALUES ($1, NULL, 'BTC', 0, 100, 5, now(), 1, now(), now())",
    )
    .bind(Uuid::new_v4())
    .execute(&pool).await.unwrap();

    // A 1.0 BTC order at price 100 = notional 100 = equity 100, so position_pct
    // 100% >> 20% default -> reject.
    let store = PostgresDurableOrderStore::default();
    let cloid = valid_cloid(2);
    let mut order = make_order(&cloid, Side::Buy, "1.0", Some("100"));
    let effective = store
        .persist_placement(
            &pool,
            &mut order,
            &passing_risk(),
            Uuid::new_v4(),
            true,
            None,
        )
        .await
        .unwrap();
    assert!(!effective.passed, "should reject: {effective:?}");
    assert_eq!(
        effective.reason.as_deref(),
        Some("position_limit_exceeded_with_reservations")
    );
    assert_eq!(order.status, OrderStatus::Rejected);
    // Order still persisted (as rejected) — the placement is durable.
    let orders: i64 = sqlx::query_scalar("SELECT count(*) FROM orders WHERE cloid=$1")
        .bind(&cloid)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(orders, 1);
}

#[tokio::test]
async fn command_queue_claim_and_defer_unknown() {
    let _guard = SERIAL.lock().await;
    let Some(pool) = try_pool().await else { return };
    let _test_lock = acquire_test_lock(&pool).await;
    clean_tables(&pool).await;

    // Seed an order + command.
    let cloid = valid_cloid(3);
    let order_id = Uuid::new_v4();
    let order_command_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO orders (order_id, command_id, cloid, symbol, side, order_type, time_in_force, size, status, max_slippage_bps, revision, created_at, updated_at) VALUES ($1,$2,$3,'BTC','buy','limit','Gtc',0.1,'pending',50,1,now(),now())",
    )
    .bind(order_id)
    .bind(order_command_id)
    .bind(&cloid)
    .execute(&pool).await.unwrap();

    let command_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO execution_commands (command_id, order_id, command_type, actor_type, actor_id, idempotency_key, status, payload, available_at, created_at, updated_at) VALUES ($1,$2,'place_order','system','test',$3,'pending','{}',now(),now(),now())",
    )
    .bind(command_id)
    .bind(order_id)
    .bind(&cloid)
    .execute(&pool).await.unwrap();

    // Use a 0 recheck delay so the requeued unknown is immediately claimable.
    let queue = hypeedge_storage::command_queue::PostgresExecutionCommandQueue::new(15, 0);
    let claimed = queue.claim(&pool, "worker_1").await.unwrap();
    assert!(claimed.is_some(), "should claim the pending command");
    let cmd = claimed.unwrap();
    assert_eq!(cmd.command_id, command_id);
    assert!(!cmd.requires_resolution);

    // defer_unknown requeues it.
    queue
        .defer_unknown(&pool, cmd.command_id, "timeout")
        .await
        .unwrap();
    let claimed_again = queue.claim(&pool, "worker_2").await.unwrap();
    let cmd2 = claimed_again.unwrap();
    assert!(
        cmd2.requires_resolution,
        "requeued unknown must require resolution"
    );
}

#[tokio::test]
async fn outbox_claim_publish_and_replay() {
    let _guard = SERIAL.lock().await;
    let Some(pool) = try_pool().await else { return };
    let _test_lock = acquire_test_lock(&pool).await;
    clean_tables(&pool).await;

    sqlx::query(
        "INSERT INTO outbox_events (event_id, event_type, aggregate_type, aggregate_id, aggregate_revision, payload) VALUES ($1,'test.event','order','o1',1,'{}')",
    )
    .bind(Uuid::new_v4())
    .execute(&pool).await.unwrap();

    let outbox = PostgresOutboxStore::default();
    let claimed = outbox.claim_batch(&pool, "dispatcher_1", 10).await.unwrap();
    assert_eq!(claimed.len(), 1);
    let ev = &claimed[0];
    assert_eq!(ev.event_type, "test.event");

    let published = outbox
        .mark_published(&pool, ev, "dispatcher_1")
        .await
        .unwrap();
    assert!(published);
    // Second mark is a no-op.
    let again = outbox
        .mark_published(&pool, ev, "dispatcher_1")
        .await
        .unwrap();
    assert!(!again);

    // Replay reads it.
    let replayed = outbox
        .read_after(&pool, 0, ev.sequence + 1, 10)
        .await
        .unwrap();
    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].sequence, ev.sequence);
}

#[tokio::test]
async fn concurrent_placements_serialize_on_risk_scope() {
    let _guard = SERIAL.lock().await;
    let Some(_pool) = try_pool().await else {
        return;
    };
    // Two placements racing for the same (NULL) account scope must serialize
    // on the `FOR UPDATE` account lock. With equity=200 and max_position_pct
    // 0.20, the second placement must observe the first's reservation and be
    // rejected (position limit), proving the DB-level admission is not racy.
    let pool = PgPool::connect(&test_pg_url()).await.unwrap();
    clean_tables(&pool).await;
    sqlx::query(
        "INSERT INTO account_state (sub_account, equity, available_balance, total_margin_used, total_unrealized_pnl, peak_equity, action_credits_remaining, exchange_updated_at, revision, updated_at) VALUES (NULL, 200, 200, 0, 0, 200, 10000, now(), 1, now())",
    )
    .execute(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO positions (position_id, sub_account, symbol, size, mark_price, leverage, exchange_updated_at, revision, created_at, updated_at) VALUES ($1, NULL, 'BTC', 0, 100, 5, now(), 1, now(), now())",
    )
    .bind(Uuid::new_v4())
    .execute(&pool).await.unwrap();

    let store = std::sync::Arc::new(PostgresDurableOrderStore::default());
    // Ceiling = 20% × 200 equity = 40 notional. Each order 0.3 BTC @ 100 = 30
    // (passes alone); two combined = 60 (second must be rejected via reservation).
    let mut order_a = make_order(&valid_cloid(10), Side::Buy, "0.3", Some("100"));
    order_a.status = OrderStatus::Submitted;
    let mut order_b = make_order(&valid_cloid(11), Side::Buy, "0.3", Some("100"));
    order_b.status = OrderStatus::Submitted;
    let pool_a = pool.clone();
    let pool_b = pool.clone();
    let store_a = store.clone();
    let store_b = store.clone();

    let a = tokio::spawn(async move {
        store_a
            .persist_placement(
                &pool_a,
                &mut order_a,
                &passing_risk(),
                Uuid::new_v4(),
                true,
                None,
            )
            .await
            .unwrap()
    });
    let b = tokio::spawn(async move {
        store_b
            .persist_placement(
                &pool_b,
                &mut order_b,
                &passing_risk(),
                Uuid::new_v4(),
                true,
                None,
            )
            .await
            .unwrap()
    });

    let (ra, rb) = tokio::join!(a, b);
    let ra = ra.unwrap();
    let rb = rb.unwrap();

    // Exactly one of the two may pass; the second sees the reservation and the
    // combined 100% position exceeds the 20% ceiling.
    let passed = [ra.passed, rb.passed];
    assert_eq!(
        passed.iter().filter(|p| **p).count(),
        1,
        "exactly one placement passes: {passed:?}"
    );
}

#[tokio::test]
async fn system_state_transition_writes_outbox() {
    let _guard = SERIAL.lock().await;
    let Some(pool) = try_pool().await else { return };
    let _test_lock = acquire_test_lock(&pool).await;
    clean_tables(&pool).await;

    let store = hypeedge_storage::system_state_store::PostgresSystemStateStore;
    store
        .transition(&pool, "cancel_only", Some("test"), true, "app")
        .await
        .unwrap();
    let loaded = store.load(&pool).await.unwrap();
    let state = loaded.unwrap();
    assert_eq!(state.state, "cancel_only");
    assert!(state.kill_switch_active);
    assert_eq!(state.reason.as_deref(), Some("test"));

    let outbox: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM outbox_events WHERE event_type='system.safety.transitioned'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(outbox, 1);
}

/// Seed the minimal account + position rows the DB risk-scope check requires.
async fn seed_account_and_position(pool: &sqlx::PgPool) {
    sqlx::query(
        "INSERT INTO account_state (sub_account, equity, available_balance, total_margin_used, total_unrealized_pnl, peak_equity, action_credits_remaining, exchange_updated_at, revision, updated_at) VALUES (NULL, 10000, 10000, 0, 0, 10000, 10000, now(), 1, now())",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO positions (position_id, sub_account, symbol, size, mark_price, leverage, exchange_updated_at, revision, created_at, updated_at) VALUES ($1, NULL, 'BTC', 0, 100, 5, now(), 1, now(), now())",
    )
    .bind(Uuid::new_v4())
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn system_order_cancel_does_not_violate_unique_command_key() {
    // A10 regression: a system order (strategy_id None) places an
    // execution_commands row with (actor_id='execution_engine', cloid); the
    // cancel command must use a distinct idempotency key or the UNIQUE
    // constraint rejects it (breaking cancel_all). A repeat cancel is idempotent.
    let _guard = SERIAL.lock().await;
    let Some(pool) = try_pool().await else { return };
    let _test_lock = acquire_test_lock(&pool).await;
    clean_tables(&pool).await;
    seed_account_and_position(&pool).await;

    let store = PostgresDurableOrderStore::default();
    let cloid = valid_cloid(101);
    let mut order = make_order(&cloid, Side::Buy, "0.1", Some("100"));
    order.strategy_id = None; // system order -> actor_id 'execution_engine'
    order.status = OrderStatus::Submitted;
    store
        .persist_placement(&pool, &mut order, &passing_risk(), Uuid::new_v4(), true, None)
        .await
        .expect("placement persists for a system order");

    store
        .persist_cancel_requested(&pool, &order, Uuid::new_v4())
        .await
        .expect("cancel command must not collide with the place idempotency key");

    store
        .persist_cancel_requested(&pool, &order, Uuid::new_v4())
        .await
        .expect("repeat cancel must be idempotent (ON CONFLICT DO NOTHING)");

    let cancels: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM execution_commands WHERE command_type='cancel_order' AND idempotency_key=$1",
    )
    .bind(format!("cancel:{}", &cloid))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(cancels, 1, "repeat cancel must not insert a second row");
}

#[tokio::test]
async fn immediate_fill_persists_fill_fields() {
    // A11 regression: an immediate-fill transition must persist filled_size,
    // avg_fill_price, and filled_at — not `status='filled'` with zero fill data.
    let _guard = SERIAL.lock().await;
    let Some(pool) = try_pool().await else { return };
    let _test_lock = acquire_test_lock(&pool).await;
    clean_tables(&pool).await;
    seed_account_and_position(&pool).await;

    let store = PostgresDurableOrderStore::default();
    let cloid = valid_cloid(102);
    let mut order = make_order(&cloid, Side::Buy, "0.1", Some("100"));
    order.status = OrderStatus::Submitted;
    store
        .persist_placement(&pool, &mut order, &passing_risk(), Uuid::new_v4(), true, None)
        .await
        .expect("placement persists");

    // Mirror the engine's immediate-fill handling: set the fill aggregates on
    // the order, then persist the `filled` transition.
    let mut filled = order.clone();
    filled.status = OrderStatus::Filled;
    filled.filled_size = hypeedge_domain::Size::new(Decimal::from_str_strict("0.1").unwrap());
    filled.avg_fill_price =
        Some(hypeedge_domain::Price::new(Decimal::from_str_strict("99").unwrap()));
    filled.filled_at = Some(Utc::now());
    store
        .persist_transition(&pool, &filled, "filled", Some(Uuid::new_v4()), Some("succeeded"))
        .await
        .expect("fill transition persists");

    let (status, filled_size, avg_fill_price, filled_at): (String, String, Option<String>, Option<chrono::DateTime<Utc>>) =
        sqlx::query_as(
            "SELECT status, filled_size::text, avg_fill_price::text, filled_at FROM orders WHERE cloid=$1",
        )
        .bind(&cloid)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "filled");
    // NUMERIC(38,18) renders the full scale; the fix is that this is the real
    // fill (not `0`), proving merge_transport_transition persists fill fields.
    assert_eq!(filled_size, "0.100000000000000000", "filled_size must be persisted (A11)");
    assert_eq!(avg_fill_price.as_deref(), Some("99.000000000000000000"));
    assert!(filled_at.is_some(), "filled_at must be persisted (A11)");
}
