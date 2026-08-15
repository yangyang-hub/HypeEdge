//! Integration test for the Postgres funding-arb cycle store against a real
//! Postgres (the schema from `migrations/0001_create_all.sql`).

use hypeedge_domain::decimal::Decimal;
use hypeedge_domain::enums::FundingArbCycleState;
use hypeedge_storage::funding_arb_store::PostgresFundingArbCycleStore;
use hypeedge_trading::funding_arb::models::FundingArbCycle;
use hypeedge_trading::funding_arb::store::FundingArbCycleStore;
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

async fn acquire_test_lock(pool: &PgPool) -> sqlx::pool::PoolConnection<sqlx::Postgres> {
    let mut conn = pool.acquire().await.expect("acquire test connection");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(0x48455045i64)
        .execute(&mut *conn)
        .await
        .expect("advisory lock");
    conn
}

fn cycle(strategy_id: &str, state: FundingArbCycleState) -> FundingArbCycle {
    FundingArbCycle {
        cycle_id: Uuid::new_v4(),
        strategy_id: strategy_id.into(),
        config_revision: 1,
        sub_account: "0xabc".into(),
        perp_symbol: "BTC".into(),
        spot_symbol: "@1".into(),
        spot_display: "BTC/USDC".into(),
        base_token: "BTC".into(),
        quote_token: "USDC".into(),
        state,
        target_perp_size: Decimal::from_str_lenient("1").unwrap(),
        target_spot_size: Decimal::from_str_lenient("1").unwrap(),
        perp_open_size: Decimal::ZERO,
        spot_open_size: Decimal::ZERO,
        baseline_spot_size: Decimal::ZERO,
        entry_funding_rate: Decimal::from_str_lenient("0.001").unwrap(),
        entry_basis_bps: Decimal::from_str_lenient("10").unwrap(),
        revision: 0,
        spot_entry_cloid: None,
        perp_entry_cloid: None,
        compensation_cloid: None,
        perp_exit_cloid: None,
        spot_exit_cloid: None,
        error_code: None,
        error_message: None,
        opened_at: None,
        closed_at: None,
        created_at: None,
        updated_at: None,
    }
}

#[tokio::test]
async fn funding_arb_cycle_create_get_transition() {
    let Some(pool) = try_pool().await else { return };
    let _guard = acquire_test_lock(&pool).await;
    sqlx::query("DELETE FROM funding_arb_cycle_events")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM funding_arb_cycles")
        .execute(&pool)
        .await
        .unwrap();
    // strategy_config_versions is shared; other strategy config tables FK into it.
    for t in [
        "trend_follow_config_versions",
        "market_maker_config_versions",
        "funding_arb_config_versions",
        "strategy_config_versions",
    ] {
        sqlx::query(&format!("DELETE FROM {t}"))
            .execute(&pool)
            .await
            .unwrap();
    }
    sqlx::query("DELETE FROM strategy_instances")
        .execute(&pool)
        .await
        .unwrap();

    let store = PostgresFundingArbCycleStore::new(pool.clone());
    // The cycle FK requires a strategy instance + a config version to exist.
    sqlx::query(
        "INSERT INTO strategy_instances (strategy_id, strategy_type, symbol) VALUES ($1, 'funding_arb', 'AUTO')",
    )
    .bind("fa_test")
    .execute(&pool)
    .await
    .unwrap();
    let (config_version_id,): (i64,) = sqlx::query_as(
        "INSERT INTO strategy_config_versions (strategy_id, version, config_hash, created_by) VALUES ($1, 1, $2, 'test') RETURNING id",
    )
    .bind("fa_test")
    .bind("a".repeat(64))
    .fetch_one(&pool)
    .await
    .unwrap();
    let mut c = cycle("fa_test", FundingArbCycleState::EnteringSpot);
    c.config_revision = config_version_id as u64;
    let created = store.create(&c).await.unwrap();
    assert_eq!(created.state, FundingArbCycleState::EnteringSpot);

    // get_active returns the non-closed cycle.
    let active = store
        .get_active("fa_test")
        .await
        .unwrap()
        .expect("active cycle");
    assert_eq!(active.cycle_id, c.cycle_id);

    // Optimistic transition Open.
    let opened = store
        .transition(
            &created,
            FundingArbCycleState::Open,
            "cycle_opened",
            None,
            serde_json::json!({ "perp_open_size": "1", "spot_open_size": "1" }),
        )
        .await
        .unwrap();
    assert_eq!(opened.state, FundingArbCycleState::Open);
    assert_eq!(opened.revision, 2, "create(1) + open(2)");
    assert_eq!(opened.perp_open_size.to_string(), "1");

    // A stale transition (revision 0 against revision 1) must be refused.
    let stale = store
        .transition(
            &created, // still at revision 0
            FundingArbCycleState::Closed,
            "cycle_closed",
            None,
            serde_json::json!({}),
        )
        .await;
    assert!(stale.is_err(), "stale revision transition must be refused");

    // Close from the current revision.
    let closed = store
        .transition(
            &opened,
            FundingArbCycleState::Closed,
            "cycle_closed",
            None,
            serde_json::json!({}),
        )
        .await
        .unwrap();
    assert_eq!(closed.state, FundingArbCycleState::Closed);
    assert!(closed.closed_at.is_some());
    // No longer active.
    assert!(store.get_active("fa_test").await.unwrap().is_none());
}

#[tokio::test]
async fn faulted_cycle_is_not_active() {
    // M-CH6: `faulted` is terminal — `get_active` must not return it, or a
    // crash-recovery path would keep driving a cycle the runtime gave up on.
    let Some(pool) = try_pool().await else { return };
    let _guard = acquire_test_lock(&pool).await;
    sqlx::query("DELETE FROM funding_arb_cycle_events")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM funding_arb_cycles")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM strategy_instances")
        .execute(&pool)
        .await
        .unwrap();

    let store = PostgresFundingArbCycleStore::new(pool.clone());
    sqlx::query(
        "INSERT INTO strategy_instances (strategy_id, strategy_type, symbol) VALUES ($1, 'funding_arb', 'AUTO')",
    )
    .bind("fa_fault_test")
    .execute(&pool)
    .await
    .unwrap();
    let (config_version_id,): (i64,) = sqlx::query_as(
        "INSERT INTO strategy_config_versions (strategy_id, version, config_hash, created_by) VALUES ($1, 1, $2, 'test') RETURNING id",
    )
    .bind("fa_fault_test")
    .bind("b".repeat(64))
    .fetch_one(&pool)
    .await
    .unwrap();
    let mut c = cycle("fa_fault_test", FundingArbCycleState::EnteringSpot);
    c.config_revision = config_version_id as u64;
    let created = store.create(&c).await.unwrap();

    // Still active while mid-transition.
    assert!(store.get_active("fa_fault_test").await.unwrap().is_some());

    let faulted = store
        .transition(
            &created,
            FundingArbCycleState::Faulted,
            "cycle_faulted",
            Some(serde_json::json!({"error_code": "e"})),
            serde_json::json!({}),
        )
        .await
        .unwrap();
    assert_eq!(faulted.state, FundingArbCycleState::Faulted);
    assert!(
        store.get_active("fa_fault_test").await.unwrap().is_none(),
        "faulted cycle must not be reported as active (M-CH6)"
    );
}
