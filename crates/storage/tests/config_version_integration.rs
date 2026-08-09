//! Integration test for the Postgres config-version repository against a real
//! Postgres (the schema from `migrations/0001_create_all.sql`).
//!
//! Requires `HYPE_TEST_PG_URL` (default `postgres://postgres:testpass@localhost:55432/hypeedge_test`).
//! Skips when Postgres is unreachable.

use hypeedge_storage::config_version_pg::PostgresConfigVersionStore;
use hypeedge_storage::config_version_store::ConfigVersionStore;
use serde_json::json;
use sqlx::PgPool;

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
            eprintln!("SKIP: Postgres unreachable ({e}); integration test skipped");
            None
        }
    }
}


/// Cross-binary serialization (shares the HYPE advisory lock with the other
/// integration test binaries so `clean` calls do not clobber one another).
async fn acquire_test_lock(pool: &PgPool) -> sqlx::pool::PoolConnection<sqlx::Postgres> {
    let mut conn = pool.acquire().await.expect("acquire test connection");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(0x48455045i64)
        .execute(&mut *conn)
        .await
        .expect("advisory lock");
    conn
}

async fn clean(pool: &PgPool) {
    for t in ["funding_arb_config_versions", "market_maker_config_versions", "trend_follow_config_versions", "strategy_config_versions", "strategy_instances"] {
        sqlx::query(&format!("DELETE FROM {t}")).execute(pool).await.ok();
    }
}

async fn seed_instance(pool: &PgPool, strategy_id: &str, strategy_type: &str) {
    sqlx::query(
        r#"
        INSERT INTO strategy_instances (strategy_id, strategy_type, sub_account, symbol, desired_state, revision)
        VALUES ($1, $2, '0xabc', $3, 'stopped', 0)
        "#,
    )
    .bind(strategy_id)
    .bind(strategy_type)
    .bind(if strategy_type == "funding_arb" { "AUTO" } else { "BTC" })
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn create_and_list_trend_follow_config_versions() {
    let Some(pool) = try_pool().await else { return };
    let _guard = acquire_test_lock(&pool).await; // keep pool alive
    clean(&pool).await;
    seed_instance(&pool, "tf_1", "trend_follow").await;
    let store = PostgresConfigVersionStore::new(pool.clone());

    let values = json!({
        "fast_ema_period": 12, "slow_ema_period": 26, "signal_ema_period": 9,
        "momentum_period": 10, "momentum_threshold": "0.0001", "atr_period": 14,
        "atr_position_multiplier": "1.0", "atr_stop_multiplier": "2.0",
        "max_position_pct": "0.2", "risk_per_trade_pct": "0.01", "macd_cross_threshold": "0.5"
    });
    let record = store
        .create_config_version("tf_1", "trend_follow", &values, "test", Some(0))
        .await
        .expect("create");
    assert_eq!(record.version, 1);
    assert_eq!(record.config_hash.len(), 64);

    // Idempotent: same values → same version.
    let dup = store
        .create_config_version("tf_1", "trend_follow", &values, "test", None)
        .await
        .unwrap();
    assert_eq!(dup.version, 1, "hash-identical config must return the existing version");

    // Different values → version 2.
    let mut v2 = values.clone();
    v2["fast_ema_period"] = json!(21);
    let rec2 = store
        .create_config_version("tf_1", "trend_follow", &v2, "test", None)
        .await
        .unwrap();
    assert_eq!(rec2.version, 2);

    let listed = store.list_config_versions("tf_1").await.unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].version, 1);
    assert_eq!(listed[1].version, 2);
}

#[tokio::test]
async fn create_funding_arb_config_version() {
    let Some(pool) = try_pool().await else { return };
    let _guard = acquire_test_lock(&pool).await;
    clean(&pool).await;
    seed_instance(&pool, "fa_1", "funding_arb").await;
    let store = PostgresConfigVersionStore::new(pool.clone());

    let values = json!({
        "spot_coin": "@1",
        "entry_funding_rate": "0.0001", "exit_funding_rate": "0",
        "max_notional_usd": "1000", "hedge_ratio": "1",
        "rebalance_threshold_bps": 50, "leverage": "1",
        "max_slippage_bps": 50, "max_basis_bps": 500,
        "min_expected_edge_bps": "5", "expected_hold_hours": 8,
        "round_trip_fee_bps": "20", "max_unhedged_seconds": 15
    });
    let record = store
        .create_config_version("fa_1", "funding_arb", &values, "test", None)
        .await
        .unwrap();
    assert_eq!(record.version, 1);
    let listed = store.list_config_versions("fa_1").await.unwrap();
    assert_eq!(listed.len(), 1);
}

#[tokio::test]
async fn revision_conflict_rejected() {
    let Some(pool) = try_pool().await else { return };
    let _guard = acquire_test_lock(&pool).await;
    clean(&pool).await;
    seed_instance(&pool, "tf_2", "trend_follow").await;
    let store = PostgresConfigVersionStore::new(pool.clone());

    let values = json!({
        "fast_ema_period": 12, "slow_ema_period": 26, "signal_ema_period": 9,
        "momentum_period": 10, "momentum_threshold": "0.0001", "atr_period": 14,
        "atr_position_multiplier": "1.0", "atr_stop_multiplier": "2.0",
        "max_position_pct": "0.2", "risk_per_trade_pct": "0.01", "macd_cross_threshold": "0.5"
    });
    // Instance revision is 0; expect 1 conflicts.
    let err = store
        .create_config_version("tf_2", "trend_follow", &values, "test", Some(1))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("revision conflict"), "err: {err}");
}

#[tokio::test]
async fn unknown_instance_rejected() {
    let Some(pool) = try_pool().await else { return };
    let _guard = acquire_test_lock(&pool).await;
    clean(&pool).await;
    let store = PostgresConfigVersionStore::new(pool.clone());
    let values = json!({"fast_ema_period": 12, "slow_ema_period": 26, "signal_ema_period": 9,
        "momentum_period": 10, "momentum_threshold": "0.0001", "atr_period": 14,
        "atr_position_multiplier": "1.0", "atr_stop_multiplier": "2.0",
        "max_position_pct": "0.2", "risk_per_trade_pct": "0.01", "macd_cross_threshold": "0.5"});
    let err = store
        .create_config_version("nope", "trend_follow", &values, "test", None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Unknown active strategy instance"), "err: {err}");
}
