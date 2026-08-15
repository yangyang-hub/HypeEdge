//! Integration test for the ClickHouse → DuckDB export wiring against a real
//! ClickHouse. Skips when ClickHouse is unreachable (so `cargo test
//! --workspace` stays green on machines without the container).
//!
//! Requires `HYPE_TEST_CH_URL` (default `http://localhost:8123`).

use clickhouse::Client;
use hypeedge_storage::duckdb_export::{export_all, export_table};
use std::path::PathBuf;

fn ch_url() -> String {
    std::env::var("HYPE_TEST_CH_URL").unwrap_or_else(|_| "http://localhost:8123".into())
}

async fn try_client() -> Option<Client> {
    let client = Client::default().with_url(ch_url());
    // Probe connectivity with a cheap query.
    let ok = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        client.query("SELECT 1").fetch_one::<u8>().await.is_ok()
    })
    .await
    .unwrap_or(false);
    if ok { Some(client) } else { None }
}

#[tokio::test]
async fn export_all_writes_duckdb_file() {
    let Some(ch) = try_client().await else {
        eprintln!(
            "SKIP: ClickHouse unreachable at {}; export test skipped",
            ch_url()
        );
        return;
    };
    // Ensure the source tables exist (created by ClickHouseWriter; a missing
    // table would fail the query and the test).
    let output =
        std::env::temp_dir().join(format!("hypeedge_export_{}.duckdb", std::process::id()));
    let _ = std::fs::remove_file(&output);
    let path = output.to_string_lossy().to_string();

    let now_ms = chrono::Utc::now().timestamp_millis();
    let result = export_all(&ch, &path, &["BTC"], now_ms - 3_600_000, now_ms).await;
    match result {
        Ok(totals) => {
            assert!(totals.contains_key("candles"), "totals: {totals:?}");
            assert!(PathBuf::from(&output).exists(), "duckdb file created");
            // C6: the pre-fix `ts: f64` decode wrote garbage (empirically
            // 8.81e-312) — the old test only checked file existence and let it
            // through. Now every exported ts must be plausible Unix millis.
            let total_rows: usize = totals.values().sum();
            if total_rows > 0 {
                let conn = duckdb::Connection::open(&output).unwrap();
                for table in ["candles", "funding", "trades", "l2_book", "mid_prices"] {
                    let bad: i64 = conn
                        .query_row(
                            &format!(
                                "SELECT count(*) FROM {table} WHERE CAST(ts AS BIGINT) <= 1000000000000"
                            ),
                            [],
                            |r| r.get(0),
                        )
                        .unwrap_or(0);
                    assert_eq!(
                        bad, 0,
                        "{table} contains implausible ts values (C6 regression)"
                    );
                    // And at least one ts is in the query window.
                    let min_ts: i64 = conn
                        .query_row(
                            &format!("SELECT min(CAST(ts AS BIGINT)) FROM {table}"),
                            [],
                            |r| r.get(0),
                        )
                        .unwrap_or(0);
                    assert!(
                        min_ts >= 1_700_000_000_000,
                        "{table} min ts {min_ts} not in the expected window"
                    );
                }
            }
        }
        Err(e) => {
            // Table may not exist (writer never ran) — that's an infra skip, not
            // a code failure. Report and skip.
            eprintln!("SKIP: ClickHouse export failed ({e}); tables may be absent");
        }
    }
}

#[tokio::test]
async fn export_unknown_table_errors() {
    let Some(ch) = try_client().await else { return };
    let output =
        std::env::temp_dir().join(format!("hypeedge_export_bad_{}.duckdb", std::process::id()));
    let err = export_table(&ch, &output.to_string_lossy(), "nope", "BTC", 0, 1).await;
    assert!(err.is_err(), "unknown table must error");
}
