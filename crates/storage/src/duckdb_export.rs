//! ClickHouse → DuckDB export wiring, port of `src/hypeedge/storage/duckdb_export.py`.
//!
//! Owns the DuckDB write side ([`DuckDBExporter`] + [`FetchedTable`]) and the
//! ClickHouse query that feeds it: fetch a market-data table for a coin + time
//! range, collect the rows as strings, and write them into a local DuckDB file
//! for offline analysis. Runs on demand (CLI/script), not as a background task.

use std::path::Path;

use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

/// A fetched table: column names + rows (values as strings).
pub struct FetchedTable {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

/// Export ClickHouse market data to a local DuckDB file.
pub struct DuckDBExporter {
    output_path: std::path::PathBuf,
}

impl DuckDBExporter {
    pub fn new(output_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            output_path: output_path.into(),
        }
    }

    /// Write one table's rows into the DuckDB file. Creates the table with
    /// `VARCHAR` columns if absent (mirrors the Python auto-schema). Returns
    /// the number of rows written.
    pub fn export_table(&self, table: &str, data: &FetchedTable) -> Result<usize, String> {
        if data.rows.is_empty() || data.columns.is_empty() {
            return Ok(0);
        }
        let path = self.output_path.as_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("create dir: {e}"))?;
        }
        let conn = duckdb::Connection::open(path).map_err(|e| format!("open duckdb: {e}"))?;

        let col_list: Vec<String> = data
            .columns
            .iter()
            .map(|c| format!("\"{c}\" VARCHAR"))
            .collect();
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {table} ({})",
            col_list.join(", ")
        );
        conn.execute(&ddl, [])
            .map_err(|e| format!("create table: {e}"))?;

        let placeholders = vec!["?".to_string(); data.columns.len()].join(", ");
        let insert_sql = format!("INSERT INTO {table} VALUES ({placeholders})");

        let mut written = 0usize;
        for row in &data.rows {
            let params = row
                .iter()
                .map(|v| duckdb::types::Value::Text(v.clone().unwrap_or_default()))
                .collect::<Vec<_>>();
            conn.execute(&insert_sql, duckdb::params_from_iter(params))
                .map_err(|e| format!("insert: {e}"))?;
            written += 1;
        }
        Ok(written)
    }
}

/// Convenience: build a `FetchedTable` from a ClickHouse query result.
///
/// The `clickhouse` crate's `RowCursor` is consumed column-by-column; this
/// helper expects the caller to have already collected rows as strings.
pub fn fetched_table(columns: Vec<String>, rows: Vec<Vec<Option<String>>>) -> FetchedTable {
    FetchedTable { columns, rows }
}

/// The market-data tables exported by `export_all`.
pub const EXPORT_TABLES: &[&str] = &["candles", "funding", "trades", "l2_book", "mid_prices"];

/// Whether a DuckDB file exists at the output path.
pub fn duckdb_file_exists(path: &Path) -> bool {
    path.exists()
}

/// Row types for each exported table. `?fields` expands the columns in
/// declaration order, matching the DuckDB column list.
///
/// `ts` is `i64` Unix **milliseconds** (C6): ClickHouse stores `ts` as
/// `DateTime64(3)`, which travels on the wire as an `Int64` of milliseconds.
/// The old `f64` read the raw millis bits as a float and produced garbage
/// values (empirically `8.81e-312`).
#[derive(Debug, Row, Serialize, Deserialize, Clone)]
struct CandleChRow {
    ts: i64,
    coin: String,
    interval: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

#[derive(Debug, Row, Serialize, Deserialize, Clone)]
struct FundingChRow {
    ts: i64,
    coin: String,
    funding_rate: f64,
    premium: f64,
    oi: f64,
    mark_px: f64,
}

#[derive(Debug, Row, Serialize, Deserialize, Clone)]
struct TradeChRow {
    ts: i64,
    coin: String,
    px: f64,
    sz: f64,
    side: u8,
    tid: u64,
}

#[derive(Debug, Row, Serialize, Deserialize, Clone)]
struct L2BookChRow {
    ts: i64,
    coin: String,
    side: u8,
    level: u16,
    px: f64,
    sz: f64,
}

#[derive(Debug, Row, Serialize, Deserialize, Clone)]
struct MidPriceChRow {
    ts: i64,
    coin: String,
    px: f64,
}

/// Earliest plausible Unix-millis timestamp (~2001-09-09): any exported row
/// below this is garbage (C6 — the pre-fix exports carried `8.81e-312`).
const TS_MIN_MILLIS: i64 = 1_000_000_000_000;

/// Export one market-data table for a coin + time range from ClickHouse into
/// the DuckDB file. Returns the number of rows written.
///
/// `start_ms`/`end_ms` are Unix milliseconds; ClickHouse stores `ts` as
/// `DateTime64(3)` (ms precision).
pub async fn export_table(
    ch: &Client,
    output_path: &str,
    table: &str,
    coin: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<usize, String> {
    let rows: Vec<Vec<Option<String>>> = query_rows(ch, table, coin, start_ms, end_ms).await?;
    if rows.is_empty() {
        return Ok(0);
    }
    let columns = table_columns(table)?;
    let data = fetched_table(columns, rows);
    let written = DuckDBExporter::new(output_path).export_table(table, &data)?;
    tracing::info!(table, coin, rows = written, "duckdb_exported");
    Ok(written)
}

/// Export all market-data tables for the given coins + time range.
/// Returns a map of table → total rows written.
pub async fn export_all(
    ch: &Client,
    output_path: &str,
    coins: &[&str],
    start_ms: i64,
    end_ms: i64,
) -> Result<std::collections::HashMap<String, usize>, String> {
    let mut results = std::collections::HashMap::new();
    for table in EXPORT_TABLES {
        let mut total = 0usize;
        for coin in coins {
            total += export_table(ch, output_path, table, coin, start_ms, end_ms).await?;
        }
        results.insert(table.to_string(), total);
    }
    tracing::info!(file = output_path, totals = ?results, "duckdb_export_all_complete");
    Ok(results)
}

/// Query one table and collect every row as a Vec<Option<String>>.
///
/// `start_ms`/`end_ms` are Unix milliseconds; the `ts` column is
/// `DateTime64(3)` so the range predicate converts it to Int64 millis with
/// `toUnixTimestamp64Milli` (an Int64 literal compared against DateTime64 is
/// otherwise interpreted as seconds, which would silently return nothing).
async fn query_rows(
    ch: &Client,
    table: &str,
    coin: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<Vec<Option<String>>>, String> {
    let sql = format!(
        "SELECT ?fields FROM {table} WHERE coin = ? AND toUnixTimestamp64Milli(ts) BETWEEN ? AND ?"
    );
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    match table {
        "candles" => {
            let mut cursor = ch
                .query(&sql)
                .bind(coin)
                .bind(start_ms)
                .bind(end_ms)
                .fetch::<CandleChRow>()
                .map_err(|e| format!("clickhouse query {table}: {e}"))?;
            while let Some(r) = cursor
                .next()
                .await
                .map_err(|e| format!("clickhouse row: {e}"))?
            {
                assert_ts_sane(table, r.ts)?;
                rows.push(vec![
                    Some(r.ts.to_string()),
                    Some(r.coin),
                    Some(r.interval),
                    Some(r.open.to_string()),
                    Some(r.high.to_string()),
                    Some(r.low.to_string()),
                    Some(r.close.to_string()),
                    Some(r.volume.to_string()),
                ]);
            }
        }
        "funding" => {
            let mut cursor = ch
                .query(&sql)
                .bind(coin)
                .bind(start_ms)
                .bind(end_ms)
                .fetch::<FundingChRow>()
                .map_err(|e| format!("clickhouse query {table}: {e}"))?;
            while let Some(r) = cursor
                .next()
                .await
                .map_err(|e| format!("clickhouse row: {e}"))?
            {
                assert_ts_sane(table, r.ts)?;
                rows.push(vec![
                    Some(r.ts.to_string()),
                    Some(r.coin),
                    Some(r.funding_rate.to_string()),
                    Some(r.premium.to_string()),
                    Some(r.oi.to_string()),
                    Some(r.mark_px.to_string()),
                ]);
            }
        }
        "trades" => {
            let mut cursor = ch
                .query(&sql)
                .bind(coin)
                .bind(start_ms)
                .bind(end_ms)
                .fetch::<TradeChRow>()
                .map_err(|e| format!("clickhouse query {table}: {e}"))?;
            while let Some(r) = cursor
                .next()
                .await
                .map_err(|e| format!("clickhouse row: {e}"))?
            {
                assert_ts_sane(table, r.ts)?;
                rows.push(vec![
                    Some(r.ts.to_string()),
                    Some(r.coin),
                    Some(r.px.to_string()),
                    Some(r.sz.to_string()),
                    Some(r.side.to_string()),
                    Some(r.tid.to_string()),
                ]);
            }
        }
        "l2_book" => {
            let mut cursor = ch
                .query(&sql)
                .bind(coin)
                .bind(start_ms)
                .bind(end_ms)
                .fetch::<L2BookChRow>()
                .map_err(|e| format!("clickhouse query {table}: {e}"))?;
            while let Some(r) = cursor
                .next()
                .await
                .map_err(|e| format!("clickhouse row: {e}"))?
            {
                assert_ts_sane(table, r.ts)?;
                rows.push(vec![
                    Some(r.ts.to_string()),
                    Some(r.coin),
                    Some(r.side.to_string()),
                    Some(r.level.to_string()),
                    Some(r.px.to_string()),
                    Some(r.sz.to_string()),
                ]);
            }
        }
        "mid_prices" => {
            let mut cursor = ch
                .query(&sql)
                .bind(coin)
                .bind(start_ms)
                .bind(end_ms)
                .fetch::<MidPriceChRow>()
                .map_err(|e| format!("clickhouse query {table}: {e}"))?;
            while let Some(r) = cursor
                .next()
                .await
                .map_err(|e| format!("clickhouse row: {e}"))?
            {
                assert_ts_sane(table, r.ts)?;
                rows.push(vec![
                    Some(r.ts.to_string()),
                    Some(r.coin),
                    Some(r.px.to_string()),
                ]);
            }
        }
        other => return Err(format!("unsupported export table: {other}")),
    }
    Ok(rows)
}

/// C6: refuse to export rows whose `ts` is not plausible Unix millis. The
/// pre-fix `f64` decode produced values like `8.81e-312`; exporting them hid
/// the bug in the DuckDB files (the old test only checked file existence).
fn assert_ts_sane(table: &str, ts: i64) -> Result<(), String> {
    if ts > TS_MIN_MILLIS {
        return Ok(());
    }
    Err(format!(
        "duckdb export: {table} row has implausible ts={ts} (expected Unix millis > {TS_MIN_MILLIS}); refusing to export garbage"
    ))
}

/// The column names for each exported table (matches the CH table schemas).
fn table_columns(table: &str) -> Result<Vec<String>, String> {
    let cols = match table {
        "candles" => vec![
            "ts", "coin", "interval", "open", "high", "low", "close", "volume",
        ],
        "funding" => vec!["ts", "coin", "funding_rate", "premium", "oi", "mark_px"],
        "trades" => vec!["ts", "coin", "px", "sz", "side", "tid"],
        "l2_book" => vec!["ts", "coin", "side", "level", "px", "sz"],
        "mid_prices" => vec!["ts", "coin", "px"],
        other => return Err(format!("unsupported export table: {other}")),
    };
    Ok(cols.iter().map(|c| c.to_string()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_columns_match_schema() {
        assert_eq!(
            table_columns("candles").unwrap(),
            vec![
                "ts", "coin", "interval", "open", "high", "low", "close", "volume"
            ]
        );
        assert_eq!(
            table_columns("mid_prices").unwrap(),
            vec!["ts", "coin", "px"]
        );
        // C6: l2_book columns must match the CH schema (ts, coin, side, level,
        // px, sz) — the pre-fix `price`/`size` names made the export fail with
        // `Unknown column price`.
        assert_eq!(
            table_columns("l2_book").unwrap(),
            vec!["ts", "coin", "side", "level", "px", "sz"]
        );
        assert!(table_columns("nope").is_err());
    }

    #[test]
    fn export_tables_matches_python() {
        assert_eq!(
            EXPORT_TABLES,
            &["candles", "funding", "trades", "l2_book", "mid_prices"]
        );
    }

    #[test]
    fn export_writes_rows_to_duckdb() {
        let tmp = std::env::temp_dir().join(format!("hypeedge_duckdb_{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let exporter = DuckDBExporter::new(tmp.clone());
        let data = fetched_table(
            vec!["ts".into(), "coin".into(), "px".into()],
            vec![
                vec![
                    Some("1700000000".into()),
                    Some("BTC".into()),
                    Some("100.5".into()),
                ],
                vec![
                    Some("1700003600".into()),
                    Some("BTC".into()),
                    Some("101.0".into()),
                ],
            ],
        );
        let n = exporter.export_table("candles", &data).unwrap();
        assert_eq!(n, 2);

        // Reopen and verify.
        let conn = duckdb::Connection::open(&tmp).unwrap();
        let count: i64 = conn
            .query_row("SELECT count(*) FROM candles", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
        let px: String = conn
            .query_row("SELECT px FROM candles LIMIT 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(px, "100.5");
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn export_empty_is_noop() {
        let exporter = DuckDBExporter::new(std::env::temp_dir().join("hypeedge_empty.duckdb"));
        let data = fetched_table(vec![], vec![]);
        assert_eq!(exporter.export_table("candles", &data).unwrap(), 0);
    }
}
