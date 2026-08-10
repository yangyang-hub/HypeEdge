//! ClickHouse → DuckDB export wiring, port of `src/hypeedge/storage/duckdb_export.py`.
//!
//! The write side ([`DuckDBExporter`] + [`FetchedTable`]) lives in `trading`
//! (DB-free); this module owns the ClickHouse query that feeds it: fetch a
//! market-data table for a coin + time range, collect the rows as strings, and
//! write them into a local DuckDB file for offline analysis. Runs on demand
//! (CLI/script), not as a background task.

use clickhouse::{Client, Row};
use hypeedge_trading::backtest::duckdb_export::{DuckDBExporter, EXPORT_TABLES, fetched_table};
use serde::{Deserialize, Serialize};

/// Row types for each exported table. `?fields` expands the columns in
/// declaration order, matching the DuckDB column list.
#[derive(Debug, Row, Serialize, Deserialize, Clone)]
struct CandleChRow {
    ts: f64,
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
    ts: f64,
    coin: String,
    funding_rate: f64,
    premium: f64,
    oi: f64,
    mark_px: f64,
}

#[derive(Debug, Row, Serialize, Deserialize, Clone)]
struct TradeChRow {
    ts: f64,
    coin: String,
    px: f64,
    sz: f64,
    side: u8,
    tid: u64,
}

#[derive(Debug, Row, Serialize, Deserialize, Clone)]
struct L2BookChRow {
    ts: f64,
    coin: String,
    side: u8,
    level: u16,
    px: f64,
    sz: f64,
}

#[derive(Debug, Row, Serialize, Deserialize, Clone)]
struct MidPriceChRow {
    ts: f64,
    coin: String,
    px: f64,
}

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
    let start_sec = start_ms as f64 / 1000.0;
    let end_sec = end_ms as f64 / 1000.0;
    let rows: Vec<Vec<Option<String>>> = query_rows(ch, table, coin, start_sec, end_sec).await?;
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
async fn query_rows(
    ch: &Client,
    table: &str,
    coin: &str,
    start_sec: f64,
    end_sec: f64,
) -> Result<Vec<Vec<Option<String>>>, String> {
    let sql = format!(
        "SELECT ?fields FROM {table} WHERE coin = ? AND ts BETWEEN ? AND ?"
    );
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    match table {
        "candles" => {
            let mut cursor = ch
                .query(&sql)
                .bind(coin)
                .bind(start_sec)
                .bind(end_sec)
                .fetch::<CandleChRow>()
                .map_err(|e| format!("clickhouse query {table}: {e}"))?;
            while let Some(r) = cursor.next().await.map_err(|e| format!("clickhouse row: {e}"))? {
                rows.push(vec![
                    Some(r.ts.to_string()), Some(r.coin), Some(r.interval),
                    Some(r.open.to_string()), Some(r.high.to_string()), Some(r.low.to_string()),
                    Some(r.close.to_string()), Some(r.volume.to_string()),
                ]);
            }
        }
        "funding" => {
            let mut cursor = ch
                .query(&sql)
                .bind(coin)
                .bind(start_sec)
                .bind(end_sec)
                .fetch::<FundingChRow>()
                .map_err(|e| format!("clickhouse query {table}: {e}"))?;
            while let Some(r) = cursor.next().await.map_err(|e| format!("clickhouse row: {e}"))? {
                rows.push(vec![
                    Some(r.ts.to_string()), Some(r.coin),
                    Some(r.funding_rate.to_string()), Some(r.premium.to_string()),
                    Some(r.oi.to_string()), Some(r.mark_px.to_string()),
                ]);
            }
        }
        "trades" => {
            let mut cursor = ch
                .query(&sql)
                .bind(coin)
                .bind(start_sec)
                .bind(end_sec)
                .fetch::<TradeChRow>()
                .map_err(|e| format!("clickhouse query {table}: {e}"))?;
            while let Some(r) = cursor.next().await.map_err(|e| format!("clickhouse row: {e}"))? {
                rows.push(vec![
                    Some(r.ts.to_string()), Some(r.coin),
                    Some(r.px.to_string()), Some(r.sz.to_string()),
                    Some(r.side.to_string()), Some(r.tid.to_string()),
                ]);
            }
        }
        "l2_book" => {
            let mut cursor = ch
                .query(&sql)
                .bind(coin)
                .bind(start_sec)
                .bind(end_sec)
                .fetch::<L2BookChRow>()
                .map_err(|e| format!("clickhouse query {table}: {e}"))?;
            while let Some(r) = cursor.next().await.map_err(|e| format!("clickhouse row: {e}"))? {
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
                .bind(start_sec)
                .bind(end_sec)
                .fetch::<MidPriceChRow>()
                .map_err(|e| format!("clickhouse query {table}: {e}"))?;
            while let Some(r) = cursor.next().await.map_err(|e| format!("clickhouse row: {e}"))? {
                rows.push(vec![Some(r.ts.to_string()), Some(r.coin), Some(r.px.to_string())]);
            }
        }
        other => return Err(format!("unsupported export table: {other}")),
    }
    Ok(rows)
}

/// The column names for each exported table (matches the CH table schemas).
fn table_columns(table: &str) -> Result<Vec<String>, String> {
    let cols = match table {
        "candles" => vec!["ts", "coin", "interval", "open", "high", "low", "close", "volume"],
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
            vec!["ts", "coin", "interval", "open", "high", "low", "close", "volume"]
        );
        assert_eq!(table_columns("mid_prices").unwrap(), vec!["ts", "coin", "px"]);
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
        assert_eq!(EXPORT_TABLES, &["candles", "funding", "trades", "l2_book", "mid_prices"]);
    }
}
