//! DuckDB export utility for local research, port of
//! `src/hypeedge/storage/duckdb_export.py`.
//!
//! Exports market data to a local DuckDB file for offline analysis. Not a
//! background task — run on demand via CLI or script. The ClickHouse query is
//! delegated to the caller (who holds the `clickhouse` client); this module
//! owns the DuckDB write.

use std::path::Path;

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

        let col_list: Vec<String> = data.columns.iter().map(|c| format!("\"{c}\" VARCHAR")).collect();
        let ddl = format!("CREATE TABLE IF NOT EXISTS {table} ({})", col_list.join(", "));
        conn.execute(&ddl, []).map_err(|e| format!("create table: {e}"))?;

        let placeholders = vec!["?".to_string(); data.columns.len()].join(", ");
        let insert_sql = format!("INSERT INTO {table} VALUES ({placeholders})");

        let mut written = 0usize;
        for row in &data.rows {
            let params = row.iter().map(|v| duckdb::types::Value::Text(v.clone().unwrap_or_default())).collect::<Vec<_>>();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_writes_rows_to_duckdb() {
        let tmp = std::env::temp_dir().join(format!("hypeedge_duckdb_{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let exporter = DuckDBExporter::new(tmp.clone());
        let data = fetched_table(
            vec!["ts".into(), "coin".into(), "px".into()],
            vec![
                vec![Some("1700000000".into()), Some("BTC".into()), Some("100.5".into())],
                vec![Some("1700003600".into()), Some("BTC".into()), Some("101.0".into())],
            ],
        );
        let n = exporter.export_table("candles", &data).unwrap();
        assert_eq!(n, 2);

        // Reopen and verify.
        let conn = duckdb::Connection::open(&tmp).unwrap();
        let count: i64 = conn.query_row("SELECT count(*) FROM candles", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 2);
        let px: String = conn.query_row("SELECT px FROM candles LIMIT 1", [], |r| r.get(0)).unwrap();
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
