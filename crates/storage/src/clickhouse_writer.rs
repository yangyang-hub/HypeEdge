//! ClickHouse writer, port of `ClickHouseWriter` in
//! `src/hypeedge/storage/clickhouse.py`.
//!
//! Subscribes to market-data events on the event bus, accumulates rows in
//! memory, and flushes when `batch_size` is reached or `flush_interval`
//! elapses. On a flush failure the batch is appended to a spool file and
//! retried on a later tick, mirroring the Python SQLite spool (this Rust
//! version uses an append-only JSONL spool; the schema is described in
//! `docs/design.md` §5.2).
//!
//! The five core market-data tables are written here. The five `mm_*`
//! analytics tables are created by the DDL but populated once the
//! market-making runtime lands (Phase 5).

use std::collections::HashMap;
use std::sync::Arc;

use clickhouse::{Client, Row};
use hypeedge_domain::events::{DomainEvent, Event, EventType};
use hypeedge_infra::event_bus::{BoundedMailbox, EventBus};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::time::{Duration, Instant};

use crate::dedup::DedupFilter;

/// A row for the `l2_book` table.
#[derive(Row, Serialize, Deserialize, Clone)]
struct L2BookRow {
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    ts: OffsetDateTime,
    coin: String,
    side: u8,
    level: u16,
    px: f64,
    sz: f64,
}

/// A row for the `trades` table.
#[derive(Row, Serialize, Deserialize, Clone)]
struct TradeRow {
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    ts: OffsetDateTime,
    coin: String,
    px: f64,
    sz: f64,
    side: u8,
    tid: u64,
}

/// A row for the `candles` table.
#[derive(Row, Serialize, Deserialize, Clone)]
struct CandleRow {
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    ts: OffsetDateTime,
    coin: String,
    interval: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

/// A row for the `funding` table.
#[derive(Row, Serialize, Deserialize, Clone)]
struct FundingRow {
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    ts: OffsetDateTime,
    coin: String,
    funding_rate: f64,
    premium: f64,
    oi: f64,
    mark_px: f64,
}

/// A row for the `mid_prices` table.
#[derive(Row, Serialize, Deserialize, Clone)]
struct MidPriceRow {
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    ts: OffsetDateTime,
    coin: String,
    px: f64,
}

/// A generic row that carries a ready-to-serialize JSON payload. We serialize
/// the domain event to a compact value string and store per-table rows
/// separately; this enum keeps the five core tables' row types unified.
#[derive(Clone)]
enum PendingRow {
    L2Book(L2BookRow),
    Trade(TradeRow),
    Candle(CandleRow),
    Funding(FundingRow),
    MidPrice(MidPriceRow),
}

impl PendingRow {
    /// The ClickHouse table this row belongs to (for per-table flush/spool).
    fn table(&self) -> &'static str {
        match self {
            PendingRow::L2Book(_) => "l2_book",
            PendingRow::Trade(_) => "trades",
            PendingRow::Candle(_) => "candles",
            PendingRow::Funding(_) => "funding",
            PendingRow::MidPrice(_) => "mid_prices",
        }
    }
}

/// One line of the JSONL spool: `{"table": "...", "row": {...}}`.
#[derive(Deserialize)]
struct SpoolEntry {
    table: String,
    row: serde_json::Value,
}

/// The ClickHouse writer task.
pub struct ClickHouseWriter {
    client: Client,
    batch_size: usize,
    flush_interval: Duration,
    spool_path: std::path::PathBuf,
    rows: Vec<PendingRow>,
    last_flush: Instant,
    flush_count: u64,
    row_count: u64,
    spooled_count: u64,
    /// In-process dedup of redelivered market-data events (C8).
    dedup: DedupFilter,
}

/// How a drain cycle ended (A12): `Flush` means flush-and-continue; `Closed`
/// means the mailbox closed and the writer must flush once and exit.
#[derive(Debug, PartialEq, Eq)]
enum DrainResult {
    Flush,
    Closed,
}

impl ClickHouseWriter {
    pub fn new(
        url: &str,
        database: &str,
        user: &str,
        password: &str,
        batch_size: usize,
        flush_interval: Duration,
        spool_path: std::path::PathBuf,
    ) -> Self {
        let client = Client::default()
            .with_url(url)
            .with_database(database)
            .with_user(user)
            .with_password(password);
        Self {
            client,
            batch_size,
            flush_interval,
            spool_path,
            rows: Vec::new(),
            last_flush: Instant::now(),
            flush_count: 0,
            row_count: 0,
            spooled_count: 0,
            dedup: DedupFilter::new(100_000),
        }
    }

    /// Run the writer task: subscribe to market-data events, batch them, and
    /// flush on size or interval. Replays any previously-spooled rows on
    /// startup, then exits cleanly when the mailbox closes.
    pub async fn run(&mut self, bus: &Arc<EventBus>) -> Result<(), String> {
        let mailbox = bus.subscribe_many(&[
            EventType::L2BookUpdate,
            EventType::TradeUpdate,
            EventType::CandleUpdate,
            EventType::FundingUpdate,
            EventType::MidPriceUpdate,
        ]);
        self.apply_ddl()
            .await
            .map_err(|e| format!("clickhouse ddl: {e}"))?;
        self.replay_spool()
            .await
            .map_err(|e| format!("clickhouse spool replay: {e}"))?;

        loop {
            match self
                .drain_until_deadline(&mailbox, self.flush_interval)
                .await
            {
                DrainResult::Flush => {
                    self.flush().await?;
                    self.last_flush = Instant::now();
                }
                DrainResult::Closed => {
                    // A12: a closed mailbox must not busy-loop; flush once and exit.
                    self.flush().await?;
                    return Ok(());
                }
            }
        }
    }

    /// Receive events until the flush interval elapses (flush), the batch fills
    /// (flush), or the mailbox closes (final flush + exit).
    async fn drain_until_deadline(
        &mut self,
        mailbox: &BoundedMailbox<Arc<Event>>,
        interval: Duration,
    ) -> DrainResult {
        let deadline = Instant::now() + interval;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return DrainResult::Flush; // interval elapsed
            }
            match tokio::time::timeout(remaining, mailbox.recv()).await {
                Ok(Some(event)) => {
                    self.enqueue(&event.payload);
                    if self.rows.len() >= self.batch_size {
                        return DrainResult::Flush; // batch full
                    }
                }
                Ok(None) => return DrainResult::Closed,
                Err(_) => return DrainResult::Flush, // timeout
            }
        }
    }

    fn enqueue(&mut self, payload: &DomainEvent) {
        match payload {
            DomainEvent::L2BookUpdate(b) => {
                let levels = b
                    .bids
                    .iter()
                    .map(|l| (1u8, l))
                    .chain(b.asks.iter().map(|l| (2u8, l)))
                    .enumerate()
                    .map(|(i, (side, l))| {
                        (
                            format!("{}|{}|{}", b.timestamp, b.symbol, i),
                            PendingRow::L2Book(L2BookRow {
                                ts: dt_ms(b.timestamp),
                                coin: b.symbol.clone(),
                                side,
                                level: i as u16,
                                px: l.price.inner().to_string().parse().unwrap_or(0.0),
                                sz: l.size.inner().to_string().parse().unwrap_or(0.0),
                            }),
                        )
                    })
                    .collect::<Vec<_>>();
                for (key, row) in levels {
                    self.push_if_new("l2_book", &key, row);
                }
            }
            DomainEvent::TradeUpdate(t) => self.push_if_new(
                "trades",
                &format!("{}|{}", t.tid, t.symbol),
                PendingRow::Trade(TradeRow {
                    ts: dt_ms(t.timestamp),
                    coin: t.symbol.clone(),
                    px: t.price.inner().to_string().parse().unwrap_or(0.0),
                    sz: t.size.inner().to_string().parse().unwrap_or(0.0),
                    side: match t.side {
                        hypeedge_domain::enums::Side::Buy => 1,
                        hypeedge_domain::enums::Side::Sell => 2,
                    },
                    tid: t.tid,
                }),
            ),
            DomainEvent::CandleUpdate(c) => self.push_if_new(
                "candles",
                &format!("{}|{}|{}", c.timestamp, c.symbol, c.interval),
                PendingRow::Candle(CandleRow {
                    ts: dt_ms(c.timestamp),
                    coin: c.symbol.clone(),
                    interval: c.interval.clone(),
                    open: c.open.inner().to_string().parse().unwrap_or(0.0),
                    high: c.high.inner().to_string().parse().unwrap_or(0.0),
                    low: c.low.inner().to_string().parse().unwrap_or(0.0),
                    close: c.close.inner().to_string().parse().unwrap_or(0.0),
                    volume: c.volume.inner().to_string().parse().unwrap_or(0.0),
                }),
            ),
            DomainEvent::FundingUpdate(f) => self.push_if_new(
                "funding",
                &format!("{}|{}", f.timestamp, f.symbol),
                PendingRow::Funding(FundingRow {
                    ts: dt_ms(f.timestamp),
                    coin: f.symbol.clone(),
                    funding_rate: f.funding_rate,
                    premium: f.premium,
                    oi: f.open_interest,
                    mark_px: f.mark_price.inner().to_string().parse().unwrap_or(0.0),
                }),
            ),
            DomainEvent::MidPriceUpdate(m) => self.push_if_new(
                "mid_prices",
                &format!("{}|{}", m.timestamp, m.symbol),
                PendingRow::MidPrice(MidPriceRow {
                    ts: dt_ms(m.timestamp),
                    coin: m.symbol.clone(),
                    px: m.price.to_string().parse().unwrap_or(0.0),
                }),
            ),
            _ => {}
        }
    }

    /// Push a row unless its natural key was already seen (C8: dedups
    /// redelivered market-data events within the process run).
    fn push_if_new(&mut self, table: &str, key: &str, row: PendingRow) {
        if self.dedup.check_and_mark(table, key) {
            return;
        }
        self.rows.push(row);
        self.row_count += 1;
    }

    /// Flush the buffered rows per table. Only the tables that fail are spooled
    /// (C5): spooling the whole batch used to lose the rows for tables that had
    /// already been written and created a duplicate-insert hazard on replay.
    async fn flush(&mut self) -> Result<(), String> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        let mut failed: Vec<PendingRow> = Vec::new();
        for table in ["l2_book", "trades", "candles", "funding", "mid_prices"] {
            let table_rows: Vec<PendingRow> = rows
                .iter()
                .filter(|r| r.table() == table)
                .cloned()
                .collect();
            if table_rows.is_empty() {
                continue;
            }
            if let Err(e) = insert_pending(&self.client, table, &table_rows).await {
                tracing::warn!(table, error = %e, "clickhouse_flush_error");
                failed.extend(table_rows);
            }
        }
        if !failed.is_empty() {
            // Only the failed tables' rows are spooled (C5).
            self.spooled_count += failed.len() as u64;
            self.append_spool(&failed);
        }
        self.flush_count += 1;
        Ok(())
    }

    /// Append the failed batch to the spool file (JSONL). One line per table +
    /// payload; a later retry drain can replay them.
    fn append_spool(&mut self, rows: &[PendingRow]) {
        use std::io::Write;
        let mut file = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.spool_path)
        {
            Ok(f) => f,
            Err(e) => {
                tracing::error!(path = %self.spool_path.display(), error = %e, "spool_open_failed");
                return;
            }
        };
        for row in rows {
            let line = match row {
                PendingRow::L2Book(r) => serde_json::json!({"table": "l2_book", "row": r}),
                PendingRow::Trade(r) => serde_json::json!({"table": "trades", "row": r}),
                PendingRow::Candle(r) => serde_json::json!({"table": "candles", "row": r}),
                PendingRow::Funding(r) => serde_json::json!({"table": "funding", "row": r}),
                PendingRow::MidPrice(r) => serde_json::json!({"table": "mid_prices", "row": r}),
            };
            let _ = writeln!(file, "{}", line);
        }
    }

    /// Replay rows previously spooled to the JSONL file (C5): read each line,
    /// group by table, insert; on success remove the spool so a replayed batch
    /// is never replayed again (dedup is also enforced by the in-memory filter).
    async fn replay_spool(&mut self) -> Result<(), String> {
        let content = match std::fs::read_to_string(&self.spool_path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(format!("spool read: {e}")),
        };
        if content.trim().is_empty() {
            return Ok(());
        }
        let mut batches: HashMap<String, Vec<PendingRow>> = HashMap::new();
        for line in content.lines() {
            let entry: SpoolEntry = match serde_json::from_str(line) {
                Ok(e) => e,
                Err(_) => continue, // tolerate a torn tail line
            };
            let row = match entry.table.as_str() {
                "l2_book" => serde_json::from_value::<L2BookRow>(entry.row)
                    .ok()
                    .map(PendingRow::L2Book),
                "trades" => serde_json::from_value::<TradeRow>(entry.row)
                    .ok()
                    .map(PendingRow::Trade),
                "candles" => serde_json::from_value::<CandleRow>(entry.row)
                    .ok()
                    .map(PendingRow::Candle),
                "funding" => serde_json::from_value::<FundingRow>(entry.row)
                    .ok()
                    .map(PendingRow::Funding),
                "mid_prices" => serde_json::from_value::<MidPriceRow>(entry.row)
                    .ok()
                    .map(PendingRow::MidPrice),
                _ => None,
            };
            if let Some(row) = row {
                batches.entry(entry.table).or_default().push(row);
            }
        }
        let mut all_ok = true;
        for (table, rows) in &batches {
            if let Err(e) = insert_pending(&self.client, table, rows).await {
                tracing::warn!(table, error = %e, "spool_replay_failed");
                all_ok = false;
            }
        }
        if all_ok {
            std::fs::remove_file(&self.spool_path).map_err(|e| format!("spool cleanup: {e}"))?;
            tracing::info!(path = %self.spool_path.display(), "spool_replayed");
        }
        Ok(())
    }

    /// Apply the ClickHouse DDL (idempotent).
    async fn apply_ddl(&self) -> Result<(), String> {
        let ddl = include_str!("./ch/ddl.sql");
        // Execute each statement; the DDL file uses `;` terminators.
        for stmt in ddl.split(';').filter(|s| !s.trim().is_empty()) {
            self.client
                .query(stmt)
                .execute()
                .await
                .map_err(|e| format!("ddl: {e}"))?;
        }
        Ok(())
    }

    pub fn stats(&self) -> (u64, u64, u64) {
        (self.flush_count, self.row_count, self.spooled_count)
    }
}

/// Convert Unix millis to `time::OffsetDateTime` (UTC).
fn dt_ms(millis: i64) -> OffsetDateTime {
    let secs = millis.div_euclid(1000);
    let sub_ms = millis.rem_euclid(1000);
    OffsetDateTime::from_unix_timestamp(secs).unwrap_or(OffsetDateTime::UNIX_EPOCH)
        + time::Duration::milliseconds(sub_ms)
}

/// Insert a batch of rows into a table via the crate's `insert` handle.
async fn insert_rows<T: Row + Serialize>(
    client: &Client,
    table: &str,
    rows: impl Iterator<Item = T>,
) -> Result<(), String> {
    let rows = rows.collect::<Vec<_>>();
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = client.insert(table).map_err(|e| e.to_string())?;
    for row in rows {
        insert.write(&row).await.map_err(|e| e.to_string())?;
    }
    insert.end().await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Dispatch a `PendingRow` batch to the matching table's `insert_rows`.
async fn insert_pending(client: &Client, table: &str, rows: &[PendingRow]) -> Result<(), String> {
    match table {
        "l2_book" => {
            insert_rows(
                client,
                table,
                rows.iter().filter_map(|r| match r {
                    PendingRow::L2Book(r) => Some(r.clone()),
                    _ => None,
                }),
            )
            .await
        }
        "trades" => {
            insert_rows(
                client,
                table,
                rows.iter().filter_map(|r| match r {
                    PendingRow::Trade(r) => Some(r.clone()),
                    _ => None,
                }),
            )
            .await
        }
        "candles" => {
            insert_rows(
                client,
                table,
                rows.iter().filter_map(|r| match r {
                    PendingRow::Candle(r) => Some(r.clone()),
                    _ => None,
                }),
            )
            .await
        }
        "funding" => {
            insert_rows(
                client,
                table,
                rows.iter().filter_map(|r| match r {
                    PendingRow::Funding(r) => Some(r.clone()),
                    _ => None,
                }),
            )
            .await
        }
        "mid_prices" => {
            insert_rows(
                client,
                table,
                rows.iter().filter_map(|r| match r {
                    PendingRow::MidPrice(r) => Some(r.clone()),
                    _ => None,
                }),
            )
            .await
        }
        other => Err(format!("unknown table {other}")),
    }
}

/// Convenience: a small self-test that the row construction from a domain event
/// produces the expected field values.
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use hypeedge_domain::decimal::{Decimal, Price, Size};
    use hypeedge_domain::enums::Side;
    use hypeedge_domain::models::{L2BookSnapshot, L2Level, Trade};

    fn millis(ms: i64) -> OffsetDateTime {
        dt_ms(ms)
    }

    #[test]
    fn trade_row_maps_fields() {
        let t = Trade {
            symbol: "BTC".into(),
            price: Price::new(Decimal::from_str_strict("65000.5").unwrap()),
            size: Size::new(Decimal::from_str_strict("0.5").unwrap()),
            side: Side::Buy,
            tid: 42,
            timestamp: 1_700_000_000_123,
            local_ts: Utc::now(),
        };
        let row = PendingRow::Trade(TradeRow {
            ts: millis(t.timestamp),
            coin: t.symbol.clone(),
            px: 65000.5,
            sz: 0.5,
            side: 1,
            tid: t.tid,
        });
        match row {
            PendingRow::Trade(r) => {
                assert_eq!(r.coin, "BTC");
                assert_eq!(r.tid, 42);
                assert_eq!(r.side, 1);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn l2_book_expands_to_levels() {
        let b = L2BookSnapshot {
            symbol: "BTC".into(),
            bids: vec![L2Level {
                price: Price::new(Decimal::from_str_strict("100").unwrap()),
                size: Size::new(Decimal::from_str_strict("2").unwrap()),
            }],
            asks: vec![L2Level {
                price: Price::new(Decimal::from_str_strict("101").unwrap()),
                size: Size::new(Decimal::from_str_strict("3").unwrap()),
            }],
            timestamp: 1,
            local_ts: Utc::now(),
            version: 0,
            connection_generation: 0,
        };
        let mut writer = ClickHouseWriter::new(
            "http://localhost:8123",
            "hypeedge",
            "default",
            "",
            1000,
            Duration::from_secs(5),
            std::path::PathBuf::from("/tmp/ch_spool_test.jsonl"),
        );
        writer.enqueue(&DomainEvent::L2BookUpdate(b));
        assert_eq!(writer.rows.len(), 2, "bid + ask levels enqueued");
        assert_eq!(writer.row_count, 2);
    }

    #[test]
    fn enqueue_dedups_redelivered_trade() {
        // C8 regression: the same trade tid redelivered on the bus must not
        // produce a second row.
        let t = Trade {
            symbol: "BTC".into(),
            price: Price::new(Decimal::from_str_strict("65000.5").unwrap()),
            size: Size::new(Decimal::from_str_strict("0.5").unwrap()),
            side: Side::Buy,
            tid: 42,
            timestamp: 1_700_000_000_123,
            local_ts: Utc::now(),
        };
        let mut writer = ClickHouseWriter::new(
            "http://localhost:8123",
            "hypeedge",
            "default",
            "",
            1000,
            Duration::from_secs(5),
            std::path::PathBuf::from("/tmp/ch_spool_test2.jsonl"),
        );
        writer.enqueue(&DomainEvent::TradeUpdate(t.clone()));
        writer.enqueue(&DomainEvent::TradeUpdate(t));
        assert_eq!(writer.rows.len(), 1, "duplicate tid deduped");
        assert_eq!(writer.row_count, 1);
    }

    #[tokio::test]
    async fn drain_closed_mailbox_returns_closed() {
        // A12 regression: a closed mailbox must signal the loop to exit, not
        // busy-loop flushing forever.
        let bus = EventBus::new(16);
        let mailbox = bus.subscribe(EventType::CandleUpdate);
        let mut writer = ClickHouseWriter::new(
            "http://localhost:8123",
            "hypeedge",
            "default",
            "",
            1000,
            Duration::from_secs(5),
            std::path::PathBuf::from("/tmp/ch_spool_test3.jsonl"),
        );
        mailbox.close();
        let result = writer
            .drain_until_deadline(&mailbox, Duration::from_secs(5))
            .await;
        assert!(
            matches!(result, DrainResult::Closed),
            "closed mailbox must yield Closed, got {result:?}"
        );
    }

    #[test]
    fn pending_row_table_names_cover_all_tables() {
        // C5 helper: the per-table dispatch in flush/spool/replay is exhaustive.
        let l2 = PendingRow::L2Book(L2BookRow {
            ts: millis(0),
            coin: "BTC".into(),
            side: 1,
            level: 0,
            px: 100.0,
            sz: 1.0,
        });
        assert_eq!(l2.table(), "l2_book");
        assert_eq!(
            PendingRow::Trade(TradeRow {
                ts: millis(0),
                coin: "BTC".into(),
                px: 1.0,
                sz: 1.0,
                side: 1,
                tid: 1,
            })
            .table(),
            "trades"
        );
        assert_eq!(
            PendingRow::Candle(CandleRow {
                ts: millis(0),
                coin: "BTC".into(),
                interval: "1m".into(),
                open: 1.0,
                high: 1.0,
                low: 1.0,
                close: 1.0,
                volume: 1.0,
            })
            .table(),
            "candles"
        );
        assert_eq!(
            PendingRow::Funding(FundingRow {
                ts: millis(0),
                coin: "BTC".into(),
                funding_rate: 0.0,
                premium: 0.0,
                oi: 0.0,
                mark_px: 1.0,
            })
            .table(),
            "funding"
        );
        assert_eq!(
            PendingRow::MidPrice(MidPriceRow {
                ts: millis(0),
                coin: "BTC".into(),
                px: 1.0,
            })
            .table(),
            "mid_prices"
        );
    }
}
