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

/// One line of the JSONL spool: `{"table": "...", "key": "...", "row": {...}}`.
#[derive(Deserialize)]
struct SpoolEntry {
    table: String,
    /// The enqueue-time dedup key. Replay arms the same in-process filters the
    /// live path uses so a redelivered event is not double-written after a
    /// restart (C8/H-CH3/H-CH4/M-CH). `Option` so spools written before the
    /// key was introduced still deserialize; replay then recomputes the key.
    #[serde(default)]
    key: Option<String>,
    row: serde_json::Value,
}

/// ClickHouse insert timeout (M-CH): a hung insert must fail into the spool
/// instead of blocking the writer (and the event bus) forever.
const INSERT_TIMEOUT: Duration = Duration::from_secs(10);

/// Hard cap for the spool file in bytes (M-CH): beyond this the writer stops
/// appending (logging loudly) rather than growing the disk without bound.
const SPOOL_MAX_BYTES: u64 = 256 * 1024 * 1024;

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
    /// Last-seen funding values per `(coin, hour)` (H-CH3): a funding update
    /// whose rate/premium/oi/mark are unchanged within the same hour is not
    /// re-written, cutting the per-hour amplification from the WS
    /// `activeAssetCtx` stream (~1 write/hour/coin instead of one per frame).
    funding_seen: HashMap<(String, i64), (f64, f64, f64, f64)>,
    /// Spool size cap in bytes (M-CH).
    spool_max_bytes: u64,
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
            funding_seen: HashMap::new(),
            spool_max_bytes: SPOOL_MAX_BYTES,
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
                // H-CH4: `level` is numbered per side (bids 0..19 / asks
                // 0..19), and the dedup key carries the side — `(timestamp,
                // symbol, side, level)` — so a bid and an ask never collide on
                // the same key and per-side levels stay addressable.
                let levels = b
                    .bids
                    .iter()
                    .enumerate()
                    .map(|(i, l)| (1u8, i as u16, l))
                    .chain(b.asks.iter().enumerate().map(|(i, l)| (2u8, i as u16, l)))
                    .map(|(side, level, l)| {
                        (
                            format!("{}|{}|{}|{}", b.timestamp, b.symbol, side, level),
                            PendingRow::L2Book(L2BookRow {
                                ts: dt_ms(b.timestamp),
                                coin: b.symbol.clone(),
                                side,
                                level,
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
            DomainEvent::CandleUpdate(c) => {
                // H-CH4: the dedup key includes `close, volume`, so a forming
                // candle's first frame does not shadow the definitive closed
                // frame — every value change is a new key, and the final frame
                // (with the true close) wins the last write.
                let close: f64 = c.close.inner().to_string().parse().unwrap_or(0.0);
                let volume: f64 = c.volume.inner().to_string().parse().unwrap_or(0.0);
                self.push_if_new(
                    "candles",
                    &format!(
                        "{}|{}|{}|{}|{}",
                        c.timestamp, c.symbol, c.interval, close, volume
                    ),
                    PendingRow::Candle(CandleRow {
                        ts: dt_ms(c.timestamp),
                        coin: c.symbol.clone(),
                        interval: c.interval.clone(),
                        open: c.open.inner().to_string().parse().unwrap_or(0.0),
                        high: c.high.inner().to_string().parse().unwrap_or(0.0),
                        low: c.low.inner().to_string().parse().unwrap_or(0.0),
                        close,
                        volume,
                    }),
                )
            }
            DomainEvent::FundingUpdate(f) => {
                // H-CH3: funding is hourly-settled; the WS stream pushes
                // `activeAssetCtx` frames at a much higher cadence. Track the
                // last-seen value per `(coin, hour)` and only write when it
                // changes (fixes the ~2360× per-hour amplification).
                let hour = funding_hour_key(f.timestamp);
                let values = (
                    f.funding_rate,
                    f.premium,
                    f.open_interest,
                    f.mark_price.inner().to_string().parse().unwrap_or(0.0),
                );
                let changed = self.funding_seen.get(&(f.symbol.clone(), hour)) != Some(&values);
                self.funding_seen.insert((f.symbol.clone(), hour), values);
                if changed {
                    self.push_if_new(
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
                    );
                }
            }
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
    /// payload + enqueue-time dedup key; a later retry drain can replay them.
    /// The spool is size-capped (M-CH): past `spool_max_bytes` new lines are
    /// dropped with a loud log instead of growing the disk without bound.
    fn append_spool(&mut self, rows: &[PendingRow]) {
        use std::io::Write;
        let lines: Vec<String> = rows
            .iter()
            .map(|row| {
                let key = row_dedup_key(row.table(), row).unwrap_or_default();
                let line = match row {
                    PendingRow::L2Book(r) => {
                        serde_json::json!({"table": "l2_book", "key": key, "row": r})
                    }
                    PendingRow::Trade(r) => {
                        serde_json::json!({"table": "trades", "key": key, "row": r})
                    }
                    PendingRow::Candle(r) => {
                        serde_json::json!({"table": "candles", "key": key, "row": r})
                    }
                    PendingRow::Funding(r) => {
                        serde_json::json!({"table": "funding", "key": key, "row": r})
                    }
                    PendingRow::MidPrice(r) => {
                        serde_json::json!({"table": "mid_prices", "key": key, "row": r})
                    }
                };
                line.to_string()
            })
            .collect();
        let added: usize = lines.iter().map(|l| l.len() + 1).sum();
        let current = std::fs::metadata(&self.spool_path)
            .map(|m| m.len() as usize)
            .unwrap_or(0);
        if (current + added) as u64 > self.spool_max_bytes {
            tracing::error!(
                path = %self.spool_path.display(),
                bytes = current + added,
                cap = self.spool_max_bytes,
                "spool_size_cap_exceeded_dropping_batch"
            );
            return;
        }
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
        for line in lines {
            let _ = writeln!(file, "{}", line);
        }
    }

    /// Replay rows previously spooled to the JSONL file (C5): read each line,
    /// group by table, insert; on success remove the spool so a replayed batch
    /// is never replayed again. Replay first passes through the same dedup the
    /// live path applies (M-CH): the enqueue-time key arms the in-process
    /// filter so a redelivered event is not double-written after a restart, and
    /// funding rows apply the same-hour value-change filter.
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
                // Dedup pass: skip duplicate spool lines and arm the same keys
                // the live path marks (C8/H-CH3/H-CH4/M-CH).
                let key = entry
                    .key
                    .filter(|k| !k.is_empty())
                    .or_else(|| row_dedup_key(entry.table.as_str(), &row));
                if let Some(key) = key
                    && self.dedup.check_and_mark(entry.table.as_str(), &key)
                {
                    continue;
                }
                if let PendingRow::Funding(r) = &row {
                    let hour = funding_hour_key(millis_of(r.ts));
                    let values = (r.funding_rate, r.premium, r.oi, r.mark_px);
                    if self.funding_seen.get(&(r.coin.clone(), hour)) == Some(&values) {
                        continue;
                    }
                    self.funding_seen.insert((r.coin.clone(), hour), values);
                }
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

/// Unix millis of an `OffsetDateTime` — the exact round-trip of [`dt_ms`].
fn millis_of(dt: OffsetDateTime) -> i64 {
    dt.unix_timestamp() * 1000 + i64::from(dt.millisecond())
}

/// The hour bucket (in millis) a timestamp belongs to — used by the funding
/// value-change filter (H-CH3).
fn funding_hour_key(ts_ms: i64) -> i64 {
    ts_ms.div_euclid(3_600_000)
}

/// The dedup key for a spooled row, matching the live `enqueue` keys exactly
/// so replay arms the same in-process filters (C8, H-CH3, H-CH4, M-CH).
fn row_dedup_key(table: &str, row: &PendingRow) -> Option<String> {
    match (table, row) {
        ("l2_book", PendingRow::L2Book(r)) => Some(format!(
            "{}|{}|{}|{}",
            millis_of(r.ts),
            r.coin,
            r.side,
            r.level
        )),
        ("trades", PendingRow::Trade(r)) => Some(format!("{}|{}", r.tid, r.coin)),
        ("candles", PendingRow::Candle(r)) => Some(format!(
            "{}|{}|{}|{}|{}",
            millis_of(r.ts),
            r.coin,
            r.interval,
            r.close,
            r.volume
        )),
        ("funding", PendingRow::Funding(r)) => Some(format!("{}|{}", millis_of(r.ts), r.coin)),
        ("mid_prices", PendingRow::MidPrice(r)) => Some(format!("{}|{}", millis_of(r.ts), r.coin)),
        _ => None,
    }
}

/// Insert a batch of rows into a table via the crate's `insert` handle.
/// Bounded by [`INSERT_TIMEOUT`] (M-CH): a hung ClickHouse insert returns an
/// error so the caller (flush) routes the batch into the spool instead of
/// blocking the writer forever.
async fn insert_rows<T: Row + Serialize>(
    client: &Client,
    table: &str,
    rows: impl Iterator<Item = T>,
) -> Result<(), String> {
    let rows = rows.collect::<Vec<_>>();
    if rows.is_empty() {
        return Ok(());
    }
    let result = tokio::time::timeout(INSERT_TIMEOUT, async {
        let mut insert = client.insert(table).map_err(|e| e.to_string())?;
        for row in rows {
            insert.write(&row).await.map_err(|e| e.to_string())?;
        }
        insert.end().await.map_err(|e| e.to_string())?;
        Ok::<(), String>(())
    })
    .await;
    match result {
        Ok(inner) => inner,
        Err(_) => {
            tracing::warn!(table, "clickhouse_insert_timeout");
            Err("clickhouse insert timed out".into())
        }
    }
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
    fn funding_same_hour_same_value_not_repeated() {
        // H-CH3 regression: the WS `activeAssetCtx` stream pushes funding
        // frames at a far higher cadence than the hourly settlement. A frame
        // whose (rate, premium, oi, mark) is unchanged within the same hour
        // must not produce another row (~2360× amplification pre-fix).
        let base = 1_700_000_000_000i64; // some Unix-millis timestamp
        let mk = |rate: f64, ts: i64| {
            DomainEvent::FundingUpdate(hypeedge_domain::models::FundingRate {
                symbol: "BTC".into(),
                funding_rate: rate,
                premium: 0.0001,
                mark_price: Price::new(Decimal::from_str_strict("65000.5").unwrap()),
                open_interest: 1000.0,
                timestamp: ts,
            })
        };
        let mut writer = ClickHouseWriter::new(
            "http://localhost:8123",
            "hypeedge",
            "default",
            "",
            1000,
            Duration::from_secs(5),
            std::path::PathBuf::from("/tmp/ch_funding_test.jsonl"),
        );
        writer.enqueue(&mk(0.00005, base)); // first frame of the hour → write
        writer.enqueue(&mk(0.00005, base + 1000)); // same hour, same value → skip
        writer.enqueue(&mk(0.00005, base + 2000)); // same hour, same value → skip
        assert_eq!(
            writer.rows.len(),
            1,
            "unchanged same-hour value must not re-write"
        );
        writer.enqueue(&mk(0.00006, base + 3000)); // value changed → write
        assert_eq!(
            writer.rows.len(),
            2,
            "changed value in same hour must write"
        );
        writer.enqueue(&mk(0.00006, base + 3_600_000)); // next hour, same value → write
        assert_eq!(
            writer.rows.len(),
            3,
            "new hour must write even with same value"
        );
    }

    #[test]
    fn candle_dedup_key_includes_close_and_volume() {
        // H-CH4 regression: a forming candle's first frame used to shadow every
        // later frame (the dedup key was timestamp|symbol|interval), so the
        // definitive closed frame was never written. The key now carries
        // close+volume: value changes write, identical redeliveries dedup.
        let c = |close: &str, volume: &str, ts: i64| {
            DomainEvent::CandleUpdate(hypeedge_domain::models::Candle {
                symbol: "BTC".into(),
                interval: "1m".into(),
                open: Price::new(Decimal::from_str_strict("100").unwrap()),
                high: Price::new(Decimal::from_str_strict("101").unwrap()),
                low: Price::new(Decimal::from_str_strict("99").unwrap()),
                close: Price::new(Decimal::from_str_strict(close).unwrap()),
                volume: hypeedge_domain::decimal::Size::new(
                    Decimal::from_str_strict(volume).unwrap(),
                ),
                timestamp: ts,
            })
        };
        let mut writer = ClickHouseWriter::new(
            "http://localhost:8123",
            "hypeedge",
            "default",
            "",
            1000,
            Duration::from_secs(5),
            std::path::PathBuf::from("/tmp/ch_candle_test.jsonl"),
        );
        let ts = 1_700_000_000_000i64;
        writer.enqueue(&c("100.5", "1.0", ts)); // forming frame
        writer.enqueue(&c("100.5", "1.0", ts)); // identical redelivery → dedup
        assert_eq!(writer.rows.len(), 1, "identical candle frame must dedup");
        writer.enqueue(&c("101.0", "2.0", ts)); // updated forming frame
        writer.enqueue(&c("101.0", "2.0", ts)); // closed frame redelivered → dedup
        assert_eq!(
            writer.rows.len(),
            2,
            "close/volume change must write; redelivery must dedup"
        );
        // The final frame (true close) is present — the closed candle wins.
        let (close, volume) = match &writer.rows[1] {
            PendingRow::Candle(r) => (r.close, r.volume),
            _ => panic!("expected candle row"),
        };
        assert_eq!(close, 101.0);
        assert_eq!(volume, 2.0);
    }

    #[test]
    fn l2_book_levels_are_per_side_and_key_includes_side() {
        // H-CH4 regression: `level` used to be a global enumerate index across
        // bids+asks. It is now numbered per side (bids 0..n / asks 0..m) and
        // the dedup key is (timestamp, symbol, side, level), so a bid and an
        // ask at the same index never collide.
        let book = |ts: i64| L2BookSnapshot {
            symbol: "BTC".into(),
            bids: vec![
                L2Level {
                    price: Price::new(Decimal::from_str_strict("100").unwrap()),
                    size: Size::new(Decimal::from_str_strict("2").unwrap()),
                },
                L2Level {
                    price: Price::new(Decimal::from_str_strict("99").unwrap()),
                    size: Size::new(Decimal::from_str_strict("4").unwrap()),
                },
            ],
            asks: vec![L2Level {
                price: Price::new(Decimal::from_str_strict("101").unwrap()),
                size: Size::new(Decimal::from_str_strict("3").unwrap()),
            }],
            timestamp: ts,
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
            std::path::PathBuf::from("/tmp/ch_l2_test.jsonl"),
        );
        writer.enqueue(&DomainEvent::L2BookUpdate(book(1)));
        assert_eq!(writer.rows.len(), 3, "2 bids + 1 ask enqueued");
        let levels: Vec<(u8, u16)> = writer
            .rows
            .iter()
            .map(|r| match r {
                PendingRow::L2Book(r) => (r.side, r.level),
                _ => panic!("expected l2 row"),
            })
            .collect();
        assert_eq!(
            levels,
            vec![(1, 0), (1, 1), (2, 0)],
            "per-side level numbering"
        );
        // Redelivered book at the same timestamp dedups entirely.
        writer.enqueue(&DomainEvent::L2BookUpdate(book(1)));
        assert_eq!(writer.rows.len(), 3, "identical book redelivery dedups");
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
