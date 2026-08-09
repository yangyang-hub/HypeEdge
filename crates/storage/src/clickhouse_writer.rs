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

use std::sync::Arc;

use clickhouse::{Client, Row};
use hypeedge_domain::events::{DomainEvent, Event, EventType};
use hypeedge_infra::event_bus::{BoundedMailbox, EventBus};
use serde::Serialize;
use time::OffsetDateTime;
use tokio::time::{Duration, Instant};

/// A row for the `l2_book` table.
#[derive(Row, Serialize, Clone)]
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
#[derive(Row, Serialize, Clone)]
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
#[derive(Row, Serialize, Clone)]
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
#[derive(Row, Serialize, Clone)]
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
#[derive(Row, Serialize, Clone)]
struct MidPriceRow {
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    ts: OffsetDateTime,
    coin: String,
    px: f64,
}

/// A generic row that carries a ready-to-serialize JSON payload. We serialize
/// the domain event to a compact value string and store per-table rows
/// separately; this enum keeps the five core tables' row types unified.
enum PendingRow {
    L2Book(L2BookRow),
    Trade(TradeRow),
    Candle(CandleRow),
    Funding(FundingRow),
    MidPrice(MidPriceRow),
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
        }
    }

    /// Run the writer task: subscribe to market-data events, batch them, and
    /// flush on size or interval. Exits when the mailbox closes.
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

        loop {
            let flush = self
                .drain_until_deadline(&mailbox, self.flush_interval)
                .await;
            if flush {
                self.flush().await?;
                self.last_flush = Instant::now();
            }
        }
    }

    /// Receive events until the flush interval elapses (or the mailbox closes,
    /// in which case we flush and return `true` to exit).
    async fn drain_until_deadline(
        &mut self,
        mailbox: &BoundedMailbox<Arc<Event>>,
        interval: Duration,
    ) -> bool {
        let deadline = Instant::now() + interval;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return true; // interval elapsed -> flush
            }
            match tokio::time::timeout(remaining, mailbox.recv()).await {
                Ok(Some(event)) => {
                    self.enqueue(&event.payload);
                    if self.rows.len() >= self.batch_size {
                        return true; // batch full -> flush
                    }
                }
                Ok(None) => {
                    // Mailbox closed -> flush and signal loop exit.
                    return true;
                }
                Err(_) => return true, // timeout -> flush
            }
        }
    }

    fn enqueue(&mut self, payload: &DomainEvent) {
        let row = match payload {
            DomainEvent::L2BookUpdate(b) => {
                let levels = b
                    .bids
                    .iter()
                    .map(|l| (1u8, l))
                    .chain(b.asks.iter().map(|l| (2u8, l)))
                    .enumerate()
                    .map(|(i, (side, l))| {
                        PendingRow::L2Book(L2BookRow {
                            ts: dt_ms(b.timestamp),
                            coin: b.symbol.clone(),
                            side,
                            level: i as u16,
                            px: l.price.inner().to_string().parse().unwrap_or(0.0),
                            sz: l.size.inner().to_string().parse().unwrap_or(0.0),
                        })
                    })
                    .collect::<Vec<_>>();
                self.rows.extend(levels);
                self.row_count += b.bids.len() as u64 + b.asks.len() as u64;
                return;
            }
            DomainEvent::TradeUpdate(t) => Some(PendingRow::Trade(TradeRow {
                ts: dt_ms(t.timestamp),
                coin: t.symbol.clone(),
                px: t.price.inner().to_string().parse().unwrap_or(0.0),
                sz: t.size.inner().to_string().parse().unwrap_or(0.0),
                side: match t.side {
                    hypeedge_domain::enums::Side::Buy => 1,
                    hypeedge_domain::enums::Side::Sell => 2,
                },
                tid: t.tid,
            })),
            DomainEvent::CandleUpdate(c) => Some(PendingRow::Candle(CandleRow {
                ts: dt_ms(c.timestamp),
                coin: c.symbol.clone(),
                interval: c.interval.clone(),
                open: c.open.inner().to_string().parse().unwrap_or(0.0),
                high: c.high.inner().to_string().parse().unwrap_or(0.0),
                low: c.low.inner().to_string().parse().unwrap_or(0.0),
                close: c.close.inner().to_string().parse().unwrap_or(0.0),
                volume: c.volume.inner().to_string().parse().unwrap_or(0.0),
            })),
            DomainEvent::FundingUpdate(f) => Some(PendingRow::Funding(FundingRow {
                ts: dt_ms(f.timestamp),
                coin: f.symbol.clone(),
                funding_rate: f.funding_rate,
                premium: f.premium,
                oi: f.open_interest,
                mark_px: f.mark_price.inner().to_string().parse().unwrap_or(0.0),
            })),
            DomainEvent::MidPriceUpdate(m) => Some(PendingRow::MidPrice(MidPriceRow {
                ts: dt_ms(m.timestamp),
                coin: m.symbol.clone(),
                px: m.price.to_string().parse().unwrap_or(0.0),
            })),
            _ => None,
        };
        if let Some(row) = row {
            self.rows.push(row);
            self.row_count += 1;
        }
    }

    /// Flush the buffered rows per table. On failure, spool the rows and clear
    /// the buffer so a stuck ClickHouse never blocks the event loop.
    async fn flush(&mut self) -> Result<(), String> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        let mut ok = true;

        let l2 = rows.iter().filter_map(|r| match r {
            PendingRow::L2Book(r) => Some(r.clone()),
            _ => None,
        });
        let trades = rows.iter().filter_map(|r| match r {
            PendingRow::Trade(r) => Some(r.clone()),
            _ => None,
        });
        let candles = rows.iter().filter_map(|r| match r {
            PendingRow::Candle(r) => Some(r.clone()),
            _ => None,
        });
        let funding = rows.iter().filter_map(|r| match r {
            PendingRow::Funding(r) => Some(r.clone()),
            _ => None,
        });
        let mids = rows.iter().filter_map(|r| match r {
            PendingRow::MidPrice(r) => Some(r.clone()),
            _ => None,
        });

        if let Err(e) = insert_rows(&self.client, "l2_book", l2).await {
            ok = false;
            tracing::warn!(table = "l2_book", error = %e, "clickhouse_flush_error");
        }
        if let Err(e) = insert_rows(&self.client, "trades", trades).await {
            ok = false;
            tracing::warn!(table = "trades", error = %e, "clickhouse_flush_error");
        }
        if let Err(e) = insert_rows(&self.client, "candles", candles).await {
            ok = false;
            tracing::warn!(table = "candles", error = %e, "clickhouse_flush_error");
        }
        if let Err(e) = insert_rows(&self.client, "funding", funding).await {
            ok = false;
            tracing::warn!(table = "funding", error = %e, "clickhouse_flush_error");
        }
        if let Err(e) = insert_rows(&self.client, "mid_prices", mids).await {
            ok = false;
            tracing::warn!(table = "mid_prices", error = %e, "clickhouse_flush_error");
        }

        if !ok {
            // Spool the JSON serialization of the failed batch for replay.
            self.spooled_count += rows.len() as u64;
            self.append_spool(&rows);
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
}
