//! Postgres implementation of the transactional exchange-fact projector, port
//! of `ExchangeFactProjector` in `src/hypeedge/account/exchange_ingestor.py`.
//!
//! Each ingest claims the inbox row and appends the fact chain in one
//! transaction (mirroring the Python `async with session, session.begin()`):
//!
//! * fill: inbox → order (find/create) → fills + order_events + positions +
//!   ledger_entries (realized_pnl, fee) + outbox_events + risk_reservation
//!   consumption + cursor
//! * order update: inbox → order projection update + order_events + outbox +
//!   reservation release + cursor
//! * funding: inbox → funding_payments + cursor
//!
//! The inbox `ON CONFLICT (source, external_event_id) DO NOTHING` makes
//! duplicates and reordering harmless (WebSocket + REST recovery converge on
//! the same key).

use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::Decimal;
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_trading::account::exchange_ingestor::{
    CommittedFillProjection, ExchangeFactProjector, IngestResult, SOURCE, TERMINAL_STATUSES,
    canonical_payload, fill_external_id, fill_position_after, funding_external_id,
    normalize_status, projected_entry_price, synthetic_cloid,
};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::decimal_sqlx::{bd_to_dec, dec_to_bd};
use crate::durable_order_store::map_sqlx;

/// A projection of the durable order row the ingestor reads/mutates.
#[derive(Debug, sqlx::FromRow)]
struct DurableOrderLite {
    order_id: Uuid,
    cloid: String,
    legacy_cloid: Option<String>,
    exchange_oid: Option<String>,
    symbol: String,
    side: String,
    size: BigDecimal,
    price: Option<BigDecimal>,
    status: String,
    strategy_id: Option<String>,
    sub_account: Option<String>,
    is_spot: bool,
    filled_size: BigDecimal,
    avg_fill_price: Option<BigDecimal>,
    revision: i64,
}

type BigDecimal = bigdecimal::BigDecimal;

/// The transactional exchange-fact projector over the durable Postgres schema.
pub struct PostgresExchangeFactProjector {
    pool: PgPool,
    account: String,
}

impl PostgresExchangeFactProjector {
    pub fn new(pool: PgPool, account: &str) -> Self {
        Self {
            pool,
            account: account.to_lowercase(),
        }
    }

    async fn claim_inbox(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        external_id: &str,
        event_type: &str,
        payload_hash: &str,
        payload: &Value,
    ) -> Result<Option<i64>, HypeEdgeError> {
        let row: Option<(i64,)> = sqlx::query_as(
            r#"
            INSERT INTO inbox_events (event_id, source, external_event_id, event_type, payload_hash, payload)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (source, external_event_id) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(SOURCE)
        .bind(external_id)
        .bind(event_type)
        .bind(payload_hash)
        .bind(payload)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx)?;
        Ok(row.map(|(id,)| id))
    }

    /// Find an order by exchange_oid, else by raw cloid (transferring oid), else
    /// create a new projection. Mirrors `_find_or_create_order`.
    async fn find_or_create_order(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        exchange_oid: &str,
        payload: &Value,
    ) -> Result<DurableOrderLite, HypeEdgeError> {
        let by_oid: Option<DurableOrderLite> = sqlx::query_as(
            r#"
            SELECT order_id, cloid, legacy_cloid, exchange_oid, symbol, side, size, price,
                   status, strategy_id, sub_account, is_spot, filled_size, avg_fill_price, revision
            FROM orders WHERE exchange_oid = $1 FOR UPDATE
            "#,
        )
        .bind(exchange_oid)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx)?;
        if let Some(order) = by_oid {
            return Ok(order);
        }

        let raw_cloid = str_of(payload.get("cloid"));
        let is_canonical_cloid = raw_cloid.starts_with("0x") && raw_cloid.len() == 34;
        if is_canonical_cloid {
            let by_cloid: Option<DurableOrderLite> = sqlx::query_as(
                r#"
                SELECT order_id, cloid, legacy_cloid, exchange_oid, symbol, side, size, price,
                       status, strategy_id, sub_account, is_spot, filled_size, avg_fill_price, revision
                FROM orders WHERE cloid = $1 FOR UPDATE
                "#,
            )
            .bind(&raw_cloid)
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_sqlx)?;
            if let Some(order) = by_cloid {
                if order.exchange_oid.as_deref().is_some_and(|oid| oid != exchange_oid) {
                    return Err(HypeEdgeError::Reconciliation {
                        message: "exchange_order_cloid_oid_conflict".into(),
                    });
                }
                sqlx::query("UPDATE orders SET exchange_oid = $1, is_spot = $2, updated_at = now() WHERE order_id = $3")
                    .bind(exchange_oid)
                    .bind(str_of(payload.get("coin")).starts_with('@'))
                    .bind(order.order_id)
                    .execute(&mut **tx)
                    .await
                    .map_err(map_sqlx)?;
                let mut updated = order;
                updated.exchange_oid = Some(exchange_oid.to_string());
                updated.is_spot = str_of(payload.get("coin")).starts_with('@');
                return Ok(updated);
            }
        }

        // Create a new projection.
        let order_id = Uuid::new_v4();
        let size_raw = payload
            .get("origSz")
            .or_else(|| payload.get("sz"))
            .and_then(|v| v.as_str())
            .and_then(|s| Decimal::from_str_lenient(s).ok())
            .unwrap_or(Decimal::from_str_strict("0.000000000000000001").unwrap());
        let size = if size_raw <= Decimal::ZERO {
            Decimal::from_str_strict("0.000000000000000001").unwrap()
        } else {
            size_raw
        };
        let cloid = if is_canonical_cloid {
            raw_cloid.clone()
        } else {
            synthetic_cloid(exchange_oid)
        };
        let limit_px = payload
            .get("limitPx")
            .or_else(|| payload.get("px"))
            .and_then(|v| v.as_str())
            .and_then(|s| Decimal::from_str_lenient(s).ok());
        let coin = str_of(payload.get("coin"));
        let is_spot = coin.starts_with('@');
        let side = if str_of(payload.get("side")).eq_ignore_ascii_case("B") {
            "buy"
        } else {
            "sell"
        };
        let sub_account = self.account.clone();

        sqlx::query(
            r#"
            INSERT INTO orders (
                order_id, cloid, exchange_oid, symbol, side, order_type, time_in_force,
                size, price, status, sub_account, is_spot, filled_size, revision,
                created_at, updated_at
            ) VALUES ($1,$2,$3,$4,$5,'limit','Gtc',$6,$7,'acknowledged',$8,$9,0,0,now(),now())
            "#,
        )
        .bind(order_id)
        .bind(&cloid)
        .bind(exchange_oid)
        .bind(if coin.is_empty() { "UNKNOWN" } else { &coin })
        .bind(side)
        .bind(dec_to_bd(size))
        .bind(limit_px.filter(|d| *d > Decimal::ZERO).map(dec_to_bd))
        .bind(&sub_account)
        .bind(is_spot)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;

        Ok(DurableOrderLite {
            order_id,
            cloid,
            legacy_cloid: None,
            exchange_oid: Some(exchange_oid.to_string()),
            symbol: if coin.is_empty() { "UNKNOWN".into() } else { coin },
            side: side.to_string(),
            size: dec_to_bd(size),
            price: limit_px.filter(|d| *d > Decimal::ZERO).map(dec_to_bd),
            status: "acknowledged".into(),
            strategy_id: None,
            sub_account: Some(sub_account),
            is_spot,
            filled_size: dec_to_bd(Decimal::ZERO),
            avg_fill_price: None,
            revision: 0,
        })
    }

    /// Update the order's fill aggregates + status and append an `order_events`
    /// row. Mirrors `_apply_fill_to_order`.
    async fn apply_fill_to_order(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        order: &mut DurableOrderLite,
        fill_size: Decimal,
        fill_price: Decimal,
        occurred_at: DateTime<Utc>,
        payload: &Value,
    ) -> Result<(), HypeEdgeError> {
        let previous_filled = bd_to_dec(order.filled_size.clone()).map_err(dec_err)?;
        let new_filled = order.size.clone().min(dec_to_bd(previous_filled + fill_size));
        let new_filled_dec = bd_to_dec(new_filled.clone()).map_err(dec_err)?;
        let mut avg = order.avg_fill_price.clone();
        if new_filled_dec > Decimal::ZERO {
            let prev_avg = order.avg_fill_price.clone().map(|p| bd_to_dec(p).map_err(dec_err)).transpose()?;
            let numerator = previous_filled * prev_avg.unwrap_or(fill_price) + fill_size * fill_price;
            avg = Some(dec_to_bd(numerator / (previous_filled + fill_size)));
        }
        let previous_status = order.status.clone();
        let new_status = if new_filled_dec >= bd_to_dec(order.size.clone()).map_err(dec_err)? {
            "filled"
        } else if matches!(previous_status.as_str(), "cancelled" | "rejected" | "expired") {
            previous_status.as_str()
        } else {
            "partial_fill"
        };
        order.filled_size = new_filled;
        order.avg_fill_price = avg;
        order.status = new_status.to_string();
        order.revision += 1;
        let filled_at = if new_status == "filled" { Some(occurred_at) } else { None };

        sqlx::query(
            r#"
            UPDATE orders SET filled_size = $1, avg_fill_price = $2, status = $3,
                   filled_at = $4, revision = $5, updated_at = now()
            WHERE order_id = $6
            "#,
        )
        .bind(order.filled_size.clone())
        .bind(order.avg_fill_price.clone())
        .bind(new_status)
        .bind(filled_at)
        .bind(order.revision)
        .bind(order.order_id)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;

        sqlx::query(
            r#"
            INSERT INTO order_events (
                event_id, order_id, cloid, revision, event_type, symbol, side,
                size, price, status, strategy_id, payload, created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(order.order_id)
        .bind(&order.cloid)
        .bind(order.revision)
        .bind("exchange_fill")
        .bind(&order.symbol)
        .bind(&order.side)
        .bind(dec_to_bd(fill_size))
        .bind(dec_to_bd(fill_price))
        .bind(&order.status)
        .bind(order.strategy_id.clone())
        .bind(payload)
        .bind(occurred_at)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Upsert the position projection. Returns the authoritative (size, entry,
    /// mark) for the committed fill projection.
    async fn apply_fill_to_position(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        order: &DurableOrderLite,
        fill: &Value,
        fill_price: Decimal,
        realized_pnl: Decimal,
        occurred_at: DateTime<Utc>,
    ) -> Result<(Decimal, Option<Decimal>, Decimal), HypeEdgeError> {
        let sub_account = order.sub_account.as_deref().unwrap_or(&self.account);
        let existing: Option<(Uuid, Option<BigDecimal>, Option<BigDecimal>)> = sqlx::query_as(
            r#"
            SELECT position_id, entry_price, size FROM positions
            WHERE sub_account = $1 AND symbol = $2 FOR UPDATE
            "#,
        )
        .bind(sub_account)
        .bind(&order.symbol)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx)?;

        let old_size = fill
            .get("startPosition")
            .and_then(|v| v.as_str())
            .and_then(|s| Decimal::from_str_lenient(s).ok())
            .unwrap_or(Decimal::ZERO);
        let new_size = fill_position_after(fill);
        let old_entry = existing.as_ref().and_then(|(_, e, _)| e.clone()).map(|e| bd_to_dec(e).map_err(dec_err)).transpose()?;
        let entry = projected_entry_price(old_size, old_entry, new_size, fill_price);
        let entry_bd = entry.map(dec_to_bd);

        match existing {
            Some((position_id, _, _)) => {
                sqlx::query(
                    r#"
                    UPDATE positions SET size = $1, entry_price = $2, mark_price = $3,
                           realized_pnl = realized_pnl + $4, exchange_updated_at = $5,
                           revision = revision + 1, updated_at = now()
                    WHERE position_id = $6
                    "#,
                )
                .bind(dec_to_bd(new_size))
                .bind(entry_bd.clone())
                .bind(dec_to_bd(fill_price))
                .bind(dec_to_bd(realized_pnl))
                .bind(occurred_at)
                .bind(position_id)
                .execute(&mut **tx)
                .await
                .map_err(map_sqlx)?;
            }
            None => {
                sqlx::query(
                    r#"
                    INSERT INTO positions (
                        position_id, sub_account, symbol, size, entry_price, mark_price,
                        realized_pnl, exchange_updated_at, revision, created_at, updated_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1,now(),now())
                    "#,
                )
                .bind(Uuid::new_v4())
                .bind(sub_account)
                .bind(&order.symbol)
                .bind(dec_to_bd(new_size))
                .bind(entry_bd.clone())
                .bind(dec_to_bd(fill_price))
                .bind(dec_to_bd(realized_pnl))
                .bind(occurred_at)
                .execute(&mut **tx)
                .await
                .map_err(map_sqlx)?;
            }
        }
        Ok((new_size, entry, fill_price))
    }

    async fn advance_cursor(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        stream: &str,
        timestamp_ms: i64,
        external_id: &str,
    ) -> Result<(), HypeEdgeError> {
        sqlx::query(
            r#"
            INSERT INTO exchange_sync_cursors (source, sub_account, stream, last_exchange_timestamp_ms, last_external_event_id)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (source, sub_account, stream)
            DO UPDATE SET
                last_exchange_timestamp_ms = EXCLUDED.last_exchange_timestamp_ms,
                last_external_event_id = EXCLUDED.last_external_event_id,
                updated_at = now()
            WHERE exchange_sync_cursors.last_exchange_timestamp_ms <= EXCLUDED.last_exchange_timestamp_ms
            "#,
        )
        .bind(SOURCE)
        .bind(&self.account)
        .bind(stream)
        .bind(timestamp_ms.max(0))
        .bind(external_id)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }
}


/// Map a domain Decimal conversion failure onto the storage error hierarchy.
fn dec_err(e: hypeedge_domain::decimal::DecimalError) -> HypeEdgeError {
    HypeEdgeError::Storage {
        message: format!("decimal conversion: {e}"),
    }
}

fn str_of(v: Option<&Value>) -> String {
    match v {
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

#[async_trait::async_trait]
impl ExchangeFactProjector for PostgresExchangeFactProjector {
    async fn ingest_fill(&self, fill: &Value) -> Result<IngestResult, HypeEdgeError> {
        let external_id = fill_external_id(fill);
        let (payload_hash, payload) = canonical_payload(fill);
        let occurred_at = DateTime::from_timestamp_millis(
            fill.get("time").and_then(|v| v.as_i64()).unwrap_or(0),
        )
        .unwrap_or_else(Utc::now);

        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;
        let Some(_inbox_id) = self
            .claim_inbox(&mut tx, &external_id, "fill", &payload_hash, &payload)
            .await?
        else {
            return Ok(IngestResult::dedup(&external_id));
        };

        let exchange_oid = str_of(fill.get("oid"));
        let mut order = self.find_or_create_order(&mut tx, &exchange_oid, fill).await?;
        let is_spot = order.is_spot || str_of(fill.get("coin")).starts_with('@');
        if is_spot && !order.is_spot {
            order.is_spot = true;
            sqlx::query("UPDATE orders SET is_spot = $1, updated_at = now() WHERE order_id = $2")
                .bind(true)
                .bind(order.order_id)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx)?;
        }

        let fill_id = Uuid::new_v4();
        let size = fill
            .get("sz")
            .and_then(|v| v.as_str())
            .and_then(|s| Decimal::from_str_lenient(s).ok())
            .unwrap_or(Decimal::ZERO);
        let price = fill
            .get("px")
            .and_then(|v| v.as_str())
            .and_then(|s| Decimal::from_str_lenient(s).ok())
            .unwrap_or(Decimal::ZERO);
        let fee = fill
            .get("fee")
            .and_then(|v| v.as_str())
            .and_then(|s| Decimal::from_str_lenient(s).ok())
            .unwrap_or(Decimal::ZERO);
        let realized_pnl = fill
            .get("closedPnl")
            .and_then(|v| v.as_str())
            .and_then(|s| Decimal::from_str_lenient(s).ok())
            .unwrap_or(Decimal::ZERO);
        let side = if str_of(fill.get("side")).eq_ignore_ascii_case("B") { "buy" } else { "sell" };
        let is_maker = !fill.get("crossed").and_then(|c| c.as_bool()).unwrap_or(false);

        sqlx::query(
            r#"
            INSERT INTO fills (
                fill_id, source, exchange_fill_id, order_id, cloid, exchange_oid,
                symbol, side, price, size, fee, realized_pnl, is_maker, is_spot,
                strategy_id, sub_account, occurred_at, timestamp, raw_event, created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,now())
            "#,
        )
        .bind(fill_id)
        .bind(SOURCE)
        .bind(&external_id)
        .bind(order.order_id)
        .bind(&order.cloid)
        .bind(&exchange_oid)
        .bind(str_of(fill.get("coin")))
        .bind(side)
        .bind(dec_to_bd(price))
        .bind(dec_to_bd(size))
        .bind(dec_to_bd(fee))
        .bind(dec_to_bd(realized_pnl))
        .bind(is_maker)
        .bind(is_spot)
        .bind(order.strategy_id.clone())
        .bind(order.sub_account.as_deref().unwrap_or(&self.account))
        .bind(occurred_at)
        .bind(occurred_at)
        .bind(&payload)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        self.apply_fill_to_order(&mut tx, &mut order, size, price, occurred_at, &payload)
            .await?;

        // Consume the active risk reservation proportionally.
        let reservation: Option<(Uuid, BigDecimal, BigDecimal)> = sqlx::query_as(
            r#"
            SELECT reservation_id, reserved_size, reserved_notional FROM risk_reservations
            WHERE order_id = $1 AND status = 'active' FOR UPDATE
            "#,
        )
        .bind(order.order_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;
        if let Some((reservation_id, reserved_size, reserved_notional)) = reservation {
            let reserved_dec = bd_to_dec(reserved_size).map_err(dec_err)?;
            let new_reserved = (reserved_dec - size).max(Decimal::ZERO);
            let new_notional = if reserved_dec > Decimal::ZERO {
                let factor = if reserved_dec > Decimal::ZERO { new_reserved / reserved_dec } else { Decimal::ZERO };
                dec_to_bd(bd_to_dec(reserved_notional).map_err(dec_err)? * factor)
            } else {
                dec_to_bd(Decimal::ZERO)
            };
            let status = if new_reserved == Decimal::ZERO || order.status == "filled" {
                "consumed"
            } else {
                "active"
            };
            let released_at = if status == "consumed" { Some(occurred_at) } else { None };
            sqlx::query(
                r#"
                UPDATE risk_reservations SET reserved_size = $1, reserved_notional = $2,
                       status = $3, released_at = $4 WHERE reservation_id = $5
                "#,
            )
            .bind(dec_to_bd(new_reserved))
            .bind(new_notional)
            .bind(status)
            .bind(released_at)
            .bind(reservation_id)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx)?;
        }

        let position = if is_spot {
            None
        } else {
            Some(
                self.apply_fill_to_position(&mut tx, &order, fill, price, realized_pnl, occurred_at)
                    .await?,
            )
        };

        // Ledger entries: realized_pnl (credit) + fee (debit).
        for (entry_type, amount) in [("realized_pnl", realized_pnl), ("fee", -fee)] {
            sqlx::query(
                r#"
                INSERT INTO ledger_entries (
                    entry_id, fill_id, entry_type, asset, amount, sub_account,
                    strategy_id, occurred_at, metadata, created_at
                ) VALUES ($1,$2,$3,'USDC',$4,$5,$6,$7,$8,now())
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(fill_id)
            .bind(entry_type)
            .bind(dec_to_bd(amount))
            .bind(order.sub_account.as_deref().unwrap_or(&self.account))
            .bind(order.strategy_id.clone())
            .bind(occurred_at)
            .bind(serde_json::json!({"exchange_oid": exchange_oid, "symbol": str_of(fill.get("coin"))}))
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx)?;
        }

        // Outbox: durable SSE event for the frontend fill feed.
        sqlx::query(
            r#"
            INSERT INTO outbox_events (
                event_id, event_type, schema_version, aggregate_type, aggregate_id,
                aggregate_revision, payload, occurred_at
            ) VALUES ($1,'exchange.fill.ingested',1,'order',$2,$3,$4,$5)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(order.order_id.to_string())
        .bind(order.revision)
        .bind(serde_json::json!({
            "fill_id": fill_id.to_string(),
            "cloid": order.cloid,
            "symbol": str_of(fill.get("coin")),
            "price": price.to_string(),
            "size": size.to_string(),
            "position_size": position.as_ref().map(|(s, _, _)| s.to_string()),
            "is_spot": is_spot,
        }))
        .bind(occurred_at)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        self.advance_cursor(&mut tx, "fills", fill.get("time").and_then(|v| v.as_i64()).unwrap_or(0), &external_id)
            .await?;
        sqlx::query("UPDATE inbox_events SET processed_at = now() WHERE external_event_id = $1 AND source = $2")
            .bind(&external_id)
            .bind(SOURCE)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx)?;
        tx.commit().await.map_err(map_sqlx)?;

        Ok(IngestResult {
            processed: true,
            external_event_id: external_id.clone(),
            fill_projection: Some(CommittedFillProjection {
                external_event_id: external_id,
                cloid: order.cloid,
                exchange_oid,
                symbol: str_of(fill.get("coin")),
                side: side.to_string(),
                price,
                size,
                fee,
                is_maker,
                occurred_at: occurred_at.timestamp_millis(),
                strategy_id: order.strategy_id,
                sub_account: order.sub_account,
                position_size: position.as_ref().map(|(s, _, _)| *s),
                position_entry_price: position.as_ref().and_then(|(_, e, _)| *e),
                position_mark_price: position.as_ref().map(|(_, _, m)| *m),
                order_status: order.status,
                is_spot,
            }),
            funding_amount: None,
        })
    }

    async fn ingest_order_update(&self, update: &Value) -> Result<IngestResult, HypeEdgeError> {
        let raw_order = update.get("order").unwrap_or(update);
        let exchange_oid = str_of(raw_order.get("oid"));
        let timestamp_ms = update
            .get("statusTimestamp")
            .or_else(|| raw_order.get("timestamp"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let event_status = normalize_status(
            update.get("status").or_else(|| raw_order.get("status")),
        );
        let external_id = format!("order:{exchange_oid}:{event_status}:{timestamp_ms}");
        let (payload_hash, payload) = canonical_payload(update);
        let occurred_at = if timestamp_ms > 0 {
            DateTime::from_timestamp_millis(timestamp_ms).unwrap_or_else(Utc::now)
        } else {
            Utc::now()
        };

        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;
        let Some(_inbox_id) = self
            .claim_inbox(&mut tx, &external_id, "order_update", &payload_hash, &payload)
            .await?
        else {
            return Ok(IngestResult::dedup(&external_id));
        };
        let mut order = self.find_or_create_order(&mut tx, &exchange_oid, raw_order).await?;

        // cloid ownership transfer (legacy_cloid preserved).
        let actual_cloid = str_of(raw_order.get("cloid"));
        if actual_cloid.starts_with("0x") && actual_cloid.len() == 34 && order.cloid != actual_cloid {
            let collision: Option<(Uuid,)> = sqlx::query_as("SELECT order_id FROM orders WHERE cloid = $1")
                .bind(&actual_cloid)
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx)?;
            match collision {
                None => {
                    let legacy = order.cloid.clone();
                    order.legacy_cloid = Some(legacy.clone());
                    order.cloid = actual_cloid.clone();
                    sqlx::query("UPDATE orders SET legacy_cloid = $1, cloid = $2, updated_at = now() WHERE order_id = $3")
                        .bind(legacy)
                        .bind(&actual_cloid)
                        .bind(order.order_id)
                        .execute(&mut *tx)
                        .await
                        .map_err(map_sqlx)?;
                }
                Some((collision_id,)) if collision_id != order.order_id => {
                    return Err(HypeEdgeError::Reconciliation {
                        message: "exchange_order_cloid_collision".into(),
                    });
                }
                _ => {}
            }
        }

        // Apply the projection update.
        let coin = str_of(raw_order.get("coin"));
        if !coin.is_empty() {
            order.symbol = coin;
        }
        order.side = if str_of(raw_order.get("side")).eq_ignore_ascii_case("B") { "buy".into() } else { "sell".into() };
        if let Some(d) = raw_order
            .get("origSz")
            .or_else(|| raw_order.get("sz"))
            .and_then(|v| v.as_str())
            .and_then(|s| Decimal::from_str_lenient(s).ok())
        {
            order.size = dec_to_bd(d);
        }
        if let Some(limit_px) = raw_order
            .get("limitPx")
            .and_then(|v| v.as_str())
            .and_then(|s| Decimal::from_str_lenient(s).ok())
            .filter(|d| *d > Decimal::ZERO)
        {
            order.price = Some(dec_to_bd(limit_px));
        }
        let was_terminal = TERMINAL_STATUSES.contains(&order.status.as_str());
        let event_terminal = TERMINAL_STATUSES.contains(&event_status.as_str());
        // C7: `filled` is the most meaningful terminal state. A stale/racing
        // cancelled/rejected snapshot (WS vs REST reordering) must never
        // regress an order that was already filled — keep `filled`.
        let filled_prevents_regression =
            order.status.as_str() == "filled" && event_terminal && event_status != "filled";
        if !filled_prevents_regression && (!was_terminal || event_terminal) {
            order.status = event_status.clone();
        }
        order.revision += 1;

        if event_terminal {
            let reservation: Option<(Uuid,)> = sqlx::query_as(
                r#"
                SELECT reservation_id FROM risk_reservations
                WHERE order_id = $1 AND status = 'active' FOR UPDATE
                "#,
            )
            .bind(order.order_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlx)?;
            if let Some((reservation_id,)) = reservation {
                let res_status = if event_status == "filled" { "consumed" } else { "released" };
                sqlx::query(
                    "UPDATE risk_reservations SET status = $1, released_at = now() WHERE reservation_id = $2",
                )
                .bind(res_status)
                .bind(reservation_id)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx)?;
            }
        }

        sqlx::query(
            r#"
            UPDATE orders SET symbol = $1, side = $2, size = $3, price = $4, status = $5,
                   revision = $6, updated_at = now()
            WHERE order_id = $7
            "#,
        )
        .bind(&order.symbol)
        .bind(&order.side)
        .bind(order.size.clone())
        .bind(order.price.clone())
        .bind(&order.status)
        .bind(order.revision)
        .bind(order.order_id)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        sqlx::query(
            r#"
            INSERT INTO order_events (
                event_id, order_id, cloid, revision, event_type, symbol, side,
                size, price, status, strategy_id, payload, created_at
            ) VALUES ($1,$2,$3,$4,'exchange_order_update',$5,$6,$7,$8,$9,$10,$11,$12)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(order.order_id)
        .bind(&order.cloid)
        .bind(order.revision)
        .bind(&order.symbol)
        .bind(&order.side)
        .bind(order.size.clone())
        .bind(order.price.clone())
        .bind(&order.status)
        .bind(order.strategy_id.clone())
        .bind(&payload)
        .bind(occurred_at)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        sqlx::query(
            r#"
            INSERT INTO outbox_events (
                event_id, event_type, schema_version, aggregate_type, aggregate_id,
                aggregate_revision, payload, occurred_at
            ) VALUES ($1,'exchange.order.updated',1,'order',$2,$3,$4,$5)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(order.order_id.to_string())
        .bind(order.revision)
        .bind(serde_json::json!({"cloid": order.cloid, "exchange_oid": exchange_oid, "status": order.status}))
        .bind(occurred_at)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        self.advance_cursor(&mut tx, "orders", timestamp_ms, &external_id).await?;
        sqlx::query("UPDATE inbox_events SET processed_at = now() WHERE external_event_id = $1 AND source = $2")
            .bind(&external_id)
            .bind(SOURCE)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx)?;
        tx.commit().await.map_err(map_sqlx)?;
        Ok(IngestResult {
            processed: true,
            external_event_id: external_id,
            fill_projection: None,
            funding_amount: None,
        })
    }

    async fn ingest_funding(&self, update: &Value) -> Result<IngestResult, HypeEdgeError> {
        let external_id = funding_external_id(update);
        let (payload_hash, payload) = canonical_payload(update);
        let delta = update.get("delta").filter(|d| d.is_object()).cloned().unwrap_or(Value::Null);
        if delta.get("type").and_then(|t| t.as_str()) != Some("funding") {
            return Err(HypeEdgeError::Reconciliation {
                message: "invalid_user_funding_update".into(),
            });
        }
        let symbol = str_of(delta.get("coin"));
        if symbol.is_empty() {
            return Err(HypeEdgeError::Reconciliation {
                message: "user_funding_missing_coin".into(),
            });
        }
        let amount = delta.get("usdc").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str_lenient(s).ok()).unwrap_or(Decimal::ZERO);
        let funding_rate = delta.get("fundingRate").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str_lenient(s).ok()).unwrap_or(Decimal::ZERO);
        let position_size = delta.get("szi").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str_lenient(s).ok()).unwrap_or(Decimal::ZERO);
        let occurred_at = DateTime::from_timestamp_millis(update.get("time").and_then(|v| v.as_i64()).unwrap_or(0)).unwrap_or_else(Utc::now);

        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;
        let Some(_inbox_id) = self
            .claim_inbox(&mut tx, &external_id, "funding", &payload_hash, &payload)
            .await?
        else {
            return Ok(IngestResult::dedup(&external_id));
        };

        // Attribute to the active funding-arb cycle for the symbol, if any.
        let cycle_id: Option<(Uuid,)> = sqlx::query_as(
            r#"
            SELECT cycle_id FROM funding_arb_cycles
            WHERE strategy_id IS NOT NULL AND sub_account = $1 AND perp_symbol = $2
              AND created_at <= $3 AND (closed_at IS NULL OR closed_at >= $3)
            ORDER BY created_at DESC LIMIT 1
            "#,
        )
        .bind(&self.account)
        .bind(&symbol)
        .bind(occurred_at)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        sqlx::query(
            r#"
            INSERT INTO funding_payments (
                payment_id, source, external_event_id, sub_account, cycle_id, symbol,
                amount, funding_rate, position_size, occurred_at, raw_event, created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now())
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(SOURCE)
        .bind(&external_id)
        .bind(&self.account)
        .bind(cycle_id.map(|(id,)| id))
        .bind(&symbol)
        .bind(dec_to_bd(amount))
        .bind(dec_to_bd(funding_rate))
        .bind(dec_to_bd(position_size))
        .bind(occurred_at)
        .bind(&payload)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        self.advance_cursor(&mut tx, "funding", update.get("time").and_then(|v| v.as_i64()).unwrap_or(0), &external_id)
            .await?;
        sqlx::query("UPDATE inbox_events SET processed_at = now() WHERE external_event_id = $1 AND source = $2")
            .bind(&external_id)
            .bind(SOURCE)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx)?;
        tx.commit().await.map_err(map_sqlx)?;
        Ok(IngestResult {
            processed: true,
            external_event_id: external_id,
            fill_projection: None,
            funding_amount: Some(amount),
        })
    }

    async fn has_order(&self, exchange_oid: &str) -> Result<bool, HypeEdgeError> {
        let row: Option<(Uuid,)> = sqlx::query_as("SELECT order_id FROM orders WHERE exchange_oid = $1")
            .bind(exchange_oid)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(row.is_some())
    }

    async fn cursor(&self, stream: &str) -> Result<i64, HypeEdgeError> {
        let row: Option<(i64,)> = sqlx::query_as(
            r#"
            SELECT last_exchange_timestamp_ms FROM exchange_sync_cursors
            WHERE source = $1 AND sub_account = $2 AND stream = $3
            "#,
        )
        .bind(SOURCE)
        .bind(&self.account)
        .bind(stream)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(row.map(|(v,)| v).unwrap_or(0))
    }
}
