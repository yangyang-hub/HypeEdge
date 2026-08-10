//! Transactional durable-order boundary, port of
//! `PostgresDurableOrderStore` in `src/hypeedge/storage/postgres.py`.
//!
//! [`PostgresDurableOrderStore::persist_placement`] is the heart of the
//! trading path: it runs the DB-enforced risk admission (`FOR UPDATE` on the
//! account scope) and writes `orders` + `risk_events` + `execution_commands`
//! + `risk_reservations` + `order_events` + `outbox_events` in one transaction.

use std::collections::{BTreeSet, HashMap};
use std::str::FromStr;

use bigdecimal::BigDecimal;
use chrono::Utc;
use hypeedge_domain::decimal::Decimal;
use hypeedge_domain::enums::{OrderStatus, OrderType, Side, TimeInForce};
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::models::{Order, RiskCheckResult, RiskLimits};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::decimal_sqlx::{bd_to_dec, dec_to_bd};
use crate::rows::{AccountStateRow, OrderRow, PositionRow, RiskReservationRow};

/// Convert a sqlx error into the domain Postgres error.
pub(crate) fn map_sqlx(e: sqlx::Error) -> HypeEdgeError {
    HypeEdgeError::Postgres {
        message: e.to_string(),
    }
}

/// The transactional durable-order store.
pub struct PostgresDurableOrderStore {
    risk_limits: Option<RiskLimits>,
    account_stale_seconds: f64,
    reservation_ttl_seconds: i64,
}

impl Default for PostgresDurableOrderStore {
    fn default() -> Self {
        Self {
            risk_limits: Some(RiskLimits::default()),
            account_stale_seconds: 360.0,
            reservation_ttl_seconds: 86_400,
        }
    }
}

impl PostgresDurableOrderStore {
    pub fn new(
        risk_limits: Option<RiskLimits>,
        account_stale_seconds: f64,
        reservation_ttl_seconds: i64,
    ) -> Self {
        Self {
            risk_limits,
            account_stale_seconds,
            reservation_ttl_seconds,
        }
    }

    /// Persist a placement in one transaction. Returns the effective risk
    /// result (which may be stricter than the supplied one when the DB-level
    /// scope check runs).
    pub async fn persist_placement(
        &self,
        pool: &sqlx::PgPool,
        order: &mut Order,
        risk_result: &RiskCheckResult,
        command_id: Uuid,
        dispatch: bool,
        reference_price: Option<Decimal>,
    ) -> Result<RiskCheckResult, HypeEdgeError> {
        let order_id = Uuid::new_v4();
        let revision = 1i64;

        let mut tx: Transaction<'_, Postgres> = pool.begin().await.map_err(map_sqlx)?;

        let mut effective_risk = risk_result.clone();
        let mut dispatch = dispatch;
        if dispatch && risk_result.passed && self.risk_limits.is_some() {
            effective_risk = self
                .check_and_lock_risk_scope(&mut tx, order, reference_price)
                .await?;
            if !effective_risk.passed {
                dispatch = false;
                order.status = OrderStatus::Rejected;
                order.error_message = effective_risk.reason.clone();
            }
        }

        let event_type = if dispatch { "submitted" } else { "rejected" };
        let command_status = if dispatch { "pending" } else { "failed" };
        let payload = order_payload(order);

        // INSERT orders
        sqlx::query(
            r#"
            INSERT INTO orders (
                order_id, command_id, cloid, symbol, side, order_type, time_in_force,
                size, price, status, strategy_id, sub_account, reduce_only, is_spot,
                risk_reducing, max_slippage_bps, filled_size, revision, error_message,
                submitted_at, created_at, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,now(),now())
            "#,
        )
        .bind(order_id)
        .bind(command_id)
        .bind(&order.cloid)
        .bind(&order.symbol)
        .bind(order.side.as_str())
        .bind(order.order_type.as_str())
        .bind(order.time_in_force.as_str())
        .bind(dec_to_bd(order.size.inner()))
        .bind(order.price.map(|p| dec_to_bd(p.inner())))
        .bind(order.status.as_str())
        .bind(order.strategy_id.clone())
        .bind(order.sub_account.clone())
        .bind(order.reduce_only)
        .bind(order.is_spot)
        .bind(order.risk_reducing)
        .bind(order.max_slippage_bps as i32)
        .bind(dec_to_bd(order.filled_size.inner()))
        .bind(revision)
        .bind(order.error_message.clone())
        .bind(order.submitted_at)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        // INSERT risk_events
        sqlx::query(
            r#"
            INSERT INTO risk_events (
                risk_event_id, command_id, order_id, sub_account, strategy_id,
                passed, reason_code, reason, checked_limits, snapshot, duration_ms
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(command_id)
        .bind(order_id)
        .bind(order.sub_account.clone())
        .bind(order.strategy_id.clone())
        .bind(effective_risk.passed)
        .bind(effective_risk.reason.clone())
        .bind(effective_risk.reason.clone())
        .bind(serde_json::to_value(&effective_risk.checked_limits).unwrap_or_default())
        .bind(serde_json::json!({
            "reservation_included": dispatch,
            "reference_price": reference_price.map(|d| d.to_string()),
        }))
        .bind(0i32)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        // INSERT execution_commands
        sqlx::query(
            r#"
            INSERT INTO execution_commands (
                command_id, order_id, command_type, actor_type, actor_id, idempotency_key,
                status, payload, completed_at, last_error_code, last_error_message,
                available_at, created_at, updated_at
            ) VALUES ($1,$2,'place_order',$3,$4,$5,$6,$7,$8,$9,$10,now(),now(),now())
            "#,
        )
        .bind(command_id)
        .bind(order_id)
        .bind(if order.strategy_id.is_some() {
            "strategy"
        } else {
            "system"
        })
        .bind(
            order
                .strategy_id
                .clone()
                .unwrap_or_else(|| "execution_engine".into()),
        )
        .bind(&order.cloid)
        .bind(command_status)
        .bind(payload.clone())
        .bind(if dispatch { None } else { Some(Utc::now()) })
        .bind(if dispatch {
            None
        } else {
            effective_risk.reason.clone()
        })
        .bind(if dispatch {
            None
        } else {
            effective_risk.reason.clone()
        })
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        // INSERT risk_reservations when dispatching
        if dispatch {
            let locked_reference_price = self
                .reference_price(&mut *tx, order, reference_price)
                .await?;
            sqlx::query(
                r#"
                INSERT INTO risk_reservations (
                    reservation_id, command_id, command_item_id, order_id, sub_account,
                    strategy_id, symbol, side, reduce_only, reserved_size,
                    reserved_notional, status, expires_at, created_at, updated_at
                ) VALUES ($1,$2,NULL,$3,$4,$5,$6,$7,$8,$9,$10,'active',$11,now(),now())
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(command_id)
            .bind(order_id)
            .bind(order.sub_account.clone())
            .bind(order.strategy_id.clone())
            .bind(&order.symbol)
            .bind(order.side.as_str())
            .bind(order.reduce_only || order.risk_reducing)
            .bind(dec_to_bd(order.size.inner()))
            .bind(dec_to_bd(order.size.inner() * locked_reference_price))
            .bind(Utc::now() + chrono::Duration::seconds(self.reservation_ttl_seconds))
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx)?;
        }

        // order_events + outbox_events
        append_event_rows(&mut tx, order, order_id, revision, event_type, &payload).await?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(effective_risk)
    }

    /// The DB-enforced risk admission (port of `_check_and_lock_risk_scope`).
    /// Runs inside the placement transaction and serializes concurrent
    /// placements for the same account scope with `FOR UPDATE`.
    async fn check_and_lock_risk_scope(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        order: &Order,
        supplied_reference_price: Option<Decimal>,
    ) -> Result<RiskCheckResult, HypeEdgeError> {
        let limits = self
            .risk_limits
            .expect("check_and_lock_risk_scope called without risk limits");

        let mut checked = vec!["postgres_account_scope_locked".to_string()];

        let account: Option<AccountStateRow> = if order.sub_account.is_none() {
            sqlx::query_as::<_, AccountStateRow>(
                "SELECT * FROM account_state WHERE sub_account IS NULL FOR UPDATE",
            )
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_sqlx)?
        } else {
            sqlx::query_as::<_, AccountStateRow>(
                "SELECT * FROM account_state WHERE sub_account = $1 FOR UPDATE",
            )
            .bind(order.sub_account.clone().unwrap())
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_sqlx)?
        };

        let Some(account) = account else {
            return Ok(risk_fail("account_state_not_available", checked));
        };

        let age_seconds = (Utc::now() - account.exchange_updated_at).num_seconds() as f64;
        if age_seconds > self.account_stale_seconds {
            return Ok(risk_fail("account_state_stale", checked));
        }
        let equity = bd_to_dec(account.equity).map_err(|e| HypeEdgeError::Postgres {
            message: e.to_string(),
        })?;

        let positions: Vec<PositionRow> = if order.sub_account.is_none() {
            sqlx::query_as::<_, PositionRow>(
                "SELECT * FROM positions WHERE sub_account IS NULL FOR UPDATE",
            )
            .fetch_all(&mut **tx)
            .await
            .map_err(map_sqlx)?
        } else {
            sqlx::query_as::<_, PositionRow>(
                "SELECT * FROM positions WHERE sub_account = $1 FOR UPDATE",
            )
            .bind(order.sub_account.clone().unwrap())
            .fetch_all(&mut **tx)
            .await
            .map_err(map_sqlx)?
        };

        // Expire reservations whose orders reached a terminal state.
        sqlx::query(
            r#"
            UPDATE risk_reservations r SET status = 'expired', released_at = now()
            FROM orders o
            WHERE r.order_id = o.order_id
              AND r.status = 'active'
              AND r.expires_at <= now()
              AND o.status IN ('filled','cancelled','rejected','expired')
              AND (($1::text IS NULL AND r.sub_account IS NULL) OR r.sub_account = $1)
            "#,
        )
        .bind(order.sub_account.clone())
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;

        let reservations: Vec<RiskReservationRow> = if order.sub_account.is_none() {
            sqlx::query_as::<_, RiskReservationRow>(
                "SELECT * FROM risk_reservations WHERE status = 'active' AND sub_account IS NULL FOR UPDATE",
            )
            .fetch_all(&mut **tx)
            .await
            .map_err(map_sqlx)?
        } else {
            sqlx::query_as::<_, RiskReservationRow>(
                "SELECT * FROM risk_reservations WHERE status = 'active' AND sub_account = $1 FOR UPDATE",
            )
            .bind(order.sub_account.clone().unwrap())
            .fetch_all(&mut **tx)
            .await
            .map_err(map_sqlx)?
        };
        checked.push("active_reservations_included".to_string());

        let reference_price = self
            .reference_price(&mut **tx, order, supplied_reference_price)
            .await?;
        if reference_price <= Decimal::ZERO {
            return Ok(risk_fail("market_price_not_available", checked));
        }

        if order.is_spot {
            checked.push("spot_balance_rechecked_at_dispatch".to_string());
            return Ok(RiskCheckResult {
                passed: true,
                reason: None,
                checked_limits: checked,
            });
        }

        let symbol = order.symbol.clone();
        let existing = positions.iter().find(|p| p.symbol == symbol);
        let existing_size = match existing {
            Some(p) => bd_to_dec(p.size.clone()).map_err(|e| HypeEdgeError::Postgres {
                message: e.to_string(),
            })?,
            None => Decimal::ZERO,
        };
        let delta = if order.side == Side::Buy {
            order.size.inner()
        } else {
            -order.size.inner()
        };
        let resulting_size = existing_size + delta;

        if order.reduce_only {
            let reduces = !existing_size.is_zero() && resulting_size.abs() < existing_size.abs();
            let no_flip = resulting_size.is_zero()
                || resulting_size.is_positive() == existing_size.is_positive();
            if !reduces || !no_flip {
                return Ok(risk_fail("invalid_reduce_only_order", checked));
            }
        }

        let opening: Vec<&RiskReservationRow> =
            reservations.iter().filter(|r| !r.reduce_only).collect();

        let mut symbol_buys = Decimal::ZERO;
        let mut symbol_sells = Decimal::ZERO;
        for r in &opening {
            if r.symbol == symbol {
                if r.side == "buy" {
                    symbol_buys += bd_to_dec(r.reserved_size.clone()).map_err(|e| {
                        HypeEdgeError::Postgres {
                            message: e.to_string(),
                        }
                    })?;
                } else {
                    symbol_sells += bd_to_dec(r.reserved_size.clone()).map_err(|e| {
                        HypeEdgeError::Postgres {
                            message: e.to_string(),
                        }
                    })?;
                }
            }
        }
        if !order.reduce_only {
            if order.side == Side::Buy {
                symbol_buys += order.size.inner();
            } else {
                symbol_sells += order.size.inner();
            }
        }
        let worst_symbol_size = existing_size
            .abs()
            .max((existing_size - symbol_buys).abs())
            .max((existing_size - symbol_sells).abs());

        if equity <= Decimal::ZERO {
            return Ok(risk_fail("non_positive_equity", checked));
        }
        let max_symbol_notional =
            equity * Decimal::from_f64(limits.max_position_pct).unwrap_or(Decimal::ZERO);
        if !order.reduce_only && worst_symbol_size * reference_price > max_symbol_notional {
            return Ok(risk_fail(
                "position_limit_exceeded_with_reservations",
                checked,
            ));
        }

        let positions_by_symbol: HashMap<&str, &PositionRow> =
            positions.iter().map(|p| (p.symbol.as_str(), p)).collect();
        let mut exposure_symbols = BTreeSet::new();
        for p in &positions {
            exposure_symbols.insert(p.symbol.clone());
        }
        for r in &opening {
            exposure_symbols.insert(r.symbol.clone());
        }
        exposure_symbols.insert(symbol.clone());

        let mut resulting_notional = Decimal::ZERO;
        for exposure_symbol in &exposure_symbols {
            let position = positions_by_symbol.get(exposure_symbol.as_str());
            let position_size = match position {
                Some(p) => bd_to_dec(p.size.clone()).map_err(|e| HypeEdgeError::Postgres {
                    message: e.to_string(),
                })?,
                None => Decimal::ZERO,
            };
            let mut buys = Decimal::ZERO;
            let mut sells = Decimal::ZERO;
            for r in &opening {
                if &r.symbol == exposure_symbol {
                    if r.side == "buy" {
                        buys += bd_to_dec(r.reserved_size.clone()).map_err(|e| {
                            HypeEdgeError::Postgres {
                                message: e.to_string(),
                            }
                        })?;
                    } else {
                        sells += bd_to_dec(r.reserved_size.clone()).map_err(|e| {
                            HypeEdgeError::Postgres {
                                message: e.to_string(),
                            }
                        })?;
                    }
                }
            }
            if exposure_symbol == &symbol && !order.reduce_only {
                if order.side == Side::Buy {
                    buys += order.size.inner();
                } else {
                    sells += order.size.inner();
                }
            }
            let worst_size = (position_size + buys)
                .abs()
                .max((position_size - sells).abs());

            let mut price_candidates = Vec::new();
            if let Some(p) = position {
                if let Some(mark) = &p.mark_price {
                    price_candidates.push(bd_to_dec(mark.clone()).map_err(|e| {
                        HypeEdgeError::Postgres {
                            message: e.to_string(),
                        }
                    })?);
                } else {
                    price_candidates.push(Decimal::ZERO);
                }
            }
            for r in &opening {
                if &r.symbol == exposure_symbol && r.reserved_size > 0 {
                    let size = bd_to_dec(r.reserved_size.clone()).map_err(|e| {
                        HypeEdgeError::Postgres {
                            message: e.to_string(),
                        }
                    })?;
                    if !size.is_zero() {
                        let notional = bd_to_dec(r.reserved_notional.clone()).map_err(|e| {
                            HypeEdgeError::Postgres {
                                message: e.to_string(),
                            }
                        })?;
                        price_candidates.push(notional.div(size));
                    }
                }
            }
            if exposure_symbol == &symbol {
                price_candidates.push(reference_price);
            }
            let exposure_price = price_candidates.into_iter().max().unwrap_or(Decimal::ZERO);
            if worst_size.is_positive() && exposure_price <= Decimal::ZERO {
                return Ok(risk_fail("market_price_not_available", checked));
            }
            resulting_notional += worst_size * exposure_price;
        }

        if !order.reduce_only
            && resulting_notional.div(equity) > Decimal::from_i128(limits.max_leverage as i128)
        {
            return Ok(risk_fail("leverage_exceeded_with_reservations", checked));
        }

        checked.push("max_position".to_string());
        checked.push("max_leverage".to_string());
        Ok(RiskCheckResult {
            passed: true,
            reason: None,
            checked_limits: checked,
        })
    }

    /// The reference price for a reservation: order price > supplied > position mark.
    async fn reference_price<'c, E>(
        &self,
        executor: E,
        order: &Order,
        supplied_reference_price: Option<Decimal>,
    ) -> Result<Decimal, HypeEdgeError>
    where
        E: sqlx::Executor<'c, Database = Postgres>,
    {
        if let Some(price) = order.price
            && price.inner() > Decimal::ZERO
        {
            return Ok(price.inner());
        }
        if let Some(supplied) = supplied_reference_price
            && supplied > Decimal::ZERO
        {
            return Ok(supplied);
        }
        let mark: Option<BigDecimal> = if order.sub_account.is_none() {
            sqlx::query_scalar(
                "SELECT mark_price FROM positions WHERE symbol = $1 AND sub_account IS NULL LIMIT 1",
            )
            .bind(&order.symbol)
            .fetch_optional(executor)
            .await
            .map_err(map_sqlx)?
        } else {
            sqlx::query_scalar(
                "SELECT mark_price FROM positions WHERE symbol = $1 AND sub_account = $2 LIMIT 1",
            )
            .bind(&order.symbol)
            .bind(order.sub_account.clone().unwrap())
            .fetch_optional(executor)
            .await
            .map_err(map_sqlx)?
        };
        match mark {
            Some(m) => bd_to_dec(m).map_err(|e| HypeEdgeError::Postgres {
                message: e.to_string(),
            }),
            None => Ok(Decimal::ZERO),
        }
    }

    /// Persist an order state transition (port of `persist_transition`).
    pub async fn persist_transition(
        &self,
        pool: &sqlx::PgPool,
        order: &Order,
        event_type: &str,
        command_id: Option<Uuid>,
        command_status: Option<&str>,
    ) -> Result<(), HypeEdgeError> {
        let mut tx = pool.begin().await.map_err(map_sqlx)?;
        let record: OrderRow =
            sqlx::query_as::<_, OrderRow>("SELECT * FROM orders WHERE cloid = $1 FOR UPDATE")
                .bind(&order.cloid)
                .fetch_one(&mut *tx)
                .await
                .map_err(map_sqlx)?;

        let merged = merge_transport_transition(&record, order)?;
        let revision = record.revision + 1;

        sqlx::query(
            "UPDATE orders SET exchange_oid=$2, status=$3, filled_size=$4, avg_fill_price=$5, error_message=$6, submitted_at=$7, acknowledged_at=$8, filled_at=$9, revision=$10, updated_at=now() WHERE cloid=$1",
        )
        .bind(&order.cloid)
        .bind(merged.exchange_oid.clone())
        .bind(merged.status.as_str())
        .bind(dec_to_bd(merged.filled_size.inner()))
        .bind(merged.avg_fill_price.map(|p| dec_to_bd(p.inner())))
        .bind(merged.error_message.clone())
        .bind(merged.submitted_at)
        .bind(merged.acknowledged_at)
        .bind(merged.filled_at)
        .bind(revision)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        if let (Some(cmd_id), Some(cmd_status)) = (command_id, command_status) {
            let completed_at = match cmd_status {
                "succeeded" | "failed" | "cancelled" => Some(Utc::now()),
                _ => None,
            };
            sqlx::query(
                "UPDATE execution_commands SET status=$2, completed_at=$3, locked_at=NULL, locked_by=NULL, last_error_message=$4, updated_at=now() WHERE command_id=$1",
            )
            .bind(cmd_id)
            .bind(cmd_status)
            .bind(completed_at)
            .bind(merged.error_message.clone())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx)?;
        }

        let reservation: Option<RiskReservationRow> = sqlx::query_as::<_, RiskReservationRow>(
            "SELECT * FROM risk_reservations WHERE order_id = $1 FOR UPDATE",
        )
        .bind(record.order_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;
        if let Some(reservation) = reservation
            && reservation.status == "active"
        {
            let new_status = match merged.status {
                OrderStatus::Filled => Some("consumed"),
                OrderStatus::Rejected | OrderStatus::Cancelled | OrderStatus::Expired => {
                    Some("released")
                }
                _ => None,
            };
            if let Some(status) = new_status {
                sqlx::query(
                        "UPDATE risk_reservations SET status=$2, released_at=now(), updated_at=now() WHERE reservation_id=$1",
                    )
                    .bind(reservation.reservation_id)
                    .bind(status)
                    .execute(&mut *tx)
                    .await
                    .map_err(map_sqlx)?;
            }
        }

        let payload = order_payload(&merged);
        append_event_rows(
            &mut tx,
            &merged,
            record.order_id,
            revision,
            event_type,
            &payload,
        )
        .await?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(())
    }

    /// Persist a cancel request command (port of `persist_cancel_requested`).
    pub async fn persist_cancel_requested(
        &self,
        pool: &sqlx::PgPool,
        order: &Order,
        command_id: Uuid,
    ) -> Result<(), HypeEdgeError> {
        let mut tx = pool.begin().await.map_err(map_sqlx)?;
        let record: OrderRow =
            sqlx::query_as::<_, OrderRow>("SELECT * FROM orders WHERE cloid = $1 FOR UPDATE")
                .bind(&order.cloid)
                .fetch_one(&mut *tx)
                .await
                .map_err(map_sqlx)?;
        let revision = record.revision + 1;
        let payload = serde_json::json!({ "cloid": order.cloid, "symbol": order.symbol });

        sqlx::query("UPDATE orders SET revision=$2, updated_at=now() WHERE cloid=$1")
            .bind(&order.cloid)
            .bind(revision)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx)?;

        sqlx::query(
            r#"
            INSERT INTO execution_commands (
                command_id, order_id, command_type, actor_type, actor_id, idempotency_key,
                status, payload, available_at, created_at, updated_at
            ) VALUES ($1,$2,'cancel_order','system','execution_engine',$3,'pending',$4,now(),now(),now())
            ON CONFLICT (actor_id, idempotency_key) DO NOTHING
            "#,
        )
        .bind(command_id)
        .bind(record.order_id)
        .bind(format!("cancel:{}", order.cloid))
        .bind(payload)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        append_event_rows(
            &mut tx,
            order,
            record.order_id,
            revision,
            "cancel_requested",
            &order_payload(order),
        )
        .await?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(())
    }

    /// Load all open (non-terminal) orders.
    pub async fn load_open_orders(&self, pool: &sqlx::PgPool) -> Result<Vec<Order>, HypeEdgeError> {
        let rows = sqlx::query_as::<_, OrderRow>(
            r#"
            SELECT * FROM orders
            WHERE status NOT IN ('filled','cancelled','rejected','expired')
            ORDER BY created_at
            "#,
        )
        .fetch_all(pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(to_domain).collect()
    }

    /// Load an order by cloid.
    pub async fn get_order(
        &self,
        pool: &sqlx::PgPool,
        cloid: &str,
    ) -> Result<Option<Order>, HypeEdgeError> {
        let row = sqlx::query_as::<_, OrderRow>("SELECT * FROM orders WHERE cloid = $1")
            .bind(cloid)
            .fetch_optional(pool)
            .await
            .map_err(map_sqlx)?;
        row.map(to_domain).transpose()
    }

    /// Upsert an exchange-discovered order before any cancel side effect (port
    /// of `persist_reconciled_order`).
    pub async fn persist_reconciled_order(
        &self,
        pool: &sqlx::PgPool,
        order: &Order,
    ) -> Result<(), HypeEdgeError> {
        let mut tx = pool.begin().await.map_err(map_sqlx)?;
        let existing: Option<OrderRow> =
            sqlx::query_as::<_, OrderRow>("SELECT * FROM orders WHERE cloid = $1 FOR UPDATE")
                .bind(&order.cloid)
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx)?;

        let (order_id, revision, merged) = match existing {
            None => {
                let order_id = Uuid::new_v4();
                let revision = 1i64;
                let merged = order.clone();
                sqlx::query(
                    r#"
                    INSERT INTO orders (
                        order_id, command_id, cloid, symbol, side, order_type, time_in_force,
                        size, price, status, strategy_id, sub_account, reduce_only, is_spot,
                        risk_reducing, max_slippage_bps, filled_size, revision, error_message,
                        exchange_oid, submitted_at, acknowledged_at, filled_at,
                        created_at, updated_at
                    ) VALUES ($1,NULL,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,now(),now())
                    "#,
                )
                .bind(order_id)
                .bind(&merged.cloid)
                .bind(&merged.symbol)
                .bind(merged.side.as_str())
                .bind(merged.order_type.as_str())
                .bind(merged.time_in_force.as_str())
                .bind(dec_to_bd(merged.size.inner()))
                .bind(merged.price.map(|p| dec_to_bd(p.inner())))
                .bind(merged.status.as_str())
                .bind(merged.strategy_id.clone())
                .bind(merged.sub_account.clone())
                .bind(merged.reduce_only)
                .bind(merged.is_spot)
                .bind(merged.risk_reducing)
                .bind(merged.max_slippage_bps as i32)
                .bind(dec_to_bd(merged.filled_size.inner()))
                .bind(revision)
                .bind(merged.error_message.clone())
                .bind(merged.exchange_oid.clone())
                .bind(merged.submitted_at.or(Some(Utc::now())))
                .bind(merged.acknowledged_at)
                .bind(merged.filled_at)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx)?;
                (order_id, revision, merged)
            }
            Some(record) => {
                let revision = record.revision + 1;
                let merged = merge_reconciled(&record, order)?;
                sqlx::query(
                    r#"
                    UPDATE orders SET exchange_oid=$2, status=$3, filled_size=$4, avg_fill_price=$5,
                        error_message=$6, submitted_at=$7, acknowledged_at=$8, filled_at=$9,
                        revision=$10, updated_at=now()
                    WHERE cloid=$1
                    "#,
                )
                .bind(&merged.cloid)
                .bind(merged.exchange_oid.clone())
                .bind(merged.status.as_str())
                .bind(dec_to_bd(merged.filled_size.inner()))
                .bind(merged.avg_fill_price.map(|p| dec_to_bd(p.inner())))
                .bind(merged.error_message.clone())
                .bind(merged.submitted_at)
                .bind(merged.acknowledged_at)
                .bind(merged.filled_at)
                .bind(revision)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx)?;
                (record.order_id, revision, merged)
            }
        };

        let payload = order_payload(&merged);
        append_event_rows(
            &mut tx,
            &merged,
            order_id,
            revision,
            "reconciled_import",
            &payload,
        )
        .await?;

        if merged.is_terminal() {
            sqlx::query(
                r#"
                UPDATE risk_reservations
                SET status = CASE WHEN $2 = 'filled' THEN 'consumed' ELSE 'released' END,
                    released_at = now(), updated_at = now()
                WHERE order_id = $1 AND status = 'active'
                "#,
            )
            .bind(order_id)
            .bind(merged.status.as_str())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx)?;
        }

        tx.commit().await.map_err(map_sqlx)?;
        Ok(())
    }
}

/// Merge an exchange-reconciled order into the committed projection (port of
/// `_update_order_record` + `merge_reconciled`). The incoming exchange truth
/// wins for the reconcilable fields; filled-size/price never regress.
fn merge_reconciled(record: &OrderRow, order: &Order) -> Result<Order, HypeEdgeError> {
    let mut merged = to_domain(record.clone())?;
    merged.exchange_oid = order.exchange_oid.clone().or(merged.exchange_oid);
    merged.status = order.status;
    if order.filled_size.inner() > merged.filled_size.inner() {
        merged.filled_size = order.filled_size;
    }
    if let Some(price) = order.avg_fill_price {
        merged.avg_fill_price = Some(price);
    }
    if order.error_message.is_some() {
        merged.error_message = order.error_message.clone();
    }
    if merged.submitted_at.is_none() {
        merged.submitted_at = order.submitted_at;
    }
    merged.acknowledged_at = order.acknowledged_at.or(merged.acknowledged_at).or(Some(Utc::now()));
    merged.filled_at = order.filled_at.or(merged.filled_at);
    Ok(merged)
}

fn risk_fail(reason: &str, checked: Vec<String>) -> RiskCheckResult {
    RiskCheckResult {
        passed: false,
        reason: Some(reason.to_string()),
        checked_limits: checked,
    }
}

/// The JSON payload written to `orders.payload` / outbox for an order.
pub fn order_payload(order: &Order) -> serde_json::Value {
    serde_json::json!({
        "cloid": order.cloid,
        "exchange_oid": order.exchange_oid,
        "symbol": order.symbol,
        "side": order.side.as_str(),
        "size": order.size.to_string(),
        "price": order.price.map(|p| p.to_string()),
        "order_type": order.order_type.as_str(),
        "time_in_force": order.time_in_force.as_str(),
        "status": order.status.as_str(),
        "strategy_id": order.strategy_id,
        "sub_account": order.sub_account,
        "reduce_only": order.reduce_only,
        "is_spot": order.is_spot,
        "risk_reducing": order.risk_reducing,
        "max_slippage_bps": order.max_slippage_bps,
        "filled_size": order.filled_size.to_string(),
        "avg_fill_price": order.avg_fill_price.map(|p| p.to_string()),
        "error_message": order.error_message,
    })
}

/// The merged order after a transport transition (port of
/// `_merge_transport_transition`).
fn merge_transport_transition(record: &OrderRow, order: &Order) -> Result<Order, HypeEdgeError> {
    let mut merged = to_domain(record.clone())?;
    if let Some(exchange_oid) = &order.exchange_oid {
        if let Some(existing) = &merged.exchange_oid
            && existing != exchange_oid
        {
            return Err(HypeEdgeError::StrategyLifecycle {
                message: "exchange order oid changed for an existing cloid".into(),
            });
        }
        merged.exchange_oid = Some(exchange_oid.clone());
    }

    let current_status = merged.status;
    let incoming_status = order.status;
    let terminal = [
        OrderStatus::Filled,
        OrderStatus::Cancelled,
        OrderStatus::Rejected,
        OrderStatus::Expired,
    ];
    let lower_than_fill = [
        OrderStatus::Pending,
        OrderStatus::Submitted,
        OrderStatus::SubmitUnknown,
        OrderStatus::Acknowledged,
        OrderStatus::Filled,
    ];
    let preserve_current = terminal.contains(&current_status)
        || (current_status == OrderStatus::PartialFill
            && lower_than_fill.contains(&incoming_status));
    merged.status = if preserve_current {
        current_status
    } else {
        incoming_status
    };
    merged.error_message = order.error_message.clone();
    if merged.submitted_at.is_none() {
        merged.submitted_at = order.submitted_at;
    }
    if merged.acknowledged_at.is_none() {
        merged.acknowledged_at = order.acknowledged_at;
    }
    // Fill aggregates never regress (A11): an immediate-fill response or a
    // status-query `filled` must persist the actual fill data, not leave
    // `status='filled'` with `filled_size=0`.
    if order.filled_size.inner() > merged.filled_size.inner() {
        merged.filled_size = order.filled_size;
    }
    if let Some(price) = order.avg_fill_price {
        merged.avg_fill_price = Some(price);
    }
    merged.filled_at = order.filled_at.or(merged.filled_at);
    Ok(merged)
}

/// Map an `OrderRow` back to a domain `Order`.
pub fn to_domain(row: OrderRow) -> Result<Order, HypeEdgeError> {
    let size = bd_to_dec(row.size).map_err(|e| HypeEdgeError::Postgres {
        message: e.to_string(),
    })?;
    let price = match row.price {
        Some(p) => Some(hypeedge_domain::Price::new(bd_to_dec(p).map_err(|e| {
            HypeEdgeError::Postgres {
                message: e.to_string(),
            }
        })?)),
        None => None,
    };
    let filled = bd_to_dec(row.filled_size).map_err(|e| HypeEdgeError::Postgres {
        message: e.to_string(),
    })?;
    let avg = match row.avg_fill_price {
        Some(p) => Some(hypeedge_domain::Price::new(bd_to_dec(p).map_err(|e| {
            HypeEdgeError::Postgres {
                message: e.to_string(),
            }
        })?)),
        None => None,
    };
    Ok(Order {
        cloid: row.cloid,
        symbol: row.symbol,
        side: Side::from_str(&row.side).unwrap_or(Side::Buy),
        size: hypeedge_domain::Size::new(size),
        price,
        order_type: OrderType::from_str(&row.order_type).unwrap_or(OrderType::Limit),
        time_in_force: TimeInForce::from_str(&row.time_in_force).unwrap_or(TimeInForce::Gtc),
        status: OrderStatus::from_str(&row.status).unwrap_or(OrderStatus::Pending),
        strategy_id: row.strategy_id,
        sub_account: row.sub_account,
        reduce_only: row.reduce_only,
        is_spot: row.is_spot,
        risk_reducing: row.risk_reducing,
        max_slippage_bps: row.max_slippage_bps as u16,
        exchange_oid: row.exchange_oid,
        filled_size: hypeedge_domain::Size::new(filled),
        avg_fill_price: avg,
        submitted_at: row.submitted_at,
        acknowledged_at: row.acknowledged_at,
        filled_at: row.filled_at,
        error_message: row.error_message,
        created_at: row.created_at,
    })
}

/// Append `order_events` + `outbox_events` rows (port of `_append_event_rows`).
async fn append_event_rows(
    conn: &mut sqlx::postgres::PgConnection,
    order: &Order,
    order_id: Uuid,
    revision: i64,
    event_type: &str,
    payload: &serde_json::Value,
) -> Result<(), HypeEdgeError> {
    sqlx::query(
        r#"
        INSERT INTO order_events (
            event_id, order_id, cloid, revision, event_type, symbol, side, size, price,
            status, error_message, strategy_id, payload
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(order_id)
    .bind(&order.cloid)
    .bind(revision)
    .bind(event_type)
    .bind(&order.symbol)
    .bind(order.side.as_str())
    .bind(dec_to_bd(order.size.inner()))
    .bind(order.price.map(|p| dec_to_bd(p.inner())))
    .bind(order.status.as_str())
    .bind(order.error_message.clone())
    .bind(order.strategy_id.clone())
    .bind(payload)
    .execute(&mut *conn)
    .await
    .map_err(map_sqlx)?;

    sqlx::query(
        r#"
        INSERT INTO outbox_events (
            event_id, event_type, aggregate_type, aggregate_id, aggregate_revision,
            correlation_id, payload
        ) VALUES ($1,$2,'order',$3,$4,$5,$6)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(format!("order.{event_type}"))
    .bind(order_id.to_string())
    .bind(revision)
    .bind(&order.cloid)
    .bind(payload)
    .execute(&mut *conn)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}
