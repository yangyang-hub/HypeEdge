//! Postgres-backed quote-plan child store, port of the `claim`/`record`/
//! `finish` paths in `src/hypeedge/execution/quote_plan_worker.py`.
//!
//! Children are claimed with `FOR UPDATE SKIP LOCKED`. A replacement placement
//! is deliberately not claimable until the cancel child for the same plan item
//! is durably `succeeded`: the claim excludes any item with a live sibling
//! cancel. Ambiguous children are never retried.
//!
//! The store holds the `PgPool` and implements the
//! [`QuotePlanStore`] trait directly, matching the
//! `PostgresConfigVersionStore` pattern.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::{Price, Size};
use hypeedge_domain::enums::Side;
use hypeedge_domain::error::HypeEdgeError;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::durable_order_store::map_sqlx;
use crate::decimal_sqlx::bd_to_dec;
use hypeedge_trading::execution::batch::{ChildActionType, GuardDecision};
use hypeedge_trading::execution::quote_plan_worker::{QuoteDispatchChild, QuotePlanStore};

/// One claimed child row (the flat join of the claim query).
#[derive(Debug, Clone, FromRow)]
pub struct QuotePlanChildRow {
    pub item_id: i64,
    pub command_id: Uuid,
    pub action_type: String,
    pub attempt_count: i32,
    pub plan_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub side: String,
    pub level: i32,
    pub source_cloid: Option<String>,
    pub target_cloid: Option<String>,
    pub desired_price: Option<bigdecimal::BigDecimal>,
    pub desired_size: Option<bigdecimal::BigDecimal>,
    pub sub_account: Option<String>,
    pub revision: i64,
    pub market_version: i64,
    pub runtime_session_id: String,
    pub config_version: i64,
    pub connection_generation: i64,
    pub valid_until: DateTime<Utc>,
    pub payload: serde_json::Value,
}

/// The Postgres-backed [`QuotePlanStore`] implementation.
pub struct PostgresQuotePlanStore {
    pool: PgPool,
    /// Lease duration for claimed child items; expired leases are reclassified
    /// back to `pending` so a crashed worker's items are never stuck (A13).
    lease_seconds: i64,
}

impl PostgresQuotePlanStore {
    pub fn new(pool: PgPool) -> Self {
        Self::with_lease(pool, 30)
    }

    pub fn with_lease(pool: PgPool, lease_seconds: i64) -> Self {
        Self {
            pool,
            lease_seconds,
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Claim one pending child with a SKIP LOCKED lease.
    async fn claim_child_inner(
        &self,
        worker_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<QuoteDispatchChild>, HypeEdgeError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;
        let lease_cutoff = now - chrono::Duration::seconds(self.lease_seconds);

        // Reap expired child leases (A13): a worker that crashed between
        // `claim_child` and `record_attempt`/`finish_without_send` left the item
        // `processing` forever; the claim query only matches `pending`, so
        // nothing could ever reclaim it (parent plan never reaches terminal,
        // slot stuck inflight). Mirror `command_queue`'s lease expiry.
        sqlx::query(
            r#"
            UPDATE execution_command_items i
            SET status = 'pending', locked_at = NULL, locked_by = NULL, updated_at = now()
            FROM execution_commands c
            WHERE c.command_id = i.command_id
              AND c.command_type = 'quote_plan'
              AND i.status = 'processing'
              AND i.locked_at < $1
            "#,
        )
        .bind(lease_cutoff)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        let row: Option<QuotePlanChildRow> = sqlx::query_as::<_, QuotePlanChildRow>(
            r#"
            SELECT
                i.id AS item_id,
                c.command_id,
                i.action_type,
                i.attempt_count,
                p.plan_id,
                p.strategy_id,
                qi.symbol,
                qi.side,
                qi.level,
                qi.source_cloid,
                qi.target_cloid,
                qi.desired_price,
                qi.desired_size,
                o.sub_account,
                p.revision,
                p.market_version,
                (c.payload->>'runtime_session_id')::text AS runtime_session_id,
                (c.payload->>'config_version')::bigint AS config_version,
                (c.payload->>'connection_generation')::bigint AS connection_generation,
                p.valid_until,
                c.payload
            FROM execution_command_items i
            JOIN execution_commands c ON c.command_id = i.command_id
            JOIN quote_plan_items qi ON qi.id = i.plan_item_id
            JOIN quote_plans p ON p.plan_id = qi.plan_id
            LEFT JOIN orders o ON o.order_id = qi.target_order_id
            WHERE i.status = 'pending'
              AND i.available_at <= $1
              AND c.command_type = 'quote_plan'
              AND c.status IN ('pending','processing')
              AND NOT EXISTS (
                  SELECT 1 FROM execution_command_items sibling
                  WHERE sibling.plan_item_id = i.plan_item_id
                    AND sibling.action_type = 'cancel'
                    AND sibling.id != i.id
                    AND sibling.status != 'succeeded'
              )
            ORDER BY c.command_id, i.ordinal
            LIMIT 1
            FOR UPDATE OF i SKIP LOCKED
            "#,
        )
        .bind(now)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        let Some(row) = row else {
            tx.commit().await.map_err(map_sqlx)?;
            return Ok(None);
        };

        // Lease the item and its parent command.
        let new_attempt = row.attempt_count + 1;
        sqlx::query(
            r#"
            UPDATE execution_command_items
            SET status = 'processing', locked_at = $2, locked_by = $3,
                attempt_count = $4, updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(row.item_id)
        .bind(now)
        .bind(worker_id)
        .bind(new_attempt)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;
        sqlx::query(
            r#"
            UPDATE execution_commands
            SET status = 'processing', locked_at = $2, locked_by = $3, updated_at = now()
            WHERE command_id = $1
            "#,
        )
        .bind(row.command_id)
        .bind(now)
        .bind(worker_id)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        tx.commit().await.map_err(map_sqlx)?;

        let side = match row.side.as_str() {
            "buy" => Side::Buy,
            "sell" => Side::Sell,
            other => {
                return Err(HypeEdgeError::Storage {
                    message: format!("invalid quote-plan item side: {other}"),
                })
            }
        };
        let action = match row.action_type.as_str() {
            "place" => ChildActionType::Place,
            "cancel" => ChildActionType::Cancel,
            "modify" => ChildActionType::Modify,
            other => {
                return Err(HypeEdgeError::Storage {
                    message: format!("invalid quote-plan item action: {other}"),
                })
            }
        };
        let price = row
            .desired_price
            .map(bd_to_dec)
            .transpose()
            .map_err(|e| HypeEdgeError::Storage { message: e.to_string() })?
            .map(Price::new);
        let size = row
            .desired_size
            .map(bd_to_dec)
            .transpose()
            .map_err(|e| HypeEdgeError::Storage { message: e.to_string() })?
            .map(Size::new);

        Ok(Some(QuoteDispatchChild {
            item_id: row.item_id,
            command_id: row.command_id,
            action,
            attempt: new_attempt as u32,
            plan_id: row.plan_id,
            strategy_id: row.strategy_id,
            symbol: row.symbol,
            runtime_session_id: row.runtime_session_id,
            config_version: row.config_version,
            plan_revision: row.revision,
            market_version: row.market_version,
            connection_generation: row.connection_generation,
            valid_until: row.valid_until,
            source_cloid: row.source_cloid,
            target_cloid: row.target_cloid,
            side,
            level: row.level as u32,
            price,
            size,
            sub_account: row.sub_account.or_else(|| {
                row.payload
                    .get("sub_account")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
            }),
        }))
    }

    /// Record one dispatch attempt and settle the item/slot state.
    #[allow(clippy::too_many_arguments)]
    async fn record_attempt_inner(
        &self,
        child: &QuoteDispatchChild,
        request_hash: &str,
        sent_at: DateTime<Utc>,
        responded_at: DateTime<Utc>,
        outcome: &str,
        status: &str,
        resolution: Option<&str>,
    ) -> Result<bool, HypeEdgeError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;

        let item_exists: Option<i64> = sqlx::query_scalar(
            "SELECT id FROM execution_command_items WHERE id = $1 FOR UPDATE",
        )
        .bind(child.item_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;
        let Some(item_id) = item_exists else {
            tx.commit().await.map_err(map_sqlx)?;
            return Ok(false);
        };

        // Insert the execution action attempt.
        sqlx::query(
            r#"
            INSERT INTO execution_actions (
                command_item_id, attempt, action_type, request_hash,
                sent_at, responded_at, outcome, estimated_credit_cost
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,1)
            "#,
        )
        .bind(item_id)
        .bind(child.attempt as i32)
        .bind(child.action.as_str())
        .bind(request_hash)
        .bind(sent_at)
        .bind(responded_at)
        .bind(outcome)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        let completed_at = if matches!(status, "pending" | "processing" | "unknown") {
            None
        } else {
            Some(responded_at)
        };
        sqlx::query(
            r#"
            UPDATE execution_command_items
            SET status = $2, resolution = $3, completed_at = $4,
                locked_at = NULL, locked_by = NULL, updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(item_id)
        .bind(status)
        .bind(resolution)
        .bind(completed_at)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        if child.action == ChildActionType::Place {
            self.link_placement_state(&mut tx, child, status, responded_at)
                .await?;
        }

        self.finish_parent_if_terminal(&mut tx, child.command_id, responded_at)
            .await?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(true)
    }

    /// Mark a child finished without sending (guard superseded/expired/blocked).
    async fn finish_without_send_inner(
        &self,
        child: &QuoteDispatchChild,
        decision: GuardDecision,
        completed_at: DateTime<Utc>,
    ) -> Result<(), HypeEdgeError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;
        let status: Option<String> = sqlx::query_scalar(
            "SELECT status FROM execution_command_items WHERE id = $1 FOR UPDATE",
        )
        .bind(child.item_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;
        let Some(current) = status else {
            tx.commit().await.map_err(map_sqlx)?;
            return Ok(());
        };
        if current != "processing" {
            tx.commit().await.map_err(map_sqlx)?;
            return Ok(());
        }

        let decision_str = decision.as_str();
        sqlx::query(
            r#"
            UPDATE execution_command_items
            SET status = $2, resolution = $3, completed_at = $4,
                locked_at = NULL, locked_by = NULL, updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(child.item_id)
        .bind(decision_str)
        .bind(format!("dispatch_guard_{decision_str}"))
        .bind(completed_at)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        // Release any active reservation for this item.
        sqlx::query(
            r#"
            UPDATE risk_reservations
            SET status = 'released', released_at = $2, updated_at = now()
            WHERE command_item_id = $1 AND status = 'active'
            "#,
        )
        .bind(child.item_id)
        .bind(completed_at)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        self.finish_parent_if_terminal(&mut tx, child.command_id, completed_at)
            .await?;
        tx.commit().await.map_err(map_sqlx)?;
        Ok(())
    }

    /// For a placement, link the target order, slot owner, and reservation.
    async fn link_placement_state(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        child: &QuoteDispatchChild,
        status: &str,
        responded_at: DateTime<Utc>,
    ) -> Result<(), HypeEdgeError> {
        let target_order_id: Option<Uuid> = match &child.target_cloid {
            Some(cloid) => sqlx::query_scalar("SELECT order_id FROM orders WHERE cloid = $1")
                .bind(cloid)
                .fetch_optional(&mut **tx)
                .await
                .map_err(map_sqlx)?,
            None => None,
        };
        let plan_item_id: Option<i64> = sqlx::query_scalar(
            "SELECT plan_item_id FROM execution_command_items WHERE id = $1",
        )
        .bind(child.item_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx)?;

        if let (Some(order_id), Some(plan_item_id)) = (target_order_id, plan_item_id) {
            sqlx::query(
                r#"
                UPDATE quote_plan_items
                SET target_order_id = $2, updated_at = now()
                WHERE id = $1
                "#,
            )
            .bind(plan_item_id)
            .bind(order_id)
            .execute(&mut **tx)
            .await
            .map_err(map_sqlx)?;
            sqlx::query(
                r#"
                UPDATE execution_command_items
                SET target_order_id = $2, updated_at = now()
                WHERE id = $1
                "#,
            )
            .bind(child.item_id)
            .bind(order_id)
            .execute(&mut **tx)
            .await
            .map_err(map_sqlx)?;

            // Slot owner projection (only when the plan revision is current).
            let slot: Option<(Uuid, i64)> = sqlx::query_as(
                r#"
                SELECT owner_order_id, plan_revision FROM quote_slots
                WHERE strategy_id = $1 AND symbol = $2 AND side = $3 AND level = $4
                FOR UPDATE
                "#,
            )
            .bind(&child.strategy_id)
            .bind(&child.symbol)
            .bind(child.side.as_str())
            .bind(child.level as i64)
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_sqlx)?;
            if let Some((_old_owner, plan_revision)) = slot
                && plan_revision == child.plan_revision
            {
                let slot_state = match status {
                    "unknown" => "unknown",
                    "succeeded" => "live",
                    _ => "empty",
                };
                sqlx::query(
                    r#"
                    UPDATE quote_slots
                    SET owner_order_id = $2, state = $3, revision = revision + 1,
                        updated_at = $4
                    WHERE strategy_id = $1 AND symbol = $2 AND side = $3 AND level = $4
                    "#,
                )
                .bind(&child.strategy_id)
                .bind(&child.symbol)
                .bind(child.side.as_str())
                .bind(child.level as i64)
                .bind(order_id)
                .bind(slot_state)
                .bind(responded_at)
                .execute(&mut **tx)
                .await
                .map_err(map_sqlx)?;
            }
        }

        // Reservation link / settle.
        let reservation: Option<(Uuid, String)> = sqlx::query_as(
            "SELECT order_id, status FROM risk_reservations WHERE command_item_id = $1 FOR UPDATE",
        )
        .bind(child.item_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx)?;
        if let Some((_, reservation_status)) = &reservation {
            if let Some(order_id) = target_order_id {
                sqlx::query(
                    "UPDATE risk_reservations SET order_id = $2, updated_at = now() WHERE command_item_id = $1",
                )
                .bind(child.item_id)
                .bind(order_id)
                .execute(&mut **tx)
                .await
                .map_err(map_sqlx)?;
            }
            if status != "pending" && reservation_status != "consumed" {
                let new_status = if matches!(status, "succeeded" | "unknown") {
                    "consumed"
                } else {
                    "released"
                };
                sqlx::query(
                    r#"
                    UPDATE risk_reservations
                    SET status = $2,
                        released_at = CASE WHEN $2 = 'released' THEN $3 ELSE released_at END,
                        updated_at = now()
                    WHERE command_item_id = $1
                    "#,
                )
                .bind(child.item_id)
                .bind(new_status)
                .bind(responded_at)
                .execute(&mut **tx)
                .await
                .map_err(map_sqlx)?;
            }
        }
        Ok(())
    }

    /// Settle the parent command once all its children are terminal.
    async fn finish_parent_if_terminal(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        command_id: Uuid,
        completed_at: DateTime<Utc>,
    ) -> Result<(), HypeEdgeError> {
        let statuses: Vec<String> =
            sqlx::query_scalar("SELECT status FROM execution_command_items WHERE command_id = $1")
                .bind(command_id)
                .fetch_all(&mut **tx)
                .await
                .map_err(map_sqlx)?;
        if statuses
            .iter()
            .any(|s| matches!(s.as_str(), "pending" | "processing"))
        {
            return Ok(());
        }
        let parent: Option<(String,)> = sqlx::query_as(
            "SELECT status FROM execution_commands WHERE command_id = $1 FOR UPDATE",
        )
        .bind(command_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx)?;
        if parent.is_none() {
            return Ok(());
        }
        let command_status = if statuses.iter().any(|s| s == "unknown") {
            "unknown"
        } else if !statuses.is_empty() && statuses.iter().all(|s| s == "succeeded") {
            "succeeded"
        } else {
            "failed"
        };
        sqlx::query(
            r#"
            UPDATE execution_commands
            SET status = $2, completed_at = $3, locked_at = NULL, locked_by = NULL,
                updated_at = now()
            WHERE command_id = $1
            "#,
        )
        .bind(command_id)
        .bind(command_status)
        .bind(completed_at)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }
}

#[async_trait]
impl QuotePlanStore for PostgresQuotePlanStore {
    async fn claim_child(
        &self,
        worker_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<QuoteDispatchChild>, HypeEdgeError> {
        self.claim_child_inner(worker_id, now).await
    }

    async fn record_attempt(
        &self,
        child: &QuoteDispatchChild,
        request_hash: &str,
        sent_at: DateTime<Utc>,
        responded_at: DateTime<Utc>,
        outcome: &str,
        status: &str,
        resolution: Option<&str>,
    ) -> Result<bool, HypeEdgeError> {
        self.record_attempt_inner(
            child,
            request_hash,
            sent_at,
            responded_at,
            outcome,
            status,
            resolution,
        )
        .await
    }

    async fn finish_without_send(
        &self,
        child: &QuoteDispatchChild,
        decision: GuardDecision,
        completed_at: DateTime<Utc>,
    ) -> Result<(), HypeEdgeError> {
        self.finish_without_send_inner(child, decision, completed_at).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigdecimal::BigDecimal;
    use crate::decimal_sqlx::dec_to_bd;

    #[test]
    fn row_types_are_from_row() {
        // Compile-time proof that the row types derive FromRow.
        fn _assert_from_row<T: for<'r> FromRow<'r, sqlx::postgres::PgRow>>() {}
        _assert_from_row::<QuotePlanChildRow>();
        assert_eq!(BigDecimal::from(0), BigDecimal::from(0));
    }

    #[test]
    fn child_action_type_mapping() {
        assert_eq!(ChildActionType::Place.as_str(), "place");
        assert_eq!(ChildActionType::Cancel.as_str(), "cancel");
        assert_eq!(ChildActionType::Modify.as_str(), "modify");
    }

    #[test]
    fn dec_to_bd_roundtrip() {
        let d = hypeedge_domain::Decimal::from_scaled(12345, 2);
        let bd = dec_to_bd(d);
        assert_eq!(bd_to_dec(bd).unwrap(), d);
    }
}
