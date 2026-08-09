//! Postgres lease queue for the signed-action executor, port of
//! `PostgresExecutionCommandQueue` in `src/hypeedge/storage/postgres.py`.
//!
//! Commands are claimed with `FOR UPDATE SKIP LOCKED`; leases that expire are
//! reclassified as `unknown` so the executor resolves the exchange outcome by
//! cloid rather than blindly resending.

use chrono::Utc;
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::traits::DurableExecutionCommand;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::durable_order_store::map_sqlx;
use crate::rows::ExecutionCommandRow;

/// Postgres lease command queue.
pub struct PostgresExecutionCommandQueue {
    lease_seconds: i64,
    unknown_recheck_seconds: i64,
}

impl Default for PostgresExecutionCommandQueue {
    fn default() -> Self {
        Self::new(15, 5)
    }
}

impl PostgresExecutionCommandQueue {
    pub fn new(lease_seconds: i64, unknown_recheck_seconds: i64) -> Self {
        Self {
            lease_seconds,
            unknown_recheck_seconds,
        }
    }

    /// Claim one ready command with a lease. Expired leases are reclassified
    /// as unknown first.
    pub async fn claim(
        &self,
        pool: &sqlx::PgPool,
        worker_id: &str,
    ) -> Result<Option<DurableExecutionCommand>, HypeEdgeError> {
        let mut tx: Transaction<'_, Postgres> = pool.begin().await.map_err(map_sqlx)?;
        let now = Utc::now();
        let lease_cutoff = now - chrono::Duration::seconds(self.lease_seconds);

        // Expire leases.
        sqlx::query(
            r#"
            UPDATE execution_commands
            SET status = 'unknown', locked_at = NULL, locked_by = NULL,
                available_at = now(), last_error_code = 'processing_lease_expired',
                last_error_message = 'Worker lease expired; exchange outcome must be queried by cloid',
                updated_at = now()
            WHERE command_type IN ('place_order','cancel_order')
              AND status = 'processing'
              AND locked_at < $1
            "#,
        )
        .bind(lease_cutoff)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        // Claim one ready command.
        let record: Option<ExecutionCommandRow> = sqlx::query_as::<_, ExecutionCommandRow>(
            r#"
            SELECT * FROM execution_commands
            WHERE command_type IN ('place_order','cancel_order')
              AND status IN ('pending','unknown')
              AND available_at <= now()
            ORDER BY priority, created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        let Some(record) = record else {
            tx.commit().await.map_err(map_sqlx)?;
            return Ok(None);
        };

        let requires_resolution = record.status == "unknown";
        let attempt_count = record.attempt_count + 1;
        sqlx::query(
            r#"
            UPDATE execution_commands
            SET status = 'processing', locked_at = now(), locked_by = $2,
                attempt_count = $3, updated_at = now()
            WHERE command_id = $1
            "#,
        )
        .bind(record.command_id)
        .bind(worker_id)
        .bind(attempt_count)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        tx.commit().await.map_err(map_sqlx)?;

        Ok(Some(DurableExecutionCommand {
            command_id: record.command_id,
            command_type: record.command_type,
            payload: record.payload,
            attempt_count: attempt_count as u32,
            requires_resolution,
        }))
    }

    /// Mark a command's exchange outcome unknown; requeue after a recheck
    /// delay (port of `defer_unknown`).
    pub async fn defer_unknown(
        &self,
        pool: &sqlx::PgPool,
        command_id: Uuid,
        reason: &str,
    ) -> Result<(), HypeEdgeError> {
        let mut tx = pool.begin().await.map_err(map_sqlx)?;
        let available_at = Utc::now() + chrono::Duration::seconds(self.unknown_recheck_seconds);
        sqlx::query(
            r#"
            UPDATE execution_commands
            SET status = 'unknown', locked_at = NULL, locked_by = NULL,
                completed_at = NULL, available_at = $2,
                last_error_code = 'exchange_outcome_unknown', last_error_message = $3,
                updated_at = now()
            WHERE command_id = $1
            "#,
        )
        .bind(command_id)
        .bind(available_at)
        .bind(reason)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;
        tx.commit().await.map_err(map_sqlx)?;
        Ok(())
    }
}
