//! Transactional outbox, port of `PostgresOutboxStore` in
//! `src/hypeedge/storage/outbox.py`. At-least-once delivery to the SSE broker:
//! events are claimed with a short `FOR UPDATE SKIP LOCKED` lease, published,
//! then marked published idempotently.

use chrono::Utc;
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::traits::DurableEvent;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::durable_order_store::map_sqlx;
use crate::rows::OutboxEventRow;

/// Postgres transactional outbox.
pub struct PostgresOutboxStore {
    lease_seconds: i64,
}

impl Default for PostgresOutboxStore {
    fn default() -> Self {
        Self::new(30)
    }
}

impl PostgresOutboxStore {
    pub fn new(lease_seconds: i64) -> Self {
        Self { lease_seconds }
    }

    /// Claim up to `limit` unpublished events with a short lease.
    pub async fn claim_batch(
        &self,
        pool: &sqlx::PgPool,
        worker_id: &str,
        limit: usize,
    ) -> Result<Vec<DurableEvent>, HypeEdgeError> {
        let mut tx: Transaction<'_, Postgres> = pool.begin().await.map_err(map_sqlx)?;
        let now = Utc::now();
        let lease_cutoff = now - chrono::Duration::seconds(self.lease_seconds);

        let records: Vec<OutboxEventRow> = sqlx::query_as::<_, OutboxEventRow>(
            r#"
            SELECT * FROM outbox_events
            WHERE published_at IS NULL
              AND (claimed_at IS NULL OR claimed_at < $1)
            ORDER BY sequence
            LIMIT $2
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(lease_cutoff)
        .bind(limit as i64)
        .fetch_all(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        for record in &records {
            sqlx::query(
                r#"
                UPDATE outbox_events
                SET claimed_at = now(), claimed_by = $2, publish_attempts = publish_attempts + 1,
                    last_publish_error = NULL
                WHERE sequence = $1
                "#,
            )
            .bind(record.sequence)
            .bind(worker_id)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx)?;
        }

        tx.commit().await.map_err(map_sqlx)?;

        Ok(records.into_iter().map(to_durable).collect())
    }

    /// Mark an event published; returns `false` if it was already published or
    /// claimed by another worker.
    pub async fn mark_published(
        &self,
        pool: &sqlx::PgPool,
        event: &DurableEvent,
        worker_id: &str,
    ) -> Result<bool, HypeEdgeError> {
        let result = sqlx::query(
            r#"
            UPDATE outbox_events
            SET published_at = now(), claimed_at = NULL, claimed_by = NULL,
                last_publish_error = NULL
            WHERE sequence = $1 AND event_id = $2 AND published_at IS NULL AND claimed_by = $3
            "#,
        )
        .bind(event.sequence)
        .bind(event.event_id)
        .bind(worker_id)
        .execute(pool)
        .await
        .map_err(map_sqlx)?;
        Ok(result.rows_affected() == 1)
    }

    /// Release a claim after a transient failure.
    pub async fn release_claim(
        &self,
        pool: &sqlx::PgPool,
        event: &DurableEvent,
        worker_id: &str,
        error: &str,
    ) -> Result<(), HypeEdgeError> {
        sqlx::query(
            r#"
            UPDATE outbox_events
            SET claimed_at = NULL, claimed_by = NULL, last_publish_error = $4
            WHERE sequence = $1 AND event_id = $2 AND published_at IS NULL AND claimed_by = $3
            "#,
        )
        .bind(event.sequence)
        .bind(event.event_id)
        .bind(worker_id)
        .bind(&error[..error.len().min(2000)])
        .execute(pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Read events strictly after `after_sequence` up to `up_to_sequence`
    /// (for SSE replay).
    pub async fn read_after(
        &self,
        pool: &sqlx::PgPool,
        after_sequence: i64,
        up_to_sequence: i64,
        limit: usize,
    ) -> Result<Vec<DurableEvent>, HypeEdgeError> {
        let rows = sqlx::query_as::<_, OutboxEventRow>(
            r#"
            SELECT * FROM outbox_events
            WHERE sequence > $1 AND sequence <= $2
            ORDER BY sequence
            LIMIT $3
            "#,
        )
        .bind(after_sequence)
        .bind(up_to_sequence)
        .bind(limit as i64)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx)?;
        Ok(rows.into_iter().map(to_durable).collect())
    }

    /// The min/max sequence bounds for SSE replay.
    pub async fn replay_bounds(
        &self,
        pool: &sqlx::PgPool,
    ) -> Result<(Option<i64>, Option<i64>), HypeEdgeError> {
        let row: (Option<i64>, Option<i64>) =
            sqlx::query_as("SELECT min(sequence), max(sequence) FROM outbox_events")
                .fetch_one(pool)
                .await
                .map_err(map_sqlx)?;
        Ok(row)
    }

    /// Append a control event idempotently (port of `append_control_event`).
    pub async fn append_control_event(
        &self,
        pool: &sqlx::PgPool,
        event_id: Uuid,
        event_type: &str,
        payload: serde_json::Value,
    ) -> Result<(), HypeEdgeError> {
        sqlx::query(
            r#"
            INSERT INTO outbox_events (event_id, event_type, aggregate_type, aggregate_id, aggregate_revision, correlation_id, payload)
            VALUES ($1,$2,'control','control',0,NULL,$3)
            ON CONFLICT (event_id) DO NOTHING
            "#,
        )
        .bind(event_id)
        .bind(event_type)
        .bind(payload)
        .execute(pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }
}

/// Map an outbox row to a `DurableEvent`.
pub fn to_durable(row: OutboxEventRow) -> DurableEvent {
    DurableEvent {
        sequence: row.sequence,
        event_id: row.event_id,
        event_type: row.event_type,
        schema_version: row.schema_version,
        aggregate_type: row.aggregate_type,
        aggregate_id: row.aggregate_id,
        aggregate_revision: row.aggregate_revision,
        correlation_id: row.correlation_id,
        payload: row.payload,
        occurred_at: row.occurred_at,
    }
}
