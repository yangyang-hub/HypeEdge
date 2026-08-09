//! Durable safety latch, port of `PostgresSystemStateStore` in
//! `src/hypeedge/storage/postgres.py`. Every transition writes an outbox event
//! (`system.safety.transitioned`) so the frontend's SSE stream reflects the
//! safety-mode change.

use chrono::Utc;
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::traits::DurableSystemState;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::durable_order_store::map_sqlx;
use crate::rows::SystemStateRow;

/// Postgres durable safety latch.
pub struct PostgresSystemStateStore;

impl Default for PostgresSystemStateStore {
    fn default() -> Self {
        Self
    }
}

impl PostgresSystemStateStore {
    /// Load the current trading safety state.
    pub async fn load(
        &self,
        pool: &sqlx::PgPool,
    ) -> Result<Option<DurableSystemState>, HypeEdgeError> {
        let row: Option<SystemStateRow> = sqlx::query_as::<_, SystemStateRow>(
            "SELECT * FROM system_state WHERE state_key = 'trading'",
        )
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx)?;
        Ok(row.map(|r| DurableSystemState {
            state: r.state,
            kill_switch_active: r.kill_switch_active,
            reason: r.reason,
        }))
    }

    /// Transition the safety state with an outbox event.
    pub async fn transition(
        &self,
        pool: &sqlx::PgPool,
        state: &str,
        reason: Option<&str>,
        kill_switch_active: bool,
        triggered_by: &str,
    ) -> Result<(), HypeEdgeError> {
        let mut tx: Transaction<'_, Postgres> = pool.begin().await.map_err(map_sqlx)?;
        let now = Utc::now();

        let existing: Option<SystemStateRow> = sqlx::query_as::<_, SystemStateRow>(
            "SELECT * FROM system_state WHERE state_key = 'trading' FOR UPDATE",
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        let revision = match &existing {
            Some(r) => r.revision + 1,
            None => 1,
        };

        if existing.is_some() {
            sqlx::query(
                r#"
                UPDATE system_state
                SET state = $2, revision = revision + 1, kill_switch_active = $3,
                    reason = $4, triggered_by = $5,
                    triggered_at = CASE WHEN $3 THEN now() ELSE triggered_at END,
                    updated_at = now()
                WHERE state_key = $1
                "#,
            )
            .bind("trading")
            .bind(state)
            .bind(kill_switch_active)
            .bind(reason)
            .bind(triggered_by)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx)?;
        } else {
            sqlx::query(
                r#"
                INSERT INTO system_state (state_key, state, revision, kill_switch_active, reason, triggered_by, triggered_at, updated_at)
                VALUES ('trading', $1, $2, $3, $4, $5, $6, now())
                "#,
            )
            .bind(state)
            .bind(revision)
            .bind(kill_switch_active)
            .bind(reason)
            .bind(triggered_by)
            .bind(if kill_switch_active { Some(now) } else { None })
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx)?;
        }

        // Outbox event for SSE.
        sqlx::query(
            r#"
            INSERT INTO outbox_events (event_id, event_type, aggregate_type, aggregate_id, aggregate_revision, payload)
            VALUES ($1, 'system.safety.transitioned', 'system_state', 'trading', $2, $3)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(revision)
        .bind(serde_json::json!({
            "state": state,
            "kill_switch_active": kill_switch_active,
            "reason": reason,
        }))
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(())
    }
}
