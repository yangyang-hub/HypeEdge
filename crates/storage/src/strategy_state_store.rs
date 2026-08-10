//! Postgres implementation of the strategy control-plane state store.
//!
//! The previous control plane was in-memory only, so every strategy instance
//! and runtime state disappeared on restart. This store maps the domain
//! boundary onto the `strategy_instances`, `strategy_runtime_state`, and
//! `strategy_config_versions` tables that already exist in the schema.

use async_trait::async_trait;
use hypeedge_domain::enums::MarketMakerLifecycle;
use hypeedge_trading::strategy::{
    StrategyConfigSnapshot, StrategyInstanceDefinition, StrategyRuntimeState, StrategyStateStore,
};
use sqlx::{FromRow, PgPool};

use crate::config_version_pg::PostgresConfigVersionStore;
use crate::config_version_store::ConfigVersionStore;
use crate::durable_order_store::map_sqlx;

/// Postgres-backed [`StrategyStateStore`].
pub struct PostgresStrategyStateStore {
    pool: PgPool,
}

impl PostgresStrategyStateStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(FromRow)]
struct InstanceRow {
    strategy_id: String,
    strategy_type: String,
    sub_account: Option<String>,
    symbol: String,
    desired_state: String,
    desired_config_version_id: Option<i64>,
    revision: i64,
}

impl InstanceRow {
    fn into_domain(self) -> Result<StrategyInstanceDefinition, String> {
        Ok(StrategyInstanceDefinition {
            strategy_id: self.strategy_id,
            strategy_type: self.strategy_type,
            sub_account: self.sub_account.unwrap_or_default(),
            symbol: self.symbol,
            desired_state: self
                .desired_state
                .parse::<MarketMakerLifecycle>()
                .map_err(|_| format!("invalid desired_state {}", self.desired_state))?,
            desired_config_revision: self.desired_config_version_id.unwrap_or(1).max(1) as u64,
            revision: self.revision.max(0) as u64,
        })
    }
}

#[async_trait]
impl StrategyStateStore for PostgresStrategyStateStore {
    async fn upsert_instance(&self, instance: &StrategyInstanceDefinition) -> Result<(), String> {
        sqlx::query(
            r#"
            INSERT INTO strategy_instances (
                strategy_id, strategy_type, sub_account, symbol, desired_state,
                desired_config_version_id, revision, created_at, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,0,now(),now())
            ON CONFLICT (strategy_id) DO UPDATE SET
                strategy_type = EXCLUDED.strategy_type,
                sub_account = EXCLUDED.sub_account,
                symbol = EXCLUDED.symbol,
                desired_state = EXCLUDED.desired_state,
                desired_config_version_id = EXCLUDED.desired_config_version_id,
                revision = strategy_instances.revision + 1,
                updated_at = now()
            "#,
        )
        .bind(&instance.strategy_id)
        .bind(&instance.strategy_type)
        .bind(if instance.sub_account.is_empty() {
            None
        } else {
            Some(instance.sub_account.as_str())
        })
        .bind(&instance.symbol)
        .bind(instance.desired_state.as_str())
        .bind(instance.desired_config_revision as i64)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn upsert_config(&self, config: &StrategyConfigSnapshot) -> Result<(), String> {
        let instance = self
            .get_instance(&config.strategy_id)
            .await?
            .ok_or_else(|| format!("unknown strategy {}", config.strategy_id))?;
        PostgresConfigVersionStore::new(self.pool.clone())
            .create_config_version(
                &config.strategy_id,
                &instance.strategy_type,
                &config.values,
                "api",
                None,
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn list_instances(&self) -> Result<Vec<StrategyInstanceDefinition>, String> {
        let rows: Vec<InstanceRow> = sqlx::query_as(
            r#"
            SELECT strategy_id, strategy_type, sub_account, symbol, desired_state,
                   desired_config_version_id, revision
            FROM strategy_instances
            WHERE archived_at IS NULL
            ORDER BY created_at
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)
        .map_err(|e| e.to_string())?;
        rows.into_iter().map(InstanceRow::into_domain).collect()
    }

    async fn get_instance(
        &self,
        strategy_id: &str,
    ) -> Result<Option<StrategyInstanceDefinition>, String> {
        let row: Option<InstanceRow> = sqlx::query_as(
            r#"
            SELECT strategy_id, strategy_type, sub_account, symbol, desired_state,
                   desired_config_version_id, revision
            FROM strategy_instances
            WHERE strategy_id = $1 AND archived_at IS NULL
            "#,
        )
        .bind(strategy_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)
        .map_err(|e| e.to_string())?;
        row.map(InstanceRow::into_domain).transpose()
    }

    async fn get_runtime(&self, strategy_id: &str) -> Result<Option<StrategyRuntimeState>, String> {
        let row: Option<(String, String, Option<i64>, Option<String>, i64)> = sqlx::query_as(
            r#"
            SELECT strategy_id, actual_state, effective_config_version_id, reason, revision
            FROM strategy_runtime_state
            WHERE strategy_id = $1
            "#,
        )
        .bind(strategy_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)
        .map_err(|e| e.to_string())?;
        row.map(
            |(strategy_id, actual_state, effective_config_revision, reason, revision)| {
                Ok(StrategyRuntimeState {
                    strategy_id,
                    actual_state: actual_state
                        .parse::<MarketMakerLifecycle>()
                        .map_err(|_| format!("invalid actual_state {actual_state}"))?,
                    effective_config_revision: effective_config_revision.map(|v| v as u64),
                    revision: revision.max(0) as u64,
                    reason,
                })
            },
        )
        .transpose()
    }

    async fn get_config(
        &self,
        strategy_id: &str,
        revision: u64,
    ) -> Result<Option<StrategyConfigSnapshot>, String> {
        let versions = PostgresConfigVersionStore::new(self.pool.clone())
            .list_config_versions(strategy_id)
            .await
            .map_err(|e| e.to_string())?;
        Ok(versions
            .into_iter()
            .find(|v| v.version == revision)
            .map(|v| StrategyConfigSnapshot {
                strategy_id: strategy_id.to_string(),
                revision,
                values: v.values,
            }))
    }

    async fn set_desired(
        &self,
        strategy_id: &str,
        state: Option<MarketMakerLifecycle>,
        config_revision: Option<u64>,
        expected_revision: Option<u64>,
    ) -> Result<StrategyInstanceDefinition, String> {
        let result = sqlx::query(
            r#"
            UPDATE strategy_instances
            SET desired_state = COALESCE($2, desired_state),
                desired_config_version_id = COALESCE($3, desired_config_version_id),
                revision = revision + 1,
                updated_at = now()
            WHERE strategy_id = $1
              AND ($4::bigint IS NULL OR revision = $4)
            "#,
        )
        .bind(strategy_id)
        .bind(state.map(|s| s.as_str()))
        .bind(config_revision.map(|v| v as i64))
        .bind(expected_revision.map(|v| v as i64))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)
        .map_err(|e| e.to_string())?;
        if result.rows_affected() != 1 {
            return Err(format!(
                "strategy {strategy_id} missing or revision conflict (expected {expected_revision:?})"
            ));
        }
        self.get_instance(strategy_id)
            .await?
            .ok_or_else(|| format!("unknown strategy {strategy_id}"))
    }

    async fn set_runtime(
        &self,
        strategy_id: &str,
        actual_state: Option<MarketMakerLifecycle>,
        effective_config_revision: Option<u64>,
        set_effective_config: bool,
        reason: Option<&str>,
        expected_revision: Option<u64>,
    ) -> Result<StrategyRuntimeState, String> {
        if let Some(expected) = expected_revision {
            let current: Option<(i64,)> = sqlx::query_as(
                "SELECT revision FROM strategy_runtime_state WHERE strategy_id = $1",
            )
            .bind(strategy_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)
            .map_err(|e| e.to_string())?;
            if current.map(|(r,)| r as u64) != Some(expected) {
                return Err(format!(
                    "strategy {strategy_id} runtime revision conflict: expected {expected}"
                ));
            }
        }
        sqlx::query(
            r#"
            INSERT INTO strategy_runtime_state (
                strategy_id, actual_state, effective_config_version_id, heartbeat_at,
                revision, reason, updated_at
            ) VALUES ($1,$2,$3,now(),1,$4,now())
            ON CONFLICT (strategy_id) DO UPDATE SET
                actual_state = COALESCE($2, strategy_runtime_state.actual_state),
                effective_config_version_id = CASE WHEN $5 THEN $3 ELSE strategy_runtime_state.effective_config_version_id END,
                heartbeat_at = now(),
                reason = $4,
                revision = strategy_runtime_state.revision + 1,
                updated_at = now()
            "#,
        )
        .bind(strategy_id)
        .bind(actual_state.map(|s| s.as_str()))
        .bind(effective_config_revision.map(|v| v as i64))
        .bind(reason)
        .bind(set_effective_config)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)
        .map_err(|e| e.to_string())?;
        self.get_runtime(strategy_id)
            .await?
            .ok_or_else(|| format!("missing runtime {strategy_id}"))
    }
}
