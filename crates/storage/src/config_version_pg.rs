//! Postgres implementation of the durable config-version repository, port of
//! the `MarketMakingRepository` config-version methods in
//! `src/hypeedge/storage/market_making.py`.
//!
//! One `strategy_config_versions` meta row + a typed strategy row joined on
//! `config_version_id`. Creation is idempotent by semantic hash (returns the
//! existing version on a duplicate) and bumps the strategy-instance revision
//! under `FOR UPDATE` as an optimistic lock.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hypeedge_domain::error::HypeEdgeError;
use sqlx::{PgPool, Postgres, Transaction};

use crate::config_version_store::{ConfigVersionRecord, ConfigVersionStore, config_hash};
use crate::durable_order_store::map_sqlx;

/// The Postgres config-version repository.
pub struct PostgresConfigVersionStore {
    pool: PgPool,
}

impl PostgresConfigVersionStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ConfigVersionStore for PostgresConfigVersionStore {
    async fn strategy_type(&self, strategy_id: &str) -> Result<Option<String>, HypeEdgeError> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT strategy_type FROM strategy_instances WHERE strategy_id = $1 AND archived_at IS NULL",
        )
        .bind(strategy_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(row.map(|(t,)| t))
    }

    async fn list_config_versions(
        &self,
        strategy_id: &str,
    ) -> Result<Vec<ConfigVersionRecord>, HypeEdgeError> {
        let rows: Vec<(i64, i64, String, Option<String>, Option<DateTime<Utc>>, serde_json::Value)> = sqlx::query_as(
            r#"
            SELECT v.id, v.version, v.config_hash, v.created_by, v.created_at,
                   COALESCE(tf.config::jsonb, mm.config::jsonb, fa.config::jsonb, '{}'::jsonb) AS config
            FROM strategy_config_versions v
            LEFT JOIN LATERAL (
                SELECT jsonb_object_agg(k, val) AS config FROM jsonb_each_text(
                    (SELECT to_jsonb(t) FROM trend_follow_config_versions t
                     WHERE t.config_version_id = v.id)
                ) AS x(k, val)
            ) tf ON true
            LEFT JOIN LATERAL (
                SELECT jsonb_object_agg(k, val) AS config FROM jsonb_each_text(
                    (SELECT to_jsonb(m) FROM market_maker_config_versions m
                     WHERE m.config_version_id = v.id)
                ) AS x(k, val)
            ) mm ON true
            LEFT JOIN LATERAL (
                SELECT jsonb_object_agg(k, val) AS config FROM jsonb_each_text(
                    (SELECT to_jsonb(f) FROM funding_arb_config_versions f
                     WHERE f.config_version_id = v.id)
                ) AS x(k, val)
            ) fa ON true
            WHERE v.strategy_id = $1
            ORDER BY v.version
            "#,
        )
        .bind(strategy_id)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        Ok(rows
            .into_iter()
            .map(
                |(_id, version, hash, created_by, created_at, values)| ConfigVersionRecord {
                    version: version as u64,
                    config_hash: hash,
                    created_by,
                    created_at,
                    values,
                },
            )
            .collect())
    }

    async fn create_config_version(
        &self,
        strategy_id: &str,
        strategy_type: &str,
        values: &serde_json::Value,
        created_by: &str,
        expected_revision: Option<u64>,
    ) -> Result<ConfigVersionRecord, HypeEdgeError> {
        let hash = config_hash(values);

        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;
        let instance: Option<(String, i64, Option<i64>)> = sqlx::query_as(
            "SELECT strategy_type, revision, desired_config_version_id FROM strategy_instances
             WHERE strategy_id = $1 AND archived_at IS NULL FOR UPDATE",
        )
        .bind(strategy_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;
        let Some((actual_type, actual_revision, _desired)) = instance else {
            return Err(HypeEdgeError::StrategyLifecycle {
                message: format!("Unknown active strategy instance: {strategy_id}"),
            });
        };
        if actual_type != strategy_type {
            return Err(HypeEdgeError::StrategyLifecycle {
                message: format!(
                    "create_{}_config_version requires strategy_type={strategy_type}",
                    actual_type
                ),
            });
        }
        if expected_revision.is_some_and(|expected| actual_revision as u64 != expected) {
            let expected = expected_revision.unwrap();
            return Err(HypeEdgeError::StrategyLifecycle {
                message: format!(
                    "Strategy revision conflict: expected={expected} actual={actual_revision}"
                ),
            });
        }

        // Idempotent by semantic hash: return the existing version.
        let existing: Option<ConfigVersionRow> = sqlx::query_as(
            r#"
            SELECT id, version, config_hash, created_by, created_at
            FROM strategy_config_versions
            WHERE strategy_id = $1 AND config_hash = $2
            "#,
        )
        .bind(strategy_id)
        .bind(&hash)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;
        if let Some(row) = existing {
            tx.commit().await.map_err(map_sqlx)?;
            return Ok(row.into_record(values.clone()));
        }

        let latest: (i64,) = sqlx::query_as(
            "SELECT COALESCE(MAX(version), 0) FROM strategy_config_versions WHERE strategy_id = $1",
        )
        .bind(strategy_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx)?;
        let next_version = latest.0 + 1;

        let inserted: ConfigVersionRow = sqlx::query_as(
            r#"
            INSERT INTO strategy_config_versions (strategy_id, version, config_hash, created_by)
            VALUES ($1, $2, $3, $4)
            RETURNING id, version, config_hash, created_by, created_at
            "#,
        )
        .bind(strategy_id)
        .bind(next_version)
        .bind(&hash)
        .bind(created_by)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        // Insert the typed config row (config_version_id = meta.id).
        write_typed_config(&mut tx, strategy_type, inserted.id, values).await?;

        // Bump the instance revision (optimistic lock already checked above).
        sqlx::query(
            "UPDATE strategy_instances SET revision = revision + 1, updated_at = now() WHERE strategy_id = $1",
        )
        .bind(strategy_id)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(inserted.into_record(values.clone()))
    }
}

#[derive(Debug, sqlx::FromRow)]
struct ConfigVersionRow {
    id: i64,
    version: i64,
    config_hash: String,
    created_by: Option<String>,
    created_at: Option<DateTime<Utc>>,
}

impl ConfigVersionRow {
    fn into_record(self, values: serde_json::Value) -> ConfigVersionRecord {
        ConfigVersionRecord {
            version: self.version as u64,
            config_hash: self.config_hash,
            created_by: self.created_by,
            created_at: self.created_at,
            values,
        }
    }
}

/// Write one typed config row for the strategy type. Mirrors the Python
/// `MarketMakerConfigVersionRecord(config_version_id=..., **normalized)`.
async fn write_typed_config(
    tx: &mut Transaction<'_, Postgres>,
    strategy_type: &str,
    config_version_id: i64,
    values: &serde_json::Value,
) -> Result<(), HypeEdgeError> {
    match strategy_type {
        "trend_follow" => {
            sqlx::query(
                r#"
                INSERT INTO trend_follow_config_versions (
                    config_version_id, fast_ema_period, slow_ema_period, signal_ema_period,
                    momentum_period, momentum_threshold, atr_period, atr_position_multiplier,
                    atr_stop_multiplier, max_position_pct, risk_per_trade_pct, macd_cross_threshold
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                "#,
            )
            .bind(config_version_id)
            .bind(int_field(values, "fast_ema_period"))
            .bind(int_field(values, "slow_ema_period"))
            .bind(int_field(values, "signal_ema_period"))
            .bind(int_field(values, "momentum_period"))
            .bind(dec_field(values, "momentum_threshold"))
            .bind(int_field(values, "atr_period"))
            .bind(dec_field(values, "atr_position_multiplier"))
            .bind(dec_field(values, "atr_stop_multiplier"))
            .bind(dec_field(values, "max_position_pct"))
            .bind(dec_field(values, "risk_per_trade_pct"))
            .bind(dec_field(values, "macd_cross_threshold"))
            .execute(&mut **tx)
            .await
            .map_err(map_sqlx)?;
        }
        "market_maker" => {
            sqlx::query(
                r#"
                INSERT INTO market_maker_config_versions (
                    config_version_id, soft_inventory_notional, hard_inventory_notional,
                    emergency_inventory_notional, quote_size, max_depth_participation,
                    inventory_skew_bps, max_inventory_shift_bps, min_half_spread_bps,
                    toxicity_spread_bps, min_expected_pnl_usdc, external_reference_weight,
                    external_max_age_seconds, external_outlier_bps, max_external_shift_ticks,
                    max_total_fair_shift_ticks, latency_risk_multiplier,
                    conservative_latency_seconds, conservative_markout_bps, min_markout_samples,
                    min_quote_lifetime_ms, refresh_cooldown_ms, max_quote_age_ms,
                    market_stale_after_ms, account_stale_after_ms
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)
                "#,
            )
            .bind(config_version_id)
            .bind(dec_field(values, "soft_inventory_notional"))
            .bind(dec_field(values, "hard_inventory_notional"))
            .bind(dec_field(values, "emergency_inventory_notional"))
            .bind(dec_field(values, "quote_size"))
            .bind(dec_field(values, "max_depth_participation"))
            .bind(dec_field(values, "inventory_skew_bps"))
            .bind(dec_field(values, "max_inventory_shift_bps"))
            .bind(dec_field(values, "min_half_spread_bps"))
            .bind(dec_field(values, "toxicity_spread_bps"))
            .bind(dec_field(values, "min_expected_pnl_usdc"))
            .bind(dec_field(values, "external_reference_weight"))
            .bind(dec_field(values, "external_max_age_seconds"))
            .bind(dec_field(values, "external_outlier_bps"))
            .bind(dec_field(values, "max_external_shift_ticks"))
            .bind(dec_field(values, "max_total_fair_shift_ticks"))
            .bind(dec_field(values, "latency_risk_multiplier"))
            .bind(dec_field(values, "conservative_latency_seconds"))
            .bind(dec_field(values, "conservative_markout_bps"))
            .bind(int_field(values, "min_markout_samples"))
            .bind(int_field(values, "min_quote_lifetime_ms"))
            .bind(int_field(values, "refresh_cooldown_ms"))
            .bind(int_field(values, "max_quote_age_ms"))
            .bind(int_field(values, "market_stale_after_ms"))
            .bind(int_field(values, "account_stale_after_ms"))
            .execute(&mut **tx)
            .await
            .map_err(map_sqlx)?;
        }
        "funding_arb" => {
            sqlx::query(
                r#"
                INSERT INTO funding_arb_config_versions (
                    config_version_id, spot_coin, entry_funding_rate, exit_funding_rate,
                    max_notional_usd, hedge_ratio, rebalance_threshold_bps, leverage,
                    max_slippage_bps, max_basis_bps, min_expected_edge_bps,
                    expected_hold_hours, round_trip_fee_bps, max_unhedged_seconds,
                    max_hold_hours
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                "#,
            )
            .bind(config_version_id)
            .bind(
                values
                    .get("spot_coin")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
            )
            .bind(dec_field(values, "entry_funding_rate"))
            .bind(dec_field(values, "exit_funding_rate"))
            .bind(dec_field(values, "max_notional_usd"))
            .bind(dec_field(values, "hedge_ratio"))
            .bind(int_field(values, "rebalance_threshold_bps"))
            .bind(dec_field(values, "leverage"))
            .bind(int_field(values, "max_slippage_bps"))
            .bind(int_field(values, "max_basis_bps"))
            .bind(dec_field(values, "min_expected_edge_bps"))
            .bind(int_field(values, "expected_hold_hours"))
            .bind(dec_field(values, "round_trip_fee_bps"))
            .bind(int_field(values, "max_unhedged_seconds"))
            .bind(int_field(values, "max_hold_hours"))
            .execute(&mut **tx)
            .await
            .map_err(map_sqlx)?;
        }
        other => {
            return Err(HypeEdgeError::StrategyLifecycle {
                message: format!("unsupported config strategy_type: {other}"),
            });
        }
    }
    Ok(())
}

/// Read an integer config field, defaulting to 0.
fn int_field(values: &serde_json::Value, key: &str) -> i64 {
    values.get(key).and_then(|v| v.as_i64()).unwrap_or(0)
}

/// Read a decimal config field into its `NUMERIC(38,18)` BigDecimal form.
fn dec_field(values: &serde_json::Value, key: &str) -> bigdecimal::BigDecimal {
    values
        .get(key)
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<bigdecimal::BigDecimal>().ok())
        .unwrap_or_else(|| bigdecimal::BigDecimal::from(0))
}
