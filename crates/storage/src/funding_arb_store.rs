//! Postgres funding-arb cycle store (wiring, follow-up): durable create /
//! get-active / optimistic-revision transition for the funding-arb runtime.
//! Mirrors `src/hypeedge/storage/funding_arb.py` and the `funding_arb_cycles`
//! + `funding_arb_cycle_events` tables.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::Decimal;
use hypeedge_domain::enums::FundingArbCycleState;
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_trading::funding_arb::models::FundingArbCycle;
use hypeedge_trading::funding_arb::store::FundingArbCycleStore;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::decimal_sqlx::{bd_to_dec, dec_to_bd};

/// Postgres-backed [`FundingArbCycleStore`].
pub struct PostgresFundingArbCycleStore {
    pool: PgPool,
}

impl PostgresFundingArbCycleStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// A row of `funding_arb_cycles`.
#[derive(FromRow)]
struct CycleRow {
    cycle_id: Uuid,
    strategy_id: String,
    config_revision: i64,
    sub_account: String,
    perp_symbol: String,
    spot_symbol: String,
    spot_display: String,
    base_token: String,
    quote_token: String,
    state: String,
    target_perp_size: bigdecimal::BigDecimal,
    target_spot_size: bigdecimal::BigDecimal,
    perp_open_size: bigdecimal::BigDecimal,
    spot_open_size: bigdecimal::BigDecimal,
    baseline_spot_size: bigdecimal::BigDecimal,
    spot_entry_cloid: Option<String>,
    perp_entry_cloid: Option<String>,
    compensation_cloid: Option<String>,
    perp_exit_cloid: Option<String>,
    spot_exit_cloid: Option<String>,
    entry_funding_rate: bigdecimal::BigDecimal,
    entry_basis_bps: bigdecimal::BigDecimal,
    error_code: Option<String>,
    error_message: Option<String>,
    revision: i64,
    opened_at: Option<DateTime<Utc>>,
    closed_at: Option<DateTime<Utc>>,
    created_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
}

fn to_domain(row: CycleRow) -> Result<FundingArbCycle, HypeEdgeError> {
    let state = row
        .state
        .parse::<FundingArbCycleState>()
        .map_err(|_| HypeEdgeError::Postgres {
            message: format!("unknown funding-arb cycle state {}", row.state),
        })?;
    Ok(FundingArbCycle {
        cycle_id: row.cycle_id,
        strategy_id: row.strategy_id,
        config_revision: row.config_revision as u64,
        sub_account: row.sub_account,
        perp_symbol: row.perp_symbol,
        spot_symbol: row.spot_symbol,
        spot_display: row.spot_display,
        base_token: row.base_token,
        quote_token: row.quote_token,
        state,
        target_perp_size: bd_to_dec(row.target_perp_size).map_err(|e| HypeEdgeError::Postgres {
            message: e.to_string(),
        })?,
        target_spot_size: bd_to_dec(row.target_spot_size).map_err(|e| HypeEdgeError::Postgres {
            message: e.to_string(),
        })?,
        perp_open_size: bd_to_dec(row.perp_open_size).map_err(|e| HypeEdgeError::Postgres {
            message: e.to_string(),
        })?,
        spot_open_size: bd_to_dec(row.spot_open_size).map_err(|e| HypeEdgeError::Postgres {
            message: e.to_string(),
        })?,
        baseline_spot_size: bd_to_dec(row.baseline_spot_size).map_err(|e| {
            HypeEdgeError::Postgres {
                message: e.to_string(),
            }
        })?,
        entry_funding_rate: bd_to_dec(row.entry_funding_rate).map_err(|e| {
            HypeEdgeError::Postgres {
                message: e.to_string(),
            }
        })?,
        entry_basis_bps: bd_to_dec(row.entry_basis_bps).map_err(|e| HypeEdgeError::Postgres {
            message: e.to_string(),
        })?,
        revision: row.revision as u64,
        spot_entry_cloid: row.spot_entry_cloid,
        perp_entry_cloid: row.perp_entry_cloid,
        compensation_cloid: row.compensation_cloid,
        perp_exit_cloid: row.perp_exit_cloid,
        spot_exit_cloid: row.spot_exit_cloid,
        error_code: row.error_code,
        error_message: row.error_message,
        opened_at: row.opened_at,
        closed_at: row.closed_at,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

const CYCLE_COLUMNS: &str = "cycle_id, strategy_id, config_version_id, config_revision, sub_account, perp_symbol, \
     spot_symbol, spot_display, base_token, quote_token, state, target_perp_size, target_spot_size, \
     perp_open_size, spot_open_size, baseline_spot_size, spot_entry_cloid, perp_entry_cloid, \
     compensation_cloid, perp_exit_cloid, spot_exit_cloid, entry_funding_rate, entry_basis_bps, \
     error_code, error_message, revision, opened_at, closed_at, created_at, updated_at";

#[async_trait]
impl FundingArbCycleStore for PostgresFundingArbCycleStore {
    async fn create(&self, cycle: &FundingArbCycle) -> Result<FundingArbCycle, String> {
        let mut tx = self.pool.begin().await.map_err(|e| format!("begin: {e}"))?;
        sqlx::query(
            &format!(
                r#"
                INSERT INTO funding_arb_cycles (
                    {columns}
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30)
                "#,
                columns = CYCLE_COLUMNS
            ),
        )
        .bind(cycle.cycle_id)
        .bind(&cycle.strategy_id)
        .bind(cycle.config_revision as i64) // config_version_id: reuse the revision as the FK target
        .bind(cycle.config_revision as i64)
        .bind(&cycle.sub_account)
        .bind(&cycle.perp_symbol)
        .bind(&cycle.spot_symbol)
        .bind(&cycle.spot_display)
        .bind(&cycle.base_token)
        .bind(&cycle.quote_token)
        .bind(cycle.state.as_str())
        .bind(dec_to_bd(cycle.target_perp_size))
        .bind(dec_to_bd(cycle.target_spot_size))
        .bind(dec_to_bd(cycle.perp_open_size))
        .bind(dec_to_bd(cycle.spot_open_size))
        .bind(dec_to_bd(cycle.baseline_spot_size))
        .bind(&cycle.spot_entry_cloid)
        .bind(&cycle.perp_entry_cloid)
        .bind(&cycle.compensation_cloid)
        .bind(&cycle.perp_exit_cloid)
        .bind(&cycle.spot_exit_cloid)
        .bind(dec_to_bd(cycle.entry_funding_rate))
        .bind(dec_to_bd(cycle.entry_basis_bps))
        .bind(&cycle.error_code)
        .bind(&cycle.error_message)
        .bind((cycle.revision as i64) + 1) // row revision: the create is the first event
        .bind(cycle.opened_at)
        .bind(cycle.closed_at)
        .bind(Utc::now())
        .bind(Utc::now())
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("insert cycle: {e}"))?;
        // First event: the create, at revision 1.
        sqlx::query(
            r#"
            INSERT INTO funding_arb_cycle_events (event_id, cycle_id, revision, event_type, to_state, payload)
            VALUES ($1, $2, $3, 'cycle_created', $4, '{}')
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(cycle.cycle_id)
        .bind((cycle.revision as i64) + 1)
        .bind(cycle.state.as_str())
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("insert cycle event: {e}"))?;
        tx.commit().await.map_err(|e| format!("commit: {e}"))?;
        // The created cycle carries revision 1 (the create event).
        let mut created = cycle.clone();
        created.revision = cycle.revision + 1;
        Ok(created)
    }

    async fn get_active(&self, strategy_id: &str) -> Result<Option<FundingArbCycle>, String> {
        let row: Option<CycleRow> = sqlx::query_as::<_, CycleRow>(
            &format!(
                "SELECT {columns} FROM funding_arb_cycles WHERE strategy_id = $1 AND state <> 'closed' ORDER BY created_at DESC LIMIT 1",
                columns = CYCLE_COLUMNS
            ),
        )
        .bind(strategy_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("get_active: {e}"))?;
        row.map(to_domain).transpose().map_err(|e| e.to_string())
    }

    async fn transition(
        &self,
        cycle: &FundingArbCycle,
        state: FundingArbCycleState,
        event_type: &str,
        payload: Option<serde_json::Value>,
        updates: serde_json::Value,
    ) -> Result<FundingArbCycle, String> {
        let mut tx = self.pool.begin().await.map_err(|e| format!("begin: {e}"))?;
        let new_revision = (cycle.revision as i64) + 1;
        let now = Utc::now();

        // Optimistic revision: only apply if the persisted row is at the cycle's
        // revision (a stale handle must not clobber a newer transition).
        let updated = sqlx::query(
            r#"
            UPDATE funding_arb_cycles
            SET state = $1, revision = $2, perp_open_size = $3, spot_open_size = $4,
                spot_entry_cloid = COALESCE($5, spot_entry_cloid),
                perp_entry_cloid = COALESCE($6, perp_entry_cloid),
                compensation_cloid = COALESCE($7, compensation_cloid),
                perp_exit_cloid = COALESCE($8, perp_exit_cloid),
                spot_exit_cloid = COALESCE($9, spot_exit_cloid),
                error_code = $10, error_message = $11,
                opened_at = CASE WHEN $1 = 'open' THEN now() ELSE opened_at END,
                closed_at = CASE WHEN $1 IN ('closed','faulted') THEN now() ELSE closed_at END,
                updated_at = now()
            WHERE cycle_id = $12 AND revision = $13
            "#,
        )
        .bind(state.as_str())
        .bind(new_revision)
        .bind(dec_to_bd(
            updates
                .get("perp_open_size")
                .and_then(as_decimal)
                .unwrap_or(cycle.perp_open_size),
        ))
        .bind(dec_to_bd(
            updates
                .get("spot_open_size")
                .and_then(as_decimal)
                .unwrap_or(cycle.spot_open_size),
        ))
        .bind(updates.get("spot_entry_cloid").and_then(|v| v.as_str()))
        .bind(updates.get("perp_entry_cloid").and_then(|v| v.as_str()))
        .bind(updates.get("compensation_cloid").and_then(|v| v.as_str()))
        .bind(updates.get("perp_exit_cloid").and_then(|v| v.as_str()))
        .bind(updates.get("spot_exit_cloid").and_then(|v| v.as_str()))
        .bind(
            updates
                .get("error_code")
                .and_then(|v| v.as_str())
                .or(cycle.error_code.as_deref()),
        )
        .bind(
            updates
                .get("error_message")
                .and_then(|v| v.as_str())
                .or(cycle.error_message.as_deref()),
        )
        .bind(cycle.cycle_id)
        .bind(cycle.revision as i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("update cycle: {e}"))?;

        if updated.rows_affected() != 1 {
            // The persisted row moved past our revision (or was deleted) —
            // refuse to clobber it.
            return Err(format!(
                "funding-arb cycle {} revision mismatch (expected {}); refusing stale transition",
                cycle.cycle_id, cycle.revision
            ));
        }

        sqlx::query(
            r#"
            INSERT INTO funding_arb_cycle_events (event_id, cycle_id, revision, event_type, from_state, to_state, payload)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(cycle.cycle_id)
        .bind(new_revision)
        .bind(event_type)
        .bind(cycle.state.as_str())
        .bind(state.as_str())
        .bind(payload.unwrap_or_else(|| serde_json::json!({})))
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("insert event: {e}"))?;

        tx.commit().await.map_err(|e| format!("commit: {e}"))?;

        // Return the updated cycle.
        let mut updated_cycle = cycle.clone();
        updated_cycle.state = state;
        updated_cycle.revision = new_revision as u64;
        if let Some(v) = updates.get("perp_open_size").and_then(as_decimal) {
            updated_cycle.perp_open_size = v;
        }
        if let Some(v) = updates.get("spot_open_size").and_then(as_decimal) {
            updated_cycle.spot_open_size = v;
        }
        updated_cycle.error_code = updates
            .get("error_code")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or(cycle.error_code.clone());
        updated_cycle.error_message = updates
            .get("error_message")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or(cycle.error_message.clone());
        updated_cycle.updated_at = Some(now);
        if state == FundingArbCycleState::Open {
            updated_cycle.opened_at = Some(now);
        }
        if matches!(
            state,
            FundingArbCycleState::Closed | FundingArbCycleState::Faulted
        ) {
            updated_cycle.closed_at = Some(now);
        }
        Ok(updated_cycle)
    }
}

/// Read a `{symbol: "...", value: "..."}`-shaped or plain numeric update field as a
/// `Decimal` (the runtime writes `spot_open_size` as raw numbers in `updates`).
fn as_decimal(v: &serde_json::Value) -> Option<Decimal> {
    match v {
        serde_json::Value::String(s) => Decimal::from_str_lenient(s).ok(),
        serde_json::Value::Number(n) => n.as_f64().and_then(|f| Decimal::from_f64(f).ok()),
        _ => None,
    }
}
