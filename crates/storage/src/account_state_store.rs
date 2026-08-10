//! Postgres persistence for authoritative clearinghouse snapshots.
//!
//! The account-state poller keeps the live tracker in memory; this store
//! mirrors the same snapshot into `account_state` + `positions` so the
//! DB-enforced risk scope (`check_and_lock_risk_scope`) has durable facts to
//! lock and validate against.

use hypeedge_domain::decimal::Price;
use hypeedge_trading::account::account_health::{AccountSnapshotSink, PolledAccountSnapshot};
use sqlx::PgPool;
use uuid::Uuid;

use crate::decimal_sqlx::dec_to_bd;
use crate::durable_order_store::map_sqlx;

/// Postgres implementation of the clearinghouse snapshot sink.
pub struct PostgresAccountSnapshotStore {
    pool: PgPool,
}

impl PostgresAccountSnapshotStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl AccountSnapshotSink for PostgresAccountSnapshotStore {
    async fn persist(&self, snapshot: &PolledAccountSnapshot) -> Result<(), String> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(map_sqlx)
            .map_err(|e| e.to_string())?;
        let account = &snapshot.account_state;
        let sub_account = account.sub_account.as_deref();

        sqlx::query(
            r#"
            INSERT INTO account_state (
                sub_account, equity, available_balance, total_margin_used,
                total_unrealized_pnl, peak_equity, exchange_updated_at,
                reconciled_at, revision, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,now(),1,now())
            ON CONFLICT (sub_account) DO UPDATE SET
                equity = EXCLUDED.equity,
                available_balance = EXCLUDED.available_balance,
                total_margin_used = EXCLUDED.total_margin_used,
                total_unrealized_pnl = EXCLUDED.total_unrealized_pnl,
                peak_equity = EXCLUDED.peak_equity,
                exchange_updated_at = EXCLUDED.exchange_updated_at,
                reconciled_at = now(),
                revision = account_state.revision + 1,
                updated_at = now()
            "#,
        )
        .bind(sub_account)
        .bind(dec_to_bd(account.equity.inner()))
        .bind(dec_to_bd(account.available_balance.inner()))
        .bind(dec_to_bd(account.total_margin_used.inner()))
        .bind(dec_to_bd(account.total_unrealized_pnl.inner()))
        .bind(dec_to_bd(account.peak_equity.inner()))
        .bind(snapshot.received_at)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)
        .map_err(|e| e.to_string())?;

        // Replace the scope's position projection atomically. The DB risk
        // scope locks these rows during placements, so delete-then-insert is
        // serialized against order admission by the transaction itself.
        sqlx::query("DELETE FROM positions WHERE sub_account IS NOT DISTINCT FROM $1")
            .bind(sub_account)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx)
            .map_err(|e| e.to_string())?;

        for position in &snapshot.positions {
            let mark = position.mark_price.unwrap_or(Price::new(
                position
                    .entry_price
                    .map(|p| p.inner())
                    .unwrap_or(hypeedge_domain::Decimal::ZERO),
            ));
            sqlx::query(
                r#"
                INSERT INTO positions (
                    position_id, sub_account, symbol, size, entry_price, mark_price,
                    unrealized_pnl, leverage, liquidation_price, exchange_updated_at,
                    revision, created_at, updated_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,1,now(),now())
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(sub_account)
            .bind(&position.symbol)
            .bind(dec_to_bd(position.size.inner()))
            .bind(position.entry_price.map(|p| dec_to_bd(p.inner())))
            .bind(dec_to_bd(mark.inner()))
            .bind(position.unrealized_pnl.map(|u| dec_to_bd(u.inner())))
            .bind(position.leverage.max(1) as i32)
            .bind(position.liquidation_price.map(|p| dec_to_bd(p.inner())))
            .bind(snapshot.received_at)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx)
            .map_err(|e| e.to_string())?;
        }

        tx.commit()
            .await
            .map_err(map_sqlx)
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}
