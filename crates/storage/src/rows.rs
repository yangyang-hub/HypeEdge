//! `FromRow` structs for the transaction-critical Postgres tables.
//!
//! Only the tables touched by the execution / outbox / projection hot paths are
//! ported here; the rest of the schema is exercised through raw SQL. Column
//! names and types match `crates/storage/migrations/0001_create_all.sql`.

use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use sqlx::FromRow;
use uuid::Uuid;

/// `orders` — the durable order projection.
#[derive(Debug, Clone, FromRow)]
pub struct OrderRow {
    pub order_id: Uuid,
    pub command_id: Uuid,
    pub cloid: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub time_in_force: String,
    pub size: BigDecimal,
    pub price: Option<BigDecimal>,
    pub status: String,
    pub strategy_id: Option<String>,
    pub sub_account: Option<String>,
    pub reduce_only: bool,
    pub is_spot: bool,
    pub risk_reducing: bool,
    pub max_slippage_bps: i32,
    pub filled_size: BigDecimal,
    pub avg_fill_price: Option<BigDecimal>,
    pub revision: i64,
    pub error_message: Option<String>,
    pub exchange_oid: Option<String>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub filled_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// `risk_events` — one row per placement decision.
#[derive(Debug, Clone, FromRow)]
pub struct RiskEventRow {
    pub id: i64,
    pub command_id: Uuid,
    pub order_id: Uuid,
    pub sub_account: Option<String>,
    pub strategy_id: Option<String>,
    pub passed: bool,
    pub reason_code: Option<String>,
    pub reason: Option<String>,
    pub checked_limits: serde_json::Value,
    pub snapshot: serde_json::Value,
    pub duration_ms: i32,
    pub created_at: DateTime<Utc>,
}

/// `execution_commands` — the durable command queue row.
#[derive(Debug, Clone, FromRow)]
pub struct ExecutionCommandRow {
    pub command_id: Uuid,
    pub order_id: Uuid,
    pub command_type: String,
    pub actor_type: String,
    pub actor_id: String,
    pub idempotency_key: String,
    pub priority: i32,
    pub status: String,
    pub payload: serde_json::Value,
    pub attempt_count: i32,
    pub available_at: DateTime<Utc>,
    pub locked_at: Option<DateTime<Utc>>,
    pub locked_by: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// `risk_reservations` — the exposure-admission reservation row.
#[derive(Debug, Clone, FromRow)]
pub struct RiskReservationRow {
    pub id: i64,
    pub reservation_id: Uuid,
    pub command_id: Uuid,
    pub command_item_id: Option<Uuid>,
    pub risk_owner_type: Option<String>,
    pub risk_owner_key: Option<String>,
    pub order_id: Uuid,
    pub sub_account: Option<String>,
    pub strategy_id: Option<String>,
    pub symbol: String,
    pub side: String,
    pub reduce_only: bool,
    pub reserved_size: BigDecimal,
    pub reserved_notional: BigDecimal,
    pub status: String,
    pub expires_at: DateTime<Utc>,
    pub released_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// `outbox_events` — the SSE/EventBus replay source.
#[derive(Debug, Clone, FromRow)]
pub struct OutboxEventRow {
    pub sequence: i64,
    pub event_id: Uuid,
    pub event_type: String,
    pub schema_version: i32,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub aggregate_revision: i64,
    pub correlation_id: Option<String>,
    pub payload: serde_json::Value,
    pub occurred_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub claimed_by: Option<String>,
    pub publish_attempts: i64,
    pub last_publish_error: Option<String>,
}

/// `system_state` — the durable safety latch row.
#[derive(Debug, Clone, FromRow)]
pub struct SystemStateRow {
    pub state_key: String,
    pub state: String,
    pub kill_switch_active: bool,
    pub reason: Option<String>,
    pub triggered_by: Option<String>,
    pub triggered_at: Option<DateTime<Utc>>,
    pub last_reconciliation_id: Option<String>,
    pub metadata: serde_json::Value,
    pub revision: i64,
    pub updated_at: DateTime<Utc>,
}

/// `positions` — the exchange-authoritative position projection.
#[derive(Debug, Clone, FromRow)]
pub struct PositionRow {
    pub id: i64,
    pub sub_account: Option<String>,
    pub symbol: String,
    pub size: BigDecimal,
    pub entry_price: Option<BigDecimal>,
    pub mark_price: Option<BigDecimal>,
    pub unrealized_pnl: Option<BigDecimal>,
    pub realized_pnl: Option<BigDecimal>,
    pub leverage: i32,
    pub liquidation_price: Option<BigDecimal>,
    pub exchange_updated_at: DateTime<Utc>,
    pub revision: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// `account_state` — the account equity/leverage projection.
#[derive(Debug, Clone, FromRow)]
pub struct AccountStateRow {
    pub sub_account: Option<String>,
    pub equity: BigDecimal,
    pub available_balance: BigDecimal,
    pub total_margin_used: BigDecimal,
    pub total_unrealized_pnl: BigDecimal,
    pub peak_equity: BigDecimal,
    pub action_credits_remaining: i64,
    pub exchange_updated_at: DateTime<Utc>,
    pub reconciled_at: Option<DateTime<Utc>>,
    pub revision: i64,
    pub updated_at: DateTime<Utc>,
}

/// `fills` — the durable fill fact.
#[derive(Debug, Clone, FromRow)]
pub struct FillRow {
    pub fill_id: Uuid,
    pub order_id: Uuid,
    pub cloid: String,
    pub exchange_oid: Option<String>,
    pub symbol: String,
    pub side: String,
    pub price: BigDecimal,
    pub size: BigDecimal,
    pub fee: BigDecimal,
    pub realized_pnl: BigDecimal,
    pub is_maker: bool,
    pub is_spot: bool,
    pub strategy_id: Option<String>,
    pub sub_account: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub source: String,
    pub exchange_fill_id: String,
}
