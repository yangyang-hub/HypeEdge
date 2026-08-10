//! Durable-boundary traits mirroring the Python `Protocol` boundaries.
//!
//! The `trading` crate depends only on these traits (never on a concrete
//! storage implementation), so the execution engine, risk checker, and
//! strategy runtimes are unit-testable against in-memory fakes. The `app`
//! crate performs the final `Arc<dyn …>` wiring to the Postgres
//! implementations in `storage`.

use async_trait::async_trait;
use uuid::Uuid;

use crate::decimal::Decimal;
use crate::error::HypeEdgeError;
use crate::models::{Order, OrderIntent, RiskCheckResult};

/// The interface strategies use to submit orders (design doc §9). Strategies
/// never access the execution engine directly.
#[async_trait]
pub trait ExecutionClient: Send + Sync {
    /// Submit an order intent. Returns the created `Order`.
    async fn submit_order(
        &self,
        intent: OrderIntent,
        deferred: Option<bool>,
    ) -> Result<Order, HypeEdgeError>;

    /// Cancel an order by cloid. Returns `true` if cancellation was accepted.
    async fn cancel_order(&self, cloid: &str) -> Result<bool, HypeEdgeError>;

    /// Cancel all open orders, optionally filtered by symbol. Returns count cancelled.
    async fn cancel_all_orders(&self, symbol: Option<&str>) -> Result<u64, HypeEdgeError>;

    /// Get current order state by cloid.
    async fn get_order(&self, cloid: &str) -> Result<Option<Order>, HypeEdgeError>;

    /// Get all open (non-terminal) orders.
    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>, HypeEdgeError>;

    /// Refresh an authenticated/durable order projection by cloid. The default
    /// falls back to `get_order`; the execution engine overrides this with the
    /// durable Postgres read (used by the funding-arb leg wait loop).
    async fn refresh_order_from_durable(
        &self,
        cloid: &str,
    ) -> Result<Option<Order>, HypeEdgeError> {
        self.get_order(cloid).await
    }

    /// Set per-symbol leverage through the serial signing boundary. The default
    /// rejects; the execution engine overrides this.
    async fn update_leverage(
        &self,
        _symbol: &str,
        _leverage: u32,
        _is_cross: bool,
    ) -> Result<serde_json::Value, HypeEdgeError> {
        Err(HypeEdgeError::Execution {
            message: "update_leverage not implemented".into(),
        })
    }
}

/// Latest-state facade over market data consumed by risk and execution
/// (mirrors `MarketDataProvider` in `market_data/provider.py`).
#[async_trait]
pub trait MarketDataProvider: Send + Sync {
    /// Current price snapshot (mid price, falling back to the book) used for
    /// stale-price checks.
    async fn get_price_snapshot(
        &self,
        symbol: &str,
    ) -> Result<Option<crate::models::MidPrice>, HypeEdgeError>;

    /// Top-of-book (best_bid, best_ask) for the symbol, used by the order
    /// normalizer's post-only-crossing check. `None` when the book is empty or
    /// the symbol is unknown.
    async fn get_best_bid_ask(
        &self,
        symbol: &str,
    ) -> Result<Option<(crate::decimal::Decimal, crate::decimal::Decimal)>, HypeEdgeError>;
}

/// A durable command queued for the signed-action executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableExecutionCommand {
    pub command_id: Uuid,
    pub command_type: String,
    /// Payload is the serialized intent; exact shape is storage-specific.
    pub payload: serde_json::Value,
    pub attempt_count: u32,
    /// `true` when the command was found in an unknown/lease-expired state and
    /// must be resolved by cloid query before resending.
    pub requires_resolution: bool,
}

/// The transactional boundary that persists placements atomically
/// (mirrors `DurableOrderStore` protocol in `execution/durable.py` and
/// `PostgresDurableOrderStore`).
#[async_trait]
pub trait DurableOrderStore: Send + Sync {
    /// Persist a placement (order + risk event + command + reservation +
    /// events) in one transaction. Returns the effective risk result (which
    /// may differ from the supplied one when the DB-level scope check runs).
    async fn persist_placement(
        &self,
        order: &Order,
        risk_result: &RiskCheckResult,
        command_id: Uuid,
        dispatch: bool,
        reference_price: Option<Decimal>,
    ) -> Result<RiskCheckResult, HypeEdgeError>;

    /// Persist an order state transition (plus optional command status and
    /// reservation release).
    async fn persist_transition(
        &self,
        order: &Order,
        event_type: &str,
        command_id: Option<Uuid>,
        command_status: Option<&str>,
    ) -> Result<(), HypeEdgeError>;

    /// Persist a cancel request command.
    async fn persist_cancel_requested(
        &self,
        order: &Order,
        command_id: Uuid,
    ) -> Result<(), HypeEdgeError>;

    /// Upsert an exchange-discovered order before any cancel side effect
    /// (port of `PostgresDurableOrderStore.persist_reconciled_order`).
    async fn persist_reconciled_order(&self, order: &Order) -> Result<(), HypeEdgeError>;

    /// Load all open (non-terminal) orders.
    async fn load_open_orders(&self) -> Result<Vec<Order>, HypeEdgeError>;

    /// Load an order by cloid.
    async fn get_order(&self, cloid: &str) -> Result<Option<Order>, HypeEdgeError>;
}

/// The durable command queue (mirrors `DurableCommandQueue`).
#[async_trait]
pub trait DurableCommandQueue: Send + Sync {
    /// Claim one ready command with a lease; reclassifies expired leases as
    /// unknown.
    async fn claim(
        &self,
        worker_id: &str,
    ) -> Result<Option<DurableExecutionCommand>, HypeEdgeError>;

    /// Mark a command's exchange outcome unknown; requeue after a recheck
    /// delay.
    async fn defer_unknown(&self, command_id: Uuid, reason: &str) -> Result<(), HypeEdgeError>;
}

/// A durable outbox event for SSE replay (mirrors `DurableEvent`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableEvent {
    pub sequence: i64,
    pub event_id: Uuid,
    pub event_type: String,
    pub schema_version: i32,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub aggregate_revision: i64,
    pub correlation_id: Option<String>,
    pub payload: serde_json::Value,
    pub occurred_at: chrono::DateTime<chrono::Utc>,
}

/// The transactional outbox (mirrors `PostgresOutboxStore`).
#[async_trait]
pub trait DurableOutboxStore: Send + Sync {
    /// Claim up to `limit` unpublished events with a short lease.
    async fn claim_batch(
        &self,
        worker_id: &str,
        limit: usize,
    ) -> Result<Vec<DurableEvent>, HypeEdgeError>;

    /// Mark an event published; returns `false` if it was already claimed by
    /// another worker or already published.
    async fn mark_published(
        &self,
        event: &DurableEvent,
        worker_id: &str,
    ) -> Result<bool, HypeEdgeError>;

    /// Release a claim after a transient failure.
    async fn release_claim(
        &self,
        event: &DurableEvent,
        worker_id: &str,
        error: &str,
    ) -> Result<(), HypeEdgeError>;

    /// Read events strictly after `after_sequence` up to `up_to_sequence`
    /// (for SSE replay).
    async fn read_after(
        &self,
        after_sequence: i64,
        up_to_sequence: i64,
        limit: usize,
    ) -> Result<Vec<DurableEvent>, HypeEdgeError>;
}

/// Durable safety latch (mirrors `PostgresSystemStateStore`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableSystemState {
    pub state: String,
    pub kill_switch_active: bool,
    pub reason: Option<String>,
}

#[async_trait]
pub trait SystemStateStore: Send + Sync {
    async fn load(&self) -> Result<Option<DurableSystemState>, HypeEdgeError>;
    async fn transition(
        &self,
        state: &str,
        reason: Option<&str>,
        kill_switch_active: bool,
        triggered_by: &str,
    ) -> Result<(), HypeEdgeError>;
}
