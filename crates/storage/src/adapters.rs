//! Pool-holding adapters that implement the domain durable-boundary traits, so
//! the app can construct `Arc<dyn DurableOrderStore>` / `DurableCommandQueue` /
//! `DurableOutboxStore` from Postgres (wiring, 6a). The raw stores keep
//! pool-per-call methods for the integration tests; these wrappers own the pool
//! and delegate. `persist_placement` clones the order because the raw store
//! takes `&mut Order` (it mutates `status` on a DB-scope rejection, which the
//! caller already mirrors from the returned `RiskCheckResult`).

use async_trait::async_trait;
use hypeedge_domain::decimal::Decimal;
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::models::{Order, RiskCheckResult, RiskLimits};
use hypeedge_domain::traits::{
    DurableCommandQueue, DurableEvent, DurableExecutionCommand, DurableOrderStore,
    DurableOutboxStore,
};
use sqlx::PgPool;
use uuid::Uuid;

use crate::command_queue::PostgresExecutionCommandQueue;
use crate::durable_order_store::PostgresDurableOrderStore;
use crate::outbox::PostgresOutboxStore;

/// Pool-holding [`DurableOrderStore`] adapter.
pub struct PooledDurableOrderStore {
    pool: PgPool,
    inner: PostgresDurableOrderStore,
}

impl PooledDurableOrderStore {
    pub fn new(
        pool: PgPool,
        risk_limits: Option<RiskLimits>,
        account_stale_seconds: f64,
        reservation_ttl_seconds: i64,
    ) -> Self {
        Self {
            pool,
            inner: PostgresDurableOrderStore::new(
                risk_limits,
                account_stale_seconds,
                reservation_ttl_seconds,
            ),
        }
    }
}

#[async_trait]
impl DurableOrderStore for PooledDurableOrderStore {
    async fn persist_placement(
        &self,
        order: &Order,
        risk_result: &RiskCheckResult,
        command_id: Uuid,
        dispatch: bool,
        reference_price: Option<Decimal>,
    ) -> Result<RiskCheckResult, HypeEdgeError> {
        let mut clone = order.clone();
        self.inner
            .persist_placement(
                &self.pool,
                &mut clone,
                risk_result,
                command_id,
                dispatch,
                reference_price,
            )
            .await
    }

    async fn persist_transition(
        &self,
        order: &Order,
        event_type: &str,
        command_id: Option<Uuid>,
        command_status: Option<&str>,
    ) -> Result<(), HypeEdgeError> {
        self.inner
            .persist_transition(&self.pool, order, event_type, command_id, command_status)
            .await
    }

    async fn persist_cancel_requested(
        &self,
        order: &Order,
        command_id: Uuid,
    ) -> Result<(), HypeEdgeError> {
        self.inner
            .persist_cancel_requested(&self.pool, order, command_id)
            .await
    }

    async fn persist_reconciled_order(&self, order: &Order) -> Result<(), HypeEdgeError> {
        self.inner.persist_reconciled_order(&self.pool, order).await
    }

    async fn load_open_orders(&self) -> Result<Vec<Order>, HypeEdgeError> {
        self.inner.load_open_orders(&self.pool).await
    }

    async fn get_order(&self, cloid: &str) -> Result<Option<Order>, HypeEdgeError> {
        self.inner.get_order(&self.pool, cloid).await
    }
}

/// Pool-holding [`DurableCommandQueue`] adapter.
pub struct PooledExecutionCommandQueue {
    pool: PgPool,
    inner: PostgresExecutionCommandQueue,
}

impl PooledExecutionCommandQueue {
    pub fn new(pool: PgPool, lease_seconds: i64, unknown_recheck_seconds: i64) -> Self {
        Self {
            pool,
            inner: PostgresExecutionCommandQueue::new(lease_seconds, unknown_recheck_seconds),
        }
    }
}

#[async_trait]
impl DurableCommandQueue for PooledExecutionCommandQueue {
    async fn claim(
        &self,
        worker_id: &str,
    ) -> Result<Option<DurableExecutionCommand>, HypeEdgeError> {
        self.inner.claim(&self.pool, worker_id).await
    }

    async fn defer_unknown(&self, command_id: Uuid, reason: &str) -> Result<(), HypeEdgeError> {
        self.inner.defer_unknown(&self.pool, command_id, reason).await
    }
}

/// Pool-holding [`DurableOutboxStore`] adapter.
pub struct PooledOutboxStore {
    pool: PgPool,
    inner: PostgresOutboxStore,
}

impl PooledOutboxStore {
    pub fn new(pool: PgPool, lease_seconds: i64) -> Self {
        Self {
            pool,
            inner: PostgresOutboxStore::new(lease_seconds),
        }
    }
}

#[async_trait]
impl DurableOutboxStore for PooledOutboxStore {
    async fn claim_batch(
        &self,
        worker_id: &str,
        limit: usize,
    ) -> Result<Vec<DurableEvent>, HypeEdgeError> {
        self.inner.claim_batch(&self.pool, worker_id, limit).await
    }

    async fn mark_published(
        &self,
        event: &DurableEvent,
        worker_id: &str,
    ) -> Result<bool, HypeEdgeError> {
        self.inner.mark_published(&self.pool, event, worker_id).await
    }

    async fn release_claim(
        &self,
        event: &DurableEvent,
        worker_id: &str,
        error: &str,
    ) -> Result<(), HypeEdgeError> {
        self.inner
            .release_claim(&self.pool, event, worker_id, error)
            .await
    }

    async fn read_after(
        &self,
        after_sequence: i64,
        up_to_sequence: i64,
        limit: usize,
    ) -> Result<Vec<DurableEvent>, HypeEdgeError> {
        self.inner
            .read_after(&self.pool, after_sequence, up_to_sequence, limit)
            .await
    }
}
