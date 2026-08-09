//! Durable funding-arb cycle store protocol, port of
//! `src/hypeedge/storage/funding_arb.py` boundary.

use async_trait::async_trait;

use super::models::FundingArbCycle;
use hypeedge_domain::enums::FundingArbCycleState;

/// The durable cycle boundary the funding-arb runtime drives.
#[async_trait]
pub trait FundingArbCycleStore: Send + Sync {
    async fn create(&self, cycle: &FundingArbCycle) -> Result<FundingArbCycle, String>;
    async fn get_active(&self, strategy_id: &str) -> Result<Option<FundingArbCycle>, String>;
    /// Optimistic-revision transition; returns the updated cycle.
    async fn transition(
        &self,
        cycle: &FundingArbCycle,
        state: FundingArbCycleState,
        event_type: &str,
        payload: Option<serde_json::Value>,
        updates: serde_json::Value,
    ) -> Result<FundingArbCycle, String>;
}
