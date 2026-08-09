//! Strategy base trait, port of `src/hypeedge/strategy/base.py`.
//!
//! Strategies submit orders through the injected `ExecutionClient` and receive
//! events sequentially via their runner.

use async_trait::async_trait;
use hypeedge_domain::enums::StrategyStatus;
use hypeedge_domain::events::{Event, EventType};

/// The strategy lifecycle contract (mirrors `StrategyBase`/`RunnableStrategy`).
#[async_trait]
pub trait Strategy: Send + Sync {
    /// Called once when the strategy starts.
    async fn on_start(&mut self) -> Result<(), String>;

    /// Called for each subscribed event.
    async fn on_event(&mut self, event: &Event) -> Result<(), String>;

    /// Called once when the strategy stops (cleanup).
    async fn on_stop(&mut self) -> Result<(), String>;

    /// The event types this strategy consumes.
    fn subscriptions(&self) -> Vec<EventType>;

    /// Current lifecycle status.
    fn status(&self) -> StrategyStatus;

    /// Set lifecycle status without tearing down the runner (pause/resume).
    fn set_status(&mut self, status: StrategyStatus);
}
