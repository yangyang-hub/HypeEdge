//! Market-maker strategy core: pure models, fair value, inventory, policy, and
//! estimators.
//!
//! Ports `src/hypeedge/strategy/market_maker/` (the pure, testable parts). The
//! coalesced runtime loop and its providers land in a follow-up increment.

pub mod adapters;
pub mod estimators;
pub mod fair_value;
pub mod inventory;
pub mod models;
pub mod policy;
pub mod runtime;
pub mod shadow;

pub use estimators::{AdverseMarkoutEstimator, DecisionLatencyEstimator, MarkoutEstimate};
pub use fair_value::FairValueModel;
pub use inventory::{InventoryController, InventoryDecision};
pub use models::{ActionBudgetSnapshot, InventorySnapshot, MarketFeatures, MarketMakerConfig};
pub use policy::MarketMakerPolicy;
pub use runtime::{
    MarketMakerRuntime, MarketMakerRuntimeFactory, MarketMakerRuntimeHandle,
    MarketMakerRuntimeSnapshot, QuoteCancelRequest, QuotePlanCommandClient,
};
pub use shadow::{ShadowActionEstimate, ShadowOrderState};
