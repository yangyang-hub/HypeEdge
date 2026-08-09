//! Trading quote models and the pure quote coordinator.
//!
//! Ports `src/hypeedge/trading/` (quotes + quote_coordinator). These are the
//! pure desired-vs-authoritative reconciliation used by the market-maker
//! runtime.

pub mod quote_coordinator;
pub mod quotes;

pub use quote_coordinator::{QuoteCoordinator, QuoteCoordinatorConfig};
pub use quotes::{
    DesiredQuote, DesiredQuoteSet, QuoteDiff, QuotePlan, QuoteRiskOwner, QuoteSlotKey,
    QuoteSlotView,
};
