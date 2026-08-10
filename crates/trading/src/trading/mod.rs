//! Trading quote models, the pure quote coordinator, and the fail-closed
//! trading-command admission service.
//!
//! Ports `src/hypeedge/trading/` (quotes + quote_coordinator +
//! command_service). These are the pure desired-vs-authoritative
//! reconciliation used by the market-maker runtime, and the command boundary
//! that admits every placement before persistence.

pub mod command_service;
pub mod quote_coordinator;
pub mod quotes;

pub use command_service::{
    ActionBudgetControllerAdapter, DataHealthDecision, DurableTradingCommandSink, GateDecision,
    InMemoryTradingCommandSink, TradingCommand, TradingCommandKind, TradingCommandReceipt,
    TradingCommandService, TradingCommandStatus,
};
pub use quote_coordinator::{QuoteCoordinator, QuoteCoordinatorConfig};
pub use quotes::{
    DesiredQuote, DesiredQuoteSet, QuoteDiff, QuotePlan, QuoteRiskOwner, QuoteSlotKey,
    QuoteSlotView,
};
