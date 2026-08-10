//! Funding-rate arbitrage strategy, port of `src/hypeedge/strategy/funding_arb/`.

pub mod live_scanner;
pub mod models;
pub mod runtime;
pub mod scanner;
pub mod store;

pub use models::{FundingArbCycle, FundingArbParams};
pub use runtime::{
    FundingArbAccountView, FundingArbDeployment, FundingArbRuntimeDependencies,
    FundingArbRuntimeHandle, InstrumentInfo, OrderOutcome, SpotBalanceView,
    build_funding_arb_plugin, decode_funding_arb_config, default_funding_arb_config,
};
pub use scanner::{FundingArbMarketScanner, FundingArbMarketSnapshot};
pub use store::FundingArbCycleStore;
