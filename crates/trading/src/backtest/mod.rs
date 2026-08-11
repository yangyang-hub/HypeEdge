//! Backtest framework, port of `src/hypeedge/backtest/`.
//!
//! [`broker`] simulates fills/fees/slippage/funding; [`engine`] drives the
//! event loop; [`metrics`] computes the 18-field performance report.

pub mod broker;
pub mod engine;
pub mod market_maker_metrics;
pub mod market_maker_replay;
pub mod metrics;
pub mod walk_forward;

pub use broker::{FeeConfig, SimulatedBroker, SlippageConfig, SlippageMode};
pub use engine::{BacktestEngine, BacktestResult, SimulatedExecutionClient};
pub use market_maker_metrics::{
    AccountingFill, AccountingLedger, AccountingPnL, ExecutionQuality, FillMarkout,
};
pub use market_maker_replay::{
    CancelEvent, DEFAULT_ASSUMPTIONS, FundingEvent, MarketMakerReplay, MarketMakerReplayResult,
    PaidActionEvent, QuoteEvent, ReplayEvent, ReplayFill, ReplayScenario, ScenarioAssumption,
    ShadowReplayOrder, TradeEvent, default_assumption,
};
pub use metrics::{MetricsCalculator, PerformanceMetrics};
pub use walk_forward::{
    MonteCarloResult, WalkForwardEngine, WalkForwardResult, WalkForwardWindow,
    bonferroni_correction, compute_max_drawdown, compute_returns, compute_sharpe, run_monte_carlo,
};
