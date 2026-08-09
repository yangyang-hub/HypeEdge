//! Backtest framework, port of `src/hypeedge/backtest/`.
//!
//! [`broker`] simulates fills/fees/slippage/funding; [`engine`] drives the
//! event loop; [`metrics`] computes the 18-field performance report.

pub mod broker;
pub mod engine;
pub mod metrics;
pub mod duckdb_export;
pub mod walk_forward;

pub use broker::{FeeConfig, SimulatedBroker, SlippageConfig, SlippageMode};
pub use engine::{BacktestEngine, BacktestResult, SimulatedExecutionClient};
pub use metrics::{MetricsCalculator, PerformanceMetrics};
pub use duckdb_export::{FetchedTable, DuckDBExporter, EXPORT_TABLES};
pub use walk_forward::{bonferroni_correction, compute_max_drawdown, compute_returns, compute_sharpe, run_monte_carlo, MonteCarloResult, WalkForwardEngine, WalkForwardResult, WalkForwardWindow};
