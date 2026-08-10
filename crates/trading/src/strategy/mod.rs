//! Strategy framework: base trait, sequential runner, indicators, params, and
//! the trend-following strategy.
//!
//! Ports `src/hypeedge/strategy/`. The strategy control plane (supervisor,
//! registry, multi-strategy plugin) and the market-maker / funding-arb runtimes
//! land in later Phase-5 increments.

pub mod base;
pub mod config_normalize;
pub mod indicators;
pub mod params;
pub mod registry;
pub mod runner;
pub mod supervisor;
pub mod trend_follow;
pub mod trend_follow_runtime;

pub use base::Strategy;
pub use config_normalize::{
    AUTO_SPOT_MARKET, default_funding_arb_config, default_market_maker_config,
    default_trend_follow_config_values, funding_arb_config_hash, market_maker_config_hash,
    normalize_funding_arb_config, normalize_market_maker_config, normalize_trend_follow_config,
    trend_follow_config_hash,
};
pub use indicators::{atr, ema, macd, momentum, sma};
pub use params::TrendParams;
pub use registry::{
    FaultedRuntimeHandle, StrategyBuildContext, StrategyConfigSnapshot, StrategyInstanceDefinition,
    StrategyRegistry, StrategyRuntimeHandle, StrategyTypeCapabilities, StrategyTypePlugin,
    funding_arb_capabilities, market_maker_capabilities, trend_follow_capabilities,
};
pub use runner::StrategyRunner;
pub use supervisor::{
    InMemoryStrategyAllocationManager, InMemoryStrategyStateStore, SYSTEM_SAFETY_PAUSE_PREFIX,
    SYSTEM_SAFETY_RECOVERED_REASON, StrategyAllocation, StrategyAllocationManager,
    StrategyRuntimeState, StrategyStateStore, StrategySupervisor,
};
pub use trend_follow::{StrategyAccountView, TrendFollowStrategy};
pub use trend_follow_runtime::{
    TrendFollowRuntimeHandle, build_trend_follow_plugin, decode_trend_follow_config,
    default_trend_follow_config,
};
