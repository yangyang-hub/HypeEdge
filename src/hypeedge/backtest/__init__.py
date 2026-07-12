"""Backtest framework — simulated matching, performance metrics, and anti-overfitting tools."""

from hypeedge.backtest.broker import FeeConfig, SimulatedBroker, SlippageConfig, SlippageMode
from hypeedge.backtest.data_feed import DataFeed
from hypeedge.backtest.engine import BacktestEngine, BacktestResult, SimulatedExecutionClient
from hypeedge.backtest.metrics import MetricsCalculator, PerformanceMetrics
from hypeedge.backtest.walk_forward import (
    MonteCarloResult,
    WalkForwardEngine,
    WalkForwardResult,
    bonferroni_correction,
    run_monte_carlo,
)

__all__ = [
    "BacktestEngine",
    "BacktestResult",
    "DataFeed",
    "FeeConfig",
    "MetricsCalculator",
    "MonteCarloResult",
    "PerformanceMetrics",
    "SimulatedBroker",
    "SimulatedExecutionClient",
    "SlippageConfig",
    "SlippageMode",
    "WalkForwardEngine",
    "WalkForwardResult",
    "bonferroni_correction",
    "run_monte_carlo",
]
