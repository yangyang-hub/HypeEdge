"""Strategy module — base class, trend following, indicators, and parameters."""

from hypeedge.strategy.base import StrategyBase
from hypeedge.strategy.indicators import atr, ema, macd, momentum, sma
from hypeedge.strategy.params import ParamWatcher, TrendParams, load_params
from hypeedge.strategy.registry import StrategyRegistry
from hypeedge.strategy.supervisor import StrategySupervisor
from hypeedge.strategy.trend_follow import TrendFollowStrategy

__all__ = [
    "ParamWatcher",
    "StrategyBase",
    "StrategyRegistry",
    "StrategySupervisor",
    "TrendFollowStrategy",
    "TrendParams",
    "atr",
    "ema",
    "load_params",
    "macd",
    "momentum",
    "sma",
]
