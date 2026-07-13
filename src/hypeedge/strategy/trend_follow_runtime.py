"""Trend-follow runtime adapter for the multi-strategy control plane."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import structlog

from hypeedge.core.enums import MarketMakerLifecycle, StrategyStatus
from hypeedge.core.events import EventBus
from hypeedge.core.exceptions import StrategyLifecycleError
from hypeedge.storage.market_making import default_trend_follow_config, normalize_trend_follow_config
from hypeedge.strategy.params import TrendParams
from hypeedge.strategy.registry import StrategyBuildContext, StrategyConfigSnapshot, StrategyRuntimeHandle
from hypeedge.strategy.runner import StrategyRunner
from hypeedge.strategy.trend_follow import TrendFollowStrategy

logger = structlog.get_logger(__name__)

TrendStrategyFactory = Callable[[StrategyBuildContext, TrendParams], TrendFollowStrategy]


def decode_trend_follow_config(snapshot: StrategyConfigSnapshot, *, symbol: str) -> TrendParams:
    """Decode a durable config snapshot into ``TrendParams``."""

    normalized = normalize_trend_follow_config(dict(snapshot.values))
    return TrendParams(
        symbol=symbol,
        fast_ema_period=int(normalized["fast_ema_period"]),
        slow_ema_period=int(normalized["slow_ema_period"]),
        signal_ema_period=int(normalized["signal_ema_period"]),
        momentum_period=int(normalized["momentum_period"]),
        momentum_threshold=float(normalized["momentum_threshold"]),
        atr_period=int(normalized["atr_period"]),
        atr_position_multiplier=float(normalized["atr_position_multiplier"]),
        atr_stop_multiplier=float(normalized["atr_stop_multiplier"]),
        max_position_pct=float(normalized["max_position_pct"]),
        risk_per_trade_pct=float(normalized["risk_per_trade_pct"]),
        macd_cross_threshold=float(normalized["macd_cross_threshold"]),
    )


def validate_trend_follow_create_config(values: dict[str, Any] | Any) -> dict[str, Decimal | int]:
    payload = values.model_dump() if hasattr(values, "model_dump") else dict(values)
    return normalize_trend_follow_config(payload)


class TrendFollowRuntimeHandle:
    """Wrap ``TrendFollowStrategy`` + ``StrategyRunner`` as a ``StrategyRuntimeHandle``."""

    def __init__(
        self,
        strategy: TrendFollowStrategy,
        event_bus: EventBus,
    ) -> None:
        self._strategy = strategy
        self._runner = StrategyRunner(strategy, event_bus)
        self._task: asyncio.Task[None] | None = None
        self._log = logger.bind(strategy_id=str(strategy.strategy_id))

    @property
    def strategy(self) -> TrendFollowStrategy:
        return self._strategy

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(
            self._runner.run(),
            name=f"trend_follow_runner:{self._strategy.strategy_id}",
        )
        self._log.info("trend_follow_runtime_started")

    async def set_mode(self, mode: MarketMakerLifecycle) -> None:
        if mode in {MarketMakerLifecycle.WARMING, MarketMakerLifecycle.SHADOW}:
            # Supervisor may briefly warm; shadow is skipped for this type at supervisor level.
            return
        if mode == MarketMakerLifecycle.RUNNING:
            self._strategy.set_status(StrategyStatus.RUNNING)
            if self._task is None or self._task.done():
                await self.start()
            return
        if mode == MarketMakerLifecycle.PAUSED:
            self._strategy.set_status(StrategyStatus.PAUSED)
            return
        if mode in {MarketMakerLifecycle.STOPPED, MarketMakerLifecycle.FAULTED, MarketMakerLifecycle.DRAINING}:
            await self.stop()
            if mode == MarketMakerLifecycle.FAULTED:
                self._strategy.set_status(StrategyStatus.ERROR)
            return
        raise StrategyLifecycleError(f"Unsupported trend_follow mode: {mode.value}")

    async def apply_config(self, config: StrategyConfigSnapshot) -> None:
        params = decode_trend_follow_config(config, symbol=str(self._strategy.params.symbol))
        self._strategy.update_params(params)

    async def stop(self) -> None:
        await self._runner.stop()
        task = self._task
        self._task = None
        if task is not None and not task.done():
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._log.info("trend_follow_runtime_stopped")


def build_trend_follow_factory(
    *,
    event_bus: EventBus,
    strategy_factory: TrendStrategyFactory,
) -> Callable[[StrategyBuildContext], StrategyRuntimeHandle]:
    def factory(context: StrategyBuildContext) -> StrategyRuntimeHandle:
        params = decode_trend_follow_config(context.config, symbol=str(context.instance.symbol))
        strategy = strategy_factory(context, params)
        return TrendFollowRuntimeHandle(strategy, event_bus)

    return factory


def build_trend_follow_plugin(
    *,
    event_bus: EventBus,
    strategy_factory: TrendStrategyFactory,
) -> Any:
    from hypeedge.strategy.plugin import TREND_FOLLOW_CAPABILITIES, StaticStrategyTypePlugin

    return StaticStrategyTypePlugin(
        strategy_type="trend_follow",
        capabilities=TREND_FOLLOW_CAPABILITIES,
        factory=build_trend_follow_factory(event_bus=event_bus, strategy_factory=strategy_factory),
        _default_config=default_trend_follow_config(),
        _validate=validate_trend_follow_create_config,
        _decode=lambda snapshot: decode_trend_follow_config(snapshot, symbol="BTC"),
    )


__all__ = [
    "TrendFollowRuntimeHandle",
    "build_trend_follow_factory",
    "build_trend_follow_plugin",
    "decode_trend_follow_config",
    "validate_trend_follow_create_config",
]
