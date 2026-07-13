"""Unit tests for multi-strategy control-plane plugins and trend config."""

from __future__ import annotations

from decimal import Decimal

import pytest

from hypeedge.core.enums import MarketMakerLifecycle
from hypeedge.core.exceptions import StrategyLifecycleError, StrategyRegistrationError
from hypeedge.core.types import StrategyId, SubAccount, Symbol
from hypeedge.storage.market_making import (
    default_trend_follow_config,
    normalize_trend_follow_config,
    trend_follow_config_hash,
)
from hypeedge.strategy.plugin import (
    MARKET_MAKER_CAPABILITIES,
    TREND_FOLLOW_CAPABILITIES,
    StaticStrategyTypePlugin,
)
from hypeedge.strategy.registry import StrategyBuildContext, StrategyRegistry


def test_trend_follow_config_normalize_and_hash_are_stable() -> None:
    config = default_trend_follow_config()
    normalized = normalize_trend_follow_config(config)
    assert normalized["fast_ema_period"] == 12
    assert trend_follow_config_hash(config) == trend_follow_config_hash(
        {**config, "max_position_pct": Decimal("0.150")}
    )


def test_trend_follow_config_rejects_bad_ema_order() -> None:
    config = default_trend_follow_config()
    config["fast_ema_period"] = 30
    config["slow_ema_period"] = 26
    with pytest.raises(StrategyRegistrationError, match="fast_ema_period"):
        normalize_trend_follow_config(config)


def test_registry_plugin_registration_and_capabilities() -> None:
    registry = StrategyRegistry()

    def factory(context: StrategyBuildContext) -> object:
        raise AssertionError("factory should not be called")

    registry.register_plugin(
        StaticStrategyTypePlugin(
            strategy_type="trend_follow",
            capabilities=TREND_FOLLOW_CAPABILITIES,
            factory=factory,  # type: ignore[arg-type]
            _default_config=default_trend_follow_config(),
            _validate=normalize_trend_follow_config,
            _decode=lambda snapshot: snapshot.values,
        )
    )
    assert "trend_follow" in registry
    caps = registry.capabilities("trend_follow")
    assert caps is not None
    assert caps.supports_shadow is False
    assert "drain" not in caps.actions


@pytest.mark.asyncio
async def test_live_capability_supervisor_rejects_trend_drain() -> None:
    from hypeedge.strategy.market_maker.adapters import LiveCapabilityStrategySupervisor
    from hypeedge.strategy.registry import StrategyInstanceDefinition
    from hypeedge.strategy.supervisor import StrategyRuntimeState

    class _Store:
        async def get_instance(self, strategy_id: StrategyId) -> StrategyInstanceDefinition:
            return StrategyInstanceDefinition(
                strategy_id,
                "trend_follow",
                SubAccount("trend_btc"),
                Symbol("BTC"),
            )

    registry = StrategyRegistry()
    registry.register_plugin(
        StaticStrategyTypePlugin(
            strategy_type="trend_follow",
            capabilities=TREND_FOLLOW_CAPABILITIES,
            factory=lambda context: context,  # type: ignore[arg-type,return-value]
            _default_config=default_trend_follow_config(),
            _validate=normalize_trend_follow_config,
            _decode=lambda snapshot: snapshot.values,
        )
    )

    class _Inner:
        def __init__(self) -> None:
            self._store = _Store()
            self._registry = registry

        async def drain(self, strategy_id: StrategyId) -> StrategyRuntimeState:
            raise AssertionError("drain should be gated")

    class _Commands:
        live_enabled = False

    supervisor = LiveCapabilityStrategySupervisor(_Inner(), _Commands(), registry=registry)
    with pytest.raises(StrategyLifecycleError, match="drain"):
        await supervisor.drain(StrategyId("trend-1"))


def test_market_maker_capabilities_include_shadow_and_drain() -> None:
    assert MARKET_MAKER_CAPABILITIES.supports_shadow is True
    assert MARKET_MAKER_CAPABILITIES.supports_drain is True
    assert MarketMakerLifecycle.SHADOW in MARKET_MAKER_CAPABILITIES.desired_states
