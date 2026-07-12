"""Application-level market-making wiring and startup safety gates."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from hypeedge.app import HypeEdgeApp
from hypeedge.config.settings import AppSettings, FeatureFlagsSettings
from hypeedge.core.enums import MarketMakerLifecycle
from hypeedge.core.exceptions import StrategyLifecycleError, TradingCommandPersistenceError
from hypeedge.core.types import StrategyId, SubAccount, Symbol
from hypeedge.strategy.market_maker.adapters import (
    DurableQuotePlanCommandAdapter,
    LiveCapabilityStrategySupervisor,
)
from hypeedge.strategy.registry import StrategyInstanceDefinition
from hypeedge.trading.quotes import QuotePlan


def _v2_features(*, market_making: bool) -> FeatureFlagsSettings:
    return FeatureFlagsSettings(
        durable_ledger_v2=True,
        execution_v2=True,
        user_stream_v2=True,
        reconciliation_v2=True,
        strategy_runner_v2=True,
        market_making_enabled=market_making,
    )


def test_market_making_feature_off_does_not_construct_control_plane() -> None:
    app = HypeEdgeApp(AppSettings(features=FeatureFlagsSettings()))

    app._init_market_making_components()

    assert app.strategy_supervisor is None
    assert app.market_making_repository is None


@pytest.mark.asyncio
async def test_quote_plan_adapter_rejects_live_plan_without_atomic_repository() -> None:
    cancelled = 0

    async def cancel_all() -> int:
        nonlocal cancelled
        cancelled += 1
        return 0

    adapter = DurableQuotePlanCommandAdapter(repository=object(), cancel_all=cancel_all)
    plan = QuotePlan(
        strategy_id=StrategyId("mm-btc"),
        symbol=Symbol("BTC"),
        session_id="shadow-session",
        config_version=1,
        revision=1,
        market_version=1,
        connection_generation=1,
        valid_until=datetime(2030, 1, 1, tzinfo=UTC),
        diffs=(),
    )

    with pytest.raises(TradingCommandPersistenceError, match="live placement rejected"):
        await adapter.submit_quote_plan(plan)
    assert cancelled == 0


@pytest.mark.asyncio
async def test_supervisor_rejects_running_before_atomic_plan_boundary() -> None:
    async def cancel_all() -> int:
        return 0

    commands = DurableQuotePlanCommandAdapter(repository=object(), cancel_all=cancel_all)
    concrete = SimpleNamespace(start=None)
    supervisor = LiveCapabilityStrategySupervisor(concrete, commands)

    with pytest.raises(StrategyLifecycleError, match="atomic durable quote-plan"):
        await supervisor.start(StrategyId("mm-btc"), target=MarketMakerLifecycle.RUNNING)


class _StateStore:
    def __init__(self, instance: StrategyInstanceDefinition) -> None:
        self.instance = instance
        self.desired_updates: list[MarketMakerLifecycle] = []

    async def list_instances(self) -> list[StrategyInstanceDefinition]:
        return [self.instance]

    async def set_desired(
        self,
        strategy_id: StrategyId,
        *,
        state: MarketMakerLifecycle | None = None,
        **_: Any,
    ) -> StrategyInstanceDefinition:
        assert strategy_id == self.instance.strategy_id
        assert state is not None
        self.desired_updates.append(state)
        self.instance = replace(self.instance, desired_state=state, revision=self.instance.revision + 1)
        return self.instance


class _Supervisor:
    def __init__(self, store: _StateStore) -> None:
        self.store = store
        self.starts: list[MarketMakerLifecycle] = []

    async def start(
        self,
        strategy_id: StrategyId,
        *,
        target: MarketMakerLifecycle,
    ) -> SimpleNamespace:
        self.starts.append(target)
        await self.store.set_desired(strategy_id, state=target)
        return SimpleNamespace(actual_state=target)


@pytest.mark.asyncio
async def test_restart_preserves_running_intent_but_restores_runtime_to_shadow() -> None:
    app = HypeEdgeApp(AppSettings(features=_v2_features(market_making=True)))
    instance = StrategyInstanceDefinition(
        strategy_id=StrategyId("mm-btc"),
        strategy_type="market_maker",
        sub_account=SubAccount("0x1111111111111111111111111111111111111111"),
        symbol=Symbol("BTC"),
        desired_state=MarketMakerLifecycle.RUNNING,
    )
    store = _StateStore(instance)
    supervisor = _Supervisor(store)
    app._market_making_state_store = store
    app._strategy_supervisor = supervisor

    await app._restore_market_making_in_shadow()

    assert supervisor.starts == [MarketMakerLifecycle.SHADOW]
    assert store.desired_updates == [MarketMakerLifecycle.SHADOW, MarketMakerLifecycle.RUNNING]
    assert store.instance.desired_state == MarketMakerLifecycle.RUNNING


def test_mainnet_keeps_market_making_disabled_by_default() -> None:
    settings = AppSettings(environment="mainnet")

    assert settings.features.market_making_enabled is False
