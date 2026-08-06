"""Application-level market-making wiring and startup safety gates."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from hypeedge.app import HypeEdgeApp
from hypeedge.config.settings import AppSettings, FeatureFlagsSettings
from hypeedge.core.enums import ActionBudgetMode, MarketMakerLifecycle, SafetyMode
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


def test_control_plane_requires_v2_dependencies() -> None:
    app = HypeEdgeApp(AppSettings(features=FeatureFlagsSettings()))

    app._init_market_making_components()

    assert app.strategy_supervisor is None
    assert app.market_making_repository is None


def test_market_making_feature_off_still_constructs_multi_strategy_control_plane() -> None:
    app = HypeEdgeApp(AppSettings(features=_v2_features(market_making=False)))
    app._pg_session_factory = MagicMock()
    app._tracker = MagicMock()
    app._account_health = MagicMock()
    app._action_budget_controller = MagicMock()
    app._market_data_provider = MagicMock()
    app._execution_engine = MagicMock()

    app._init_market_making_components()

    assert app.strategy_supervisor is not None
    assert app.market_making_repository is not None
    assert app._quote_plan_worker is None


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
    instance = StrategyInstanceDefinition(StrategyId("mm-btc"), "market_maker", SubAccount("mm_btc"), Symbol("BTC"))
    concrete = SimpleNamespace(start=None, _store=_StateStore(instance))
    supervisor = LiveCapabilityStrategySupervisor(concrete, commands)

    with pytest.raises(StrategyLifecycleError, match="atomic durable quote-plan"):
        await supervisor.start(StrategyId("mm-btc"), target=MarketMakerLifecycle.RUNNING)


class _StateStore:
    def __init__(self, instance: StrategyInstanceDefinition) -> None:
        self.instance = instance
        self.desired_updates: list[MarketMakerLifecycle] = []

    async def list_instances(self) -> list[StrategyInstanceDefinition]:
        return [self.instance]

    async def get_instance(self, strategy_id: StrategyId) -> StrategyInstanceDefinition:
        del strategy_id
        return self.instance

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
        self.pauses: list[StrategyId] = []

    async def start(
        self,
        strategy_id: StrategyId,
        *,
        target: MarketMakerLifecycle,
    ) -> SimpleNamespace:
        self.starts.append(target)
        await self.store.set_desired(strategy_id, state=target)
        return SimpleNamespace(actual_state=target)

    async def pause(self, strategy_id: StrategyId) -> SimpleNamespace:
        self.pauses.append(strategy_id)
        await self.store.set_desired(strategy_id, state=MarketMakerLifecycle.PAUSED)
        return SimpleNamespace(actual_state=MarketMakerLifecycle.PAUSED)


class _SafetyStateStore:
    def __init__(self, instance: StrategyInstanceDefinition, runtime: SimpleNamespace) -> None:
        self.instance = instance
        self.runtime = runtime

    async def list_instances(self) -> list[StrategyInstanceDefinition]:
        return [self.instance]

    async def get_runtime(self, strategy_id: StrategyId) -> SimpleNamespace:
        assert strategy_id == self.instance.strategy_id
        return self.runtime


class _SafetySupervisor:
    def __init__(self) -> None:
        self.suspensions: list[tuple[StrategyId, str]] = []
        self.resumes: list[StrategyId] = []

    async def suspend_for_safety(self, strategy_id: StrategyId, reason: str) -> None:
        self.suspensions.append((strategy_id, reason))

    async def resume_from_safety(self, strategy_id: StrategyId) -> None:
        self.resumes.append(strategy_id)


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


@pytest.mark.asyncio
async def test_funding_arb_restore_starts_running_without_shadow() -> None:
    app = HypeEdgeApp(AppSettings(features=_v2_features(market_making=False)))
    instance = StrategyInstanceDefinition(
        strategy_id=StrategyId("fa-btc"),
        strategy_type="funding_arb",
        sub_account=SubAccount("0x1111111111111111111111111111111111111111"),
        symbol=Symbol("BTC"),
        desired_state=MarketMakerLifecycle.RUNNING,
    )
    store = _StateStore(instance)
    supervisor = _Supervisor(store)
    app._market_making_state_store = store
    app._strategy_supervisor = supervisor

    await app._restore_market_making_in_shadow()

    assert supervisor.starts == [MarketMakerLifecycle.RUNNING]
    assert MarketMakerLifecycle.SHADOW not in store.desired_updates


@pytest.mark.asyncio
async def test_account_health_failure_safety_suspends_without_overwriting_desired_state() -> None:
    app = HypeEdgeApp(AppSettings())
    app._safety_controller.transition(SafetyMode.NORMAL, "test_ready")
    app._trading_enabled = True
    app._metrics = MagicMock()
    instance = StrategyInstanceDefinition(
        strategy_id=StrategyId("fa-auto"),
        strategy_type="funding_arb",
        sub_account=SubAccount("0x1111111111111111111111111111111111111111"),
        symbol=Symbol("AUTO"),
        desired_state=MarketMakerLifecycle.RUNNING,
    )
    store = _SafetyStateStore(
        instance,
        SimpleNamespace(actual_state=MarketMakerLifecycle.RUNNING, reason="runtime_running"),
    )
    supervisor = _SafetySupervisor()
    app._market_making_state_store = store
    app._strategy_supervisor = supervisor

    await app._on_account_health_failure("clearinghouse_poll_failed:OSError")

    assert app.trading_enabled is False
    assert app.safety_mode == SafetyMode.CANCEL_ONLY.value
    assert app._safety_controller.reason == ("automatic_safety_degradation:clearinghouse_poll_failed:OSError")
    assert instance.desired_state == MarketMakerLifecycle.RUNNING
    assert supervisor.suspensions == [(instance.strategy_id, "clearinghouse_poll_failed:OSError")]
    app._metrics.set_trading_enabled.assert_called_once_with(False)


@pytest.mark.asyncio
async def test_automatic_safety_recovery_requires_history_budget_and_full_reconciliation() -> None:
    app = HypeEdgeApp(AppSettings())
    app._safety_controller.transition(
        SafetyMode.CANCEL_ONLY,
        "automatic_safety_degradation:action_credits_unavailable",
    )
    app._trading_prerequisites_ok = True
    fresh = SimpleNamespace(is_fresh=True)
    health = SimpleNamespace(
        inventory=fresh,
        clearinghouse=fresh,
        user_stream=fresh,
        reconciliation=fresh,
        allows_risk_increase=True,
        blocking_reasons=(),
    )
    budget = SimpleNamespace(
        remote_fresh=True,
        cancel_headroom_fresh=True,
        mode=ActionBudgetMode.NORMAL,
    )
    app._account_health = SimpleNamespace(get_account_health=lambda: health)
    app._action_budget_controller = SimpleNamespace(snapshot=lambda: budget)
    app._exchange_ingestor = SimpleNamespace(recover_history=AsyncMock(), is_running=True)
    app._reconciler = SimpleNamespace(
        reconcile=AsyncMock(return_value=SimpleNamespace(success=True, errors=[])),
    )
    app._refresh_action_budget = AsyncMock(return_value=True)  # type: ignore[method-assign]
    app._persist_system_state = AsyncMock(return_value=True)  # type: ignore[method-assign]
    app._metrics = MagicMock()
    instance = StrategyInstanceDefinition(
        strategy_id=StrategyId("fa-auto"),
        strategy_type="funding_arb",
        sub_account=SubAccount("0x1111111111111111111111111111111111111111"),
        symbol=Symbol("AUTO"),
        desired_state=MarketMakerLifecycle.RUNNING,
    )
    store = _SafetyStateStore(
        instance,
        SimpleNamespace(
            actual_state=MarketMakerLifecycle.PAUSED,
            reason="system_safety_pause:action_credits_unavailable",
        ),
    )
    supervisor = _SafetySupervisor()
    app._market_making_state_store = store
    app._strategy_supervisor = supervisor

    assert await app._try_recover_automatic_safety() is True

    app._exchange_ingestor.recover_history.assert_awaited_once()
    app._reconciler.reconcile.assert_awaited_once()
    app._refresh_action_budget.assert_awaited_once()
    assert [call.args[0] for call in app._persist_system_state.await_args_list] == [
        "recovering",
        "reconciling",
        "normal",
    ]
    assert app.trading_enabled is True
    assert app.safety_mode == SafetyMode.NORMAL.value
    assert supervisor.resumes == [instance.strategy_id]


@pytest.mark.asyncio
async def test_system_recovery_does_not_resume_operator_paused_instance() -> None:
    app = HypeEdgeApp(AppSettings())
    instance = StrategyInstanceDefinition(
        strategy_id=StrategyId("fa-paused"),
        strategy_type="funding_arb",
        sub_account=SubAccount("0x1111111111111111111111111111111111111111"),
        symbol=Symbol("AUTO"),
        desired_state=MarketMakerLifecycle.PAUSED,
    )
    store = _SafetyStateStore(
        instance,
        SimpleNamespace(
            actual_state=MarketMakerLifecycle.PAUSED,
            reason="system_safety_pause:user_stream_disconnected",
        ),
    )
    supervisor = _SafetySupervisor()
    app._market_making_state_store = store
    app._strategy_supervisor = supervisor

    await app._resume_system_suspended_strategies()

    assert supervisor.resumes == []


def test_mainnet_keeps_market_making_disabled_by_default() -> None:
    settings = AppSettings(environment="mainnet")

    assert settings.features.market_making_enabled is False
