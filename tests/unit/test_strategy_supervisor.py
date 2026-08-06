"""Multi-instance registry and lifecycle supervisor tests."""

from __future__ import annotations

from decimal import Decimal

import pytest

from hypeedge.core.enums import MarketMakerLifecycle
from hypeedge.core.exceptions import StrategyLifecycleError, StrategyRegistrationError
from hypeedge.core.types import StrategyId, SubAccount, Symbol
from hypeedge.strategy.registry import (
    StrategyBuildContext,
    StrategyConfigSnapshot,
    StrategyInstanceDefinition,
    StrategyRegistry,
)
from hypeedge.strategy.supervisor import (
    InMemoryStrategyAllocationManager,
    InMemoryStrategyStateStore,
    StrategySupervisor,
)


class _Handle:
    def __init__(self, context: StrategyBuildContext) -> None:
        self.context = context
        self.calls: list[str] = []

    async def start(self) -> None:
        self.calls.append("start")

    async def set_mode(self, mode: MarketMakerLifecycle) -> None:
        self.calls.append(f"mode:{mode.value}")

    async def apply_config(self, config: StrategyConfigSnapshot) -> None:
        self.calls.append(f"config:{config.revision}")

    async def stop(self) -> None:
        self.calls.append("stop")


async def _setup(
    *,
    strategy_id: str = "maker-1",
    sub_account: str = "sub-1",
    symbol: str = "BTC",
) -> tuple[StrategySupervisor, InMemoryStrategyStateStore, InMemoryStrategyAllocationManager, list[_Handle]]:
    store = InMemoryStrategyStateStore()
    sid = StrategyId(strategy_id)
    await store.add_instance(
        StrategyInstanceDefinition(
            strategy_id=sid,
            strategy_type="market_maker",
            sub_account=SubAccount(sub_account),
            symbol=Symbol(symbol),
        ),
        [StrategyConfigSnapshot(sid, 1, {"spread_bps": "5"})],
    )
    handles: list[_Handle] = []
    registry = StrategyRegistry()

    def factory(context: StrategyBuildContext) -> _Handle:
        handle = _Handle(context)
        handles.append(handle)
        return handle

    registry.register("market_maker", factory)
    allocations = InMemoryStrategyAllocationManager()
    return StrategySupervisor(registry, store, allocations), store, allocations, handles


@pytest.mark.asyncio
async def test_start_pause_resume_drain_stop_are_idempotent() -> None:
    supervisor, store, allocations, handles = await _setup()
    sid = StrategyId("maker-1")

    running = await supervisor.start(sid)
    replay = await supervisor.start(sid)
    assert replay == running
    assert running.actual_state == MarketMakerLifecycle.RUNNING
    assert running.effective_config_revision == 1
    assert len(handles) == 1
    assert handles[0].calls == ["start", "config:1", "mode:shadow", "mode:running"]

    paused = await supervisor.pause(sid)
    assert (await supervisor.pause(sid)) == paused
    assert paused.actual_state == MarketMakerLifecycle.PAUSED
    assert paused.reason == "operator_pause"
    assert (await store.get_instance(sid)).desired_state == MarketMakerLifecycle.PAUSED
    resumed = await supervisor.resume(sid)
    assert resumed.actual_state == MarketMakerLifecycle.RUNNING
    drained = await supervisor.drain(sid)
    assert drained.actual_state == MarketMakerLifecycle.DRAINING
    stopped = await supervisor.stop(sid)
    assert (await supervisor.stop(sid)) == stopped
    assert stopped.actual_state == MarketMakerLifecycle.STOPPED
    assert await allocations.get(sid) is None
    assert (await store.get_instance(sid)).desired_state == MarketMakerLifecycle.STOPPED


@pytest.mark.asyncio
async def test_system_safety_suspend_preserves_operator_intent_and_can_resume() -> None:
    supervisor, store, _, handles = await _setup()
    sid = StrategyId("maker-1")
    await supervisor.start(sid)

    suspended = await supervisor.suspend_for_safety(sid, "user_stream_disconnected")

    assert suspended.actual_state == MarketMakerLifecycle.PAUSED
    assert suspended.reason == "system_safety_pause:user_stream_disconnected"
    assert (await store.get_instance(sid)).desired_state == MarketMakerLifecycle.RUNNING
    assert handles[0].calls[-1] == "mode:paused"

    resumed = await supervisor.resume_from_safety(sid)

    assert resumed.actual_state == MarketMakerLifecycle.RUNNING
    assert resumed.reason == "system_safety_recovered"
    assert (await store.get_instance(sid)).desired_state == MarketMakerLifecycle.RUNNING
    assert handles[0].calls[-1] == "mode:running"


@pytest.mark.asyncio
async def test_operator_pause_during_safety_suspend_prevents_automatic_resume() -> None:
    supervisor, store, _, handles = await _setup()
    sid = StrategyId("maker-1")
    await supervisor.start(sid)
    await supervisor.suspend_for_safety(sid, "action_credits_unavailable")

    paused = await supervisor.pause(sid)
    call_count = len(handles[0].calls)
    recovered = await supervisor.resume_from_safety(sid)

    assert paused.reason == "operator_pause"
    assert recovered == paused
    assert (await store.get_instance(sid)).desired_state == MarketMakerLifecycle.PAUSED
    assert len(handles[0].calls) == call_count


@pytest.mark.asyncio
async def test_safety_suspend_fences_persisted_active_state_without_process_handle() -> None:
    supervisor, store, _, handles = await _setup()
    sid = StrategyId("maker-1")
    await store.set_desired(sid, state=MarketMakerLifecycle.RUNNING)
    runtime = await store.get_runtime(sid)
    await store.set_runtime(
        sid,
        actual_state=MarketMakerLifecycle.RUNNING,
        reason="restored_runtime_state",
        expected_revision=runtime.revision,
    )

    suspended = await supervisor.suspend_for_safety(sid, "startup_health_unavailable")

    assert suspended.actual_state == MarketMakerLifecycle.PAUSED
    assert suspended.reason == "system_safety_pause:startup_health_unavailable"
    assert (await store.get_instance(sid)).desired_state == MarketMakerLifecycle.RUNNING

    recovered = await supervisor.resume_from_safety(sid)

    assert recovered.actual_state == MarketMakerLifecycle.SHADOW
    assert (await store.get_instance(sid)).desired_state == MarketMakerLifecycle.RUNNING
    assert len(handles) == 1


@pytest.mark.asyncio
async def test_desired_config_precedes_effective_config() -> None:
    supervisor, store, _, handles = await _setup()
    sid = StrategyId("maker-1")
    await store.add_config(StrategyConfigSnapshot(sid, 2, {"spread_bps": "7"}))

    runtime = await supervisor.activate_config(sid, 2)
    assert runtime.effective_config_revision is None
    assert (await store.get_instance(sid)).desired_config_revision == 2

    runtime = await supervisor.start(sid, target=MarketMakerLifecycle.SHADOW)
    assert runtime.effective_config_revision == 2
    assert handles[0].calls == ["start", "config:2", "mode:shadow"]

    await store.add_config(StrategyConfigSnapshot(sid, 3, {"spread_bps": "9"}))
    runtime = await supervisor.activate_config(sid, 3)
    assert runtime.effective_config_revision == 3
    assert handles[0].calls[-1] == "config:3"


@pytest.mark.asyncio
async def test_plugin_validation_rejects_legacy_invalid_config_before_activation() -> None:
    from hypeedge.storage.market_making import default_funding_arb_config
    from hypeedge.strategy.funding_arb import build_funding_arb_plugin

    store = InMemoryStrategyStateStore()
    sid = StrategyId("fa-1")
    valid = default_funding_arb_config()
    invalid = {**valid, "entry_funding_rate": Decimal("0")}
    await store.add_instance(
        StrategyInstanceDefinition(
            strategy_id=sid,
            strategy_type="funding_arb",
            sub_account=SubAccount("sub-1"),
            symbol=Symbol("BTC"),
        ),
        [
            StrategyConfigSnapshot(sid, 1, valid),
            StrategyConfigSnapshot(sid, 2, invalid),
        ],
    )
    registry = StrategyRegistry()
    registry.register_plugin(build_funding_arb_plugin())
    supervisor = StrategySupervisor(registry, store, InMemoryStrategyAllocationManager())

    with pytest.raises(StrategyRegistrationError, match="entry_funding_rate"):
        await supervisor.activate_config(sid, 2)

    assert (await store.get_instance(sid)).desired_config_revision == 1


@pytest.mark.asyncio
async def test_allocation_is_exclusive_across_instances() -> None:
    supervisor, store, allocations, _ = await _setup()
    sid2 = StrategyId("maker-2")
    await store.add_instance(
        StrategyInstanceDefinition(
            strategy_id=sid2,
            strategy_type="market_maker",
            sub_account=SubAccount("sub-1"),
            symbol=Symbol("BTC"),
        ),
        [StrategyConfigSnapshot(sid2, 1, {})],
    )

    await supervisor.start(StrategyId("maker-1"))
    with pytest.raises(StrategyLifecycleError, match="already owned"):
        await supervisor.start(sid2)
    assert await allocations.get(StrategyId("maker-1")) is not None
    assert await allocations.get(sid2) is None


@pytest.mark.asyncio
async def test_funding_arb_uses_auto_allocation_even_for_legacy_fixed_symbol_instance() -> None:
    store = InMemoryStrategyStateStore()
    sid = StrategyId("fa-auto-1")
    await store.add_instance(
        StrategyInstanceDefinition(
            strategy_id=sid,
            strategy_type="funding_arb",
            sub_account=SubAccount("0xabc"),
            symbol=Symbol("HYPE"),
        ),
        [StrategyConfigSnapshot(sid, 1, {})],
    )
    handles: list[_Handle] = []
    registry = StrategyRegistry()

    def factory(context: StrategyBuildContext) -> _Handle:
        handle = _Handle(context)
        handles.append(handle)
        return handle

    registry.register("funding_arb", factory)
    allocations = InMemoryStrategyAllocationManager()
    supervisor = StrategySupervisor(registry, store, allocations)

    await supervisor.start(sid)

    allocation = await allocations.get(sid)
    assert allocation is not None
    assert allocation.symbol == Symbol("AUTO")


@pytest.mark.asyncio
async def test_fault_retains_allocation_and_requires_manual_recovery() -> None:
    supervisor, _, allocations, handles = await _setup()
    sid = StrategyId("maker-1")
    await supervisor.start(sid)

    faulted = await supervisor.fault(sid, "user stream gap")
    assert faulted.actual_state == MarketMakerLifecycle.FAULTED
    assert await allocations.get(sid) is not None
    fault_call_count = len(handles[0].calls)
    assert await supervisor.fault(sid, "duplicate") == faulted
    assert len(handles[0].calls) == fault_call_count
    with pytest.raises(StrategyLifecycleError, match="use recover"):
        await supervisor.start(sid)

    recovered = await supervisor.recover(sid, target=MarketMakerLifecycle.SHADOW)
    assert recovered.actual_state == MarketMakerLifecycle.SHADOW


def test_registry_rejects_duplicate_and_unknown_types() -> None:
    registry = StrategyRegistry()
    registry.register("market_maker", lambda context: _Handle(context))
    with pytest.raises(StrategyRegistrationError, match="already registered"):
        registry.register("MARKET_MAKER", lambda context: _Handle(context))
