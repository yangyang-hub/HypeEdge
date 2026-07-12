"""Multi-instance registry and lifecycle supervisor tests."""

from __future__ import annotations

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
