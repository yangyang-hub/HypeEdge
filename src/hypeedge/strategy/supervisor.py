"""Persistent multi-instance strategy lifecycle supervisor."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, replace
from typing import Any, Protocol

import structlog

from hypeedge.core.enums import MarketMakerLifecycle
from hypeedge.core.exceptions import StrategyLifecycleError, StrategyRegistrationError
from hypeedge.core.types import StrategyId, SubAccount, Symbol
from hypeedge.strategy.registry import (
    StrategyBuildContext,
    StrategyConfigSnapshot,
    StrategyInstanceDefinition,
    StrategyRegistry,
    StrategyRuntimeHandle,
)

logger = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class StrategyRuntimeState:
    strategy_id: StrategyId
    actual_state: MarketMakerLifecycle = MarketMakerLifecycle.STOPPED
    effective_config_revision: int | None = None
    revision: int = 0
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class StrategyAllocation:
    strategy_id: StrategyId
    sub_account: SubAccount
    symbol: Symbol
    fence: int


class StrategyStateStore(Protocol):
    async def list_instances(self) -> list[StrategyInstanceDefinition]: ...

    async def get_instance(self, strategy_id: StrategyId) -> StrategyInstanceDefinition: ...

    async def get_runtime(self, strategy_id: StrategyId) -> StrategyRuntimeState: ...

    async def get_config(self, strategy_id: StrategyId, revision: int) -> StrategyConfigSnapshot: ...

    async def set_desired(
        self,
        strategy_id: StrategyId,
        *,
        state: MarketMakerLifecycle | None = None,
        config_revision: int | None = None,
        expected_revision: int | None = None,
    ) -> StrategyInstanceDefinition: ...

    async def set_runtime(
        self,
        strategy_id: StrategyId,
        *,
        actual_state: MarketMakerLifecycle | None = None,
        effective_config_revision: int | None = None,
        set_effective_config: bool = False,
        reason: str | None = None,
        expected_revision: int | None = None,
    ) -> StrategyRuntimeState: ...


class StrategyAllocationManager(Protocol):
    async def acquire(
        self,
        strategy_id: StrategyId,
        sub_account: SubAccount,
        symbol: Symbol,
    ) -> StrategyAllocation: ...

    async def release(self, strategy_id: StrategyId) -> None: ...

    async def get(self, strategy_id: StrategyId) -> StrategyAllocation | None: ...


_ALLOWED_TRANSITIONS: dict[MarketMakerLifecycle, frozenset[MarketMakerLifecycle]] = {
    MarketMakerLifecycle.STOPPED: frozenset({MarketMakerLifecycle.WARMING}),
    MarketMakerLifecycle.WARMING: frozenset(
        {
            MarketMakerLifecycle.SHADOW,
            MarketMakerLifecycle.PAUSED,
            MarketMakerLifecycle.STOPPED,
            MarketMakerLifecycle.FAULTED,
        }
    ),
    MarketMakerLifecycle.SHADOW: frozenset(
        {
            MarketMakerLifecycle.RUNNING,
            MarketMakerLifecycle.PAUSED,
            MarketMakerLifecycle.DRAINING,
            MarketMakerLifecycle.STOPPED,
            MarketMakerLifecycle.FAULTED,
        }
    ),
    MarketMakerLifecycle.RUNNING: frozenset(
        {MarketMakerLifecycle.PAUSED, MarketMakerLifecycle.DRAINING, MarketMakerLifecycle.FAULTED}
    ),
    MarketMakerLifecycle.PAUSED: frozenset(
        {
            MarketMakerLifecycle.WARMING,
            MarketMakerLifecycle.SHADOW,
            MarketMakerLifecycle.RUNNING,
            MarketMakerLifecycle.DRAINING,
            MarketMakerLifecycle.STOPPED,
            MarketMakerLifecycle.FAULTED,
        }
    ),
    MarketMakerLifecycle.DRAINING: frozenset(
        {MarketMakerLifecycle.PAUSED, MarketMakerLifecycle.STOPPED, MarketMakerLifecycle.FAULTED}
    ),
    MarketMakerLifecycle.FAULTED: frozenset({MarketMakerLifecycle.STOPPED}),
}


class StrategySupervisor:
    """Own multiple strategy runtimes with durable desired/effective state."""

    def __init__(
        self,
        registry: StrategyRegistry,
        state_store: StrategyStateStore,
        allocations: StrategyAllocationManager,
    ) -> None:
        self._registry = registry
        self._store = state_store
        self._allocations = allocations
        self._handles: dict[StrategyId, StrategyRuntimeHandle] = {}
        self._locks: defaultdict[StrategyId, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def start(
        self,
        strategy_id: StrategyId,
        *,
        target: MarketMakerLifecycle = MarketMakerLifecycle.RUNNING,
        expected_revision: int | None = None,
    ) -> StrategyRuntimeState:
        if target not in {MarketMakerLifecycle.SHADOW, MarketMakerLifecycle.RUNNING}:
            raise StrategyLifecycleError(f"Start target must be shadow or running, got {target.value}")
        async with self._locks[strategy_id]:
            return await self._start_locked(strategy_id, target, expected_revision=expected_revision)

    async def pause(self, strategy_id: StrategyId) -> StrategyRuntimeState:
        async with self._locks[strategy_id]:
            instance = await self._store.set_desired(strategy_id, state=MarketMakerLifecycle.PAUSED)
            runtime = await self._store.get_runtime(strategy_id)
            if runtime.actual_state == MarketMakerLifecycle.PAUSED:
                return runtime
            handle = self._require_handle(strategy_id)
            try:
                await handle.set_mode(MarketMakerLifecycle.PAUSED)
                return await self._transition(instance, runtime, MarketMakerLifecycle.PAUSED, "operator_pause")
            except Exception as exc:
                await self._fault_locked(instance, exc)
                raise StrategyLifecycleError(f"Failed to pause strategy {strategy_id}") from exc

    async def resume(
        self,
        strategy_id: StrategyId,
        *,
        target: MarketMakerLifecycle = MarketMakerLifecycle.RUNNING,
    ) -> StrategyRuntimeState:
        if target not in {MarketMakerLifecycle.SHADOW, MarketMakerLifecycle.RUNNING}:
            raise StrategyLifecycleError(f"Resume target must be shadow or running, got {target.value}")
        async with self._locks[strategy_id]:
            return await self._start_locked(strategy_id, target)

    async def drain(self, strategy_id: StrategyId) -> StrategyRuntimeState:
        async with self._locks[strategy_id]:
            instance = await self._store.set_desired(strategy_id, state=MarketMakerLifecycle.DRAINING)
            runtime = await self._store.get_runtime(strategy_id)
            if runtime.actual_state == MarketMakerLifecycle.DRAINING:
                return runtime
            handle = self._require_handle(strategy_id)
            try:
                await handle.set_mode(MarketMakerLifecycle.DRAINING)
                return await self._transition(instance, runtime, MarketMakerLifecycle.DRAINING, "operator_drain")
            except Exception as exc:
                await self._fault_locked(instance, exc)
                raise StrategyLifecycleError(f"Failed to drain strategy {strategy_id}") from exc

    async def stop(self, strategy_id: StrategyId) -> StrategyRuntimeState:
        async with self._locks[strategy_id]:
            instance = await self._store.set_desired(strategy_id, state=MarketMakerLifecycle.STOPPED)
            runtime = await self._store.get_runtime(strategy_id)
            if runtime.actual_state == MarketMakerLifecycle.STOPPED and strategy_id not in self._handles:
                await self._allocations.release(strategy_id)
                return runtime
            handle = self._handles.get(strategy_id)
            try:
                if handle is not None:
                    await handle.stop()
                runtime = await self._transition(
                    instance,
                    await self._store.get_runtime(strategy_id),
                    MarketMakerLifecycle.STOPPED,
                    "operator_stop",
                    recovery=True,
                )
                self._handles.pop(strategy_id, None)
                await self._allocations.release(strategy_id)
                return runtime
            except Exception as exc:
                await self._fault_locked(instance, exc)
                raise StrategyLifecycleError(f"Failed to stop strategy {strategy_id}") from exc

    async def fault(self, strategy_id: StrategyId, reason: str) -> StrategyRuntimeState:
        async with self._locks[strategy_id]:
            runtime = await self._store.get_runtime(strategy_id)
            if runtime.actual_state == MarketMakerLifecycle.FAULTED:
                return runtime
            instance = await self._store.set_desired(strategy_id, state=MarketMakerLifecycle.FAULTED)
            return await self._fault_locked(instance, StrategyLifecycleError(reason))

    async def recover(
        self,
        strategy_id: StrategyId,
        *,
        target: MarketMakerLifecycle = MarketMakerLifecycle.SHADOW,
    ) -> StrategyRuntimeState:
        """Explicit/manual recovery; FAULTED never resumes automatically."""
        async with self._locks[strategy_id]:
            runtime = await self._store.get_runtime(strategy_id)
            if runtime.actual_state != MarketMakerLifecycle.FAULTED:
                raise StrategyLifecycleError(f"Strategy {strategy_id} is not faulted")
            handle = self._handles.pop(strategy_id, None)
            if handle is not None:
                await handle.stop()
            instance = await self._store.get_instance(strategy_id)
            await self._transition(instance, runtime, MarketMakerLifecycle.STOPPED, "manual_recovery", recovery=True)
            return await self._start_locked(strategy_id, target)

    async def activate_config(
        self,
        strategy_id: StrategyId,
        config_revision: int,
        *,
        expected_revision: int | None = None,
    ) -> StrategyRuntimeState:
        """Set desired revision first; effective advances only after runtime acknowledgement."""
        async with self._locks[strategy_id]:
            config = await self._store.get_config(strategy_id, config_revision)
            instance = await self._store.set_desired(
                strategy_id,
                config_revision=config_revision,
                expected_revision=expected_revision,
            )
            runtime = await self._store.get_runtime(strategy_id)
            if runtime.actual_state == MarketMakerLifecycle.STOPPED:
                return runtime
            if runtime.effective_config_revision == config_revision:
                return runtime
            handle = self._require_handle(strategy_id)
            try:
                await handle.apply_config(config)
                return await self._store.set_runtime(
                    strategy_id,
                    effective_config_revision=config_revision,
                    set_effective_config=True,
                    reason="config_applied",
                    expected_revision=runtime.revision,
                )
            except Exception as exc:
                await self._fault_locked(instance, exc)
                raise StrategyLifecycleError(f"Failed to apply config for strategy {strategy_id}") from exc

    async def restore(self) -> list[StrategyRuntimeState]:
        """Rebuild desired active runtimes after reconciliation on process restart."""
        restored: list[StrategyRuntimeState] = []
        for instance in await self._store.list_instances():
            if instance.desired_state == MarketMakerLifecycle.FAULTED:
                async with self._locks[instance.strategy_id]:
                    runtime = await self._store.get_runtime(instance.strategy_id)
                    if runtime.actual_state != MarketMakerLifecycle.STOPPED:
                        await self._allocations.acquire(
                            instance.strategy_id,
                            instance.sub_account,
                            instance.symbol,
                        )
                    if runtime.actual_state != MarketMakerLifecycle.FAULTED:
                        runtime = await self._transition(
                            instance,
                            runtime,
                            MarketMakerLifecycle.FAULTED,
                            "restart_fault_latched",
                            recovery=True,
                        )
                    restored.append(runtime)
                continue
            if instance.desired_state == MarketMakerLifecycle.STOPPED:
                async with self._locks[instance.strategy_id]:
                    runtime = await self._store.get_runtime(instance.strategy_id)
                    if runtime.actual_state != MarketMakerLifecycle.STOPPED:
                        runtime = await self._transition(
                            instance, runtime, MarketMakerLifecycle.STOPPED, "restart_stopped", recovery=True
                        )
                    await self._allocations.release(instance.strategy_id)
                restored.append(runtime)
                continue
            desired = instance.desired_state
            target = (
                MarketMakerLifecycle.RUNNING if desired == MarketMakerLifecycle.RUNNING else MarketMakerLifecycle.SHADOW
            )
            async with self._locks[instance.strategy_id]:
                runtime = await self._start_locked(instance.strategy_id, target, recovering=True)
            if desired == MarketMakerLifecycle.PAUSED:
                runtime = await self.pause(instance.strategy_id)
            elif desired == MarketMakerLifecycle.DRAINING:
                runtime = await self.drain(instance.strategy_id)
            restored.append(runtime)
        return restored

    def runtime_snapshot(self, strategy_id: StrategyId) -> Any | None:
        """Return a read-only runtime snapshot when the concrete handle exposes one."""
        handle = self._handles.get(strategy_id)
        snapshot = getattr(handle, "snapshot", None) if handle is not None else None
        return snapshot() if callable(snapshot) else None

    async def _start_locked(
        self,
        strategy_id: StrategyId,
        target: MarketMakerLifecycle,
        *,
        expected_revision: int | None = None,
        recovering: bool = False,
    ) -> StrategyRuntimeState:
        runtime = await self._store.get_runtime(strategy_id)
        if runtime.actual_state == MarketMakerLifecycle.FAULTED and not recovering:
            raise StrategyLifecycleError(f"Strategy {strategy_id} is faulted; use recover()")
        instance = await self._store.set_desired(
            strategy_id,
            state=target,
            expected_revision=expected_revision,
        )
        if (
            strategy_id in self._handles
            and runtime.actual_state == target
            and runtime.effective_config_revision == instance.desired_config_revision
        ):
            return runtime

        capabilities = self._registry.capabilities(instance.strategy_type)
        supports_shadow = True if capabilities is None else capabilities.supports_shadow
        if target == MarketMakerLifecycle.SHADOW and not supports_shadow:
            raise StrategyLifecycleError(
                f"Strategy type {instance.strategy_type} does not support shadow start"
            )
        if not supports_shadow and target != MarketMakerLifecycle.RUNNING:
            raise StrategyLifecycleError(
                f"Strategy type {instance.strategy_type} start target must be running"
            )

        await self._allocations.acquire(strategy_id, instance.sub_account, instance.symbol)
        config = await self._store.get_config(strategy_id, instance.desired_config_revision)
        try:
            runtime = await self._transition(
                instance,
                runtime,
                MarketMakerLifecycle.WARMING,
                "runtime_warming",
                recovery=True,
            )
            handle = self._handles.get(strategy_id)
            if handle is None:
                handle = self._registry.create(StrategyBuildContext(instance=instance, config=config))
                self._handles[strategy_id] = handle
                await handle.start()
            await handle.apply_config(config)
            runtime = await self._store.set_runtime(
                strategy_id,
                effective_config_revision=config.revision,
                set_effective_config=True,
                reason="config_applied",
                expected_revision=runtime.revision,
            )
            if supports_shadow:
                await handle.set_mode(MarketMakerLifecycle.SHADOW)
                runtime = await self._transition(instance, runtime, MarketMakerLifecycle.SHADOW, "runtime_shadow")
                if target == MarketMakerLifecycle.RUNNING:
                    runtime = await self._transition(
                        instance, runtime, MarketMakerLifecycle.RUNNING, "runtime_running"
                    )
                    await handle.set_mode(MarketMakerLifecycle.RUNNING)
            else:
                runtime = await self._transition(
                    instance, runtime, MarketMakerLifecycle.RUNNING, "runtime_running", recovery=True
                )
                await handle.set_mode(MarketMakerLifecycle.RUNNING)
            return runtime
        except Exception as exc:
            await self._fault_locked(instance, exc)
            raise StrategyLifecycleError(f"Failed to start strategy {strategy_id}") from exc

    async def _fault_locked(
        self,
        instance: StrategyInstanceDefinition,
        error: Exception,
    ) -> StrategyRuntimeState:
        runtime = await self._store.get_runtime(instance.strategy_id)
        if runtime.actual_state == MarketMakerLifecycle.FAULTED:
            return runtime
        handle = self._handles.get(instance.strategy_id)
        if handle is not None:
            try:
                await handle.set_mode(MarketMakerLifecycle.FAULTED)
            except Exception:
                logger.exception("strategy_fault_mode_failed", strategy_id=str(instance.strategy_id))
        runtime = await self._store.get_runtime(instance.strategy_id)
        return await self._transition(
            instance,
            runtime,
            MarketMakerLifecycle.FAULTED,
            str(error) or error.__class__.__name__,
            recovery=True,
        )

    async def _transition(
        self,
        instance: StrategyInstanceDefinition,
        runtime: StrategyRuntimeState,
        target: MarketMakerLifecycle,
        reason: str,
        *,
        recovery: bool = False,
    ) -> StrategyRuntimeState:
        if runtime.actual_state == target:
            return runtime
        if not recovery and target not in _ALLOWED_TRANSITIONS[runtime.actual_state]:
            raise StrategyLifecycleError(
                f"Invalid strategy transition: {runtime.actual_state.value} -> {target.value} "
                f"strategy_id={instance.strategy_id}"
            )
        updated = await self._store.set_runtime(
            instance.strategy_id,
            actual_state=target,
            reason=reason,
            expected_revision=runtime.revision,
        )
        logger.info(
            "strategy_lifecycle_changed",
            strategy_id=str(instance.strategy_id),
            old_state=runtime.actual_state.value,
            new_state=target.value,
            reason=reason,
            revision=updated.revision,
        )
        return updated

    def _require_handle(self, strategy_id: StrategyId) -> StrategyRuntimeHandle:
        handle = self._handles.get(strategy_id)
        if handle is None:
            raise StrategyLifecycleError(f"Strategy runtime is not active: {strategy_id}")
        return handle


class InMemoryStrategyStateStore:
    """Optimistic-revision state store for tests and local simulations."""

    def __init__(self) -> None:
        self._instances: dict[StrategyId, StrategyInstanceDefinition] = {}
        self._runtimes: dict[StrategyId, StrategyRuntimeState] = {}
        self._configs: dict[tuple[StrategyId, int], StrategyConfigSnapshot] = {}
        self._lock = asyncio.Lock()

    async def add_instance(
        self,
        instance: StrategyInstanceDefinition,
        configs: list[StrategyConfigSnapshot],
    ) -> None:
        async with self._lock:
            if instance.strategy_id in self._instances:
                raise StrategyRegistrationError(f"Strategy instance already exists: {instance.strategy_id}")
            if not configs:
                raise StrategyRegistrationError("At least one immutable config version is required")
            for config in configs:
                if config.strategy_id != instance.strategy_id:
                    raise StrategyRegistrationError("Config strategy_id does not match instance")
                self._configs[(config.strategy_id, config.revision)] = config
            if (instance.strategy_id, instance.desired_config_revision) not in self._configs:
                raise StrategyRegistrationError("Desired config revision is not registered")
            self._instances[instance.strategy_id] = instance
            self._runtimes[instance.strategy_id] = StrategyRuntimeState(instance.strategy_id)

    async def add_config(self, config: StrategyConfigSnapshot) -> None:
        async with self._lock:
            key = (config.strategy_id, config.revision)
            if key in self._configs:
                raise StrategyRegistrationError(f"Config revision already exists: {config.revision}")
            if config.strategy_id not in self._instances:
                raise StrategyRegistrationError(f"Unknown strategy instance: {config.strategy_id}")
            self._configs[key] = config

    async def list_instances(self) -> list[StrategyInstanceDefinition]:
        async with self._lock:
            return list(self._instances.values())

    async def get_instance(self, strategy_id: StrategyId) -> StrategyInstanceDefinition:
        async with self._lock:
            try:
                return self._instances[strategy_id]
            except KeyError as exc:
                raise StrategyRegistrationError(f"Unknown strategy instance: {strategy_id}") from exc

    async def get_runtime(self, strategy_id: StrategyId) -> StrategyRuntimeState:
        async with self._lock:
            try:
                return self._runtimes[strategy_id]
            except KeyError as exc:
                raise StrategyRegistrationError(f"Unknown strategy instance: {strategy_id}") from exc

    async def get_config(self, strategy_id: StrategyId, revision: int) -> StrategyConfigSnapshot:
        async with self._lock:
            try:
                return self._configs[(strategy_id, revision)]
            except KeyError as exc:
                raise StrategyRegistrationError(
                    f"Unknown config revision: strategy_id={strategy_id} revision={revision}"
                ) from exc

    async def set_desired(
        self,
        strategy_id: StrategyId,
        *,
        state: MarketMakerLifecycle | None = None,
        config_revision: int | None = None,
        expected_revision: int | None = None,
    ) -> StrategyInstanceDefinition:
        async with self._lock:
            current = self._instances[strategy_id]
            if expected_revision is not None and current.revision != expected_revision:
                raise StrategyLifecycleError(
                    f"Strategy revision conflict: expected={expected_revision} actual={current.revision}"
                )
            desired_config = config_revision if config_revision is not None else current.desired_config_revision
            if (strategy_id, desired_config) not in self._configs:
                raise StrategyRegistrationError(
                    f"Unknown config revision: strategy_id={strategy_id} revision={desired_config}"
                )
            desired_state = state or current.desired_state
            if desired_state == current.desired_state and desired_config == current.desired_config_revision:
                return current
            updated = replace(
                current,
                desired_state=desired_state,
                desired_config_revision=desired_config,
                revision=current.revision + 1,
            )
            self._instances[strategy_id] = updated
            return updated

    async def set_runtime(
        self,
        strategy_id: StrategyId,
        *,
        actual_state: MarketMakerLifecycle | None = None,
        effective_config_revision: int | None = None,
        set_effective_config: bool = False,
        reason: str | None = None,
        expected_revision: int | None = None,
    ) -> StrategyRuntimeState:
        async with self._lock:
            current = self._runtimes[strategy_id]
            if expected_revision is not None and current.revision != expected_revision:
                raise StrategyLifecycleError(
                    f"Runtime revision conflict: expected={expected_revision} actual={current.revision}"
                )
            target_state = actual_state or current.actual_state
            target_effective = effective_config_revision if set_effective_config else current.effective_config_revision
            if (
                target_state == current.actual_state
                and target_effective == current.effective_config_revision
                and reason == current.reason
            ):
                return current
            updated = replace(
                current,
                actual_state=target_state,
                effective_config_revision=target_effective,
                reason=reason,
                revision=current.revision + 1,
            )
            self._runtimes[strategy_id] = updated
            return updated


class InMemoryStrategyAllocationManager:
    """Exclusive `(sub_account, symbol)` leases with monotonic fencing."""

    def __init__(self) -> None:
        self._by_strategy: dict[StrategyId, StrategyAllocation] = {}
        self._by_scope: dict[tuple[SubAccount, Symbol], StrategyId] = {}
        self._next_fence = 1
        self._lock = asyncio.Lock()

    async def acquire(
        self,
        strategy_id: StrategyId,
        sub_account: SubAccount,
        symbol: Symbol,
    ) -> StrategyAllocation:
        async with self._lock:
            existing = self._by_strategy.get(strategy_id)
            if existing is not None:
                if existing.sub_account == sub_account and existing.symbol == symbol:
                    return existing
                raise StrategyLifecycleError(f"Strategy {strategy_id} already owns a different allocation")
            scope = (sub_account, symbol)
            owner = self._by_scope.get(scope)
            if owner is not None and owner != strategy_id:
                raise StrategyLifecycleError(
                    f"Allocation is already owned: sub_account={sub_account} symbol={symbol} owner={owner}"
                )
            allocation = StrategyAllocation(strategy_id, sub_account, symbol, self._next_fence)
            self._next_fence += 1
            self._by_strategy[strategy_id] = allocation
            self._by_scope[scope] = strategy_id
            return allocation

    async def release(self, strategy_id: StrategyId) -> None:
        async with self._lock:
            allocation = self._by_strategy.pop(strategy_id, None)
            if allocation is not None:
                self._by_scope.pop((allocation.sub_account, allocation.symbol), None)

    async def get(self, strategy_id: StrategyId) -> StrategyAllocation | None:
        async with self._lock:
            return self._by_strategy.get(strategy_id)
