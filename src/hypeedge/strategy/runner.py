"""Sequential EventBus consumer for a single strategy."""

from __future__ import annotations

import asyncio
from typing import Any, Protocol, cast

import structlog

from hypeedge.core.events import Event, EventBus
from hypeedge.core.types import StrategyId

logger = structlog.get_logger(__name__)


class RunnableStrategy(Protocol):
    strategy_id: StrategyId

    def subscriptions(self) -> frozenset[str]: ...

    async def on_start(self) -> None: ...

    async def on_event(self, event: Event) -> None: ...

    async def on_stop(self) -> None: ...


class StrategyRunner:
    """Own subscriptions and deliver events sequentially to a strategy."""

    def __init__(self, strategy: RunnableStrategy, event_bus: EventBus) -> None:
        self._strategy = strategy
        self._event_bus = event_bus
        self._subscriptions: list[tuple[frozenset[str], asyncio.Queue[Event]]] = []
        self._stop_event = asyncio.Event()
        self._running = False

    async def run(self) -> None:
        declared = self._strategy.subscriptions()
        reliable_types = frozenset(
            event_type for event_type in declared if not self._event_bus.is_lossy_event(event_type)
        )
        lossy_types = frozenset(event_type for event_type in declared if self._event_bus.is_lossy_event(event_type))

        if reliable_types:
            self._subscriptions.append((reliable_types, self._event_bus.subscribe_many(reliable_types)))
        for event_type in lossy_types:
            event_types = frozenset({event_type})
            self._subscriptions.append((event_types, self._event_bus.subscribe(event_type, maxsize=1)))

        self._stop_event.clear()
        self._running = True
        readers: dict[asyncio.Task[Event], tuple[asyncio.Queue[Event], bool]] = {}
        stop_reader = asyncio.create_task(self._stop_event.wait(), name="strategy_runner_stop")
        started = False
        try:
            await self._strategy.on_start()
            started = True
            for event_types, queue in self._subscriptions:
                is_reliable = not any(self._event_bus.is_lossy_event(event_type) for event_type in event_types)
                task = asyncio.create_task(queue.get(), name="strategy_event_reader")
                readers[task] = (queue, is_reliable)

            logger.info(
                "strategy_runner_started",
                strategy_id=str(self._strategy.strategy_id),
                reliable_subscriptions=sorted(reliable_types),
                lossy_subscriptions=sorted(lossy_types),
            )
            while self._running:
                waiters: set[asyncio.Task[Any]] = set(readers)
                waiters.add(stop_reader)
                done, _ = await asyncio.wait(
                    waiters,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_reader in done:
                    break

                ready: list[tuple[bool, Event]] = []
                for completed in done:
                    if completed is stop_reader:
                        continue
                    event_reader = cast(asyncio.Task[Event], completed)
                    queue, is_reliable = readers.pop(event_reader)
                    ready.append((is_reliable, event_reader.result()))
                    next_reader = asyncio.create_task(queue.get(), name=event_reader.get_name())
                    readers[next_reader] = (queue, is_reliable)

                # Reliable lifecycle/safety facts always run before market-data notifications.
                ready.sort(key=lambda item: not item[0])
                for _, event in ready:
                    await self._strategy.on_event(event)
        except asyncio.CancelledError:
            raise
        finally:
            self._running = False
            stop_reader.cancel()
            for task in readers:
                task.cancel()
            await asyncio.gather(stop_reader, *readers, return_exceptions=True)
            for event_types, queue in self._subscriptions:
                self._event_bus.unsubscribe_many(event_types, queue)
            self._subscriptions.clear()
            if started:
                await self._strategy.on_stop()
            logger.info("strategy_runner_stopped", strategy_id=str(self._strategy.strategy_id))

    async def stop(self) -> None:
        self._running = False
        self._stop_event.set()
