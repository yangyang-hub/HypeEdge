"""Strategy base class — abstract interface for all trading strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

import structlog

from hypeedge.core.enums import StrategyStatus
from hypeedge.core.events import Event
from hypeedge.core.types import StrategyId

if TYPE_CHECKING:
    from hypeedge.core.events import EventBus
    from hypeedge.execution.engine import ExecutionClient

logger = structlog.get_logger(__name__)


class StrategyBase(ABC):
    """Abstract base class for all trading strategies.

    Subclass this to implement a strategy. The framework calls:
    1. on_start() once at startup
    2. on_event(event) for each subscribed event
    3. on_stop() once at shutdown

    Strategies submit orders through the injected ExecutionClient,
    never directly through the execution engine.
    """

    def __init__(
        self,
        strategy_id: StrategyId,
        event_bus: EventBus,
        execution_client: ExecutionClient,
    ) -> None:
        self.strategy_id = strategy_id
        self._event_bus = event_bus
        self._execution = execution_client
        self._status = StrategyStatus.STOPPED
        self._log = logger.bind(strategy_id=str(strategy_id))

    @abstractmethod
    async def on_start(self) -> None:
        """Called once when the strategy starts. Use for initialization."""
        ...

    def subscriptions(self) -> frozenset[str]:
        """Event types consumed by this strategy runner."""
        return frozenset()

    @abstractmethod
    async def on_event(self, event: Event) -> None:
        """Called for each event the strategy is subscribed to."""
        ...

    @abstractmethod
    async def on_stop(self) -> None:
        """Called once when the strategy stops. Use for cleanup."""
        ...

    @property
    def status(self) -> StrategyStatus:
        return self._status

    @status.setter
    def status(self, value: StrategyStatus) -> None:
        self._log.debug("strategy_status_change", old=self._status.value, new=value.value)
        self._status = value

    def get_status(self) -> dict[str, Any]:
        """Return strategy status info for monitoring."""
        return {
            "strategy_id": str(self.strategy_id),
            "status": self._status.value,
        }
