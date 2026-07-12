"""In-process async event bus using asyncio.Queue per subscriber."""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import structlog

from hypeedge.core.exceptions import EventBusBackpressureError

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class Event:
    """An event published to the event bus."""

    event_type: str
    payload: Any
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    correlation_id: str | None = None


# --- Event type constants ---
# Market data
EVENT_L2_BOOK_UPDATE = "L2BookUpdate"
EVENT_TRADE_UPDATE = "TradeUpdate"
EVENT_CANDLE_UPDATE = "CandleUpdate"
EVENT_FUNDING_UPDATE = "FundingUpdate"
EVENT_MID_PRICE_UPDATE = "MidPriceUpdate"
EVENT_EXTERNAL_REFERENCE_UPDATE = "ExternalReferenceUpdate"

# Market-making analytics (lossy, append-only ClickHouse projections)
EVENT_MM_FEATURE_SAMPLE = "MarketMakerFeatureSample"
EVENT_MM_QUOTE_DECISION = "MarketMakerQuoteDecision"
EVENT_MM_INVENTORY_SAMPLE = "MarketMakerInventorySample"
EVENT_MM_ACTION_CREDIT_SAMPLE = "MarketMakerActionCreditSample"
EVENT_MM_FILL_MARKOUT = "MarketMakerFillMarkout"

# Execution
EVENT_ORDER_SUBMITTED = "OrderSubmitted"
EVENT_ORDER_ACKNOWLEDGED = "OrderAcknowledged"
EVENT_ORDER_FILLED = "OrderFilled"
EVENT_ORDER_PARTIAL_FILL = "OrderPartialFill"
EVENT_ORDER_CANCELLED = "OrderCancelled"
EVENT_ORDER_REJECTED = "OrderRejected"
EVENT_ORDER_EXPIRED = "OrderExpired"

# Account
EVENT_POSITION_CHANGED = "PositionChanged"
EVENT_BALANCE_CHANGED = "BalanceChanged"
EVENT_ACCOUNT_STATE_UPDATE = "AccountStateUpdate"

# Strategy
EVENT_SIGNAL_GENERATED = "SignalGenerated"

# System
EVENT_RISK_CHECK_PASSED = "RiskCheckPassed"
EVENT_RISK_CHECK_FAILED = "RiskCheckFailed"
EVENT_KILL_SWITCH_TRIGGERED = "KillSwitchTriggered"
EVENT_RECONCILIATION_COMPLETE = "ReconciliationComplete"
EVENT_ACTION_CREDITS_LOW = "ActionCreditsLow"
EVENT_WS_CONNECTED = "WsConnected"
EVENT_WS_DISCONNECTED = "WsDisconnected"

# All event types for validation
ALL_EVENT_TYPES: set[str] = {
    EVENT_L2_BOOK_UPDATE,
    EVENT_TRADE_UPDATE,
    EVENT_CANDLE_UPDATE,
    EVENT_FUNDING_UPDATE,
    EVENT_MID_PRICE_UPDATE,
    EVENT_EXTERNAL_REFERENCE_UPDATE,
    EVENT_MM_FEATURE_SAMPLE,
    EVENT_MM_QUOTE_DECISION,
    EVENT_MM_INVENTORY_SAMPLE,
    EVENT_MM_ACTION_CREDIT_SAMPLE,
    EVENT_MM_FILL_MARKOUT,
    EVENT_ORDER_SUBMITTED,
    EVENT_ORDER_ACKNOWLEDGED,
    EVENT_ORDER_FILLED,
    EVENT_ORDER_PARTIAL_FILL,
    EVENT_ORDER_CANCELLED,
    EVENT_ORDER_REJECTED,
    EVENT_ORDER_EXPIRED,
    EVENT_POSITION_CHANGED,
    EVENT_BALANCE_CHANGED,
    EVENT_ACCOUNT_STATE_UPDATE,
    EVENT_SIGNAL_GENERATED,
    EVENT_RISK_CHECK_PASSED,
    EVENT_RISK_CHECK_FAILED,
    EVENT_KILL_SWITCH_TRIGGERED,
    EVENT_RECONCILIATION_COMPLETE,
    EVENT_ACTION_CREDITS_LOW,
    EVENT_WS_CONNECTED,
    EVENT_WS_DISCONNECTED,
}

LOSSY_EVENT_TYPES: set[str] = {
    EVENT_L2_BOOK_UPDATE,
    EVENT_TRADE_UPDATE,
    EVENT_CANDLE_UPDATE,
    EVENT_FUNDING_UPDATE,
    EVENT_MID_PRICE_UPDATE,
    EVENT_EXTERNAL_REFERENCE_UPDATE,
    EVENT_MM_FEATURE_SAMPLE,
    EVENT_MM_QUOTE_DECISION,
    EVENT_MM_INVENTORY_SAMPLE,
    EVENT_MM_ACTION_CREDIT_SAMPLE,
    EVENT_MM_FILL_MARKOUT,
}

RELIABLE_EVENT_TYPES: set[str] = ALL_EVENT_TYPES - LOSSY_EVENT_TYPES


class EventBus:
    """In-process async event bus.

    Uses asyncio.Queue per subscriber to decouple publishers from consumers.
    Market-data events are lossy: when a queue is full, the oldest event is
    dropped because strategies generally prefer the latest snapshot. Trading,
    risk, kill-switch, reconciliation, and account events are reliable: async
    publishers apply backpressure and sync publishers fail loudly instead of
    dropping state transitions.

    Usage:
        bus = EventBus()

        # Subscribe to specific event types
        queue = bus.subscribe(EVENT_L2_BOOK_UPDATE)
        async for event in queue_to_iter(queue):
            handle(event)

        # Subscribe to all events (for audit/logging)
        audit_queue = bus.subscribe_all()

        # Publish
        await bus.publish(Event(event_type=EVENT_L2_BOOK_UPDATE, payload=snapshot))
    """

    def __init__(
        self,
        queue_maxsize: int = 10_000,
        *,
        reliable_event_types: set[str] | None = None,
        lossy_event_types: set[str] | None = None,
    ) -> None:
        self._subscribers: dict[str, list[asyncio.Queue[Event]]] = defaultdict(list)
        self._wildcard_subscribers: list[asyncio.Queue[Event]] = []
        self._queue_maxsize = queue_maxsize
        self._reliable_event_types = reliable_event_types or RELIABLE_EVENT_TYPES
        self._lossy_event_types = lossy_event_types or LOSSY_EVENT_TYPES
        self._publish_count: int = 0
        self._drop_count: int = 0

    def subscribe(self, event_type: str, *, maxsize: int | None = None) -> asyncio.Queue[Event]:
        """Subscribe to events of a specific type. Returns a queue to read from."""
        q: asyncio.Queue[Event] = asyncio.Queue(maxsize=self._queue_maxsize if maxsize is None else maxsize)
        self._subscribers[event_type].append(q)
        logger.debug("event_bus_subscribe", event_type=event_type, queue_id=id(q))
        return q

    def subscribe_many(
        self,
        event_types: Iterable[str],
        *,
        maxsize: int | None = None,
    ) -> asyncio.Queue[Event]:
        """Subscribe one queue to declared event types, preserving their publish order."""
        q: asyncio.Queue[Event] = asyncio.Queue(maxsize=self._queue_maxsize if maxsize is None else maxsize)
        subscribed_types = frozenset(event_types)
        for event_type in subscribed_types:
            self._subscribers[event_type].append(q)
        logger.debug(
            "event_bus_subscribe_many",
            event_types=sorted(subscribed_types),
            queue_id=id(q),
        )
        return q

    def subscribe_all(self) -> asyncio.Queue[Event]:
        """Subscribe to all events (for audit/logging/metrics)."""
        q: asyncio.Queue[Event] = asyncio.Queue(maxsize=self._queue_maxsize)
        self._wildcard_subscribers.append(q)
        logger.debug("event_bus_subscribe_all", queue_id=id(q))
        return q

    def unsubscribe(self, event_type: str, queue: asyncio.Queue[Event]) -> None:
        """Remove a specific queue from an event type subscription.

        Call this when a subscriber is shutting down to prevent memory leaks
        from orphaned queues. For wildcard subscriptions use `unsubscribe_all()`
        or track the queue externally.
        """
        queues = self._subscribers.get(event_type)
        if queues:
            try:
                queues.remove(queue)
                logger.debug("event_bus_unsubscribe", event_type=event_type, queue_id=id(queue))
            except ValueError:
                pass  # Queue not in this subscription list

    def unsubscribe_many(self, event_types: Iterable[str], queue: asyncio.Queue[Event]) -> None:
        """Remove a queue from a declared group of event subscriptions."""
        for event_type in frozenset(event_types):
            self.unsubscribe(event_type, queue)

    def unsubscribe_wildcard(self, queue: asyncio.Queue[Event]) -> None:
        """Remove a specific queue from the wildcard subscription list."""
        try:
            self._wildcard_subscribers.remove(queue)
            logger.debug("event_bus_unsubscribe_wildcard", queue_id=id(queue))
        except ValueError:
            pass

    async def publish(self, event: Event) -> None:
        """Publish an event to all matching subscribers."""
        queues = self._subscribers.get(event.event_type, [])
        all_queues = queues + self._wildcard_subscribers

        for q in all_queues:
            if self._is_lossy(event.event_type):
                self._put_lossy(q, event)
            else:
                await q.put(event)

        self._publish_count += 1

    def publish_sync(self, event: Event) -> None:
        """Synchronous publish for use from sync contexts (e.g. callbacks)."""
        queues = self._subscribers.get(event.event_type, [])
        for q in queues + self._wildcard_subscribers:
            if self._is_lossy(event.event_type):
                self._put_lossy(q, event)
                continue
            try:
                q.put_nowait(event)
            except asyncio.QueueFull as exc:
                raise EventBusBackpressureError(event_type=event.event_type, queue_id=id(q)) from exc
        self._publish_count += 1

    @property
    def stats(self) -> dict[str, int]:
        """Return event bus statistics."""
        return {
            "publish_count": self._publish_count,
            "drop_count": self._drop_count,
            "event_types": len(self._subscribers),
            "subscribers": sum(len(qs) for qs in self._subscribers.values()) + len(self._wildcard_subscribers),
        }

    def unsubscribe_all(self) -> None:
        """Clear all subscriptions (used during shutdown)."""
        self._subscribers.clear()
        self._wildcard_subscribers.clear()

    def _is_lossy(self, event_type: str) -> bool:
        """Return whether this event type can drop older queued events."""
        return event_type in self._lossy_event_types and event_type not in self._reliable_event_types

    def is_lossy_event(self, event_type: str) -> bool:
        """Expose delivery semantics so consumers can isolate lossy mailboxes."""
        return self._is_lossy(event_type)

    def _put_lossy(self, queue: asyncio.Queue[Event], event: Event) -> None:
        """Put an event, dropping the oldest queued item if the subscriber is behind."""
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            with contextlib.suppress(asyncio.QueueEmpty):
                queue.get_nowait()
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(event)
            self._drop_count += 1
