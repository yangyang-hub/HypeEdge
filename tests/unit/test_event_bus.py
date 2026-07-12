"""Tests for the EventBus."""

import pytest

from hypeedge.core.events import (
    EVENT_L2_BOOK_UPDATE,
    EVENT_ORDER_ACKNOWLEDGED,
    EVENT_ORDER_SUBMITTED,
    Event,
    EventBus,
)
from hypeedge.core.exceptions import EventBusBackpressureError


@pytest.mark.asyncio
async def test_publish_and_subscribe(event_bus: EventBus):
    """Test basic publish/subscribe flow."""
    queue = event_bus.subscribe("TestEvent")

    event = Event(event_type="TestEvent", payload={"key": "value"})
    await event_bus.publish(event)

    received = queue.get_nowait()
    assert received.event_type == "TestEvent"
    assert received.payload == {"key": "value"}


@pytest.mark.asyncio
async def test_subscribe_only_receives_matching_events(event_bus: EventBus):
    """Subscribers only get events of their subscribed type."""
    queue_a = event_bus.subscribe("EventA")
    queue_b = event_bus.subscribe("EventB")

    await event_bus.publish(Event(event_type="EventA", payload="a"))
    await event_bus.publish(Event(event_type="EventB", payload="b"))

    assert queue_a.get_nowait().payload == "a"
    assert queue_b.get_nowait().payload == "b"

    assert queue_a.empty()
    assert queue_b.empty()


@pytest.mark.asyncio
async def test_subscribe_many_uses_one_ordered_queue_for_reliable_events(event_bus: EventBus):
    queue = event_bus.subscribe_many({EVENT_ORDER_SUBMITTED, EVENT_ORDER_ACKNOWLEDGED})

    await event_bus.publish(Event(event_type=EVENT_ORDER_SUBMITTED, payload="submitted"))
    await event_bus.publish(Event(event_type=EVENT_ORDER_ACKNOWLEDGED, payload="acknowledged"))

    assert queue.get_nowait().payload == "submitted"
    assert queue.get_nowait().payload == "acknowledged"
    event_bus.unsubscribe_many({EVENT_ORDER_SUBMITTED, EVENT_ORDER_ACKNOWLEDGED}, queue)
    assert event_bus.stats["subscribers"] == 0


@pytest.mark.asyncio
async def test_subscribe_all_receives_everything(event_bus: EventBus):
    """subscribe_all() receives all event types."""
    all_queue = event_bus.subscribe_all()

    await event_bus.publish(Event(event_type="EventA", payload="a"))
    await event_bus.publish(Event(event_type="EventB", payload="b"))

    received = [all_queue.get_nowait(), all_queue.get_nowait()]
    types = {e.event_type for e in received}
    assert types == {"EventA", "EventB"}


@pytest.mark.asyncio
async def test_drop_oldest_when_full(event_bus: EventBus):
    """When queue is full, oldest events are dropped."""
    small_bus = EventBus(queue_maxsize=2)
    queue = small_bus.subscribe(EVENT_L2_BOOK_UPDATE)

    # Fill the queue
    await small_bus.publish(Event(event_type=EVENT_L2_BOOK_UPDATE, payload="first"))
    await small_bus.publish(Event(event_type=EVENT_L2_BOOK_UPDATE, payload="second"))

    # Overflow — should drop oldest
    await small_bus.publish(Event(event_type=EVENT_L2_BOOK_UPDATE, payload="third"))

    items = []
    while not queue.empty():
        items.append(queue.get_nowait().payload)

    # "first" should be dropped, "second" and "third" remain
    assert "first" not in items
    assert "third" in items


def test_reliable_publish_sync_raises_when_full():
    """Reliable trading events must never silently drop older state."""
    small_bus = EventBus(queue_maxsize=1)
    small_bus.subscribe(EVENT_ORDER_SUBMITTED)

    small_bus.publish_sync(Event(event_type=EVENT_ORDER_SUBMITTED, payload="first"))

    with pytest.raises(EventBusBackpressureError):
        small_bus.publish_sync(Event(event_type=EVENT_ORDER_SUBMITTED, payload="second"))


@pytest.mark.asyncio
async def test_publish_sync(event_bus: EventBus):
    """Test synchronous publish (for use from callbacks)."""
    queue = event_bus.subscribe("TestEvent")

    event_bus.publish_sync(Event(event_type="TestEvent", payload="sync"))

    received = queue.get_nowait()
    assert received.payload == "sync"


@pytest.mark.asyncio
async def test_stats(event_bus: EventBus):
    """Test event bus statistics."""
    event_bus.subscribe("A")
    event_bus.subscribe("B")
    event_bus.subscribe_all()

    await event_bus.publish(Event(event_type="A", payload=1))
    await event_bus.publish(Event(event_type="A", payload=2))

    stats = event_bus.stats
    assert stats["publish_count"] == 2
    assert stats["event_types"] == 2  # "A" and "B"
    assert stats["subscribers"] == 3  # A + B + wildcard


@pytest.mark.asyncio
async def test_unsubscribe_all(event_bus: EventBus):
    """Test clearing all subscriptions."""
    queue = event_bus.subscribe("TestEvent")

    event_bus.unsubscribe_all()

    await event_bus.publish(Event(event_type="TestEvent", payload="should_not_receive"))

    assert queue.empty()


@pytest.mark.asyncio
async def test_unsubscribe_specific_queue(event_bus: EventBus):
    """Test removing a specific queue from an event type."""
    queue_a = event_bus.subscribe("TestEvent")
    queue_b = event_bus.subscribe("TestEvent")

    # Remove queue_a only
    event_bus.unsubscribe("TestEvent", queue_a)

    await event_bus.publish(Event(event_type="TestEvent", payload="after_unsub"))

    # queue_a should no longer receive events
    assert queue_a.empty()
    # queue_b should still receive events
    assert queue_b.get_nowait().payload == "after_unsub"


@pytest.mark.asyncio
async def test_unsubscribe_wildcard(event_bus: EventBus):
    """Test removing a specific wildcard queue."""
    wildcard_queue = event_bus.subscribe_all()

    event_bus.unsubscribe_wildcard(wildcard_queue)

    await event_bus.publish(Event(event_type="TestEvent", payload="after_unsub"))

    assert wildcard_queue.empty()


@pytest.mark.asyncio
async def test_unsubscribe_nonexistent_queue(event_bus: EventBus):
    """Unsubscribing a queue that doesn't exist should not raise."""
    import asyncio

    orphan_queue: asyncio.Queue[Event] = asyncio.Queue()
    # Should not raise
    event_bus.unsubscribe("TestEvent", orphan_queue)
    event_bus.unsubscribe_wildcard(orphan_queue)
