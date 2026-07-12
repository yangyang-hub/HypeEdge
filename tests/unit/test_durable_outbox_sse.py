"""Failure-path tests for durable outbox delivery and SSE replay."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from hypeedge.api.routes.events import SseBroker, _event_stream
from hypeedge.core.events import EVENT_ACTION_CREDITS_LOW, Event, EventBus
from hypeedge.storage.outbox import (
    DurableControlEventWriter,
    DurableEvent,
    OutboxDispatcher,
    ReplayBounds,
)
from hypeedge.storage.postgres import OutboxEventRecord, PostgresReconciliationStore


def _event(sequence: int, event_type: str = "order.filled") -> DurableEvent:
    return DurableEvent(
        sequence=sequence,
        event_id=uuid.uuid5(uuid.NAMESPACE_URL, f"event:{sequence}"),
        event_type=event_type,
        schema_version=1,
        aggregate_type="order",
        aggregate_id="order-1",
        aggregate_revision=sequence,
        correlation_id="cloid-1",
        payload={"sequence": sequence},
        occurred_at=datetime(2026, 7, 11, tzinfo=UTC),
    )


class FakeOutboxStore:
    def __init__(self, events: list[DurableEvent]) -> None:
        self.events = events
        self.published: set[int] = set()
        self.claimed: set[int] = set()
        self.fail_mark_once = False
        self.releases: list[tuple[int, str]] = []

    async def claim_batch(self, worker_id: str, limit: int = 100) -> list[DurableEvent]:
        del worker_id
        claimed = [
            event
            for event in self.events
            if event.sequence not in self.published and event.sequence not in self.claimed
        ]
        claimed = claimed[:limit]
        self.claimed.update(event.sequence for event in claimed)
        return claimed

    async def mark_published(self, event: DurableEvent, worker_id: str) -> bool:
        del worker_id
        if self.fail_mark_once:
            self.fail_mark_once = False
            raise RuntimeError("database_disconnected_after_publish")
        self.claimed.discard(event.sequence)
        self.published.add(event.sequence)
        return True

    async def release_claim(self, event: DurableEvent, worker_id: str, error: str) -> None:
        del worker_id
        self.claimed.discard(event.sequence)
        self.releases.append((event.sequence, error))

    async def replay_bounds(self) -> ReplayBounds:
        if not self.events:
            return ReplayBounds(None, None)
        return ReplayBounds(self.events[0].sequence, self.events[-1].sequence)

    async def read_after(self, after_sequence: int, up_to_sequence: int, limit: int = 500) -> list[DurableEvent]:
        return [event for event in self.events if after_sequence < event.sequence <= up_to_sequence][:limit]


class FailingSink:
    def __init__(self, broker: SseBroker) -> None:
        self.broker = broker
        self.fail_once = True

    async def publish(self, event: DurableEvent) -> None:
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("fanout_failed")
        await self.broker.publish(event)


def _app(store: FakeOutboxStore) -> SimpleNamespace:
    return SimpleNamespace(event_bus=EventBus(), outbox_store=store, is_shutting_down=False)


@pytest.mark.asyncio
async def test_dispatcher_retries_sink_failure_without_marking_published() -> None:
    store = FakeOutboxStore([_event(1)])
    broker = SseBroker(_app(store))
    queue, _ = broker.subscribe(None)
    dispatcher = OutboxDispatcher(store, FailingSink(broker), worker_id="worker")

    assert await dispatcher.dispatch_once() == 0
    assert store.published == set()
    assert store.releases == [(1, "RuntimeError")]
    assert await dispatcher.dispatch_once() == 1
    assert (await queue.get()).sequence == 1
    assert store.published == {1}


@pytest.mark.asyncio
async def test_crash_after_fanout_before_mark_does_not_duplicate_connected_client() -> None:
    store = FakeOutboxStore([_event(1)])
    store.fail_mark_once = True
    broker = SseBroker(_app(store))
    queue, _ = broker.subscribe(None)
    dispatcher = OutboxDispatcher(store, broker, worker_id="worker")

    assert await dispatcher.dispatch_once() == 0
    assert (await queue.get()).sequence == 1
    assert await dispatcher.dispatch_once() == 1
    assert queue.empty()
    assert store.published == {1}


@pytest.mark.asyncio
async def test_durable_replay_reads_unpublished_committed_rows_after_restart() -> None:
    store = FakeOutboxStore([_event(4), _event(7)])
    app = _app(store)
    request = SimpleNamespace(is_disconnected=AsyncMock(return_value=True))
    stream = _event_stream(request, app, after_sequence=4)
    try:
        frame = await anext(stream)
        assert frame.startswith("id: 7\nevent: order.filled")
        assert json.loads(frame.split("data: ", 1)[1])["sequence"] == 7
    finally:
        await stream.aclose()
        await app._api_sse_broker.stop()


@pytest.mark.asyncio
async def test_retention_gap_emits_explicit_resync_at_latest_sequence() -> None:
    store = FakeOutboxStore([_event(10), _event(11)])
    app = _app(store)
    request = SimpleNamespace(is_disconnected=AsyncMock(return_value=True))
    stream = _event_stream(request, app, after_sequence=2)
    try:
        frame = await anext(stream)
        assert frame.startswith("id: 11\nevent: StreamResyncRequired")
        data = json.loads(frame.split("data: ", 1)[1])
        assert data["payload"] == {
            "reason": "retention_gap",
            "requested_after": 2,
            "earliest_available": 10,
            "latest_available": 11,
        }
    finally:
        await stream.aclose()
        await app._api_sse_broker.stop()


@pytest.mark.asyncio
async def test_client_sequence_ahead_of_database_is_reset_by_resync_event() -> None:
    store = FakeOutboxStore([_event(3), _event(5)])
    app = _app(store)
    request = SimpleNamespace(is_disconnected=AsyncMock(return_value=True))
    stream = _event_stream(request, app, after_sequence=99)
    try:
        frame = await anext(stream)
        assert frame.startswith("id: 5\nevent: StreamResyncRequired")
        assert json.loads(frame.split("data: ", 1)[1])["payload"]["latest_available"] == 5
    finally:
        await stream.aclose()
        await app._api_sse_broker.stop()


@pytest.mark.asyncio
async def test_durable_broker_isolates_multiple_clients_and_drops_only_slow_one() -> None:
    store = FakeOutboxStore([])
    broker = SseBroker(_app(store), client_queue_size=1)
    slow, _ = broker.subscribe(None)
    fast, _ = broker.subscribe(None)
    slow.put_nowait(SseBroker._from_durable(_event(1)))

    await broker.publish(_event(2))

    assert slow not in broker._clients
    assert fast in broker._clients
    assert (await fast.get()).sequence == 2


@pytest.mark.asyncio
async def test_control_writer_persists_non_transactional_control_event() -> None:
    event_bus = EventBus()
    store = SimpleNamespace(append_control_event=AsyncMock())
    writer = DurableControlEventWriter(event_bus, store)  # type: ignore[arg-type]
    writer.start()
    task = asyncio.create_task(writer.run())
    event = Event(event_type=EVENT_ACTION_CREDITS_LOW, payload={"remaining": 100})
    try:
        await event_bus.publish(event)
        for _ in range(20):
            if store.append_control_event.await_count:
                break
            await asyncio.sleep(0)
        store.append_control_event.assert_awaited_once_with(event)
    finally:
        await writer.stop()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_control_writer_retries_same_event_after_database_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import hypeedge.storage.outbox as outbox_module

    event_bus = EventBus()
    append = AsyncMock(side_effect=[RuntimeError("postgres_down"), None])
    store = SimpleNamespace(append_control_event=append)
    writer = DurableControlEventWriter(event_bus, store)  # type: ignore[arg-type]
    writer.start()
    real_sleep = asyncio.sleep
    monkeypatch.setattr(outbox_module.asyncio, "sleep", AsyncMock())
    task = asyncio.create_task(writer.run())
    event = Event(event_type=EVENT_ACTION_CREDITS_LOW, payload={"remaining": 10})
    try:
        await event_bus.publish(event)
        for _ in range(20):
            if append.await_count == 2:
                break
            await real_sleep(0)
        assert append.await_args_list[0].args == append.await_args_list[1].args == (event,)
    finally:
        await writer.stop()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_reconciliation_result_and_outbox_event_share_transaction() -> None:
    run = SimpleNamespace(
        status="running",
        completed_queries=[],
        error_code=None,
        error_message=None,
        finished_at=None,
    )

    class FakeTransaction:
        async def __aenter__(self) -> None:
            return None

        async def __aexit__(self, *args: object) -> None:
            return None

    class FakeSession:
        def __init__(self) -> None:
            self.added: list[object] = []

        async def __aenter__(self) -> FakeSession:
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        def begin(self) -> FakeTransaction:
            return FakeTransaction()

        async def execute(self, statement: object) -> MagicMock:
            del statement
            result = MagicMock()
            result.scalar_one.return_value = run
            return result

        def add(self, record: object) -> None:
            self.added.append(record)

    session = FakeSession()
    store = PostgresReconciliationStore(lambda: session, "0xabc")  # type: ignore[arg-type]
    run_id = uuid.uuid4()
    await store.finish(
        run_id,
        success=False,
        errors=["exchange_timeout"],
        diffs=[],
        exchange_positions={},
        exchange_account=None,
    )

    outbox = next(record for record in session.added if isinstance(record, OutboxEventRecord))
    assert outbox.event_type == "reconciliation.completed"
    assert outbox.aggregate_id == str(run_id)
    assert outbox.payload["success"] is False


def test_outbox_model_has_recoverable_delivery_lease() -> None:
    columns = OutboxEventRecord.__table__.c
    assert {"claimed_at", "claimed_by", "publish_attempts", "last_publish_error"} <= set(columns.keys())
    assert "ix_outbox_events_dispatch" in {index.name for index in OutboxEventRecord.__table__.indexes}
