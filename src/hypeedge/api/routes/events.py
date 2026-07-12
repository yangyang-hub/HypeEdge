"""Durable Postgres-backed SSE replay and isolated live fan-out."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections import deque
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import structlog
from fastapi import APIRouter, Depends, Header, Request
from fastapi.responses import StreamingResponse

from hypeedge.api.auth import require_viewer
from hypeedge.api.deps import AppDep
from hypeedge.api.schemas import decimal_string
from hypeedge.storage.outbox import DurableEvent, DurableOutboxStore

logger = structlog.get_logger(__name__)

router = APIRouter(tags=["events"], dependencies=[Depends(require_viewer)])


def _precise_payload(value: Any) -> Any:  # noqa: ANN401
    if isinstance(value, (float, Decimal)):
        return decimal_string(value)
    if isinstance(value, dict):
        return {str(key): _precise_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_precise_payload(item) for item in value]
    return value


_LEGACY_SSE_EVENT_TYPES = {
    "OrderSubmitted",
    "OrderAcknowledged",
    "OrderFilled",
    "OrderPartialFill",
    "OrderCancelled",
    "OrderRejected",
    "PositionChanged",
    "BalanceChanged",
    "AccountStateUpdate",
    "SignalGenerated",
    "RiskCheckPassed",
    "RiskCheckFailed",
    "KillSwitchTriggered",
    "ReconciliationComplete",
    "ActionCreditsLow",
    "WsConnected",
    "WsDisconnected",
}


@dataclass(frozen=True)
class BufferedEvent:
    sequence: int
    event_type: str
    data: str

    def encode(self) -> str:
        return f"id: {self.sequence}\nevent: {self.event_type}\nretry: 3000\ndata: {self.data}\n\n"


class SseBroker:
    """Many bounded client queues fed only by committed durable events in production."""

    def __init__(self, app: Any, replay_size: int = 1_000, client_queue_size: int = 256) -> None:
        self._app = app
        self._event_bus = app.event_bus
        self._store: DurableOutboxStore | None = getattr(app, "outbox_store", None)
        self._client_queue_size = client_queue_size
        self._source_queue: asyncio.Queue[Any] | None = None
        self._clients: dict[asyncio.Queue[BufferedEvent], int] = {}
        self._replay: deque[BufferedEvent] = deque(maxlen=replay_size)
        self._seen_order: deque[int] = deque(maxlen=max(1_000, replay_size * 2))
        self._seen_sequences: set[int] = set()
        self._sequence = 0
        self._task: asyncio.Task[None] | None = None

    @property
    def store(self) -> DurableOutboxStore | None:
        return self._store

    def start(self) -> None:
        if self._store is not None or (self._task is not None and not self._task.done()):
            return
        # Compatibility for monitor-only/test deployments without Postgres.
        self._source_queue = self._event_bus.subscribe_all()
        self._task = asyncio.create_task(self._run_legacy_bus(), name="api_sse_legacy_broker")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._source_queue is not None:
            self._event_bus.unsubscribe_wildcard(self._source_queue)
            self._source_queue = None
        self._clients.clear()

    def subscribe(self, after_sequence: int | None) -> tuple[asyncio.Queue[BufferedEvent], list[BufferedEvent]]:
        queue: asyncio.Queue[BufferedEvent] = asyncio.Queue(maxsize=self._client_queue_size)
        self._clients[queue] = after_sequence or 0
        replay = []
        if self._store is None:
            replay = [event for event in self._replay if after_sequence is None or event.sequence > after_sequence]
        return queue, replay

    def unsubscribe(self, queue: asyncio.Queue[BufferedEvent]) -> None:
        self._clients.pop(queue, None)

    def is_subscribed(self, queue: asyncio.Queue[BufferedEvent]) -> bool:
        return queue in self._clients

    def advance_client(self, queue: asyncio.Queue[BufferedEvent], sequence: int) -> None:
        if queue in self._clients:
            self._clients[queue] = max(self._clients[queue], sequence)

    async def publish(self, event: DurableEvent) -> None:
        """Fan out one committed sequence; a crash retry of that sequence is ignored."""

        if event.sequence in self._seen_sequences:
            return
        if len(self._seen_order) == self._seen_order.maxlen:
            oldest = self._seen_order.popleft()
            self._seen_sequences.discard(oldest)
        self._seen_order.append(event.sequence)
        self._seen_sequences.add(event.sequence)
        self._sequence = max(self._sequence, event.sequence)
        buffered = self._from_durable(event)
        self._replay.append(buffered)
        self._fan_out(buffered)

    def _fan_out(self, event: BufferedEvent) -> None:
        for client, cursor in tuple(self._clients.items()):
            if event.sequence <= cursor:
                continue
            if client.full():
                # Closing only this subscription makes the browser reconnect
                # with its last durable sequence; other clients are unaffected.
                self._clients.pop(client, None)
                continue
            client.put_nowait(event)
            self._clients[client] = event.sequence

    async def _run_legacy_bus(self) -> None:
        assert self._source_queue is not None
        while not self._app.is_shutting_down:
            event = await self._source_queue.get()
            if event.event_type not in _LEGACY_SSE_EVENT_TYPES:
                continue
            payload = event.payload
            if hasattr(payload, "__dict__"):
                payload = {key: value for key, value in payload.__dict__.items() if not key.startswith("_")}
            self._sequence += 1
            body = json.dumps(
                {
                    "schema_version": 1,
                    "sequence": self._sequence,
                    "event_type": event.event_type,
                    "payload": _precise_payload(payload),
                    "timestamp": datetime.now(UTC).isoformat(),
                    "correlation_id": event.correlation_id,
                },
                default=str,
            )
            buffered = BufferedEvent(self._sequence, event.event_type, body)
            self._replay.append(buffered)
            self._fan_out(buffered)

    @staticmethod
    def _from_durable(event: DurableEvent) -> BufferedEvent:
        body = json.dumps(
            {
                "schema_version": event.schema_version,
                "sequence": event.sequence,
                "event_id": str(event.event_id),
                "event_type": event.event_type,
                "payload": _precise_payload(event.payload),
                "timestamp": event.occurred_at.isoformat(),
                "correlation_id": event.correlation_id,
            },
            separators=(",", ":"),
            default=str,
        )
        return BufferedEvent(event.sequence, event.event_type, body)


def _get_broker(app: Any) -> SseBroker:
    broker = getattr(app, "_api_sse_broker", None)
    if broker is None:
        broker = SseBroker(app)
        app._api_sse_broker = broker
    broker.start()
    return broker


def _resync_event(requested: int, earliest: int | None, latest: int) -> BufferedEvent:
    body = json.dumps(
        {
            "schema_version": 1,
            "sequence": latest,
            "event_type": "StreamResyncRequired",
            "payload": {
                "reason": "retention_gap",
                "requested_after": requested,
                "earliest_available": earliest,
                "latest_available": latest,
            },
            "timestamp": datetime.now(UTC).isoformat(),
            "correlation_id": None,
        },
        separators=(",", ":"),
    )
    return BufferedEvent(latest, "StreamResyncRequired", body)


async def _durable_replay(
    broker: SseBroker,
    queue: asyncio.Queue[BufferedEvent],
    after_sequence: int | None,
) -> AsyncGenerator[BufferedEvent, None]:
    store = broker.store
    assert store is not None
    bounds = await store.replay_bounds()
    latest = bounds.latest or 0
    broker.advance_client(queue, latest)
    if after_sequence is None:
        return
    if after_sequence > latest or (
        after_sequence > 0 and bounds.earliest is not None and after_sequence < bounds.earliest - 1
    ):
        yield _resync_event(after_sequence, bounds.earliest, latest)
        return

    cursor = after_sequence
    while cursor < latest:
        page = await store.read_after(cursor, latest)
        if not page:
            break
        for event in page:
            buffered = SseBroker._from_durable(event)
            cursor = event.sequence
            yield buffered


async def _event_stream(
    request: Request,
    app: Any,
    after_sequence: int | None,
) -> AsyncGenerator[str, None]:
    broker = _get_broker(app)
    queue, replay = broker.subscribe(after_sequence)
    last_sent = after_sequence or 0
    try:
        # Flush immediately so reverse proxies (e.g. Next.js) do not buffer the
        # response until the first durable event or 15s heartbeat arrives.
        yield ": connected\n\n"
        if broker.store is not None:
            try:
                async for event in _durable_replay(broker, queue, after_sequence):
                    if event.event_type == "StreamResyncRequired" or event.sequence >= last_sent:
                        last_sent = event.sequence
                        yield event.encode()
            except Exception:
                logger.exception("sse_durable_replay_failed", after_sequence=after_sequence)
                yield "event: error\ndata: {\"detail\":\"sse_replay_unavailable\"}\n\n"
        else:
            for event in replay:
                last_sent = max(last_sent, event.sequence)
                yield event.encode()
        while not app.is_shutting_down and broker.is_subscribed(queue) and not await request.is_disconnected():
            try:
                event = await asyncio.wait_for(queue.get(), timeout=15.0)
            except TimeoutError:
                yield ": heartbeat\n\n"
                continue
            if event.sequence <= last_sent:
                continue
            last_sent = event.sequence
            yield event.encode()
    finally:
        broker.unsubscribe(queue)


@router.get("/events")
async def sse_events(
    request: Request,
    app: AppDep,
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
) -> StreamingResponse:
    """Stream committed events and resume from the Postgres sequence."""

    after_sequence: int | None = None
    if last_event_id:
        with contextlib.suppress(ValueError):
            parsed = int(last_event_id)
            if parsed >= 0:
                after_sequence = parsed
    return StreamingResponse(
        _event_stream(request, app, after_sequence),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
