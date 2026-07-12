"""Crash-safe outbox dispatch and durable control-event ingestion.

The outbox row is committed in the same transaction as its aggregate.  A
dispatcher leases rows in sequence order, publishes them to the SSE broker,
and only then marks them published.  A crash after publish is therefore an
at-least-once retry with the same database sequence, which clients can safely
deduplicate.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

import structlog
from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hypeedge.core.events import (
    EVENT_ACTION_CREDITS_LOW,
    EVENT_RISK_CHECK_FAILED,
    Event,
    EventBus,
)
from hypeedge.storage.postgres import OutboxEventRecord

logger = structlog.get_logger(__name__)

CONTROL_EVENT_TYPES = frozenset(
    {
        EVENT_ACTION_CREDITS_LOW,
        EVENT_RISK_CHECK_FAILED,
    }
)


@dataclass(frozen=True)
class DurableEvent:
    """Immutable event envelope whose sequence is assigned by Postgres."""

    sequence: int
    event_id: uuid.UUID
    event_type: str
    schema_version: int
    aggregate_type: str
    aggregate_id: str
    aggregate_revision: int
    correlation_id: str | None
    payload: dict[str, Any]
    occurred_at: datetime


@dataclass(frozen=True)
class ReplayBounds:
    """Current retained sequence range; ``None`` values mean an empty stream."""

    earliest: int | None
    latest: int | None


class DurableEventSink(Protocol):
    async def publish(self, event: DurableEvent) -> None:
        """Fan an already committed event out to connected consumers."""
        ...


class DurableOutboxStore(Protocol):
    async def claim_batch(self, worker_id: str, limit: int = 100) -> Sequence[DurableEvent]: ...

    async def mark_published(self, event: DurableEvent, worker_id: str) -> bool: ...

    async def release_claim(self, event: DurableEvent, worker_id: str, error: str) -> None: ...

    async def replay_bounds(self) -> ReplayBounds: ...

    async def read_after(
        self, after_sequence: int, up_to_sequence: int, limit: int = 500
    ) -> Sequence[DurableEvent]: ...


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _json_payload(value: Any) -> dict[str, Any]:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        value = dataclasses.asdict(value)
    elif hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    elif hasattr(value, "__dict__"):
        value = {key: item for key, item in vars(value).items() if not key.startswith("_")}
    if not isinstance(value, dict):
        value = {"value": value}
    return cast(dict[str, Any], json.loads(json.dumps(value, default=str)))


class PostgresOutboxStore:
    """Postgres implementation using short ``FOR UPDATE SKIP LOCKED`` leases."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        lease_seconds: int = 30,
    ) -> None:
        self._session_factory = session_factory
        self._lease_seconds = lease_seconds

    async def claim_batch(self, worker_id: str, limit: int = 100) -> Sequence[DurableEvent]:
        now = _utcnow()
        lease_cutoff = now - timedelta(seconds=self._lease_seconds)
        async with self._session_factory() as session, session.begin():
            records = (
                (
                    await session.execute(
                        select(OutboxEventRecord)
                        .where(
                            OutboxEventRecord.published_at.is_(None),
                            or_(
                                OutboxEventRecord.claimed_at.is_(None),
                                OutboxEventRecord.claimed_at < lease_cutoff,
                            ),
                        )
                        .order_by(OutboxEventRecord.sequence)
                        .limit(limit)
                        .with_for_update(skip_locked=True)
                    )
                )
                .scalars()
                .all()
            )
            for record in records:
                record.claimed_at = now
                record.claimed_by = worker_id
                record.publish_attempts += 1
                record.last_publish_error = None
            return [self._to_event(record) for record in records]

    async def mark_published(self, event: DurableEvent, worker_id: str) -> bool:
        async with self._session_factory() as session, session.begin():
            result = cast(
                CursorResult[Any],
                await session.execute(
                    update(OutboxEventRecord)
                    .where(
                        OutboxEventRecord.sequence == event.sequence,
                        OutboxEventRecord.event_id == event.event_id,
                        OutboxEventRecord.published_at.is_(None),
                        OutboxEventRecord.claimed_by == worker_id,
                    )
                    .values(
                        published_at=_utcnow(),
                        claimed_at=None,
                        claimed_by=None,
                        last_publish_error=None,
                    )
                ),
            )
            return result.rowcount == 1

    async def release_claim(self, event: DurableEvent, worker_id: str, error: str) -> None:
        async with self._session_factory() as session, session.begin():
            await session.execute(
                update(OutboxEventRecord)
                .where(
                    OutboxEventRecord.sequence == event.sequence,
                    OutboxEventRecord.event_id == event.event_id,
                    OutboxEventRecord.published_at.is_(None),
                    OutboxEventRecord.claimed_by == worker_id,
                )
                .values(claimed_at=None, claimed_by=None, last_publish_error=error[:2_000])
            )

    async def replay_bounds(self) -> ReplayBounds:
        async with self._session_factory() as session:
            earliest, latest = (
                await session.execute(
                    select(func.min(OutboxEventRecord.sequence), func.max(OutboxEventRecord.sequence))
                )
            ).one()
        return ReplayBounds(
            earliest=int(earliest) if earliest is not None else None,
            latest=int(latest) if latest is not None else None,
        )

    async def read_after(
        self,
        after_sequence: int,
        up_to_sequence: int,
        limit: int = 500,
    ) -> Sequence[DurableEvent]:
        async with self._session_factory() as session:
            records = (
                (
                    await session.execute(
                        select(OutboxEventRecord)
                        .where(
                            OutboxEventRecord.sequence > after_sequence,
                            OutboxEventRecord.sequence <= up_to_sequence,
                        )
                        .order_by(OutboxEventRecord.sequence)
                        .limit(limit)
                    )
                )
                .scalars()
                .all()
            )
        return [self._to_event(record) for record in records]

    async def append_control_event(self, event: Event) -> None:
        """Idempotently persist an in-process control event before SSE delivery."""

        try:
            event_id = uuid.UUID(event.event_id)
        except ValueError:
            event_id = uuid.uuid5(uuid.NAMESPACE_URL, f"hypeedge:event:{event.event_id}")
        occurred_at = event.timestamp if event.timestamp.tzinfo is not None else event.timestamp.replace(tzinfo=UTC)
        statement = (
            pg_insert(OutboxEventRecord)
            .values(
                event_id=event_id,
                event_type=event.event_type,
                aggregate_type="system",
                aggregate_id=event.event_type,
                aggregate_revision=int(occurred_at.timestamp() * 1_000_000),
                correlation_id=event.correlation_id,
                payload=_json_payload(event.payload),
                occurred_at=occurred_at,
            )
            .on_conflict_do_nothing(index_elements=[OutboxEventRecord.event_id])
        )
        async with self._session_factory() as session, session.begin():
            await session.execute(statement)

    @staticmethod
    def _to_event(record: OutboxEventRecord) -> DurableEvent:
        return DurableEvent(
            sequence=record.sequence,
            event_id=record.event_id,
            event_type=record.event_type,
            schema_version=record.schema_version,
            aggregate_type=record.aggregate_type,
            aggregate_id=record.aggregate_id,
            aggregate_revision=record.aggregate_revision,
            correlation_id=record.correlation_id,
            payload=dict(record.payload),
            occurred_at=record.occurred_at,
        )


class OutboxDispatcher:
    """At-least-once dispatcher with stable sequence-based deduplication."""

    def __init__(
        self,
        store: DurableOutboxStore,
        sink: DurableEventSink,
        *,
        worker_id: str | None = None,
        batch_size: int = 100,
        poll_interval: float = 0.1,
    ) -> None:
        self._store = store
        self._sink = sink
        self._worker_id = worker_id or f"outbox-{uuid.uuid4()}"
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._running = False

    async def run(self) -> None:
        self._running = True
        while self._running:
            dispatched = await self.dispatch_once()
            if dispatched == 0:
                await asyncio.sleep(self._poll_interval)

    async def stop(self) -> None:
        self._running = False

    async def dispatch_once(self) -> int:
        events = await self._store.claim_batch(self._worker_id, self._batch_size)
        dispatched = 0
        for index, event in enumerate(events):
            try:
                await self._sink.publish(event)
                marked = await self._store.mark_published(event, self._worker_id)
                if not marked:
                    raise RuntimeError("outbox_publish_lease_lost")
                dispatched += 1
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await self._store.release_claim(event, self._worker_id, "dispatcher_cancelled")
                raise
            except Exception as exc:
                logger.exception("outbox_dispatch_failed", sequence=event.sequence, event_type=event.event_type)
                await self._store.release_claim(event, self._worker_id, type(exc).__name__)
                for unattempted in events[index + 1 :]:
                    await self._store.release_claim(unattempted, self._worker_id, "earlier_sequence_failed")
                break
        return dispatched


class DurableControlEventWriter:
    """Persist selected EventBus control events without making SSE depend on the bus."""

    def __init__(self, event_bus: EventBus, store: PostgresOutboxStore) -> None:
        self._event_bus = event_bus
        self._store = store
        self._queue: asyncio.Queue[Event] | None = None
        self._running = False

    def start(self) -> None:
        """Subscribe synchronously so startup control events cannot race the task."""

        if self._queue is None:
            self._queue = self._event_bus.subscribe_all()

    async def run(self) -> None:
        self.start()
        assert self._queue is not None
        self._running = True
        try:
            while self._running:
                event = await self._queue.get()
                if event.event_type in CONTROL_EVENT_TYPES:
                    while self._running:
                        try:
                            await self._store.append_control_event(event)
                            break
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            logger.exception("control_event_persist_failed", event_type=event.event_type)
                            await asyncio.sleep(0.25)
        finally:
            if self._queue is not None:
                self._event_bus.unsubscribe_wildcard(self._queue)
                self._queue = None

    async def stop(self) -> None:
        self._running = False
