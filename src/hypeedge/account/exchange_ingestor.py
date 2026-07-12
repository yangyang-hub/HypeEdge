"""Authenticated exchange event ingestion into the Postgres fact chain.

WebSocket delivery is the low-latency path. Incremental REST history is the
durability path after disconnects and restarts. Both converge through the same
inbox key and one transaction, so duplicates and reordering are harmless.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hypeedge.core.enums import OrderStatus, Side
from hypeedge.core.events import EVENT_ORDER_FILLED, EVENT_ORDER_PARTIAL_FILL, Event, EventBus
from hypeedge.core.models import Fill, Position
from hypeedge.core.types import Cloid, OrderId, Price, Size, StrategyId, SubAccount, Symbol, Timestamp, Usd
from hypeedge.storage.postgres import (
    ExchangeSyncCursorRecord,
    FillRecord,
    InboxEventRecord,
    LedgerEntryRecord,
    OrderEvent,
    OrderRecord,
    OutboxEventRecord,
    PositionRecord,
    RiskReservationRecord,
)

logger = structlog.get_logger(__name__)

if TYPE_CHECKING:
    from hypeedge.account.health import MutableAccountHealthProvider
    from hypeedge.account.tracker import AccountTracker
    from hypeedge.execution.engine import ExecutionEngine

SOURCE = "hyperliquid"
OPEN_STATUSES = {"pending", "submitted", "submit_unknown", "acknowledged", "partial_fill"}
TERMINAL_STATUSES = {"filled", "cancelled", "rejected", "expired"}


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _decimal(value: Any, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    return Decimal(str(value))


def _canonical_payload(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    normalized = json.loads(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str))
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest(), normalized


def _synthetic_cloid(exchange_oid: str) -> str:
    return "0x" + hashlib.md5(f"exchange-order:{exchange_oid}".encode(), usedforsecurity=False).hexdigest()


def fill_external_id(fill: dict[str, Any]) -> str:
    """Return the stable exchange identity used by inbox and fills."""

    if fill.get("tid") is not None:
        return f"fill:{fill['tid']}"
    return "fill:" + ":".join(str(fill.get(key, "")) for key in ("hash", "oid", "time", "coin", "side", "px", "sz"))


def fill_position_after(fill: dict[str, Any]) -> Decimal:
    start = _decimal(fill.get("startPosition"))
    signed_size = _decimal(fill.get("sz")) * (Decimal(1) if str(fill.get("side", "")).upper() == "B" else Decimal(-1))
    return start + signed_size


def projected_entry_price(
    old_size: Decimal,
    old_entry: Decimal | None,
    new_size: Decimal,
    fill_price: Decimal,
) -> Decimal | None:
    """Average-cost entry projection; exchange reconciliation remains authoritative."""

    if new_size == 0:
        return None
    if old_size == 0 or old_size * new_size < 0:
        return fill_price
    if old_size * new_size > 0 and abs(new_size) > abs(old_size):
        if old_entry is None:
            return fill_price
        added = abs(new_size) - abs(old_size)
        return ((abs(old_size) * old_entry) + (added * fill_price)) / abs(new_size)
    return old_entry


def _status(raw: Any) -> str:
    value = str(raw or "").lower()
    return {
        "open": "acknowledged",
        "filled": "filled",
        "canceled": "cancelled",
        "cancelled": "cancelled",
        "rejected": "rejected",
        "expired": "expired",
        "triggered": "acknowledged",
    }.get(value, "acknowledged")


@dataclass(frozen=True)
class IngestResult:
    processed: bool
    external_event_id: str
    fill_projection: CommittedFillProjection | None = None


@dataclass(frozen=True)
class CommittedFillProjection:
    """Values committed by the fact transaction and safe for live projection."""

    external_event_id: str
    cloid: str
    exchange_oid: str
    symbol: str
    side: str
    price: Decimal
    size: Decimal
    fee: Decimal
    is_maker: bool
    occurred_at: datetime
    strategy_id: str | None
    sub_account: str | None
    position_size: Decimal
    position_entry_price: Decimal | None
    position_mark_price: Decimal | None
    order_status: str


class ExchangeFactProjector:
    """Transactional projector shared by live and recovery ingestion."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], account_address: str) -> None:
        self._session_factory = session_factory
        self._account = account_address.lower()

    async def ingest_fill(self, fill: dict[str, Any]) -> IngestResult:
        external_id = fill_external_id(fill)
        payload_hash, payload = _canonical_payload(fill)
        occurred_at = datetime.fromtimestamp(int(fill["time"]) / 1000, tz=UTC)
        async with self._session_factory() as session, session.begin():
            inbox_id = await self._claim_inbox(session, external_id, "fill", payload_hash, payload)
            if inbox_id is None:
                return IngestResult(False, external_id)

            exchange_oid = str(fill["oid"])
            order = await self._find_or_create_order(session, exchange_oid, fill)
            fill_id = uuid.uuid4()
            size = _decimal(fill["sz"])
            price = _decimal(fill["px"])
            fee = _decimal(fill.get("fee"))
            realized_pnl = _decimal(fill.get("closedPnl"))
            session.add(
                FillRecord(
                    fill_id=fill_id,
                    source=SOURCE,
                    exchange_fill_id=external_id,
                    order_id=order.order_id,
                    cloid=order.cloid,
                    exchange_oid=exchange_oid,
                    symbol=str(fill["coin"]),
                    side="buy" if str(fill["side"]).upper() == "B" else "sell",
                    price=price,
                    size=size,
                    fee=fee,
                    realized_pnl=realized_pnl,
                    is_maker=not bool(fill.get("crossed", False)),
                    strategy_id=order.strategy_id,
                    sub_account=order.sub_account or self._account,
                    occurred_at=occurred_at,
                    timestamp=occurred_at,
                    raw_event=payload,
                )
            )
            await self._apply_fill_to_order(session, order, size, price, occurred_at, payload)
            reservation = (
                await session.execute(
                    select(RiskReservationRecord)
                    .where(RiskReservationRecord.order_id == order.order_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if reservation is not None and reservation.status == "active":
                original_size = reservation.reserved_size
                reservation.reserved_size = max(Decimal(0), original_size - size)
                if original_size > 0:
                    reservation.reserved_notional *= reservation.reserved_size / original_size
                if reservation.reserved_size == 0 or order.status == "filled":
                    reservation.status = "consumed"
                    reservation.released_at = occurred_at
            position = await self._apply_fill_to_position(session, order, fill, price, realized_pnl, occurred_at)
            for entry_type, amount in (("realized_pnl", realized_pnl), ("fee", -fee)):
                session.add(
                    LedgerEntryRecord(
                        fill_id=fill_id,
                        entry_type=entry_type,
                        amount=amount,
                        sub_account=order.sub_account or self._account,
                        strategy_id=order.strategy_id,
                        occurred_at=occurred_at,
                        metadata_={"exchange_oid": exchange_oid, "symbol": str(fill["coin"])},
                    )
                )
            session.add(
                OutboxEventRecord(
                    event_type="exchange.fill.ingested",
                    aggregate_type="order",
                    aggregate_id=str(order.order_id),
                    aggregate_revision=order.revision,
                    payload={
                        "fill_id": str(fill_id),
                        "cloid": order.cloid,
                        "symbol": str(fill["coin"]),
                        "price": str(price),
                        "size": str(size),
                        "position_size": str(position.size),
                    },
                    occurred_at=occurred_at,
                )
            )
            await self._advance_cursor(session, "fills", int(fill["time"]), external_id)
            inbox = await session.get(InboxEventRecord, inbox_id)
            assert inbox is not None
            inbox.processed_at = _utcnow()
        return IngestResult(
            True,
            external_id,
            CommittedFillProjection(
                external_event_id=external_id,
                cloid=order.cloid,
                exchange_oid=exchange_oid,
                symbol=str(fill["coin"]),
                side="buy" if str(fill["side"]).upper() == "B" else "sell",
                price=price,
                size=size,
                fee=fee,
                is_maker=not bool(fill.get("crossed", False)),
                occurred_at=occurred_at,
                strategy_id=order.strategy_id,
                sub_account=order.sub_account or self._account,
                position_size=position.size,
                position_entry_price=position.entry_price,
                position_mark_price=position.mark_price,
                order_status=order.status,
            ),
        )

    async def ingest_order_update(self, update: dict[str, Any]) -> IngestResult:
        raw_order = update.get("order", update)
        exchange_oid = str(raw_order["oid"])
        timestamp_ms = int(update.get("statusTimestamp", raw_order.get("timestamp", 0)))
        event_status = _status(update.get("status", raw_order.get("status")))
        external_id = f"order:{exchange_oid}:{event_status}:{timestamp_ms}"
        payload_hash, payload = _canonical_payload(update)
        async with self._session_factory() as session, session.begin():
            inbox_id = await self._claim_inbox(session, external_id, "order_update", payload_hash, payload)
            if inbox_id is None:
                return IngestResult(False, external_id)
            order = await self._find_or_create_order(session, exchange_oid, raw_order)
            actual_cloid = str(raw_order.get("cloid") or "")
            if actual_cloid.startswith("0x") and len(actual_cloid) == 34 and order.cloid != actual_cloid:
                collision = (
                    await session.execute(select(OrderRecord.id).where(OrderRecord.cloid == actual_cloid))
                ).scalar_one_or_none()
                if collision is None:
                    order.legacy_cloid = order.cloid
                    order.cloid = actual_cloid
            order.symbol = str(raw_order.get("coin", order.symbol))
            order.side = "buy" if str(raw_order.get("side", "B")).upper() == "B" else "sell"
            order.size = _decimal(raw_order.get("origSz", raw_order.get("sz", order.size)))
            limit_px = _decimal(raw_order.get("limitPx"))
            order.price = limit_px if limit_px > 0 else order.price
            # Older open messages must never regress a terminal/fill projection.
            if order.status not in TERMINAL_STATUSES or event_status in TERMINAL_STATUSES:
                order.status = event_status
            order.filled_size = min(order.size, max(order.filled_size, order.size - _decimal(raw_order.get("sz"))))
            order.revision += 1
            if event_status in TERMINAL_STATUSES:
                reservation = (
                    await session.execute(
                        select(RiskReservationRecord)
                        .where(RiskReservationRecord.order_id == order.order_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if reservation is not None and reservation.status == "active":
                    reservation.status = "consumed" if event_status == "filled" else "released"
                    reservation.released_at = _utcnow()
            occurred_at = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC) if timestamp_ms else _utcnow()
            session.add(
                OrderEvent(
                    order_id=order.order_id,
                    cloid=order.cloid,
                    revision=order.revision,
                    event_type="exchange_order_update",
                    symbol=order.symbol,
                    side=order.side,
                    size=order.size,
                    price=order.price,
                    status=order.status,
                    strategy_id=order.strategy_id,
                    payload=payload,
                    created_at=occurred_at,
                )
            )
            session.add(
                OutboxEventRecord(
                    event_type="exchange.order.updated",
                    aggregate_type="order",
                    aggregate_id=str(order.order_id),
                    aggregate_revision=order.revision,
                    payload={"cloid": order.cloid, "exchange_oid": exchange_oid, "status": order.status},
                    occurred_at=occurred_at,
                )
            )
            await self._advance_cursor(session, "orders", timestamp_ms, external_id)
            inbox = await session.get(InboxEventRecord, inbox_id)
            assert inbox is not None
            inbox.processed_at = _utcnow()
        return IngestResult(True, external_id)

    async def _claim_inbox(
        self,
        session: AsyncSession,
        external_id: str,
        event_type: str,
        payload_hash: str,
        payload: dict[str, Any],
    ) -> int | None:
        statement = (
            pg_insert(InboxEventRecord)
            .values(
                source=SOURCE,
                external_event_id=external_id,
                event_type=event_type,
                payload_hash=payload_hash,
                payload=payload,
            )
            .on_conflict_do_nothing(index_elements=["source", "external_event_id"])
            .returning(InboxEventRecord.id)
        )
        return (await session.execute(statement)).scalar_one_or_none()

    async def _find_or_create_order(
        self, session: AsyncSession, exchange_oid: str, payload: dict[str, Any]
    ) -> OrderRecord:
        order = (
            await session.execute(select(OrderRecord).where(OrderRecord.exchange_oid == exchange_oid).with_for_update())
        ).scalar_one_or_none()
        if order is not None:
            return order
        size = _decimal(payload.get("origSz", payload.get("sz")), "0.000000000000000001")
        if size <= 0:
            size = Decimal("0.000000000000000001")
        cloid = str(payload.get("cloid") or "")
        if not cloid.startswith("0x") or len(cloid) != 34:
            cloid = _synthetic_cloid(exchange_oid)
        order = OrderRecord(
            order_id=uuid.uuid4(),
            cloid=cloid,
            exchange_oid=exchange_oid,
            symbol=str(payload.get("coin", "UNKNOWN")),
            side="buy" if str(payload.get("side", "B")).upper() == "B" else "sell",
            order_type="limit",
            time_in_force="Gtc",
            size=size,
            price=_decimal(payload.get("limitPx")) or _decimal(payload.get("px")) or None,
            status="acknowledged",
            sub_account=self._account,
            revision=0,
        )
        session.add(order)
        await session.flush()
        return order

    async def _apply_fill_to_order(
        self,
        session: AsyncSession,
        order: OrderRecord,
        fill_size: Decimal,
        fill_price: Decimal,
        occurred_at: datetime,
        payload: dict[str, Any],
    ) -> None:
        previous_filled = order.filled_size
        new_filled = min(order.size, previous_filled + fill_size)
        if new_filled > 0:
            order.avg_fill_price = (
                (previous_filled * (order.avg_fill_price or fill_price)) + (fill_size * fill_price)
            ) / (previous_filled + fill_size)
        order.filled_size = new_filled
        order.status = "filled" if new_filled >= order.size else "partial_fill"
        order.filled_at = occurred_at if order.status == "filled" else None
        order.revision += 1
        session.add(
            OrderEvent(
                order_id=order.order_id,
                cloid=order.cloid,
                revision=order.revision,
                event_type="exchange_fill",
                symbol=order.symbol,
                side=order.side,
                size=fill_size,
                price=fill_price,
                status=order.status,
                strategy_id=order.strategy_id,
                payload=payload,
                created_at=occurred_at,
            )
        )

    async def _apply_fill_to_position(
        self,
        session: AsyncSession,
        order: OrderRecord,
        fill: dict[str, Any],
        fill_price: Decimal,
        realized_pnl: Decimal,
        occurred_at: datetime,
    ) -> PositionRecord:
        position = (
            await session.execute(
                select(PositionRecord)
                .where(
                    PositionRecord.sub_account == (order.sub_account or self._account),
                    PositionRecord.symbol == order.symbol,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        old_size = _decimal(fill.get("startPosition"))
        new_size = fill_position_after(fill)
        if position is None:
            position = PositionRecord(
                position_id=uuid.uuid4(),
                sub_account=order.sub_account or self._account,
                symbol=order.symbol,
                realized_pnl=Decimal(0),
                revision=0,
            )
            session.add(position)
        old_entry = position.entry_price
        position.entry_price = projected_entry_price(old_size, old_entry, new_size, fill_price)
        position.size = new_size
        position.mark_price = fill_price
        position.realized_pnl += realized_pnl
        position.exchange_updated_at = occurred_at
        position.revision += 1
        return position

    async def _advance_cursor(self, session: AsyncSession, stream: str, timestamp_ms: int, external_id: str) -> None:
        statement = pg_insert(ExchangeSyncCursorRecord).values(
            source=SOURCE,
            sub_account=self._account,
            stream=stream,
            last_exchange_timestamp_ms=max(0, timestamp_ms),
            last_external_event_id=external_id,
        )
        statement = statement.on_conflict_do_update(
            constraint="uq_exchange_sync_cursor_scope",
            set_={
                "last_exchange_timestamp_ms": statement.excluded.last_exchange_timestamp_ms,
                "last_external_event_id": statement.excluded.last_external_event_id,
                "updated_at": _utcnow(),
            },
            where=ExchangeSyncCursorRecord.last_exchange_timestamp_ms <= statement.excluded.last_exchange_timestamp_ms,
        )
        await session.execute(statement)

    async def cursor(self, stream: str) -> int:
        async with self._session_factory() as session:
            value = (
                await session.execute(
                    select(ExchangeSyncCursorRecord.last_exchange_timestamp_ms).where(
                        ExchangeSyncCursorRecord.source == SOURCE,
                        ExchangeSyncCursorRecord.sub_account == self._account,
                        ExchangeSyncCursorRecord.stream == stream,
                    )
                )
            ).scalar_one_or_none()
            return int(value or 0)


class ExchangeEventIngestor:
    """Own the authenticated subscriptions and incremental REST gap recovery."""

    def __init__(
        self,
        info_client: Any,
        account_address: str,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        poll_interval_seconds: float = 30.0,
        tracker: AccountTracker | None = None,
        engine: ExecutionEngine | None = None,
        event_bus: EventBus | None = None,
        account_health: MutableAccountHealthProvider | None = None,
    ) -> None:
        self._info = info_client
        self._account = account_address
        self._projector = ExchangeFactProjector(session_factory, account_address)
        self._poll_interval = poll_interval_seconds
        self._tracker = tracker
        self._engine = engine
        self._event_bus = event_bus
        self._account_health = account_health
        self._queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(maxsize=10_000)
        self._running = False
        self._history_recovered = False
        self._subscriptions: list[tuple[dict[str, str], int]] = []

    async def run(self) -> None:
        self._running = True
        loop = asyncio.get_running_loop()

        def callback(message: dict[str, Any]) -> None:
            loop.call_soon_threadsafe(self._enqueue_message, message)

        for kind in ("userFills", "orderUpdates"):
            subscription = {"type": kind, "user": self._account}
            subscription_id = await asyncio.to_thread(self._info.subscribe, subscription, callback)
            self._subscriptions.append((subscription, subscription_id))
        if self._account_health is not None:
            from hypeedge.account.health import AccountHealthDimension

            self._account_health.record_success(AccountHealthDimension.USER_STREAM)
        if not self._history_recovered:
            await self.recover_history()
        poll_task = asyncio.create_task(self._poll_history(), name="exchange-history-recovery")
        try:
            while self._running:
                kind, payload = await self._queue.get()
                try:
                    if kind == "fill":
                        await self._ingest_fill(payload)
                    else:
                        await self._projector.ingest_order_update(payload)
                    if self._account_health is not None:
                        from hypeedge.account.health import AccountHealthDimension

                        self._account_health.record_success(AccountHealthDimension.USER_STREAM)
                finally:
                    self._queue.task_done()
        except asyncio.CancelledError:
            raise
        finally:
            poll_task.cancel()
            await asyncio.gather(poll_task, return_exceptions=True)
            for subscription, subscription_id in self._subscriptions:
                await asyncio.to_thread(self._info.unsubscribe, subscription, subscription_id)
            self._subscriptions.clear()
            self._running = False

    def _enqueue_message(self, message: dict[str, Any]) -> None:
        channel = message.get("channel")
        data = message.get("data", {})
        if channel == "userFills":
            for fill in data.get("fills", []):
                try:
                    self._queue.put_nowait(("fill", fill))
                except asyncio.QueueFull:
                    logger.error("exchange_event_queue_full", channel=channel)
                    self._record_stream_failure("user_fill_queue_overflow")
        elif channel == "orderUpdates":
            for update in data if isinstance(data, list) else [data]:
                try:
                    self._queue.put_nowait(("order", update))
                except asyncio.QueueFull:
                    logger.error("exchange_event_queue_full", channel=channel)
                    self._record_stream_failure("order_update_queue_overflow")

    async def recover_history(self) -> None:
        fill_cursor = await self._projector.cursor("fills")
        start_ms = max(0, fill_cursor - 1)
        end_ms = int(_utcnow().timestamp() * 1000)
        while start_ms <= end_ms:
            fills = await asyncio.to_thread(
                self._info.user_fills_by_time,
                self._account,
                start_ms,
                end_ms,
            )
            if not isinstance(fills, list):
                raise RuntimeError("invalid_user_fills_history_response")
            ordered = sorted(fills, key=lambda item: (int(item.get("time", 0)), fill_external_id(item)))
            for fill in ordered:
                await self._ingest_fill(fill)
            if len(ordered) < 2000:
                break
            if fill_cursor == 0:
                logger.warning("initial_fill_history_bootstrap_truncated", retained=len(ordered))
                break
            latest_ms = int(ordered[-1].get("time", 0))
            if latest_ms <= start_ms:
                raise RuntimeError("user_fills_history_cursor_not_advancing")
            start_ms = latest_ms

        orders = await asyncio.to_thread(self._info.historical_orders, self._account)
        if not isinstance(orders, list):
            raise RuntimeError("invalid_historical_orders_response")
        order_cursor = await self._projector.cursor("orders")
        if len(orders) >= 2000 and order_cursor:
            oldest_ms = min(
                int(item.get("statusTimestamp", item.get("order", {}).get("timestamp", 0))) for item in orders
            )
            if order_cursor < oldest_ms:
                raise RuntimeError("historical_orders_gap_exceeds_exchange_retention")
        for update in sorted(
            orders,
            key=lambda item: int(item.get("statusTimestamp", item.get("order", {}).get("timestamp", 0))),
        ):
            timestamp_ms = int(update.get("statusTimestamp", update.get("order", {}).get("timestamp", 0)))
            if timestamp_ms >= order_cursor:
                await self._projector.ingest_order_update(update)
        self._history_recovered = True

    async def _ingest_fill(self, fill_payload: dict[str, Any]) -> IngestResult:
        """Commit a fill, then update process projections and publish its domain event."""
        result = await self._projector.ingest_fill(fill_payload)
        if self._tracker is None and self._engine is None and self._event_bus is None:
            return result
        projection = result.fill_projection
        if not result.processed or projection is None:
            return result

        fill = Fill(
            cloid=Cloid(projection.cloid),
            exchange_oid=OrderId(projection.exchange_oid),
            symbol=Symbol(projection.symbol),
            side=Side(projection.side),
            price=Price(projection.price),
            size=Size(projection.size),
            fee=Usd(projection.fee),
            is_maker=projection.is_maker,
            timestamp=Timestamp(int(projection.occurred_at.timestamp() * 1000)),
            strategy_id=StrategyId(projection.strategy_id) if projection.strategy_id else None,
            sub_account=SubAccount(projection.sub_account) if projection.sub_account else None,
        )
        position = Position(
            symbol=fill.symbol,
            size=Size(projection.position_size),
            entry_price=Price(projection.position_entry_price) if projection.position_entry_price is not None else None,
            mark_price=Price(projection.position_mark_price)
            if projection.position_mark_price is not None
            else fill.price,
            sub_account=fill.sub_account,
            strategy_id=fill.strategy_id,
        )
        if self._tracker is not None:
            self._tracker.apply_authoritative_fill(projection.external_event_id, fill, position)
        if self._account_health is not None:
            from hypeedge.account.health import AccountHealthDimension

            self._account_health.record_success(AccountHealthDimension.INVENTORY)

        order = await self._engine.refresh_order_from_durable(projection.cloid) if self._engine is not None else None
        if order is not None and self._event_bus is not None:
            if order.status == OrderStatus.FILLED:
                event_type = EVENT_ORDER_FILLED
            elif order.status == OrderStatus.PARTIAL_FILL:
                event_type = EVENT_ORDER_PARTIAL_FILL
            else:
                return result
            await self._event_bus.publish(Event(event_type=event_type, payload=order, correlation_id=projection.cloid))
        return result

    async def _poll_history(self) -> None:
        while self._running:
            await asyncio.sleep(self._poll_interval)
            try:
                await self.recover_history()
            except Exception:
                logger.exception("exchange_history_recovery_failed")

    async def stop(self) -> None:
        self._running = False

    def _record_stream_failure(self, reason: str) -> None:
        if self._account_health is None:
            return
        from hypeedge.account.health import AccountHealthDimension

        self._account_health.record_failure(AccountHealthDimension.USER_STREAM, reason)
