"""Regression tests for exchange fact identity and out-of-order projection math."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from hypeedge.account.exchange_ingestor import (
    CommittedFillProjection,
    ExchangeEventIngestor,
    IngestResult,
    _canonical_payload,
    _synthetic_cloid,
    fill_external_id,
    fill_position_after,
    projected_entry_price,
)
from hypeedge.account.tracker import AccountTracker
from hypeedge.core.enums import OrderStatus, OrderType, Side, TimeInForce
from hypeedge.core.events import EventBus
from hypeedge.core.models import Order
from hypeedge.core.types import Cloid, Price, Size, StrategyId, Symbol
from hypeedge.storage.postgres import ExchangeSyncCursorRecord, LedgerEntryRecord
from hypeedge.strategy.params import TrendParams
from hypeedge.strategy.runner import StrategyRunner
from hypeedge.strategy.trend_follow import TrendFollowStrategy


def _fill(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "coin": "BTC",
        "px": "100",
        "sz": "2",
        "side": "B",
        "time": 1_700_000_000_000,
        "startPosition": "-1",
        "hash": "0xabc",
        "oid": 42,
        "tid": 99,
    }
    value.update(overrides)
    return value


def _projection(**overrides: object) -> CommittedFillProjection:
    values: dict[str, object] = {
        "external_event_id": "fill:99",
        "cloid": "0x" + "a" * 32,
        "exchange_oid": "42",
        "symbol": "BTC",
        "side": "buy",
        "price": Decimal("100"),
        "size": Decimal("2"),
        "fee": Decimal("0.1"),
        "is_maker": True,
        "occurred_at": datetime.fromtimestamp(1_700_000_000, tz=UTC),
        "strategy_id": "trend",
        "sub_account": "0xaccount",
        "position_size": Decimal("2"),
        "position_entry_price": Decimal("100"),
        "position_mark_price": Decimal("100"),
        "order_status": "filled",
    }
    values.update(overrides)
    return CommittedFillProjection(**values)  # type: ignore[arg-type]


def test_fill_identity_is_stable_across_live_and_rest_payload_order() -> None:
    first = _fill()
    second = dict(reversed(list(first.items())))

    assert fill_external_id(first) == fill_external_id(second) == "fill:99"
    assert _canonical_payload(first)[0] == _canonical_payload(second)[0]


def test_fill_identity_has_deterministic_fallback_without_tid() -> None:
    fill = _fill()
    del fill["tid"]

    assert fill_external_id(fill) == fill_external_id(dict(fill))


def test_fill_before_order_uses_restart_stable_placeholder_cloid() -> None:
    cloid = _synthetic_cloid("42")

    assert cloid == _synthetic_cloid("42")
    assert cloid.startswith("0x")
    assert len(cloid) == 34


def test_position_after_fill_uses_exchange_start_position() -> None:
    assert fill_position_after(_fill()) == Decimal("1")
    assert fill_position_after(_fill(side="S", startPosition="1", sz="2")) == Decimal("-1")


def test_projected_entry_price_handles_add_reduce_close_and_flip() -> None:
    assert projected_entry_price(Decimal(0), None, Decimal(2), Decimal(100)) == Decimal(100)
    assert projected_entry_price(Decimal(2), Decimal(100), Decimal(3), Decimal(130)) == Decimal(110)
    assert projected_entry_price(Decimal(3), Decimal(110), Decimal(1), Decimal(90)) == Decimal(110)
    assert projected_entry_price(Decimal(1), Decimal(110), Decimal(0), Decimal(90)) is None
    assert projected_entry_price(Decimal(1), Decimal(110), Decimal(-1), Decimal(90)) == Decimal(90)


def test_fact_chain_schema_has_dedup_and_restart_constraints() -> None:
    ledger_constraints = {constraint.name for constraint in LedgerEntryRecord.__table__.constraints}
    cursor_constraints = {constraint.name for constraint in ExchangeSyncCursorRecord.__table__.constraints}

    assert "uq_ledger_entries_fill_type" in ledger_constraints
    assert "uq_exchange_sync_cursor_scope" in cursor_constraints


async def test_restart_recovery_replays_cursor_overlap_for_inbox_dedup() -> None:
    info = MagicMock()
    info.user_fills_by_time.return_value = [_fill(time=1000)]
    info.historical_orders.return_value = []
    ingestor = ExchangeEventIngestor(info, "0xAccount", MagicMock())
    projector = MagicMock()
    projector.cursor = AsyncMock(side_effect=[1000, 0])
    projector.ingest_fill = AsyncMock()
    projector.ingest_order_update = AsyncMock()
    ingestor._projector = projector

    await ingestor.recover_history()

    assert info.user_fills_by_time.call_args.args[:3] == ("0xAccount", 999, info.user_fills_by_time.call_args.args[2])
    projector.ingest_fill.assert_awaited_once()


async def test_restart_fails_closed_when_order_history_has_retention_gap() -> None:
    info = MagicMock()
    info.user_fills_by_time.return_value = []
    info.historical_orders.return_value = [
        {"statusTimestamp": 2000 + index, "order": {"oid": index}} for index in range(2000)
    ]
    ingestor = ExchangeEventIngestor(info, "0xAccount", MagicMock())
    projector = MagicMock()
    projector.cursor = AsyncMock(side_effect=[0, 1000])
    projector.ingest_fill = AsyncMock()
    projector.ingest_order_update = AsyncMock()
    ingestor._projector = projector

    with pytest.raises(RuntimeError, match="retention"):
        await ingestor.recover_history()


async def test_resting_fill_converges_tracker_engine_and_strategy_after_commit() -> None:
    bus = EventBus()
    tracker = AccountTracker()
    cloid = "0x" + "a" * 32
    committed_order = Order(
        cloid=Cloid(cloid),
        symbol=Symbol("BTC"),
        side=Side.BUY,
        size=Size(2),
        price=Price(100),
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        status=OrderStatus.FILLED,
        strategy_id=StrategyId("trend"),
        filled_size=Size(2),
        avg_fill_price=Price(100),
    )
    engine = MagicMock()
    engine.refresh_order_from_durable = AsyncMock(return_value=committed_order)
    ingestor = ExchangeEventIngestor(
        MagicMock(),
        "0xAccount",
        MagicMock(),
        tracker=tracker,
        engine=engine,
        event_bus=bus,
    )
    ingestor._projector = MagicMock()
    ingestor._projector.ingest_fill = AsyncMock(return_value=IngestResult(True, "fill:99", _projection(cloid=cloid)))
    strategy = TrendFollowStrategy(
        StrategyId("trend"),
        bus,
        AsyncMock(),
        TrendParams(symbol="BTC"),
        account_tracker=tracker,
    )
    runner = StrategyRunner(strategy, bus)
    task = asyncio.create_task(runner.run())
    await asyncio.sleep(0)
    strategy._working_order_cloid = cloid

    await ingestor._ingest_fill(_fill(cloid=cloid))
    for _ in range(5):
        if strategy.position_size == 2:
            break
        await asyncio.sleep(0)

    position = tracker.get_position(Symbol("BTC"))
    assert position is not None and position.size == Size(2)
    assert strategy.position_size == 2
    assert strategy._working_order_cloid is None
    engine.refresh_order_from_durable.assert_awaited_once_with(cloid)
    await runner.stop()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_duplicate_committed_fill_does_not_reapply_live_projection() -> None:
    tracker = AccountTracker()
    engine = MagicMock()
    engine.refresh_order_from_durable = AsyncMock(return_value=None)
    ingestor = ExchangeEventIngestor(
        MagicMock(), "0xAccount", MagicMock(), tracker=tracker, engine=engine, event_bus=EventBus()
    )
    ingestor._projector = MagicMock()
    ingestor._projector.ingest_fill = AsyncMock(
        side_effect=[
            IngestResult(True, "fill:99", _projection()),
            IngestResult(False, "fill:99"),
        ]
    )

    await ingestor._ingest_fill(_fill())
    await ingestor._ingest_fill(_fill())

    assert tracker.fill_count == 1
    assert tracker.total_fees == pytest.approx(0.1)
    engine.refresh_order_from_durable.assert_awaited_once()


async def test_post_commit_live_projection_failure_propagates_fail_closed() -> None:
    tracker = AccountTracker()
    engine = MagicMock()
    engine.refresh_order_from_durable = AsyncMock(side_effect=RuntimeError("process projection failed"))
    ingestor = ExchangeEventIngestor(
        MagicMock(), "0xAccount", MagicMock(), tracker=tracker, engine=engine, event_bus=EventBus()
    )
    ingestor._projector = MagicMock()
    ingestor._projector.ingest_fill = AsyncMock(return_value=IngestResult(True, "fill:99", _projection()))

    with pytest.raises(RuntimeError, match="process projection failed"):
        await ingestor._ingest_fill(_fill())

    assert tracker.get_position(Symbol("BTC")) is not None
