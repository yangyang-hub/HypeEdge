"""Behavioral coverage for the V2 durable safety kernel.

These tests deliberately exercise failure boundaries with transaction-aware
fakes.  They do not assert SQL text; they assert the state that would be
committed (or left recoverable) at each boundary.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from hypeedge.account.exchange_ingestor import ExchangeEventIngestor, ExchangeFactProjector, _status
from hypeedge.core.enums import OrderStatus, OrderType, Side, TimeInForce
from hypeedge.core.models import Order, RiskCheckResult
from hypeedge.core.types import Cloid, Price, Size, SubAccount, Symbol
from hypeedge.execution.worker import SignedActionExecutor
from hypeedge.risk.checker import RiskLimits
from hypeedge.storage.outbox import (
    DurableControlEventWriter,
    DurableEvent,
    OutboxDispatcher,
    PostgresOutboxStore,
    ReplayBounds,
    _json_payload,
)
from hypeedge.storage.postgres import (
    ExecutionCommandRecord,
    OutboxEventRecord,
    PostgresDurableOrderStore,
    PostgresExecutionCommandQueue,
    PostgresSystemStateStore,
    RiskReservationRecord,
    SystemStateRecord,
)


class _Transaction:
    def __init__(self, session: _Session) -> None:
        self.session = session

    async def __aenter__(self) -> None:
        self.session.in_transaction = True

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.session.in_transaction = False
        self.session.committed = exc is None
        self.session.rolled_back = exc is not None


class _Result:
    def __init__(self, *, scalar: object = None, scalars: list[object] | None = None, rowcount: int = 1) -> None:
        self._scalar = scalar
        self._scalars = scalars or []
        self.rowcount = rowcount

    def scalar_one_or_none(self) -> object:
        return self._scalar

    def scalar_one(self) -> object:
        if self._scalar is None:
            raise AssertionError("expected one scalar")
        return self._scalar

    def scalars(self) -> _Result:
        return self

    def all(self) -> list[object]:
        return self._scalars

    def one(self) -> object:
        return self._scalar


class _Session:
    def __init__(self, results: list[_Result] | None = None, *, get_result: object = None) -> None:
        self.results = list(results or [])
        self.get_result = get_result
        self.added: list[object] = []
        self.executed: list[object] = []
        self.in_transaction = False
        self.committed = False
        self.rolled_back = False

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None

    def begin(self) -> _Transaction:
        return _Transaction(self)

    async def execute(self, statement: object) -> _Result:
        self.executed.append(statement)
        return self.results.pop(0) if self.results else _Result()

    async def get(self, model: object, key: object) -> object:
        del model, key
        return self.get_result

    async def flush(self) -> None:
        return None

    def add(self, record: object) -> None:
        assert self.in_transaction, "durable records must be added inside one transaction"
        self.added.append(record)


class _Factory:
    def __init__(self, *sessions: _Session) -> None:
        self.sessions = list(sessions)

    def __call__(self) -> _Session:
        if not self.sessions:
            raise AssertionError("unexpected session")
        return self.sessions.pop(0)


def _order(*, side: Side = Side.BUY, size: float = 1, reduce_only: bool = False) -> Order:
    return Order(
        cloid=Cloid("0x" + "a" * 32),
        symbol=Symbol("BTC"),
        side=side,
        size=Size(size),
        price=Price(100),
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        status=OrderStatus.SUBMITTED,
        sub_account=SubAccount("0xaccount"),
        reduce_only=reduce_only,
    )


def _durable_event(sequence: int) -> DurableEvent:
    return DurableEvent(
        sequence=sequence,
        event_id=uuid.uuid4(),
        event_type="order.updated",
        schema_version=1,
        aggregate_type="order",
        aggregate_id="order-1",
        aggregate_revision=sequence,
        correlation_id=None,
        payload={"sequence": sequence},
        occurred_at=datetime.now(UTC),
    )


async def test_fact_projector_duplicate_fill_short_circuits_without_side_effects() -> None:
    session = _Session()
    projector = ExchangeFactProjector(_Factory(session), "0xAccount")  # type: ignore[arg-type]
    projector._claim_inbox = AsyncMock(return_value=None)  # type: ignore[method-assign]
    projector._find_or_create_order = AsyncMock()  # type: ignore[method-assign]

    result = await projector.ingest_fill(
        {"tid": 7, "time": 1_700_000_000_000, "oid": 4, "coin": "BTC", "side": "B", "px": "100", "sz": "1"}
    )

    assert result.processed is False
    projector._find_or_create_order.assert_not_awaited()
    assert session.added == []
    assert session.committed is True


async def test_fact_projector_rolls_back_entire_fill_projection_on_failure() -> None:
    inbox = SimpleNamespace(processed_at=None)
    session = _Session(get_result=inbox)
    projector = ExchangeFactProjector(_Factory(session), "0xAccount")  # type: ignore[arg-type]
    order = SimpleNamespace(
        order_id=uuid.uuid4(),
        cloid="0x" + "b" * 32,
        strategy_id=None,
        sub_account="0xaccount",
        symbol="BTC",
        revision=0,
        status="acknowledged",
    )
    projector._claim_inbox = AsyncMock(return_value=9)  # type: ignore[method-assign]
    projector._find_or_create_order = AsyncMock(return_value=order)  # type: ignore[method-assign]
    projector._apply_fill_to_order = AsyncMock(side_effect=RuntimeError("projection_failed"))  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="projection_failed"):
        await projector.ingest_fill(
            {"tid": 8, "time": 1_700_000_000_000, "oid": 4, "coin": "BTC", "side": "B", "px": "100", "sz": "1"}
        )

    assert session.rolled_back is True
    assert inbox.processed_at is None
    assert any(record.__class__.__name__ == "FillRecord" for record in session.added)


async def test_fact_projector_success_commits_fill_ledger_outbox_cursor_and_inbox() -> None:
    inbox = SimpleNamespace(processed_at=None)
    reservation = SimpleNamespace(
        status="active", reserved_size=Decimal("2"), reserved_notional=Decimal("200"), released_at=None
    )
    session = _Session([_Result(scalar=reservation)], get_result=inbox)
    projector = ExchangeFactProjector(_Factory(session), "0xAccount")  # type: ignore[arg-type]
    order = SimpleNamespace(
        order_id=uuid.uuid4(),
        cloid="0x" + "b" * 32,
        strategy_id="trend",
        sub_account="0xaccount",
        symbol="BTC",
        revision=2,
        status="partial_fill",
    )
    position = SimpleNamespace(size=Decimal("1"), entry_price=Decimal("100"), mark_price=Decimal("100"))
    projector._claim_inbox = AsyncMock(return_value=9)  # type: ignore[method-assign]
    projector._find_or_create_order = AsyncMock(return_value=order)  # type: ignore[method-assign]
    projector._apply_fill_to_order = AsyncMock()  # type: ignore[method-assign]
    projector._apply_fill_to_position = AsyncMock(return_value=position)  # type: ignore[method-assign]
    projector._advance_cursor = AsyncMock()  # type: ignore[method-assign]

    result = await projector.ingest_fill(
        {
            "tid": 8,
            "time": 1_700_000_000_000,
            "oid": 4,
            "coin": "BTC",
            "side": "S",
            "px": "100",
            "sz": "1",
            "fee": "0.1",
            "closedPnl": "2",
            "crossed": True,
        }
    )

    assert result.processed is True and session.committed is True
    assert reservation.reserved_size == Decimal("1") and reservation.reserved_notional == Decimal("100")
    assert inbox.processed_at is not None
    assert [record.entry_type for record in session.added if record.__class__.__name__ == "LedgerEntryRecord"] == [
        "realized_pnl",
        "fee",
    ]
    assert any(isinstance(record, OutboxEventRecord) for record in session.added)


async def test_filled_order_consumes_remaining_reservation() -> None:
    inbox = SimpleNamespace(processed_at=None)
    reservation = SimpleNamespace(
        status="active", reserved_size=Decimal("1"), reserved_notional=Decimal("100"), released_at=None
    )
    session = _Session([_Result(scalar=reservation)], get_result=inbox)
    projector = ExchangeFactProjector(_Factory(session), "0xAccount")  # type: ignore[arg-type]
    order = SimpleNamespace(
        order_id=uuid.uuid4(),
        cloid="0x" + "b" * 32,
        strategy_id=None,
        sub_account="0xaccount",
        symbol="BTC",
        revision=2,
        status="filled",
    )
    projector._claim_inbox = AsyncMock(return_value=9)  # type: ignore[method-assign]
    projector._find_or_create_order = AsyncMock(return_value=order)  # type: ignore[method-assign]
    projector._apply_fill_to_order = AsyncMock()  # type: ignore[method-assign]
    projector._apply_fill_to_position = AsyncMock(
        return_value=SimpleNamespace(size=Decimal(0), entry_price=None, mark_price=Decimal("100"))
    )  # type: ignore[method-assign]
    projector._advance_cursor = AsyncMock()  # type: ignore[method-assign]
    await projector.ingest_fill(
        {"tid": 9, "time": 1_700_000_000_000, "oid": 4, "coin": "BTC", "side": "S", "px": "100", "sz": "1"}
    )
    assert reservation.status == "consumed" and reservation.released_at is not None


async def test_order_update_does_not_regress_terminal_order_on_late_open_event() -> None:
    inbox = SimpleNamespace(processed_at=None)
    session = _Session(get_result=inbox)
    projector = ExchangeFactProjector(_Factory(session), "0xAccount")  # type: ignore[arg-type]
    order = SimpleNamespace(
        order_id=uuid.uuid4(),
        cloid="0x" + "c" * 32,
        legacy_cloid=None,
        exchange_oid="42",
        symbol="BTC",
        side="buy",
        size=Decimal("2"),
        price=Decimal("100"),
        status="filled",
        filled_size=Decimal("2"),
        revision=3,
        strategy_id=None,
        sub_account="0xaccount",
    )
    projector._claim_inbox = AsyncMock(return_value=3)  # type: ignore[method-assign]
    projector._find_or_create_order = AsyncMock(return_value=order)  # type: ignore[method-assign]
    projector._advance_cursor = AsyncMock()  # type: ignore[method-assign]

    result = await projector.ingest_order_update(
        {
            "status": "open",
            "statusTimestamp": 2000,
            "order": {
                "oid": 42,
                "coin": "BTC",
                "side": "B",
                "origSz": "2",
                "sz": "1",
                "limitPx": "100",
            },
        }
    )

    assert result.processed is True
    assert order.status == "filled"
    assert order.filled_size == Decimal("2")
    assert inbox.processed_at is not None


async def test_order_update_terminal_releases_reservation_and_adopts_real_cloid() -> None:
    inbox = SimpleNamespace(processed_at=None)
    reservation = SimpleNamespace(status="active", released_at=None)
    session = _Session([_Result(scalar=None), _Result(scalar=reservation)], get_result=inbox)
    projector = ExchangeFactProjector(_Factory(session), "0xAccount")  # type: ignore[arg-type]
    synthetic = "0x" + "d" * 32
    actual = "0x" + "e" * 32
    order = SimpleNamespace(
        order_id=uuid.uuid4(),
        cloid=synthetic,
        legacy_cloid=None,
        exchange_oid="42",
        symbol="BTC",
        side="buy",
        size=Decimal("2"),
        price=None,
        status="acknowledged",
        filled_size=Decimal("0"),
        revision=0,
        strategy_id=None,
        sub_account="0xaccount",
    )
    projector._claim_inbox = AsyncMock(return_value=3)  # type: ignore[method-assign]
    projector._find_or_create_order = AsyncMock(return_value=order)  # type: ignore[method-assign]
    projector._advance_cursor = AsyncMock()  # type: ignore[method-assign]

    await projector.ingest_order_update(
        {
            "status": "canceled",
            "statusTimestamp": 3000,
            "order": {"oid": 42, "cloid": actual, "coin": "BTC", "side": "S", "origSz": "2", "sz": "1"},
        }
    )

    assert order.cloid == actual and order.legacy_cloid == synthetic
    assert order.status == "cancelled" and reservation.status == "released" and reservation.released_at is not None


def test_exchange_status_normalization_is_fail_safe_for_unknown_values() -> None:
    assert _status("filled") == "filled"
    assert _status("canceled") == "cancelled"
    assert _status("unexpected-new-status") == "acknowledged"


async def test_projector_find_create_and_projection_helpers_cover_partial_close_and_cursor() -> None:
    existing = SimpleNamespace(order_id=uuid.uuid4())
    session = _Session([_Result(scalar=existing)])
    projector = ExchangeFactProjector(MagicMock(), "0xAccount")
    assert await projector._find_or_create_order(session, "42", {}) is existing

    create_session = _Session([_Result(scalar=None)])
    create_session.in_transaction = True
    created = await projector._find_or_create_order(
        create_session, "43", {"coin": "ETH", "side": "S", "origSz": "0", "limitPx": "200"}
    )
    assert created.exchange_oid == "43" and created.side == "sell" and created.size > 0

    order = SimpleNamespace(
        order_id=uuid.uuid4(),
        cloid="0x" + "f" * 32,
        symbol="BTC",
        side="buy",
        strategy_id=None,
        sub_account="0xaccount",
        size=Decimal("2"),
        filled_size=Decimal("0.5"),
        avg_fill_price=Decimal("90"),
        status="acknowledged",
        filled_at=None,
        revision=0,
    )
    projection_session = _Session()
    projection_session.in_transaction = True
    await projector._apply_fill_to_order(
        projection_session, order, Decimal("0.5"), Decimal("110"), datetime.now(UTC), {"tid": 1}
    )
    assert order.filled_size == Decimal("1.0") and order.avg_fill_price == Decimal("100")
    assert order.status == "partial_fill"

    position_session = _Session([_Result(scalar=None)])
    position_session.in_transaction = True
    position = await projector._apply_fill_to_position(
        position_session,
        order,
        {"startPosition": "0", "sz": "1", "side": "B"},
        Decimal("110"),
        Decimal("3"),
        datetime.now(UTC),
    )
    assert position.size == Decimal("1") and position.entry_price == Decimal("110")

    cursor_session = _Session([_Result()])
    await projector._advance_cursor(cursor_session, "fills", -4, "fill:1")
    assert len(cursor_session.executed) == 1
    read_session = _Session([_Result(scalar=123)])
    projector = ExchangeFactProjector(_Factory(read_session), "0xAccount")  # type: ignore[arg-type]
    assert await projector.cursor("fills") == 123


def test_ingestor_normalizes_live_messages_and_drops_only_overflowing_items() -> None:
    ingestor = ExchangeEventIngestor(MagicMock(), "0xAccount", MagicMock())
    ingestor._queue = asyncio.Queue(maxsize=1)
    ingestor._enqueue_message({"channel": "userFills", "data": {"fills": [{"tid": 1}, {"tid": 2}]}})
    assert ingestor._queue.get_nowait() == ("fill", {"tid": 1})
    ingestor._enqueue_message({"channel": "orderUpdates", "data": {"order": {"oid": 3}}})
    assert ingestor._queue.get_nowait() == ("order", {"order": {"oid": 3}})
    ingestor._enqueue_message({"channel": "ignored", "data": {}})
    assert ingestor._queue.empty()


@pytest.mark.parametrize("fills", [None, {"not": "a-list"}])
async def test_history_recovery_rejects_invalid_fill_response(fills: object) -> None:
    info = MagicMock(user_fills_by_time=MagicMock(return_value=fills), historical_orders=MagicMock(return_value=[]))
    ingestor = ExchangeEventIngestor(info, "0xAccount", MagicMock())
    ingestor._projector = SimpleNamespace(cursor=AsyncMock(return_value=0))
    with pytest.raises(RuntimeError, match="invalid_user_fills"):
        await ingestor.recover_history()


async def test_history_recovery_rejects_nonadvancing_full_page() -> None:
    fills = [{"tid": index, "time": 0} for index in range(2000)]
    info = MagicMock(user_fills_by_time=MagicMock(return_value=fills), historical_orders=MagicMock(return_value=[]))
    ingestor = ExchangeEventIngestor(info, "0xAccount", MagicMock())
    ingestor._projector = SimpleNamespace(cursor=AsyncMock(return_value=1), ingest_fill=AsyncMock())
    with pytest.raises(RuntimeError, match="cursor_not_advancing"):
        await ingestor.recover_history()


async def test_history_bootstrap_full_page_is_explicitly_truncated_without_looping() -> None:
    fills = [{"tid": index, "time": index} for index in range(2000)]
    info = MagicMock(user_fills_by_time=MagicMock(return_value=fills), historical_orders=MagicMock(return_value=[]))
    ingestor = ExchangeEventIngestor(info, "0xAccount", MagicMock())
    projector = SimpleNamespace(
        cursor=AsyncMock(side_effect=[0, 0]), ingest_fill=AsyncMock(), ingest_order_update=AsyncMock()
    )
    ingestor._projector = projector
    await ingestor.recover_history()
    assert projector.ingest_fill.await_count == 2000
    assert info.user_fills_by_time.call_count == 1


async def test_history_rejects_invalid_order_response() -> None:
    info = MagicMock(user_fills_by_time=MagicMock(return_value=[]), historical_orders=MagicMock(return_value=None))
    ingestor = ExchangeEventIngestor(info, "0xAccount", MagicMock())
    ingestor._projector = SimpleNamespace(cursor=AsyncMock(return_value=0), ingest_fill=AsyncMock())
    with pytest.raises(RuntimeError, match="invalid_historical_orders"):
        await ingestor.recover_history()


async def test_history_orders_are_sorted_and_cursor_filters_older_updates() -> None:
    orders = [
        {"statusTimestamp": 3000, "order": {"oid": 3}},
        {"statusTimestamp": 1000, "order": {"oid": 1}},
        {"statusTimestamp": 2000, "order": {"oid": 2}},
    ]
    info = MagicMock(user_fills_by_time=MagicMock(return_value=[]), historical_orders=MagicMock(return_value=orders))
    ingestor = ExchangeEventIngestor(info, "0xAccount", MagicMock())
    projector = SimpleNamespace(
        cursor=AsyncMock(side_effect=[0, 2000]), ingest_fill=AsyncMock(), ingest_order_update=AsyncMock()
    )
    ingestor._projector = projector
    await ingestor.recover_history()
    assert [call.args[0]["order"]["oid"] for call in projector.ingest_order_update.await_args_list] == [2, 3]
    assert ingestor._history_recovered is True


async def test_run_subscription_failure_does_not_claim_recovery_complete() -> None:
    info = MagicMock()
    info.subscribe.side_effect = RuntimeError("authentication failed")
    ingestor = ExchangeEventIngestor(info, "0xAccount", MagicMock())
    with pytest.raises(RuntimeError, match="authentication failed"):
        await ingestor.run()
    assert ingestor._history_recovered is False


async def test_run_processes_live_fill_and_unsubscribes_on_cancel() -> None:
    callbacks: list[object] = []
    info = MagicMock()

    def subscribe(_subscription: object, callback: object) -> int:
        callbacks.append(callback)
        return len(callbacks)

    info.subscribe.side_effect = subscribe
    ingestor = ExchangeEventIngestor(info, "0xAccount", MagicMock(), poll_interval_seconds=60)
    projector = SimpleNamespace(
        ingest_fill=AsyncMock(), ingest_order_update=AsyncMock(), cursor=AsyncMock(return_value=0)
    )
    ingestor._projector = projector
    ingestor.recover_history = AsyncMock()
    task = asyncio.create_task(ingestor.run())
    for _ in range(50):
        if len(callbacks) == 2:
            break
        await asyncio.sleep(0)
    callback = callbacks[0]
    assert callable(callback)
    callback({"channel": "userFills", "data": {"fills": [{"tid": 1}]}})
    for _ in range(100):
        if projector.ingest_fill.await_count:
            break
        await asyncio.sleep(0.001)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    projector.ingest_fill.assert_awaited_once_with({"tid": 1})
    assert info.unsubscribe.call_count == 2


async def test_history_poll_logs_failure_and_continues_until_stopped(monkeypatch: pytest.MonkeyPatch) -> None:
    import hypeedge.account.exchange_ingestor as module

    ingestor = ExchangeEventIngestor(MagicMock(), "0xAccount", MagicMock(), poll_interval_seconds=0)
    ingestor._running = True
    calls = 0

    async def recover() -> None:
        nonlocal calls
        calls += 1
        ingestor._running = False
        raise RuntimeError("temporary REST failure")

    ingestor.recover_history = recover  # type: ignore[method-assign]
    monkeypatch.setattr(module.logger, "exception", MagicMock())
    await ingestor._poll_history()
    assert calls == 1
    await ingestor.stop()


async def test_worker_empty_claim_and_unknown_defer_paths() -> None:
    queue = SimpleNamespace(claim=AsyncMock(return_value=None), defer_unknown=AsyncMock())
    engine = SimpleNamespace(execute_durable_command=AsyncMock())
    worker = SignedActionExecutor(queue, engine, worker_id="worker")  # type: ignore[arg-type]
    assert await worker.run_once() is False
    engine.execute_durable_command.assert_not_awaited()
    await worker.stop()


async def test_worker_run_cancellation_preserves_cancelled_error() -> None:
    queue = SimpleNamespace(claim=AsyncMock(return_value=None), defer_unknown=AsyncMock())
    engine = SimpleNamespace(execute_durable_command=AsyncMock())
    worker = SignedActionExecutor(queue, engine, poll_interval_ms=1)  # type: ignore[arg-type]
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_execution_queue_recovers_expired_lease_as_unknown_before_claim() -> None:
    expired = SimpleNamespace(
        status="processing",
        locked_at=datetime.now(UTC) - timedelta(minutes=1),
        locked_by="dead",
        available_at=datetime.now(UTC),
        last_error_code=None,
        last_error_message=None,
    )
    record = SimpleNamespace(
        command_id=uuid.uuid4(),
        command_type="place_order",
        payload={"cloid": "0x" + "a" * 32},
        status="unknown",
        attempt_count=2,
        locked_at=None,
        locked_by=None,
    )
    session = _Session([_Result(scalars=[expired]), _Result(scalar=record)])
    queue = PostgresExecutionCommandQueue(_Factory(session), lease_seconds=1)  # type: ignore[arg-type]

    claimed = await queue.claim("worker-b")

    assert claimed is not None and claimed.requires_resolution is True
    assert expired.status == "unknown" and expired.locked_by is None
    assert expired.last_error_code == "processing_lease_expired"
    assert record.status == "processing" and record.locked_by == "worker-b" and record.attempt_count == 3


async def test_execution_queue_empty_and_defer_unknown_are_recoverable() -> None:
    empty = _Session([_Result(scalars=[]), _Result(scalar=None)])
    queue = PostgresExecutionCommandQueue(_Factory(empty), unknown_recheck_seconds=7)  # type: ignore[arg-type]
    assert await queue.claim("worker") is None

    command = SimpleNamespace(
        status="processing",
        locked_at=datetime.now(UTC),
        locked_by="worker",
        completed_at=datetime.now(UTC),
        available_at=datetime.now(UTC),
        last_error_code=None,
        last_error_message=None,
    )
    deferred = _Session([_Result(scalar=command)])
    queue = PostgresExecutionCommandQueue(_Factory(deferred), unknown_recheck_seconds=7)  # type: ignore[arg-type]
    before = datetime.now(UTC)
    await queue.defer_unknown(uuid.uuid4(), "lookup inconclusive")
    assert command.status == "unknown" and command.locked_at is None and command.completed_at is None
    assert command.last_error_code == "exchange_outcome_unknown"
    assert command.available_at >= before + timedelta(seconds=6)


async def test_transactional_risk_keeps_expired_open_order_reservations_active() -> None:
    account = SimpleNamespace(equity=Decimal("1000"), exchange_updated_at=datetime.now(UTC))
    position = SimpleNamespace(symbol="BTC", size=Decimal("1"), mark_price=Decimal("100"))
    active = SimpleNamespace(symbol="BTC", side="buy", reserved_size=Decimal("1"), reserved_notional=Decimal("100"))
    expired = SimpleNamespace(
        status="active",
        symbol="BTC",
        side="buy",
        reserved_size=Decimal("0.5"),
        reserved_notional=Decimal("50"),
        released_at=None,
    )
    session = _Session(
        [
            _Result(scalar=account),
            _Result(scalars=[position]),
            _Result(scalars=[]),
            _Result(scalars=[active, expired]),
        ]
    )
    store = PostgresDurableOrderStore(
        MagicMock(),
        risk_limits=RiskLimits(max_position_pct=0.8, max_leverage=2),
        account_stale_seconds=10,
    )

    result = await store._check_and_lock_risk_scope(session, _order(size=1), 100)

    assert result.passed is True
    assert "active_reservations_included" in result.checked_limits
    assert expired.status == "active" and expired.released_at is None


async def test_transactional_risk_allows_strict_reduce_only_to_lower_leverage() -> None:
    account = SimpleNamespace(equity=Decimal("1000"), exchange_updated_at=datetime.now(UTC))
    position = SimpleNamespace(symbol="BTC", size=Decimal("12"), mark_price=Decimal("100"))
    session = _Session(
        [
            _Result(scalar=account),
            _Result(scalars=[position]),
            _Result(scalars=[]),
            _Result(scalars=[]),
        ]
    )
    store = PostgresDurableOrderStore(
        MagicMock(),
        risk_limits=RiskLimits(max_position_pct=0.95, max_leverage=1),
    )

    result = await store._check_and_lock_risk_scope(
        session,
        _order(side=Side.SELL, size=3, reduce_only=True),
        100,
    )

    assert result.passed is True


@pytest.mark.parametrize(
    ("account", "expected"),
    [
        (None, "account_state_not_available"),
        (
            SimpleNamespace(equity=Decimal("1000"), exchange_updated_at=datetime.now(UTC) - timedelta(hours=1)),
            "account_state_stale",
        ),
    ],
)
async def test_transactional_risk_fails_closed_without_fresh_account(account: object, expected: str) -> None:
    session = _Session([_Result(scalar=account)])
    store = PostgresDurableOrderStore(MagicMock(), risk_limits=RiskLimits(), account_stale_seconds=1)
    result = await store._check_and_lock_risk_scope(session, _order(), 100)
    assert result.passed is False and result.reason == expected


async def test_transactional_risk_rejects_reservation_that_exceeds_position_limit() -> None:
    account = SimpleNamespace(equity=Decimal("1000"), exchange_updated_at=datetime.now(UTC))
    active = SimpleNamespace(symbol="BTC", side="buy", reserved_size=Decimal("4"), reserved_notional=Decimal("400"))
    session = _Session([_Result(scalar=account), _Result(scalars=[]), _Result(scalars=[]), _Result(scalars=[active])])
    store = PostgresDurableOrderStore(MagicMock(), risk_limits=RiskLimits(max_position_pct=0.5, max_leverage=5))
    result = await store._check_and_lock_risk_scope(session, _order(size=2), 100)
    assert result.passed is False and result.reason == "position_limit_exceeded_with_reservations"


async def test_transactional_risk_uses_worst_fill_order_for_opposite_reservations() -> None:
    account = SimpleNamespace(equity=Decimal("1000"), exchange_updated_at=datetime.now(UTC))
    reservations = [
        SimpleNamespace(
            symbol="BTC",
            side="buy",
            reduce_only=False,
            reserved_size=Decimal("5"),
            reserved_notional=Decimal("500"),
        ),
        SimpleNamespace(
            symbol="BTC",
            side="sell",
            reduce_only=False,
            reserved_size=Decimal("5"),
            reserved_notional=Decimal("500"),
        ),
    ]
    session = _Session(
        [_Result(scalar=account), _Result(scalars=[]), _Result(scalars=[]), _Result(scalars=reservations)]
    )
    store = PostgresDurableOrderStore(
        MagicMock(),
        risk_limits=RiskLimits(max_position_pct=0.5, max_leverage=5),
    )

    result = await store._check_and_lock_risk_scope(session, _order(size=5), 100)

    assert result.passed is False
    assert result.reason == "position_limit_exceeded_with_reservations"


async def test_reconciled_terminal_order_releases_active_reservation() -> None:
    order = _order()
    order.status = OrderStatus.CANCELLED
    record = SimpleNamespace(order_id=uuid.uuid4(), revision=1)
    reservation = SimpleNamespace(status="active", released_at=None)
    session = _Session([_Result(scalar=record), _Result(scalar=reservation)])
    store = PostgresDurableOrderStore(_Factory(session), risk_limits=RiskLimits())  # type: ignore[arg-type]

    await store.persist_reconciled_order(order)

    assert reservation.status == "released"
    assert reservation.released_at is not None


async def test_persist_placement_rejection_never_creates_command_reservation() -> None:
    session = _Session()
    store = PostgresDurableOrderStore(_Factory(session), risk_limits=RiskLimits())  # type: ignore[arg-type]
    store._check_and_lock_risk_scope = AsyncMock(return_value=RiskCheckResult(False, "reservation_conflict"))  # type: ignore[method-assign]
    order = _order()

    result = await store.persist_placement(
        order,
        RiskCheckResult(True),
        command_id=uuid.uuid4(),
        dispatch=True,
        reference_price=100,
    )

    assert result is not None and result.passed is False
    assert order.status == OrderStatus.REJECTED
    assert not any(isinstance(item, RiskReservationRecord) for item in session.added)
    command = next(item for item in session.added if isinstance(item, ExecutionCommandRecord))
    assert command.status == "failed"


async def test_system_state_create_load_and_restore_transition_are_durable() -> None:
    create_session = _Session([_Result(scalar=None)])
    store = PostgresSystemStateStore(_Factory(create_session))  # type: ignore[arg-type]
    await store.transition("halted", "manual", kill_switch_active=True, triggered_by="admin")
    record = next(item for item in create_session.added if isinstance(item, SystemStateRecord))
    outbox = next(item for item in create_session.added if isinstance(item, OutboxEventRecord))
    assert record.state == "halted" and record.kill_switch_active is True and record.revision == 1
    assert outbox.payload["kill_switch_active"] is True

    load_session = _Session(get_result=record)
    loaded = await PostgresSystemStateStore(_Factory(load_session)).load()  # type: ignore[arg-type]
    assert loaded is not None and loaded.state == "halted" and loaded.kill_switch_active is True

    restore_session = _Session([_Result(scalar=record)])
    await PostgresSystemStateStore(_Factory(restore_session)).transition(  # type: ignore[arg-type]
        "recovering",
        "operator reset",
        kill_switch_active=False,
    )
    assert record.state == "recovering" and record.revision == 2 and record.triggered_at is None


async def test_postgres_outbox_claim_lease_mark_release_and_bounds() -> None:
    record = SimpleNamespace(
        sequence=4,
        event_id=uuid.uuid4(),
        event_type="order.updated",
        schema_version=1,
        aggregate_type="order",
        aggregate_id="1",
        aggregate_revision=1,
        correlation_id=None,
        payload={"ok": True},
        occurred_at=datetime.now(UTC),
        claimed_at=None,
        claimed_by=None,
        publish_attempts=0,
        last_publish_error="old",
        published_at=None,
    )
    claim_session = _Session([_Result(scalars=[record])])
    store = PostgresOutboxStore(_Factory(claim_session), lease_seconds=2)  # type: ignore[arg-type]
    events = await store.claim_batch("outbox-a", limit=1)
    assert len(events) == 1 and record.claimed_by == "outbox-a" and record.publish_attempts == 1

    mark_session = _Session([_Result(rowcount=1)])
    assert await PostgresOutboxStore(_Factory(mark_session)).mark_published(events[0], "outbox-a") is True  # type: ignore[arg-type]
    release_session = _Session([_Result()])
    await PostgresOutboxStore(_Factory(release_session)).release_claim(events[0], "outbox-a", "x" * 3000)  # type: ignore[arg-type]

    bounds_session = _Session([_Result(scalar=(2, 9))])
    bounds = await PostgresOutboxStore(_Factory(bounds_session)).replay_bounds()  # type: ignore[arg-type]
    assert bounds == ReplayBounds(2, 9)

    empty_bounds = await PostgresOutboxStore(_Factory(_Session([_Result(scalar=(None, None))]))).replay_bounds()  # type: ignore[arg-type]
    assert empty_bounds == ReplayBounds(None, None)

    read_record = SimpleNamespace(**vars(record))
    read_session = _Session([_Result(scalars=[read_record])])
    replay = await PostgresOutboxStore(_Factory(read_session)).read_after(1, 9, limit=1)  # type: ignore[arg-type]
    assert [event.sequence for event in replay] == [4]


async def test_outbox_control_append_normalizes_payload_and_event_identity() -> None:
    from hypeedge.core.events import EVENT_ACTION_CREDITS_LOW, Event

    session = _Session([_Result()])
    store = PostgresOutboxStore(_Factory(session))  # type: ignore[arg-type]
    event = Event(event_type=EVENT_ACTION_CREDITS_LOW, payload=SimpleNamespace(remaining=5), event_id="legacy")
    await store.append_control_event(event)
    assert len(session.executed) == 1
    assert _json_payload(SimpleNamespace(value=Decimal("1.2"))) == {"value": "1.2"}
    assert _json_payload([1, 2]) == {"value": [1, 2]}


async def test_control_writer_ignores_non_control_events_and_unsubscribes_on_cancel() -> None:
    from hypeedge.core.events import Event, EventBus

    bus = EventBus()
    store = SimpleNamespace(append_control_event=AsyncMock())
    writer = DurableControlEventWriter(bus, store)  # type: ignore[arg-type]
    writer.start()
    task = asyncio.create_task(writer.run())
    await bus.publish(Event(event_type="order.submitted", payload={}))
    await asyncio.sleep(0)
    store.append_control_event.assert_not_awaited()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert writer._queue is None


async def test_dispatcher_stops_batch_on_lease_loss_and_releases_later_sequences() -> None:
    first, second = _durable_event(1), _durable_event(2)
    store = SimpleNamespace(
        claim_batch=AsyncMock(return_value=[first, second]),
        mark_published=AsyncMock(return_value=False),
        release_claim=AsyncMock(),
    )
    sink = SimpleNamespace(publish=AsyncMock())
    dispatcher = OutboxDispatcher(store, sink, worker_id="worker")  # type: ignore[arg-type]
    assert await dispatcher.dispatch_once() == 0
    assert store.release_claim.await_args_list[0].args[0] == first
    assert store.release_claim.await_args_list[1].args == (second, "worker", "earlier_sequence_failed")


async def test_dispatcher_run_polls_when_empty_and_stops_cleanly(monkeypatch: pytest.MonkeyPatch) -> None:
    import hypeedge.storage.outbox as module

    store = SimpleNamespace(claim_batch=AsyncMock(return_value=[]))
    sink = SimpleNamespace(publish=AsyncMock())
    dispatcher = OutboxDispatcher(store, sink, poll_interval=0)  # type: ignore[arg-type]

    async def sleep(_delay: float) -> None:
        await dispatcher.stop()

    monkeypatch.setattr(module.asyncio, "sleep", sleep)
    await dispatcher.run()
    assert store.claim_batch.await_count == 1
