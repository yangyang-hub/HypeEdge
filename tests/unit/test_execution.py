"""Tests for the execution infrastructure (nonce, cloid, engine)."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from hypeedge.core.enums import OrderStatus, OrderType, SafetyMode, Side, TimeInForce
from hypeedge.core.events import (
    EVENT_ORDER_ACKNOWLEDGED,
    EVENT_ORDER_SUBMITTED,
    EventBus,
)
from hypeedge.core.exceptions import KillSwitchTriggeredError, NonceError, OrderRejectedError, OrderTimeoutError
from hypeedge.core.models import AccountState, Order, OrderIntent, RiskCheckResult
from hypeedge.core.types import Cloid, Price, Size, StrategyId, Symbol, Usd
from hypeedge.execution.cloid import CloidGenerator
from hypeedge.execution.durable import DurableExecutionCommand
from hypeedge.execution.engine import ExecutionEngine
from hypeedge.execution.nonce import NonceManager
from hypeedge.market_data.provider import MarketPriceSnapshot
from hypeedge.risk.kill_switch import KillSwitch
from hypeedge.risk.safety import SafetyController


class InMemoryDurableOrderStore:
    def __init__(self) -> None:
        self.placements: list[tuple[OrderStatus, bool, uuid.UUID]] = []
        self.transitions: list[tuple[str, str | None]] = []
        self.cancel_requests: list[uuid.UUID] = []
        self.open_orders: list[Order] = []
        self.transition_filled_sizes: list[Size] = []

    async def persist_placement(
        self,
        order: Order,
        risk_result: RiskCheckResult,
        *,
        command_id: uuid.UUID,
        dispatch: bool,
        reference_price: float | None = None,
        price_observed_at: datetime | None = None,
    ) -> None:
        del reference_price, price_observed_at
        self.placements.append((order.status, dispatch, command_id))

    async def persist_transition(
        self,
        order: Order,
        event_type: str,
        *,
        command_id: uuid.UUID | None = None,
        command_status: str | None = None,
    ) -> None:
        self.transitions.append((event_type, command_status))
        self.transition_filled_sizes.append(order.filled_size)

    async def persist_cancel_requested(self, order: Order, *, command_id: uuid.UUID) -> None:
        self.cancel_requests.append(command_id)

    async def persist_reconciled_order(self, order: Order) -> None:
        self.open_orders.append(order)

    async def load_open_orders(self) -> list[Order]:
        return list(self.open_orders)

    async def get_order(self, cloid: str) -> Order | None:
        return next((order for order in self.open_orders if str(order.cloid) == cloid), None)


class RejectingDurableOrderStore(InMemoryDurableOrderStore):
    async def persist_placement(
        self,
        order: Order,
        risk_result: RiskCheckResult,
        *,
        command_id: uuid.UUID,
        dispatch: bool,
        reference_price: float | None = None,
        price_observed_at: datetime | None = None,
    ) -> RiskCheckResult:
        await super().persist_placement(
            order,
            risk_result,
            command_id=command_id,
            dispatch=dispatch,
            reference_price=reference_price,
            price_observed_at=price_observed_at,
        )
        return RiskCheckResult(False, "position_limit_exceeded_with_reservations")


async def _start_manager(manager: NonceManager) -> asyncio.Task[None]:
    """Start a NonceManager in the background and return the task."""
    return asyncio.create_task(manager.run())


async def _stop_manager(manager: NonceManager, task: asyncio.Task[None]) -> None:
    """Stop a NonceManager and wait for its task to finish."""
    import contextlib

    await manager.stop()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


# --- Test CloidGenerator ---


class TestCloidGenerator:
    def test_generate_returns_non_empty(self):
        cloid = CloidGenerator.generate()
        assert len(str(cloid)) > 0
        assert CloidGenerator.validate(str(cloid))

    def test_generate_with_strategy_id(self):
        cloid = CloidGenerator.generate(StrategyId("trend_v1"))
        assert "trend_v1" in str(cloid)

    def test_generate_uniqueness(self):
        cloids = {str(CloidGenerator.generate()) for _ in range(100)}
        assert len(cloids) == 100

    def test_generate_for_strategy_deterministic_prefix(self):
        cloid = CloidGenerator.generate_for_strategy(StrategyId("strat"), seq=42)
        assert str(cloid).startswith("strat_")

    def test_to_hl_cloid_format(self):
        cloid = CloidGenerator.generate()
        hl = CloidGenerator.to_hl_cloid(cloid)
        assert hl.startswith("0x")
        assert len(hl) == 34  # "0x" + 32 hex chars

    def test_to_hl_cloid_deterministic(self):
        cloid = Cloid("test_cloid_123")
        hl1 = CloidGenerator.to_hl_cloid(cloid)
        hl2 = CloidGenerator.to_hl_cloid(cloid)
        assert hl1 == hl2

    def test_to_hl_cloid_preserves_canonical_exchange_id(self):
        canonical = Cloid("0x" + "a" * 32)
        assert CloidGenerator.to_hl_cloid(canonical) == canonical

    def test_validate_empty_string(self):
        assert CloidGenerator.validate("") is False
        assert CloidGenerator.validate("   ") is False

    def test_validate_valid(self):
        assert CloidGenerator.validate("abc_123") is True

    def test_validate_too_long(self):
        assert CloidGenerator.validate("x" * 65) is False
        assert CloidGenerator.validate("x" * 64) is True

    def test_max_64_chars(self):
        long_id = StrategyId("a" * 30)
        cloid = CloidGenerator.generate(long_id)
        assert len(str(cloid)) <= 64


# --- Test NonceManager ---


class TestNonceManager:
    @pytest.mark.asyncio
    async def test_submit_serializes_actions(self):
        """Actions are executed one at a time through the queue."""
        manager = NonceManager()
        execution_order: list[int] = []

        mock_exchange = MagicMock()
        manager.set_exchange(mock_exchange)

        async def slow_action(n: int) -> str:
            execution_order.append(n)
            await asyncio.sleep(0.01)
            return f"result_{n}"

        task = await _start_manager(manager)

        results = await asyncio.gather(
            manager.submit(slow_action, 1),
            manager.submit(slow_action, 2),
            manager.submit(slow_action, 3),
        )

        await _stop_manager(manager, task)

        assert results == ["result_1", "result_2", "result_3"]
        assert execution_order == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_submit_without_exchange_raises(self):
        manager = NonceManager()
        task = await _start_manager(manager)

        with pytest.raises(NonceError):
            await manager.submit(lambda: None)

        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_stats_tracking(self):
        manager = NonceManager()
        mock_exchange = MagicMock()
        manager.set_exchange(mock_exchange)

        task = await _start_manager(manager)

        await manager.submit(lambda: "ok")
        stats = manager.stats
        assert stats["total_actions"] == 1
        assert stats["total_errors"] == 0

        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_sync_sdk_call_does_not_block_event_loop(self):
        manager = NonceManager()
        manager.set_exchange(MagicMock())
        task = await _start_manager(manager)

        import time

        started = asyncio.Event()

        def blocking_action() -> str:
            started.set()
            time.sleep(0.05)
            return "ok"

        action_task = asyncio.create_task(manager.submit(blocking_action))
        await started.wait()
        await asyncio.sleep(0.005)
        assert not action_task.done()
        assert await action_task == "ok"
        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_timeout_is_unknown_and_not_blindly_retried(self, monkeypatch: pytest.MonkeyPatch):
        import time

        import hypeedge.execution.nonce as nonce_module

        monkeypatch.setattr(nonce_module, "_SUBMIT_TIMEOUT_S", 0.01)
        manager = NonceManager()
        manager.set_exchange(MagicMock())
        calls = 0

        def slow_action() -> None:
            nonlocal calls
            calls += 1
            time.sleep(0.05)

        task = await _start_manager(manager)
        with pytest.raises(OrderTimeoutError):
            await manager.submit(slow_action, cloid_hint="test_cloid")
        assert calls == 1
        await _stop_manager(manager, task)


# --- Test ExecutionEngine ---


def _make_intent(
    symbol: str = "BTC",
    side: Side = Side.BUY,
    size: float = 0.01,
    price: float | None = 50000.0,
    order_type: OrderType = OrderType.LIMIT,
) -> OrderIntent:
    return OrderIntent(
        symbol=Symbol(symbol),
        side=side,
        size=Size(size),
        price=Price(price) if price else None,
        order_type=order_type,
        time_in_force=TimeInForce.GTC,
        strategy_id=StrategyId("test"),
    )


class TestExecutionEngine:
    @pytest.mark.asyncio
    async def test_same_canonical_cloid_is_idempotent_before_kill_gate(self) -> None:
        bus = EventBus()
        kill_switch = KillSwitch(bus)
        manager = MagicMock(exchange=MagicMock())
        manager.submit = AsyncMock(
            return_value={
                "status": "ok",
                "response": {"data": {"statuses": [{"resting": {"oid": 1}}]}},
            }
        )
        engine = ExecutionEngine(manager, bus, kill_switch)
        base = _make_intent()
        intent = OrderIntent(**{**base.__dict__, "cloid": Cloid("same-command")})

        first = await engine.submit_order(intent)
        kill_switch.trigger("after_first_submit")
        replay = await engine.submit_order(intent)

        assert replay is first
        manager.submit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_same_canonical_cloid_with_different_payload_is_rejected(self) -> None:
        bus = EventBus()
        manager = MagicMock(exchange=MagicMock())
        manager.submit = AsyncMock(
            return_value={
                "status": "ok",
                "response": {"data": {"statuses": [{"resting": {"oid": 1}}]}},
            }
        )
        engine = ExecutionEngine(manager, bus, KillSwitch(bus))
        base = _make_intent()
        first = OrderIntent(**{**base.__dict__, "cloid": Cloid("same-command")})
        conflict = OrderIntent(**{**first.__dict__, "size": Size(0.02)})

        await engine.submit_order(first)
        with pytest.raises(OrderRejectedError, match="already bound"):
            await engine.submit_order(conflict)
        manager.submit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_submit_order_success_acknowledged(self):
        """Successful limit order → PENDING → SUBMITTED → ACKNOWLEDGED."""
        bus = EventBus(queue_maxsize=100)
        kill_switch = KillSwitch(bus)
        manager = NonceManager()

        mock_exchange = MagicMock()
        mock_exchange.order = MagicMock(
            return_value={
                "status": "ok",
                "response": {"data": {"statuses": [{"resting": {"oid": 12345}}]}},
            }
        )
        manager.set_exchange(mock_exchange)

        engine = ExecutionEngine(manager, bus, kill_switch, account_address="0xabc")
        task = await _start_manager(manager)

        order = await engine.submit_order(_make_intent())

        assert order.status == OrderStatus.ACKNOWLEDGED
        assert order.exchange_oid is not None
        assert order.cloid is not None

        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_durable_placement_commits_before_exchange_and_journals_ack(self):
        bus = EventBus(queue_maxsize=100)
        manager = NonceManager()
        exchange = MagicMock()
        store = InMemoryDurableOrderStore()

        def assert_durable_before_exchange(*args: object, **kwargs: object) -> dict[str, object]:
            assert len(store.placements) == 1
            return {"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 7}}]}}}

        exchange.order.side_effect = assert_durable_before_exchange
        manager.set_exchange(exchange)
        engine = ExecutionEngine(manager, bus, KillSwitch(bus), durable_store=store)
        task = await _start_manager(manager)

        order = await engine.submit_order(_make_intent())

        assert store.placements[0][0:2] == (OrderStatus.SUBMITTED, True)
        assert store.transitions == [("acknowledged", "succeeded")]
        assert str(order.cloid).startswith("0x") and len(str(order.cloid)) == 34
        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_deferred_placement_never_calls_sdk_in_request_coroutine(self):
        bus = EventBus(queue_maxsize=100)
        manager = NonceManager()
        exchange = MagicMock()
        manager.set_exchange(exchange)
        store = InMemoryDurableOrderStore()
        engine = ExecutionEngine(
            manager,
            bus,
            KillSwitch(bus),
            durable_store=store,
            deferred_execution=True,
        )

        order = await engine.submit_order(_make_intent())

        assert order.status == OrderStatus.SUBMITTED
        exchange.order.assert_not_called()
        assert store.placements[0][0:2] == (OrderStatus.SUBMITTED, True)

    @pytest.mark.asyncio
    async def test_deferred_order_uses_account_address_as_durable_scope(self):
        bus = EventBus(queue_maxsize=100)
        store = InMemoryDurableOrderStore()
        engine = ExecutionEngine(
            NonceManager(),
            bus,
            KillSwitch(bus),
            account_address="0xABC",
            durable_store=store,
            deferred_execution=True,
        )

        order = await engine.submit_order(_make_intent())

        assert str(order.sub_account) == "0xabc"

    @pytest.mark.asyncio
    async def test_worker_aborts_pending_placement_after_kill_without_sdk_send(self):
        bus = EventBus(queue_maxsize=100)
        manager = NonceManager()
        exchange = MagicMock()
        manager.set_exchange(exchange)
        kill_switch = KillSwitch(bus)
        store = InMemoryDurableOrderStore()
        order = Order(
            cloid=Cloid("0x" + "f" * 32),
            symbol=Symbol("BTC"),
            side=Side.BUY,
            size=Size(0.01),
            price=Price(50_000),
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            status=OrderStatus.SUBMITTED,
        )
        store.open_orders.append(order)
        engine = ExecutionEngine(manager, bus, kill_switch, durable_store=store, deferred_execution=True)
        kill_switch.trigger("manual")
        command = DurableExecutionCommand(uuid.uuid4(), "place_order", {"cloid": str(order.cloid)}, 1, False)

        resolved = await engine.execute_durable_command(command)

        assert resolved is True
        assert order.status == OrderStatus.CANCELLED
        assert store.transitions[-1] == ("dispatch_aborted", "cancelled")
        exchange.order.assert_not_called()

    @pytest.mark.asyncio
    async def test_nonce_preflight_aborts_placement_queued_before_kill(self):
        bus = EventBus(queue_maxsize=100)
        manager = NonceManager()
        exchange = MagicMock()
        exchange.order.return_value = {
            "status": "ok",
            "response": {"data": {"statuses": [{"resting": {"oid": 1}}]}},
        }
        manager.set_exchange(exchange)
        kill_switch = KillSwitch(bus)
        store = InMemoryDurableOrderStore()
        order = Order(
            cloid=Cloid("0x" + "9" * 32),
            symbol=Symbol("BTC"),
            side=Side.BUY,
            size=Size(0.01),
            price=Price(50_000),
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            status=OrderStatus.SUBMITTED,
        )
        store.open_orders.append(order)
        engine = ExecutionEngine(manager, bus, kill_switch, durable_store=store, deferred_execution=True)
        blocker_entered = asyncio.Event()
        release_blocker = asyncio.Event()

        async def blocker() -> None:
            blocker_entered.set()
            await release_blocker.wait()

        manager_task = await _start_manager(manager)
        blocker_task = asyncio.create_task(manager.submit(blocker))
        await blocker_entered.wait()
        command = DurableExecutionCommand(uuid.uuid4(), "place_order", {"cloid": str(order.cloid)}, 1, False)
        placement_task = asyncio.create_task(engine.execute_durable_command(command))
        while manager.queue_depth == 0:
            await asyncio.sleep(0)
        kill_switch.trigger("manual")
        release_blocker.set()

        assert await placement_task is True
        await blocker_task
        assert order.status == OrderStatus.CANCELLED
        assert store.transitions[-1] == ("dispatch_aborted", "cancelled")
        exchange.order.assert_not_called()
        await _stop_manager(manager, manager_task)

    @pytest.mark.asyncio
    async def test_recovered_cancel_command_queries_open_then_cancels(self):
        bus = EventBus(queue_maxsize=100)
        manager = NonceManager()
        exchange = MagicMock()
        exchange.cancel_by_cloid.return_value = {
            "status": "ok",
            "response": {"data": {"statuses": ["success"]}},
        }
        manager.set_exchange(exchange)
        manager.query_order_status = AsyncMock(return_value={"status": "order", "order": {"status": "open"}})  # type: ignore[method-assign]
        store = InMemoryDurableOrderStore()
        order = Order(
            cloid=Cloid("0x" + "e" * 32),
            symbol=Symbol("BTC"),
            side=Side.BUY,
            size=Size(0.01),
            price=Price(50_000),
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            status=OrderStatus.ACKNOWLEDGED,
        )
        store.open_orders.append(order)
        engine = ExecutionEngine(manager, bus, KillSwitch(bus), durable_store=store, deferred_execution=True)
        task = await _start_manager(manager)
        command = DurableExecutionCommand(uuid.uuid4(), "cancel_order", {"cloid": str(order.cloid)}, 2, True)

        resolved = await engine.execute_durable_cancel_command(command)

        assert resolved is True
        assert order.status == OrderStatus.CANCELLED
        exchange.cancel_by_cloid.assert_called_once()
        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_database_reservation_rejection_prevents_sdk_send(self):
        bus = EventBus(queue_maxsize=100)
        manager = NonceManager()
        exchange = MagicMock()
        manager.set_exchange(exchange)
        engine = ExecutionEngine(
            manager,
            bus,
            KillSwitch(bus),
            durable_store=RejectingDurableOrderStore(),
            deferred_execution=True,
        )

        order = await engine.submit_order(_make_intent())

        assert order.status == OrderStatus.REJECTED
        assert order.error_message == "position_limit_exceeded_with_reservations"
        exchange.order.assert_not_called()

    @pytest.mark.asyncio
    async def test_flat_market_open_uses_fresh_provider_price_for_risk_and_reservation(self):
        bus = EventBus(queue_maxsize=100)
        manager = NonceManager()
        manager.set_exchange(MagicMock())
        provider = MagicMock()
        provider.get_price_snapshot.return_value = MarketPriceSnapshot(50_000.0, datetime.now(UTC))
        risk_checker = MagicMock()
        risk_checker.check = AsyncMock(return_value=RiskCheckResult(True))
        store = InMemoryDurableOrderStore()
        engine = ExecutionEngine(
            manager,
            bus,
            KillSwitch(bus),
            risk_checker=risk_checker,
            durable_store=store,
            deferred_execution=True,
            market_data_provider=provider,
        )

        order = await engine.submit_order(_make_intent(price=None, order_type=OrderType.MARKET))

        assert order.status == OrderStatus.SUBMITTED
        risk_checker.check.assert_awaited_once()
        assert risk_checker.check.await_args.kwargs["reference_price"] == 50_000.0

    @pytest.mark.asyncio
    async def test_durable_failure_prevents_exchange_side_effect(self):
        class FailingStore(InMemoryDurableOrderStore):
            async def persist_placement(
                self,
                order: Order,
                risk_result: RiskCheckResult,
                *,
                command_id: uuid.UUID,
                dispatch: bool,
                reference_price: float | None = None,
                price_observed_at: datetime | None = None,
            ) -> None:
                del reference_price, price_observed_at
                raise RuntimeError("postgres_down")

        bus = EventBus(queue_maxsize=100)
        manager = NonceManager()
        exchange = MagicMock()
        manager.set_exchange(exchange)
        engine = ExecutionEngine(manager, bus, KillSwitch(bus), durable_store=FailingStore())

        with pytest.raises(RuntimeError, match="postgres_down"):
            await engine.submit_order(_make_intent())
        exchange.order.assert_not_called()

    @pytest.mark.asyncio
    async def test_recover_open_orders_restores_engine_projection(self):
        bus = EventBus(queue_maxsize=100)
        manager = NonceManager()
        store = InMemoryDurableOrderStore()
        recovered = Order(
            cloid=Cloid("0x" + "a" * 32),
            symbol=Symbol("BTC"),
            side=Side.BUY,
            size=Size(0.01),
            price=Price(50_000),
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            status=OrderStatus.ACKNOWLEDGED,
        )
        store.open_orders.append(recovered)
        engine = ExecutionEngine(manager, bus, KillSwitch(bus), durable_store=store)

        assert await engine.recover_open_orders() == 1
        assert await engine.get_order(str(recovered.cloid)) is recovered

    @pytest.mark.asyncio
    async def test_submit_order_kill_switch_blocks(self):
        bus = EventBus(queue_maxsize=100)
        kill_switch = KillSwitch(bus)
        manager = NonceManager()
        manager.set_exchange(MagicMock())

        engine = ExecutionEngine(manager, bus, kill_switch)
        kill_switch.trigger("test")

        with pytest.raises(KillSwitchTriggeredError):
            await engine.submit_order(_make_intent())

    @pytest.mark.asyncio
    async def test_submit_order_rejected_by_exchange(self):
        bus = EventBus(queue_maxsize=100)
        kill_switch = KillSwitch(bus)
        manager = NonceManager()

        mock_exchange = MagicMock()
        mock_exchange.order = MagicMock(
            return_value={
                "status": "err",
                "response": "Insufficient margin",
            }
        )
        manager.set_exchange(mock_exchange)

        engine = ExecutionEngine(manager, bus, kill_switch, account_address="0xabc")
        task = await _start_manager(manager)

        order = await engine.submit_order(_make_intent())

        assert order.status == OrderStatus.REJECTED
        assert order.error_message is not None

        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_submit_order_error_status(self):
        bus = EventBus(queue_maxsize=100)
        kill_switch = KillSwitch(bus)
        manager = NonceManager()

        mock_exchange = MagicMock()
        mock_exchange.order = MagicMock(
            return_value={
                "status": "ok",
                "response": {"data": {"statuses": [{"error": "Order too small"}]}},
            }
        )
        manager.set_exchange(mock_exchange)

        engine = ExecutionEngine(manager, bus, kill_switch, account_address="0xabc")
        task = await _start_manager(manager)

        order = await engine.submit_order(_make_intent())
        assert order.status == OrderStatus.REJECTED
        assert order.error_message == "Order too small"

        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_cancel_order(self):
        bus = EventBus(queue_maxsize=100)
        kill_switch = KillSwitch(bus)
        manager = NonceManager()

        mock_exchange = MagicMock()
        mock_exchange.order = MagicMock(
            return_value={
                "status": "ok",
                "response": {"data": {"statuses": [{"resting": {"oid": 100}}]}},
            }
        )
        mock_exchange.cancel_by_cloid = MagicMock(
            return_value={"status": "ok", "response": {"data": {"statuses": ["success"]}}}
        )
        manager.set_exchange(mock_exchange)

        engine = ExecutionEngine(manager, bus, kill_switch, account_address="0xabc")
        task = await _start_manager(manager)

        order = await engine.submit_order(_make_intent())
        assert order.status == OrderStatus.ACKNOWLEDGED

        result = await engine.cancel_order(str(order.cloid))
        assert result is True
        assert order.status == OrderStatus.CANCELLED

        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "cancel_response",
        [
            {"status": "ok"},
            {"status": "order", "order": {"status": "open"}},
            {"unexpected": True},
        ],
    )
    async def test_cancel_unconfirmed_response_enters_cancel_unknown(self, cancel_response: object) -> None:
        bus = EventBus()
        manager = MagicMock(exchange=MagicMock())
        manager.submit = AsyncMock(
            side_effect=[
                {"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 1}}]}}},
                cancel_response,
            ]
        )
        engine = ExecutionEngine(manager, bus, KillSwitch(bus))
        order = await engine.submit_order(_make_intent())

        assert await engine.cancel_order(str(order.cloid)) is False
        assert order.status == OrderStatus.CANCEL_UNKNOWN

    @pytest.mark.asyncio
    async def test_cancel_timeout_enters_cancel_unknown_and_persists_unknown(self) -> None:
        bus = EventBus()
        manager = MagicMock(exchange=MagicMock())
        manager.submit = AsyncMock(side_effect=OrderTimeoutError("cancel timed out", cloid="known"))
        store = InMemoryDurableOrderStore()
        order = Order(
            cloid=Cloid("0x" + "d" * 32),
            symbol=Symbol("BTC"),
            side=Side.BUY,
            size=Size(0.01),
            price=Price(50_000),
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            status=OrderStatus.ACKNOWLEDGED,
            strategy_id=StrategyId("test"),
        )
        store.open_orders.append(order)
        engine = ExecutionEngine(manager, bus, KillSwitch(bus), durable_store=store)
        engine.import_exchange_order(order)

        assert await engine.cancel_order(str(order.cloid)) is False
        assert order.status == OrderStatus.CANCEL_UNKNOWN
        assert store.transitions[-1] == ("cancel_unknown", "unknown")

    @pytest.mark.asyncio
    async def test_cancel_is_persisted_before_exchange_and_journals_result(self):
        bus = EventBus(queue_maxsize=100)
        manager = NonceManager()
        exchange = MagicMock()
        store = InMemoryDurableOrderStore()
        exchange.order.return_value = {
            "status": "ok",
            "response": {"data": {"statuses": [{"resting": {"oid": 100}}]}},
        }

        def assert_cancel_was_durable(*args: object, **kwargs: object) -> dict[str, str]:
            assert len(store.cancel_requests) == 1
            return {"status": "ok", "response": {"data": {"statuses": ["success"]}}}

        exchange.cancel_by_cloid.side_effect = assert_cancel_was_durable
        manager.set_exchange(exchange)
        engine = ExecutionEngine(manager, bus, KillSwitch(bus), durable_store=store)
        task = await _start_manager(manager)
        order = await engine.submit_order(_make_intent())

        assert await engine.cancel_order(str(order.cloid)) is True
        assert store.transitions[-1] == ("cancelled", "succeeded")
        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_cancel_remains_allowed_after_kill_switch(self):
        bus = EventBus(queue_maxsize=100)
        safety = SafetyController(SafetyMode.NORMAL)
        kill_switch = KillSwitch(bus, safety)
        manager = NonceManager()
        exchange = MagicMock()
        exchange.order.return_value = {
            "status": "ok",
            "response": {"data": {"statuses": [{"resting": {"oid": 100}}]}},
        }
        exchange.cancel_by_cloid.return_value = {
            "status": "ok",
            "response": {"data": {"statuses": ["success"]}},
        }
        manager.set_exchange(exchange)
        engine = ExecutionEngine(manager, bus, kill_switch, safety_controller=safety)
        task = await _start_manager(manager)
        order = await engine.submit_order(_make_intent())

        kill_switch.trigger("test")
        assert await engine.cancel_order(str(order.cloid)) is True
        sdk_cloid = exchange.cancel_by_cloid.call_args.args[1]
        assert sdk_cloid.to_raw().startswith("0x")
        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_market_reduce_only_uses_market_close(self):
        bus = EventBus(queue_maxsize=100)
        kill_switch = KillSwitch(bus)
        manager = NonceManager()
        exchange = MagicMock()
        exchange.market_close.return_value = {
            "status": "ok",
            "response": {"data": {"statuses": [{"filled": {"oid": 1, "avgPx": "50000"}}]}},
        }
        manager.set_exchange(exchange)
        engine = ExecutionEngine(manager, bus, kill_switch)
        task = await _start_manager(manager)
        intent = _make_intent(side=Side.SELL, price=None, order_type=OrderType.MARKET)
        intent = OrderIntent(**{**intent.__dict__, "reduce_only": True})

        await engine.submit_order(intent)
        exchange.market_close.assert_called_once()
        exchange.market_open.assert_not_called()
        assert exchange.market_close.call_args.kwargs["cloid"].to_raw().startswith("0x")
        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_order(self):
        bus = EventBus(queue_maxsize=100)
        kill_switch = KillSwitch(bus)
        manager = NonceManager()
        manager.set_exchange(MagicMock())

        engine = ExecutionEngine(manager, bus, kill_switch)
        result = await engine.cancel_order("nonexistent_cloid")
        assert result is False

    @pytest.mark.asyncio
    async def test_get_open_orders(self):
        bus = EventBus(queue_maxsize=100)
        kill_switch = KillSwitch(bus)
        manager = NonceManager()

        mock_exchange = MagicMock()
        mock_exchange.order = MagicMock(
            return_value={
                "status": "ok",
                "response": {"data": {"statuses": [{"resting": {"oid": 100}}]}},
            }
        )
        mock_exchange.cancel_by_cloid = MagicMock(
            return_value={"status": "ok", "response": {"data": {"statuses": ["success"]}}}
        )
        manager.set_exchange(mock_exchange)

        engine = ExecutionEngine(manager, bus, kill_switch, account_address="0xabc")
        task = await _start_manager(manager)

        await engine.submit_order(_make_intent(symbol="BTC"))
        await engine.submit_order(_make_intent(symbol="ETH"))

        open_orders = await engine.get_open_orders()
        assert len(open_orders) == 2

        await engine.cancel_order(str(open_orders[0].cloid))
        open_after = await engine.get_open_orders()
        assert len(open_after) == 1

        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_events_published(self):
        bus = EventBus(queue_maxsize=100)
        kill_switch = KillSwitch(bus)
        manager = NonceManager()

        mock_exchange = MagicMock()
        mock_exchange.order = MagicMock(
            return_value={
                "status": "ok",
                "response": {"data": {"statuses": [{"resting": {"oid": 100}}]}},
            }
        )
        manager.set_exchange(mock_exchange)

        engine = ExecutionEngine(manager, bus, kill_switch, account_address="0xabc")
        sub_queue = bus.subscribe_all()
        task = await _start_manager(manager)

        await engine.submit_order(_make_intent())

        events = []
        while not sub_queue.empty():
            events.append(sub_queue.get_nowait())

        event_types = {e.event_type for e in events}
        assert EVENT_ORDER_SUBMITTED in event_types
        assert EVENT_ORDER_ACKNOWLEDGED in event_types

        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_action_credits_low_rejects_order(self):
        """Order rejected when action credits are below watermark."""
        bus = EventBus(queue_maxsize=100)
        kill_switch = KillSwitch(bus)
        manager = NonceManager()

        mock_exchange = MagicMock()
        manager.set_exchange(mock_exchange)

        from hypeedge.market_data.rate_limiter import RateLimiter

        rate_limiter = RateLimiter()
        rate_limiter.update_action_credits(0)  # Below watermark

        engine = ExecutionEngine(
            manager,
            bus,
            kill_switch,
            account_address="0xabc",
            rate_limiter=rate_limiter,
        )
        task = await _start_manager(manager)

        order = await engine.submit_order(_make_intent())
        assert order.status == OrderStatus.REJECTED
        assert order.error_message == "action_credits_below_threshold"

        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_every_placement_passes_risk_gate(self):
        bus = EventBus(queue_maxsize=100)
        kill_switch = KillSwitch(bus)
        manager = NonceManager()
        exchange = MagicMock()
        manager.set_exchange(exchange)
        risk_checker = MagicMock()
        risk_checker.check = AsyncMock(return_value=RiskCheckResult(passed=False, reason="test_limit"))
        engine = ExecutionEngine(manager, bus, kill_switch, risk_checker=risk_checker)

        intent = _make_intent()
        order = await engine.submit_order(intent)
        assert order.status == OrderStatus.REJECTED
        risk_checker.check.assert_awaited_once()
        checked_intent = risk_checker.check.await_args.args[0]
        assert checked_intent.symbol == intent.symbol
        assert checked_intent.side == intent.side
        assert checked_intent.size == intent.size
        assert str(checked_intent.cloid).startswith("0x")
        exchange.order.assert_not_called()


class TestKillSwitchCancelAll:
    @pytest.mark.asyncio
    async def test_trigger_cancels_orders(self):
        """Kill switch trigger should call registered cancel-all function."""
        bus = EventBus(queue_maxsize=100)
        kill_switch = KillSwitch(bus)

        cancel_called = asyncio.Event()

        async def mock_cancel_all() -> int:
            cancel_called.set()
            return 5

        kill_switch.register_cancel_all(mock_cancel_all)
        kill_switch.trigger("test")

        await asyncio.sleep(0.05)
        assert cancel_called.is_set()

    @pytest.mark.asyncio
    async def test_single_cancel_task_retries_until_exchange_is_empty(self) -> None:
        bus = EventBus()
        safety = SafetyController(SafetyMode.NORMAL)
        kill_switch = KillSwitch(bus, safety)
        cancel_all = AsyncMock(return_value=1)
        verifier = AsyncMock(side_effect=[False, True])
        kill_switch.register_cancel_all(cancel_all, verifier)

        kill_switch.trigger("test")
        first_task = kill_switch.cancellation_task
        kill_switch.trigger("duplicate")

        assert kill_switch.cancellation_task is first_task
        assert first_task is not None
        await first_task
        assert cancel_all.await_count == 2
        assert verifier.await_count == 2
        assert safety.mode == SafetyMode.HALTED
        assert await kill_switch.wait_until_halted() is True

    def test_reset_requires_confirmed_recovery(self) -> None:
        kill_switch = KillSwitch(EventBus(), SafetyController(SafetyMode.HALTED))
        kill_switch.trigger("test")
        with pytest.raises(RuntimeError, match="confirmed_recovery"):
            kill_switch.reset()

    @pytest.mark.asyncio
    async def test_trigger_blocks_new_orders(self):
        """After trigger, check() should raise."""
        bus = EventBus(queue_maxsize=100)
        kill_switch = KillSwitch(bus)
        kill_switch.trigger("test")

        with pytest.raises(KillSwitchTriggeredError):
            kill_switch.check()


class TestPerStrategyLossLimit:
    @pytest.mark.asyncio
    async def test_rejects_strategy_with_excessive_loss(self):
        """Strategy that has lost more than max_strategy_loss_pct should be rejected."""
        from hypeedge.account.tracker import AccountTracker
        from hypeedge.risk.checker import RiskChecker, RiskLimits

        tracker = AccountTracker()
        tracker.update_account_state(
            AccountState(
                equity=Usd(10_000.0),
                available_balance=Usd(8_000.0),
                total_margin_used=Usd(2_000.0),
                total_unrealized_pnl=Usd(0.0),
                peak_equity=Usd(10_000.0),
            )
        )

        checker = RiskChecker(tracker, RiskLimits(max_strategy_loss_pct=0.05))
        checker.record_strategy_pnl("trend_v1", -600.0)

        intent = OrderIntent(
            symbol=Symbol("BTC"),
            side=Side.BUY,
            size=Size(0.01),
            price=Price(100.0),
            strategy_id=StrategyId("trend_v1"),
        )
        result = await checker.check(intent)
        assert result.passed is False
        assert "strategy_loss_exceeded" in result.reason

    @pytest.mark.asyncio
    async def test_passes_strategy_within_loss_limit(self):
        """Strategy with loss under the limit should pass."""
        from hypeedge.account.tracker import AccountTracker
        from hypeedge.risk.checker import RiskChecker, RiskLimits

        tracker = AccountTracker()
        tracker.update_account_state(
            AccountState(
                equity=Usd(10_000.0),
                available_balance=Usd(8_000.0),
                total_margin_used=Usd(2_000.0),
                total_unrealized_pnl=Usd(0.0),
                peak_equity=Usd(10_000.0),
            )
        )

        checker = RiskChecker(tracker, RiskLimits(max_strategy_loss_pct=0.05))
        checker.record_strategy_pnl("trend_v1", -400.0)

        intent = OrderIntent(
            symbol=Symbol("BTC"),
            side=Side.BUY,
            size=Size(0.01),
            price=Price(100.0),
            strategy_id=StrategyId("trend_v1"),
        )
        result = await checker.check(intent)
        assert result.passed is True
