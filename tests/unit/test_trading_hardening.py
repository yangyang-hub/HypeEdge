"""Failure-path regression tests for the live trading control plane."""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from hypeedge.account.reconciler import Reconciler
from hypeedge.account.tracker import AccountTracker
from hypeedge.api.routes.events import BufferedEvent, SseBroker, _event_stream, sse_events
from hypeedge.app import HypeEdgeApp
from hypeedge.core.enums import OrderStatus, OrderType, SafetyMode, Side, TimeInForce
from hypeedge.core.events import EVENT_ORDER_FILLED, EVENT_ORDER_REJECTED, Event, EventBus
from hypeedge.core.exceptions import (
    ExecutionError,
    KillSwitchTriggeredError,
    OrderRejectedError,
    OrderTimeoutError,
    SigningError,
)
from hypeedge.core.models import AccountState, Candle, Fill, Order, OrderIntent, Position, RiskCheckResult
from hypeedge.core.types import Cloid, OrderId, Price, Size, StrategyId, Symbol, Timestamp, Usd
from hypeedge.execution.engine import ExecutionEngine
from hypeedge.execution.nonce import NonceManager
from hypeedge.risk.kill_switch import KillSwitch
from hypeedge.risk.safety import SafetyController
from hypeedge.strategy.params import TrendParams
from hypeedge.strategy.trend_follow import TrendFollowStrategy


class TestDurableKillTrigger:
    @pytest.mark.asyncio
    async def test_halting_latch_commits_before_cancel_workflow_starts(self) -> None:
        calls: list[str] = []
        app = HypeEdgeApp.__new__(HypeEdgeApp)
        app._system_state_store = MagicMock()

        async def persist(*args: object, **kwargs: object) -> None:
            calls.append("durable")

        app._system_state_store.transition = AsyncMock(side_effect=persist)
        app._kill_switch = MagicMock()
        app._kill_switch.trigger.side_effect = lambda reason: calls.append("trigger")
        app._kill_switch_active = False
        app._trading_enabled = True
        app._metrics = None

        assert await app.trigger_kill_switch("manual") is True
        assert calls == ["durable", "trigger"]
        app._system_state_store.transition.assert_awaited_once_with("halting", "manual", kill_switch_active=True)
        assert app._kill_switch_active is True
        assert app._trading_enabled is False

    @pytest.mark.asyncio
    async def test_persistence_failure_never_starts_cancel_workflow(self) -> None:
        app = HypeEdgeApp.__new__(HypeEdgeApp)
        app._system_state_store = MagicMock()
        app._system_state_store.transition = AsyncMock(side_effect=OSError("postgres down"))
        app._kill_switch = MagicMock()
        app._kill_switch_active = False
        app._trading_enabled = True
        app._metrics = None

        assert await app.trigger_kill_switch("manual") is False
        app._kill_switch.trigger.assert_not_called()
        assert app._trading_enabled is False


def _intent(*, reduce_only: bool = False, cloid: str | None = None) -> OrderIntent:
    return OrderIntent(
        symbol=Symbol("BTC"),
        side=Side.BUY,
        size=Size(0.01),
        price=Price(50_000),
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        strategy_id=StrategyId("trend"),
        reduce_only=reduce_only,
        cloid=Cloid(cloid) if cloid else None,
    )


def _order(
    cloid: str = "working",
    *,
    status: OrderStatus = OrderStatus.ACKNOWLEDGED,
    strategy_id: str = "trend",
    side: Side = Side.BUY,
) -> Order:
    return Order(
        cloid=Cloid(cloid),
        symbol=Symbol("BTC"),
        side=side,
        size=Size(0.01),
        price=Price(50_000),
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        status=status,
        strategy_id=StrategyId(strategy_id),
    )


def _candle(price: float = 100.0) -> Candle:
    return Candle(
        symbol=Symbol("BTC"),
        interval="1m",
        open=Price(price),
        high=Price(price + 1),
        low=Price(price - 1),
        close=Price(price),
        volume=Size(1),
        timestamp=Timestamp(1),
    )


async def _start_manager(manager: NonceManager) -> asyncio.Task[None]:
    task = asyncio.create_task(manager.run())
    await asyncio.sleep(0)
    return task


async def _stop_manager(manager: NonceManager, task: asyncio.Task[None]) -> None:
    await manager.stop()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


class TestNonceFailureRecovery:
    @pytest.mark.asyncio
    async def test_timeout_uses_status_lookup_and_never_retries(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import hypeedge.execution.nonce as nonce_module

        monkeypatch.setattr(nonce_module, "_SUBMIT_TIMEOUT_S", 0.01)
        manager = NonceManager()
        exchange = MagicMock(account_address="0xabc")
        info = MagicMock()
        info.query_order_by_cloid.return_value = {"status": "order", "order": {"status": "open"}}
        manager.set_exchange(exchange)
        manager.set_info(info)
        calls = 0

        def slow_action() -> str:
            nonlocal calls
            calls += 1
            time.sleep(0.05)
            return "late"

        task = await _start_manager(manager)
        result = await manager.submit(slow_action, cloid_hint="known-order")
        assert result["status"] == "order"
        assert calls == 1
        assert manager.stats == {"total_actions": 1, "total_errors": 0, "queue_depth": 0}
        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_type_error_with_cloid_is_deterministic_and_not_retried(self) -> None:
        manager = NonceManager()
        manager.set_exchange(MagicMock())
        calls = 0

        def invalid_action() -> None:
            nonlocal calls
            calls += 1
            raise TypeError("bad sdk arguments")

        task = await _start_manager(manager)
        with pytest.raises(TypeError, match="bad sdk arguments"):
            await manager.submit(invalid_action, cloid_hint="order-1")
        assert calls == 1
        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_unknown_exception_with_cloid_becomes_unknown(self) -> None:
        manager = NonceManager()
        manager.set_exchange(MagicMock())
        task = await _start_manager(manager)

        with pytest.raises(OrderTimeoutError, match="unknown outcome"):
            await manager.submit(lambda: (_ for _ in ()).throw(OSError("reset")), cloid_hint="order-1")
        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_unsigned_action_retries_then_exhausts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import hypeedge.execution.nonce as nonce_module

        monkeypatch.setattr(nonce_module, "_BACKOFF_DELAYS", [0.0, 0.0, 0.0])
        manager = NonceManager()
        manager.set_exchange(MagicMock())
        calls = 0

        def transient_action() -> None:
            nonlocal calls
            calls += 1
            raise OSError("network")

        task = await _start_manager(manager)
        with pytest.raises(ExecutionError, match="failed after 2 retries"):
            await manager.submit(transient_action)
        assert calls == 3
        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_signing_error_is_never_retried(self) -> None:
        manager = NonceManager()
        manager.set_exchange(MagicMock())
        action = MagicMock(side_effect=SigningError("invalid signature"))
        task = await _start_manager(manager)

        with pytest.raises(SigningError, match="invalid signature"):
            await manager.submit(action)
        action.assert_called_once()
        await _stop_manager(manager, task)

    @pytest.mark.asyncio
    async def test_status_lookup_falls_back_to_wallet_address(self) -> None:
        manager = NonceManager()
        exchange = MagicMock(spec=[])
        exchange.wallet = SimpleNamespace(address="0xwallet")
        info = MagicMock()
        info.query_order_by_cloid.return_value = {"status": "order"}
        manager.set_exchange(exchange)
        manager.set_info(info)

        result = await manager._query_order_status("readable-cloid")
        assert result == {"status": "order"}
        assert info.query_order_by_cloid.call_args.args[0] == "0xwallet"

    @pytest.mark.asyncio
    async def test_status_lookup_without_address_or_on_error_returns_none(self) -> None:
        manager = NonceManager()
        manager.set_exchange(MagicMock(spec=[]))
        manager.set_info(MagicMock())
        assert await manager._query_order_status("order") is None

        manager.exchange.account_address = "0xabc"
        manager.info.query_order_by_cloid.side_effect = OSError("down")
        assert await manager._query_order_status("order") is None


class TestExecutionFailureModes:
    @pytest.mark.asyncio
    async def test_risk_timeout_rejects_and_enters_cancel_only(self) -> None:
        bus = EventBus()
        safety = SafetyController(SafetyMode.NORMAL)
        checker = MagicMock()
        checker.check = AsyncMock(return_value=RiskCheckResult(passed=False, reason="risk_check_timeout"))
        manager = MagicMock(exchange=MagicMock())
        engine = ExecutionEngine(
            manager,
            bus,
            KillSwitch(bus, safety),
            risk_checker=checker,
            safety_controller=safety,
        )

        order = await engine.submit_order(_intent())
        assert order.status == OrderStatus.REJECTED
        assert safety.mode == SafetyMode.CANCEL_ONLY
        manager.submit.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("reason", ["risk_check_error: database failed", "drawdown_exceeded: 0.12 >= 0.10"])
    async def test_critical_risk_failure_triggers_durable_kill(self, reason: str) -> None:
        bus = EventBus()
        checker = MagicMock()
        checker.check = AsyncMock(return_value=RiskCheckResult(passed=False, reason=reason))
        durable_kill = AsyncMock(return_value=True)
        engine = ExecutionEngine(
            MagicMock(exchange=MagicMock()),
            bus,
            KillSwitch(bus),
            risk_checker=checker,
            durable_kill_trigger=durable_kill,
        )

        order = await engine.submit_order(_intent())

        assert order.status == OrderStatus.REJECTED
        durable_kill.assert_awaited_once_with(reason)

    @pytest.mark.asyncio
    async def test_ordinary_risk_rejection_does_not_degrade_safety(self) -> None:
        bus = EventBus()
        safety = SafetyController(SafetyMode.NORMAL)
        checker = MagicMock()
        checker.check = AsyncMock(return_value=RiskCheckResult(passed=False, reason="position_limit"))
        engine = ExecutionEngine(
            MagicMock(exchange=MagicMock()),
            bus,
            KillSwitch(bus, safety),
            risk_checker=checker,
            safety_controller=safety,
        )

        assert (await engine.submit_order(_intent())).status == OrderStatus.REJECTED
        assert safety.mode == SafetyMode.NORMAL

    @pytest.mark.asyncio
    async def test_timeout_marks_submission_unknown(self) -> None:
        bus = EventBus()
        manager = MagicMock(exchange=MagicMock())
        manager.submit = AsyncMock(side_effect=OrderTimeoutError("timeout", cloid="order"))
        engine = ExecutionEngine(manager, bus, KillSwitch(bus))

        order = await engine.submit_order(_intent(cloid="order"))
        assert order.status == OrderStatus.SUBMIT_UNKNOWN
        assert order.error_message == "timeout"

    @pytest.mark.asyncio
    async def test_immediate_fill_updates_account_projection(self) -> None:
        bus = EventBus()
        tracker = AccountTracker()
        manager = MagicMock(exchange=MagicMock())
        manager.submit = AsyncMock(
            return_value={
                "status": "ok",
                "response": {"data": {"statuses": [{"filled": {"oid": 7, "avgPx": "50010", "totalSz": "0.01"}}]}},
            }
        )
        engine = ExecutionEngine(manager, bus, KillSwitch(bus), account_tracker=tracker)

        order = await engine.submit_order(_intent())
        position = tracker.get_position(Symbol("BTC"))
        assert order.status == OrderStatus.FILLED
        assert position is not None
        assert position.size == Size(0.01)
        assert position.entry_price == Price(50_010)

    @pytest.mark.asyncio
    async def test_immediate_fill_does_not_seed_durable_authoritative_aggregate(self) -> None:
        bus = EventBus()
        persisted_filled_sizes: list[Size] = []
        store = MagicMock()
        store.get_order = AsyncMock(return_value=None)
        store.persist_placement = AsyncMock(return_value=None)

        async def capture_transition(order: Order, *args: object, **kwargs: object) -> None:
            del args, kwargs
            persisted_filled_sizes.append(order.filled_size)

        store.persist_transition = AsyncMock(side_effect=capture_transition)
        manager = MagicMock(exchange=MagicMock())
        manager.submit = AsyncMock(
            return_value={
                "status": "ok",
                "response": {"data": {"statuses": [{"filled": {"oid": 7, "avgPx": "100", "totalSz": "0.01"}}]}},
            }
        )
        engine = ExecutionEngine(manager, bus, KillSwitch(bus), durable_store=store)

        order = await engine.submit_order(_intent())

        assert order.filled_size == Size(0.01)
        assert order.avg_fill_price == Price(100)
        assert persisted_filled_sizes[-1] == Size(0)

    @pytest.mark.asyncio
    async def test_unknown_exchange_response_is_not_acknowledged(self) -> None:
        bus = EventBus()
        manager = MagicMock(exchange=MagicMock())
        manager.submit = AsyncMock(return_value={"unexpected": True})
        engine = ExecutionEngine(manager, bus, KillSwitch(bus))

        order = await engine.submit_order(_intent())
        assert order.status == OrderStatus.SUBMIT_UNKNOWN
        assert order.error_message == "unknown_exchange_response"

    @pytest.mark.asyncio
    async def test_cancel_failure_preserves_open_order(self) -> None:
        bus = EventBus()
        manager = MagicMock(exchange=MagicMock())
        manager.submit = AsyncMock(
            side_effect=[
                {"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 1}}]}}},
                OSError("cancel failed"),
            ]
        )
        engine = ExecutionEngine(manager, bus, KillSwitch(bus))
        order = await engine.submit_order(_intent())

        with pytest.raises(ExecutionError):
            await engine.cancel_order(str(order.cloid))
        assert order.status == OrderStatus.ACKNOWLEDGED

    @pytest.mark.asyncio
    async def test_cancel_all_filters_symbol_and_skips_terminal_orders(self) -> None:
        bus = EventBus()
        engine = ExecutionEngine(MagicMock(exchange=MagicMock()), bus, KillSwitch(bus))
        btc = _order("btc")
        eth = _order("eth")
        eth.symbol = Symbol("ETH")
        terminal = _order("filled", status=OrderStatus.FILLED)
        engine.import_exchange_order(btc)
        engine.import_exchange_order(eth)
        engine.import_exchange_order(terminal)
        engine.cancel_order = AsyncMock(return_value=True)  # type: ignore[method-assign]

        assert await engine.cancel_all_orders("BTC") == 1
        engine.cancel_order.assert_awaited_once_with("btc")

    @pytest.mark.asyncio
    async def test_exchange_exception_rejects_and_persists_failure(self) -> None:
        bus = EventBus()
        manager = MagicMock(exchange=MagicMock())
        manager.submit = AsyncMock(side_effect=ValueError("invalid price"))
        store = MagicMock()
        store.get_order = AsyncMock(return_value=None)
        store.persist_placement = AsyncMock()
        store.persist_transition = AsyncMock()
        engine = ExecutionEngine(manager, bus, KillSwitch(bus), durable_store=store)

        order = await engine.submit_order(_intent())
        assert order.status == OrderStatus.REJECTED
        store.persist_placement.assert_awaited_once()
        transition = store.persist_transition.await_args
        assert transition.args[1] == "rejected"
        assert transition.kwargs["command_status"] == "failed"

    @pytest.mark.asyncio
    async def test_cancel_persists_request_and_failure(self) -> None:
        bus = EventBus()
        manager = MagicMock(exchange=MagicMock())
        manager.submit = AsyncMock(side_effect=OSError("down"))
        store = MagicMock()
        store.persist_cancel_requested = AsyncMock()
        store.persist_transition = AsyncMock()
        engine = ExecutionEngine(manager, bus, KillSwitch(bus), durable_store=store)
        order = _order("0x" + "b" * 32)
        engine.import_exchange_order(order)

        with pytest.raises(ExecutionError):
            await engine.cancel_order(str(order.cloid))
        store.persist_cancel_requested.assert_awaited_once()
        assert store.persist_transition.await_args.args[1] == "cancel_failed"
        assert order.error_message == "down"

    @pytest.mark.asyncio
    async def test_cancel_requires_exchange_but_terminal_order_does_not(self) -> None:
        bus = EventBus()
        manager = MagicMock(exchange=None)
        engine = ExecutionEngine(manager, bus, KillSwitch(bus))
        open_order = _order("open")
        filled = _order("filled", status=OrderStatus.FILLED)
        engine.import_exchange_order(open_order)
        engine.import_exchange_order(filled)

        with pytest.raises(ExecutionError, match="No Exchange"):
            await engine.cancel_order("open")
        assert await engine.cancel_order("filled") is False

    @pytest.mark.asyncio
    async def test_recover_open_orders_from_durable_store(self) -> None:
        store = MagicMock()
        recovered = [_order("one"), _order("two")]
        store.load_open_orders = AsyncMock(return_value=recovered)
        bus = EventBus()
        engine = ExecutionEngine(MagicMock(), bus, KillSwitch(bus), durable_store=store)

        assert await engine.recover_open_orders() == 2
        assert await engine.get_order("one") is recovered[0]

    @pytest.mark.asyncio
    async def test_recover_without_store_is_noop(self) -> None:
        bus = EventBus()
        engine = ExecutionEngine(MagicMock(), bus, KillSwitch(bus))
        assert await engine.recover_open_orders() == 0

    @pytest.mark.asyncio
    async def test_market_open_uses_side_and_canonical_cloid(self) -> None:
        bus = EventBus()
        exchange = MagicMock()
        manager = MagicMock(exchange=exchange)
        manager.submit = AsyncMock(return_value="accepted")
        engine = ExecutionEngine(manager, bus, KillSwitch(bus))
        intent = OrderIntent(
            symbol=Symbol("BTC"),
            side=Side.SELL,
            size=Size(0.02),
            order_type=OrderType.MARKET,
            strategy_id=StrategyId("trend"),
        )

        order = await engine.submit_order(intent)
        assert order.status == OrderStatus.ACKNOWLEDGED
        args = manager.submit.await_args.args
        assert args[:4] == (exchange.market_open, "BTC", False, 0.02)
        assert manager.submit.await_args.kwargs["cloid"].to_raw().startswith("0x")

    @pytest.mark.parametrize(
        ("exchange_status", "expected"),
        [
            ("filled", OrderStatus.FILLED),
            ("cancelled", OrderStatus.CANCELLED),
            ("margincanceled", OrderStatus.REJECTED),
            ("open", OrderStatus.ACKNOWLEDGED),
        ],
    )
    @pytest.mark.asyncio
    async def test_status_lookup_response_maps_to_terminal_or_open_state(
        self,
        exchange_status: str,
        expected: OrderStatus,
    ) -> None:
        bus = EventBus()
        manager = MagicMock(exchange=MagicMock())
        manager.submit = AsyncMock(return_value={"status": "order", "order": {"status": exchange_status}})
        engine = ExecutionEngine(manager, bus, KillSwitch(bus))
        assert (await engine.submit_order(_intent())).status == expected

    @pytest.mark.asyncio
    async def test_success_without_detailed_status_is_acknowledged(self) -> None:
        bus = EventBus()
        manager = MagicMock(exchange=MagicMock())
        manager.submit = AsyncMock(return_value={"status": "ok", "response": {"data": {"statuses": []}}})
        engine = ExecutionEngine(manager, bus, KillSwitch(bus))
        assert (await engine.submit_order(_intent())).status == OrderStatus.ACKNOWLEDGED


class TestSafetyControllerPermissions:
    @pytest.mark.parametrize(
        ("mode", "reduce_only", "error"),
        [
            (SafetyMode.NORMAL, False, None),
            (SafetyMode.REDUCE_ONLY, True, None),
            (SafetyMode.REDUCE_ONLY, False, OrderRejectedError),
            (SafetyMode.CANCEL_ONLY, True, OrderRejectedError),
            (SafetyMode.HALTING, True, KillSwitchTriggeredError),
            (SafetyMode.HALTED, False, KillSwitchTriggeredError),
        ],
    )
    def test_placement_permission_matrix(
        self,
        mode: SafetyMode,
        reduce_only: bool,
        error: type[Exception] | None,
    ) -> None:
        controller = SafetyController(mode)
        if error is None:
            controller.check_placement(_intent(reduce_only=reduce_only))
        else:
            with pytest.raises(error):
                controller.check_placement(_intent(reduce_only=reduce_only))

    def test_cancel_only_does_not_override_halt(self) -> None:
        controller = SafetyController(SafetyMode.HALTED)
        controller.enter_cancel_only("reconcile failed")
        assert controller.mode == SafetyMode.HALTED

    @pytest.mark.parametrize("mode", [SafetyMode.STARTING, SafetyMode.RECONCILING])
    def test_emergency_close_rejected_before_state_is_known(self, mode: SafetyMode) -> None:
        with pytest.raises(OrderRejectedError):
            SafetyController(mode).check_emergency_close()

    @pytest.mark.parametrize("mode", [SafetyMode.NORMAL, SafetyMode.CANCEL_ONLY, SafetyMode.HALTED])
    def test_emergency_close_allowed_after_startup(self, mode: SafetyMode) -> None:
        SafetyController(mode).check_emergency_close()


def _exchange_state(*, positions: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {
        "assetPositions": positions or [],
        "marginSummary": {
            "accountValue": "10000",
            "totalMarginAvailable": "9000",
            "totalMarginUsed": "1000",
        },
        "withdrawable": "9000",
    }


class TestReconciliationFailures:
    @pytest.mark.asyncio
    async def test_position_fetch_failure_is_fail_closed_and_degrades_safety(self) -> None:
        bus = EventBus()
        tracker = AccountTracker()
        tracker.update_position_from_exchange(Symbol("ETH"), Position(Symbol("ETH"), Size(1), Price(100)))
        info = MagicMock()
        info.open_orders.return_value = []
        info.user_state.side_effect = OSError("user state unavailable")
        safety = SafetyController(SafetyMode.NORMAL)
        engine = MagicMock()
        engine.get_open_orders = AsyncMock(return_value=[])
        reconciler = Reconciler(
            bus,
            tracker,
            engine,
            info_client=info,
            account_address="0xabc",
            safety_controller=safety,
        )

        result = await reconciler.reconcile()
        assert result.success is False
        assert tracker.get_position(Symbol("ETH")) is not None
        assert safety.mode == SafetyMode.CANCEL_ONLY

    @pytest.mark.asyncio
    async def test_unknown_local_order_fails_without_guessing_terminal_status(self) -> None:
        bus = EventBus()
        local = _order("local")
        engine = MagicMock()
        engine.get_open_orders = AsyncMock(return_value=[local])
        engine.import_exchange_order_authoritative = AsyncMock()
        info = MagicMock()
        info.open_orders.return_value = []
        info.user_state.return_value = _exchange_state()
        info.query_order_by_cloid.return_value = {"status": "unknownOid"}
        reconciler = Reconciler(bus, AccountTracker(), engine, info_client=info, account_address="0xabc")

        result = await reconciler.reconcile()
        assert result.success is False
        assert "order_status_unknown:local" in result.errors[0]
        assert local.status == OrderStatus.ACKNOWLEDGED

    @pytest.mark.asyncio
    async def test_missing_local_order_applies_authoritative_filled_status(self) -> None:
        local = _order("local")
        engine = MagicMock()
        engine.get_open_orders = AsyncMock(return_value=[local])
        engine.import_exchange_order_authoritative = AsyncMock()
        info = MagicMock()
        info.open_orders.return_value = []
        info.user_state.return_value = _exchange_state()
        info.query_order_by_cloid.return_value = {"status": "order", "order": {"status": "filled"}}
        reconciler = Reconciler(EventBus(), AccountTracker(), engine, info_client=info, account_address="0xabc")

        result = await reconciler.reconcile()
        assert result.success is True
        assert result.orders_corrected == 1
        assert local.status == OrderStatus.FILLED

    @pytest.mark.asyncio
    async def test_exchange_only_order_is_imported_with_full_semantics(self) -> None:
        engine = MagicMock()
        engine.get_open_orders = AsyncMock(return_value=[])
        engine.import_exchange_order_authoritative = AsyncMock()
        info = MagicMock()
        canonical = "0x" + "a" * 32
        info.open_orders.return_value = [
            {
                "cloid": canonical,
                "coin": "ETH",
                "side": "A",
                "sz": "2.5",
                "limitPx": "3000",
                "oid": 12,
                "reduceOnly": True,
            }
        ]
        info.user_state.return_value = _exchange_state()
        reconciler = Reconciler(EventBus(), AccountTracker(), engine, info_client=info, account_address="0xabc")

        result = await reconciler.reconcile()
        imported = engine.import_exchange_order_authoritative.await_args.args[0]
        assert result.success is True
        assert imported.cloid == Cloid(canonical)
        assert imported.side == Side.SELL
        assert imported.size == Size(2.5)
        assert imported.reduce_only is True

    @pytest.mark.asyncio
    async def test_successful_recovery_transitions_to_normal_and_publishes(self) -> None:
        bus = EventBus()
        queue = bus.subscribe_all()
        safety = SafetyController(SafetyMode.RECOVERING)
        engine = MagicMock()
        engine.get_open_orders = AsyncMock(return_value=[])
        info = MagicMock()
        info.open_orders.return_value = []
        info.user_state.return_value = _exchange_state()
        reconciler = Reconciler(
            bus,
            AccountTracker(),
            engine,
            info_client=info,
            account_address="0xabc",
            safety_controller=safety,
        )

        result = await reconciler.reconcile()
        assert result.success is True
        assert safety.mode == SafetyMode.NORMAL
        assert queue.get_nowait().payload is result

    def test_position_size_mismatch_is_replaced_by_exchange(self) -> None:
        tracker = AccountTracker()
        tracker.update_position_from_exchange(Symbol("BTC"), Position(Symbol("BTC"), Size(1), Price(40_000)))
        reconciler = Reconciler(EventBus(), tracker, MagicMock())

        corrected = reconciler._reconcile_positions(
            {"BTC": {"szi": "2", "entryPx": "50_000", "leverage": {"value": "3"}}}
        )
        position = tracker.get_position(Symbol("BTC"))
        assert corrected == 1
        assert position is not None
        assert position.size == Size(2)
        assert position.entry_price == Price(50_000)

    @pytest.mark.parametrize(
        ("response", "message"),
        [({"bad": True}, "invalid_user_state_response"), ([], "invalid_user_state_response")],
    )
    @pytest.mark.asyncio
    async def test_invalid_position_snapshot_is_rejected(self, response: object, message: str) -> None:
        info = MagicMock()
        info.user_state.return_value = response
        reconciler = Reconciler(EventBus(), AccountTracker(), MagicMock(), info_client=info, account_address="0xabc")
        with pytest.raises(RuntimeError, match=message):
            await reconciler._fetch_exchange_positions()


class TestSseIsolationAndReplay:
    @staticmethod
    def _app() -> SimpleNamespace:
        return SimpleNamespace(event_bus=EventBus(), is_shutting_down=False)

    def test_buffered_event_encoding_is_valid_sse(self) -> None:
        encoded = BufferedEvent(7, "OrderFilled", '{"ok":true}').encode()
        assert encoded == 'id: 7\nevent: OrderFilled\nretry: 3000\ndata: {"ok":true}\n\n'

    @pytest.mark.asyncio
    async def test_multiple_clients_are_isolated_and_unsupported_events_are_filtered(self) -> None:
        app = self._app()
        broker = SseBroker(app)
        broker.start()
        first, _ = broker.subscribe(None)
        second, _ = broker.subscribe(None)
        try:
            app.event_bus.publish_sync(Event(event_type="CandleUpdate", payload={"close": 1}))
            app.event_bus.publish_sync(Event(event_type="OrderFilled", payload={"cloid": "x"}))
            one = await asyncio.wait_for(first.get(), 0.2)
            two = await asyncio.wait_for(second.get(), 0.2)
            assert one.sequence == two.sequence == 1
            assert json.loads(one.data)["payload"] == {"cloid": "x"}

            broker.unsubscribe(first)
            app.event_bus.publish_sync(Event(event_type="OrderRejected", payload={"cloid": "y"}))
            assert (await asyncio.wait_for(second.get(), 0.2)).sequence == 2
            assert first.empty()
        finally:
            await broker.stop()

    @pytest.mark.asyncio
    async def test_replay_respects_last_event_id(self) -> None:
        app = self._app()
        broker = SseBroker(app, replay_size=2)
        broker.start()
        try:
            for index in range(3):
                app.event_bus.publish_sync(Event(event_type="OrderFilled", payload={"index": index}))
            for _ in range(20):
                if broker._sequence == 3:
                    break
                await asyncio.sleep(0.005)
            queue, replay = broker.subscribe(after_sequence=1)
            assert [event.sequence for event in replay] == [2, 3]
            broker.unsubscribe(queue)
        finally:
            await broker.stop()

    @pytest.mark.asyncio
    async def test_slow_client_is_dropped_without_affecting_fast_client(self) -> None:
        app = self._app()
        broker = SseBroker(app)
        broker.start()
        slow, _ = broker.subscribe(None)
        fast, _ = broker.subscribe(None)
        for index in range(slow.maxsize):
            slow.put_nowait(BufferedEvent(index, "OrderFilled", "{}"))
        try:
            app.event_bus.publish_sync(Event(event_type="OrderFilled", payload={"cloid": "fast"}))
            event = await asyncio.wait_for(fast.get(), 0.2)
            assert event.sequence == 1
            assert slow not in broker._clients
            assert fast in broker._clients
        finally:
            await broker.stop()

    @pytest.mark.asyncio
    async def test_event_stream_replays_then_unsubscribes_disconnected_client(self) -> None:
        app = self._app()
        broker = SseBroker(app)
        broker._replay.append(BufferedEvent(1, "OrderFilled", "{}"))
        app._api_sse_broker = broker
        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=True))
        stream = _event_stream(request, app, after_sequence=0)
        try:
            assert await anext(stream) == BufferedEvent(1, "OrderFilled", "{}").encode()
            with pytest.raises(StopAsyncIteration):
                await anext(stream)
            assert not broker._clients
            assert app.event_bus.stats["subscribers"] == 1
        finally:
            await broker.stop()

    @pytest.mark.asyncio
    async def test_sse_endpoint_ignores_invalid_last_event_id(self) -> None:
        app = self._app()
        response = await sse_events(MagicMock(), app, last_event_id="not-a-number")
        assert response.media_type == "text/event-stream"
        assert response.headers["x-accel-buffering"] == "no"

    @pytest.mark.asyncio
    async def test_event_stream_emits_heartbeat_while_idle(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import hypeedge.api.routes.events as events_module

        app = self._app()
        request = SimpleNamespace(is_disconnected=AsyncMock(side_effect=[False, True]))

        async def timeout_wait_for(awaitable: object, timeout: float) -> None:
            assert timeout == 15.0
            close = getattr(awaitable, "close", None)
            if close is not None:
                close()
            raise TimeoutError

        monkeypatch.setattr(events_module.asyncio, "wait_for", timeout_wait_for)
        stream = _event_stream(request, app, after_sequence=None)
        broker: SseBroker | None = None
        try:
            assert await anext(stream) == ": heartbeat\n\n"
            with pytest.raises(StopAsyncIteration):
                await anext(stream)
            broker = app._api_sse_broker
            assert not broker._clients
        finally:
            if broker is None:
                broker = app._api_sse_broker
            await broker.stop()


class TestStrategyFillProjection:
    @staticmethod
    def _strategy(tracker: AccountTracker | None = None) -> TrendFollowStrategy:
        return TrendFollowStrategy(
            StrategyId("trend"),
            EventBus(),
            AsyncMock(),
            TrendParams(symbol="BTC"),
            account_tracker=tracker,
        )

    @pytest.mark.asyncio
    async def test_stale_terminal_event_cannot_clear_current_working_order(self) -> None:
        strategy = self._strategy()
        strategy._working_order_cloid = "current"
        await strategy.on_event(Event(EVENT_ORDER_REJECTED, _order("stale", status=OrderStatus.REJECTED)))
        assert strategy._working_order_cloid == "current"

    @pytest.mark.asyncio
    async def test_fill_waits_for_account_projection_before_unblocking(self) -> None:
        tracker = AccountTracker()
        strategy = self._strategy(tracker)
        strategy._working_order_cloid = "current"

        fill_order = _order("current", status=OrderStatus.FILLED)
        await strategy.on_event(Event(EVENT_ORDER_FILLED, fill_order))
        assert strategy._working_order_cloid == "current"

        tracker.update_fill(
            Fill(
                cloid=Cloid("current"),
                exchange_oid=OrderId("1"),
                symbol=Symbol("BTC"),
                side=Side.BUY,
                price=Price(50_000),
                size=Size(0.01),
                fee=Usd(0),
                is_maker=False,
                timestamp=Timestamp(1),
                strategy_id=StrategyId("trend"),
            )
        )
        await strategy.on_event(Event(EVENT_ORDER_FILLED, fill_order))
        assert strategy._working_order_cloid is None
        assert strategy.position_size == 0.01

    @pytest.mark.asyncio
    async def test_close_fill_waits_until_position_projection_is_flat(self) -> None:
        tracker = AccountTracker()
        tracker.update_position_from_exchange(Symbol("BTC"), Position(Symbol("BTC"), Size(0.01), Price(50_000)))
        strategy = self._strategy(tracker)
        strategy._working_order_cloid = "close"
        strategy._working_order_is_close = True
        close_order = _order("close", status=OrderStatus.FILLED, side=Side.SELL)

        await strategy.on_event(Event(EVENT_ORDER_FILLED, close_order))
        assert strategy._working_order_cloid == "close"

        tracker.remove_position(Symbol("BTC"))
        await strategy.on_event(Event(EVENT_ORDER_FILLED, close_order))
        assert strategy._working_order_cloid is None
        assert strategy.position_size == 0

    @pytest.mark.asyncio
    async def test_matching_rejection_releases_working_order_without_changing_position(self) -> None:
        tracker = AccountTracker()
        tracker.update_position_from_exchange(Symbol("BTC"), Position(Symbol("BTC"), Size(1), Price(50_000)))
        strategy = self._strategy(tracker)
        strategy._working_order_cloid = "current"

        await strategy.on_event(Event(EVENT_ORDER_REJECTED, _order("current", status=OrderStatus.REJECTED)))
        assert strategy._working_order_cloid is None
        assert strategy.position_size == 1

    def test_invalid_position_sizing_inputs_are_fail_safe(self) -> None:
        strategy = self._strategy()
        assert strategy._calculate_position_size(0, 1) == 0
        assert strategy._calculate_position_size(100, 0) == 0

    @pytest.mark.asyncio
    async def test_open_short_sets_upper_stop_and_publishes_signal(self) -> None:
        strategy = self._strategy()
        strategy._execution.submit_order.return_value = _order("short", side=Side.SELL)
        signal_queue = strategy._event_bus.subscribe_all()

        await strategy._open_position(Side.SELL, 100, 2)
        intent = strategy._execution.submit_order.await_args.args[0]
        assert intent.side == Side.SELL
        assert strategy.stop_price == 104
        assert strategy._working_order_cloid == "short"
        assert signal_queue.get_nowait().payload.action == "sell"

    @pytest.mark.asyncio
    async def test_open_failure_does_not_create_working_order(self) -> None:
        strategy = self._strategy()
        strategy._execution.submit_order.side_effect = OSError("down")
        await strategy._open_position(Side.BUY, 100, 1)
        assert strategy._working_order_cloid is None

    @pytest.mark.parametrize(("size", "expected_side"), [(1.0, Side.SELL), (-1.0, Side.BUY)])
    @pytest.mark.asyncio
    async def test_close_derives_side_and_is_always_reduce_only(self, size: float, expected_side: Side) -> None:
        tracker = AccountTracker()
        tracker.update_position_from_exchange(Symbol("BTC"), Position(Symbol("BTC"), Size(size), Price(100)))
        strategy = self._strategy(tracker)
        strategy._execution.submit_order.return_value = _order("close", side=expected_side)

        await strategy._close_position(101)
        intent = strategy._execution.submit_order.await_args.args[0]
        assert intent.side == expected_side
        assert intent.size == abs(size)
        assert intent.order_type == OrderType.MARKET
        assert intent.reduce_only is True
        assert strategy._working_order_is_close is True

    @pytest.mark.asyncio
    async def test_close_failure_preserves_projected_position(self) -> None:
        tracker = AccountTracker()
        tracker.update_position_from_exchange(Symbol("BTC"), Position(Symbol("BTC"), Size(1), Price(100)))
        strategy = self._strategy(tracker)
        strategy._execution.submit_order.side_effect = OSError("down")
        await strategy._close_position(99)
        assert strategy.position_size == 1
        assert strategy._working_order_cloid is None

    @pytest.mark.asyncio
    async def test_process_candle_closes_long_and_short_stops(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import hypeedge.strategy.trend_follow as strategy_module

        monkeypatch.setattr(strategy_module, "macd", lambda *_: ([1.0], [0.0], [1.0]))
        monkeypatch.setattr(strategy_module, "atr", lambda *_: [1.0])
        monkeypatch.setattr(strategy_module, "momentum", lambda *_: [1.0])
        strategy = self._strategy()
        strategy._close_position = AsyncMock()  # type: ignore[method-assign]
        strategy._position_size = 1
        strategy._stop_price = 101
        await strategy._process_candle(_candle(100))
        strategy._close_position.assert_awaited_once_with(100.0)

        strategy._close_position.reset_mock()
        strategy._position_size = -1
        strategy._stop_price = 99
        await strategy._process_candle(_candle(100))
        strategy._close_position.assert_awaited_once_with(100.0)

    @pytest.mark.parametrize(
        ("previous_above", "macd_values", "momentum_value", "expected_side"),
        [
            (False, ([1.0], [0.0], [1.0]), 1.0, Side.BUY),
            (True, ([0.0], [1.0], [-1.0]), -1.0, Side.SELL),
        ],
    )
    @pytest.mark.asyncio
    async def test_process_candle_opens_on_forced_cross(
        self,
        monkeypatch: pytest.MonkeyPatch,
        previous_above: bool,
        macd_values: tuple[list[float], list[float], list[float]],
        momentum_value: float,
        expected_side: Side,
    ) -> None:
        import hypeedge.strategy.trend_follow as strategy_module

        monkeypatch.setattr(strategy_module, "macd", lambda *_: macd_values)
        monkeypatch.setattr(strategy_module, "atr", lambda *_: [1.0])
        monkeypatch.setattr(strategy_module, "momentum", lambda *_: [momentum_value])
        strategy = self._strategy()
        strategy._prev_macd_above_signal = previous_above
        strategy._open_position = AsyncMock()  # type: ignore[method-assign]

        await strategy._process_candle(_candle())
        strategy._open_position.assert_awaited_once_with(expected_side, 100.0, 1.0)

    @pytest.mark.asyncio
    async def test_process_candle_ignores_nan_indicators(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import hypeedge.strategy.trend_follow as strategy_module

        monkeypatch.setattr(strategy_module, "macd", lambda *_: ([], [], []))
        monkeypatch.setattr(strategy_module, "atr", lambda *_: [])
        monkeypatch.setattr(strategy_module, "momentum", lambda *_: [])
        strategy = self._strategy()
        strategy._open_position = AsyncMock()  # type: ignore[method-assign]
        await strategy._process_candle(_candle())
        strategy._open_position.assert_not_awaited()

    def test_tracker_equity_and_position_are_used_for_state_and_sizing(self) -> None:
        tracker = AccountTracker()
        tracker.update_account_state(AccountState(Usd(1_000), Usd(900), Usd(100), Usd(0), Usd(1_000)))
        tracker.update_position_from_exchange(Symbol("BTC"), Position(Symbol("BTC"), Size(2), Price(90)))
        strategy = self._strategy(tracker)
        strategy._sync_position_from_tracker()
        assert strategy.position_size == 2
        assert strategy.entry_price == 90
        assert strategy._calculate_position_size(100, 1) <= 1.5


def test_account_state_helper_is_well_formed() -> None:
    """Keep the exact account projection contract visible in this regression module."""
    state = AccountState(Usd(100), Usd(90), Usd(10), Usd(0), Usd(100))
    assert state.available_balance + state.total_margin_used == state.equity
