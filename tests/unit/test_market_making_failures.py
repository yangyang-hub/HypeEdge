"""Deterministic P8 failure-injection matrix for market-making safety."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from hypeedge.account.tracker import AccountTracker
from hypeedge.config.settings import ActionBudgetSettings, ClickHouseSettings
from hypeedge.core.enums import ActionBudgetMode, OrderStatus, SafetyMode, Side, TimeInForce
from hypeedge.core.events import EVENT_L2_BOOK_UPDATE, EVENT_ORDER_FILLED, Event, EventBus
from hypeedge.core.exceptions import EventBusBackpressureError, TradingCommandPersistenceError
from hypeedge.core.models import Fill, L2BookSnapshot, L2Level, OrderIntent, Position, RiskCheckResult
from hypeedge.core.types import Cloid, OrderId, Price, Size, StrategyId, Symbol, Timestamp, Usd
from hypeedge.execution.batch import (
    BatchChild,
    BatchExecutionCommand,
    BatchOutcome,
    ChildActionType,
    ChildOutcome,
    DispatchGuardContext,
    GuardDecision,
    NetworkAttempt,
    evaluate_dispatch_guard,
)
from hypeedge.execution.emergency_cancel import (
    EmergencyCancelJournal,
    EmergencyCancelTarget,
    WalEmergencyCancelExecutor,
)
from hypeedge.execution.recovery import RecoveryOwner, RecoveryReason, RecoveryRegistry
from hypeedge.market_data.features import MarketFeatureEngine
from hypeedge.risk.action_budget import (
    ActionBudgetController,
    BudgetAction,
    CancelHeadroomSnapshot,
    NetworkAttemptDebit,
    RemoteActionSnapshot,
)
from hypeedge.risk.canary import CanaryDirective, CanaryGateEvaluator, CanaryObservation, CanaryRiskEnvelope
from hypeedge.risk.kill_switch import KillSwitch
from hypeedge.risk.safety import SafetyController
from hypeedge.storage.clickhouse import ClickHouseWriter
from hypeedge.trading.command_service import DataHealthDecision, GateDecision, TradingCommandService
from hypeedge.trading.quotes import QuoteRiskOwner, QuoteSlotKey

NOW = datetime(2026, 7, 11, tzinfo=UTC)
OWNER = "0x" + "1" * 40
BTC = Symbol("BTC")
STRATEGY = StrategyId("mm-btc")


def _child(ordinal: int, action: ChildActionType, *, depends_on: uuid.UUID | None = None) -> BatchChild:
    return BatchChild(uuid.uuid4(), ordinal, action, 7, depends_on=depends_on)


def _book(*, version: int, bid: str = "99.9", ask: str = "100.1", received_at: datetime = NOW) -> L2BookSnapshot:
    return L2BookSnapshot(
        BTC,
        (L2Level(Price(bid), Size("1")),),
        (L2Level(Price(ask), Size("1")),),
        Timestamp(1),
        received_at,
        version,
        1,
    )


def _budget_controller(
    *,
    now: datetime = NOW,
    address_remaining: int = 10_000,
    cancel_remaining: int = 10_000,
    settings: ActionBudgetSettings | None = None,
) -> ActionBudgetController:
    selected = settings or ActionBudgetSettings(
        cancel_retry_buffer=0,
        close_action_reserve=0,
        address_conserve_threshold=0,
        address_critical_threshold=0,
        address_cancel_only_threshold=0,
        runway_conserve_hours=0,
        runway_critical_hours=0,
        runway_cancel_only_hours=0,
    )
    controller = ActionBudgetController(OWNER, selected, clock=lambda: now)
    controller.reconcile_remote(RemoteActionSnapshot(OWNER, 10_000, 10_000 - address_remaining, now))
    controller.reconcile_cancel_headroom(CancelHeadroomSnapshot(10_000, 10_000 - cancel_remaining, now))
    return controller


def test_submit_cancel_response_loss_late_result_and_parent_child_crash_are_idempotent() -> None:
    cancel = _child(0, ChildActionType.CANCEL)
    place = _child(1, ChildActionType.PLACE, depends_on=cancel.child_id)
    batch = BatchExecutionCommand(uuid.uuid4(), 7, (cancel, place))
    sent = cancel.record_attempt(NetworkAttempt.sent(b"cancel", sent_at=NOW, attempt_id=uuid.uuid4()))
    unknown = sent.resolve(ChildOutcome.UNKNOWN, "response_lost_after_network_boundary")
    batch = batch.replace_child(unknown)

    assert batch.outcome == BatchOutcome.UNKNOWN
    assert batch.actual_child_action_cost == 1
    assert batch.dispatchable_children() == ()
    with pytest.raises(ValueError, match="UNKNOWN"):
        unknown.record_attempt(NetworkAttempt.sent(b"unsafe-resend", sent_at=NOW))

    late_success = unknown.resolve(ChildOutcome.SUCCEEDED, "late_exchange_response")
    recovered = batch.replace_child(late_success)
    assert recovered.dispatchable_children() == (place,)
    assert recovered.replace_child(late_success) == recovered


def test_fill_queue_overflow_is_loud_and_rest_backfill_projection_is_exactly_once() -> None:
    bus = EventBus(queue_maxsize=1)
    queue = bus.subscribe(EVENT_ORDER_FILLED, maxsize=1)
    first = Event(EVENT_ORDER_FILLED, "fill-1")
    bus.publish_sync(first)
    with pytest.raises(EventBusBackpressureError):
        bus.publish_sync(Event(EVENT_ORDER_FILLED, "fill-2"))
    assert queue.get_nowait() is first

    tracker = AccountTracker()
    fill = Fill(Cloid("fill"), OrderId("1"), BTC, Side.BUY, Price("100"), Size("1"), Usd("0.01"), True, Timestamp(1))
    position = Position(BTC, Size("1"), Price("100"), Price("100"))
    assert tracker.apply_authoritative_fill("rest-fill-1", fill, position) is True
    assert tracker.apply_authoritative_fill("rest-fill-1", fill, position) is False


def test_l2_lossy_overflow_keeps_latest_while_stale_and_crossed_books_fail_closed() -> None:
    bus = EventBus(queue_maxsize=1)
    queue = bus.subscribe(EVENT_L2_BOOK_UPDATE, maxsize=1)
    bus.publish_sync(Event(EVENT_L2_BOOK_UPDATE, _book(version=1)))
    latest = _book(version=3)
    bus.publish_sync(Event(EVENT_L2_BOOK_UPDATE, latest))
    assert queue.get_nowait().payload is latest
    assert bus.stats["drop_count"] == 1

    engine = MarketFeatureEngine()
    stale = engine.build(_book(version=4, received_at=NOW - timedelta(seconds=10)), healthy=False)
    assert stale.healthy is False
    with pytest.raises(ValueError, match="non-crossed"):
        engine.build(_book(version=5, bid="100.1", ask="100.0"), healthy=False)


class _AllowData:
    async def check_placement(self, intent: OrderIntent) -> DataHealthDecision:
        del intent
        return DataHealthDecision(True, reference_price=Price("100"), best_bid=Price("99"), best_ask=Price("101"))


class _AllowRisk:
    async def check(self, intent: OrderIntent, *, reference_price: float | None = None) -> RiskCheckResult:
        del intent, reference_price
        return RiskCheckResult(True)


class _AllowBudget:
    async def check_placement(self, intent: OrderIntent) -> GateDecision:
        del intent
        return GateDecision.allow()


class _IdentityNormalizer:
    def normalize(
        self,
        intent: OrderIntent,
        *,
        best_bid: Price | None = None,
        best_ask: Price | None = None,
    ) -> OrderIntent:
        del best_bid, best_ask
        return intent


class _BrokenSink:
    def __init__(self) -> None:
        self.persist_calls = 0

    async def persist(self, command: object) -> object:
        del command
        self.persist_calls += 1
        raise OSError("postgres unavailable")


async def test_postgres_failure_yields_zero_placements_but_emergency_cancel_is_wal_durable(tmp_path: Path) -> None:
    sink = _BrokenSink()
    service = TradingCommandService(
        safety=SafetyController(SafetyMode.NORMAL),
        data_health=_AllowData(),
        risk=_AllowRisk(),
        action_budget=_AllowBudget(),
        normalizer=_IdentityNormalizer(),
        sink=sink,  # type: ignore[arg-type]
    )
    intent = OrderIntent(BTC, Side.BUY, Size("0.01"), Price("99"), time_in_force=TimeInForce.ALO)
    with pytest.raises(TradingCommandPersistenceError):
        await service.submit_order(intent)
    assert sink.persist_calls == 1

    target = EmergencyCancelTarget("BTC", oid=42)
    journal = EmergencyCancelJournal(tmp_path / "emergency.jsonl")
    await journal.append(attempt_id="crashed-after-fsync", event="dispatch_intent", target=target)
    orders = _OpenOrders([target])
    signed = _SignedCancels(orders)
    result = await WalEmergencyCancelExecutor(signed, orders, journal).recover_pending()
    assert result.success and result.cancelled == 1
    assert signed.calls == [("BTC", 42)]
    assert await journal.pending_attempts() == ()


class _OpenOrders:
    def __init__(self, targets: list[EmergencyCancelTarget]) -> None:
        self.targets = targets

    async def get_open_orders(self) -> list[EmergencyCancelTarget]:
        return list(self.targets)


class _CancelExchange:
    def __init__(self, owner: _OpenOrders) -> None:
        self._owner = owner

    def cancel(self, symbol: str, oid: int | str) -> dict[str, str]:
        self._owner.targets = [target for target in self._owner.targets if target.oid != oid]
        return {"status": "ok", "symbol": symbol}


class _SignedCancels:
    def __init__(self, orders: _OpenOrders) -> None:
        self.exchange = _CancelExchange(orders)
        self.calls: list[tuple[str, int | str]] = []

    async def submit(self, action_fn: Any, *args: Any, cloid_hint: str | None = None, **kwargs: Any) -> Any:
        del cloid_hint
        self.calls.append((str(args[0]), args[1]))
        return action_fn(*args, **kwargs)


async def test_clickhouse_unavailable_spools_decimal_and_replays_after_recovery(tmp_path: Path) -> None:
    writer = ClickHouseWriter(
        ClickHouseSettings(batch_size=100, spool_path=str(tmp_path / "spool.sqlite3")),
        EventBus(),
    )
    writer._client = MagicMock()  # noqa: SLF001
    writer._client.insert.side_effect = RuntimeError("clickhouse unavailable")  # noqa: SLF001
    writer._mm_inventory_rows = [{"strategy_id": "mm", "position_size": Decimal("0.123456789012345678")}]  # noqa: SLF001
    await writer._spool.initialize()  # noqa: SLF001
    await writer._flush_buffer("_mm_inventory_rows", "mm_inventory_samples")  # noqa: SLF001
    pending = await writer._spool.pending()  # noqa: SLF001
    assert pending[0][2][0]["position_size"] == Decimal("0.123456789012345678")

    writer._client.insert.side_effect = None  # noqa: SLF001
    await writer._replay_spool()  # noqa: SLF001
    assert await writer._spool.pending() == []  # noqa: SLF001


def test_remote_budget_correction_and_three_independent_budget_recoveries() -> None:
    controller = _budget_controller()
    controller.debit_network_attempt(
        NetworkAttemptDebit("shadow", (BudgetAction.PLACE,), 1, NOW + timedelta(milliseconds=1))
    )
    assert controller.snapshot(now=NOW + timedelta(seconds=1)).address_remaining == 9_999
    controller.reconcile_remote(RemoteActionSnapshot(OWNER, 10_000, 500, NOW + timedelta(seconds=1)))
    assert controller.snapshot(now=NOW + timedelta(seconds=1)).address_remaining == 9_500

    assert _budget_controller(address_remaining=0).mode == ActionBudgetMode.EXHAUSTED
    assert _budget_controller(address_remaining=100).mode == ActionBudgetMode.NORMAL
    assert _budget_controller(cancel_remaining=0).mode == ActionBudgetMode.EXHAUSTED
    assert _budget_controller(cancel_remaining=100).mode == ActionBudgetMode.NORMAL

    settings = ActionBudgetSettings(
        ip_weight_limit_per_minute=10,
        ip_emergency_reserve=2,
        cancel_retry_buffer=0,
        close_action_reserve=0,
        address_conserve_threshold=0,
        address_critical_threshold=0,
        address_cancel_only_threshold=0,
        runway_conserve_hours=0,
        runway_critical_hours=0,
        runway_cancel_only_hours=0,
    )
    ip = _budget_controller(settings=settings)
    ip.debit_network_attempt(NetworkAttemptDebit("ip", (), 10, NOW))
    assert ip.mode == ActionBudgetMode.EXHAUSTED
    recovered = ActionBudgetController(OWNER, settings, clock=lambda: NOW + timedelta(seconds=61))
    recovered.reconcile_remote(RemoteActionSnapshot(OWNER, 10_000, 0, NOW + timedelta(seconds=61)))
    recovered.reconcile_cancel_headroom(CancelHeadroomSnapshot(10_000, 0, NOW + timedelta(seconds=61)))
    assert recovered.mode == ActionBudgetMode.NORMAL


async def test_kill_switch_authoritatively_cancels_dual_side_partial_and_unknown_orders() -> None:
    possible_live = ["bid_ack", "ask_partial", "bid_submit_unknown", "ask_cancel_unknown"]
    calls: list[tuple[str, ...]] = []

    async def cancel_all() -> int:
        calls.append(tuple(possible_live))
        count = len(possible_live)
        possible_live.clear()
        return count

    async def verify() -> bool:
        return not possible_live

    safety = SafetyController(SafetyMode.NORMAL)
    kill = KillSwitch(EventBus(), safety)
    kill.register_cancel_all(cancel_all, verify)
    kill.trigger("p8_failure_injection")
    assert await kill.wait_until_halted() is True
    assert safety.mode == SafetyMode.HALTED
    assert calls == [("bid_ack", "ask_partial", "bid_submit_unknown", "ask_cancel_unknown")]


def test_late_revision_orphan_and_unknown_sla_produce_cancel_only_facts() -> None:
    slot = QuoteSlotKey(STRATEGY, BTC, Side.BUY)
    owner = QuoteRiskOwner(
        OrderId("1"),
        Cloid("late"),
        Price("99"),
        Size("1"),
        OrderStatus.SUBMIT_UNKNOWN,
        6,
        NOW - timedelta(seconds=20),
    )
    registry = RecoveryRegistry(
        (RecoveryOwner(slot, owner, RecoveryReason.SUBMIT_UNKNOWN, NOW - timedelta(seconds=20)),)
    )
    assert registry.placement_blocked(slot)
    assert registry.oldest_unresolved_age(now=NOW) == timedelta(seconds=20)
    assert registry.sla_exceeded(now=NOW, sla=timedelta(seconds=15))

    envelope = CanaryRiskEnvelope(
        1,
        Usd("100"),
        Usd("10"),
        Usd("5"),
        Usd("10"),
        100,
        1000,
        100,
        10,
        1,
        Usd("1"),
        timedelta(seconds=15),
        timedelta(days=1),
        Usd("1000"),
    )
    observation = CanaryObservation(
        NOW,
        NOW - timedelta(hours=1),
        Usd("50"),
        Usd("5"),
        Usd("0"),
        Usd("0"),
        1,
        1,
        1000,
        1000,
        0,
        Usd("0"),
        registry.oldest_unresolved_age(now=NOW),
        Usd("1"),
        True,
        True,
        True,
    )
    decision = CanaryGateEvaluator().evaluate_live(envelope, observation)
    assert decision.directive == CanaryDirective.CANCEL_ONLY
    assert "unknown_sla_exceeded" in decision.reasons


def test_postgres_dispatch_failure_blocks_place_but_never_cancel() -> None:
    context = DispatchGuardContext(
        NOW,
        NOW + timedelta(seconds=1),
        "session",
        "session",
        1,
        1,
        7,
        7,
        1,
        1,
        True,
        True,
        True,
        False,
        True,
        True,
        True,
        True,
        True,
    )
    assert evaluate_dispatch_guard(ChildActionType.PLACE, context) == GuardDecision.BLOCKED
    assert evaluate_dispatch_guard(ChildActionType.CANCEL, context) == GuardDecision.ALLOW


def test_external_soak_requirements_are_machine_readable_and_not_faked() -> None:
    path = Path(__file__).parents[1] / "fixtures" / "market_making_p8_validation.json"
    checklist = json.loads(path.read_text())
    assert checklist["schema_version"] == 1
    evidence = checklist["required_external_evidence"]
    assert {item["status"] for item in evidence} == {"evidence_required"}
    assert {item["id"] for item in evidence} >= {
        "mainnet_shadow_14_complete_utc_days",
        "testnet_clean_14_complete_utc_days",
        "testnet_exchange_recovery_drill",
    }
