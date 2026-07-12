"""Unified trading-command admission tests."""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest

from hypeedge.core.enums import SafetyMode, Side, TimeInForce
from hypeedge.core.exceptions import TradingCommandConflictError, TradingCommandPersistenceError
from hypeedge.core.models import OrderIntent, RiskCheckResult
from hypeedge.core.types import Price, Size, StrategyId, Symbol
from hypeedge.risk.safety import SafetyController
from hypeedge.trading.command_service import (
    DataHealthDecision,
    GateDecision,
    InMemoryTradingCommandSink,
    TradingCommandService,
)


def _intent(*, size: str = "0.002") -> OrderIntent:
    return OrderIntent(
        symbol=Symbol("BTC"),
        side=Side.BUY,
        size=Size(size),
        price=Price("100.19"),
        time_in_force=TimeInForce.ALO,
        strategy_id=StrategyId("maker-1"),
    )


class _DataHealth:
    def __init__(self, calls: list[str], *, allowed: bool = True) -> None:
        self._calls = calls
        self._allowed = allowed

    async def check_placement(self, intent: OrderIntent) -> DataHealthDecision:
        self._calls.append("data_health")
        return DataHealthDecision(
            self._allowed,
            None if self._allowed else "book_stale",
            reference_price=Price("100"),
            best_bid=Price("99.9"),
            best_ask=Price("100.5"),
            market_version=12,
            connection_generation=3,
        )


class _Risk:
    def __init__(self, calls: list[str], *, passed: bool = True) -> None:
        self._calls = calls
        self._passed = passed

    async def check(self, intent: OrderIntent, *, reference_price: float | None = None) -> RiskCheckResult:
        self._calls.append("risk")
        assert reference_price == 100.0
        return RiskCheckResult(self._passed, None if self._passed else "inventory_limit")


class _Budget:
    def __init__(self, calls: list[str], *, allowed: bool = True) -> None:
        self._calls = calls
        self._allowed = allowed

    async def check_placement(self, intent: OrderIntent) -> GateDecision:
        self._calls.append("action_budget")
        return GateDecision(self._allowed, None if self._allowed else "cancel_reserve_low")


class _Normalizer:
    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    def normalize(
        self,
        intent: OrderIntent,
        *,
        best_bid: Price | None = None,
        best_ask: Price | None = None,
    ) -> OrderIntent:
        self._calls.append("normalize")
        assert best_bid == Decimal("99.9")
        assert best_ask == Decimal("100.5")
        return OrderIntent(
            symbol=intent.symbol,
            side=intent.side,
            size=intent.size,
            price=Price("100.1"),
            time_in_force=intent.time_in_force,
            strategy_id=intent.strategy_id,
        )


class _Safety(SafetyController):
    def __init__(self, calls: list[str]) -> None:
        super().__init__(SafetyMode.NORMAL)
        self._calls = calls

    def check_placement(self, intent: OrderIntent) -> None:
        self._calls.append("safety")
        super().check_placement(intent)


def _service(
    calls: list[str],
    sink: InMemoryTradingCommandSink,
    *,
    data_allowed: bool = True,
    risk_passed: bool = True,
    budget_allowed: bool = True,
) -> TradingCommandService:
    return TradingCommandService(
        safety=_Safety(calls),
        data_health=_DataHealth(calls, allowed=data_allowed),
        risk=_Risk(calls, passed=risk_passed),
        action_budget=_Budget(calls, allowed=budget_allowed),
        normalizer=_Normalizer(calls),
        sink=sink,
    )


@pytest.mark.asyncio
async def test_placement_pipeline_order_and_normalized_durable_receipt() -> None:
    calls: list[str] = []
    sink = InMemoryTradingCommandSink()

    receipt = await _service(calls, sink).submit_order(_intent())

    assert receipt.accepted
    assert calls == ["safety", "data_health", "risk", "action_budget", "normalize"]
    assert receipt.intent is not None and receipt.intent.price == Decimal("100.1")
    assert sink.receipts == (receipt,)


@pytest.mark.asyncio
async def test_gate_exception_fails_closed_and_persists_rejection() -> None:
    calls: list[str] = []
    sink = InMemoryTradingCommandSink()
    service = _service(calls, sink, data_allowed=False)

    receipt = await service.submit_order(_intent())

    assert not receipt.accepted
    assert receipt.rejection_gate == "data_health"
    assert receipt.rejection_reason == "book_stale"
    assert calls == ["safety", "data_health"]


@pytest.mark.asyncio
async def test_cancel_bypasses_all_placement_gates_but_is_durable() -> None:
    calls: list[str] = []
    sink = InMemoryTradingCommandSink()

    receipt = await _service(calls, sink).cancel_order("0x123", strategy_id=StrategyId("maker-1"))

    assert receipt.accepted
    assert receipt.target_cloid == "0x123"
    assert calls == []


@pytest.mark.asyncio
async def test_sink_is_idempotent_and_rejects_command_id_payload_reuse() -> None:
    calls: list[str] = []
    sink = InMemoryTradingCommandSink()
    service = _service(calls, sink)
    command_id = uuid.uuid4()

    first = await service.submit_order(_intent(), command_id=command_id)
    replay = await service.submit_order(_intent(), command_id=command_id)

    assert replay is first
    assert len(sink.receipts) == 1
    with pytest.raises(TradingCommandConflictError):
        await service.submit_order(_intent(size="0.003"), command_id=command_id)


@pytest.mark.asyncio
async def test_sink_failure_never_returns_accepted_receipt() -> None:
    class _BrokenSink:
        async def persist(self, command: object) -> object:
            raise OSError("postgres unavailable")

    calls: list[str] = []
    service = TradingCommandService(
        safety=_Safety(calls),
        data_health=_DataHealth(calls),
        risk=_Risk(calls),
        action_budget=_Budget(calls),
        normalizer=_Normalizer(calls),
        sink=_BrokenSink(),  # type: ignore[arg-type]
    )

    with pytest.raises(TradingCommandPersistenceError):
        await service.submit_order(_intent())
