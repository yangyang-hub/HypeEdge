"""Safety tests for the live quote-plan worker boundary."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from hypeedge.core.enums import Side
from hypeedge.core.types import Price, Size, StrategyId, Symbol
from hypeedge.execution.batch import ChildActionType, DispatchGuardContext
from hypeedge.execution.quote_plan_worker import QuoteDispatchChild, QuotePlanWorker

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _child(action: ChildActionType) -> QuoteDispatchChild:
    return QuoteDispatchChild(
        item_id=1,
        command_id=str(uuid.uuid4()),
        action=action,
        attempt=1,
        plan_id=str(uuid.uuid4()),
        strategy_id=StrategyId("mm-btc"),
        symbol=Symbol("BTC"),
        runtime_session_id="s1",
        config_version=2,
        plan_revision=4,
        market_version=10,
        connection_generation=3,
        valid_until=NOW + timedelta(seconds=1),
        source_cloid="0x" + "1" * 32,
        target_cloid="0x" + "2" * 32,
        side=Side.BUY,
        level=0,
        price=Price("100"),
        size=Size("0.01"),
        sub_account=None,
    )


def _context(**overrides: object) -> DispatchGuardContext:
    values: dict[str, object] = {
        "now": NOW,
        "deadline": NOW + timedelta(seconds=1),
        "expected_session_id": "s1",
        "active_session_id": "s1",
        "expected_config_version": 2,
        "active_config_version": 2,
        "expected_plan_revision": 4,
        "active_plan_revision": 4,
        "expected_connection_generation": 3,
        "active_connection_generation": 3,
        "market_fresh": True,
        "account_fresh": True,
        "user_stream_fresh": True,
        "postgres_fresh": True,
        "safety_allows_place": True,
        "lifecycle_allows_place": True,
        "budget_allows_place": True,
        "reservation_valid": True,
        "alo_valid": True,
    }
    values.update(overrides)
    return DispatchGuardContext(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "failed_gate",
    ["market_fresh", "account_fresh", "safety_allows_place", "budget_allows_place", "alo_valid"],
)
async def test_failed_placement_guard_never_calls_network(failed_gate: str) -> None:
    executor = MagicMock(submit_order=AsyncMock(), cancel_order=AsyncMock())
    guards = MagicMock(context=AsyncMock(return_value=_context(**{failed_gate: False})))
    worker = QuotePlanWorker(MagicMock(), executor, guards, MagicMock())
    worker._finish_without_send = AsyncMock()  # type: ignore[method-assign]  # noqa: SLF001

    await worker.dispatch(_child(ChildActionType.PLACE))

    executor.submit_order.assert_not_awaited()
    worker._finish_without_send.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001


async def test_cancel_bypasses_placement_guard_and_dispatches() -> None:
    executor = MagicMock(submit_order=AsyncMock(), cancel_order=AsyncMock(return_value=True))
    guards = MagicMock(context=AsyncMock(side_effect=AssertionError("cancel must not call placement guard")))
    budget = MagicMock()
    worker = QuotePlanWorker(MagicMock(), executor, guards, budget, clock=lambda: NOW)
    worker._record_attempt = AsyncMock(return_value=True)  # type: ignore[method-assign]  # noqa: SLF001

    await worker.dispatch(_child(ChildActionType.CANCEL))

    executor.cancel_order.assert_awaited_once()
    guards.context.assert_not_awaited()
    budget.debit_network_attempt.assert_called_once()
