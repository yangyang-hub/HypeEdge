"""Tests for the risk checker — fail-safe behavior."""

from __future__ import annotations

import asyncio

import pytest

from hypeedge.account.tracker import AccountTracker
from hypeedge.core.enums import Side
from hypeedge.core.models import AccountState, OrderIntent, RiskCheckResult
from hypeedge.core.types import Price, Size, Symbol, Usd
from hypeedge.risk.checker import RiskChecker, RiskLimits


def _make_account_state(equity: float = 10_000.0, peak: float = 10_000.0) -> AccountState:
    return AccountState(
        equity=Usd(equity),
        available_balance=Usd(equity * 0.8),
        total_margin_used=Usd(equity * 0.2),
        total_unrealized_pnl=Usd(0.0),
        peak_equity=Usd(peak),
    )


class TestRiskCheckerFailSafe:
    @pytest.mark.asyncio
    async def test_rejects_when_no_account_state(self):
        """Risk checker must reject when account state is not available (fail-closed)."""
        tracker = AccountTracker()
        checker = RiskChecker(tracker)
        intent = OrderIntent(symbol=Symbol("BTC"), side=Side.BUY, size=Size(1.0), price=Price(50000.0))

        result = await checker.check(intent)
        assert result.passed is False
        assert "account_state_not_available" in result.reason

    @pytest.mark.asyncio
    async def test_fail_safe_on_timeout(self):
        """Risk check must reject on timeout (fail-safe)."""
        tracker = AccountTracker()
        tracker.update_account_state(_make_account_state())

        # Use extremely short timeout
        checker = RiskChecker(tracker, RiskLimits(timeout_ms=1))

        # Override _run_checks to simulate slow check
        async def slow_check(intent: OrderIntent) -> RiskCheckResult:
            await asyncio.sleep(10.0)
            return RiskCheckResult(passed=True)

        checker._run_checks = slow_check  # type: ignore[assignment]

        intent = OrderIntent(
            symbol=Symbol("BTC"),
            side=Side.BUY,
            size=Size(0.01),
            price=Price(100.0),
        )
        result = await checker.check(intent)
        assert result.passed is False
        assert "timeout" in result.reason

    @pytest.mark.asyncio
    async def test_fail_safe_on_exception(self):
        """Risk check must reject on internal error."""
        tracker = AccountTracker()
        tracker.update_account_state(_make_account_state())
        checker = RiskChecker(tracker)

        async def broken_check(intent: OrderIntent) -> RiskCheckResult:
            raise RuntimeError("database connection failed")

        checker._run_checks = broken_check  # type: ignore[assignment]

        intent = OrderIntent(
            symbol=Symbol("BTC"),
            side=Side.BUY,
            size=Size(0.01),
            price=Price(100.0),
        )
        result = await checker.check(intent)
        assert result.passed is False
        assert "error" in result.reason
