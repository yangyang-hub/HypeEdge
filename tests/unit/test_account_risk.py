"""Tests for account tracker, risk checker, and reconciler."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from hypeedge.account.reconciler import Reconciler
from hypeedge.account.tracker import AccountTracker
from hypeedge.core.enums import Side
from hypeedge.core.events import EventBus
from hypeedge.core.models import AccountState, Fill, OrderIntent
from hypeedge.core.types import Cloid, OrderId, Price, Size, Symbol, Timestamp, Usd
from hypeedge.risk.checker import RiskChecker, RiskLimits

# --- Helpers ---


def _make_fill(
    side: Side = Side.BUY,
    size: float = 1.0,
    price: float = 100.0,
    fee: float = 0.05,
    symbol: str = "BTC",
) -> Fill:
    return Fill(
        cloid=Cloid("test_cloid"),
        exchange_oid=OrderId("test_oid"),
        symbol=Symbol(symbol),
        side=side,
        price=Price(price),
        size=Size(size),
        fee=Usd(fee),
        is_maker=False,
        timestamp=Timestamp(1000),
    )


def _make_account_state(
    equity: float = 10_000.0,
    peak: float = 10_000.0,
) -> AccountState:
    return AccountState(
        equity=Usd(equity),
        available_balance=Usd(equity * 0.8),
        total_margin_used=Usd(equity * 0.2),
        total_unrealized_pnl=Usd(0.0),
        peak_equity=Usd(peak),
    )


# --- Test AccountTracker ---


class TestAccountTracker:
    def test_update_fill_creates_position(self):
        tracker = AccountTracker()
        fill = _make_fill(side=Side.BUY, size=1.0, price=100.0)

        tracker.update_fill(fill)

        pos = tracker.get_position(Symbol("BTC"))
        assert pos is not None
        assert pos.size == Size(1.0)
        assert pos.entry_price == Price(100.0)

    def test_update_fill_sell_creates_short(self):
        tracker = AccountTracker()
        fill = _make_fill(side=Side.SELL, size=1.0, price=100.0)

        tracker.update_fill(fill)

        pos = tracker.get_position(Symbol("BTC"))
        assert pos is not None
        assert pos.size == Size(-1.0)

    def test_update_fill_adds_to_position_vwap(self):
        tracker = AccountTracker()
        tracker.update_fill(_make_fill(side=Side.BUY, size=1.0, price=100.0))
        tracker.update_fill(_make_fill(side=Side.BUY, size=1.0, price=110.0))

        pos = tracker.get_position(Symbol("BTC"))
        assert pos is not None
        assert pos.size == Size(2.0)
        # VWAP: (1*100 + 1*110) / 2 = 105
        assert pos.entry_price is not None
        assert abs(pos.entry_price - 105.0) < 0.01

    def test_update_fill_closes_position(self):
        tracker = AccountTracker()
        tracker.update_fill(_make_fill(side=Side.BUY, size=1.0, price=100.0))
        tracker.update_fill(_make_fill(side=Side.SELL, size=1.0, price=105.0))

        pos = tracker.get_position(Symbol("BTC"))
        assert pos is None

    def test_partial_reduction_keeps_entry_price(self):
        tracker = AccountTracker()
        tracker.update_fill(_make_fill(side=Side.BUY, size=10.0, price=100.0))
        tracker.update_fill(_make_fill(side=Side.SELL, size=5.0, price=110.0))

        pos = tracker.get_position(Symbol("BTC"))
        assert pos is not None
        assert pos.size == Size(5.0)
        assert pos.entry_price == Price(100.0)

    def test_peak_equity_tracking(self):
        tracker = AccountTracker()
        tracker.update_account_state(_make_account_state(equity=10_000))
        assert tracker.peak_equity == Usd(10_000.0)

        tracker.update_account_state(_make_account_state(equity=12_000))
        assert tracker.peak_equity == Usd(12_000.0)

        tracker.update_account_state(_make_account_state(equity=11_000))
        assert tracker.peak_equity == Usd(12_000.0)  # peak doesn't decrease

    def test_drawdown_calculation(self):
        tracker = AccountTracker()
        tracker.update_account_state(_make_account_state(equity=10_000, peak=10_000))
        assert tracker.drawdown_pct == 0.0

        tracker.update_account_state(_make_account_state(equity=9_000, peak=10_000))
        assert abs(tracker.drawdown_pct - 0.10) < 1e-6

    def test_fees_tracking(self):
        tracker = AccountTracker()
        tracker.update_fill(_make_fill(fee=0.05))
        tracker.update_fill(_make_fill(fee=0.10))

        assert abs(tracker.total_fees - 0.15) < 1e-6
        assert tracker.fill_count == 2

    def test_leverage_calculation(self):
        tracker = AccountTracker()
        tracker.update_account_state(_make_account_state(equity=10_000))
        tracker.update_fill(_make_fill(side=Side.BUY, size=1.0, price=50_000.0))

        # Position value = 1.0 * 50000 = 50000, equity = 10000 → leverage = 5.0
        assert abs(tracker.get_leverage() - 5.0) < 0.01

    def test_get_status_dict(self):
        tracker = AccountTracker()
        tracker.update_account_state(_make_account_state())
        status = tracker.get_status()

        assert "equity" in status
        assert "positions" in status
        assert "leverage" in status


# --- Test RiskChecker ---


class TestRiskChecker:
    @pytest.mark.asyncio
    async def test_check_passes_normal_order(self):
        tracker = AccountTracker()
        tracker.update_account_state(_make_account_state(equity=10_000))

        checker = RiskChecker(tracker)
        intent = OrderIntent(
            symbol=Symbol("BTC"),
            side=Side.BUY,
            size=Size(0.1),
            price=Price(100.0),
        )

        result = await checker.check(intent)
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_check_rejects_no_account_state(self):
        tracker = AccountTracker()  # No account state
        checker = RiskChecker(tracker)
        intent = OrderIntent(symbol=Symbol("BTC"), side=Side.BUY, size=Size(1.0))

        result = await checker.check(intent)
        assert result.passed is False
        assert "account_state_not_available" in result.reason

    @pytest.mark.asyncio
    async def test_check_rejects_drawdown_exceeded(self):
        tracker = AccountTracker()
        tracker.update_account_state(_make_account_state(equity=8_500, peak=10_000))
        # drawdown = 15%, limit = 10%

        checker = RiskChecker(tracker, RiskLimits(max_drawdown_pct=0.10))
        intent = OrderIntent(symbol=Symbol("BTC"), side=Side.BUY, size=Size(0.1), price=Price(100.0))

        result = await checker.check(intent)
        assert result.passed is False
        assert "drawdown_exceeded" in result.reason

    @pytest.mark.asyncio
    async def test_check_rejects_position_too_large(self):
        tracker = AccountTracker()
        tracker.update_account_state(_make_account_state(equity=10_000))

        checker = RiskChecker(tracker, RiskLimits(max_position_pct=0.10))
        # Order value = 1.0 * 50000 = 50000 → 500% of equity
        intent = OrderIntent(
            symbol=Symbol("BTC"),
            side=Side.BUY,
            size=Size(1.0),
            price=Price(50_000.0),
        )

        result = await checker.check(intent)
        assert result.passed is False
        assert "position_pct_exceeded" in result.reason

    @pytest.mark.asyncio
    async def test_check_rejects_leverage_exceeded(self):
        tracker = AccountTracker()
        tracker.update_account_state(_make_account_state(equity=10_000))

        checker = RiskChecker(tracker, RiskLimits(max_leverage=3))
        # New order: 0.5 * 50000 = 25000, leverage = 2.5 (passes)
        # But if we already have a position...
        tracker.update_fill(_make_fill(side=Side.BUY, size=0.2, price=50_000.0))
        # Existing: 0.2 * 50000 = 10000, new: 0.5 * 50000 = 25000, total = 35000
        # Leverage = 35000 / 10000 = 3.5 > 3
        intent = OrderIntent(
            symbol=Symbol("BTC"),
            side=Side.BUY,
            size=Size(0.5),
            price=Price(50_000.0),
        )

        result = await checker.check(intent)
        assert result.passed is False
        assert "leverage_exceeded" in result.reason

    @pytest.mark.asyncio
    async def test_check_stats_tracking(self):
        tracker = AccountTracker()
        tracker.update_account_state(_make_account_state(equity=10_000))

        checker = RiskChecker(tracker)
        intent = OrderIntent(symbol=Symbol("BTC"), side=Side.BUY, size=Size(0.01), price=Price(100.0))

        await checker.check(intent)
        await checker.check(intent)

        assert checker.stats["check_count"] == 2
        assert checker.stats["pass_count"] == 2


# --- Test Reconciler ---


class TestReconciler:
    @pytest.mark.asyncio
    async def test_reconcile_with_no_client_returns_failure(self):
        bus = EventBus(queue_maxsize=100)
        tracker = AccountTracker()
        mock_engine = AsyncMock()
        mock_engine.get_open_orders = AsyncMock(return_value=[])

        reconciler = Reconciler(bus, tracker, mock_engine)

        result = await reconciler.reconcile()
        assert result.success is False

    @pytest.mark.asyncio
    async def test_exchange_query_failure_is_fail_closed_without_mutation(self):
        bus = EventBus(queue_maxsize=100)
        tracker = AccountTracker()
        tracker.update_fill(_make_fill(side=Side.BUY, symbol="ETH"))
        mock_engine = MagicMock()
        mock_engine.get_open_orders = AsyncMock(return_value=[])
        mock_engine.import_exchange_order_authoritative = AsyncMock()
        mock_info = MagicMock()
        mock_info.open_orders.side_effect = OSError("network down")
        mock_info.user_state.return_value = {"assetPositions": [], "marginSummary": {}}
        reconciler = Reconciler(bus, tracker, mock_engine, info_client=mock_info, account_address="0xabc")

        result = await reconciler.reconcile()
        assert result.success is False
        assert tracker.get_position(Symbol("ETH")) is not None

    @pytest.mark.asyncio
    async def test_imports_exchange_only_open_order(self):
        bus = EventBus(queue_maxsize=100)
        tracker = AccountTracker()
        mock_engine = MagicMock()
        mock_engine.get_open_orders = AsyncMock(return_value=[])
        mock_engine.import_exchange_order_authoritative = AsyncMock()
        mock_info = MagicMock()
        mock_info.open_orders.return_value = [
            {"cloid": "0x" + "1" * 32, "coin": "BTC", "side": "B", "sz": "0.1", "limitPx": "50000", "oid": 7}
        ]
        mock_info.user_state.return_value = {"assetPositions": [], "marginSummary": {}}
        reconciler = Reconciler(bus, tracker, mock_engine, info_client=mock_info, account_address="0xabc")

        result = await reconciler.reconcile()
        assert result.success is True
        mock_engine.import_exchange_order_authoritative.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_reconcile_corrects_missing_position(self):
        bus = EventBus(queue_maxsize=100)
        tracker = AccountTracker()
        mock_engine = AsyncMock()
        mock_engine.get_open_orders = AsyncMock(return_value=[])

        mock_info = MagicMock()
        mock_info.open_orders.return_value = []
        mock_info.user_state.return_value = {
            "assetPositions": [
                {
                    "position": {
                        "coin": "BTC",
                        "szi": "1.5",
                        "entryPx": "50000.0",
                        "leverage": {"value": "3", "type": "cross"},
                    }
                }
            ],
            "marginSummary": {
                "accountValue": "10000.0",
                "totalMarginAvailable": "8000.0",
                "totalMarginUsed": "2000.0",
                "totalNtlPos": "75000.0",
            },
        }

        reconciler = Reconciler(bus, tracker, mock_engine, info_client=mock_info, account_address="0xabc")

        result = await reconciler.reconcile()
        assert result.success is True
        assert result.positions_corrected >= 1

        pos = tracker.get_position(Symbol("BTC"))
        assert pos is not None
        assert abs(pos.size - 1.5) < 1e-6

    @pytest.mark.asyncio
    async def test_reconcile_removes_stale_position(self):
        bus = EventBus(queue_maxsize=100)
        tracker = AccountTracker()
        # Local has a position, but exchange doesn't
        tracker.update_fill(_make_fill(side=Side.BUY, size=1.0, price=100.0, symbol="ETH"))
        assert tracker.get_position(Symbol("ETH")) is not None

        mock_engine = AsyncMock()
        mock_engine.get_open_orders = AsyncMock(return_value=[])

        mock_info = MagicMock()
        mock_info.open_orders.return_value = []
        mock_info.user_state.return_value = {
            "assetPositions": [],
            "marginSummary": {
                "accountValue": "10000.0",
                "totalMarginAvailable": "10000.0",
                "totalMarginUsed": "0.0",
                "totalNtlPos": "0.0",
            },
        }

        reconciler = Reconciler(bus, tracker, mock_engine, info_client=mock_info, account_address="0xabc")

        result = await reconciler.reconcile()
        assert result.success is True
        assert result.positions_corrected >= 1
        assert tracker.get_position(Symbol("ETH")) is None
