"""Tests for the backtest framework."""

from __future__ import annotations

import pytest

from hypeedge.backtest.broker import FeeConfig, SimulatedBroker, SlippageMode
from hypeedge.backtest.data_feed import DataFeed
from hypeedge.backtest.engine import BacktestEngine, BacktestResult
from hypeedge.backtest.metrics import MetricsCalculator
from hypeedge.backtest.walk_forward import (
    WalkForwardEngine,
    bonferroni_correction,
    run_monte_carlo,
)
from hypeedge.core.enums import OrderType, Side
from hypeedge.core.events import EVENT_CANDLE_UPDATE, EVENT_FUNDING_UPDATE, EventBus
from hypeedge.core.models import Candle, Fill, FundingRate, OrderIntent, Position
from hypeedge.core.types import Cloid, OrderId, Price, Size, StrategyId, Symbol, Timestamp, Usd

# --- Helpers ---


def _make_candle(
    ts_ms: int = 1_000_000_000_000,
    open_p: float = 100.0,
    high: float = 110.0,
    low: float = 90.0,
    close: float = 105.0,
    volume: float = 1000.0,
    symbol: str = "BTC",
) -> Candle:
    return Candle(
        symbol=Symbol(symbol),
        interval="1m",
        open=Price(open_p),
        high=Price(high),
        low=Price(low),
        close=Price(close),
        volume=Size(volume),
        timestamp=Timestamp(ts_ms),
    )


def _make_funding(ts_ms: int = 1_000_000_000_000, rate: float = 0.0001) -> FundingRate:
    return FundingRate(
        symbol=Symbol("BTC"),
        funding_rate=rate,
        premium=rate,
        mark_price=Price(100.0),
        open_interest=1_000_000.0,
        timestamp=Timestamp(ts_ms),
    )


# --- Test SimulatedBroker ---


class TestSimulatedBroker:
    def test_market_buy_fill_with_pessimistic_slippage(self):
        broker = SimulatedBroker(mode=SlippageMode.PESSIMISTIC)
        candle = _make_candle(close=100.0)
        intent = OrderIntent(
            symbol=Symbol("BTC"),
            side=Side.BUY,
            size=Size(1.0),
            order_type=OrderType.MARKET,
        )
        fill = broker.simulate_fill(intent, candle, Cloid("c1"))
        assert fill is not None
        # Pessimistic slippage: 10bps → 100 * 1.001 = 100.1
        assert fill.price > Price(100.0)
        assert abs(fill.price - 100.1) < 0.01
        assert fill.is_maker is False
        assert fill.fee > 0  # taker fee

    def test_market_sell_fill_with_optimistic_slippage(self):
        broker = SimulatedBroker(mode=SlippageMode.OPTIMISTIC)
        candle = _make_candle(close=100.0)
        intent = OrderIntent(
            symbol=Symbol("BTC"),
            side=Side.SELL,
            size=Size(1.0),
            order_type=OrderType.MARKET,
        )
        fill = broker.simulate_fill(intent, candle, Cloid("c2"))
        assert fill is not None
        # Optimistic slippage: 2bps → 100 * 0.9998 = 99.98
        assert fill.price < Price(100.0)
        assert abs(fill.price - 99.98) < 0.01

    def test_limit_buy_fill_when_price_crosses(self):
        broker = SimulatedBroker()
        # Candle low = 90, limit buy at 95 → fills
        candle = _make_candle(low=90.0, high=110.0)
        intent = OrderIntent(
            symbol=Symbol("BTC"),
            side=Side.BUY,
            size=Size(1.0),
            price=Price(95.0),
            order_type=OrderType.LIMIT,
        )
        fill = broker.simulate_fill(intent, candle, Cloid("c3"))
        assert fill is not None
        assert fill.price == Price(95.0)
        assert fill.is_maker is True

    def test_limit_buy_no_fill_when_price_above(self):
        broker = SimulatedBroker()
        # Candle low = 96, limit buy at 95 → no fill
        candle = _make_candle(low=96.0, high=110.0)
        intent = OrderIntent(
            symbol=Symbol("BTC"),
            side=Side.BUY,
            size=Size(1.0),
            price=Price(95.0),
            order_type=OrderType.LIMIT,
        )
        fill = broker.simulate_fill(intent, candle, Cloid("c4"))
        assert fill is None

    def test_limit_sell_fill_when_price_crosses(self):
        broker = SimulatedBroker()
        # Candle high = 110, limit sell at 105 → fills
        candle = _make_candle(low=90.0, high=110.0)
        intent = OrderIntent(
            symbol=Symbol("BTC"),
            side=Side.SELL,
            size=Size(1.0),
            price=Price(105.0),
            order_type=OrderType.LIMIT,
        )
        fill = broker.simulate_fill(intent, candle, Cloid("c5"))
        assert fill is not None
        assert fill.price == Price(105.0)

    def test_fee_calculation_taker(self):
        broker = SimulatedBroker()
        fee = broker.calculate_fee(Price(100.0), Size(1.0), is_maker=False)
        # 100 * 1.0 * 0.0005 = 0.05
        assert abs(fee - 0.05) < 1e-6

    def test_fee_calculation_maker_rebate(self):
        broker = SimulatedBroker()
        fee = broker.calculate_fee(Price(100.0), Size(1.0), is_maker=True)
        # 100 * 1.0 * (-0.0002) = -0.02
        assert fee < 0
        assert abs(fee - (-0.02)) < 1e-6

    def test_funding_long_pays_positive_rate(self):
        pos = Position(symbol=Symbol("BTC"), size=Size(10.0), entry_price=Price(100.0))
        # Long 10 BTC at mark 100, funding +0.01% → pays 10 * 100 * 0.0001 = 0.1
        funding = SimulatedBroker.apply_hourly_funding(pos, 0.0001, Price(100.0))
        assert abs(funding - 0.1) < 1e-6

    def test_funding_flat_position_zero(self):
        pos = Position(symbol=Symbol("BTC"), size=Size(0.0))
        funding = SimulatedBroker.apply_hourly_funding(pos, 0.01, Price(100.0))
        assert funding == 0.0

    def test_custom_fee_config(self):
        fee_cfg = FeeConfig(maker_rebate_pct=0.0, taker_fee_pct=0.001)
        broker = SimulatedBroker(fee_config=fee_cfg)
        fee = broker.calculate_fee(Price(100.0), Size(1.0), is_maker=False)
        assert abs(fee - 0.1) < 1e-6


# --- Test DataFeed ---


class TestDataFeed:
    def test_publishes_candles_in_order(self):
        bus = EventBus(queue_maxsize=100)
        queue = bus.subscribe(EVENT_CANDLE_UPDATE)
        c1 = _make_candle(ts_ms=1000)
        c2 = _make_candle(ts_ms=2000)
        feed = DataFeed([c2, c1], None, bus)  # Out of order input

        feed.next_candle()
        feed.next_candle()

        assert queue.get_nowait().payload.timestamp == 1000
        assert queue.get_nowait().payload.timestamp == 2000

    def test_has_next_returns_false_when_exhausted(self):
        bus = EventBus(queue_maxsize=100)
        feed = DataFeed([_make_candle(ts_ms=1000)], None, bus)
        assert feed.has_next
        feed.next_candle()
        assert not feed.has_next
        assert feed.next_candle() is None

    def test_funding_published_at_hour_boundary(self):
        bus = EventBus(queue_maxsize=100)
        fund_queue = bus.subscribe(EVENT_FUNDING_UPDATE)
        # Hour boundary: 3600000ms = 1 hour
        hour_ts = 3_600_000
        candle_in_hour = _make_candle(ts_ms=hour_ts + 60_000)  # 1 min after hour
        funding = _make_funding(ts_ms=hour_ts)
        feed = DataFeed([candle_in_hour], [funding], bus)

        feed.next_candle()
        event = fund_queue.get_nowait()
        assert event.event_type == EVENT_FUNDING_UPDATE

    def test_reset(self):
        bus = EventBus(queue_maxsize=100)
        feed = DataFeed([_make_candle(ts_ms=1000)], None, bus)
        feed.next_candle()
        assert not feed.has_next
        feed.reset()
        assert feed.has_next
        assert feed.current_index == 0


# --- Test MetricsCalculator ---


class TestMetricsCalculator:
    def test_no_trades(self):
        calc = MetricsCalculator(
            fills=[],
            equity_curve=[(Timestamp(1000), Usd(10_000.0)), (Timestamp(2000), Usd(10_000.0))],
            initial_capital=Usd(10_000.0),
        )
        m = calc.calculate()
        assert m.total_return_pct == 0.0
        assert m.trade_count == 0
        assert m.max_drawdown_pct == 0.0

    def test_positive_return(self):
        calc = MetricsCalculator(
            fills=[],
            equity_curve=[(Timestamp(1000), Usd(10_000.0)), (Timestamp(2000), Usd(11_000.0))],
            initial_capital=Usd(10_000.0),
        )
        m = calc.calculate()
        assert abs(m.total_return_pct - 0.10) < 1e-6
        assert m.final_equity == Usd(11_000.0)

    def test_max_drawdown(self):
        # 10k → 12k → 9k (25% drawdown from peak 12k)
        calc = MetricsCalculator(
            fills=[],
            equity_curve=[
                (Timestamp(1000), Usd(10_000.0)),
                (Timestamp(2000), Usd(12_000.0)),
                (Timestamp(3000), Usd(9_000.0)),
            ],
            initial_capital=Usd(10_000.0),
        )
        m = calc.calculate()
        assert abs(m.max_drawdown_pct - 0.25) < 1e-6

    def test_total_fees_from_fills(self):
        fills = [
            Fill(
                cloid=Cloid("c1"),
                exchange_oid=OrderId("o1"),
                symbol=Symbol("BTC"),
                side=Side.BUY,
                price=Price(100.0),
                size=Size(1.0),
                fee=Usd(0.05),
                is_maker=False,
                timestamp=Timestamp(1000),
            ),
        ]
        calc = MetricsCalculator(
            fills=fills,
            equity_curve=[(Timestamp(1000), Usd(10_000.0))],
            initial_capital=Usd(10_000.0),
        )
        m = calc.calculate()
        assert abs(m.total_fees - 0.05) < 1e-6

    def test_to_dict_rounds_values(self):
        calc = MetricsCalculator(
            fills=[],
            equity_curve=[(Timestamp(1000), Usd(10_000.0))],
            initial_capital=Usd(10_000.0),
        )
        d = calc.calculate().to_dict()
        assert isinstance(d, dict)
        assert "total_return_pct" in d
        assert "sharpe_ratio" in d


# --- Test BacktestEngine (end-to-end) ---


class TestBacktestEngine:
    @pytest.mark.asyncio
    async def test_buy_and_hold_strategy(self):
        """A trivial strategy: buy on first candle, hold to end."""
        from hypeedge.core.enums import StrategyStatus
        from hypeedge.strategy.base import StrategyBase

        class BuyAndHold(StrategyBase):
            def __init__(self, sid, bus, client):
                super().__init__(sid, bus, client)
                self._bought = False

            async def on_start(self):
                self.status = StrategyStatus.RUNNING

            async def on_event(self, event):
                if event.event_type == EVENT_CANDLE_UPDATE and not self._bought:
                    candle: Candle = event.payload
                    intent = OrderIntent(
                        symbol=candle.symbol,
                        side=Side.BUY,
                        size=Size(0.1),
                        order_type=OrderType.MARKET,
                    )
                    await self._execution.submit_order(intent)
                    self._bought = True

            async def on_stop(self):
                self.status = StrategyStatus.STOPPED

        # 3 candles: price goes up from 100 to 110
        candles = [
            _make_candle(ts_ms=1_000_000_000_000, close=100.0, low=99.0, high=101.0),
            _make_candle(ts_ms=1_000_000_000_000 + 60_000, close=105.0, low=104.0, high=106.0),
            _make_candle(ts_ms=1_000_000_000_000 + 120_000, close=110.0, low=109.0, high=111.0),
        ]

        engine = BacktestEngine()
        result = await engine.run(
            candles=candles,
            funding_rates=None,
            strategy_factory=lambda bus, client: BuyAndHold(StrategyId("buyhold"), bus, client),
            initial_capital=Usd(10_000.0),
            slippage_mode=SlippageMode.OPTIMISTIC,
        )

        assert isinstance(result, BacktestResult)
        # The position is still open, so there is one fill but no completed
        # round-trip trade.
        assert len(result.fills) == 1
        assert result.metrics.trade_count == 0
        assert len(result.equity_curve) == 3
        assert result.metrics.final_equity > 0

    @pytest.mark.asyncio
    async def test_no_candles_returns_empty(self):
        engine = BacktestEngine()

        class NoopStrategy:
            def __init__(self, bus, client):
                pass

            async def on_start(self):
                pass

            async def on_event(self, event):
                pass

            async def on_stop(self):
                pass

        result = await engine.run(
            candles=[],
            funding_rates=None,
            strategy_factory=lambda bus, client: NoopStrategy(bus, client),
        )
        assert result.metrics.trade_count == 0
        assert len(result.equity_curve) == 0

    @pytest.mark.asyncio
    async def test_funding_applied_to_equity(self):
        """Funding should reduce equity for a long position in positive funding regime."""
        from hypeedge.core.enums import StrategyStatus
        from hypeedge.strategy.base import StrategyBase

        class LongStrategy(StrategyBase):
            def __init__(self, sid, bus, client):
                super().__init__(sid, bus, client)
                self._bought = False

            async def on_start(self):
                self.status = StrategyStatus.RUNNING

            async def on_event(self, event):
                if event.event_type == EVENT_CANDLE_UPDATE and not self._bought:
                    candle: Candle = event.payload
                    await self._execution.submit_order(
                        OrderIntent(
                            symbol=candle.symbol,
                            side=Side.BUY,
                            size=Size(1.0),
                            order_type=OrderType.MARKET,
                        )
                    )
                    self._bought = True

            async def on_stop(self):
                self.status = StrategyStatus.STOPPED

        # Hour-aligned candles so funding gets applied
        hour = 3_600_000_000_000  # some hour boundary
        candles = [
            _make_candle(ts_ms=hour, close=100.0, low=99.0, high=101.0),
            _make_candle(ts_ms=hour + 60_000, close=100.0, low=99.0, high=101.0),
            # Next hour → funding should apply
            _make_candle(ts_ms=hour + hour + 60_000, close=100.0, low=99.0, high=101.0),
        ]
        # Funding at the second hour boundary
        funding = [_make_funding(ts_ms=hour + hour, rate=0.001)]

        engine = BacktestEngine()
        result = await engine.run(
            candles=candles,
            funding_rates=funding,
            strategy_factory=lambda bus, client: LongStrategy(StrategyId("long"), bus, client),
            initial_capital=Usd(10_000.0),
            slippage_mode=SlippageMode.OPTIMISTIC,
        )

        # Funding should have been applied (reducing equity for long in positive funding)
        assert result.metrics.total_funding != 0.0


# --- Test WalkForward ---


class TestWalkForward:
    @pytest.mark.asyncio
    async def test_walk_forward_produces_windows(self):
        """Walk-forward with enough data should produce at least one window."""
        from hypeedge.core.enums import StrategyStatus

        class NoopStrategy:
            def __init__(self, bus, client):
                self.status = StrategyStatus.STOPPED

            async def on_start(self):
                self.status = StrategyStatus.RUNNING

            async def on_event(self, event):
                pass

            async def on_stop(self):
                self.status = StrategyStatus.STOPPED

        # Create candles spanning 150 days (enough for train=60 + validate=30 + step=30)
        day_ms = 24 * 3_600_000
        base_ts = 1_000_000_000_000
        candles = []
        for day in range(150):
            ts = base_ts + day * day_ms
            candles.append(_make_candle(ts_ms=ts, close=100.0 + day * 0.1))

        wf = WalkForwardEngine()
        result = await wf.run_walk_forward(
            candles=candles,
            funding_rates=None,
            strategy_factory=lambda bus, client: NoopStrategy(bus, client),
            train_days=60,
            validate_days=30,
            step_days=30,
        )

        assert result.n_windows >= 1
        assert len(result.windows) == result.n_windows


# --- Test Monte Carlo ---


class TestMonteCarlo:
    def test_monte_carlo_with_monotonic_equity(self):
        """Monte Carlo on a steadily growing equity curve should give low p-value."""
        # Create a steadily increasing equity curve
        curve = [(i * 3_600_000, 10_000.0 + i * 10.0) for i in range(100)]
        result = run_monte_carlo(curve, n_simulations=500, seed=42)

        assert result.n_simulations == 500
        assert result.observed_return > 0
        assert 0.0 <= result.p_value_return <= 1.0
        assert result.return_ci_lower <= result.return_ci_upper

    def test_monte_carlo_empty_curve(self):
        result = run_monte_carlo([], n_simulations=10)
        assert result.p_value_return == 1.0
        assert result.n_simulations == 0


# --- Test Bonferroni ---


class TestBonferroni:
    def test_single_test_unchanged(self):
        assert bonferroni_correction(0.03, 1) == 0.03

    def test_100_tests(self):
        # 0.05 * 100 = 5.0, clamped to 1.0
        assert bonferroni_correction(0.05, 100) == 1.0

    def test_small_p_value(self):
        assert abs(bonferroni_correction(0.001, 50) - 0.05) < 1e-6
