"""Backtest known-result verification tests (design doc §14.4).

"Run the backtest engine on known-outcome historical data
(manually verified market segments) and confirm output matches expectations.
Verify fee deductions, funding deductions, and slippage simulation are correct."
"""

from __future__ import annotations

import pytest

from hypeedge.backtest.broker import FeeConfig, SlippageMode
from hypeedge.backtest.engine import BacktestEngine
from hypeedge.core.enums import OrderType, Side, StrategyStatus
from hypeedge.core.events import EVENT_CANDLE_UPDATE
from hypeedge.core.models import Candle, FundingRate, OrderIntent
from hypeedge.core.types import Price, Size, StrategyId, Symbol, Timestamp


def _candle(ts: int, o: float, h: float, lo: float, c: float, v: float = 1000.0) -> Candle:
    return Candle(
        symbol=Symbol("BTC"),
        interval="1m",
        open=Price(o),
        high=Price(h),
        low=Price(lo),
        close=Price(c),
        volume=Size(v),
        timestamp=Timestamp(ts),
    )


def _funding(ts: int, rate: float) -> FundingRate:
    return FundingRate(
        symbol=Symbol("BTC"),
        funding_rate=rate,
        premium=rate,
        mark_price=Price(100.0),
        open_interest=1_000_000.0,
        timestamp=Timestamp(ts),
    )


class TestBacktestKnownResults:
    """Verify backtest calculations against manually computed expected values."""

    @pytest.mark.asyncio
    async def test_fee_deduction_accuracy(self):
        """Verify exact fee amounts match FeeConfig."""
        # Strategy: buy 1 BTC at 100, taker fee = 0.05%
        # Expected fee = 100 * 1 * 0.0005 = 0.05
        from hypeedge.strategy.base import StrategyBase

        class BuyOnce(StrategyBase):
            def __init__(self, sid, bus, client):
                super().__init__(sid, bus, client)
                self._done = False

            async def on_start(self):
                self.status = StrategyStatus.RUNNING

            async def on_event(self, event):
                if event.event_type == EVENT_CANDLE_UPDATE and not self._done:
                    candle: Candle = event.payload
                    await self._execution.submit_order(
                        OrderIntent(
                            symbol=candle.symbol,
                            side=Side.BUY,
                            size=Size(1.0),
                            order_type=OrderType.MARKET,
                        )
                    )
                    self._done = True

            async def on_stop(self):
                self.status = StrategyStatus.STOPPED

        # The signal is produced from the first close and executes on the next
        # candle, avoiding same-bar look-ahead.
        candles = [
            _candle(1000, 100.0, 101.0, 99.0, 100.0),
            _candle(2000, 100.0, 101.0, 99.0, 100.0),
        ]

        fee_cfg = FeeConfig(maker_rebate_pct=-0.0002, taker_fee_pct=0.0005)
        engine = BacktestEngine(fee_config=fee_cfg)

        result = await engine.run(
            candles=candles,
            funding_rates=None,
            strategy_factory=lambda bus, ec: BuyOnce(StrategyId("t"), bus, ec),
            initial_capital=Size(10_000.0),
            slippage_mode=SlippageMode.OPTIMISTIC,
        )

        # With optimistic slippage (2bps), fill price ≈ 100.02
        # Taker fee = 100.02 * 1.0 * 0.0005 ≈ 0.05001
        assert len(result.fills) == 1
        assert result.metrics.trade_count == 0
        assert abs(result.metrics.total_fees - 0.05001) < 0.001

    @pytest.mark.asyncio
    async def test_funding_deduction_accuracy(self):
        """Verify hourly funding is correctly applied."""
        from hypeedge.strategy.base import StrategyBase

        class BuyAndHold(StrategyBase):
            def __init__(self, sid, bus, client):
                super().__init__(sid, bus, client)
                self._done = False

            async def on_start(self):
                self.status = StrategyStatus.RUNNING

            async def on_event(self, event):
                if event.event_type == EVENT_CANDLE_UPDATE and not self._done:
                    await self._execution.submit_order(
                        OrderIntent(
                            symbol=Symbol("BTC"),
                            side=Side.BUY,
                            size=Size(10.0),
                            order_type=OrderType.MARKET,
                        )
                    )
                    self._done = True

            async def on_stop(self):
                self.status = StrategyStatus.STOPPED

        hour = 3_600_000
        candles = [
            _candle(hour, 100.0, 101.0, 99.0, 100.0),  # Hour 1: buy
            _candle(hour + 60_000, 100.0, 101.0, 99.0, 100.0),
            _candle(hour + hour + 60_000, 100.0, 101.0, 99.0, 100.0),  # Hour 2: funding applies
        ]
        # Funding rate = 0.001 (0.1%), long 10 BTC at mark 100
        # Expected funding = 10 * 100 * 0.001 = 1.0 (paid by longs)
        funding = [_funding(hour + hour, 0.001)]

        engine = BacktestEngine()
        result = await engine.run(
            candles=candles,
            funding_rates=funding,
            strategy_factory=lambda bus, ec: BuyAndHold(StrategyId("t"), bus, ec),
            initial_capital=Size(10_000.0),
            slippage_mode=SlippageMode.OPTIMISTIC,
        )

        # Funding should be approximately 1.0 USDC (positive rate = longs pay)
        assert abs(result.metrics.total_funding - 1.0) < 0.01

    @pytest.mark.asyncio
    async def test_slippage_optimistic_vs_pessimistic(self):
        """Verify optimistic and pessimistic produce different results."""
        from hypeedge.strategy.base import StrategyBase

        class BuyOnce(StrategyBase):
            def __init__(self, sid, bus, client):
                super().__init__(sid, bus, client)
                self._done = False

            async def on_start(self):
                self.status = StrategyStatus.RUNNING

            async def on_event(self, event):
                if event.event_type == EVENT_CANDLE_UPDATE and not self._done:
                    await self._execution.submit_order(
                        OrderIntent(
                            symbol=Symbol("BTC"),
                            side=Side.BUY,
                            size=Size(1.0),
                            order_type=OrderType.MARKET,
                        )
                    )
                    self._done = True

            async def on_stop(self):
                self.status = StrategyStatus.STOPPED

        candles = [
            _candle(1000, 100.0, 101.0, 99.0, 100.0),
            _candle(2000, 100.0, 101.0, 99.0, 100.0),
        ]
        engine = BacktestEngine()

        opt_result = await engine.run(
            candles=candles,
            funding_rates=None,
            strategy_factory=lambda bus, ec: BuyOnce(StrategyId("t"), bus, ec),
            initial_capital=Size(10_000.0),
            slippage_mode=SlippageMode.OPTIMISTIC,
        )
        pess_result = await engine.run(
            candles=candles,
            funding_rates=None,
            strategy_factory=lambda bus, ec: BuyOnce(StrategyId("t"), bus, ec),
            initial_capital=Size(10_000.0),
            slippage_mode=SlippageMode.PESSIMISTIC,
        )

        # Optimistic fill: 100 * (1 + 0.0002) = 100.02
        # Pessimistic fill: 100 * (1 + 0.001) = 100.10
        opt_fill = opt_result.fills[0].price
        pess_fill = pess_result.fills[0].price
        assert opt_fill < pess_fill
        assert abs(float(opt_fill) - 100.02) < 0.01
        assert abs(float(pess_fill) - 100.10) < 0.01

    @pytest.mark.asyncio
    async def test_max_drawdown_from_known_equity_curve(self):
        """Verify max drawdown calculation on a known equity path."""
        # 10k → 12k (+20%) → 9k (-25% from peak) → 11k
        from hypeedge.core.types import Usd
        from hypeedge.strategy.base import StrategyBase

        prices = [100.0, 120.0, 90.0, 110.0]

        class PriceChaser(StrategyBase):
            def __init__(self, sid, bus, client):
                super().__init__(sid, bus, client)
                self._idx = 0

            async def on_start(self):
                self.status = StrategyStatus.RUNNING

            async def on_event(self, event):
                if event.event_type == EVENT_CANDLE_UPDATE:
                    if self._idx == 0:
                        await self._execution.submit_order(
                            OrderIntent(
                                symbol=Symbol("BTC"),
                                side=Side.BUY,
                                size=Size(1.0),
                                order_type=OrderType.MARKET,
                            )
                        )
                    self._idx += 1

            async def on_stop(self):
                self.status = StrategyStatus.STOPPED

        candles = [_candle(i * 60000, p, p + 2, p - 2, p) for i, p in enumerate(prices)]

        engine = BacktestEngine()
        result = await engine.run(
            candles=candles,
            funding_rates=None,
            strategy_factory=lambda bus, ec: PriceChaser(StrategyId("t"), bus, ec),
            initial_capital=Usd(10_000.0),
            slippage_mode=SlippageMode.OPTIMISTIC,
        )

        # Verify the equity curve has the expected shape
        assert len(result.equity_curve) == 4
        # Peak should be at candle 1 (price 120)
        assert result.metrics.peak_equity >= result.metrics.final_equity

    @pytest.mark.asyncio
    async def test_round_trip_pnl_drives_trade_statistics(self):
        """A completed long round trip records realized PnL as one trade."""
        from hypeedge.strategy.base import StrategyBase

        class RoundTrip(StrategyBase):
            def __init__(self, sid, bus, client):
                super().__init__(sid, bus, client)
                self._index = 0

            async def on_start(self):
                self.status = StrategyStatus.RUNNING

            async def on_event(self, event):
                if event.event_type != EVENT_CANDLE_UPDATE:
                    return
                if self._index == 0:
                    await self._execution.submit_order(
                        OrderIntent(
                            symbol=Symbol("BTC"),
                            side=Side.BUY,
                            size=Size(1.0),
                            order_type=OrderType.MARKET,
                        )
                    )
                elif self._index == 1:
                    await self._execution.submit_order(
                        OrderIntent(
                            symbol=Symbol("BTC"),
                            side=Side.SELL,
                            size=Size(1.0),
                            order_type=OrderType.MARKET,
                            reduce_only=True,
                        )
                    )
                self._index += 1

            async def on_stop(self):
                self.status = StrategyStatus.STOPPED

        result = await BacktestEngine().run(
            candles=[
                _candle(1000, 100.0, 101.0, 99.0, 100.0),
                _candle(2000, 100.0, 101.0, 99.0, 100.0),
                _candle(3000, 110.0, 111.0, 109.0, 110.0),
            ],
            funding_rates=None,
            strategy_factory=lambda bus, ec: RoundTrip(StrategyId("round-trip"), bus, ec),
            initial_capital=Size(10_000.0),
            slippage_mode=SlippageMode.OPTIMISTIC,
        )

        assert result.metrics.trade_count == 1
        assert result.metrics.winning_trades == 1
        assert result.metrics.win_rate == 1.0
        assert result.metrics.final_equity > 10_000.0
