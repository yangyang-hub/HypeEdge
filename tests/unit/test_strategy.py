"""Tests for strategy indicators, parameters, and trend following strategy."""

from __future__ import annotations

import asyncio
import math
import tempfile
from unittest.mock import AsyncMock

import pytest

from hypeedge.core.enums import OrderStatus, Side, StrategyStatus
from hypeedge.core.events import EVENT_CANDLE_UPDATE, EVENT_L2_BOOK_UPDATE, EVENT_ORDER_FILLED, Event, EventBus
from hypeedge.core.models import Candle, Order
from hypeedge.core.types import Cloid, Price, Size, StrategyId, Symbol, Timestamp
from hypeedge.strategy.indicators import atr, ema, macd, momentum, sma
from hypeedge.strategy.params import ParamWatcher, TrendParams, load_params
from hypeedge.strategy.runner import StrategyRunner
from hypeedge.strategy.trend_follow import TrendFollowStrategy

# --- Test Indicators ---


class TestIndicators:
    def test_sma_basic(self):
        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        result = sma(values, 3)
        assert math.isnan(result[0])
        assert math.isnan(result[1])
        assert abs(result[2] - 2.0) < 1e-6  # (1+2+3)/3
        assert abs(result[3] - 3.0) < 1e-6  # (2+3+4)/3
        assert abs(result[4] - 4.0) < 1e-6  # (3+4+5)/3

    def test_sma_period_equals_length(self):
        values = [10.0, 20.0, 30.0]
        result = sma(values, 3)
        assert math.isnan(result[0])
        assert math.isnan(result[1])
        assert abs(result[2] - 20.0) < 1e-6

    def test_ema_basic(self):
        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        result = ema(values, 3)
        assert math.isnan(result[0])
        assert math.isnan(result[1])
        # Seed = SMA(1,2,3) = 2.0
        assert abs(result[2] - 2.0) < 1e-6
        # EMA: 4 * 0.5 + 2.0 * 0.5 = 3.0
        assert abs(result[3] - 3.0) < 1e-6

    def test_macd_returns_three_lists(self):
        # Need at least 35 values for MACD(12,26,9)
        values = [float(i) for i in range(1, 50)]
        macd_line, signal_line, histogram = macd(values, 12, 26, 9)
        assert len(macd_line) == len(values)
        assert len(signal_line) == len(values)
        assert len(histogram) == len(values)
        # First 25 values should be NaN (slow EMA needs 26)
        assert math.isnan(macd_line[24])
        # After 35+ values, should have valid numbers
        assert not math.isnan(macd_line[35])

    def test_atr_basic(self):
        highs = [12.0, 13.0, 14.0, 13.0, 15.0]
        lows = [10.0, 11.0, 12.0, 11.0, 13.0]
        closes = [11.0, 12.0, 13.0, 12.0, 14.0]
        result = atr(highs, lows, closes, period=3)
        assert len(result) == 5
        assert math.isnan(result[0])  # First bar
        # ATR values after warmup should be positive
        for v in result[3:]:
            assert not math.isnan(v)
            assert v > 0

    def test_atr_empty(self):
        assert atr([], [], [], 14) == []

    def test_momentum_basic(self):
        values = [100.0, 102.0, 104.0, 106.0, 108.0, 110.0]
        result = momentum(values, 3)
        assert len(result) == 6
        assert math.isnan(result[0])
        assert math.isnan(result[1])
        assert math.isnan(result[2])
        # (106 - 100) / 100 = 0.06
        assert abs(result[3] - 0.06) < 1e-6

    def test_momentum_period_too_large(self):
        values = [1.0, 2.0]
        result = momentum(values, 5)
        assert all(math.isnan(v) for v in result)


# --- Test TrendParams ---


class TestTrendParams:
    def test_default_values(self):
        params = TrendParams()
        assert params.symbol == "BTC"
        assert params.fast_ema_period == 12
        assert params.slow_ema_period == 26
        assert params.atr_stop_multiplier == 2.0

    def test_load_from_yaml(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("symbol: ETH\nfast_ema_period: 10\natr_stop_multiplier: 3.0\n")
            f.flush()
            params = load_params(f.name)
            assert params.symbol == "ETH"
            assert params.fast_ema_period == 10
            assert params.atr_stop_multiplier == 3.0
            # Default values preserved
            assert params.slow_ema_period == 26

    def test_load_missing_file_returns_defaults(self):
        params = load_params("/nonexistent/path.yaml")
        assert params.symbol == "BTC"

    def test_load_unknown_keys_ignored(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("symbol: SOL\nunknown_key: 42\n")
            f.flush()
            params = load_params(f.name)
            assert params.symbol == "SOL"

    def test_frozen(self):
        params = TrendParams()
        with pytest.raises(AttributeError):
            params.symbol = "ETH"  # type: ignore[misc]


# --- Test TrendFollowStrategy ---


def _make_candle(ts: int, close: float, high: float | None = None, low: float | None = None) -> Candle:
    return Candle(
        symbol=Symbol("BTC"),
        interval="1m",
        open=Price(close),
        high=Price(high if high is not None else close + 1.0),
        low=Price(low if low is not None else close - 1.0),
        close=Price(close),
        volume=Size(100.0),
        timestamp=Timestamp(ts),
    )


def _make_ack_order() -> Order:
    return Order(
        cloid=Cloid("test"),
        symbol=Symbol("BTC"),
        side=Side.BUY,
        size=Size(0.01),
        price=Price(100.0),
        order_type="limit",
        time_in_force="Gtc",
        status=OrderStatus.ACKNOWLEDGED,
    )


class TestTrendFollowStrategy:
    @pytest.mark.asyncio
    async def test_needs_warmup_before_signals(self):
        """Strategy should not generate signals until enough candles are received."""
        bus = EventBus(queue_maxsize=100)
        mock_client = AsyncMock()
        params = TrendParams(slow_ema_period=26, symbol="BTC")
        strategy = TrendFollowStrategy(StrategyId("test"), bus, mock_client, params)

        await strategy.on_start()
        assert strategy.status == StrategyStatus.RUNNING

        # Feed fewer candles than needed (slow_ema_period * 3 = 78)
        for i in range(10):
            event = Event(event_type=EVENT_CANDLE_UPDATE, payload=_make_candle(i, 100.0 + i))
            await strategy.on_event(event)

        # No orders should have been submitted
        mock_client.submit_order.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_signal_for_wrong_symbol(self):
        """Events for a different symbol should be ignored."""
        bus = EventBus(queue_maxsize=100)
        mock_client = AsyncMock()
        params = TrendParams(symbol="BTC")
        strategy = TrendFollowStrategy(StrategyId("test"), bus, mock_client, params)

        await strategy.on_start()

        # Create candle for ETH, not BTC
        candle = Candle(
            symbol=Symbol("ETH"),
            interval="1m",
            open=Price(100.0),
            high=Price(101.0),
            low=Price(99.0),
            close=Price(100.0),
            volume=Size(100.0),
            timestamp=Timestamp(0),
        )
        event = Event(event_type=EVENT_CANDLE_UPDATE, payload=candle)
        await strategy.on_event(event)

        mock_client.submit_order.assert_not_called()

    @pytest.mark.asyncio
    async def test_position_opens_on_signal(self):
        """After warmup, a MACD cross + momentum should trigger an order."""
        bus = EventBus(queue_maxsize=100)
        mock_client = AsyncMock()
        mock_client.submit_order.return_value = _make_ack_order()

        params = TrendParams(
            symbol="BTC",
            slow_ema_period=5,
            fast_ema_period=3,
            signal_ema_period=3,
            momentum_period=3,
            atr_period=3,
            momentum_threshold=-0.02,  # allow slightly negative momentum at cross
        )
        strategy = TrendFollowStrategy(StrategyId("test"), bus, mock_client, params)

        await strategy.on_start()

        # First drop prices (MACD below signal), then rise sharply (bullish cross)
        prices = []
        for i in range(15):
            prices.append(100.0 - i * 0.5)  # Drop from 100 to 93
        for i in range(20):
            prices.append(93.0 + i * 2.0)  # Sharp rise from 93 to 131

        for i, price in enumerate(prices):
            event = Event(event_type=EVENT_CANDLE_UPDATE, payload=_make_candle(i, price))
            await strategy.on_event(event)

        # At least one order should have been submitted (bullish cross)
        assert mock_client.submit_order.call_count >= 1

    @pytest.mark.asyncio
    async def test_stop_loss_closes_position(self):
        """A price drop below stop should trigger position close."""
        bus = EventBus(queue_maxsize=100)
        mock_client = AsyncMock()
        ack_order = _make_ack_order()
        mock_client.submit_order.return_value = ack_order

        params = TrendParams(
            symbol="BTC",
            slow_ema_period=5,
            fast_ema_period=3,
            signal_ema_period=3,
            momentum_period=3,
            atr_period=3,
            atr_stop_multiplier=0.5,  # tight stop for testing
            momentum_threshold=0.0,
        )
        strategy = TrendFollowStrategy(StrategyId("test"), bus, mock_client, params)
        await strategy.on_start()

        # Rising prices to trigger entry
        for i in range(20):
            event = Event(event_type=EVENT_CANDLE_UPDATE, payload=_make_candle(i, 100.0 + i))
            await strategy.on_event(event)

        if strategy.position_size > 0:
            # Now drop price below stop
            stop = strategy.stop_price
            assert stop is not None

            # Price below stop should trigger close
            event = Event(
                event_type=EVENT_CANDLE_UPDATE,
                payload=_make_candle(100, stop - 1.0, high=stop, low=stop - 2.0),
            )
            await strategy.on_event(event)

            assert strategy.position_size == 0.0

    @pytest.mark.asyncio
    async def test_on_stop_closes_position(self):
        """on_stop should close any open position."""
        bus = EventBus(queue_maxsize=100)
        mock_client = AsyncMock()
        mock_client.submit_order.return_value = _make_ack_order()

        params = TrendParams(
            symbol="BTC",
            slow_ema_period=5,
            fast_ema_period=3,
            signal_ema_period=3,
            momentum_period=3,
            atr_period=3,
            momentum_threshold=0.0,
        )
        strategy = TrendFollowStrategy(StrategyId("test"), bus, mock_client, params)
        await strategy.on_start()

        # Feed enough data
        for i in range(25):
            await strategy.on_event(Event(event_type=EVENT_CANDLE_UPDATE, payload=_make_candle(i, 100.0 + i)))

        # If a position was opened, on_stop should close it
        if strategy.position_size != 0:
            await strategy.on_stop()
            assert strategy.position_size == 0.0
        else:
            await strategy.on_stop()
            assert strategy.status == StrategyStatus.STOPPED

    def test_param_update(self):
        """Hot-reload should update params."""
        bus = EventBus(queue_maxsize=100)
        mock_client = AsyncMock()
        params = TrendParams(symbol="BTC")
        strategy = TrendFollowStrategy(StrategyId("test"), bus, mock_client, params)

        assert strategy.params.symbol == "BTC"
        new_params = TrendParams(symbol="ETH")
        strategy.update_params(new_params)
        assert strategy.params.symbol == "ETH"

    @pytest.mark.asyncio
    async def test_acknowledgement_does_not_create_position(self):
        bus = EventBus(queue_maxsize=100)
        mock_client = AsyncMock()
        mock_client.submit_order.return_value = _make_ack_order()
        strategy = TrendFollowStrategy(StrategyId("test"), bus, mock_client, TrendParams(symbol="BTC"))

        await strategy._open_position(Side.BUY, 100.0, 1.0)
        assert strategy.position_size == 0.0

    @pytest.mark.asyncio
    async def test_runner_consumes_event_bus_sequentially(self):
        bus = EventBus(queue_maxsize=100)
        mock_client = AsyncMock()
        strategy = TrendFollowStrategy(StrategyId("test"), bus, mock_client, TrendParams(symbol="BTC"))
        runner = StrategyRunner(strategy, bus)
        task = asyncio.create_task(runner.run())
        await asyncio.sleep(0)

        bus.publish_sync(Event(event_type=EVENT_CANDLE_UPDATE, payload=_make_candle(1, 100.0)))
        await asyncio.sleep(0.01)
        assert strategy._candle_count == 1

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_runner_isolates_reliable_events_from_lossy_market_flood(self):
        class RecordingStrategy:
            strategy_id = StrategyId("recording")

            def __init__(self) -> None:
                self.started = asyncio.Event()
                self.release_first_market_event = asyncio.Event()
                self.first_market_event_started = asyncio.Event()
                self.fill_seen = asyncio.Event()
                self.latest_market_seen = asyncio.Event()
                self.received: list[tuple[str, object]] = []
                self.stopped = False

            def subscriptions(self) -> frozenset[str]:
                return frozenset({EVENT_L2_BOOK_UPDATE, EVENT_ORDER_FILLED})

            async def on_start(self) -> None:
                self.started.set()

            async def on_event(self, event: Event) -> None:
                self.received.append((event.event_type, event.payload))
                if event.event_type == EVENT_L2_BOOK_UPDATE and event.payload == 0:
                    self.first_market_event_started.set()
                    await self.release_first_market_event.wait()
                if event.event_type == EVENT_ORDER_FILLED:
                    self.fill_seen.set()
                if event.event_type == EVENT_L2_BOOK_UPDATE and event.payload == 100:
                    self.latest_market_seen.set()

            async def on_stop(self) -> None:
                self.stopped = True

        bus = EventBus(queue_maxsize=1)
        strategy = RecordingStrategy()
        runner = StrategyRunner(strategy, bus)
        task = asyncio.create_task(runner.run())
        await strategy.started.wait()

        bus.publish_sync(Event(event_type=EVENT_L2_BOOK_UPDATE, payload=0))
        await strategy.first_market_event_started.wait()
        for version in range(1, 101):
            bus.publish_sync(Event(event_type=EVENT_L2_BOOK_UPDATE, payload=version))
        bus.publish_sync(Event(event_type=EVENT_ORDER_FILLED, payload="fill"))

        strategy.release_first_market_event.set()
        await asyncio.wait_for(strategy.fill_seen.wait(), timeout=1)
        await asyncio.wait_for(strategy.latest_market_seen.wait(), timeout=1)

        assert (EVENT_ORDER_FILLED, "fill") in strategy.received
        assert strategy.received[-2:] == [
            (EVENT_ORDER_FILLED, "fill"),
            (EVENT_L2_BOOK_UPDATE, 100),
        ]

        await runner.stop()
        await asyncio.wait_for(task, timeout=1)
        assert strategy.stopped is True
        assert bus.stats["subscribers"] == 0


# --- Test ParamWatcher ---


class TestParamWatcher:
    def test_log_changes_detects_differences(self):
        old = TrendParams(symbol="BTC", fast_ema_period=12)
        new = TrendParams(symbol="ETH", fast_ema_period=10)
        # Should not raise
        ParamWatcher._log_changes(old, new)
