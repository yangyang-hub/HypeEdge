"""Trend following strategy (design doc §7.1).

Signal logic:
- MACD crossover (fast EMA - slow EMA vs signal line) for trend direction
- Momentum confirmation (rate of change > threshold)
- ATR-based position sizing and stop-loss

Entry:
- BUY: MACD line crosses above signal line AND momentum > threshold AND flat
- SELL: MACD line crosses below signal line AND momentum < -threshold AND flat

Exit:
- Stop-loss: price hits entry ± ATR * stop_multiplier
- Signal reversal: MACD cross in opposite direction
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

import structlog

from hypeedge.core.enums import OrderType, Side, StrategyStatus
from hypeedge.core.events import (
    EVENT_CANDLE_UPDATE,
    EVENT_ORDER_CANCELLED,
    EVENT_ORDER_EXPIRED,
    EVENT_ORDER_FILLED,
    EVENT_ORDER_REJECTED,
    EVENT_SIGNAL_GENERATED,
    Event,
    EventBus,
)
from hypeedge.core.models import Candle, Order, OrderIntent, Signal
from hypeedge.core.types import Price, Size, StrategyId, Symbol
from hypeedge.strategy.indicators import atr, macd, momentum
from hypeedge.strategy.params import TrendParams

if TYPE_CHECKING:
    from hypeedge.account.tracker import AccountTracker
    from hypeedge.execution.engine import ExecutionClient

logger = structlog.get_logger(__name__)

# Minimum number of candles before generating signals
_MIN_CANDLES_FACTOR = 3  # Need at least slow_ema_period * 3 bars


class TrendFollowStrategy:
    """Trend following strategy using MACD + momentum + ATR.

    Subscribes to EVENT_CANDLE_UPDATE via EventBus.
    Submits orders through the injected ExecutionClient.
    Uses AccountTracker for real equity-based position sizing.
    """

    def __init__(
        self,
        strategy_id: StrategyId,
        event_bus: EventBus,
        execution_client: ExecutionClient,
        params: TrendParams,
        account_tracker: AccountTracker | None = None,
    ) -> None:
        self.strategy_id = strategy_id
        self._event_bus = event_bus
        self._execution = execution_client
        self._status = StrategyStatus.STOPPED
        self._log = logger.bind(strategy_id=str(strategy_id))
        self._tracker = account_tracker

        self._params = params
        self._symbol = Symbol(params.symbol)

        # Price buffers
        self._closes: list[float] = []
        self._highs: list[float] = []
        self._lows: list[float] = []

        # Position tracking
        self._position_size: float = 0.0  # + long, - short, 0 flat
        self._entry_price: float | None = None
        self._stop_price: float | None = None

        # MACD state for cross detection
        self._prev_macd_above_signal: bool | None = None

        self._candle_count = 0
        self._working_order_cloid: str | None = None
        self._working_order_is_close = False

    def subscriptions(self) -> frozenset[str]:
        return frozenset(
            {
                EVENT_CANDLE_UPDATE,
                EVENT_ORDER_FILLED,
                EVENT_ORDER_CANCELLED,
                EVENT_ORDER_REJECTED,
                EVENT_ORDER_EXPIRED,
            }
        )

    @property
    def params(self) -> TrendParams:
        return self._params

    @property
    def status(self) -> StrategyStatus:
        return self._status

    def set_status(self, status: StrategyStatus) -> None:
        """Update lifecycle status without tearing down the runner (pause/resume)."""
        self._status = status

    def update_params(self, new_params: TrendParams) -> None:
        """Hot-reload parameters (design doc §15.2)."""
        old = self._params
        self._params = new_params
        self._log.info(
            "strategy_params_updated",
            old_symbol=old.symbol,
            new_symbol=new_params.symbol,
        )

    @property
    def position_size(self) -> float:
        return self._position_size

    @property
    def entry_price(self) -> float | None:
        return self._entry_price

    @property
    def stop_price(self) -> float | None:
        return self._stop_price

    async def on_start(self) -> None:
        """Initialize strategy state. StrategyRunner owns subscriptions."""
        self._status = StrategyStatus.RUNNING
        self._sync_position_from_tracker()
        self._log.info("trend_strategy_started", symbol=self._params.symbol)

    async def on_event(self, event: Event) -> None:
        """Process candle events and generate signals."""
        if event.event_type != EVENT_CANDLE_UPDATE:
            if isinstance(event.payload, Order) and event.payload.strategy_id == self.strategy_id:
                if self._working_order_cloid != str(event.payload.cloid):
                    return
                self._sync_position_from_tracker()
                if event.event_type == EVENT_ORDER_FILLED and self._tracker is not None:
                    # Keep the strategy blocked until the fill/account projection
                    # confirms the resulting position; order status alone is not
                    # sufficient to infer inventory.
                    position = self._tracker.get_position(self._symbol)
                    projection_confirmed = position is None if self._working_order_is_close else position is not None
                    if projection_confirmed:
                        self._working_order_cloid = None
                        self._working_order_is_close = False
                else:
                    self._working_order_cloid = None
                    self._working_order_is_close = False
            return

        candle: Candle = event.payload
        if str(candle.symbol) != self._params.symbol:
            return

        self._candle_count += 1
        self._sync_position_from_tracker()
        self._closes.append(float(candle.close))
        self._highs.append(float(candle.high))
        self._lows.append(float(candle.low))

        # Need enough data before generating signals
        min_candles = self._params.slow_ema_period * _MIN_CANDLES_FACTOR
        if self._candle_count < min_candles:
            return

        if self._status == StrategyStatus.PAUSED:
            return

        await self._process_candle(candle)

    async def on_stop(self) -> None:
        """Clean up — close any open positions."""
        self._sync_position_from_tracker()
        if self._position_size != 0 and self._working_order_cloid is None:
            self._log.info("strategy_stopping_closing_position", size=self._position_size)
            try:
                await self._close_position(self._closes[-1] if self._closes else 0.0)
            except Exception:
                self._log.exception("strategy_stop_close_failed")
        self._status = StrategyStatus.STOPPED
        self._log.info("trend_strategy_stopped")

    async def _process_candle(self, candle: Candle) -> None:
        """Core signal logic on each candle."""
        p = self._params

        # Compute indicators (MACD uses fast/slow EMA internally)
        macd_line, signal_line, histogram = macd(
            self._closes,
            p.fast_ema_period,
            p.slow_ema_period,
            p.signal_ema_period,
        )
        atr_values = atr(self._highs, self._lows, self._closes, p.atr_period)
        mom_values = momentum(self._closes, p.momentum_period)

        # Get latest valid values
        macd_val = macd_line[-1] if macd_line else math.nan
        signal_val = signal_line[-1] if signal_line else math.nan
        atr_val = atr_values[-1] if atr_values else math.nan
        mom_val = mom_values[-1] if mom_values else math.nan

        if any(math.isnan(v) for v in [macd_val, signal_val, atr_val, mom_val]):
            return

        # MACD cross detection
        macd_above = macd_val > signal_val
        prev_above = self._prev_macd_above_signal
        self._prev_macd_above_signal = macd_above

        current_price = float(candle.close)

        # Check stop-loss first
        if self._stop_price is not None:
            if self._position_size > 0 and current_price <= self._stop_price:
                self._log.info(
                    "stop_loss_triggered_long",
                    price=current_price,
                    stop=self._stop_price,
                    entry=self._entry_price,
                )
                await self._close_position(current_price)
                return
            elif self._position_size < 0 and current_price >= self._stop_price:
                self._log.info(
                    "stop_loss_triggered_short",
                    price=current_price,
                    stop=self._stop_price,
                    entry=self._entry_price,
                )
                await self._close_position(current_price)
                return

        # Signal generation
        if prev_above is not None:
            # Bullish cross: MACD crosses above signal
            bullish_cross = macd_above and not prev_above
            # Bearish cross: MACD crosses below signal
            bearish_cross = not macd_above and prev_above

            if bullish_cross and mom_val > p.momentum_threshold:
                # BUY signal
                if self._position_size < 0:
                    # Close short first
                    await self._close_position(current_price)
                if self._position_size == 0:
                    await self._open_position(Side.BUY, current_price, atr_val)

            elif bearish_cross and mom_val < -p.momentum_threshold:
                # SELL signal
                if self._position_size > 0:
                    await self._close_position(current_price)
                if self._position_size == 0:
                    await self._open_position(Side.SELL, current_price, atr_val)

    async def _open_position(self, side: Side, price: float, atr_val: float) -> None:
        """Open a new position with ATR-based sizing."""
        p = self._params
        size = self._calculate_position_size(price, atr_val)
        if size <= 0 or self._working_order_cloid is not None:
            return

        # Set stop-loss
        stop_distance = atr_val * p.atr_stop_multiplier
        if side == Side.BUY:
            self._stop_price = price - stop_distance
        else:
            self._stop_price = price + stop_distance

        self._log.info(
            "opening_position",
            side=str(side),
            size=size,
            price=price,
            stop=self._stop_price,
            atr=atr_val,
        )

        intent = OrderIntent(
            symbol=self._symbol,
            side=side,
            size=Size(size),
            price=Price(price),
            order_type=OrderType.LIMIT,
            strategy_id=self.strategy_id,
        )

        try:
            order = await self._execution.submit_order(intent)
            if order.status.value not in ("rejected", "cancelled", "expired"):
                self._working_order_cloid = str(order.cloid)
                self._working_order_is_close = False
                self._sync_position_from_tracker()
                if (
                    order.status.value == "filled"
                    and self._tracker is not None
                    and self._tracker.get_position(self._symbol) is not None
                ):
                    self._working_order_cloid = None
                # Publish signal event
                self._event_bus.publish_sync(
                    Event(
                        event_type=EVENT_SIGNAL_GENERATED,
                        payload=Signal(
                            strategy_id=self.strategy_id,
                            symbol=self._symbol,
                            action="buy" if side == Side.BUY else "sell",
                            size=Size(size),
                            price=Price(price),
                            confidence=min(abs(atr_val / price) * 100, 1.0),
                            metadata={
                                "atr": atr_val,
                                "stop": self._stop_price,
                                "order_cloid": str(order.cloid),
                            },
                        ),
                        correlation_id=str(order.cloid),
                    )
                )
        except Exception:
            self._log.exception("open_position_failed")

    async def _close_position(self, price: float) -> None:
        """Close the current position."""
        self._sync_position_from_tracker()
        if self._position_size == 0 or self._working_order_cloid is not None:
            return

        side = Side.SELL if self._position_size > 0 else Side.BUY
        size = abs(self._position_size)

        self._log.info(
            "closing_position",
            side=str(side),
            size=size,
            price=price,
            entry=self._entry_price,
            stop=self._stop_price,
        )

        intent = OrderIntent(
            symbol=self._symbol,
            side=side,
            size=Size(size),
            order_type=OrderType.MARKET,
            reduce_only=True,
            strategy_id=self.strategy_id,
        )

        try:
            order = await self._execution.submit_order(intent)
            if order.status.value not in ("rejected", "cancelled", "expired"):
                self._working_order_cloid = str(order.cloid)
                self._working_order_is_close = True
            self._sync_position_from_tracker()
            if (
                order.status.value == "filled"
                and self._tracker is not None
                and self._tracker.get_position(self._symbol) is None
            ):
                self._working_order_cloid = None
                self._working_order_is_close = False
        except Exception:
            self._log.exception("close_position_failed")

    def _sync_position_from_tracker(self) -> None:
        """Use fill/reconciliation-derived position state; never infer from ACK."""
        if self._tracker is None:
            return
        position = self._tracker.get_position(self._symbol)
        if position is None:
            self._position_size = 0.0
            self._entry_price = None
            self._stop_price = None
            return
        self._position_size = float(position.size)
        self._entry_price = float(position.entry_price) if position.entry_price is not None else None

    def _calculate_position_size(self, price: float, atr_val: float) -> float:
        """ATR-based position sizing.

        size = (equity * risk_per_trade_pct) / (ATR * atr_position_multiplier)
        Capped at max_position_pct of equity / price.
        """
        # Use AccountTracker for real equity; fall back to default
        equity = float(self._tracker.current_equity) if self._tracker else 10_000.0
        p = self._params

        if atr_val <= 0 or price <= 0 or equity <= 0:
            return 0.0

        # Risk-based sizing
        risk_amount = equity * p.risk_per_trade_pct
        size = risk_amount / (atr_val * p.atr_position_multiplier)

        # Cap at max position %
        max_size = (equity * p.max_position_pct) / price
        return min(size, max_size)
