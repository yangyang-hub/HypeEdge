"""Backtest engine — orchestrates strategy, broker, and data feed for simulation.

Replaces the Phase 1 skeleton with a fully functional event-driven backtest
that reuses the same StrategyBase.on_event() code path as live trading.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import structlog

from hypeedge.backtest.broker import (
    FeeConfig,
    SimulatedBroker,
    SlippageConfig,
    SlippageMode,
)
from hypeedge.backtest.data_feed import DataFeed
from hypeedge.backtest.metrics import MetricsCalculator, PerformanceMetrics
from hypeedge.core.enums import OrderStatus, Side
from hypeedge.core.events import (
    EVENT_CANDLE_UPDATE,
    EVENT_ORDER_CANCELLED,
    EVENT_ORDER_FILLED,
    EVENT_ORDER_SUBMITTED,
    Event,
    EventBus,
)
from hypeedge.core.models import Candle, Fill, FundingRate, Order, OrderIntent, Position
from hypeedge.core.types import Cloid, Price, Size, Timestamp, Usd

logger = structlog.get_logger(__name__)

# Type alias for the strategy factory callable
StrategyFactory = Any  # Callable[[EventBus, ExecutionClient], StrategyBase]


@dataclass(frozen=True)
class BacktestResult:
    """Complete result of a single backtest run."""

    metrics: PerformanceMetrics
    fills: list[Fill]
    equity_curve: list[tuple[Timestamp, Usd]]


class SimulatedExecutionClient:
    """Simulated execution client for backtesting.

    Implements the ExecutionClient Protocol so strategies can use the same
    interface in both live and backtest modes. Fills are simulated against
    candle data via the SimulatedBroker.
    """

    def __init__(
        self,
        broker: SimulatedBroker,
        event_bus: EventBus,
    ) -> None:
        self._broker = broker
        self._event_bus = event_bus
        self._open_orders: dict[Cloid, Order] = {}
        self._cloid_counter = 0

    async def submit_order(self, intent: OrderIntent) -> Order:
        """Submit an order intent. Returns the created Order.

        In backtest mode, the order is created but NOT immediately filled.
        The engine will attempt fills on subsequent candles.
        """
        cloid = intent.cloid or self._generate_cloid()
        order = Order(
            cloid=cloid,
            symbol=intent.symbol,
            side=intent.side,
            size=intent.size,
            price=intent.price,
            order_type=intent.order_type,
            time_in_force=intent.time_in_force,
            status=OrderStatus.SUBMITTED,
            strategy_id=intent.strategy_id,
            reduce_only=intent.reduce_only,
            created_at=datetime.now(UTC),
        )
        self._open_orders[cloid] = order

        self._event_bus.publish_sync(Event(event_type=EVENT_ORDER_SUBMITTED, payload=order, correlation_id=str(cloid)))
        return order

    async def cancel_order(self, cloid: str) -> bool:
        """Cancel an order by cloid."""
        c = Cloid(cloid)
        if c in self._open_orders:
            order = self._open_orders.pop(c)
            order.status = OrderStatus.CANCELLED
            self._event_bus.publish_sync(Event(event_type=EVENT_ORDER_CANCELLED, payload=order, correlation_id=cloid))
            return True
        return False

    async def cancel_all_orders(self, symbol: str | None = None) -> int:
        """Cancel all open orders, optionally filtered by symbol."""
        to_cancel = []
        for cloid, order in self._open_orders.items():
            if symbol is None or str(order.symbol) == symbol:
                to_cancel.append(cloid)

        for cloid in to_cancel:
            await self.cancel_order(str(cloid))
        return len(to_cancel)

    async def get_order(self, cloid: str) -> Order | None:
        return self._open_orders.get(Cloid(cloid))

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        if symbol is None:
            return list(self._open_orders.values())
        return [o for o in self._open_orders.values() if str(o.symbol) == symbol]

    def try_fill_orders(self, candle: Candle, fills_collector: list[Fill]) -> None:
        """Attempt to fill open orders against the given candle.

        Called by the engine after publishing each candle event.
        Successfully filled orders are removed from _open_orders and their
        Fill records are appended to fills_collector.
        """
        filled_cloids: list[Cloid] = []

        for cloid, order in self._open_orders.items():
            # Build an OrderIntent-like object for the broker
            intent = OrderIntent(
                symbol=order.symbol,
                side=order.side,
                size=order.remaining_size,
                price=order.price,
                order_type=order.order_type,
                time_in_force=order.time_in_force,
                strategy_id=order.strategy_id,
                reduce_only=order.reduce_only,
                cloid=cloid,
            )
            fill = self._broker.simulate_fill(intent, candle, cloid)
            if fill is not None:
                # Update order state
                order.status = OrderStatus.FILLED
                order.filled_size = order.size
                order.avg_fill_price = fill.price
                order.filled_at = datetime.now(UTC)
                filled_cloids.append(cloid)

                fills_collector.append(fill)
                self._event_bus.publish_sync(
                    Event(event_type=EVENT_ORDER_FILLED, payload=fill, correlation_id=str(cloid))
                )

        for cloid in filled_cloids:
            del self._open_orders[cloid]

    def _generate_cloid(self) -> Cloid:
        self._cloid_counter += 1
        return Cloid(f"bt_cloid_{self._cloid_counter}")


class BacktestEngine:
    """Main backtest orchestrator.

    Wires DataFeed, SimulatedBroker, SimulatedExecutionClient, and a strategy
    together. Replays historical data through the EventBus, lets the strategy
    react via on_event(), simulates fills, and computes performance metrics.
    """

    def __init__(
        self,
        fee_config: FeeConfig | None = None,
        slippage_config: SlippageConfig | None = None,
    ) -> None:
        self._fee_config = fee_config or FeeConfig()
        self._slippage_config = slippage_config or SlippageConfig()

    async def run(
        self,
        candles: list[Candle],
        funding_rates: list[FundingRate] | None,
        strategy_factory: StrategyFactory,
        initial_capital: Usd | None = None,
        slippage_mode: SlippageMode = SlippageMode.PESSIMISTIC,
    ) -> BacktestResult:
        """Run a backtest.

        Args:
            candles: Historical candle data (sorted by timestamp).
            funding_rates: Historical funding rate data (optional).
            strategy_factory: Callable that creates a strategy given
                (event_bus, execution_client). Example:
                lambda eb, ec: MyStrategy(StrategyId("test"), eb, ec)
            initial_capital: Starting equity in USDC.
            slippage_mode: Optimistic or pessimistic fill simulation.

        Returns:
            BacktestResult with metrics, fills, and equity curve.
        """
        if initial_capital is None:
            initial_capital = Usd(10_000.0)

        # Create isolated components for this run
        event_bus = EventBus(queue_maxsize=10_000)
        broker = SimulatedBroker(
            fee_config=self._fee_config,
            slippage_config=self._slippage_config,
            mode=slippage_mode,
        )
        execution_client = SimulatedExecutionClient(broker, event_bus)
        data_feed = DataFeed(candles, funding_rates, event_bus)

        # Portfolio state tracking
        equity = initial_capital
        peak_equity = initial_capital
        positions: dict[str, Position] = {}  # symbol -> Position
        all_fills: list[Fill] = []
        equity_curve: list[tuple[Timestamp, Usd]] = []
        total_funding = Usd(0.0)
        applied_funding: set[tuple[str, int]] = set()
        realized_trade_pnls: list[Usd] = []

        # Create and start strategy
        strategy = strategy_factory(event_bus, execution_client)
        await strategy.on_start()

        logger.info(
            "backtest_started",
            candles=data_feed.total_candles,
            initial_capital=float(initial_capital),
            slippage_mode=str(slippage_mode),
        )

        # Main simulation loop
        while data_feed.has_next:
            candle = data_feed.next_candle()
            if candle is None:
                break

            # 1. Fill orders submitted on earlier candles. This prevents a
            # strategy from seeing a candle's close/high/low and then filling
            # a newly-created order against that same candle.
            prev_fill_count = len(all_fills)
            execution_client.try_fill_orders(candle, all_fills)

            # 2. Apply fills to cash and positions.
            for fill in all_fills[prev_fill_count:]:
                equity = Usd(equity - fill.fee)
                realized = self._update_position(positions, fill)
                equity = Usd(equity + realized)
                if realized != Usd(0.0):
                    realized_trade_pnls.append(realized)

            # 3. Mark positions to the current close before recording equity.
            position = positions.get(str(candle.symbol))
            if position is not None:
                position.mark_price = candle.close

            # 4. Apply each exchange funding record at most once.
            if funding_rates:
                funding_amount = self._apply_funding_once(positions, funding_rates, candle.timestamp, applied_funding)
                if funding_amount != Usd(0.0):
                    equity = Usd(equity - funding_amount)
                    total_funding = Usd(total_funding + funding_amount)

            marked_equity = Usd(equity + self._unrealized_pnl(positions))
            if marked_equity > peak_equity:
                peak_equity = marked_equity
            equity_curve.append((candle.timestamp, marked_equity))

            # 5. Let the strategy react after this candle has been fully
            # accounted for. Orders created here are eligible next candle.
            candle_event = Event(
                event_type=EVENT_CANDLE_UPDATE,
                payload=candle,
                correlation_id=str(candle.symbol),
            )
            try:
                await strategy.on_event(candle_event)
            except Exception:
                logger.exception(
                    "backtest_strategy_error",
                    candle_ts=candle.timestamp,
                    symbol=str(candle.symbol),
                )

        # Stop strategy
        await strategy.on_stop()

        # Orders created by on_stop cannot be honestly filled without a later
        # market observation. Leave them open rather than introducing a
        # synthetic same-bar fill; final equity includes marked open positions.

        # Calculate metrics
        calculator = MetricsCalculator(
            fills=all_fills,
            equity_curve=equity_curve,
            initial_capital=initial_capital,
            funding_total=total_funding,
            trade_pnls=realized_trade_pnls,
        )
        metrics = calculator.calculate()

        logger.info(
            "backtest_complete",
            trades=metrics.trade_count,
            total_return=f"{metrics.total_return_pct:.4%}",
            max_drawdown=f"{metrics.max_drawdown_pct:.4%}",
            sharpe=f"{metrics.sharpe_ratio:.2f}",
        )

        return BacktestResult(
            metrics=metrics,
            fills=all_fills,
            equity_curve=equity_curve,
        )

    @staticmethod
    def _apply_fill_to_equity(fill: Fill, equity: Usd) -> Usd:
        """Update equity based on a fill's fee impact.

        Fees reduce equity (taker), rebates increase equity (maker).
        The position's PnL is tracked separately via mark-to-market.
        """
        return Usd(equity - fill.fee)

    @staticmethod
    def _update_position(positions: dict[str, Position], fill: Fill) -> Usd:
        """Update an average-cost position and return realized PnL."""
        key = str(fill.symbol)
        pos = positions.get(key)

        if pos is None:
            # New position
            signed_size = fill.size if fill.side == Side.BUY else -fill.size
            positions[key] = Position(
                symbol=fill.symbol,
                size=Size(signed_size),
                entry_price=fill.price,
                mark_price=fill.price,
            )
            return Usd(0.0)
        else:
            old_size = float(pos.size)
            signed_fill = float(fill.size) if fill.side == Side.BUY else -float(fill.size)
            new_size = old_size + signed_fill
            entry = float(pos.entry_price or fill.price)

            # Same direction: increase the position using VWAP.
            if old_size * signed_fill > 0:
                new_entry = (abs(old_size) * entry + abs(signed_fill) * float(fill.price)) / abs(new_size)
                pos.size = Size(new_size)
                pos.entry_price = Price(new_entry)
                pos.mark_price = fill.price
                return Usd(0.0)

            # Opposite direction: close all or part of the old position.
            closing_size = min(abs(old_size), abs(signed_fill))
            direction = 1.0 if old_size > 0 else -1.0
            realized = Usd((float(fill.price) - entry) * closing_size * direction)

            if abs(new_size) < 1e-12:
                del positions[key]
            elif old_size * new_size > 0:
                # Partial reduction: entry price must remain unchanged.
                pos.size = Size(new_size)
                pos.mark_price = fill.price
            else:
                # Position flipped: the residual starts at the flip fill.
                pos.size = Size(new_size)
                pos.entry_price = fill.price
                pos.mark_price = fill.price
            return realized

    @staticmethod
    def _apply_funding_once(
        positions: dict[str, Position],
        funding_rates: list[FundingRate],
        current_ts: int,
        applied: set[tuple[str, int]],
    ) -> Usd:
        """Apply due funding records exactly once and return account cost."""
        total = Usd(0.0)
        for pos in positions.values():
            if pos.is_flat or pos.mark_price is None:
                continue
            for rate in funding_rates:
                key = (str(rate.symbol), int(rate.timestamp))
                if key not in applied and rate.timestamp <= current_ts and str(rate.symbol) == str(pos.symbol):
                    funding = SimulatedBroker.apply_hourly_funding(pos, rate.funding_rate, pos.mark_price)
                    total = Usd(total + funding)
                    applied.add(key)
        return total

    @staticmethod
    def _unrealized_pnl(positions: dict[str, Position]) -> Usd:
        total = 0.0
        for position in positions.values():
            if position.entry_price is None or position.mark_price is None:
                continue
            total += float(position.size) * (float(position.mark_price) - float(position.entry_price))
        return Usd(total)
