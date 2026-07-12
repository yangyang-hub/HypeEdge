"""Simulated broker for backtesting — fee, slippage, and fill modeling.

Two fill modes (optimistic / pessimistic) produce an income range per the
design doc §6 requirement.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

import structlog

from hypeedge.core.enums import OrderType, Side
from hypeedge.core.models import Candle, Fill, OrderIntent, Position
from hypeedge.core.types import Cloid, OrderId, Price, Size, Usd

logger = structlog.get_logger(__name__)


class SlippageMode(StrEnum):
    """Fill price slippage assumption mode."""

    OPTIMISTIC = "optimistic"
    PESSIMISTIC = "pessimistic"


@dataclass(frozen=True)
class FeeConfig:
    """Fee structure for the simulated exchange.

    Hyperliquid maker rebate is typically negative (you get paid).
    Taker fee is positive (you pay).
    """

    maker_rebate_pct: float = -0.0002  # -0.02% rebate (paid to maker)
    taker_fee_pct: float = 0.0005  # +0.05% fee (paid by taker)


@dataclass(frozen=True)
class SlippageConfig:
    """Slippage assumptions in basis points.

    Optimistic: fills near the theoretical price (low latency, deep book).
    Pessimistic: fills far from theoretical price (high latency, thin book).
    """

    optimistic_bps: float = 2.0  # 0.02% adverse slippage
    pessimistic_bps: float = 10.0  # 0.10% adverse slippage


class SimulatedBroker:
    """Simulates order fills against candle data.

    Design doc §6: "模拟撮合应实现乐观/悲观两级假设，产出收益区间而非单点估计。"
    """

    def __init__(
        self,
        fee_config: FeeConfig | None = None,
        slippage_config: SlippageConfig | None = None,
        mode: SlippageMode = SlippageMode.PESSIMISTIC,
    ) -> None:
        self._fee = fee_config or FeeConfig()
        self._slippage = slippage_config or SlippageConfig()
        self._mode = mode
        self._next_oid = 0

    @property
    def mode(self) -> SlippageMode:
        return self._mode

    def simulate_fill(
        self,
        intent: OrderIntent,
        candle: Candle,
        cloid: Cloid,
    ) -> Fill | None:
        """Simulate a fill for the given order intent against a candle.

        Returns a Fill if the order would have been executed, None otherwise.

        Fill logic:
        - MARKET: always fills at candle close ± slippage.
        - LIMIT BUY: fills if candle.low <= limit price.
        - LIMIT SELL: fills if candle.high >= limit price.
        - Fill price is the limit price (or candle close for market), with slippage applied.
        """
        if intent.order_type == OrderType.MARKET:
            return self._fill_market(intent, candle, cloid)
        elif intent.order_type == OrderType.LIMIT:
            return self._fill_limit(intent, candle, cloid)
        else:
            logger.debug(
                "broker_unsupported_order_type",
                order_type=str(intent.order_type),
                cloid=str(cloid),
            )
            return None

    def apply_slippage(self, price: Price, side: Side) -> Price:
        """Apply slippage to a fill price based on current mode.

        Buy: slippage increases the fill price (you pay more).
        Sell: slippage decreases the fill price (you receive less).
        """
        bps = self._slippage_bps
        factor = bps / 10_000.0
        if side == Side.BUY:
            return Price(price * (1.0 + factor))
        else:
            return Price(price * (1.0 - factor))

    def calculate_fee(self, price: Price, size: Size, is_maker: bool) -> Usd:
        """Calculate the fee for a fill.

        Returns positive Usd for taker fees, negative Usd for maker rebates.
        """
        notional = price * size
        if is_maker:
            return Usd(notional * self._fee.maker_rebate_pct)
        else:
            return Usd(notional * self._fee.taker_fee_pct)

    @staticmethod
    def apply_hourly_funding(position: Position, funding_rate: float, mark_price: Price) -> Usd:
        """Apply hourly funding to a position.

        Design doc §3.5: funding settles every hour (not 8h like Binance).
        Funding payment = position_size * mark_price * funding_rate
        Positive funding_rate: longs pay shorts.
        Negative funding_rate: shorts pay longs.

        A positive return value is a cost paid by the account. A negative
        value is funding received and must therefore increase equity when
        subtracted from cash.
        """
        if position.is_flat:
            return Usd(0.0)
        return Usd(position.size * mark_price * funding_rate)

    def _fill_market(self, intent: OrderIntent, candle: Candle, cloid: Cloid) -> Fill:
        """Fill a market order at candle close with slippage."""
        base_price = candle.close
        fill_price = self.apply_slippage(base_price, intent.side)
        fee = self.calculate_fee(fill_price, intent.size, is_maker=False)
        oid = self._next_order_id()

        logger.debug(
            "broker_market_fill",
            side=str(intent.side),
            base_price=base_price,
            fill_price=fill_price,
            fee=fee,
            cloid=str(cloid),
        )
        return Fill(
            cloid=cloid,
            exchange_oid=oid,
            symbol=intent.symbol,
            side=intent.side,
            price=fill_price,
            size=intent.size,
            fee=fee,
            is_maker=False,
            timestamp=candle.timestamp,
            strategy_id=intent.strategy_id,
        )

    def _fill_limit(self, intent: OrderIntent, candle: Candle, cloid: Cloid) -> Fill | None:
        """Fill a limit order if the candle price crosses the limit price."""
        if intent.price is None:
            return None

        limit_price = intent.price

        if intent.side == Side.BUY:
            # Buy limit fills if candle low <= limit price
            if candle.low > limit_price:
                return None
            # Fill at the limit price (maker assumption: resting on book)
            fill_price = limit_price
            is_maker = True
        else:
            # Sell limit fills if candle high >= limit price
            if candle.high < limit_price:
                return None
            fill_price = limit_price
            is_maker = True

        fee = self.calculate_fee(fill_price, intent.size, is_maker=is_maker)
        oid = self._next_order_id()

        logger.debug(
            "broker_limit_fill",
            side=str(intent.side),
            limit_price=limit_price,
            fill_price=fill_price,
            fee=fee,
            cloid=str(cloid),
        )
        return Fill(
            cloid=cloid,
            exchange_oid=oid,
            symbol=intent.symbol,
            side=intent.side,
            price=fill_price,
            size=intent.size,
            fee=fee,
            is_maker=is_maker,
            timestamp=candle.timestamp,
            strategy_id=intent.strategy_id,
        )

    def _next_order_id(self) -> OrderId:
        self._next_oid += 1
        return OrderId(f"bt_{self._next_oid}")

    @property
    def _slippage_bps(self) -> float:
        if self._mode == SlippageMode.OPTIMISTIC:
            return self._slippage.optimistic_bps
        return self._slippage.pessimistic_bps
