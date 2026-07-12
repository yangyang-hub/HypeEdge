"""Domain models used across HypeEdge modules."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from hypeedge.core.enums import OrderStatus, OrderType, Side, TimeInForce
from hypeedge.core.types import Cloid, OrderId, Price, Size, StrategyId, SubAccount, Symbol, Timestamp, Usd

# --- Market Data Models ---


@dataclass(frozen=True)
class L2Level:
    """A single price level in the order book."""

    price: Price
    size: Size


@dataclass(frozen=True)
class L2BookSnapshot:
    """Full L2 order book snapshot for a symbol."""

    symbol: Symbol
    bids: tuple[L2Level, ...]  # Sorted best -> worst
    asks: tuple[L2Level, ...]  # Sorted best -> worst
    timestamp: Timestamp
    local_ts: datetime = field(default_factory=lambda: datetime.now(UTC))
    version: int = 0
    connection_generation: int = 0

    def __post_init__(self) -> None:
        """Freeze level collections even when callers pass compatibility lists."""
        object.__setattr__(self, "bids", tuple(self.bids))
        object.__setattr__(self, "asks", tuple(self.asks))

    @property
    def exchange_ts(self) -> Timestamp:
        """Exchange event time, retained separately from local receipt time."""
        return self.timestamp

    @property
    def received_at(self) -> datetime:
        """Immutable local receipt time captured when this version was updated."""
        return self.local_ts


@dataclass(frozen=True)
class Trade:
    """A single trade from the exchange."""

    symbol: Symbol
    price: Price
    size: Size
    side: Side
    tid: int  # Trade ID
    timestamp: Timestamp
    local_ts: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class Candle:
    """OHLCV candlestick."""

    symbol: Symbol
    interval: str  # e.g. "1m", "5m", "1h"
    open: Price
    high: Price
    low: Price
    close: Price
    volume: Size
    timestamp: Timestamp


@dataclass(frozen=True)
class FundingRate:
    """Funding rate snapshot."""

    symbol: Symbol
    funding_rate: float
    premium: float
    mark_price: Price
    open_interest: float
    timestamp: Timestamp


# --- Order Models ---


@dataclass(frozen=True)
class OrderIntent:
    """Strategy's intent to place an order. This is the input to the risk + execution pipeline."""

    symbol: Symbol
    side: Side
    size: Size
    price: Price | None = None  # None for market orders
    order_type: OrderType = OrderType.LIMIT
    time_in_force: TimeInForce = TimeInForce.GTC
    strategy_id: StrategyId | None = None
    sub_account: SubAccount | None = None
    reduce_only: bool = False
    cloid: Cloid | None = None  # Pre-assigned, or auto-generated
    client_id: str | None = None  # Additional client tracking ID


@dataclass
class Order:
    """Full order with lifecycle state."""

    cloid: Cloid
    symbol: Symbol
    side: Side
    size: Size
    price: Price | None
    order_type: OrderType
    time_in_force: TimeInForce
    status: OrderStatus = OrderStatus.PENDING
    strategy_id: StrategyId | None = None
    sub_account: SubAccount | None = None
    reduce_only: bool = False
    exchange_oid: OrderId | None = None
    filled_size: Size = field(default_factory=lambda: Size(0.0))
    avg_fill_price: Price | None = None
    submitted_at: datetime | None = None
    acknowledged_at: datetime | None = None
    filled_at: datetime | None = None
    error_message: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def is_terminal(self) -> bool:
        return self.status in {
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }

    @property
    def remaining_size(self) -> Size:
        filled = self.filled_size or Size(0.0)
        return Size(self.size - filled)


@dataclass(frozen=True)
class Fill:
    """A single fill (execution) record."""

    cloid: Cloid
    exchange_oid: OrderId
    symbol: Symbol
    side: Side
    price: Price
    size: Size
    fee: Usd
    is_maker: bool
    timestamp: Timestamp
    strategy_id: StrategyId | None = None
    sub_account: SubAccount | None = None


# --- Account Models ---


@dataclass
class Position:
    """Current position for a symbol."""

    symbol: Symbol
    size: Size  # Positive = long, negative = short
    entry_price: Price | None = None
    mark_price: Price | None = None
    unrealized_pnl: Usd | None = None
    leverage: int = 1
    liquidation_price: Price | None = None
    sub_account: SubAccount | None = None
    strategy_id: StrategyId | None = None

    @property
    def is_long(self) -> bool:
        return self.size > 0

    @property
    def is_short(self) -> bool:
        return self.size < 0

    @property
    def is_flat(self) -> bool:
        return self.size == Size(0.0)


@dataclass
class AccountState:
    """Account balance and state."""

    equity: Usd
    available_balance: Usd
    total_margin_used: Usd
    total_unrealized_pnl: Usd
    peak_equity: Usd  # For drawdown tracking
    sub_account: SubAccount | None = None

    @property
    def drawdown_pct(self) -> float:
        """Current drawdown from peak equity as a fraction (0.0 = at peak)."""
        if self.peak_equity <= 0:
            return 0.0
        return max(0.0, float(1 - self.equity / self.peak_equity))


# --- Risk Models ---


@dataclass(frozen=True)
class RiskCheckResult:
    """Result of a risk check."""

    passed: bool
    reason: str | None = None
    checked_limits: list[str] = field(default_factory=list)


# --- Signal Models ---


@dataclass(frozen=True)
class Signal:
    """Strategy signal output."""

    strategy_id: StrategyId
    symbol: Symbol
    action: str  # "buy", "sell", "close", "cancel_all"
    size: Size | None = None
    price: Price | None = None
    confidence: float | None = None  # 0.0-1.0
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
