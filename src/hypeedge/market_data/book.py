"""In-memory L2 order book manager."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

import structlog

from hypeedge.core.models import L2BookSnapshot, L2Level
from hypeedge.core.types import Price, Size, Symbol, Timestamp

logger = structlog.get_logger(__name__)


class OrderBook:
    """Maintains an in-memory L2 order book for a single symbol.

    Updated from WebSocket feed, readable by strategies with zero latency.
    Thread-safe for single-event-loop use (asyncio).
    """

    def __init__(self, symbol: Symbol, depth: int = 20) -> None:
        self.symbol = symbol
        self._depth = depth
        self._bids: list[L2Level] = []  # Sorted: best (highest) first
        self._asks: list[L2Level] = []  # Sorted: best (lowest) first
        self._last_update_ts: Timestamp | None = None
        self._version = 0
        self._snapshot: L2BookSnapshot | None = None

    def update(
        self,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
        ts: Timestamp,
        *,
        received_at: datetime | None = None,
        connection_generation: int = 0,
    ) -> L2BookSnapshot:
        """Update the book with a full snapshot from WebSocket.

        Args:
            bids: List of (price, size) tuples, best first
            asks: List of (price, size) tuples, best first
            ts: Exchange timestamp
            received_at: Local time at the WebSocket receive boundary
            connection_generation: Monotonic WebSocket reconnection generation
        """
        self._bids = [L2Level(price=Price(px), size=Size(sz)) for px, sz in bids[: self._depth]]
        self._asks = [L2Level(price=Price(px), size=Size(sz)) for px, sz in asks[: self._depth]]
        self._last_update_ts = ts
        self._version += 1
        self._snapshot = L2BookSnapshot(
            symbol=self.symbol,
            bids=tuple(self._bids),
            asks=tuple(self._asks),
            timestamp=ts,
            local_ts=received_at or datetime.now(UTC),
            version=self._version,
            connection_generation=connection_generation,
        )
        return self._snapshot

    def get_snapshot(self) -> L2BookSnapshot | None:
        """Return the snapshot created at update time without refreshing freshness."""
        return self._snapshot

    @property
    def best_bid(self) -> Price | None:
        """Best bid price."""
        return self._bids[0].price if self._bids else None

    @property
    def best_ask(self) -> Price | None:
        """Best ask price."""
        return self._asks[0].price if self._asks else None

    @property
    def mid_price(self) -> float | None:
        """Mid price = (best_bid + best_ask) / 2."""
        if self.best_bid is not None and self.best_ask is not None:
            return float((self.best_bid + self.best_ask) / 2)
        return None

    @property
    def spread(self) -> float | None:
        """Bid-ask spread."""
        if self.best_bid is not None and self.best_ask is not None:
            return float(self.best_ask - self.best_bid)
        return None

    @property
    def spread_bps(self) -> float | None:
        """Spread in basis points."""
        mid = self.mid_price
        sp = self.spread
        if mid and sp and mid > 0:
            return (sp / mid) * 10_000
        return None

    @property
    def last_update_ts(self) -> Timestamp | None:
        return self._last_update_ts

    def __repr__(self) -> str:
        bid = self.best_bid
        ask = self.best_ask
        return f"OrderBook({self.symbol}: bid={bid} ask={ask})"


class BookManager:
    """Manages order books for multiple symbols."""

    def __init__(self, depth: int = 20) -> None:
        self._depth = depth
        self._books: dict[Symbol, OrderBook] = {}

    def get_book(self, symbol: Symbol) -> OrderBook:
        """Get or create an order book for a symbol."""
        if symbol not in self._books:
            self._books[symbol] = OrderBook(symbol, self._depth)
        return self._books[symbol]

    def get_snapshot(self, symbol: Symbol) -> L2BookSnapshot | None:
        """Get a snapshot for a specific symbol."""
        book = self._books.get(symbol)
        return book.get_snapshot() if book else None

    def get_mid_price(self, symbol: Symbol) -> float | None:
        """Get mid price for a symbol."""
        book = self._books.get(symbol)
        return book.mid_price if book else None

    @property
    def symbols(self) -> list[Symbol]:
        """List of symbols with active books."""
        return list(self._books.keys())

    @property
    def active_books(self) -> Iterator[OrderBook]:
        """Iterate over all active books."""
        return iter(self._books.values())
