"""Market data provider protocol (interface definition)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from hypeedge.core.models import Candle, FundingRate, L2BookSnapshot, Trade
from hypeedge.core.types import Symbol, Timestamp


@dataclass(frozen=True)
class MarketPriceSnapshot:
    price: float
    observed_at: datetime
    exchange_ts: Timestamp | None = None
    version: int = 0
    connection_generation: int = 0

    @property
    def received_at(self) -> datetime:
        """Local observation time used for freshness checks."""
        return self.observed_at


class MarketDataProvider(Protocol):
    """Interface for market data access. Implementations provide real-time
    and historical market data to the rest of the system."""

    def get_book(self, symbol: Symbol) -> L2BookSnapshot | None:
        """Get the latest L2 order book snapshot for a symbol."""
        ...

    def get_mid_price(self, symbol: Symbol) -> float | None:
        """Get the latest mid price for a symbol."""
        ...

    def get_price_snapshot(self, symbol: Symbol) -> MarketPriceSnapshot | None:
        """Return a normalized mid/mark price and its actual observation time."""
        ...

    def get_last_trade(self, symbol: Symbol) -> Trade | None:
        """Get the most recent trade for a symbol."""
        ...

    def get_funding(self, symbol: Symbol) -> FundingRate | None:
        """Get the latest normalized funding snapshot for a symbol."""
        ...

    def get_candles(self, symbol: Symbol, interval: str, limit: int = 300) -> list[Candle]:
        """Get recent in-memory candles in timestamp order."""
        ...

    async def ensure_candles(
        self,
        symbol: Symbol,
        interval: str,
        limit: int,
        start_ms: int,
        end_ms: int,
    ) -> list[Candle]:
        """Return a warm recent series, coalescing any required backfill."""
        ...

    async def backfill_candles(self, symbol: Symbol, interval: str, start_ms: int, end_ms: int) -> list[Candle]:
        """Backfill historical candle data via REST."""
        ...

    async def backfill_funding(self, symbol: Symbol, start_ms: int, end_ms: int) -> list[FundingRate]:
        """Backfill historical funding rate data via REST."""
        ...
