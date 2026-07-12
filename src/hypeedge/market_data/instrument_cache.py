"""Instrument metadata cache for Hyperliquid contract info (Phase 1).

Fetches and caches contract metadata from the Hyperliquid `meta` endpoint,
including price decimals, size decimals, and minimum order sizes.
This data rarely changes and is essential for order construction and display.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal

import structlog

from hypeedge.core.types import Symbol
from hypeedge.market_data.rest_client import RestClient

logger = structlog.get_logger(__name__)

# Refresh interval for metadata (contracts rarely change)
META_REFRESH_INTERVAL_HOURS = 6.0


@dataclass(frozen=True)
class InstrumentInfo:
    """Cached metadata for a single perpetual contract."""

    symbol: Symbol
    sz_decimals: int  # Number of decimal places for size
    max_leverage: int
    tick_size: Decimal  # Minimum price increment
    lot_size: Decimal  # Minimum size increment (10^(-sz_decimals))
    min_size: Decimal  # Minimum order size
    min_notional: Decimal | None = None


class InstrumentMetaCache:
    """In-memory cache of Hyperliquid contract metadata.

    Fetches from the `meta` endpoint on startup and periodically refreshes.
    Provides synchronous access for order construction and price formatting.
    """

    def __init__(
        self,
        rest_client: RestClient,
        *,
        refresh_interval_hours: float = META_REFRESH_INTERVAL_HOURS,
    ) -> None:
        self._rest_client = rest_client
        self._refresh_interval_s = refresh_interval_hours * 3600
        self._instruments: dict[Symbol, InstrumentInfo] = {}
        self._running = False

    @property
    def is_loaded(self) -> bool:
        """Whether metadata has been loaded at least once."""
        return bool(self._instruments)

    def get(self, symbol: Symbol) -> InstrumentInfo | None:
        """Get cached instrument info for a symbol."""
        return self._instruments.get(symbol)

    def get_sz_decimals(self, symbol: Symbol) -> int | None:
        """Get size decimals for a symbol (convenience method)."""
        info = self._instruments.get(symbol)
        return info.sz_decimals if info else None

    def get_tick_size(self, symbol: Symbol) -> Decimal | None:
        """Get tick size for a symbol (convenience method)."""
        info = self._instruments.get(symbol)
        return info.tick_size if info else None

    async def run(self) -> None:
        """Main loop: fetch meta on startup, then refresh periodically."""
        self._running = True
        try:
            await self._fetch_meta()
            while self._running:
                await asyncio.sleep(self._refresh_interval_s)
                if not self._running:
                    return
                await self._fetch_meta()
        except asyncio.CancelledError:
            logger.debug("instrument_cache_cancelled")
        finally:
            self._running = False

    async def _fetch_meta(self) -> None:
        """Fetch and parse contract metadata from the exchange."""
        try:
            data = await self._rest_client.get_meta()
            universe = data.get("universe", [])
            if not universe:
                logger.warning("meta_empty_universe")
                return

            new_instruments: dict[Symbol, InstrumentInfo] = {}
            for asset in universe:
                name = asset.get("name", "")
                if not name:
                    continue

                symbol = Symbol(name)
                sz_decimals = int(asset.get("szDecimals", 0))
                max_leverage = int(asset.get("maxLeverage", 50))
                # Hyperliquid provides individual fields; defaults for safety
                tick_size = Decimal(str(asset.get("tickSize", "0.01") or "0.01"))
                lot_size = Decimal(1).scaleb(-sz_decimals)
                min_size = lot_size

                new_instruments[symbol] = InstrumentInfo(
                    symbol=symbol,
                    sz_decimals=sz_decimals,
                    max_leverage=max_leverage,
                    tick_size=tick_size,
                    lot_size=lot_size,
                    min_size=min_size,
                )

            self._instruments = new_instruments
            logger.info("meta_loaded", instruments=len(new_instruments))

        except Exception:
            logger.exception("meta_fetch_failed")
