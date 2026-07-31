"""Authoritative Hyperliquid perpetual and spot instrument metadata cache."""

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
    """Exchange rules and asset identity for one perpetual or spot market."""

    symbol: Symbol
    sz_decimals: int  # Number of decimal places for size
    max_leverage: int
    tick_size: Decimal  # Smallest decimal increment before the 5-significant-figure rule
    lot_size: Decimal  # Minimum size increment (10^(-sz_decimals))
    min_size: Decimal  # Minimum order size
    display_name: str = ""
    min_notional: Decimal | None = None
    max_price_decimals: int = 6
    max_significant_figures: int = 5
    is_spot: bool = False
    base_token: str | None = None
    quote_token: str | None = None
    only_isolated: bool = False
    margin_mode: str | None = None

    def __post_init__(self) -> None:
        if not self.display_name:
            object.__setattr__(self, "display_name", str(self.symbol))


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
        self._spot_aliases: dict[str, Symbol] = {}
        self._load_lock = asyncio.Lock()
        self._running = False

    @property
    def is_loaded(self) -> bool:
        """Whether metadata has been loaded at least once."""
        return bool(self._instruments)

    def get(self, symbol: Symbol) -> InstrumentInfo | None:
        """Get cached instrument info for a symbol."""
        direct = self._instruments.get(symbol)
        if direct is not None:
            return direct
        resolved = self._spot_aliases.get(str(symbol))
        return self._instruments.get(resolved) if resolved is not None else None

    def resolve_spot(self, market: str | Symbol) -> InstrumentInfo | None:
        """Resolve a display pair or exchange coin (``@N``) to spot metadata."""
        info = self.get(Symbol(str(market).strip()))
        return info if info is not None and info.is_spot else None

    def get_spot(self, market: str | Symbol) -> InstrumentInfo | None:
        """Alias for :meth:`resolve_spot` used by risk/runtime boundaries."""
        return self.resolve_spot(market)

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
            await self._refresh_meta()
            while self._running:
                await asyncio.sleep(self._refresh_interval_s)
                if not self._running:
                    return
                await self._refresh_meta()
        except asyncio.CancelledError:
            logger.debug("instrument_cache_cancelled")
        finally:
            self._running = False

    async def ensure_loaded(self) -> None:
        """Load both metadata sets once, propagating failures to startup gates."""
        if self.is_loaded and self._spot_aliases:
            return
        async with self._load_lock:
            if self.is_loaded and self._spot_aliases:
                return
            await self._fetch_meta()

    async def _refresh_meta(self) -> None:
        try:
            async with self._load_lock:
                await self._fetch_meta()
        except Exception:
            logger.exception("meta_fetch_failed")

    async def _fetch_meta(self) -> None:
        """Fetch and atomically replace perpetual + spot metadata."""
        perp_data, spot_data = await asyncio.gather(
            self._rest_client.get_meta(),
            self._rest_client.get_spot_meta(),
        )
        perp_universe = perp_data.get("universe", [])
        spot_universe = spot_data.get("universe", [])
        tokens = spot_data.get("tokens", [])
        if not isinstance(perp_universe, list) or not perp_universe:
            raise ValueError("meta_empty_universe")
        if not isinstance(spot_universe, list) or not isinstance(tokens, list):
            raise ValueError("invalid_spot_meta_response")

        new_instruments: dict[Symbol, InstrumentInfo] = {}
        for asset in perp_universe:
            if not isinstance(asset, dict):
                continue
            name = str(asset.get("name", "")).strip()
            if not name:
                continue
            symbol = Symbol(name)
            sz_decimals = int(asset.get("szDecimals", 0))
            max_price_decimals = max(0, 6 - sz_decimals)
            lot_size = Decimal(1).scaleb(-sz_decimals)
            new_instruments[symbol] = InstrumentInfo(
                symbol=symbol,
                display_name=name,
                sz_decimals=sz_decimals,
                max_leverage=int(asset.get("maxLeverage", 50)),
                tick_size=Decimal(1).scaleb(-max_price_decimals),
                lot_size=lot_size,
                min_size=lot_size,
                max_price_decimals=max_price_decimals,
                only_isolated=bool(asset.get("onlyIsolated", False)),
                margin_mode=str(asset["marginMode"]) if asset.get("marginMode") else None,
            )

        token_by_index = {
            int(token["index"]): token
            for token in tokens
            if isinstance(token, dict) and token.get("index") is not None and token.get("name")
        }
        aliases: dict[str, Symbol] = {}
        for asset in spot_universe:
            if not isinstance(asset, dict):
                continue
            raw_tokens = asset.get("tokens")
            exchange_name = str(asset.get("name", "")).strip()
            if not exchange_name or not isinstance(raw_tokens, list) or len(raw_tokens) != 2:
                continue
            base = token_by_index.get(int(raw_tokens[0]))
            quote = token_by_index.get(int(raw_tokens[1]))
            if base is None or quote is None:
                continue
            base_name = str(base["name"])
            quote_name = str(quote["name"])
            display_name = f"{base_name}/{quote_name}"
            symbol = Symbol(exchange_name)
            sz_decimals = int(base.get("szDecimals", 0))
            max_price_decimals = max(0, 8 - sz_decimals)
            lot_size = Decimal(1).scaleb(-sz_decimals)
            new_instruments[symbol] = InstrumentInfo(
                symbol=symbol,
                display_name=display_name,
                sz_decimals=sz_decimals,
                max_leverage=1,
                tick_size=Decimal(1).scaleb(-max_price_decimals),
                lot_size=lot_size,
                min_size=lot_size,
                max_price_decimals=max_price_decimals,
                is_spot=True,
                base_token=base_name,
                quote_token=quote_name,
            )
            aliases[exchange_name] = symbol
            existing = aliases.get(display_name)
            if existing is not None and existing != symbol:
                logger.error("spot_display_alias_ambiguous", display_name=display_name)
                aliases.pop(display_name, None)
            else:
                aliases[display_name] = symbol

        self._instruments = new_instruments
        self._spot_aliases = aliases
        logger.info(
            "meta_loaded",
            instruments=len(new_instruments),
            spot_markets=sum(1 for info in new_instruments.values() if info.is_spot),
        )
