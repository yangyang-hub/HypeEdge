"""Rate-limited discovery of liquid Hyperliquid spot/perpetual hedge markets."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

import structlog

from hypeedge.config.settings import FundingArbSettings
from hypeedge.core.exceptions import MarketDataError
from hypeedge.core.models import L2BookSnapshot, L2Level
from hypeedge.core.types import Price, Size, Symbol, Timestamp
from hypeedge.market_data.book import BookManager
from hypeedge.market_data.instrument_cache import InstrumentMetaCache
from hypeedge.market_data.rest_client import RestClient

logger = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class FundingArbMarketSnapshot:
    """One exact USDC spot/perpetual candidate with executable books."""

    perp_symbol: Symbol
    spot_symbol: Symbol
    spot_display: str
    funding_rate: Decimal
    perp_24h_volume_usd: Decimal
    spot_24h_volume_usd: Decimal
    perp_book: L2BookSnapshot
    spot_book: L2BookSnapshot


class FundingArbMarketScanner(Protocol):
    """Async market-discovery boundary consumed by the funding-arbitrage runtime."""

    async def scan(self) -> tuple[FundingArbMarketSnapshot, ...]: ...

    async def get_market(
        self,
        perp_symbol: Symbol,
        spot_symbol: Symbol,
    ) -> FundingArbMarketSnapshot | None: ...


@dataclass(frozen=True, slots=True)
class _MarketDescriptor:
    perp_symbol: Symbol
    spot_symbol: Symbol
    spot_display: str
    funding_rate: Decimal
    perp_24h_volume_usd: Decimal
    spot_24h_volume_usd: Decimal

    @property
    def liquidity_volume_usd(self) -> Decimal:
        return min(self.perp_24h_volume_usd, self.spot_24h_volume_usd)


@dataclass(frozen=True, slots=True)
class _CachedBooks:
    loaded_at: float
    perp_book: L2BookSnapshot
    spot_book: L2BookSnapshot


class HyperliquidFundingArbMarketScanner:
    """Discover exact token pairs, then fetch books only for top-volume candidates."""

    def __init__(
        self,
        rest_client: RestClient,
        metadata: InstrumentMetaCache,
        books: BookManager,
        settings: FundingArbSettings,
    ) -> None:
        self._rest_client = rest_client
        self._metadata = metadata
        self._books = books
        self._settings = settings
        self._universe: tuple[_MarketDescriptor, ...] = ()
        self._universe_loaded_at: float | None = None
        self._book_cache: dict[tuple[Symbol, Symbol], _CachedBooks] = {}
        self._lock = asyncio.Lock()

    async def scan(self) -> tuple[FundingArbMarketSnapshot, ...]:
        """Return executable books for the highest-volume common markets."""
        async with self._lock:
            await self._refresh_universe_if_needed()
            descriptors = self._universe[: self._settings.max_candidate_markets]
            markets = await asyncio.gather(
                *(self._snapshot(descriptor) for descriptor in descriptors),
                return_exceptions=True,
            )
        snapshots: list[FundingArbMarketSnapshot] = []
        failure_count = 0
        for descriptor, result in zip(descriptors, markets, strict=True):
            if isinstance(result, BaseException):
                failure_count += 1
                logger.warning(
                    "funding_arb_candidate_book_failed",
                    perp_symbol=str(descriptor.perp_symbol),
                    spot_symbol=str(descriptor.spot_symbol),
                    error=str(result),
                )
            elif result is not None:
                snapshots.append(result)
        if descriptors and failure_count == len(descriptors):
            raise MarketDataError("all funding-arbitrage candidate book requests failed")
        return tuple(snapshots)

    async def get_market(
        self,
        perp_symbol: Symbol,
        spot_symbol: Symbol,
    ) -> FundingArbMarketSnapshot | None:
        """Refresh one selected/active market even if it falls outside the top-N scan."""
        async with self._lock:
            await self._refresh_universe_if_needed()
            descriptor = next(
                (
                    item
                    for item in self._universe
                    if item.perp_symbol == perp_symbol and item.spot_symbol == spot_symbol
                ),
                None,
            )
            if descriptor is None:
                return None
            return await self._snapshot(descriptor)

    async def _refresh_universe_if_needed(self) -> None:
        now = asyncio.get_running_loop().time()
        if (
            self._universe_loaded_at is not None
            and now - self._universe_loaded_at < self._settings.universe_refresh_seconds
        ):
            return
        perp_payload, spot_payload = await asyncio.gather(
            self._rest_client.get_meta_and_asset_ctxs(),
            self._rest_client.get_spot_meta_and_asset_ctxs(),
        )
        descriptors = self._parse_universe(perp_payload, spot_payload)
        self._universe = tuple(
            sorted(
                descriptors,
                key=lambda item: (item.liquidity_volume_usd, item.spot_24h_volume_usd, str(item.perp_symbol)),
                reverse=True,
            )
        )
        self._universe_loaded_at = now
        logger.info("funding_arb_universe_refreshed", common_markets=len(self._universe))

    def _parse_universe(self, perp_payload: list[Any], spot_payload: list[Any]) -> list[_MarketDescriptor]:
        perp_meta, perp_contexts = self._payload_parts(perp_payload, "perpetual")
        spot_meta, spot_contexts = self._payload_parts(spot_payload, "spot")
        perp_assets = perp_meta.get("universe")
        spot_assets = spot_meta.get("universe")
        tokens = spot_meta.get("tokens")
        if not isinstance(perp_assets, list) or not isinstance(spot_assets, list) or not isinstance(tokens, list):
            raise MarketDataError("funding_arb_market_metadata_invalid")

        perp_by_name: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
        for asset, context in zip(perp_assets, perp_contexts, strict=False):
            if not isinstance(asset, dict) or not isinstance(context, dict):
                continue
            name = str(asset.get("name", "")).strip()
            if name:
                perp_by_name[name] = (asset, context)

        token_by_index = {
            int(token["index"]): token
            for token in tokens
            if isinstance(token, dict) and token.get("index") is not None and token.get("name")
        }
        spot_context_by_coin = {
            str(context["coin"]): context
            for context in spot_contexts
            if isinstance(context, dict) and context.get("coin")
        }
        descriptors: list[_MarketDescriptor] = []
        for index, asset in enumerate(spot_assets):
            if not isinstance(asset, dict):
                continue
            raw_tokens = asset.get("tokens")
            spot_name = str(asset.get("name", "")).strip()
            if not spot_name or not isinstance(raw_tokens, list) or len(raw_tokens) != 2:
                continue
            base = token_by_index.get(int(raw_tokens[0]))
            quote = token_by_index.get(int(raw_tokens[1]))
            if base is None or quote is None or str(quote.get("name")) != "USDC":
                continue
            base_name = str(base.get("name", "")).strip()
            perp_pair = perp_by_name.get(base_name)
            if not base_name or perp_pair is None:
                continue
            spot_info = self._metadata.resolve_spot(spot_name)
            perp_info = self._metadata.get(Symbol(base_name))
            if (
                spot_info is None
                or perp_info is None
                or not spot_info.is_spot
                or perp_info.is_spot
                or spot_info.base_token != base_name
                or spot_info.quote_token != "USDC"
            ):
                continue
            context = spot_context_by_coin.get(spot_name)
            if context is None and index < len(spot_contexts) and isinstance(spot_contexts[index], dict):
                context = spot_contexts[index]
            if context is None:
                continue
            perp_context = perp_pair[1]
            funding_rate = self._decimal(perp_context.get("funding"))
            perp_volume = self._decimal(perp_context.get("dayNtlVlm"))
            spot_volume = self._decimal(context.get("dayNtlVlm"))
            if funding_rate is None or perp_volume is None or spot_volume is None:
                continue
            descriptors.append(
                _MarketDescriptor(
                    perp_symbol=perp_info.symbol,
                    spot_symbol=spot_info.symbol,
                    spot_display=spot_info.display_name,
                    funding_rate=funding_rate,
                    perp_24h_volume_usd=max(Decimal(0), perp_volume),
                    spot_24h_volume_usd=max(Decimal(0), spot_volume),
                )
            )
        return descriptors

    async def _snapshot(self, descriptor: _MarketDescriptor) -> FundingArbMarketSnapshot:
        key = (descriptor.perp_symbol, descriptor.spot_symbol)
        now = asyncio.get_running_loop().time()
        cached = self._book_cache.get(key)
        if cached is None or now - cached.loaded_at >= self._settings.book_refresh_seconds:
            perp_raw, spot_raw = await asyncio.gather(
                self._rest_client.get_l2_book(str(descriptor.perp_symbol)),
                self._rest_client.get_l2_book(str(descriptor.spot_symbol)),
            )
            received_at = datetime.now(UTC)
            perp_book = self._books.apply_snapshot(self._parse_book(perp_raw, descriptor.perp_symbol, received_at))
            spot_book = self._books.apply_snapshot(self._parse_book(spot_raw, descriptor.spot_symbol, received_at))
            cached = _CachedBooks(now, perp_book, spot_book)
            self._book_cache[key] = cached
        return FundingArbMarketSnapshot(
            perp_symbol=descriptor.perp_symbol,
            spot_symbol=descriptor.spot_symbol,
            spot_display=descriptor.spot_display,
            funding_rate=descriptor.funding_rate,
            perp_24h_volume_usd=descriptor.perp_24h_volume_usd,
            spot_24h_volume_usd=descriptor.spot_24h_volume_usd,
            perp_book=cached.perp_book,
            spot_book=cached.spot_book,
        )

    @staticmethod
    def _payload_parts(payload: list[Any], label: str) -> tuple[dict[str, Any], list[Any]]:
        if (
            not isinstance(payload, list)
            or len(payload) != 2
            or not isinstance(payload[0], dict)
            or not isinstance(payload[1], list)
        ):
            raise MarketDataError(f"funding_arb_{label}_contexts_invalid")
        return payload[0], payload[1]

    @classmethod
    def _parse_book(cls, payload: dict[str, Any], symbol: Symbol, received_at: datetime) -> L2BookSnapshot:
        levels = payload.get("levels")
        if not isinstance(levels, list):
            levels = []
        bids = cls._levels(levels[0] if len(levels) > 0 else [])
        asks = cls._levels(levels[1] if len(levels) > 1 else [])
        raw_timestamp = payload.get("time")
        timestamp = int(raw_timestamp) if raw_timestamp is not None else int(received_at.timestamp() * 1000)
        return L2BookSnapshot(
            symbol=symbol,
            bids=bids,
            asks=asks,
            timestamp=Timestamp(timestamp),
            local_ts=received_at,
        )

    @classmethod
    def _levels(cls, values: Any) -> tuple[L2Level, ...]:  # noqa: ANN401
        if not isinstance(values, list):
            return ()
        levels: list[L2Level] = []
        for value in values:
            if not isinstance(value, dict):
                continue
            price = cls._decimal(value.get("px"))
            size = cls._decimal(value.get("sz"))
            if price is None or size is None or price <= 0 or size <= 0:
                continue
            levels.append(L2Level(Price(price), Size(size)))
        return tuple(levels)

    @staticmethod
    def _decimal(value: Any) -> Decimal | None:  # noqa: ANN401
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None
        return parsed if parsed.is_finite() else None


__all__ = [
    "FundingArbMarketScanner",
    "FundingArbMarketSnapshot",
    "HyperliquidFundingArbMarketScanner",
]
