"""Async Binance public WebSocket feed for advisory reference prices."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any, Protocol

import structlog
import websockets

from hypeedge.config.settings import AppSettings
from hypeedge.core.events import EVENT_EXTERNAL_REFERENCE_UPDATE, EVENT_L2_BOOK_UPDATE, Event, EventBus
from hypeedge.core.models import L2BookSnapshot
from hypeedge.core.types import Price, Symbol, Timestamp
from hypeedge.market_data.external_reference import (
    ExternalMarket,
    ExternalVenueQuote,
    LatestExternalReferenceProvider,
)

logger = structlog.get_logger(__name__)
RawWebSocketMessage = str | bytes


class BinanceTransport(Protocol):
    """Injectable transport boundary used by deterministic unit tests."""

    def stream(self, url: str) -> AsyncIterator[RawWebSocketMessage]:
        """Yield messages until the connection closes or raises."""
        ...


class WebsocketsBinanceTransport:
    """Production transport backed by the asyncio-native websockets client."""

    async def stream(self, url: str) -> AsyncIterator[RawWebSocketMessage]:
        async with websockets.connect(url, ping_interval=20, ping_timeout=10, close_timeout=5) as websocket:
            async for message in websocket:
                yield message


class BinanceReferenceFeed:
    """Maintains independent spot/perpetual connections with exponential backoff."""

    def __init__(
        self,
        settings: AppSettings,
        event_bus: EventBus,
        provider: LatestExternalReferenceProvider,
        transport: BinanceTransport | None = None,
    ) -> None:
        self._settings = settings.external_reference
        self._event_bus = event_bus
        self._provider = provider
        self._transport = transport or WebsocketsBinanceTransport()
        self._venue_to_symbol = {
            venue_symbol.upper(): Symbol(symbol.upper()) for symbol, venue_symbol in self._settings.symbol_map.items()
        }
        self._running = False
        self._tasks: list[asyncio.Task[None]] = []
        self._generations: dict[str, int] = {"spot": 0, "perpetual": 0}

    async def run(self) -> None:
        """Run both venue streams; external failure is isolated from Hyperliquid."""
        if not self._settings.external_reference_enabled:
            logger.info("binance_reference_disabled")
            return
        self._running = True
        self._tasks = [
            asyncio.create_task(self._run_stream("spot", self._spot_url()), name="binance-reference-spot"),
            asyncio.create_task(
                self._run_stream("perpetual", self._perpetual_url()), name="binance-reference-perpetual"
            ),
            asyncio.create_task(self._consume_hyperliquid_books(), name="binance-reference-hl-basis"),
        ]
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            if self._running:
                raise
        finally:
            self._tasks.clear()
            self._running = False

    async def stop(self) -> None:
        """Stop feed tasks without touching the native Hyperliquid connection."""
        self._running = False
        tasks, self._tasks = self._tasks, []
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_stream(self, stream_type: str, url: str) -> None:
        delay = self._settings.reconnect_delay_min_seconds
        while self._running:
            self._generations[stream_type] += 1
            generation = self._generations[stream_type]
            try:
                logger.info(
                    "binance_reference_connected",
                    stream_type=stream_type,
                    connection_generation=generation,
                )
                async for raw in self._transport.stream(url):
                    if not self._running:
                        return
                    delay = self._settings.reconnect_delay_min_seconds
                    await self._handle_message(raw, stream_type, generation)
                if not self._running:
                    return
                raise ConnectionError("Binance reference stream ended")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "binance_reference_disconnected",
                    stream_type=stream_type,
                    error=str(exc),
                    reconnect_in_seconds=delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._settings.reconnect_delay_max_seconds)

    async def _consume_hyperliquid_books(self) -> None:
        """Calibrate basis from native L2 without blocking its lossy publisher."""
        queue = self._event_bus.subscribe(EVENT_L2_BOOK_UPDATE, maxsize=1)
        try:
            while self._running:
                event = await queue.get()
                book = event.payload
                if not isinstance(book, L2BookSnapshot) or not book.bids or not book.asks:
                    continue
                if book.bids[0].price >= book.asks[0].price:
                    continue
                mid = Price((book.bids[0].price + book.asks[0].price) / 2)
                snapshot = self._provider.update_hyperliquid_mid(book.symbol, mid)
                self._event_bus.publish_sync(
                    Event(
                        event_type=EVENT_EXTERNAL_REFERENCE_UPDATE,
                        payload=snapshot,
                        correlation_id=str(book.symbol),
                    )
                )
        finally:
            self._event_bus.unsubscribe(EVENT_L2_BOOK_UPDATE, queue)

    async def _handle_message(self, raw: RawWebSocketMessage, stream_type: str, generation: int) -> None:
        received_at = datetime.now(UTC)
        try:
            message = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
            logger.warning("binance_reference_json_invalid")
            return
        if not isinstance(message, dict):
            return
        data = message.get("data", message)
        if not isinstance(data, dict):
            return
        quote = self._parse_quote(data, stream_type, generation, received_at)
        if quote is None:
            return
        snapshot = self._provider.update_quote(quote)
        self._event_bus.publish_sync(
            Event(
                event_type=EVENT_EXTERNAL_REFERENCE_UPDATE,
                payload=snapshot,
                correlation_id=str(quote.symbol),
            )
        )

    def _parse_quote(
        self,
        data: dict[str, Any],
        stream_type: str,
        generation: int,
        received_at: datetime,
    ) -> ExternalVenueQuote | None:
        venue_symbol = str(data.get("s", "")).upper()
        symbol = self._venue_to_symbol.get(venue_symbol)
        if symbol is None:
            return None
        received_ms = int(received_at.timestamp() * 1000)
        exchange_ts = Timestamp(int(data.get("E", data.get("T", received_ms))))
        try:
            if data.get("e") == "markPriceUpdate":
                mark_price = Price(data["p"])
                if mark_price <= 0:
                    return None
                return ExternalVenueQuote(
                    symbol=symbol,
                    venue_symbol=venue_symbol,
                    market="perpetual_mark",
                    mark_price=mark_price,
                    exchange_ts=exchange_ts,
                    received_at=received_at,
                    sequence=int(data.get("E", data.get("T", received_ms))),
                    connection_generation=generation,
                )
            bid = Price(data["b"])
            ask = Price(data["a"])
            if bid <= 0 or ask <= 0:
                return None
            market: ExternalMarket = "spot" if stream_type == "spot" else "perpetual"
            return ExternalVenueQuote(
                symbol=symbol,
                venue_symbol=venue_symbol,
                market=market,
                bid=bid,
                ask=ask,
                exchange_ts=exchange_ts,
                received_at=received_at,
                sequence=int(data.get("u", data.get("E", received_ms))),
                connection_generation=generation,
            )
        except (KeyError, TypeError, ValueError, ArithmeticError):
            logger.warning("binance_reference_payload_invalid", stream_type=stream_type, symbol=venue_symbol)
            return None

    def _spot_url(self) -> str:
        streams = [f"{venue.lower()}@bookTicker" for venue in self._venue_to_symbol]
        return self._combined_url(self._settings.spot_ws_url, streams)

    def _perpetual_url(self) -> str:
        streams = [
            stream
            for venue in self._venue_to_symbol
            for stream in (f"{venue.lower()}@bookTicker", f"{venue.lower()}@markPrice@1s")
        ]
        return self._combined_url(self._settings.perpetual_ws_url, streams)

    @staticmethod
    def _combined_url(base_url: str, streams: list[str]) -> str:
        separator = "&" if "?" in base_url else "?"
        return f"{base_url}{separator}streams={'/'.join(streams)}"
