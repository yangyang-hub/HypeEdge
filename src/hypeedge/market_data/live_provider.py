"""Live market data provider combining WS + REST + BookManager (Phase 1).

Implements the MarketDataProvider Protocol defined in provider.py.
Subscribes to EventBus events to maintain in-memory trade state,
delegates book queries to BookManager, and backfill to RestClient.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from typing import Any

import structlog

from hypeedge.config.settings import AppSettings
from hypeedge.core.events import (
    EVENT_CANDLE_UPDATE,
    EVENT_FUNDING_UPDATE,
    EVENT_MID_PRICE_UPDATE,
    EVENT_TRADE_UPDATE,
    Event,
    EventBus,
)
from hypeedge.core.models import Candle, FundingRate, L2BookSnapshot, Trade
from hypeedge.core.types import Symbol, Timestamp
from hypeedge.market_data.book import BookManager
from hypeedge.market_data.provider import MarketPriceSnapshot
from hypeedge.market_data.rest_client import RestClient

logger = structlog.get_logger(__name__)


class LiveMarketDataProvider:
    """Concrete MarketDataProvider backed by live WS feed + REST backfill.

    Provides synchronous (zero-latency) access to current market state
    for strategies, and async methods for historical data retrieval.
    """

    def __init__(
        self,
        settings: AppSettings,
        event_bus: EventBus,
        rest_client: RestClient,
        book_manager: BookManager,
    ) -> None:
        self._settings = settings
        self._event_bus = event_bus
        self._rest_client = rest_client
        self._book_manager = book_manager

        # In-memory latest state per symbol
        self._last_trades: dict[Symbol, Trade] = {}
        self._mid_prices: dict[Symbol, float] = {}
        self._mid_price_timestamps: dict[Symbol, datetime] = {}
        self._mid_price_versions: dict[Symbol, int] = {}
        self._mid_price_connection_generations: dict[Symbol, int] = {}
        self._mid_price_exchange_timestamps: dict[Symbol, Timestamp | None] = {}
        self._funding: dict[Symbol, FundingRate] = {}
        self._candles: dict[tuple[Symbol, str], list[Candle]] = {}
        self._candle_backfill_locks: dict[tuple[Symbol, str], asyncio.Lock] = {}
        self._max_candles_per_series = 1_500

        self._running = False
        self._subscriber_task: asyncio.Task[None] | None = None
        self._subscriptions: list[tuple[str, asyncio.Queue[Event]]] = []

    async def start(self) -> None:
        """Subscribe to events and begin tracking market state."""
        self._running = True

        self._subscriptions = [
            (EVENT_TRADE_UPDATE, self._event_bus.subscribe(EVENT_TRADE_UPDATE)),
            (EVENT_MID_PRICE_UPDATE, self._event_bus.subscribe(EVENT_MID_PRICE_UPDATE)),
            (EVENT_FUNDING_UPDATE, self._event_bus.subscribe(EVENT_FUNDING_UPDATE)),
            (EVENT_CANDLE_UPDATE, self._event_bus.subscribe(EVENT_CANDLE_UPDATE)),
        ]

        self._subscriber_task = asyncio.create_task(
            self._consume_events([queue for _, queue in self._subscriptions]),
            name="market_data_provider",
        )
        logger.info("live_market_data_provider_started")

    async def stop(self) -> None:
        """Stop consuming events."""
        self._running = False
        if self._subscriber_task:
            self._subscriber_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._subscriber_task
            self._subscriber_task = None
        for event_type, queue in self._subscriptions:
            self._event_bus.unsubscribe(event_type, queue)
        self._subscriptions.clear()
        logger.info("live_market_data_provider_stopped")

    # --- MarketDataProvider interface ---

    def get_book(self, symbol: Symbol) -> L2BookSnapshot | None:
        """Get the latest L2 order book snapshot."""
        return self._book_manager.get_snapshot(symbol)

    def get_mid_price(self, symbol: Symbol) -> float | None:
        """Get the latest mid price for a symbol.

        Prefers the allMids WS price (most accurate). Falls back to
        (best_bid + best_ask) / 2 from the order book.
        """
        mid = self._mid_prices.get(symbol)
        if mid is not None:
            return mid
        return self._book_manager.get_mid_price(symbol)

    def get_price_snapshot(self, symbol: Symbol) -> MarketPriceSnapshot | None:
        mid = self._mid_prices.get(symbol)
        observed_at = self._mid_price_timestamps.get(symbol)
        if mid is not None and observed_at is not None:
            return MarketPriceSnapshot(
                mid,
                observed_at,
                exchange_ts=self._mid_price_exchange_timestamps.get(symbol),
                version=self._mid_price_versions.get(symbol, 0),
                connection_generation=self._mid_price_connection_generations.get(symbol, 0),
            )
        book = self._book_manager.get_snapshot(symbol)
        book_mid = self._book_manager.get_mid_price(symbol)
        if book is None or book_mid is None:
            return None
        return MarketPriceSnapshot(
            book_mid,
            book.received_at,
            exchange_ts=book.exchange_ts,
            version=book.version,
            connection_generation=book.connection_generation,
        )

    def get_last_trade(self, symbol: Symbol) -> Trade | None:
        """Get the most recent trade for a symbol."""
        return self._last_trades.get(symbol)

    def get_funding(self, symbol: Symbol) -> FundingRate | None:
        """Get the latest normalized funding/mark/open-interest snapshot."""
        return self._funding.get(symbol)

    def get_candles(self, symbol: Symbol, interval: str, limit: int = 300) -> list[Candle]:
        """Get recent candles in ascending timestamp order."""
        series = self._candles.get((symbol, interval), [])
        return list(series[-limit:])

    async def ensure_candles(
        self,
        symbol: Symbol,
        interval: str,
        limit: int,
        start_ms: int,
        end_ms: int,
    ) -> list[Candle]:
        """Warm a candle series once, coalescing concurrent API requests."""
        key = (symbol, interval)
        lock = self._candle_backfill_locks.setdefault(key, asyncio.Lock())
        async with lock:
            cached = self.get_candles(symbol, interval, limit)
            if len(cached) >= limit:
                return cached
            history = await self.backfill_candles(symbol, interval, start_ms, end_ms)
            for candle in history:
                self._handle_candle(candle)
            return self.get_candles(symbol, interval, limit)

    async def backfill_candles(self, symbol: Symbol, interval: str, start_ms: int, end_ms: int) -> list[Candle]:
        """Backfill historical candle data via REST."""
        return await self._rest_client.backfill_candles(
            coin=str(symbol),
            interval=interval,
            start_time=start_ms,
            end_time=end_ms,
        )

    async def backfill_funding(self, symbol: Symbol, start_ms: int, end_ms: int) -> list[FundingRate]:
        """Backfill historical funding rate data via REST."""
        return await self._rest_client.backfill_funding(
            coin=str(symbol),
            start_time=start_ms,
            end_time=end_ms,
        )

    # --- Internal event consumption ---

    async def _consume_events(
        self,
        queues: list[asyncio.Queue[Event]],
    ) -> None:
        """Consume trade and mid-price events to maintain latest state."""
        readers = {
            asyncio.create_task(queue.get(), name=f"provider_reader_{index}"): queue
            for index, queue in enumerate(queues)
        }

        try:
            while self._running:
                done, _ = await asyncio.wait(
                    readers,
                    timeout=1.0,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in done:
                    queue = readers.pop(task)
                    try:
                        event: Event = task.result()
                        self._handle_event(event)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception("provider_event_error")

                    readers[asyncio.create_task(queue.get(), name=task.get_name())] = queue

        except asyncio.CancelledError:
            logger.debug("live_provider_cancelled")
        finally:
            for task in readers:
                task.cancel()
            await asyncio.gather(*readers, return_exceptions=True)

    def _handle_event(self, event: Event) -> None:
        """Route an event to the appropriate handler."""
        if event.event_type == EVENT_TRADE_UPDATE:
            self._handle_trade(event.payload)
        elif event.event_type == EVENT_MID_PRICE_UPDATE:
            self._handle_mid_price(event.payload, event.timestamp)
        elif event.event_type == EVENT_FUNDING_UPDATE:
            self._handle_funding(event.payload)
        elif event.event_type == EVENT_CANDLE_UPDATE:
            self._handle_candle(event.payload)

    def _handle_trade(self, payload: Any) -> None:
        """Update latest trade state."""
        if isinstance(payload, Trade):
            self._last_trades[payload.symbol] = payload

    def _handle_mid_price(self, payload: Any, event_received_at: datetime | None = None) -> None:
        """Update mid price from allMids."""
        if isinstance(payload, dict):
            symbol = payload.get("symbol")
            price = payload.get("price")
            if symbol is not None and price is not None:
                self._mid_prices[symbol] = float(price)
                received_at = payload.get("received_at", event_received_at)
                self._mid_price_timestamps[symbol] = (
                    received_at if isinstance(received_at, datetime) else datetime.now(UTC)
                )
                self._mid_price_versions[symbol] = self._mid_price_versions.get(symbol, 0) + 1
                generation = payload.get("connection_generation", 0)
                self._mid_price_connection_generations[symbol] = generation if isinstance(generation, int) else 0
                exchange_ts = payload.get("exchange_ts")
                self._mid_price_exchange_timestamps[symbol] = (
                    Timestamp(int(exchange_ts)) if isinstance(exchange_ts, (int, float)) else None
                )

    def _handle_funding(self, payload: Any) -> None:
        if isinstance(payload, FundingRate):
            self._funding[payload.symbol] = payload

    def _handle_candle(self, payload: Any) -> None:
        if not isinstance(payload, Candle):
            return
        key = (payload.symbol, payload.interval)
        series = self._candles.setdefault(key, [])
        if series and series[-1].timestamp == payload.timestamp:
            series[-1] = payload
        elif not series or payload.timestamp > series[-1].timestamp:
            series.append(payload)
        else:
            by_timestamp = {candle.timestamp: candle for candle in series}
            by_timestamp[payload.timestamp] = payload
            series[:] = sorted(by_timestamp.values(), key=lambda candle: candle.timestamp)
        if len(series) > self._max_candles_per_series:
            del series[: len(series) - self._max_candles_per_series]
