"""WebSocket feed for Hyperliquid market data (Phase 1 implementation)."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime
from typing import Any

import structlog
import websockets

from hypeedge.config.settings import AppSettings
from hypeedge.core.events import (
    EVENT_CANDLE_UPDATE,
    EVENT_FUNDING_UPDATE,
    EVENT_L2_BOOK_UPDATE,
    EVENT_MID_PRICE_UPDATE,
    EVENT_TRADE_UPDATE,
    EVENT_WS_CONNECTED,
    EVENT_WS_DISCONNECTED,
    Event,
    EventBus,
)
from hypeedge.core.models import Candle, FundingRate, Trade
from hypeedge.core.types import Price, Size, Symbol, Timestamp
from hypeedge.market_data.book import BookManager

logger = structlog.get_logger(__name__)


class WebSocketFeed:
    """Manages WebSocket connections to Hyperliquid for market data.

    Subscribes to configured channels (l2Book, trades, candle, allMids)
    and publishes parsed events to the EventBus.

    Features:
    - Automatic reconnection with exponential backoff
    - Per-symbol order book tracking via BookManager
    - Latency measurement (exchange ts vs local ts)
    """

    def __init__(self, settings: AppSettings, event_bus: EventBus) -> None:
        self._settings = settings
        self._event_bus = event_bus
        self._ws_url = settings.exchange.ws_url
        self._coins = [Symbol(c) for c in settings.market_data.coins]
        self._channels = settings.market_data.ws_subscriptions
        self._candle_intervals = settings.market_data.candle_intervals
        self._book_manager = BookManager(depth=settings.market_data.l2_book_depth)

        self._running = False
        self._ws: Any | None = None
        self._reconnect_delay = settings.market_data.ws_reconnect_delay_min
        self._max_reconnect_delay = settings.market_data.ws_reconnect_delay_max
        self._message_count = 0
        self._last_message_ts: float = 0.0
        self._connection_generation = 0

    async def run(self) -> None:
        """Main loop: connect, subscribe, receive messages, reconnect on failure."""
        self._running = True

        while self._running:
            try:
                async with websockets.connect(
                    self._ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    self._connection_generation += 1
                    self._reconnect_delay = self._settings.market_data.ws_reconnect_delay_min

                    logger.info(
                        "ws_connected",
                        url=self._ws_url,
                        connection_generation=self._connection_generation,
                    )
                    self._event_bus.publish_sync(
                        Event(
                            event_type=EVENT_WS_CONNECTED,
                            payload={
                                "url": self._ws_url,
                                "connection_generation": self._connection_generation,
                            },
                        )
                    )

                    await self._subscribe_all(ws)

                    async for raw_message in ws:
                        if not self._running:
                            break
                        await self._handle_message(raw_message)

            except (
                websockets.ConnectionClosed,
                websockets.InvalidHandshake,
                OSError,
            ) as e:
                if not self._running:
                    break

                logger.warning(
                    "ws_disconnected",
                    error=str(e),
                    reconnect_in=self._reconnect_delay,
                )
                self._event_bus.publish_sync(Event(event_type=EVENT_WS_DISCONNECTED, payload={"error": str(e)}))

                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)

            except Exception:
                logger.exception("ws_unexpected_error")
                if not self._running:
                    break
                await asyncio.sleep(self._reconnect_delay)

        logger.info("ws_feed_stopped", messages_processed=self._message_count)

    async def stop(self) -> None:
        """Gracefully stop the WebSocket feed."""
        self._running = False
        if self._ws:
            await self._ws.close()
        logger.info("ws_feed_stopping")

    async def _subscribe_all(self, ws: Any) -> None:
        """Subscribe to all configured channels using Hyperliquid's channel-specific schemas."""
        for subscription in self._build_subscriptions():
            await ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))
            logger.debug("ws_subscribed", subscription=subscription)
            await asyncio.sleep(0.05)

    def _build_subscriptions(self) -> list[dict[str, str]]:
        """Build Hyperliquid WebSocket subscription payloads.

        Channel schemas differ: allMids is global, candle requires an interval,
        and most other market-data channels are per coin.
        """
        subscriptions: list[dict[str, str]] = []

        for channel in self._channels:
            if channel == "allMids":
                subscriptions.append({"type": "allMids"})
            elif channel == "candle":
                for coin in self._coins:
                    for interval in self._candle_intervals:
                        subscriptions.append({"type": "candle", "coin": str(coin), "interval": interval})
            elif channel in {"l2Book", "trades", "activeAssetCtx"}:
                for coin in self._coins:
                    subscriptions.append({"type": channel, "coin": str(coin)})
            else:
                logger.warning("ws_subscription_unsupported", channel=channel)

        return subscriptions

    async def _handle_message(self, raw: str | bytes) -> None:
        """Parse and dispatch a WebSocket message."""
        local_ts = datetime.now(UTC)

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("ws_json_decode_error", raw=raw[:200])
            return

        self._message_count += 1
        self._last_message_ts = time.time()

        # Hyperliquid WS messages come as:
        # {"channel": "l2Book", "data": {...}}
        channel = data.get("channel")
        if not channel or channel == "subscriptionResponse":
            return

        try:
            handler = self._HANDLERS.get(channel)
            if handler:
                await handler(self, data.get("data", {}), local_ts)
            else:
                logger.debug("ws_unhandled_channel", channel=channel)
        except Exception:
            logger.exception("ws_message_handler_error", channel=channel)

    async def _handle_l2_book(self, data: dict[str, Any], local_ts: datetime) -> None:
        """Handle L2 order book update."""
        coin = Symbol(data.get("coin", ""))
        if not coin:
            return

        levels = data.get("levels", [[], []])
        bids = self._parse_book_levels(levels[0]) if len(levels) > 0 else []
        asks = self._parse_book_levels(levels[1]) if len(levels) > 1 else []

        ts = Timestamp(int(data.get("time", 0)))

        # Update in-memory book
        book = self._book_manager.get_book(coin)
        snapshot = book.update(
            bids,
            asks,
            ts,
            received_at=local_ts,
            connection_generation=self._connection_generation,
        )

        # Publish event
        self._event_bus.publish_sync(
            Event(
                event_type=EVENT_L2_BOOK_UPDATE,
                payload=snapshot,
                correlation_id=coin,
            )
        )

    @staticmethod
    def _parse_book_levels(levels: Any) -> list[tuple[float, float]]:
        """Parse Hyperliquid L2 levels.

        The live API uses objects such as ``{"px": "100", "sz": "2", "n": 3}``.
        A two-item sequence is accepted as a compatibility fallback for recorded fixtures.
        Invalid levels are ignored without discarding the entire book snapshot.
        """
        if not isinstance(levels, list):
            return []

        parsed: list[tuple[float, float]] = []
        for level in levels:
            try:
                if isinstance(level, dict):
                    px = level["px"]
                    sz = level["sz"]
                elif isinstance(level, (list, tuple)) and len(level) >= 2:
                    px, sz = level[0], level[1]
                else:
                    continue
                parsed.append((float(px), float(sz)))
            except (KeyError, TypeError, ValueError):
                logger.warning("ws_invalid_l2_level", level=level)
        return parsed

    async def _handle_trades(self, data: dict[str, Any], local_ts: datetime) -> None:
        """Handle trade update."""
        # Trades come as a list
        trades_raw = data if isinstance(data, list) else data.get("trades", [])
        if not isinstance(trades_raw, list):
            trades_raw = [trades_raw]

        for t in trades_raw:
            coin = Symbol(t.get("coin", ""))
            if not coin:
                continue

            side_str = t.get("side", "")
            from hypeedge.core.enums import Side

            trade = Trade(
                symbol=coin,
                price=Price(t.get("px", 0)),
                size=Size(t.get("sz", 0)),
                side=Side.BUY if side_str == "B" else Side.SELL,
                tid=int(t.get("tid", 0)),
                timestamp=Timestamp(int(t.get("time", 0))),
                local_ts=local_ts,
            )
            self._event_bus.publish_sync(Event(event_type=EVENT_TRADE_UPDATE, payload=trade, correlation_id=coin))

    async def _handle_candle(self, data: dict[str, Any], local_ts: datetime) -> None:
        """Handle candle update."""
        coin = Symbol(data.get("s", ""))
        if not coin:
            return

        candle = Candle(
            symbol=coin,
            interval=data.get("i", ""),
            open=Price(data.get("o", 0)),
            high=Price(data.get("h", 0)),
            low=Price(data.get("l", 0)),
            close=Price(data.get("c", 0)),
            volume=Size(data.get("v", 0)),
            timestamp=Timestamp(int(data.get("t", 0))),
        )
        self._event_bus.publish_sync(Event(event_type=EVENT_CANDLE_UPDATE, payload=candle, correlation_id=coin))

    async def _handle_all_mids(self, data: dict[str, Any], local_ts: datetime) -> None:
        """Handle allMids update (mark prices for all assets)."""
        # allMids comes as { "coin": "price_str", ... }
        mids = data.get("mids", data)
        if not isinstance(mids, dict):
            return

        for coin_str, price_str in mids.items():
            try:
                symbol = Symbol(coin_str)
                price = Price(price_str)
                self._event_bus.publish_sync(
                    Event(
                        event_type=EVENT_MID_PRICE_UPDATE,
                        payload={
                            "symbol": symbol,
                            "price": price,
                            "received_at": local_ts,
                            "connection_generation": self._connection_generation,
                        },
                        correlation_id=symbol,
                    )
                )
            except (ValueError, TypeError):
                logger.warning("mid_price_parse_error", coin=coin_str, price=price_str)

    async def _handle_active_asset_ctx(self, data: dict[str, Any], local_ts: datetime) -> None:
        """Handle activeAssetCtx updates for funding, mark price, and open interest."""
        coin = Symbol(data.get("coin", ""))
        ctx = data.get("ctx", data)
        if not coin or not isinstance(ctx, dict):
            return

        mark_price_raw = ctx.get("markPx") or ctx.get("midPx") or 0
        open_interest_raw = ctx.get("openInterest") or ctx.get("openInterestUsd") or 0
        funding_raw = ctx.get("funding") or ctx.get("fundingRate") or 0
        funding = FundingRate(
            symbol=coin,
            funding_rate=float(funding_raw),
            premium=float(ctx.get("premium", 0)),
            mark_price=Price(mark_price_raw),
            open_interest=float(open_interest_raw),
            timestamp=Timestamp(int(ctx.get("time", int(local_ts.timestamp() * 1000)))),
        )
        self._event_bus.publish_sync(Event(event_type=EVENT_FUNDING_UPDATE, payload=funding, correlation_id=coin))

    # Dispatch table
    _HANDLERS: dict[str, Any] = {
        "l2Book": _handle_l2_book,
        "trades": _handle_trades,
        "candle": _handle_candle,
        "allMids": _handle_all_mids,
        "activeAssetCtx": _handle_active_asset_ctx,
    }

    @property
    def book_manager(self) -> BookManager:
        """Access the book manager for strategy reads."""
        return self._book_manager

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "message_count": self._message_count,
            "last_message_ts": self._last_message_ts,
            "is_connected": self._ws is not None,
            "tracked_symbols": [str(s) for s in self._book_manager.symbols],
        }
