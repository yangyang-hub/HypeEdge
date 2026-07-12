"""Public read-only WebSocket stream for normalized market data."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import defaultdict
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from hypeedge.api.schemas import decimal_string
from hypeedge.core.events import (
    EVENT_CANDLE_UPDATE,
    EVENT_FUNDING_UPDATE,
    EVENT_L2_BOOK_UPDATE,
    EVENT_TRADE_UPDATE,
    Event,
)
from hypeedge.core.models import Candle, FundingRate, L2BookSnapshot, Trade
from hypeedge.core.types import Symbol

router = APIRouter(tags=["market-stream"])

_STREAM_EVENTS = (
    EVENT_L2_BOOK_UPDATE,
    EVENT_TRADE_UPDATE,
    EVENT_CANDLE_UPDATE,
    EVENT_FUNDING_UPDATE,
)
_ALLOWED_INTERVALS = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d"}


class MarketWsGuard:
    """Bound public market-stream resource use per process and client IP."""

    def __init__(self, total_limit: int, per_ip_limit: int) -> None:
        self._total_limit = total_limit
        self._per_ip_limit = per_ip_limit
        self._total = 0
        self._by_ip: dict[str, int] = defaultdict(int)

    def acquire(self, client_ip: str) -> bool:
        if self._total >= self._total_limit or self._by_ip[client_ip] >= self._per_ip_limit:
            return False
        self._total += 1
        self._by_ip[client_ip] += 1
        return True

    def release(self, client_ip: str) -> None:
        if self._by_ip.get(client_ip, 0) <= 0:
            return
        self._total = max(0, self._total - 1)
        self._by_ip[client_ip] -= 1
        if self._by_ip[client_ip] == 0:
            self._by_ip.pop(client_ip, None)


class MarketMessageBudget:
    """Assign sequences before throttling so dropped updates are observable."""

    def __init__(self, limit: int, *, sequence: int, now: float, sent: int) -> None:
        self._limit = limit
        self._sequence = sequence
        self._second = int(now)
        self._sent = sent

    def next_event(self, now: float) -> tuple[int, bool]:
        current_second = int(now)
        if current_second != self._second:
            self._second = current_second
            self._sent = 0
        self._sequence += 1
        allowed = self._sent < self._limit
        if allowed:
            self._sent += 1
        return self._sequence, allowed

    def next_heartbeat(self) -> int:
        self._sequence += 1
        return self._sequence


def _guard(app: Any) -> MarketWsGuard:  # noqa: ANN401
    guard = getattr(app, "_market_ws_guard", None)
    if guard is None:
        settings = app.settings.api
        guard = MarketWsGuard(settings.market_ws_max_connections, settings.market_ws_max_connections_per_ip)
        app._market_ws_guard = guard
    return guard


def _symbol(raw: str) -> Symbol | None:
    normalized = raw.strip().upper()
    if not normalized or len(normalized) > 20:
        return None
    if not all(char.isalnum() or char in "_.-" for char in normalized):
        return None
    return Symbol(normalized)


def _serialize(payload: Any) -> tuple[str, str, dict[str, Any]] | None:
    if isinstance(payload, L2BookSnapshot):
        return (
            "book",
            str(payload.symbol),
            {
                "bids": [[decimal_string(level.price), decimal_string(level.size)] for level in payload.bids],
                "asks": [[decimal_string(level.price), decimal_string(level.size)] for level in payload.asks],
                "timestamp": int(payload.timestamp),
                "source": "websocket",
            },
        )
    if isinstance(payload, Trade):
        return (
            "trade",
            str(payload.symbol),
            {
                "price": decimal_string(payload.price),
                "size": decimal_string(payload.size),
                "side": str(payload.side),
                "tid": payload.tid,
                "timestamp": int(payload.timestamp),
            },
        )
    if isinstance(payload, Candle):
        return (
            "candle",
            str(payload.symbol),
            {
                "interval": payload.interval,
                "open": decimal_string(payload.open),
                "high": decimal_string(payload.high),
                "low": decimal_string(payload.low),
                "close": decimal_string(payload.close),
                "volume": decimal_string(payload.volume),
                "timestamp": int(payload.timestamp),
            },
        )
    if isinstance(payload, FundingRate):
        return (
            "funding",
            str(payload.symbol),
            {
                "funding_rate": decimal_string(payload.funding_rate),
                "premium": decimal_string(payload.premium),
                "mark_price": decimal_string(payload.mark_price),
                "open_interest": decimal_string(payload.open_interest),
                "timestamp": int(payload.timestamp),
            },
        )
    return None


def _data(payload: Any) -> dict[str, Any] | None:
    serialized = _serialize(payload)
    return serialized[2] if serialized is not None else None


@router.websocket("/ws/v1/market")
async def market_stream(websocket: WebSocket) -> None:
    """Stream a snapshot followed by sequenced normalized market events.

    This endpoint is intentionally read-only and carries public market data,
    so it never accepts or needs the privileged trading bearer token.
    """
    symbol = _symbol(websocket.query_params.get("symbol", "BTC"))
    interval = websocket.query_params.get("interval", "1m")
    if symbol is None or interval not in _ALLOWED_INTERVALS:
        await websocket.close(code=1008, reason="invalid market subscription")
        return

    app = websocket.app.state.hype_app
    allowed_origins = set(app.settings.api.cors_origins)
    origin = websocket.headers.get("origin")
    if origin is not None and origin not in allowed_origins:
        await websocket.close(code=1008, reason="origin not allowed")
        return
    provider = getattr(app, "_market_data_provider", None)
    if provider is None:
        await websocket.close(code=1013, reason="market data unavailable")
        return

    client_ip = websocket.client.host if websocket.client is not None else "unknown"
    guard = _guard(app)
    if not guard.acquire(client_ip):
        await websocket.close(code=1013, reason="market stream connection limit exceeded")
        return

    subscriptions: list[tuple[str, asyncio.Queue[Event]]] = []
    readers: dict[asyncio.Task[Event], tuple[str, asyncio.Queue[Event]]] = {}
    try:
        await websocket.accept()
        queue_size = app.settings.api.market_ws_queue_size
        subscriptions = [
            (event_type, app.event_bus.subscribe(event_type, maxsize=queue_size)) for event_type in _STREAM_EVENTS
        ]
        readers = {
            asyncio.create_task(queue.get(), name=f"market_ws_{event_type}"): (event_type, queue)
            for event_type, queue in subscriptions
        }
        sequence = 1
        book = provider.get_book(symbol)
        funding = provider.get_funding(symbol)
        trade = provider.get_last_trade(symbol)
        candles = provider.get_candles(symbol, interval, 300)
        snapshot_cutoffs = {
            "book": int(book.timestamp) if book is not None else -1,
            "funding": int(funding.timestamp) if funding is not None else -1,
            "trade": int(trade.timestamp) if trade is not None else -1,
            "candle": max((int(candle.timestamp) for candle in candles), default=-1),
        }
        await websocket.send_json(
            {
                "schema_version": 1,
                "sequence": sequence,
                "type": "snapshot",
                "symbol": str(symbol),
                "data": {
                    "book": _data(book),
                    "funding": _data(funding),
                    "last_trade": _data(trade),
                    "candles": [_data(candle) for candle in candles],
                    "interval": interval,
                },
            }
        )
    except Exception:
        for task in readers:
            task.cancel()
        await asyncio.gather(*readers, return_exceptions=True)
        for event_type, queue in subscriptions:
            app.event_bus.unsubscribe(event_type, queue)
        guard.release(client_ip)
        with contextlib.suppress(RuntimeError):
            await websocket.close()
        raise
    try:
        message_limit = app.settings.api.market_ws_messages_per_second
        budget = MarketMessageBudget(message_limit, sequence=sequence, now=time.monotonic(), sent=1)
        while not app.is_shutting_down:
            done, _ = await asyncio.wait(readers, timeout=15.0, return_when=asyncio.FIRST_COMPLETED)
            if not done:
                sequence = budget.next_heartbeat()
                await websocket.send_json(
                    {"schema_version": 1, "sequence": sequence, "type": "heartbeat", "symbol": str(symbol)}
                )
                continue
            for task in done:
                event_type, queue = readers.pop(task)
                event: Event = task.result()
                readers[asyncio.create_task(queue.get(), name=task.get_name())] = (event_type, queue)
                serialized = _serialize(event.payload)
                if serialized is None:
                    continue
                message_type, event_symbol, data = serialized
                if event_symbol != symbol or (message_type == "candle" and data["interval"] != interval):
                    continue
                timestamp = data.get("timestamp")
                if isinstance(timestamp, int) and timestamp <= snapshot_cutoffs.get(message_type, -1):
                    continue
                if isinstance(timestamp, int):
                    snapshot_cutoffs[message_type] = timestamp
                # Advance the public sequence even when this event is locally
                # throttled. The next delivered event then exposes a gap and
                # forces the client to refresh the authoritative REST snapshot.
                sequence, allowed = budget.next_event(time.monotonic())
                if not allowed:
                    continue
                await websocket.send_json(
                    {
                        "schema_version": 1,
                        "sequence": sequence,
                        "type": message_type,
                        "symbol": event_symbol,
                        "data": data,
                    }
                )
    except WebSocketDisconnect:
        pass
    finally:
        for task in readers:
            task.cancel()
        await asyncio.gather(*readers, return_exceptions=True)
        for event_type, queue in subscriptions:
            app.event_bus.unsubscribe(event_type, queue)
        guard.release(client_ip)
        with contextlib.suppress(RuntimeError):
            await websocket.close()
