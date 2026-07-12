"""Market data API routes."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal

from fastapi import APIRouter, Depends, Query

from hypeedge.api.auth import require_viewer
from hypeedge.api.deps import AppDep, MarketDataDep, RestClientDep
from hypeedge.api.errors import ApiProblem
from hypeedge.api.schemas import decimal_string
from hypeedge.core.types import Symbol

router = APIRouter(prefix="/market", tags=["market"], dependencies=[Depends(require_viewer)])


def _validated_symbol(symbol: str) -> Symbol:
    normalized = symbol.strip().upper()
    if not normalized or len(normalized) > 20 or not all(char.isalnum() or char in "_.-" for char in normalized):
        raise ApiProblem(422, "INVALID_SYMBOL", "Symbol format is invalid")
    return Symbol(normalized)


@router.get("/{symbol}/funding")
async def get_funding_rate(symbol: str, market_data: MarketDataDep) -> dict[str, Any]:
    """Get current funding rate for a symbol."""
    if market_data is None:
        raise ApiProblem(503, "MARKET_DATA_UNAVAILABLE", "Market data provider is not available", retryable=True)
    funding = market_data.get_funding(_validated_symbol(symbol))
    if funding is None:
        raise ApiProblem(503, "MARKET_DATA_NOT_READY", "Funding snapshot has not been received", retryable=True)
    return {
        "ok": True,
        "data": {
            "symbol": str(funding.symbol),
            "funding_rate": decimal_string(funding.funding_rate),
            "premium": decimal_string(funding.premium),
            "mark_price": decimal_string(funding.mark_price),
            "open_interest": decimal_string(funding.open_interest),
            "timestamp": int(funding.timestamp),
        },
    }


@router.get("/{symbol}/book")
async def get_order_book(
    symbol: str,
    market_data: MarketDataDep,
    rest_client: RestClientDep,
) -> dict[str, Any]:
    """Get current L2 order book for a symbol."""
    normalized = _validated_symbol(symbol)
    snapshot = market_data.get_book(normalized) if market_data is not None else None
    if snapshot is not None:
        return {
            "ok": True,
            "data": {
                "symbol": str(snapshot.symbol),
                "bids": [[decimal_string(level.price), decimal_string(level.size)] for level in snapshot.bids],
                "asks": [[decimal_string(level.price), decimal_string(level.size)] for level in snapshot.asks],
                "timestamp": int(snapshot.timestamp),
                "source": "websocket",
            },
        }
    if rest_client is None:
        raise ApiProblem(503, "MARKET_DATA_UNAVAILABLE", "Market data client is not available", retryable=True)

    try:
        book = await rest_client.get_l2_book(str(normalized))
        levels = book.get("levels")
        if not isinstance(levels, list) or len(levels) != 2:
            raise ValueError("invalid l2Book response")

        def parse_side(raw_side: Any) -> list[list[str]]:
            if not isinstance(raw_side, list):
                raise ValueError("invalid l2Book side")
            parsed: list[list[str]] = []
            for level in raw_side:
                if isinstance(level, dict):
                    parsed.append([decimal_string(str(level["px"])), decimal_string(str(level["sz"]))])
                elif isinstance(level, (list, tuple)) and len(level) >= 2:
                    parsed.append([decimal_string(str(level[0])), decimal_string(str(level[1]))])
            return parsed

        return {
            "ok": True,
            "data": {
                "symbol": str(normalized),
                "bids": parse_side(levels[0]),
                "asks": parse_side(levels[1]),
                "timestamp": int(book.get("time", 0)),
                "source": "rest",
            },
        }
    except Exception as exc:
        raise ApiProblem(502, "EXCHANGE_QUERY_FAILED", "Exchange order book query failed", retryable=True) from exc


@router.get("/{symbol}/candles")
async def get_candles(
    symbol: str,
    market_data: MarketDataDep,
    rest_client: RestClientDep,
    interval: Literal["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d"] = "1m",
    limit: int = Query(default=300, ge=1, le=1_000),
) -> dict[str, Any]:
    """Return normalized candles; use REST history until the live cache is warm."""
    normalized = _validated_symbol(symbol)
    candles = market_data.get_candles(normalized, interval, limit) if market_data is not None else []
    if len(candles) < limit and market_data is not None:
        from time import time

        interval_ms = {
            "1m": 60_000,
            "3m": 180_000,
            "5m": 300_000,
            "15m": 900_000,
            "30m": 1_800_000,
            "1h": 3_600_000,
            "2h": 7_200_000,
            "4h": 14_400_000,
            "8h": 28_800_000,
            "12h": 43_200_000,
            "1d": 86_400_000,
        }[interval]
        end_ms = int(time() * 1_000)
        try:
            candles = await market_data.ensure_candles(
                normalized,
                interval,
                limit,
                end_ms - interval_ms * (limit + 2),
                end_ms,
            )
        except Exception as exc:
            if not candles:
                raise ApiProblem(502, "EXCHANGE_QUERY_FAILED", "Exchange candle query failed", retryable=True) from exc
    elif not candles and rest_client is None:
        raise ApiProblem(503, "MARKET_DATA_UNAVAILABLE", "Market data provider is not available", retryable=True)

    return {
        "ok": True,
        "data": [
            {
                "symbol": str(candle.symbol),
                "interval": candle.interval,
                "open": decimal_string(candle.open),
                "high": decimal_string(candle.high),
                "low": decimal_string(candle.low),
                "close": decimal_string(candle.close),
                "volume": decimal_string(candle.volume),
                "timestamp": int(candle.timestamp),
            }
            for candle in candles
        ],
    }


@router.get("/{symbol}/meta")
async def get_instrument_meta(symbol: str, app: AppDep) -> dict[str, Any]:
    cache = getattr(app, "_instrument_cache", None)
    info = cache.get(Symbol(symbol)) if cache is not None else None
    if info is None:
        raise ApiProblem(404, "INSTRUMENT_NOT_FOUND", "Instrument metadata is not available")

    exponent = Decimal(str(info.tick_size)).normalize().as_tuple().exponent
    price_decimals = max(0, -exponent) if isinstance(exponent, int) else 0
    return {
        "ok": True,
        "data": {
            "symbol": str(info.symbol),
            "price_decimals": price_decimals,
            "size_decimals": info.sz_decimals,
            "tick_size": decimal_string(info.tick_size),
            "lot_size": decimal_string(info.lot_size),
            "min_order_size": decimal_string(info.min_size),
            "max_leverage": info.max_leverage,
        },
    }
