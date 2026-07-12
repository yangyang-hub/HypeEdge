from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from hypeedge.config.settings import AppSettings
from hypeedge.core.models import Candle, FundingRate
from hypeedge.core.types import Price, Size, Symbol, Timestamp
from hypeedge.market_data.book import BookManager
from hypeedge.market_data.live_provider import LiveMarketDataProvider


def _provider() -> LiveMarketDataProvider:
    from hypeedge.core.events import EventBus

    return LiveMarketDataProvider(AppSettings(), EventBus(), MagicMock(), BookManager())


def _candle(timestamp: int, close: float) -> Candle:
    return Candle(
        symbol=Symbol("BTC"),
        interval="1m",
        open=Price(close - 1),
        high=Price(close + 1),
        low=Price(close - 2),
        close=Price(close),
        volume=Size(10),
        timestamp=Timestamp(timestamp),
    )


def test_candle_cache_replaces_same_timestamp_and_orders_late_updates() -> None:
    provider = _provider()
    provider._handle_candle(_candle(2, 102))
    provider._handle_candle(_candle(1, 101))
    provider._handle_candle(_candle(2, 103))

    candles = provider.get_candles(Symbol("BTC"), "1m")
    assert [int(candle.timestamp) for candle in candles] == [1, 2]
    assert float(candles[-1].close) == 103


def test_funding_cache_returns_authoritative_snapshot() -> None:
    provider = _provider()
    funding = FundingRate(
        symbol=Symbol("BTC"),
        funding_rate=0.0001,
        premium=0.00005,
        mark_price=Price(60_000),
        open_interest=1234.5,
        timestamp=Timestamp(99),
    )
    provider._handle_funding(funding)
    assert provider.get_funding(Symbol("BTC")) is funding


async def test_start_stop_unsubscribes_all_market_queues() -> None:
    from hypeedge.core.events import EventBus

    bus = EventBus()
    provider = LiveMarketDataProvider(AppSettings(), bus, MagicMock(), BookManager())
    await provider.start()
    assert len(provider._subscriptions) == 4
    await provider.stop()
    assert provider._subscriptions == []


def test_book_price_snapshot_preserves_update_metadata_across_reads() -> None:
    book_manager = BookManager()
    received_at = datetime(2026, 7, 11, tzinfo=UTC)
    book_manager.get_book(Symbol("BTC")).update(
        [(100.0, 1.0)],
        [(102.0, 1.0)],
        Timestamp(1234),
        received_at=received_at,
        connection_generation=3,
    )
    provider = LiveMarketDataProvider(AppSettings(), MagicMock(), MagicMock(), book_manager)

    first = provider.get_price_snapshot(Symbol("BTC"))
    second = provider.get_price_snapshot(Symbol("BTC"))

    assert first == second
    assert first is not None
    assert first.price == 101.0
    assert first.exchange_ts == Timestamp(1234)
    assert first.received_at == received_at
    assert first.version == 1
    assert first.connection_generation == 3
