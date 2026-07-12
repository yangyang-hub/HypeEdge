"""Tests for Binance public reference WebSocket parsing and isolation."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest

from hypeedge.config.settings import AppSettings, ExternalReferenceSettings
from hypeedge.core.events import ALL_EVENT_TYPES, EVENT_EXTERNAL_REFERENCE_UPDATE, EVENT_L2_BOOK_UPDATE, Event, EventBus
from hypeedge.core.models import L2BookSnapshot, L2Level
from hypeedge.core.types import Price, Size, Symbol, Timestamp
from hypeedge.market_data.binance_feed import BinanceReferenceFeed, WebsocketsBinanceTransport
from hypeedge.market_data.external_reference import LatestExternalReferenceProvider


def _feed() -> tuple[BinanceReferenceFeed, EventBus, LatestExternalReferenceProvider]:
    settings = AppSettings(
        external_reference=ExternalReferenceSettings(
            external_reference_enabled=True,
            symbol_map={"BTC": "BTCUSDT"},
        )
    )
    bus = EventBus()
    provider = LatestExternalReferenceProvider(settings.external_reference)
    return BinanceReferenceFeed(settings, bus, provider), bus, provider


@pytest.mark.asyncio
async def test_spot_and_perpetual_messages_publish_latest_snapshot() -> None:
    feed, bus, provider = _feed()
    queue = bus.subscribe(EVENT_EXTERNAL_REFERENCE_UPDATE, maxsize=1)
    spot = {"stream": "btcusdt@bookTicker", "data": {"s": "BTCUSDT", "u": 7, "b": "100", "a": "102"}}
    perpetual = {
        "stream": "btcusdt@bookTicker",
        "data": {"e": "bookTicker", "E": 1234, "s": "BTCUSDT", "u": 9, "b": "100.1", "a": "102.1"},
    }

    await feed._handle_message(json.dumps(spot), "spot", 1)
    await feed._handle_message(json.dumps(perpetual), "perpetual", 2)

    event = queue.get_nowait()
    assert event.payload.raw_price == Price("101.06")
    assert event.payload.sequence == 9
    assert provider.get_external_reference(Symbol("BTC")).quality == "healthy"


def test_mark_price_parser_retains_exchange_metadata() -> None:
    feed, _, _ = _feed()
    received_at = datetime.now(UTC)

    quote = feed._parse_quote(
        {"e": "markPriceUpdate", "E": 12345, "s": "BTCUSDT", "p": "100.125"},
        "perpetual",
        4,
        received_at,
    )

    assert quote is not None
    assert quote.market == "perpetual_mark"
    assert quote.mark_price == Price("100.125")
    assert quote.exchange_ts == 12345
    assert quote.sequence == 12345
    assert quote.connection_generation == 4
    assert quote.received_at is received_at


def test_invalid_or_unknown_payload_is_ignored() -> None:
    feed, _, _ = _feed()
    now = datetime.now(UTC)
    assert feed._parse_quote({"s": "UNKNOWN", "b": "1", "a": "2"}, "spot", 1, now) is None
    assert feed._parse_quote({"s": "BTCUSDT", "b": "0", "a": "2"}, "spot", 1, now) is None
    assert feed._parse_quote({"s": "BTCUSDT", "b": "bad", "a": "2"}, "spot", 1, now) is None


def test_combined_urls_include_all_required_public_streams() -> None:
    feed, _, _ = _feed()
    assert "btcusdt@bookTicker" in feed._spot_url()
    perpetual_url = feed._perpetual_url()
    assert "btcusdt@bookTicker" in perpetual_url
    assert "btcusdt@markPrice@1s" in perpetual_url


@pytest.mark.asyncio
async def test_reconnect_uses_capped_exponential_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    class FailingTransport:
        async def stream(self, url: str) -> AsyncIterator[str | bytes]:
            _ = url
            if False:
                yield ""
            raise OSError("offline")

    settings = AppSettings(
        external_reference=ExternalReferenceSettings(
            external_reference_enabled=True,
            symbol_map={"BTC": "BTCUSDT"},
            reconnect_delay_min_seconds=1,
            reconnect_delay_max_seconds=2,
        )
    )
    feed = BinanceReferenceFeed(
        settings,
        EventBus(),
        LatestExternalReferenceProvider(settings.external_reference),
        FailingTransport(),
    )
    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)
        if len(delays) == 3:
            feed._running = False

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    feed._running = True
    await feed._run_stream("spot", feed._spot_url())

    assert delays == [1, 2, 2]
    assert feed._generations["spot"] == 3


@pytest.mark.asyncio
async def test_disabled_run_does_not_open_transport() -> None:
    settings = AppSettings()
    provider = LatestExternalReferenceProvider(settings.external_reference)
    feed = BinanceReferenceFeed(settings, EventBus(), provider)

    await feed.run()

    assert feed._tasks == []


@pytest.mark.asyncio
async def test_websockets_transport_yields_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeWebSocket:
        def __aiter__(self) -> AsyncIterator[str]:
            async def messages() -> AsyncIterator[str]:
                yield "one"
                yield "two"

            return messages()

    class FakeContext:
        async def __aenter__(self) -> FakeWebSocket:
            return FakeWebSocket()

        async def __aexit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr("hypeedge.market_data.binance_feed.websockets.connect", lambda *args, **kwargs: FakeContext())

    messages = [message async for message in WebsocketsBinanceTransport().stream("wss://example")]

    assert messages == ["one", "two"]


@pytest.mark.asyncio
async def test_enabled_run_can_be_stopped_without_leaking_tasks() -> None:
    class HangingTransport:
        async def stream(self, url: str) -> AsyncIterator[str | bytes]:
            _ = url
            await asyncio.Event().wait()
            if False:
                yield ""

    feed, _, _ = _feed()
    feed._transport = HangingTransport()
    run_task = asyncio.create_task(feed.run())
    for _ in range(10):
        if len(feed._tasks) == 3:
            break
        await asyncio.sleep(0)

    await feed.stop()
    await run_task

    assert feed._running is False
    assert feed._tasks == []


@pytest.mark.asyncio
async def test_malformed_messages_and_unknown_symbols_are_ignored() -> None:
    feed, bus, _ = _feed()
    queue = bus.subscribe(EVENT_EXTERNAL_REFERENCE_UPDATE)

    await feed._handle_message("not-json", "spot", 1)
    await feed._handle_message("[]", "spot", 1)
    await feed._handle_message('{"data": []}', "spot", 1)
    await feed._handle_message('{"s": "UNKNOWN", "b": "1", "a": "2"}', "spot", 1)

    assert queue.empty()


@pytest.mark.asyncio
async def test_hyperliquid_book_subscription_drives_basis_calibration() -> None:
    feed, bus, provider = _feed()
    now = datetime.now(UTC)
    provider.update_quote(
        feed._parse_quote(
            {"s": "BTCUSDT", "u": 1, "b": "99", "a": "101"},
            "spot",
            1,
            now,
        )  # type: ignore[arg-type]
    )
    provider.update_quote(
        feed._parse_quote(
            {"e": "bookTicker", "E": 2, "s": "BTCUSDT", "u": 2, "b": "99", "a": "101"},
            "perpetual",
            1,
            now,
        )  # type: ignore[arg-type]
    )
    feed._running = True
    task = asyncio.create_task(feed._consume_hyperliquid_books())
    await asyncio.sleep(0)
    bus.publish_sync(
        Event(
            event_type=EVENT_L2_BOOK_UPDATE,
            payload=L2BookSnapshot(
                symbol=Symbol("BTC"),
                bids=(L2Level(Price("100"), Size("1")),),
                asks=(L2Level(Price("102"), Size("1")),),
                timestamp=Timestamp(3),
                local_ts=now,
            ),
        )
    )
    await asyncio.sleep(0)
    feed._running = False
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    snapshot = provider.get_external_reference(Symbol("BTC"))
    assert snapshot.adjusted_price is not None
    assert snapshot.adjusted_price > snapshot.raw_price  # type: ignore[operator]
    assert snapshot.basis_bps > 0
    assert EVENT_EXTERNAL_REFERENCE_UPDATE in ALL_EVENT_TYPES
