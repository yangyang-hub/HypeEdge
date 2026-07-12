"""Tests for Hyperliquid WebSocket subscription construction."""

from datetime import UTC, datetime

from hypeedge.config.settings import AppSettings
from hypeedge.core.events import EVENT_L2_BOOK_UPDATE, EventBus
from hypeedge.market_data.ws_feed import WebSocketFeed


def test_build_subscriptions_uses_channel_specific_payloads():
    settings = AppSettings(
        market_data={
            "coins": ["BTC", "ETH"],
            "ws_subscriptions": ["l2Book", "trades", "candle", "allMids", "activeAssetCtx"],
            "candle_intervals": ["1m", "5m"],
        }
    )
    feed = WebSocketFeed(settings, EventBus())

    subscriptions = feed._build_subscriptions()

    assert {"type": "allMids"} in subscriptions
    assert subscriptions.count({"type": "allMids"}) == 1
    assert {"type": "l2Book", "coin": "BTC"} in subscriptions
    assert {"type": "trades", "coin": "ETH"} in subscriptions
    assert {"type": "activeAssetCtx", "coin": "BTC"} in subscriptions
    assert {"type": "candle", "coin": "BTC", "interval": "1m"} in subscriptions
    assert {"type": "candle", "coin": "ETH", "interval": "5m"} in subscriptions


async def test_l2_book_parses_hyperliquid_object_levels():
    bus = EventBus()
    queue = bus.subscribe(EVENT_L2_BOOK_UPDATE)
    feed = WebSocketFeed(AppSettings(market_data={"coins": ["BTC"]}), bus)

    received_at = datetime.now(UTC)
    feed._connection_generation = 4
    await feed._handle_l2_book(
        {
            "coin": "BTC",
            "time": 1_700_000_000_000,
            "levels": [
                [{"px": "68000.5", "sz": "1.25", "n": 2}],
                [{"px": "68001.0", "sz": "0.75", "n": 1}],
            ],
        },
        received_at,
    )

    snapshot = queue.get_nowait().payload
    assert float(snapshot.bids[0].price) == 68000.5
    assert float(snapshot.bids[0].size) == 1.25
    assert float(snapshot.asks[0].price) == 68001.0
    assert snapshot.exchange_ts == 1_700_000_000_000
    assert snapshot.received_at == received_at
    assert snapshot.version == 1
    assert snapshot.connection_generation == 4


def test_l2_book_level_parser_skips_invalid_entries():
    assert WebSocketFeed._parse_book_levels([{"px": "1", "sz": "2"}, {"px": "bad"}, None]) == [(1.0, 2.0)]
