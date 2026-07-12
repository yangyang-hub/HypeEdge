from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from hypeedge.config.settings import AppSettings
from hypeedge.core.events import EventBus
from hypeedge.core.exceptions import MarketDataError
from hypeedge.market_data.rest_client import RestClient


def _client() -> tuple[RestClient, AsyncMock]:
    limiter = MagicMock()
    limiter.acquire = AsyncMock()
    client = RestClient(AppSettings(), EventBus(), limiter)
    http_client = AsyncMock()
    http_client.post.side_effect = httpx.ConnectError("offline", request=httpx.Request("POST", "https://x/info"))
    client._ensure_client = AsyncMock(return_value=http_client)  # type: ignore[method-assign]
    return client, http_client


async def test_candle_backfill_fails_after_bounded_retries() -> None:
    client, http_client = _client()
    with (
        patch("hypeedge.market_data.rest_client.asyncio.sleep", new=AsyncMock()),
        pytest.raises(MarketDataError, match="Candle backfill failed after 3 attempts"),
    ):
        await client.backfill_candles("BTC", "1m", 1_000, 61_000)
    assert http_client.post.await_count == 3


async def test_funding_backfill_fails_after_bounded_retries() -> None:
    client, http_client = _client()
    with (
        patch("hypeedge.market_data.rest_client.asyncio.sleep", new=AsyncMock()),
        pytest.raises(MarketDataError, match="Funding backfill failed after 3 attempts"),
    ):
        await client.backfill_funding("BTC", 1_000, 3_601_000)
    assert http_client.post.await_count == 3


async def test_funding_backfill_posts_flat_body() -> None:
    """Hyperliquid fundingHistory expects flat fields, not candleSnapshot's nested req."""
    limiter = MagicMock()
    limiter.acquire = AsyncMock()
    client = RestClient(AppSettings(), EventBus(), limiter)
    http_client = AsyncMock()
    response = MagicMock()
    response.raise_for_status = MagicMock()
    response.json.side_effect = [
        [{"coin": "BTC", "fundingRate": "0.0001", "premium": "0.001", "time": 3_600_000}],
        [],
    ]
    http_client.post = AsyncMock(return_value=response)
    client._ensure_client = AsyncMock(return_value=http_client)  # type: ignore[method-assign]

    with patch("hypeedge.market_data.rest_client.asyncio.sleep", new=AsyncMock()):
        rates = await client.backfill_funding("BTC", 1_000, 3_601_000)

    assert len(rates) == 1
    assert http_client.post.await_args_list[0].kwargs["json"] == {
        "type": "fundingHistory",
        "coin": "BTC",
        "startTime": 1_000,
        "endTime": 3_601_000,
    }


async def test_candle_backfill_skips_empty_pages() -> None:
    """Sparse early history must advance past empty pages instead of exiting."""
    limiter = MagicMock()
    limiter.acquire = AsyncMock()
    settings = AppSettings()
    settings.market_data.backfill_batch_size = 2
    client = RestClient(settings, EventBus(), limiter)
    http_client = AsyncMock()
    empty = MagicMock()
    empty.raise_for_status = MagicMock()
    empty.json.return_value = []
    filled = MagicMock()
    filled.raise_for_status = MagicMock()
    filled.json.return_value = [
        {"t": 180_000, "o": "1", "h": "2", "l": "0.5", "c": "1.5", "v": "10"},
    ]
    http_client.post = AsyncMock(side_effect=[empty, filled, empty])
    client._ensure_client = AsyncMock(return_value=http_client)  # type: ignore[method-assign]

    with patch("hypeedge.market_data.rest_client.asyncio.sleep", new=AsyncMock()):
        # interval 1m => page width 2 * 60_000 = 120_000 ms
        candles = await client.backfill_candles("BTC", "1m", 0, 300_000)

    assert len(candles) == 1
    assert candles[0].timestamp == 180_000
    assert http_client.post.await_count >= 2
    assert http_client.post.await_args_list[0].kwargs["json"]["req"]["endTime"] == 120_000
    assert http_client.post.await_args_list[1].kwargs["json"]["req"]["startTime"] == 120_000

