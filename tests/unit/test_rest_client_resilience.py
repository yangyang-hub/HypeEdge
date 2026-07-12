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
