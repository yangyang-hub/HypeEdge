"""REST client for Hyperliquid info and exchange endpoints (Phase 1)."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import httpx
import structlog

from hypeedge.config.settings import AppSettings
from hypeedge.core.events import EventBus
from hypeedge.core.exceptions import MarketDataError
from hypeedge.core.models import Candle, FundingRate
from hypeedge.core.types import Price, Size, Symbol, Timestamp
from hypeedge.market_data.rate_limiter import RateLimiter

logger = structlog.get_logger(__name__)


class RestClient:
    """Async REST client for Hyperliquid API.

    Handles:
    - Info endpoint queries (with rate limiting)
    - Historical data backfill (fundingHistory, candleSnapshot)
    - Account state polling (clearinghouseState)
    - Action credit monitoring (userRateLimit)
    """

    def __init__(self, settings: AppSettings, event_bus: EventBus, rate_limiter: RateLimiter) -> None:
        self._settings = settings
        self._event_bus = event_bus
        self._rate_limiter = rate_limiter
        self._base_url = settings.exchange.api_url
        self._client: httpx.AsyncClient | None = None

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=httpx.Timeout(10.0, connect=5.0),
                headers={"Content-Type": "application/json"},
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # --- Info endpoint queries ---

    async def post_info(self, request_type: str, payload: dict[str, Any] | None = None) -> Any:
        """Send a POST to the /info endpoint with rate limiting.

        Args:
            request_type: Info request type (e.g. "l2Book", "clearinghouseState")
            payload: Additional payload parameters
        """
        await self._rate_limiter.acquire(request_type)

        client = await self._ensure_client()
        body = {"type": request_type}
        if payload:
            body.update(payload)

        response = await client.post("/info", json=body)
        response.raise_for_status()
        return response.json()

    async def get_l2_book(self, coin: str) -> dict[str, Any]:
        """Get current L2 order book snapshot."""
        return cast(dict[str, Any], await self.post_info("l2Book", {"coin": coin}))

    async def get_clearinghouse_state(self, user: str) -> dict[str, Any]:
        """Get account clearinghouse state (weight 2, can poll frequently)."""
        return cast(dict[str, Any], await self.post_info("clearinghouseState", {"user": user}))

    async def get_user_rate_limit(self, user: str) -> dict[str, Any]:
        """Get current action credit usage."""
        return cast(dict[str, Any], await self.post_info("userRateLimit", {"user": user}))

    async def get_meta(self) -> dict[str, Any]:
        """Get exchange metadata (asset info, etc.)."""
        return cast(dict[str, Any], await self.post_info("meta"))

    # --- Historical backfill ---

    async def backfill_candles(
        self,
        coin: str,
        interval: str,
        start_time: int,
        end_time: int,
    ) -> list[Candle]:
        """Backfill historical candle data.

        Handles rate limiting and pagination automatically.
        Uses candleSnapshot endpoint (weight 20 + per-item surcharge).

        Args:
            coin: Symbol name
            interval: Candle interval (e.g. "1m", "5m", "1h", "1d")
            start_time: Start timestamp in ms
            end_time: End timestamp in ms
        """
        all_candles: list[Candle] = []
        batch_size = self._settings.market_data.backfill_batch_size
        interval_ms = _interval_to_ms(interval)
        consecutive_failures = 0

        logger.info("backfill_candles_start", coin=coin, interval=interval, start=start_time, end=end_time)

        while start_time < end_time:
            page_end = min(end_time, start_time + (batch_size * interval_ms))
            estimated_items = max(1, min(batch_size, (page_end - start_time) // interval_ms))
            await self._rate_limiter.acquire("candleSnapshot", item_count=max(1, estimated_items))

            client = await self._ensure_client()
            body = {
                "type": "candleSnapshot",
                "req": {
                    "coin": coin,
                    "interval": interval,
                    "startTime": start_time,
                    "endTime": page_end,
                },
            }

            try:
                response = await client.post("/info", json=body)
                response.raise_for_status()
                data = response.json()
            except (httpx.HTTPStatusError, httpx.RequestError) as e:
                logger.error("backfill_candles_error", error=str(e), start_time=start_time)
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    raise MarketDataError(
                        f"Candle backfill failed after {consecutive_failures} attempts: coin={coin} interval={interval}"
                    ) from e
                await asyncio.sleep(0.5 * (2 ** (consecutive_failures - 1)))
                continue

            consecutive_failures = 0

            if not data:
                # Empty pages can occur at the start of a window (sparse history).
                # Advance past this page instead of treating it as end-of-stream.
                if page_end >= end_time:
                    break
                start_time = page_end
                await asyncio.sleep(0.5)
                continue

            for candle_data in data:
                candle = Candle(
                    symbol=Symbol(coin),
                    interval=interval,
                    open=Price(candle_data.get("o", 0)),
                    high=Price(candle_data.get("h", 0)),
                    low=Price(candle_data.get("l", 0)),
                    close=Price(candle_data.get("c", 0)),
                    volume=Size(candle_data.get("v", 0)),
                    timestamp=Timestamp(int(candle_data.get("t", 0))),
                )
                all_candles.append(candle)

            # Move start_time forward
            last_ts = int(data[-1].get("t", 0))
            if last_ts <= start_time:
                break
            start_time = last_ts + 1

            logger.debug(
                "backfill_candles_batch",
                coin=coin,
                fetched=len(data),
                total=len(all_candles),
            )

            # Rate limit between batches
            await asyncio.sleep(0.5)

        logger.info("backfill_candles_done", coin=coin, interval=interval, total=len(all_candles))
        return all_candles

    async def backfill_funding(
        self,
        coin: str,
        start_time: int,
        end_time: int,
    ) -> list[FundingRate]:
        """Backfill historical funding rate data.

        Uses fundingHistory endpoint (weight 20 + per-item surcharge).
        """
        all_funding: list[FundingRate] = []
        batch_size = self._settings.market_data.backfill_batch_size
        funding_interval_ms = 60 * 60_000
        consecutive_failures = 0

        logger.info("backfill_funding_start", coin=coin, start=start_time, end=end_time)

        while start_time < end_time:
            page_end = min(end_time, start_time + (batch_size * funding_interval_ms))
            estimated_items = max(1, min(batch_size, (page_end - start_time) // funding_interval_ms))
            await self._rate_limiter.acquire("fundingHistory", item_count=estimated_items)

            client = await self._ensure_client()
            # Hyperliquid fundingHistory uses a flat body (unlike candleSnapshot's nested req).
            body = {
                "type": "fundingHistory",
                "coin": coin,
                "startTime": start_time,
                "endTime": page_end,
            }

            try:
                response = await client.post("/info", json=body)
                response.raise_for_status()
                data = response.json()
            except (httpx.HTTPStatusError, httpx.RequestError) as e:
                response_text = ""
                if isinstance(e, httpx.HTTPStatusError):
                    response_text = e.response.text[:200]
                logger.error(
                    "backfill_funding_error",
                    error=str(e),
                    start_time=start_time,
                    response=response_text or None,
                )
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    raise MarketDataError(
                        f"Funding backfill failed after {consecutive_failures} attempts: coin={coin}"
                    ) from e
                await asyncio.sleep(0.5 * (2 ** (consecutive_failures - 1)))
                continue

            consecutive_failures = 0

            if not data:
                if page_end >= end_time:
                    break
                start_time = page_end
                await asyncio.sleep(0.5)
                continue

            for item in data:
                funding = FundingRate(
                    symbol=Symbol(coin),
                    funding_rate=float(item.get("fundingRate", 0)),
                    premium=float(item.get("premium", 0)),
                    mark_price=Price(item.get("markPx", 0)),
                    open_interest=float(item.get("openInterest", 0)),
                    timestamp=Timestamp(int(item.get("time", 0))),
                )
                all_funding.append(funding)

            last_ts = int(data[-1].get("time", 0))
            if last_ts <= start_time:
                break
            start_time = last_ts + 1
            await asyncio.sleep(0.5)

        logger.info("backfill_funding_done", coin=coin, total=len(all_funding))
        return all_funding

    async def poll_action_credits(self, user: str) -> int:
        """Poll current action credit usage and update rate limiter."""
        data = await self.poll_action_credit_snapshot(user)
        if data is None:
            return -1
        if "remaining" in data:
            return int(data["remaining"])
        used = int(data.get("nRequestsUsed", 0))
        cap = int(data.get("nRequestsCap", 0))
        return max(0, cap - used)

    async def poll_action_credit_snapshot(self, user: str) -> dict[str, Any] | None:
        """Fetch one authoritative quota snapshot and update the shared limiter."""
        try:
            data = await self.get_user_rate_limit(user)
            if "remaining" in data:
                remaining = int(data["remaining"])
            else:
                used = int(data.get("nRequestsUsed", 0))
                cap = int(data.get("nRequestsCap", 0))
                remaining = max(0, cap - used)
            self._rate_limiter.update_action_credits(remaining)
            return data
        except Exception:
            logger.exception("poll_action_credits_error")
            return None


def _interval_to_ms(interval: str) -> int:
    """Convert Hyperliquid candle interval strings to milliseconds."""
    interval_map = {
        "1m": 60_000,
        "3m": 3 * 60_000,
        "5m": 5 * 60_000,
        "15m": 15 * 60_000,
        "30m": 30 * 60_000,
        "1h": 60 * 60_000,
        "2h": 2 * 60 * 60_000,
        "4h": 4 * 60 * 60_000,
        "8h": 8 * 60 * 60_000,
        "12h": 12 * 60 * 60_000,
        "1d": 24 * 60 * 60_000,
        "3d": 3 * 24 * 60 * 60_000,
        "1w": 7 * 24 * 60 * 60_000,
        "1M": 30 * 24 * 60 * 60_000,
    }
    try:
        return interval_map[interval]
    except KeyError as exc:
        raise MarketDataError(f"Unsupported candle interval: {interval}") from exc
