"""Tests for the rate limiter."""

import pytest

from hypeedge.market_data.rate_limiter import RateLimiter


class TestRateLimiter:
    def test_calculate_weight_known_endpoint(self):
        limiter = RateLimiter()
        assert limiter._calculate_weight("l2Book") == 2
        assert limiter._calculate_weight("clearinghouseState") == 2
        assert limiter._calculate_weight("userRole") == 60

    def test_calculate_weight_unknown_endpoint(self):
        limiter = RateLimiter()
        assert limiter._calculate_weight("unknownEndpoint") == 20  # Default

    def test_calculate_weight_exchange(self):
        limiter = RateLimiter()
        # Single order: batch_length=0 → 1 + 0 = 1
        assert limiter._calculate_weight("exchange", batch_length=0) == 1
        # Batch of 40: 1 + 40//40 = 2
        assert limiter._calculate_weight("exchange", batch_length=40) == 2
        # Batch of 80: 1 + 80//40 = 3
        assert limiter._calculate_weight("exchange", batch_length=80) == 3
        assert limiter.estimate_weight("exchange", batch_length=80) == 3

    def test_calculate_weight_with_items(self):
        limiter = RateLimiter()
        # fundingHistory: base 20 + per-item
        # 200 items → 200 // 20 = 10 extra → total 30
        weight = limiter._calculate_weight("fundingHistory", item_count=200)
        assert weight == 20 + 10

        # A partial batch still consumes one extra item-weight bucket.
        assert limiter._calculate_weight("fundingHistory", item_count=1) == 21

    def test_calculate_weight_candle_items(self):
        limiter = RateLimiter()
        # candleSnapshot: base 20 + per-item (60 per batch)
        # 120 items → 120 // 60 = 2 extra → total 22
        weight = limiter._calculate_weight("candleSnapshot", item_count=120)
        assert weight == 20 + 2

    def test_update_action_credits(self):
        limiter = RateLimiter(action_credits_low_watermark=500)

        limiter.update_action_credits(5000)
        assert limiter.action_credits_remaining == 5000

        limiter.update_action_credits(100)
        assert limiter.action_credits_remaining == 100

    @pytest.mark.asyncio
    async def test_acquire_below_limit(self):
        limiter = RateLimiter(ip_weight_limit=1200)

        # Should succeed immediately
        await limiter.acquire("l2Book")  # weight 2
        assert limiter.ip_weight_remaining <= 1200

    @pytest.mark.asyncio
    async def test_acquire_respects_limit(self):
        limiter = RateLimiter(ip_weight_limit=10)

        # Use most of the budget
        for _ in range(4):
            await limiter.acquire("l2Book")  # 2 weight each = 8 total

        # This should succeed (2 more = 10 total)
        await limiter.acquire("l2Book")

        # Next acquire would need to wait (budget exhausted)
        assert limiter.ip_weight_remaining == 0

    @pytest.mark.asyncio
    async def test_action_credits_check(self):
        limiter = RateLimiter()

        limiter.update_action_credits(100)
        assert await limiter.acquire_action_credits(1) is True

        limiter.update_action_credits(0)
        assert await limiter.acquire_action_credits(1) is False
