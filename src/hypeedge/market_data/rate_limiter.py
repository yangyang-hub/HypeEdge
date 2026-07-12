"""Hyperliquid rate limiter for IP weight and address action quota."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

import structlog

logger = structlog.get_logger(__name__)

# Hyperliquid rate limit constants (design doc §3.1, §3.2)
IP_WEIGHT_LIMIT_PER_MIN = 1200
DEFAULT_INFO_WEIGHT = 20
LIGHTWEIGHT_INFO_WEIGHT = 2
EXCHANGE_WEIGHT_BASE = 1
ACTION_CREDITS_INITIAL = 10_000
ACTION_CREDITS_LOW_WATERMARK_DEFAULT = 1000
PER_ITEM_BATCH_SIZE = 20  # Most endpoints add +1 weight per 20 items
CANDLE_PER_ITEM_BATCH_SIZE = 60  # candleSnapshot adds +1 per 60 items

# Endpoint weight mapping
ENDPOINT_WEIGHTS: dict[str, int] = {
    "l2Book": LIGHTWEIGHT_INFO_WEIGHT,
    "allMids": LIGHTWEIGHT_INFO_WEIGHT,
    "clearinghouseState": LIGHTWEIGHT_INFO_WEIGHT,
    "orderStatus": LIGHTWEIGHT_INFO_WEIGHT,
    "spotClearinghouseState": LIGHTWEIGHT_INFO_WEIGHT,
    "exchangeStatus": LIGHTWEIGHT_INFO_WEIGHT,
    "userRole": 60,
    "explorer": 40,
}

# Endpoints with per-item weight surcharges
PER_ITEM_ENDPOINTS: dict[str, int] = {
    "fundingHistory": PER_ITEM_BATCH_SIZE,
    "candleSnapshot": CANDLE_PER_ITEM_BATCH_SIZE,
    "userFills": PER_ITEM_BATCH_SIZE,
    "userFillsByTime": PER_ITEM_BATCH_SIZE,
    "recentTrades": PER_ITEM_BATCH_SIZE,
    "historicalOrders": PER_ITEM_BATCH_SIZE,
}


@dataclass
class RateLimitState:
    """Mutable state tracking for rate limits."""

    # IP weight tracking (sliding window)
    weight_timestamps: list[tuple[float, int]] = field(default_factory=list)

    # Address action credits (queried from exchange)
    action_credits_remaining: int = ACTION_CREDITS_INITIAL
    action_credits_last_query: float = 0.0


class RateLimiter:
    """Dual-dimension rate limiter for Hyperliquid.

    Tracks:
    1. IP weight consumption (1200 weight/min sliding window)
    2. Address action credits (queried from userRateLimit endpoint)

    Usage:
        limiter = RateLimiter()
        await limiter.acquire("fundingHistory", item_count=200)
        # ... make the API call ...
    """

    def __init__(
        self,
        ip_weight_limit: int = IP_WEIGHT_LIMIT_PER_MIN,
        action_credits_low_watermark: int = ACTION_CREDITS_LOW_WATERMARK_DEFAULT,
    ) -> None:
        self._ip_weight_limit = ip_weight_limit
        self._action_credits_low_watermark = action_credits_low_watermark
        self._state = RateLimitState()
        self._lock = asyncio.Lock()

    async def acquire(self, endpoint: str, batch_length: int = 0, item_count: int = 0) -> None:
        """Acquire rate limit capacity for a request.

        Blocks if necessary until capacity is available.
        Raises RateLimitExceededError if address action credits are exhausted.

        Args:
            endpoint: API endpoint name (e.g. "fundingHistory", "l2Book")
            batch_length: Number of items in a batch request (for exchange endpoint)
            item_count: Number of items expected in response (for per-item weight)
        """
        weight = self._calculate_weight(endpoint, batch_length, item_count)

        async with self._lock:
            # Wait until IP weight capacity is available
            attempts = 0
            while self._current_weight() + weight > self._ip_weight_limit:
                attempts += 1
                if attempts > 60:  # Safety: don't block forever
                    from hypeedge.core.exceptions import RateLimitExceededError

                    raise RateLimitExceededError(
                        f"IP weight limit exceeded after 60 retries: "
                        f"need {weight}, have {self._ip_weight_limit - self._current_weight()}",
                        limit_type="ip_weight",
                    )
                logger.debug("rate_limiter_waiting", endpoint=endpoint, weight=weight, wait=1.0)
                await asyncio.sleep(1.0)

            self._record_weight(weight)
            logger.debug(
                "rate_limiter_acquired",
                endpoint=endpoint,
                weight=weight,
                total_weight=self._current_weight(),
                remaining=self._ip_weight_limit - self._current_weight(),
            )

    async def acquire_action_credits(self, count: int = 1) -> bool:
        """Check if address action credits are available.

        Returns True if credits are available, False if exhausted.
        Does not actually consume credits (exchange does that) — this is
        a pre-check to avoid wasting API calls.
        """
        async with self._lock:
            if self._state.action_credits_remaining < count:
                logger.warning(
                    "action_credits_low",
                    remaining=self._state.action_credits_remaining,
                    requested=count,
                )
                return False
            return True

    def update_action_credits(self, remaining: int) -> None:
        """Update the tracked action credits (from userRateLimit response)."""
        old = self._state.action_credits_remaining
        self._state.action_credits_remaining = remaining
        self._state.action_credits_last_query = time.time()

        if remaining < self._action_credits_low_watermark:
            logger.warning(
                "action_credits_low_watermark",
                remaining=remaining,
                watermark=self._action_credits_low_watermark,
            )

        logger.debug("action_credits_updated", remaining=remaining, previous=old)

    def check_action_credits(self) -> bool:
        """Synchronous check if action credits are above the low watermark.

        Returns True if credits are available, False if below watermark.
        Used by ExecutionEngine before submitting orders (design doc §3.2, §8.1).
        """
        is_fresh = time.time() - self._state.action_credits_last_query <= 120.0
        return is_fresh and self._state.action_credits_remaining >= self._action_credits_low_watermark

    @property
    def action_credits_remaining(self) -> int:
        return self._state.action_credits_remaining

    @property
    def action_credits_low_watermark(self) -> int:
        return self._action_credits_low_watermark

    def action_credits_are_fresh(self, max_age_seconds: float = 120.0) -> bool:
        """Whether the quota snapshot is recent enough for a trading gate."""
        return (
            self._state.action_credits_last_query > 0
            and time.time() - self._state.action_credits_last_query <= max_age_seconds
        )

    @property
    def ip_weight_remaining(self) -> int:
        return max(0, self._ip_weight_limit - int(self._current_weight()))

    @property
    def ip_weight_used(self) -> int:
        """Return locally observed IP weight in the active sliding window."""
        return int(self._current_weight())

    def estimate_weight(self, endpoint: str, batch_length: int = 0, item_count: int = 0) -> int:
        """Public, side-effect-free request weight estimator.

        Action-budget callers use the same formula as ``acquire`` so batch
        child action counts are never confused with per-request IP weight.
        """
        return self._calculate_weight(endpoint, batch_length, item_count)

    def _calculate_weight(self, endpoint: str, batch_length: int = 0, item_count: int = 0) -> int:
        """Calculate the weight cost of a request."""
        # Exchange endpoint: 1 + floor(batch_length / 40)
        if endpoint == "exchange":
            return EXCHANGE_WEIGHT_BASE + (batch_length // 40)

        # Known endpoint with specific weight
        base_weight = ENDPOINT_WEIGHTS.get(endpoint, DEFAULT_INFO_WEIGHT)

        # Add per-item surcharge
        per_item_batch = PER_ITEM_ENDPOINTS.get(endpoint)
        item_weight = 0
        if per_item_batch and item_count > 0:
            item_weight = (item_count + per_item_batch - 1) // per_item_batch

        return base_weight + item_weight

    def _current_weight(self) -> float:
        """Get total weight consumed in the current 1-minute window."""
        now = time.time()
        cutoff = now - 60.0

        # Prune old entries
        self._state.weight_timestamps = [(ts, w) for ts, w in self._state.weight_timestamps if ts > cutoff]

        return sum(w for _, w in self._state.weight_timestamps)

    def _record_weight(self, weight: int) -> None:
        """Record a weight consumption."""
        self._state.weight_timestamps.append((time.time(), weight))
