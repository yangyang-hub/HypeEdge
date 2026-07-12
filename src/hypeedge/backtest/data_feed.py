"""Data feed — replays historical candles and funding rates through the EventBus."""

from __future__ import annotations

import structlog

from hypeedge.core.events import (
    EVENT_CANDLE_UPDATE,
    EVENT_FUNDING_UPDATE,
    Event,
    EventBus,
)
from hypeedge.core.models import Candle, FundingRate

logger = structlog.get_logger(__name__)

# Hyperliquid funding settles every hour (design doc §3.5)
_HOUR_MS = 3_600_000


class DataFeed:
    """Replays historical data as EventBus events for backtesting.

    Publishes candles as EVENT_CANDLE_UPDATE and funding rates as
    EVENT_FUNDING_UPDATE at hour boundaries. The engine controls pacing
    by calling next_candle() one step at a time.
    """

    def __init__(
        self,
        candles: list[Candle],
        funding_rates: list[FundingRate] | None,
        event_bus: EventBus,
    ) -> None:
        self._candles = sorted(candles, key=lambda c: c.timestamp)
        self._funding = sorted(funding_rates, key=lambda f: f.timestamp) if funding_rates else []
        self._event_bus = event_bus
        self._candle_idx = 0
        self._funding_idx = 0
        self._current_hour_ms: int = 0  # tracks the current funding hour boundary

    @property
    def total_candles(self) -> int:
        return len(self._candles)

    @property
    def has_next(self) -> bool:
        """True if there are more candles to replay."""
        return self._candle_idx < len(self._candles)

    @property
    def current_index(self) -> int:
        """Index of the next candle to be yielded."""
        return self._candle_idx

    def next_candle(self) -> Candle | None:
        """Advance to the next candle, publish it and any applicable funding events.

        Returns the next Candle, or None if the feed is exhausted.
        At each hour boundary (timestamp aligned to hour), publishes any
        funding rate data available for that hour.
        """
        if self._candle_idx >= len(self._candles):
            return None

        candle = self._candles[self._candle_idx]
        self._candle_idx += 1

        # Publish candle event
        self._event_bus.publish_sync(
            Event(
                event_type=EVENT_CANDLE_UPDATE,
                payload=candle,
                correlation_id=str(candle.symbol),
            )
        )

        # Check if we crossed an hour boundary for funding
        candle_hour_ms = (candle.timestamp // _HOUR_MS) * _HOUR_MS
        if candle_hour_ms > self._current_hour_ms:
            self._current_hour_ms = candle_hour_ms
            self._publish_funding_for_hour(candle_hour_ms, candle.symbol)

        return candle

    def _publish_funding_for_hour(self, hour_ms: int, symbol: str) -> None:
        """Publish any funding rate data that matches the given hour boundary."""
        while self._funding_idx < len(self._funding):
            funding = self._funding[self._funding_idx]
            funding_hour_ms = (funding.timestamp // _HOUR_MS) * _HOUR_MS
            if funding_hour_ms < hour_ms:
                # Skip stale funding entries
                self._funding_idx += 1
                continue
            if funding_hour_ms > hour_ms:
                break
            # Match: same hour boundary
            self._event_bus.publish_sync(
                Event(
                    event_type=EVENT_FUNDING_UPDATE,
                    payload=funding,
                    correlation_id=str(funding.symbol),
                )
            )
            self._funding_idx += 1

    def reset(self) -> None:
        """Reset the feed to the beginning (for walk-forward window reuse)."""
        self._candle_idx = 0
        self._funding_idx = 0
        self._current_hour_ms = 0
