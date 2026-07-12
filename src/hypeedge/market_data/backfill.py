"""Backfill scheduler for REST historical data (Phase 1B).

Orchestrates periodic REST backfill of candle and funding data,
publishing results to the EventBus for ClickHouse persistence.
Uses checkpoint files to resume from the last successful position.
"""

from __future__ import annotations

import asyncio
import time

import structlog

from hypeedge.config.settings import AppSettings
from hypeedge.core.events import (
    EVENT_CANDLE_UPDATE,
    EVENT_FUNDING_UPDATE,
    Event,
    EventBus,
)
from hypeedge.core.types import Symbol
from hypeedge.market_data.checkpoint import BackfillCheckpointStore
from hypeedge.market_data.rest_client import RestClient

logger = structlog.get_logger(__name__)


class BackfillScheduler:
    """Schedules and runs REST backfill tasks on startup and periodically.

    On startup, uses checkpoint files to determine the last successfully
    fetched timestamp and resumes from there. Falls back to a configurable
    window (default 7 days) if no checkpoint exists.
    Then runs a periodic refresh to fill gaps.
    All fetched data is published to the EventBus so ClickHouseWriter
    can persist it.
    """

    def __init__(
        self,
        settings: AppSettings,
        event_bus: EventBus,
        rest_client: RestClient,
        checkpoint_store: BackfillCheckpointStore,
    ) -> None:
        self._settings = settings
        self._event_bus = event_bus
        self._rest_client = rest_client
        self._checkpoint = checkpoint_store
        self._coins = [Symbol(c) for c in settings.market_data.coins]
        self._intervals = settings.market_data.candle_intervals
        self._backfill_window_ms = settings.backfill.backfill_window_days * 24 * 60 * 60 * 1000
        self._refresh_interval_s = settings.backfill.refresh_interval_hours * 3600
        self._running = False

    async def run(self) -> None:
        """Main loop: initial backfill then periodic refresh."""
        self._running = True
        try:
            await self._run_initial_backfill()
            await self._run_periodic_refresh()
        except asyncio.CancelledError:
            logger.debug("backfill_scheduler_cancelled")
        finally:
            self._running = False
            logger.info("backfill_scheduler_stopped")

    async def _run_initial_backfill(self) -> None:
        """Backfill recent data on startup, resuming from checkpoints."""
        now_ms = int(time.time() * 1000)

        logger.info(
            "backfill_initial_start",
            coins=[str(c) for c in self._coins],
            intervals=self._intervals,
            window_days=self._backfill_window_ms // (24 * 60 * 60 * 1000),
        )

        # Backfill candles for each coin + interval
        for coin in self._coins:
            for interval in self._intervals:
                if not self._running:
                    return
                start_ms = self._get_start_ms("candleSnapshot", coin, interval, now_ms)
                if start_ms >= now_ms:
                    logger.debug("backfill_skipped_up_to_date", coin=str(coin), interval=interval)
                    continue
                await self._backfill_candles(coin, interval, start_ms, now_ms)

        # Backfill funding for each coin
        for coin in self._coins:
            if not self._running:
                return
            start_ms = self._get_start_ms("fundingHistory", coin, "1h", now_ms)
            if start_ms >= now_ms:
                logger.debug("backfill_skipped_up_to_date", coin=str(coin), endpoint="fundingHistory")
                continue
            await self._backfill_funding(coin, start_ms, now_ms)

        logger.info("backfill_initial_complete")

    async def _run_periodic_refresh(self) -> None:
        """Periodically backfill recent data to fill gaps."""
        while self._running:
            await asyncio.sleep(self._refresh_interval_s)
            if not self._running:
                return

            now_ms = int(time.time() * 1000)
            # Refresh last 2 intervals worth of data to catch any gaps
            gap_window_ms = int(self._refresh_interval_s * 1000 * 1.5)
            start_ms = now_ms - gap_window_ms

            logger.info("backfill_periodic_refresh_start")

            for coin in self._coins:
                for interval in self._intervals:
                    if not self._running:
                        return
                    await self._backfill_candles(coin, interval, start_ms, now_ms)

            for coin in self._coins:
                if not self._running:
                    return
                await self._backfill_funding(coin, start_ms, now_ms)

            logger.info("backfill_periodic_refresh_complete")

    def _get_start_ms(self, endpoint: str, coin: Symbol, interval: str, now_ms: int) -> int:
        """Determine the start timestamp for backfill.

        Uses checkpoint if available, otherwise falls back to ``now - window``.
        """
        checkpoint_ts = self._checkpoint.get(endpoint, str(coin), interval)
        if checkpoint_ts is not None:
            logger.debug(
                "backfill_resuming_from_checkpoint",
                endpoint=endpoint,
                coin=str(coin),
                interval=interval,
                checkpoint_ts=checkpoint_ts,
            )
            return checkpoint_ts + 1  # Resume after the last fetched timestamp

        fallback_ms = now_ms - self._backfill_window_ms
        logger.debug(
            "backfill_no_checkpoint_using_window",
            endpoint=endpoint,
            coin=str(coin),
            interval=interval,
            start_ms=fallback_ms,
        )
        return fallback_ms

    async def _backfill_candles(self, coin: Symbol, interval: str, start_ms: int, end_ms: int) -> None:
        """Backfill candles and publish to EventBus."""
        try:
            candles = await self._rest_client.backfill_candles(
                coin=str(coin),
                interval=interval,
                start_time=start_ms,
                end_time=end_ms,
            )
            last_ts = 0
            for candle in candles:
                self._event_bus.publish_sync(
                    Event(event_type=EVENT_CANDLE_UPDATE, payload=candle, correlation_id=str(coin)),
                )
                if candle.timestamp > last_ts:
                    last_ts = candle.timestamp

            # Save checkpoint with the latest timestamp we fetched
            if last_ts > 0:
                self._checkpoint.save("candleSnapshot", str(coin), interval, last_ts)

            logger.debug(
                "backfill_candles_published",
                coin=str(coin),
                interval=interval,
                count=len(candles),
                checkpoint_ts=last_ts if last_ts > 0 else None,
            )
        except Exception:
            logger.exception("backfill_candles_failed", coin=str(coin), interval=interval)

    async def _backfill_funding(self, coin: Symbol, start_ms: int, end_ms: int) -> None:
        """Backfill funding rates and publish to EventBus."""
        try:
            rates = await self._rest_client.backfill_funding(
                coin=str(coin),
                start_time=start_ms,
                end_time=end_ms,
            )
            last_ts = 0
            for rate in rates:
                self._event_bus.publish_sync(
                    Event(event_type=EVENT_FUNDING_UPDATE, payload=rate, correlation_id=str(coin)),
                )
                if rate.timestamp > last_ts:
                    last_ts = rate.timestamp

            if last_ts > 0:
                self._checkpoint.save("fundingHistory", str(coin), "1h", last_ts)

            logger.debug(
                "backfill_funding_published",
                coin=str(coin),
                count=len(rates),
                checkpoint_ts=last_ts if last_ts > 0 else None,
            )
        except Exception:
            logger.exception("backfill_funding_failed", coin=str(coin))
