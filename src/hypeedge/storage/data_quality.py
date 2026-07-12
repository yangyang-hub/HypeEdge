"""Data quality checker for ClickHouse market data (Phase 1B).

Periodically queries ClickHouse tables to detect:
- Time gaps in candle/funding series
- Duplicate rows by timestamp
- Order book anomalies (bid >= ask at same timestamp)
- Candle internal consistency (high < low, etc.)

Results are logged and exposed as Prometheus metrics.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import structlog

from hypeedge.config.settings import AppSettings
from hypeedge.core.types import Symbol

logger = structlog.get_logger(__name__)


@dataclass
class QualityReport:
    """Result of a single data quality check."""

    table: str
    coin: str
    interval: str | None = None
    check_time: datetime = field(default_factory=lambda: datetime.now(UTC))
    total_rows: int = 0
    gaps: list[tuple[int, int]] = field(default_factory=list)  # (start_ms, end_ms)
    duplicate_count: int = 0
    anomaly_count: int = 0
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def has_issues(self) -> bool:
        return len(self.gaps) > 0 or self.duplicate_count > 0 or self.anomaly_count > 0


class DataQualityChecker:
    """Runs periodic data quality checks against ClickHouse.

    Queries are executed in a background task. Results are logged
    and pushed to Prometheus metrics for alerting.
    """

    def __init__(self, settings: AppSettings, client: Any) -> None:
        """Initialize the quality checker.

        Args:
            settings: Application settings (for coins, intervals, check frequency).
            client: clickhouse-connect client instance.
        """
        self._settings = settings
        self._client = client
        self._coins = [Symbol(c) for c in settings.market_data.coins]
        self._intervals = settings.market_data.candle_intervals
        self._check_interval_s = settings.backfill.quality_check_interval_hours * 3600
        self._running = False

    async def run(self) -> None:
        """Main loop: run quality checks periodically."""
        self._running = True
        try:
            while self._running:
                await self._run_checks()
                await asyncio.sleep(self._check_interval_s)
        except asyncio.CancelledError:
            logger.debug("data_quality_checker_cancelled")
        finally:
            self._running = False
            logger.info("data_quality_checker_stopped")

    async def run_once(self) -> list[QualityReport]:
        """Run all quality checks once and return reports."""
        return await self._run_checks()

    async def _run_checks(self) -> list[QualityReport]:
        """Execute all quality checks and return reports."""
        if not self._client:
            return []

        reports: list[QualityReport] = []
        loop = asyncio.get_running_loop()

        for coin in self._coins:
            # Candle gap + duplicate checks
            for interval in self._intervals:
                report = await loop.run_in_executor(
                    None,
                    self._check_candle_quality,
                    str(coin),
                    interval,
                )
                reports.append(report)

            # Funding gap check
            report = await loop.run_in_executor(
                None,
                self._check_funding_quality,
                str(coin),
            )
            reports.append(report)

            # Book anomaly check
            report = await loop.run_in_executor(
                None,
                self._check_book_quality,
                str(coin),
            )
            reports.append(report)

        # Log summary
        issues = [r for r in reports if r.has_issues]
        if issues:
            logger.warning(
                "data_quality_issues_found",
                total_checks=len(reports),
                issues=len(issues),
                gaps=sum(len(r.gaps) for r in issues),
                duplicates=sum(r.duplicate_count for r in issues),
                anomalies=sum(r.anomaly_count for r in issues),
            )
        else:
            logger.info("data_quality_ok", checks=len(reports))

        return reports

    def _check_candle_quality(self, coin: str, interval: str) -> QualityReport:
        """Check candle data for gaps and duplicates."""
        report = QualityReport(table="candles", coin=coin, interval=interval)

        try:
            # Get row count
            count_result = self._client.command(
                f"SELECT count() FROM candles WHERE coin = '{coin}' AND interval = '{interval}'"
            )
            report.total_rows = int(count_result) if count_result else 0

            if report.total_rows == 0:
                return report

            # Check for gaps: find consecutive rows where ts difference exceeds interval
            interval_ms = _interval_to_ms(interval)
            gap_query = f"""
                SELECT ts, lead_ts FROM (
                    SELECT ts, leadInFrame(ts) OVER (ORDER BY ts) AS lead_ts
                    FROM candles
                    WHERE coin = '{coin}' AND interval = '{interval}'
                    ORDER BY ts
                ) WHERE lead_ts - ts > {interval_ms * 2}
            """
            gap_rows = self._client.query(gap_query).result_rows
            for row in gap_rows:
                start_s, end_s = row[0], row[1]
                report.gaps.append((int(start_s * 1000), int(end_s * 1000)))

            # Check for duplicates
            dup_query = f"""
                SELECT count() FROM (
                    SELECT ts, count() AS cnt FROM candles
                    WHERE coin = '{coin}' AND interval = '{interval}'
                    GROUP BY ts HAVING cnt > 1
                )
            """
            dup_result = self._client.command(dup_query)
            report.duplicate_count = int(dup_result) if dup_result else 0

            # Check for internal consistency (high < low)
            anomaly_query = f"""
                SELECT count() FROM candles
                WHERE coin = '{coin}' AND interval = '{interval}'
                AND (high < low OR high < open OR high < close OR low > open OR low > close)
            """
            anomaly_result = self._client.command(anomaly_query)
            report.anomaly_count = int(anomaly_result) if anomaly_result else 0

        except Exception as e:
            logger.error("candle_quality_check_error", coin=coin, interval=interval, error=str(e))

        return report

    def _check_funding_quality(self, coin: str) -> QualityReport:
        """Check funding data for gaps and duplicates."""
        report = QualityReport(table="funding", coin=coin, interval="1h")

        try:
            count_result = self._client.command(f"SELECT count() FROM funding WHERE coin = '{coin}'")
            report.total_rows = int(count_result) if count_result else 0

            if report.total_rows == 0:
                return report

            # Funding gaps: hourly interval = 3600000ms, tolerate 2x
            gap_query = f"""
                SELECT ts, lead_ts FROM (
                    SELECT ts, leadInFrame(ts) OVER (ORDER BY ts) AS lead_ts
                    FROM funding WHERE coin = '{coin}' ORDER BY ts
                ) WHERE lead_ts - ts > 7200
            """
            gap_rows = self._client.query(gap_query).result_rows
            for row in gap_rows:
                report.gaps.append((int(row[0] * 1000), int(row[1] * 1000)))

            # Duplicates
            dup_query = f"""
                SELECT count() FROM (
                    SELECT ts, count() AS cnt FROM funding
                    WHERE coin = '{coin}' GROUP BY ts HAVING cnt > 1
                )
            """
            dup_result = self._client.command(dup_query)
            report.duplicate_count = int(dup_result) if dup_result else 0

        except Exception as e:
            logger.error("funding_quality_check_error", coin=coin, error=str(e))

        return report

    def _check_book_quality(self, coin: str) -> QualityReport:
        """Check order book data for bid/ask anomalies."""
        report = QualityReport(table="l2_book", coin=coin)

        try:
            count_result = self._client.command(f"SELECT count() FROM l2_book WHERE coin = '{coin}'")
            report.total_rows = int(count_result) if count_result else 0

            if report.total_rows == 0:
                return report

            # Find timestamps where best bid >= best ask
            anomaly_query = f"""
                SELECT count() FROM (
                    SELECT ts FROM l2_book
                    WHERE coin = '{coin}' AND side = 'bid' AND level = 0
                    AND px >= (
                        SELECT any(px) FROM l2_book AS a
                        WHERE a.ts = l2_book.ts AND a.coin = '{coin}' AND a.side = 'ask' AND a.level = 0
                    )
                )
            """
            anomaly_result = self._client.command(anomaly_query)
            report.anomaly_count = int(anomaly_result) if anomaly_result else 0

        except Exception as e:
            logger.error("book_quality_check_error", coin=coin, error=str(e))

        return report


def _interval_to_ms(interval: str) -> int:
    """Convert candle interval string to milliseconds."""
    interval_map = {
        "1m": 60_000,
        "3m": 180_000,
        "5m": 300_000,
        "15m": 900_000,
        "30m": 1_800_000,
        "1h": 3_600_000,
        "2h": 7_200_000,
        "4h": 14_400_000,
        "8h": 28_800_000,
        "12h": 43_200_000,
        "1d": 86_400_000,
        "3d": 259_200_000,
        "1w": 604_800_000,
        "1M": 2_592_000_000,
    }
    return interval_map.get(interval, 60_000)
