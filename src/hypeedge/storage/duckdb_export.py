"""DuckDB export utility for local research (Phase 1B, optional).

Exports data from ClickHouse to local DuckDB files for offline analysis.
Not a background task — run on demand via CLI or script.

Requires ``duckdb`` package (optional dependency: ``pip install hypeedge[duckdb]``).
"""

from __future__ import annotations

import asyncio
from typing import Any

import structlog

from hypeedge.config.settings import ClickHouseSettings

logger = structlog.get_logger(__name__)


class DuckDBExporter:
    """Export ClickHouse market data to a local DuckDB file.

    Usage::

        exporter = DuckDBExporter(clickhouse_settings, output_path="research.duckdb")
        await exporter.export_all(coins=["BTC", "ETH"], start_ms=..., end_ms=...)
    """

    def __init__(self, ch_settings: ClickHouseSettings, output_path: str) -> None:
        self._ch_settings = ch_settings
        self._output_path = output_path
        self._ch_client: Any = None

    async def export_table(
        self,
        table: str,
        coin: str,
        start_ms: int,
        end_ms: int,
    ) -> int:
        """Export a single table for a coin and time range.

        Returns:
            Number of rows exported.
        """
        import duckdb  # noqa: F811 — optional dependency

        loop = asyncio.get_running_loop()

        def _do_export() -> int:
            import clickhouse_connect

            # Connect to ClickHouse
            ch = clickhouse_connect.get_client(
                host=self._ch_settings.host,
                port=self._ch_settings.port,
                username=self._ch_settings.username,
                password=self._ch_settings.password,
                database=self._ch_settings.database,
            )

            start_sec = start_ms / 1000.0
            end_sec = end_ms / 1000.0

            # Query data from ClickHouse
            result = ch.query(
                f"SELECT * FROM {table} WHERE coin = %(coin)s AND ts BETWEEN %(start)s AND %(end)s",
                parameters={"coin": coin, "start": start_sec, "end": end_sec},
            )

            if not result.result_rows:
                return 0

            # Write to DuckDB
            db = duckdb.connect(self._output_path)
            try:
                columns = result.column_names
                placeholders = ", ".join(["?" for _ in columns])

                # Create table if not exists (using DuckDB auto-schema)
                col_list = ", ".join(f'"{c}" VARCHAR' for c in columns)
                db.execute(f"CREATE TABLE IF NOT EXISTS {table} ({col_list})")

                # Insert rows
                for row in result.result_rows:
                    db.execute(f"INSERT INTO {table} VALUES ({placeholders})", list(row))

                return len(result.result_rows)
            finally:
                db.close()

        count = await loop.run_in_executor(None, _do_export)
        logger.info("duckdb_exported", table=table, coin=coin, rows=count)
        return count

    async def export_all(
        self,
        coins: list[str],
        start_ms: int,
        end_ms: int,
    ) -> dict[str, int]:
        """Export all market data tables for the given coins and time range.

        Returns:
            Dict mapping table name to row count exported.
        """
        tables = ["candles", "funding", "trades", "l2_book", "mid_prices"]
        results: dict[str, int] = {}

        for table in tables:
            total = 0
            for coin in coins:
                count = await self.export_table(table, coin, start_ms, end_ms)
                total += count
            results[table] = total

        logger.info("duckdb_export_all_complete", file=self._output_path, totals=results)
        return results
