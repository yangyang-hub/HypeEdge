"""Tests for DuckDB export utility."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

duckdb = pytest.importorskip("duckdb", reason="duckdb not installed")

from hypeedge.config.settings import ClickHouseSettings  # noqa: E402
from hypeedge.storage.duckdb_export import DuckDBExporter  # noqa: E402


class TestDuckDBExporter:
    def test_init(self):
        settings = ClickHouseSettings()
        exporter = DuckDBExporter(settings, "test.duckdb")
        assert exporter._output_path == "test.duckdb"

    @pytest.mark.asyncio
    async def test_export_table_no_data(self):
        """Export returns 0 when ClickHouse has no data."""
        settings = ClickHouseSettings()
        exporter = DuckDBExporter(settings, "test.duckdb")

        mock_ch = MagicMock()
        mock_ch.query.return_value = MagicMock(result_rows=[], column_names=["ts", "coin"])

        with patch("clickhouse_connect.get_client", return_value=mock_ch), patch("duckdb.connect"):
            count = await exporter.export_table("candles", "BTC", 1000, 2000)

        assert count == 0

    @pytest.mark.asyncio
    async def test_export_table_with_data(self):
        """Export returns correct row count."""
        settings = ClickHouseSettings()
        exporter = DuckDBExporter(settings, "test.duckdb")

        mock_ch = MagicMock()
        mock_ch.query.return_value = MagicMock(
            result_rows=[[1.0, "BTC"], [2.0, "BTC"], [3.0, "BTC"]],
            column_names=["ts", "coin"],
        )

        mock_duckdb = MagicMock()

        with (
            patch("clickhouse_connect.get_client", return_value=mock_ch),
            patch("duckdb.connect", return_value=mock_duckdb),
        ):
            count = await exporter.export_table("candles", "BTC", 1000, 2000)

        assert count == 3

    @pytest.mark.asyncio
    async def test_export_all_aggregates_counts(self):
        """export_all sums rows across coins per table."""
        settings = ClickHouseSettings()
        exporter = DuckDBExporter(settings, "test.duckdb")

        # Mock export_table to return fixed counts
        call_log: list[tuple[str, str]] = []

        async def mock_export(table: str, coin: str, start_ms: int, end_ms: int) -> int:
            call_log.append((table, coin))
            return 5

        exporter.export_table = mock_export  # type: ignore[assignment]

        results = await exporter.export_all(["BTC", "ETH"], 1000, 2000)

        # 5 tables * 2 coins = 10 calls
        assert len(call_log) == 10
        # Each table should have 5+5=10 rows
        for _table, count in results.items():
            assert count == 10
        # All 5 tables should be present
        assert set(results.keys()) == {"candles", "funding", "trades", "l2_book", "mid_prices"}
