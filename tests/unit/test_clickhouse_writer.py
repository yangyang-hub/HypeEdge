"""Tests for ClickHouse buffer ownership and retry behavior."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

from hypeedge.config.settings import ClickHouseSettings
from hypeedge.core.events import EventBus
from hypeedge.storage.clickhouse import ClickHouseWriter


async def test_failed_flush_spools_detached_batch(tmp_path) -> None:
    writer = ClickHouseWriter(
        ClickHouseSettings(batch_size=100, spool_path=str(tmp_path / "spool.sqlite3")),
        EventBus(),
    )
    writer._client = MagicMock()
    writer._client.insert.side_effect = RuntimeError("clickhouse unavailable")
    writer._trade_rows = [{"ts": 1, "coin": "BTC"}]
    await writer._spool.initialize()

    await writer._flush_buffer("_trade_rows", "trades")

    assert writer._trade_rows == []
    pending = await writer._spool.pending()
    assert pending[0][1:] == ("trades", [{"ts": 1, "coin": "BTC"}])


async def test_successful_flush_clears_only_detached_batch(tmp_path) -> None:
    writer = ClickHouseWriter(
        ClickHouseSettings(batch_size=100, spool_path=str(tmp_path / "spool.sqlite3")),
        EventBus(),
    )
    writer._client = MagicMock()
    writer._trade_rows = [{"ts": 1, "coin": "BTC"}]

    await writer._flush_buffer("_trade_rows", "trades")

    assert writer._trade_rows == []
    writer._client.insert.assert_called_once()


def test_normalize_cell_converts_float_seconds_to_datetime() -> None:
    ts = ClickHouseWriter._normalize_cell("ts", 1_783_839_000.123)
    assert isinstance(ts, datetime)
    assert ts.tzinfo is UTC
    assert abs(ts.timestamp() - 1_783_839_000.123) < 1e-6


def test_normalize_cell_converts_decimal_prices() -> None:
    assert ClickHouseWriter._normalize_cell("px", Decimal("123.45")) == 123.45


def test_rows_to_column_data_normalizes_ts() -> None:
    writer = ClickHouseWriter(ClickHouseSettings(), EventBus())
    columns, data = writer._rows_to_column_data([{"ts": 1_700_000_000.0, "coin": "BTC", "px": Decimal("1")}])
    assert columns == ["ts", "coin", "px"]
    assert isinstance(data[0][0], datetime)
    assert data[0][1] == "BTC"
    assert data[0][2] == 1.0
