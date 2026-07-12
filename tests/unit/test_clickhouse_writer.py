"""Tests for ClickHouse buffer ownership and retry behavior."""

from __future__ import annotations

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
