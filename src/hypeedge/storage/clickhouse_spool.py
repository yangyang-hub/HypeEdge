"""Durable SQLite spool for ClickHouse batches."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


class ClickHouseSpool:
    """Stores detached batches until ClickHouse acknowledges them."""

    def __init__(self, path: str) -> None:
        self._path = Path(path)

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_sync)

    async def put(self, table: str, rows: list[dict[str, Any]]) -> str:
        batch_id = uuid.uuid4().hex
        payload = json.dumps(rows, default=self._json_default, separators=(",", ":"))
        await asyncio.to_thread(self._put_sync, batch_id, table, payload)
        return batch_id

    async def pending(self, limit: int = 100) -> list[tuple[str, str, list[dict[str, Any]]]]:
        records = await asyncio.to_thread(self._pending_sync, limit)
        return [
            (batch_id, table, json.loads(payload, object_hook=self._json_object_hook))
            for batch_id, table, payload in records
        ]

    async def acknowledge(self, batch_id: str) -> None:
        await asyncio.to_thread(self._acknowledge_sync, batch_id)

    def _initialize_sync(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self._path) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS clickhouse_batches (
                    batch_id TEXT PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

    def _put_sync(self, batch_id: str, table: str, payload: str) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self._path) as connection:
            connection.execute(
                "INSERT INTO clickhouse_batches (batch_id, table_name, payload) VALUES (?, ?, ?)",
                (batch_id, table, payload),
            )

    def _pending_sync(self, limit: int) -> list[tuple[str, str, str]]:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self._path) as connection:
            rows = connection.execute(
                "SELECT batch_id, table_name, payload FROM clickhouse_batches ORDER BY created_at, batch_id LIMIT ?",
                (limit,),
            ).fetchall()
        return [(str(batch_id), str(table), str(payload)) for batch_id, table, payload in rows]

    def _acknowledge_sync(self, batch_id: str) -> None:
        with sqlite3.connect(self._path) as connection:
            connection.execute("DELETE FROM clickhouse_batches WHERE batch_id = ?", (batch_id,))

    @staticmethod
    def _json_default(value: Any) -> Any:
        if isinstance(value, datetime):
            return {"__datetime__": value.isoformat()}
        if isinstance(value, Decimal):
            return {"__decimal__": str(value)}
        raise TypeError(f"Unsupported ClickHouse spool value: {type(value).__name__}")

    @staticmethod
    def _json_object_hook(value: dict[str, Any]) -> Any:
        encoded = value.get("__datetime__")
        if encoded is not None and len(value) == 1:
            return datetime.fromisoformat(str(encoded))
        decimal_encoded = value.get("__decimal__")
        if decimal_encoded is not None and len(value) == 1:
            return Decimal(str(decimal_encoded))
        return value
