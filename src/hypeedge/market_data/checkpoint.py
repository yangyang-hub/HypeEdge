"""Backfill checkpoint store — JSON file persistence for backfill progress (Phase 1B).

Tracks the last successfully fetched timestamp per (endpoint, coin, interval)
so that backfill can resume from where it left off after restarts.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from pathlib import Path

import structlog

logger = structlog.get_logger(__name__)


class BackfillCheckpointStore:
    """JSON-file-backed store for backfill progress.

    Each entry is keyed by ``f"{endpoint}:{coin}:{interval}"`` and stores
    the last successfully processed millisecond timestamp.

    File format::

        {
            "candleSnapshot:BTC:1m": 1717200000000,
            "fundingHistory:ETH:1h": 1717196400000
        }

    Uses atomic writes (write to temp file, then rename) to prevent corruption.
    """

    def __init__(self, state_dir: str) -> None:
        self._state_dir = Path(state_dir)
        self._path = self._state_dir / "backfill_checkpoints.json"
        self._data: dict[str, int] = {}

    def load(self) -> None:
        """Load checkpoints from disk. Creates the file if it doesn't exist."""
        if self._path.exists():
            try:
                text = self._path.read_text(encoding="utf-8")
                self._data = json.loads(text)
                logger.info("checkpoints_loaded", path=str(self._path), entries=len(self._data))
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("checkpoints_load_failed", error=str(e), path=str(self._path))
                self._data = {}
        else:
            self._data = {}
            logger.info("checkpoints_no_file", path=str(self._path))

    def get(self, endpoint: str, coin: str, interval: str) -> int | None:
        """Get the last successful timestamp for a backfill key.

        Returns:
            Millisecond timestamp, or None if no checkpoint exists.
        """
        key = self._make_key(endpoint, coin, interval)
        return self._data.get(key)

    def save(self, endpoint: str, coin: str, interval: str, last_ts: int) -> None:
        """Update the checkpoint for a backfill key and flush to disk."""
        key = self._make_key(endpoint, coin, interval)
        self._data[key] = last_ts
        self._flush()

    def all_entries(self) -> dict[str, int]:
        """Return all checkpoint entries (for inspection/debugging)."""
        return dict(self._data)

    @staticmethod
    def _make_key(endpoint: str, coin: str, interval: str) -> str:
        return f"{endpoint}:{coin}:{interval}"

    def _flush(self) -> None:
        """Write checkpoints to disk atomically."""
        self._state_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Atomic write: write to temp file in same dir, then rename
            fd, tmp_path = tempfile.mkstemp(
                dir=str(self._state_dir),
                prefix=".backfill_checkpoints_",
                suffix=".tmp",
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(self._data, f, indent=2)
                # rename is atomic on POSIX
                os.replace(tmp_path, str(self._path))
            except BaseException:
                # Clean up temp file on any error
                with contextlib.suppress(OSError):
                    os.unlink(tmp_path)
                raise
            logger.debug("checkpoints_flushed", entries=len(self._data))
        except OSError as e:
            logger.error("checkpoints_flush_failed", error=str(e), path=str(self._path))
