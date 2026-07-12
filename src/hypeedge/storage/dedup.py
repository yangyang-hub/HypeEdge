"""Deduplication filter for ClickHouse writes (Phase 1B).

Prevents duplicate rows from being written to ClickHouse by tracking
seen keys in an in-memory set with configurable capacity.
When capacity is reached, the oldest entries are evicted (FIFO).
"""

from __future__ import annotations

from collections import OrderedDict

import structlog

logger = structlog.get_logger(__name__)


class DedupFilter:
    """Thread-safe (single event-loop) deduplication filter using an ordered set.

    Maintains a bounded set of seen keys per table. When the total key count
    exceeds ``max_keys``, the oldest entries are evicted to keep memory bounded.

    This is intentionally simple: a Python ``OrderedDict`` with LRU-style eviction.
    For 1M keys at ~64 bytes each, memory usage is ~64MB — acceptable for a
    personal trading system.
    """

    def __init__(self, max_keys: int = 1_000_000) -> None:
        self._max_keys = max_keys
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._dedup_count: int = 0

    def is_duplicate(self, table: str, key: str) -> bool:
        """Check if a key has been seen before.

        Returns:
            True if the key was previously marked as seen (duplicate).
        """
        composite = f"{table}:{key}"
        return composite in self._seen

    def mark_seen(self, table: str, key: str) -> None:
        """Mark a key as seen. Evicts oldest entries if at capacity."""
        composite = f"{table}:{key}"
        if composite in self._seen:
            # Move to end (most recently used)
            self._seen.move_to_end(composite)
            return

        self._seen[composite] = None

        # Evict oldest entries if over capacity
        while len(self._seen) > self._max_keys:
            self._seen.popitem(last=False)

    def check_and_mark(self, table: str, key: str) -> bool:
        """Check if duplicate, and if not, mark as seen.

        Returns:
            True if the key is a duplicate (already seen).
        """
        if self.is_duplicate(table, key):
            self._dedup_count += 1
            return True
        self.mark_seen(table, key)
        return False

    def reset(self, table: str | None = None) -> None:
        """Reset the filter. If table is given, only reset that table's entries."""
        if table is None:
            self._seen.clear()
            self._dedup_count = 0
            return

        prefix = f"{table}:"
        keys_to_remove = [k for k in self._seen if k.startswith(prefix)]
        for k in keys_to_remove:
            del self._seen[k]

    @property
    def stats(self) -> dict[str, int]:
        """Return filter statistics."""
        return {
            "seen_keys": len(self._seen),
            "max_keys": self._max_keys,
            "dedup_count": self._dedup_count,
        }
