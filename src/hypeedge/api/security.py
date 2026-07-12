"""Small in-process abuse controls for the single-process API server."""

from __future__ import annotations

import time
from collections import defaultdict, deque


class SlidingWindowLimiter:
    """Bound events per key in a monotonic sliding window."""

    def __init__(self, window_seconds: float = 60.0, max_keys: int = 10_000) -> None:
        self._window_seconds = window_seconds
        self._max_keys = max_keys
        self._events: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, key: str, limit: int) -> bool:
        now = time.monotonic()
        if key not in self._events and len(self._events) >= self._max_keys:
            self._events.pop(next(iter(self._events)))
        events = self._events[key]
        cutoff = now - self._window_seconds
        while events and events[0] <= cutoff:
            events.popleft()
        if len(events) >= limit:
            return False
        events.append(now)
        return True
