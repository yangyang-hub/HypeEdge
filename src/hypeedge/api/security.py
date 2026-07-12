"""Small in-process abuse controls for the single-process API server."""

from __future__ import annotations

import ipaddress
import time
from collections import defaultdict, deque

from starlette.requests import Request


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


def _is_loopback(host: str) -> bool:
    if host in {"localhost", "unknown", ""}:
        return host == "localhost"
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def client_ip_for_rate_limit(request: Request) -> str:
    """Prefer X-Forwarded-For when the immediate peer is the local Next.js proxy."""
    peer = request.client.host if request.client is not None else "unknown"
    if not _is_loopback(peer):
        return peer
    forwarded = request.headers.get("x-forwarded-for", "")
    if not forwarded:
        return peer
    # First hop is the original client; ignore untrusted later proxies on intranet.
    candidate = forwarded.split(",", 1)[0].strip()
    if not candidate:
        return peer
    try:
        ipaddress.ip_address(candidate)
    except ValueError:
        return peer
    return candidate
