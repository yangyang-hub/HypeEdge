"""Unit tests for API abuse controls."""

from __future__ import annotations

from starlette.requests import Request

from hypeedge.api.security import SlidingWindowLimiter, client_ip_for_rate_limit


def _request(*, peer: str, forwarded_for: str | None = None) -> Request:
    headers: list[tuple[bytes, bytes]] = []
    if forwarded_for is not None:
        headers.append((b"x-forwarded-for", forwarded_for.encode()))
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/api/v1/system/status",
        "raw_path": b"/api/v1/system/status",
        "query_string": b"",
        "headers": headers,
        "client": (peer, 12345),
        "server": ("127.0.0.1", 37001),
    }
    return Request(scope)


def test_sliding_window_limiter_blocks_after_limit() -> None:
    limiter = SlidingWindowLimiter(window_seconds=60.0)
    assert limiter.allow("k", 1) is True
    assert limiter.allow("k", 1) is False


def test_rate_limit_uses_peer_when_not_loopback() -> None:
    request = _request(peer="192.168.31.20", forwarded_for="10.0.0.1")
    assert client_ip_for_rate_limit(request) == "192.168.31.20"


def test_rate_limit_uses_forwarded_for_behind_local_proxy() -> None:
    request = _request(peer="127.0.0.1", forwarded_for="192.168.31.20, 127.0.0.1")
    assert client_ip_for_rate_limit(request) == "192.168.31.20"


def test_rate_limit_ignores_invalid_forwarded_for() -> None:
    request = _request(peer="127.0.0.1", forwarded_for="not-an-ip")
    assert client_ip_for_rate_limit(request) == "127.0.0.1"
