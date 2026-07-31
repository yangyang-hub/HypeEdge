"""Shared pytest fixtures for HypeEdge tests."""

from collections.abc import Iterator
from pathlib import Path

import pytest

from hypeedge.core.events import EventBus


@pytest.fixture(scope="session", autouse=True)
def _isolate_local_env_file() -> Iterator[None]:
    """Hide the developer's local .env so unit tests assert code defaults.

    pydantic-settings reads '.env' relative to cwd; without isolation a developer's
    local config (LOG_LEVEL, CLICKHOUSE host, exchange keys, API tokens) leaks into
    default-value assertions and fails them. We rename .env aside for the test
    session and restore it on teardown.
    """

    env_path = Path(".env")
    backup = Path(".env.__pytest_disabled__")
    renamed = False
    if env_path.exists() and not backup.exists():
        env_path.rename(backup)
        renamed = True
    yield
    if renamed:
        if env_path.exists():
            env_path.unlink()
        backup.rename(env_path)


@pytest.fixture
def event_bus() -> EventBus:
    """Provide a fresh EventBus for each test."""
    return EventBus(queue_maxsize=100)
