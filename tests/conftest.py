"""Shared pytest fixtures for HypeEdge tests."""

import pytest

from hypeedge.core.events import EventBus


@pytest.fixture
def event_bus() -> EventBus:
    """Provide a fresh EventBus for each test."""
    return EventBus(queue_maxsize=100)
