"""Tests for BackfillCheckpointStore."""

import json
from pathlib import Path

import pytest

from hypeedge.market_data.checkpoint import BackfillCheckpointStore


@pytest.fixture
def checkpoint_dir(tmp_path: Path) -> Path:
    """Provide a temporary directory for checkpoint files."""
    return tmp_path / "state"


def test_save_and_get(checkpoint_dir: Path) -> None:
    """Save a checkpoint and retrieve it."""
    store = BackfillCheckpointStore(str(checkpoint_dir))
    store.save("candleSnapshot", "BTC", "1m", 1717200000000)

    result = store.get("candleSnapshot", "BTC", "1m")
    assert result == 1717200000000


def test_get_nonexistent_returns_none(checkpoint_dir: Path) -> None:
    """Getting a non-existent key returns None."""
    store = BackfillCheckpointStore(str(checkpoint_dir))
    assert store.get("candleSnapshot", "BTC", "1m") is None


def test_persist_and_reload(checkpoint_dir: Path) -> None:
    """Checkpoints survive across store instances."""
    store1 = BackfillCheckpointStore(str(checkpoint_dir))
    store1.save("candleSnapshot", "BTC", "1m", 1000)
    store1.save("fundingHistory", "ETH", "1h", 2000)

    # Create a new store and load from disk
    store2 = BackfillCheckpointStore(str(checkpoint_dir))
    store2.load()

    assert store2.get("candleSnapshot", "BTC", "1m") == 1000
    assert store2.get("fundingHistory", "ETH", "1h") == 2000
    assert store2.get("candleSnapshot", "ETH", "1m") is None


def test_update_overwrites(checkpoint_dir: Path) -> None:
    """Saving the same key overwrites the previous value."""
    store = BackfillCheckpointStore(str(checkpoint_dir))
    store.save("candleSnapshot", "BTC", "1m", 1000)
    store.save("candleSnapshot", "BTC", "1m", 2000)

    assert store.get("candleSnapshot", "BTC", "1m") == 2000


def test_file_format_is_valid_json(checkpoint_dir: Path) -> None:
    """The persisted file is valid JSON with expected structure."""
    store = BackfillCheckpointStore(str(checkpoint_dir))
    store.save("candleSnapshot", "BTC", "1m", 1717200000000)

    path = checkpoint_dir / "backfill_checkpoints.json"
    assert path.exists()

    with open(path) as f:
        data = json.load(f)

    assert "candleSnapshot:BTC:1m" in data
    assert data["candleSnapshot:BTC:1m"] == 1717200000000


def test_load_from_empty_dir(checkpoint_dir: Path) -> None:
    """Loading from a directory with no checkpoint file works."""
    store = BackfillCheckpointStore(str(checkpoint_dir))
    store.load()
    assert store.all_entries() == {}


def test_load_corrupted_file(checkpoint_dir: Path) -> None:
    """Loading a corrupted JSON file does not crash, returns empty."""
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    (checkpoint_dir / "backfill_checkpoints.json").write_text("not valid json{{{")

    store = BackfillCheckpointStore(str(checkpoint_dir))
    store.load()
    assert store.all_entries() == {}


def test_all_entries(checkpoint_dir: Path) -> None:
    """all_entries returns a copy of all stored checkpoints."""
    store = BackfillCheckpointStore(str(checkpoint_dir))
    store.save("candleSnapshot", "BTC", "1m", 1000)
    store.save("fundingHistory", "ETH", "1h", 2000)

    entries = store.all_entries()
    assert len(entries) == 2
    assert entries["candleSnapshot:BTC:1m"] == 1000
