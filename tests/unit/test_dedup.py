"""Tests for DedupFilter."""

from hypeedge.storage.dedup import DedupFilter


class TestDedupFilter:
    def test_new_key_not_duplicate(self) -> None:
        f = DedupFilter(max_keys=100)
        assert f.check_and_mark("candles", "BTC:1m:1000") is False

    def test_same_key_is_duplicate(self) -> None:
        f = DedupFilter(max_keys=100)
        f.check_and_mark("candles", "BTC:1m:1000")
        assert f.check_and_mark("candles", "BTC:1m:1000") is True

    def test_different_tables_not_duplicate(self) -> None:
        f = DedupFilter(max_keys=100)
        f.check_and_mark("candles", "BTC:1m:1000")
        # Same key but different table → not duplicate
        assert f.check_and_mark("funding", "BTC:1m:1000") is False

    def test_different_keys_not_duplicate(self) -> None:
        f = DedupFilter(max_keys=100)
        f.check_and_mark("candles", "BTC:1m:1000")
        assert f.check_and_mark("candles", "BTC:1m:1001") is False

    def test_eviction_when_full(self) -> None:
        f = DedupFilter(max_keys=3)
        f.check_and_mark("candles", "key1")
        f.check_and_mark("candles", "key2")
        f.check_and_mark("candles", "key3")
        # Adding a 4th key should evict the oldest (key1)
        f.check_and_mark("candles", "key4")
        assert f.is_duplicate("candles", "key1") is False  # evicted
        assert f.is_duplicate("candles", "key4") is True

    def test_mark_seen_is_idempotent(self) -> None:
        f = DedupFilter(max_keys=100)
        f.mark_seen("candles", "BTC:1000")
        f.mark_seen("candles", "BTC:1000")  # second call should not raise
        assert f.is_duplicate("candles", "BTC:1000") is True

    def test_reset_all(self) -> None:
        f = DedupFilter(max_keys=100)
        f.check_and_mark("candles", "BTC:1000")
        f.check_and_mark("funding", "ETH:2000")
        f.reset()
        assert f.is_duplicate("candles", "BTC:1000") is False
        assert f.is_duplicate("funding", "ETH:2000") is False
        assert f.stats["dedup_count"] == 0

    def test_reset_by_table(self) -> None:
        f = DedupFilter(max_keys=100)
        f.check_and_mark("candles", "BTC:1000")
        f.check_and_mark("funding", "ETH:2000")
        f.reset(table="candles")
        assert f.is_duplicate("candles", "BTC:1000") is False
        assert f.is_duplicate("funding", "ETH:2000") is True

    def test_stats(self) -> None:
        f = DedupFilter(max_keys=100)
        f.check_and_mark("candles", "BTC:1000")
        f.check_and_mark("candles", "BTC:1000")  # duplicate
        stats = f.stats
        assert stats["seen_keys"] == 1
        assert stats["dedup_count"] == 1
