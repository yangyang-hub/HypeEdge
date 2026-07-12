"""Tests for the in-memory order book."""

from datetime import UTC, datetime, timedelta

from hypeedge.core.types import Symbol, Timestamp
from hypeedge.market_data.book import BookManager, OrderBook


class TestOrderBook:
    def test_update_and_read(self):
        book = OrderBook(Symbol("BTC"), depth=5)

        book.update(
            bids=[(50000.0, 1.0), (49999.0, 2.0)],
            asks=[(50001.0, 1.5), (50002.0, 0.5)],
            ts=Timestamp(1000),
        )

        assert book.best_bid == 50000.0
        assert book.best_ask == 50001.0
        assert book.mid_price == 50000.5
        assert book.spread == 1.0
        assert abs(book.spread_bps - 0.2) < 0.01  # 1.0 / 50000.5 * 10000 ≈ 0.2 bps

    def test_snapshot(self):
        book = OrderBook(Symbol("ETH"), depth=3)

        book.update(
            bids=[(3000.0, 10.0), (2999.0, 20.0), (2998.0, 30.0)],
            asks=[(3001.0, 15.0), (3002.0, 25.0), (3003.0, 35.0)],
            ts=Timestamp(2000),
        )

        snap = book.get_snapshot()
        assert snap is not None
        assert snap.symbol == Symbol("ETH")
        assert len(snap.bids) == 3
        assert len(snap.asks) == 3
        assert snap.timestamp == Timestamp(2000)

    def test_empty_book_returns_none(self):
        book = OrderBook(Symbol("BTC"))
        assert book.get_snapshot() is None
        assert book.best_bid is None
        assert book.best_ask is None
        assert book.mid_price is None

    def test_depth_truncation(self):
        book = OrderBook(Symbol("BTC"), depth=2)

        book.update(
            bids=[(50000.0, 1.0), (49999.0, 2.0), (49998.0, 3.0)],
            asks=[(50001.0, 1.5), (50002.0, 0.5), (50003.0, 0.1)],
            ts=Timestamp(1000),
        )

        snap = book.get_snapshot()
        assert len(snap.bids) == 2  # Truncated to depth
        assert len(snap.asks) == 2

    def test_overwrite_updates(self):
        """Latest update should replace previous data."""
        book = OrderBook(Symbol("BTC"))

        book.update(bids=[(50000.0, 1.0)], asks=[(50001.0, 2.0)], ts=Timestamp(1000))
        assert book.best_bid == 50000.0

        book.update(bids=[(50010.0, 5.0)], asks=[(50011.0, 3.0)], ts=Timestamp(2000))
        assert book.best_bid == 50010.0

    def test_reads_do_not_refresh_snapshot_freshness_or_version(self):
        book = OrderBook(Symbol("BTC"))
        received_at = datetime(2026, 7, 11, tzinfo=UTC)
        first = book.update(
            bids=[(50000.0, 1.0)],
            asks=[(50001.0, 1.0)],
            ts=Timestamp(1000),
            received_at=received_at,
            connection_generation=7,
        )

        assert book.get_snapshot() is first
        assert book.get_snapshot() is first
        assert first.exchange_ts == Timestamp(1000)
        assert first.received_at == received_at
        assert first.version == 1
        assert first.connection_generation == 7
        assert isinstance(first.bids, tuple)

        second = book.update(
            bids=[(50010.0, 1.0)],
            asks=[(50011.0, 1.0)],
            ts=Timestamp(2000),
            received_at=received_at + timedelta(seconds=1),
            connection_generation=8,
        )
        assert second.version == 2
        assert second.received_at == received_at + timedelta(seconds=1)
        assert first.version == 1
        assert first.received_at == received_at


class TestBookManager:
    def test_get_or_create(self):
        mgr = BookManager(depth=10)

        book = mgr.get_book(Symbol("BTC"))
        assert book is not None
        assert book.symbol == Symbol("BTC")

        # Same symbol returns same book
        book2 = mgr.get_book(Symbol("BTC"))
        assert book is book2

    def test_get_snapshot(self):
        mgr = BookManager()
        book = mgr.get_book(Symbol("BTC"))
        book.update(bids=[(50000.0, 1.0)], asks=[(50001.0, 1.0)], ts=Timestamp(1000))

        snap = mgr.get_snapshot(Symbol("BTC"))
        assert snap is not None
        assert snap.symbol == Symbol("BTC")

        assert mgr.get_snapshot(Symbol("ETH")) is None

    def test_get_mid_price(self):
        mgr = BookManager()
        book = mgr.get_book(Symbol("BTC"))
        book.update(bids=[(50000.0, 1.0)], asks=[(50002.0, 1.0)], ts=Timestamp(1000))

        assert mgr.get_mid_price(Symbol("BTC")) == 50001.0
        assert mgr.get_mid_price(Symbol("ETH")) is None

    def test_symbols(self):
        mgr = BookManager()
        mgr.get_book(Symbol("BTC"))
        mgr.get_book(Symbol("ETH"))

        symbols = mgr.symbols
        assert Symbol("BTC") in symbols
        assert Symbol("ETH") in symbols
