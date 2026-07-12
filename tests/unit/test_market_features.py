"""Tests for market-maker microstructure feature calculation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from hypeedge.core.enums import Side
from hypeedge.core.models import L2BookSnapshot, L2Level, Trade
from hypeedge.core.types import Price, Size, Symbol, Timestamp
from hypeedge.market_data.features import MarketFeatureEngine


def _book(*, ts: datetime, version: int = 1, bid_size: str = "2", ask_size: str = "1") -> L2BookSnapshot:
    return L2BookSnapshot(
        symbol=Symbol("BTC"),
        bids=(L2Level(Price("99"), Size(bid_size)),),
        asks=(L2Level(Price("101"), Size(ask_size)),),
        timestamp=Timestamp(int(ts.timestamp() * 1000)),
        local_ts=ts,
        version=version,
        connection_generation=3,
    )


def test_microprice_weights_toward_ask_when_bid_size_is_larger() -> None:
    now = datetime(2026, 7, 11, tzinfo=UTC)
    features = MarketFeatureEngine().build(_book(ts=now), healthy=True)
    assert features.microprice > features.mid_price
    assert features.normalized_ofi == Decimal("0.3333333333333333333333333333")


def test_signed_trade_flow_and_short_return() -> None:
    now = datetime(2026, 7, 11, tzinfo=UTC)
    engine = MarketFeatureEngine()
    engine.observe_book(_book(ts=now))
    engine.observe_trade(
        Trade(
            symbol=Symbol("BTC"),
            price=Price("100"),
            size=Size("2"),
            side=Side.BUY,
            tid=1,
            timestamp=Timestamp(1),
            local_ts=now,
        )
    )
    later = now + timedelta(seconds=1)
    moved = L2BookSnapshot(
        symbol=Symbol("BTC"),
        bids=(L2Level(Price("100"), Size("2")),),
        asks=(L2Level(Price("102"), Size("1")),),
        timestamp=Timestamp(2),
        local_ts=later,
        version=2,
        connection_generation=3,
    )
    features = engine.build(moved, healthy=True)
    assert features.trade_flow == Decimal("1")
    assert features.short_return > 0
    assert features.market_version == 2
