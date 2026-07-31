"""Tests for perpetual/spot metadata identity and precision parsing."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from hypeedge.core.types import Symbol
from hypeedge.market_data.instrument_cache import InstrumentMetaCache


async def test_spot_display_pair_resolves_to_exchange_coin() -> None:
    rest = MagicMock()
    rest.get_meta = AsyncMock(
        return_value={
            "universe": [
                {
                    "name": "HYPE",
                    "szDecimals": 2,
                    "maxLeverage": 10,
                    "onlyIsolated": True,
                    "marginMode": "strictIsolated",
                }
            ]
        }
    )
    rest.get_spot_meta = AsyncMock(
        return_value={
            "tokens": [
                {"name": "USDC", "index": 0, "szDecimals": 8},
                {"name": "HYPE", "index": 1105, "szDecimals": 2},
            ],
            "universe": [{"name": "@1035", "index": 1035, "tokens": [1105, 0]}],
        }
    )
    cache = InstrumentMetaCache(rest)

    await cache.ensure_loaded()

    spot = cache.resolve_spot("HYPE/USDC")
    assert spot is not None
    assert spot.symbol == Symbol("@1035")
    assert spot.base_token == "HYPE"
    assert spot.quote_token == "USDC"
    assert spot.sz_decimals == 2
    assert spot.max_price_decimals == 6
    assert cache.resolve_spot("@1035") == spot
    perp = cache.get(Symbol("HYPE"))
    assert perp is not None and perp.only_isolated is True
    assert perp.max_price_decimals == 4
