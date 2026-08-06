"""Tests for rate-limited automatic spot/perpetual market discovery."""

from __future__ import annotations

from typing import Any

from hypeedge.config.settings import FundingArbSettings
from hypeedge.core.types import Symbol
from hypeedge.market_data.book import BookManager
from hypeedge.market_data.funding_arb_scanner import HyperliquidFundingArbMarketScanner
from hypeedge.market_data.instrument_cache import InstrumentMetaCache


class _Rest:
    def __init__(self) -> None:
        self.perp_meta: dict[str, Any] = {
            "universe": [
                {"name": "HYPE", "szDecimals": 2, "maxLeverage": 10},
                {"name": "PURR", "szDecimals": 2, "maxLeverage": 10},
                {"name": "HYPEX", "szDecimals": 2, "maxLeverage": 10},
            ]
        }
        self.perp_contexts: list[dict[str, str]] = [
            {"funding": "0.001", "dayNtlVlm": "100000"},
            {"funding": "0.002", "dayNtlVlm": "60000"},
            {"funding": "0.003", "dayNtlVlm": "90000"},
        ]
        self.spot_meta: dict[str, Any] = {
            "tokens": [
                {"name": "HYPE", "index": 0, "szDecimals": 2},
                {"name": "USDC", "index": 1, "szDecimals": 2},
                {"name": "PURR", "index": 2, "szDecimals": 2},
                {"name": "HYPEX", "index": 3, "szDecimals": 2},
                {"name": "USDT", "index": 4, "szDecimals": 2},
                {"name": "HYPE2", "index": 5, "szDecimals": 2},
            ],
            "universe": [
                {"name": "@1", "tokens": [0, 1]},
                {"name": "@2", "tokens": [2, 1]},
                {"name": "@3", "tokens": [3, 4]},
                {"name": "@4", "tokens": [5, 1]},
            ],
        }
        self.spot_contexts: list[dict[str, str]] = [
            {"coin": "@1", "dayNtlVlm": "50000"},
            {"coin": "@2", "dayNtlVlm": "70000"},
            {"coin": "@3", "dayNtlVlm": "80000"},
            {"coin": "@4", "dayNtlVlm": "1000000"},
        ]
        self.meta_context_calls = 0
        self.spot_context_calls = 0
        self.l2_calls: list[str] = []

    async def get_meta(self) -> dict[str, Any]:
        return self.perp_meta

    async def get_spot_meta(self) -> dict[str, Any]:
        return self.spot_meta

    async def get_meta_and_asset_ctxs(self) -> list[Any]:
        self.meta_context_calls += 1
        return [self.perp_meta, self.perp_contexts]

    async def get_spot_meta_and_asset_ctxs(self) -> list[Any]:
        self.spot_context_calls += 1
        return [self.spot_meta, self.spot_contexts]

    async def get_l2_book(self, coin: str) -> dict[str, Any]:
        self.l2_calls.append(coin)
        return {
            "coin": coin,
            "time": 1_786_000_000_000,
            "levels": [
                [{"px": "99.9", "sz": "10"}, {"px": "99.8", "sz": "5"}],
                [{"px": "100.1", "sz": "10"}, {"px": "100.2", "sz": "5"}],
            ],
        }


async def _scanner(
    *,
    max_candidate_markets: int,
) -> tuple[HyperliquidFundingArbMarketScanner, _Rest, BookManager]:
    rest = _Rest()
    metadata = InstrumentMetaCache(rest)  # type: ignore[arg-type]
    await metadata.ensure_loaded()
    books = BookManager()
    scanner = HyperliquidFundingArbMarketScanner(
        rest,  # type: ignore[arg-type]
        metadata,
        books,
        FundingArbSettings(max_candidate_markets=max_candidate_markets),
    )
    return scanner, rest, books


async def test_exact_token_join_keeps_only_usdc_markets_with_same_named_perp() -> None:
    scanner, rest, _ = await _scanner(max_candidate_markets=8)

    descriptors = scanner._parse_universe(
        [rest.perp_meta, rest.perp_contexts],
        [rest.spot_meta, rest.spot_contexts],
    )

    assert {(item.perp_symbol, item.spot_symbol) for item in descriptors} == {
        (Symbol("HYPE"), Symbol("@1")),
        (Symbol("PURR"), Symbol("@2")),
    }
    # HYPEX/USDT is rejected by quote identity and HYPE2 never fuzzy-matches HYPE.
    assert rest.l2_calls == []


async def test_top_volume_books_are_cached_and_injected_into_shared_manager() -> None:
    scanner, rest, books = await _scanner(max_candidate_markets=1)

    first = await scanner.scan()
    second = await scanner.scan()

    assert len(first) == 1
    assert first[0].perp_symbol == Symbol("PURR")
    assert first[0].spot_symbol == Symbol("@2")
    assert second == first
    assert rest.meta_context_calls == 1
    assert rest.spot_context_calls == 1
    assert rest.l2_calls == ["PURR", "@2"]
    assert books.get_snapshot(Symbol("PURR")) == first[0].perp_book
    assert books.get_snapshot(Symbol("@2")) == first[0].spot_book

    selected = await scanner.get_market(Symbol("HYPE"), Symbol("@1"))
    assert selected is not None
    assert selected.spot_display == "HYPE/USDC"
    assert rest.l2_calls == ["PURR", "@2", "HYPE", "@1"]
    assert books.get_snapshot(Symbol("@1")) == selected.spot_book


async def test_empty_universe_is_cached_instead_of_polling_every_strategy_tick() -> None:
    scanner, rest, _ = await _scanner(max_candidate_markets=8)
    rest.spot_meta["universe"] = []
    rest.spot_contexts = []

    assert await scanner.scan() == ()
    assert await scanner.scan() == ()

    assert rest.meta_context_calls == 1
    assert rest.spot_context_calls == 1
    assert rest.l2_calls == []
