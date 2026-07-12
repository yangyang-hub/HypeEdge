"""Tests for research-only shadow quote lifecycle state."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from hypeedge.core.enums import ActionBudgetMode, QuoteDecision, Side
from hypeedge.core.types import Price, Size, StrategyId, Symbol, Usd
from hypeedge.strategy.market_maker.shadow import ShadowOrderState
from hypeedge.trading.quote_coordinator import QuoteCoordinator, QuoteCoordinatorConfig
from hypeedge.trading.quotes import DesiredQuote, DesiredQuoteSet, QuoteSlotKey

NOW = datetime(2026, 7, 11, tzinfo=UTC)


def _desired(revision: int, current_slot_revision: int) -> DesiredQuoteSet:
    strategy_id = StrategyId("mm")
    symbol = Symbol("BTC")
    return DesiredQuoteSet(
        strategy_id=strategy_id,
        symbol=symbol,
        session_id="shadow",
        config_version=1,
        model_version="v1",
        market_version=revision,
        connection_generation=1,
        current_slot_revision=current_slot_revision,
        revision=revision,
        fair_price=Price("100"),
        reservation_price=Price("100"),
        inventory_notional=Usd("0"),
        expected_utility_usdc=Usd("1"),
        budget_mode=ActionBudgetMode.NORMAL,
        bid=DesiredQuote(
            QuoteSlotKey(strategy_id, symbol, Side.BUY),
            QuoteDecision.QUOTE,
            Price("99"),
            Size("1"),
            Usd("0.5"),
            "test",
        ),
        ask=DesiredQuote(
            QuoteSlotKey(strategy_id, symbol, Side.SELL),
            QuoteDecision.QUOTE,
            Price("101"),
            Size("1"),
            Usd("0.5"),
            "test",
        ),
        created_at=NOW,
        valid_until=NOW + timedelta(seconds=30),
    )


def test_shadow_state_turns_initial_places_into_later_keeps() -> None:
    state = ShadowOrderState()
    coordinator = QuoteCoordinator(QuoteCoordinatorConfig())
    bid, ask = state.views(StrategyId("mm"), Symbol("BTC"))
    first = coordinator.coordinate(_desired(1, 0), bid, ask, tick_size=Price("0.1"), now=NOW)
    estimate = state.apply(first, now=NOW)
    assert estimate.optimistic == 2

    bid, ask = state.views(StrategyId("mm"), Symbol("BTC"))
    second = coordinator.coordinate(_desired(2, 1), bid, ask, tick_size=Price("0.1"), now=NOW)
    assert all(diff.estimated_incremental_actions == 0 for diff in second.diffs)


def test_shadow_partial_fill_reduces_remaining_size_without_replenishing() -> None:
    state = ShadowOrderState()
    coordinator = QuoteCoordinator(QuoteCoordinatorConfig())
    bid, ask = state.views(StrategyId("mm"), Symbol("BTC"))
    state.apply(coordinator.coordinate(_desired(1, 0), bid, ask, tick_size=Price("0.1"), now=NOW), now=NOW)
    bid_key = QuoteSlotKey(StrategyId("mm"), Symbol("BTC"), Side.BUY)
    state.simulate_fill(bid_key, size=Size("0.4"))
    bid, _ = state.views(StrategyId("mm"), Symbol("BTC"))
    assert bid.current_owner is not None
    assert bid.current_owner.remaining_size == Size("0.6")
