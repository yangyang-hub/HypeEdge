"""Tests for the pure inventory-aware market-making policy."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from hypeedge.core.enums import ActionBudgetMode, QuoteDecision
from hypeedge.core.types import Price, Size, StrategyId, Symbol, Usd
from hypeedge.strategy.market_maker import (
    ActionBudgetSnapshot,
    FairValueModel,
    InventoryController,
    InventorySnapshot,
    MarketFeatures,
    MarketMakerConfig,
    MarketMakerPolicy,
)

NOW = datetime(2026, 7, 11, tzinfo=UTC)


def _features(**overrides: object) -> MarketFeatures:
    values: dict[str, object] = {
        "symbol": Symbol("BTC"),
        "market_version": 10,
        "connection_generation": 2,
        "exchange_ts": 1,
        "received_at": NOW,
        "healthy": True,
        "best_bid": Price("99.9"),
        "best_ask": Price("100.1"),
        "best_bid_size": Size("5"),
        "best_ask_size": Size("5"),
        "microprice": Price("100.02"),
        "normalized_ofi": Decimal("0.2"),
        "trade_flow": Decimal("0.1"),
        "short_return": Decimal("0"),
        "return_variance_per_second": Decimal("0.000001"),
        "expected_adverse_markout_bps": Decimal("0.2"),
        "latency_buffer_bps": Decimal("0.1"),
        "toxicity": Decimal("0.1"),
        "funding_rate": Decimal("0.0001"),
    }
    values.update(overrides)
    return MarketFeatures(**values)  # type: ignore[arg-type]


def _config(**overrides: object) -> MarketMakerConfig:
    values: dict[str, object] = {
        "version": 1,
        "model_version": "mm-v1",
        "tick_size": Decimal("0.1"),
        "lot_size": Decimal("0.001"),
        "min_size": Decimal("0.001"),
        "soft_inventory_notional": Usd("100"),
        "hard_inventory_notional": Usd("150"),
        "emergency_inventory_notional": Usd("200"),
        "quote_size": Size("0.1"),
        "max_depth_participation": Decimal("0.1"),
    }
    values.update(overrides)
    return MarketMakerConfig(**values)  # type: ignore[arg-type]


def _inventory(size: str = "0") -> InventorySnapshot:
    return InventorySnapshot(
        position_size=Size(size),
        equity=Usd("1000"),
        available_balance=Usd("900"),
        margin_used=Usd("100"),
        observed_at=NOW,
        healthy=True,
    )


def _budget(mode: ActionBudgetMode = ActionBudgetMode.NORMAL) -> ActionBudgetSnapshot:
    return ActionBudgetSnapshot(
        mode=mode,
        address_actions_remaining=9000,
        cancel_headroom=9000,
        ip_weight_remaining=1000,
        action_shadow_cost_usdc=Usd("0.0005"),
        observed_at=NOW,
        healthy=True,
    )


def test_fair_value_shift_is_bounded_by_tick_cap() -> None:
    features = _features(microprice=Price("110"), normalized_ofi=Decimal("1"))
    fair = FairValueModel().calculate(features, _config(max_fair_shift_ticks=Decimal("2")))
    assert fair == Price("100.2")


def test_long_inventory_moves_reservation_down_and_blocks_bid_at_soft_limit() -> None:
    features = _features()
    config = _config()
    fair = FairValueModel().calculate(features, config)
    result = InventoryController().calculate(fair, _inventory("1.1"), features, config)
    assert result.reservation_price < fair
    assert result.allow_bid is False
    assert result.allow_ask is True


def test_policy_emits_two_post_only_candidates_when_healthy() -> None:
    result = MarketMakerPolicy().quote(
        strategy_id=StrategyId("mm_btc"),
        session_id="session-1",
        revision=1,
        current_slot_revision=0,
        features=_features(),
        inventory=_inventory(),
        budget=_budget(),
        config=_config(),
    )
    assert result.bid.decision == QuoteDecision.QUOTE
    assert result.ask.decision == QuoteDecision.QUOTE
    assert result.bid.price is not None and result.bid.price < Price("100.1")
    assert result.ask.price is not None and result.ask.price > Price("99.9")


def test_critical_budget_only_quotes_inventory_reducing_side() -> None:
    result = MarketMakerPolicy().quote(
        strategy_id=StrategyId("mm_btc"),
        session_id="session-1",
        revision=1,
        current_slot_revision=0,
        features=_features(),
        inventory=_inventory("0.5"),
        budget=_budget(ActionBudgetMode.CRITICAL),
        config=_config(),
    )
    assert result.bid.decision == QuoteDecision.NO_QUOTE
    assert result.ask.decision == QuoteDecision.QUOTE


def test_cancel_only_budget_produces_no_quotes() -> None:
    result = MarketMakerPolicy().quote(
        strategy_id=StrategyId("mm_btc"),
        session_id="session-1",
        revision=1,
        current_slot_revision=0,
        features=_features(),
        inventory=_inventory(),
        budget=_budget(ActionBudgetMode.CANCEL_ONLY),
        config=_config(),
    )
    assert result.bid.decision == QuoteDecision.NO_QUOTE
    assert result.ask.decision == QuoteDecision.NO_QUOTE


def test_edge_threshold_can_select_no_quote() -> None:
    result = MarketMakerPolicy().quote(
        strategy_id=StrategyId("mm_btc"),
        session_id="session-1",
        revision=1,
        current_slot_revision=0,
        features=_features(),
        inventory=_inventory(),
        budget=_budget(),
        config=_config(min_expected_pnl_usdc=Usd("100")),
    )
    assert result.bid.reason == "expected_edge_below_threshold"
    assert result.ask.reason == "expected_edge_below_threshold"
