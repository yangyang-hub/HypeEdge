"""Deterministic replay and exact accounting invariants."""

from __future__ import annotations

from decimal import Decimal

import pytest

from hypeedge.backtest.market_maker_metrics import AccountingFill, AccountingLedger, FillMarkout
from hypeedge.backtest.market_maker_replay import (
    FundingEvent,
    MarketMakerReplay,
    PaidActionEvent,
    QuoteEvent,
    ReplayScenario,
    TradeEvent,
)
from hypeedge.core.enums import Side
from hypeedge.core.types import Price, Size, Usd


def test_decimal_accounting_handles_partial_close_funding_action_and_open_inventory() -> None:
    ledger = AccountingLedger()
    ledger.record_fill(AccountingFill(Side.BUY, Price("100"), Size("2"), Usd("0.02")))
    ledger.record_fill(AccountingFill(Side.SELL, Price("110"), Size("0.5"), Usd("0.01")))
    ledger.record_funding(Usd("-1.25"))
    ledger.record_paid_action(Usd("0.75"))

    pnl = ledger.close(Price("105"))

    assert pnl.realized_trading == Decimal("5.0")
    assert pnl.unrealized_inventory_change == Decimal("7.5")
    assert pnl.ending_inventory == Decimal("1.5")
    assert pnl.ending_inventory_cost == Decimal("100")
    assert pnl.net == Decimal("10.53")
    pnl.assert_ledger_identity(Usd("10.53"))
    with pytest.raises(ValueError, match="does not equal ledger"):
        pnl.assert_ledger_identity(Usd("10.54"))


def test_average_cost_ledger_supports_position_flip() -> None:
    ledger = AccountingLedger()
    ledger.record_fill(AccountingFill(Side.SELL, Price("100"), Size("1")))
    ledger.record_fill(AccountingFill(Side.BUY, Price("90"), Size("2")))

    pnl = ledger.close(Price("95"))

    assert pnl.realized_trading == Decimal("10")
    assert pnl.unrealized_inventory_change == Decimal("5")
    assert pnl.ending_inventory == Decimal("1")
    assert pnl.ending_inventory_cost == Decimal("90")


def test_three_scenarios_apply_queue_and_latency_conservatively() -> None:
    events = [
        QuoteEvent(0, "bid-1", Side.BUY, Price("100"), Size("2"), Size("1")),
        TradeEvent(10, Side.SELL, Price("100"), Size("1")),
        TradeEvent(50, Side.SELL, Price("100"), Size("1")),
        TradeEvent(150, Side.SELL, Price("100"), Size("1")),
    ]
    replay = MarketMakerReplay()

    optimistic = replay.run(events, scenario=ReplayScenario.OPTIMISTIC, ending_mark_price=Price("101"))
    neutral = replay.run(events, scenario=ReplayScenario.NEUTRAL, ending_mark_price=Price("101"))
    pessimistic = replay.run(events, scenario=ReplayScenario.PESSIMISTIC, ending_mark_price=Price("101"))

    assert sum(Decimal(fill.size) for fill in optimistic.fills) == Decimal("2")
    assert sum(Decimal(fill.size) for fill in neutral.fills) == Decimal("1")
    assert pessimistic.fills == ()
    assert "does not prove" in optimistic.research_disclaimer


def test_event_time_sort_is_deterministic_and_partial_fill_is_retained() -> None:
    events = [
        TradeEvent(20, Side.BUY, Price("101"), Size("0.4")),
        QuoteEvent(0, "ask", Side.SELL, Price("101"), Size("1")),
        TradeEvent(10, Side.BUY, Price("101"), Size("0.3")),
    ]
    replay = MarketMakerReplay(maker_rebate_rate=Decimal("0.0001"))

    first = replay.run(events, scenario=ReplayScenario.OPTIMISTIC, ending_mark_price=Price("100"))
    second = replay.run(events, scenario=ReplayScenario.OPTIMISTIC, ending_mark_price=Price("100"))

    assert first == second
    assert [fill.event_time_ms for fill in first.fills] == [10, 20]
    assert first.shadow_orders[0].remaining == Decimal("0.3")
    assert first.execution_quality.partial_fills == 2
    assert first.accounting_pnl.net_fee_rebate == Decimal("0.00707")


def test_funding_and_paid_actions_are_accounting_but_markout_is_not() -> None:
    events = [FundingEvent(1, Usd("2.5")), PaidActionEvent(2, Usd("0.5"))]
    result = MarketMakerReplay().run(
        events,
        scenario=ReplayScenario.NEUTRAL,
        ending_mark_price=Price("100"),
    )
    original_net = result.accounting_pnl.net

    changed_quality = result.execution_quality.__class__(
        markouts=(FillMarkout("fill-1", 1_000, Usd("999999")),),
    )

    assert original_net == Decimal("2.0")
    assert changed_quality.markouts[0].value == Decimal("999999")
    assert result.accounting_pnl.net == original_net


def test_invalid_ledger_inputs_fail_closed() -> None:
    ledger = AccountingLedger()
    with pytest.raises(ValueError, match="fill size"):
        ledger.record_fill(AccountingFill(Side.BUY, Price("1"), Size("0")))
    with pytest.raises(ValueError, match="cannot be negative"):
        ledger.record_paid_action(Usd("-1"))
