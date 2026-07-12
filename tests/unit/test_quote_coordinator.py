"""QuoteCoordinator invariants and transition-cost tests."""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import pytest

from hypeedge.core.enums import ActionBudgetMode, OrderStatus, QuoteAction, QuoteDecision, Side
from hypeedge.core.types import Cloid, OrderId, Price, Size, StrategyId, Symbol, Usd
from hypeedge.trading.quote_coordinator import QuoteCoordinator, QuoteCoordinatorConfig
from hypeedge.trading.quotes import DesiredQuote, DesiredQuoteSet, QuoteRiskOwner, QuoteSlotKey, QuoteSlotView

NOW = datetime(2026, 1, 1, tzinfo=UTC)
STRATEGY = StrategyId("mm-btc")
SYMBOL = Symbol("BTC")
BID = QuoteSlotKey(STRATEGY, SYMBOL, Side.BUY)
ASK = QuoteSlotKey(STRATEGY, SYMBOL, Side.SELL)


def desired_quote(slot: QuoteSlotKey, *, price: str = "100", size: str = "1", edge: str = "1") -> DesiredQuote:
    return DesiredQuote(slot, QuoteDecision.QUOTE, Price(price), Size(size), Usd(edge), "profitable")


def desired_set(
    *,
    bid: DesiredQuote | None = None,
    ask: DesiredQuote | None = None,
    revision: int = 2,
    slot_revision: int = 3,
) -> DesiredQuoteSet:
    return DesiredQuoteSet(
        strategy_id=STRATEGY,
        symbol=SYMBOL,
        session_id="session-1",
        config_version=1,
        model_version="m1",
        market_version=9,
        connection_generation=4,
        current_slot_revision=slot_revision,
        revision=revision,
        fair_price=Price("100.5"),
        reservation_price=Price("100.5"),
        inventory_notional=Usd("0"),
        expected_utility_usdc=Usd("2"),
        budget_mode=ActionBudgetMode.NORMAL,
        bid=bid or desired_quote(BID, price="100"),
        ask=ask or desired_quote(ASK, price="101"),
        created_at=NOW,
        valid_until=NOW + timedelta(seconds=1),
    )


def owner(
    slot: QuoteSlotKey,
    *,
    price: str,
    size: str = "1",
    status: OrderStatus = OrderStatus.ACKNOWLEDGED,
    plan_revision: int = 1,
    suffix: str = "1",
    age: timedelta = timedelta(seconds=1),
) -> QuoteRiskOwner:
    return QuoteRiskOwner(
        order_id=OrderId(f"oid-{suffix}"),
        cloid=Cloid(f"cloid-{slot.side}-{suffix}"),
        price=Price(price),
        remaining_size=Size(size),
        status=status,
        plan_revision=plan_revision,
        live_since=NOW - age,
    )


def view(slot: QuoteSlotKey, *owners: QuoteRiskOwner, revision: int = 3, plan_revision: int = 1) -> QuoteSlotView:
    return QuoteSlotView(slot, revision, plan_revision, tuple(owners), NOW - timedelta(seconds=1))


def coordinator(**overrides: object) -> QuoteCoordinator:
    values = {
        "min_quote_lifetime": timedelta(milliseconds=500),
        "refresh_cooldown": timedelta(milliseconds=100),
        "max_quote_age": timedelta(seconds=10),
        "price_hysteresis_ticks": 1,
        "size_hysteresis": Size("0.1"),
        "replace_hysteresis_usdc": Usd("0.2"),
        "action_shadow_cost_usdc": Usd("0.1"),
        "failure_tail_cost_per_action_usdc": Usd("0.05"),
    }
    values.update(overrides)
    return QuoteCoordinator(QuoteCoordinatorConfig(**values))  # type: ignore[arg-type]


def test_empty_slots_place_two_children_and_count_real_child_actions() -> None:
    plan = coordinator().coordinate(desired_set(), view(BID), view(ASK), tick_size=Price("0.5"), now=NOW)
    assert [diff.action for diff in plan.diffs] == [QuoteAction.PLACE, QuoteAction.PLACE]
    assert plan.estimated_incremental_actions == 2
    assert all(diff.transition_cost_usdc == Usd("0.15") for diff in plan.diffs)


def test_unchanged_and_small_size_difference_keep_without_action() -> None:
    bid_owner = owner(BID, price="99.5", size="0.95")
    ask_owner = owner(ASK, price="101.5", size="1.05")
    plan = coordinator().coordinate(
        desired_set(), view(BID, bid_owner), view(ASK, ask_owner), tick_size=Price("0.5"), now=NOW
    )
    assert {diff.action for diff in plan.diffs} == {QuoteAction.KEEP}
    assert plan.estimated_incremental_actions == 0


def test_replace_is_cancel_then_place_and_costs_two_exchange_children() -> None:
    bid_owner = owner(BID, price="98")
    plan = coordinator().coordinate(desired_set(), view(BID, bid_owner), view(ASK), tick_size=Price("0.5"), now=NOW)
    bid_diff = plan.diffs[0]
    assert bid_diff.action == QuoteAction.CANCEL_THEN_PLACE
    assert bid_diff.child_actions == ("cancel", "place")
    assert bid_diff.estimated_incremental_actions == 2
    assert bid_diff.transition_cost_usdc == Usd("0.30")


def test_replace_suppressed_by_min_lifetime_cooldown_and_net_hysteresis() -> None:
    young = owner(BID, price="98", age=timedelta(milliseconds=100))
    plan = coordinator().coordinate(desired_set(), view(BID, young), view(ASK), tick_size=Price("0.5"), now=NOW)
    assert plan.diffs[0].reason == "minimum_quote_lifetime"

    old = owner(BID, price="98")
    cooling = QuoteSlotView(BID, 3, 1, (old,), NOW - timedelta(milliseconds=10))
    plan = coordinator().coordinate(desired_set(), cooling, view(ASK), tick_size=Price("0.5"), now=NOW)
    assert plan.diffs[0].reason == "refresh_cooldown"

    low_edge = desired_quote(BID, price="100", edge="0.4")
    plan = coordinator().coordinate(
        desired_set(bid=low_edge), view(BID, old), view(ASK), tick_size=Price("0.5"), now=NOW
    )
    assert plan.diffs[0].reason == "replace_not_incrementally_better"


def test_max_age_forces_refresh_but_unknown_always_blocks() -> None:
    ancient = owner(BID, price="99", age=timedelta(seconds=20))
    plan = coordinator().coordinate(
        desired_set(bid=desired_quote(BID, edge="0")),
        view(BID, ancient),
        view(ASK),
        tick_size=Price("0.5"),
        now=NOW,
    )
    assert plan.diffs[0].action == QuoteAction.CANCEL_THEN_PLACE
    assert plan.diffs[0].reason == "maximum_quote_age"

    unknown = owner(BID, price="99", status=OrderStatus.CANCEL_UNKNOWN)
    plan = coordinator().coordinate(desired_set(), view(BID, unknown), view(ASK), tick_size=Price("0.5"), now=NOW)
    assert plan.diffs[0].action == QuoteAction.BLOCKED_UNKNOWN


def test_no_quote_is_protective_cancel_and_bypasses_refresh_suppression() -> None:
    no_bid = DesiredQuote(BID, QuoteDecision.NO_QUOTE, None, None, Usd("0"), "risk_off")
    young = owner(BID, price="100", age=timedelta(milliseconds=1))
    plan = coordinator().coordinate(
        desired_set(bid=no_bid), view(BID, young), view(ASK), tick_size=Price("0.5"), now=NOW
    )
    assert plan.diffs[0].action == QuoteAction.CANCEL
    assert plan.diffs[0].estimated_incremental_actions == 1


def test_policy_keep_and_empty_no_quote_do_not_create_actions() -> None:
    keep_bid = DesiredQuote(BID, QuoteDecision.KEEP, None, None, Usd("0"), "stable")
    no_ask = DesiredQuote(ASK, QuoteDecision.NO_QUOTE, None, None, Usd("0"), "no_edge")
    bid_owner = owner(BID, price="100")
    plan = coordinator().coordinate(
        desired_set(bid=keep_bid, ask=no_ask),
        view(BID, bid_owner),
        view(ASK),
        tick_size=Price("0.5"),
        now=NOW,
    )
    assert [diff.action for diff in plan.diffs] == [QuoteAction.KEEP, QuoteAction.NO_ACTION]

    empty_plan = coordinator().coordinate(
        desired_set(bid=keep_bid, ask=no_ask), view(BID), view(ASK), tick_size=Price("0.5"), now=NOW
    )
    assert empty_plan.diffs[0].action == QuoteAction.NO_ACTION


def test_orphaned_live_owner_blocks_replacement_even_when_not_unknown() -> None:
    orphan = owner(BID, price="99", plan_revision=0)
    plan = coordinator().coordinate(desired_set(), view(BID, orphan), view(ASK), tick_size=Price("0.5"), now=NOW)
    assert plan.diffs[0].action == QuoteAction.BLOCKED_UNKNOWN
    assert plan.diffs[0].reason == "orphaned_live_owner_requires_recovery"


def test_invalid_tick_and_mismatched_view_fail_closed() -> None:
    with pytest.raises(ValueError, match="tick size"):
        coordinator().coordinate(desired_set(), view(BID), view(ASK), tick_size=Price("0"), now=NOW)
    wrong = QuoteSlotKey(StrategyId("other"), SYMBOL, Side.BUY)
    with pytest.raises(ValueError, match="does not belong"):
        coordinator().coordinate(desired_set(), view(wrong), view(ASK), tick_size=Price("0.5"), now=NOW)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"min_quote_lifetime": timedelta(seconds=-1)},
        {"price_hysteresis_ticks": -1},
        {"size_hysteresis": Size("-1")},
        {"replace_hysteresis_usdc": Usd("-1")},
        {"action_shadow_cost_usdc": Usd("-1")},
        {"failure_tail_cost_per_action_usdc": Usd("-1")},
        {"modify_enabled": True},
    ],
)
def test_invalid_coordinator_configuration_is_rejected(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        QuoteCoordinatorConfig(**kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("revision", "slot_revision", "now", "reason"),
    [
        (1, 3, NOW, "stale_plan_revision"),
        (2, 2, NOW, "slot_revision_mismatch"),
        (2, 3, NOW + timedelta(seconds=1), "candidate_expired"),
    ],
)
def test_revision_and_deadline_fencing(revision: int, slot_revision: int, now: datetime, reason: str) -> None:
    plan = coordinator().coordinate(
        desired_set(revision=revision, slot_revision=slot_revision),
        view(BID),
        view(ASK),
        tick_size=Price("0.5"),
        now=now,
    )
    assert plan.fenced is True
    assert plan.fence_reason == reason
    assert plan.diffs == ()


def test_random_slot_combinations_preserve_owner_and_action_invariants() -> None:
    randomizer = random.Random(20260711)
    statuses = [
        OrderStatus.ACKNOWLEDGED,
        OrderStatus.PARTIAL_FILL,
        OrderStatus.SUBMITTED,
        OrderStatus.SUBMIT_UNKNOWN,
        OrderStatus.CANCEL_UNKNOWN,
    ]
    for iteration in range(1_000):
        status = randomizer.choice(statuses)
        risk_owner = owner(
            BID,
            price=str(randomizer.randint(95, 105)),
            size=str(randomizer.choice(["0.25", "0.5", "1"])),
            status=status,
            suffix=str(iteration),
        )
        slot_view = view(BID, risk_owner)
        plan = coordinator().coordinate(desired_set(), slot_view, view(ASK), tick_size=Price("0.5"), now=NOW)
        diff = plan.diffs[0]
        assert diff.source == risk_owner
        assert diff.estimated_incremental_actions == len(diff.child_actions)
        if status in {OrderStatus.SUBMIT_UNKNOWN, OrderStatus.CANCEL_UNKNOWN}:
            assert diff.action == QuoteAction.BLOCKED_UNKNOWN
        if status == OrderStatus.SUBMITTED:
            assert diff.action == QuoteAction.KEEP


def test_multiple_current_desired_owners_fail_closed() -> None:
    first = owner(BID, price="100", suffix="a")
    second = owner(BID, price="99", suffix="b")
    with pytest.raises(ValueError, match="more than one current desired owner"):
        coordinator().coordinate(desired_set(), view(BID, first, second), view(ASK), tick_size=Price("0.5"), now=NOW)
