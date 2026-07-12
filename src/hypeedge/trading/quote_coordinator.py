"""Pure desired-vs-authoritative quote reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from hypeedge.core.enums import QuoteAction, QuoteDecision, Side
from hypeedge.core.types import Size, Usd
from hypeedge.trading.quotes import (
    DesiredQuote,
    DesiredQuoteSet,
    QuoteDiff,
    QuotePlan,
    QuoteRiskOwner,
    QuoteSlotView,
)


@dataclass(frozen=True, slots=True)
class QuoteCoordinatorConfig:
    min_quote_lifetime: timedelta = timedelta(milliseconds=500)
    refresh_cooldown: timedelta = timedelta(milliseconds=100)
    max_quote_age: timedelta = timedelta(seconds=15)
    price_hysteresis_ticks: int = 1
    size_hysteresis: Size = Size("0")
    replace_hysteresis_usdc: Usd = Usd("0")
    action_shadow_cost_usdc: Usd = Usd("0")
    failure_tail_cost_per_action_usdc: Usd = Usd("0")
    modify_enabled: bool = False

    def __post_init__(self) -> None:
        if min(self.min_quote_lifetime, self.refresh_cooldown, self.max_quote_age) < timedelta(0):
            raise ValueError("quote timing controls cannot be negative")
        if self.price_hysteresis_ticks < 0 or self.size_hysteresis < 0:
            raise ValueError("quote hysteresis cannot be negative")
        if self.replace_hysteresis_usdc < 0:
            raise ValueError("replace hysteresis cannot be negative")
        if self.action_shadow_cost_usdc < 0 or self.failure_tail_cost_per_action_usdc < 0:
            raise ValueError("transition costs cannot be negative")
        if self.modify_enabled:
            raise ValueError("MODIFY is disabled until exchange recovery semantics are authoritative")


class QuoteCoordinator:
    """Compute a minimum-action plan without mutating slot state."""

    def __init__(self, config: QuoteCoordinatorConfig) -> None:
        self._config = config

    def coordinate(
        self,
        desired: DesiredQuoteSet,
        bid_view: QuoteSlotView,
        ask_view: QuoteSlotView,
        *,
        tick_size: Decimal,
        now: datetime,
    ) -> QuotePlan:
        self._validate_views(desired, bid_view, ask_view)
        fence_reason = self._fence_reason(desired, bid_view, ask_view, now)
        if fence_reason is not None:
            return QuotePlan(
                strategy_id=desired.strategy_id,
                symbol=desired.symbol,
                session_id=desired.session_id,
                config_version=desired.config_version,
                revision=desired.revision,
                market_version=desired.market_version,
                connection_generation=desired.connection_generation,
                valid_until=desired.valid_until,
                diffs=(),
                fair_price=desired.fair_price,
                reservation_price=desired.reservation_price,
                inventory_notional=desired.inventory_notional,
                budget_mode=desired.budget_mode,
                fenced=True,
                fence_reason=fence_reason,
            )
        if tick_size <= 0:
            raise ValueError("tick size must be positive")
        diffs = (
            self._diff_slot(desired.bid, bid_view, tick_size=tick_size, now=now),
            self._diff_slot(desired.ask, ask_view, tick_size=tick_size, now=now),
        )
        return QuotePlan(
            strategy_id=desired.strategy_id,
            symbol=desired.symbol,
            session_id=desired.session_id,
            config_version=desired.config_version,
            revision=desired.revision,
            market_version=desired.market_version,
            connection_generation=desired.connection_generation,
            valid_until=desired.valid_until,
            diffs=diffs,
            fair_price=desired.fair_price,
            reservation_price=desired.reservation_price,
            inventory_notional=desired.inventory_notional,
            budget_mode=desired.budget_mode,
        )

    @staticmethod
    def _validate_views(desired: DesiredQuoteSet, bid_view: QuoteSlotView, ask_view: QuoteSlotView) -> None:
        expected = {Side.BUY: bid_view, Side.SELL: ask_view}
        for side, view in expected.items():
            if (
                view.key.strategy_id != desired.strategy_id
                or view.key.symbol != desired.symbol
                or view.key.side != side
            ):
                raise ValueError("authoritative slot view does not belong to desired quote set")
            _ = view.current_owner  # Validate the at-most-one-current-owner invariant.

    @staticmethod
    def _fence_reason(
        desired: DesiredQuoteSet, bid_view: QuoteSlotView, ask_view: QuoteSlotView, now: datetime
    ) -> str | None:
        if now >= desired.valid_until:
            return "candidate_expired"
        if desired.revision <= max(bid_view.plan_revision, ask_view.plan_revision):
            return "stale_plan_revision"
        if desired.current_slot_revision != max(bid_view.revision, ask_view.revision):
            return "slot_revision_mismatch"
        return None

    def _diff_slot(self, desired: DesiredQuote, view: QuoteSlotView, *, tick_size: Decimal, now: datetime) -> QuoteDiff:
        owner = view.current_owner
        if view.has_unknown or view.has_orphaned_owner:
            reason = (
                "unknown_risk_owner_requires_reconciliation"
                if view.has_unknown
                else "orphaned_live_owner_requires_recovery"
            )
            return self._make_diff(desired, owner, QuoteAction.BLOCKED_UNKNOWN, (), reason)
        if view.has_inflight:
            return self._make_diff(desired, owner, QuoteAction.KEEP, (), "child_action_inflight")
        if desired.decision == QuoteDecision.KEEP:
            action = QuoteAction.KEEP if owner is not None else QuoteAction.NO_ACTION
            return self._make_diff(desired, owner, action, (), "policy_keep")
        if desired.decision == QuoteDecision.NO_QUOTE:
            if owner is None:
                return self._make_diff(desired, None, QuoteAction.NO_ACTION, (), desired.reason)
            return self._make_diff(desired, owner, QuoteAction.CANCEL, ("cancel",), desired.reason)
        if owner is None:
            return self._make_diff(desired, None, QuoteAction.PLACE, ("place",), desired.reason)
        if desired.price is None or desired.size is None:  # defensive narrowing
            raise ValueError("QUOTE requires price and size")
        age = now - owner.live_since
        price_ticks = abs(Decimal(desired.price) - Decimal(owner.price)) / tick_size
        size_delta = abs(Decimal(desired.size) - Decimal(owner.remaining_size))
        within_hysteresis = price_ticks <= self._config.price_hysteresis_ticks and size_delta <= Decimal(
            self._config.size_hysteresis
        )
        if within_hysteresis:
            return self._make_diff(desired, owner, QuoteAction.KEEP, (), "within_price_size_hysteresis")
        if age < self._config.min_quote_lifetime:
            return self._make_diff(desired, owner, QuoteAction.KEEP, (), "minimum_quote_lifetime")
        if view.last_transition_at is not None and now - view.last_transition_at < self._config.refresh_cooldown:
            return self._make_diff(desired, owner, QuoteAction.KEEP, (), "refresh_cooldown")
        forced_by_age = age >= self._config.max_quote_age
        replace_cost = self._transition_cost(2)
        net = Usd(Decimal(desired.gross_edge_usdc) - Decimal(replace_cost))
        if not forced_by_age and net <= self._config.replace_hysteresis_usdc:
            return self._make_diff(desired, owner, QuoteAction.KEEP, (), "replace_not_incrementally_better")
        return self._make_diff(
            desired,
            owner,
            QuoteAction.CANCEL_THEN_PLACE,
            ("cancel", "place"),
            "maximum_quote_age" if forced_by_age else "replace_hysteresis_passed",
        )

    def _transition_cost(self, child_count: int) -> Usd:
        per_child = Decimal(self._config.action_shadow_cost_usdc) + Decimal(
            self._config.failure_tail_cost_per_action_usdc
        )
        return Usd(per_child * child_count)

    def _make_diff(
        self,
        desired: DesiredQuote,
        source: QuoteRiskOwner | None,
        action: QuoteAction,
        child_actions: tuple[str, ...],
        reason: str,
    ) -> QuoteDiff:
        cost = self._transition_cost(len(child_actions))
        net = Usd(Decimal(desired.gross_edge_usdc) - Decimal(cost))
        return QuoteDiff(
            slot=desired.slot,
            action=action,
            source=source,
            desired=desired,
            child_actions=child_actions,
            reason=reason,
            gross_edge_usdc=desired.gross_edge_usdc,
            transition_cost_usdc=cost,
            net_incremental_utility_usdc=net,
        )
