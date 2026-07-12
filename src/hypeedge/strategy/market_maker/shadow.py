"""Research-only virtual quote lifecycle for mainnet shadow evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from hypeedge.core.enums import OrderStatus, QuoteAction, Side
from hypeedge.core.types import Cloid, Price, Size, StrategyId, Symbol
from hypeedge.trading.quotes import QuotePlan, QuoteRiskOwner, QuoteSlotKey, QuoteSlotView


@dataclass(frozen=True, slots=True)
class ShadowActionEstimate:
    optimistic: int
    neutral: int
    pessimistic: int


class ShadowOrderState:
    """Virtual resting orders; never writes authoritative trading facts."""

    def __init__(self) -> None:
        self._views: dict[QuoteSlotKey, QuoteSlotView] = {}

    def views(self, strategy_id: StrategyId, symbol: Symbol) -> tuple[QuoteSlotView, QuoteSlotView]:
        return (
            self._view(QuoteSlotKey(strategy_id, symbol, Side.BUY)),
            self._view(QuoteSlotKey(strategy_id, symbol, Side.SELL)),
        )

    def apply(self, plan: QuotePlan, *, now: datetime) -> ShadowActionEstimate:
        if plan.fenced:
            return ShadowActionEstimate(0, 0, 0)
        child_actions = 0
        for diff in plan.diffs:
            view = self._view(diff.slot)
            owners = list(view.owners)
            if diff.action == QuoteAction.CANCEL:
                owners = []
                child_actions += 1
            elif diff.action == QuoteAction.PLACE:
                owners = [self._owner(plan, diff.slot, diff.desired.price, diff.desired.size, now)]
                child_actions += 1
            elif diff.action == QuoteAction.CANCEL_THEN_PLACE:
                owners = [self._owner(plan, diff.slot, diff.desired.price, diff.desired.size, now)]
                child_actions += 2
            elif diff.action in {QuoteAction.KEEP, QuoteAction.NO_ACTION, QuoteAction.BLOCKED_UNKNOWN}:
                continue
            else:
                raise ValueError(f"unsupported shadow quote action: {diff.action}")
            self._views[diff.slot] = QuoteSlotView(
                key=diff.slot,
                revision=view.revision + 1,
                plan_revision=plan.revision,
                owners=tuple(owners),
                last_transition_at=now,
            )
        return ShadowActionEstimate(
            optimistic=child_actions,
            neutral=child_actions + (1 if child_actions else 0),
            pessimistic=child_actions * 2,
        )

    def simulate_fill(self, slot: QuoteSlotKey, *, size: Size) -> None:
        view = self._view(slot)
        owner = view.current_owner
        if owner is None:
            raise KeyError(slot)
        remaining = Decimal(owner.remaining_size) - Decimal(size)
        owners: tuple[QuoteRiskOwner, ...]
        if remaining <= 0:
            owners = ()
        else:
            owners = (
                QuoteRiskOwner(
                    order_id=owner.order_id,
                    cloid=owner.cloid,
                    price=owner.price,
                    remaining_size=Size(remaining),
                    status=OrderStatus.PARTIAL_FILL,
                    plan_revision=owner.plan_revision,
                    live_since=owner.live_since,
                    exchange_order_id_known=True,
                ),
            )
        self._views[slot] = QuoteSlotView(
            key=slot,
            revision=view.revision + 1,
            plan_revision=view.plan_revision,
            owners=owners,
            last_transition_at=view.last_transition_at,
        )

    def simulate_fill_by_cloid(self, cloid: Cloid, *, size: Size) -> bool:
        """Apply a fill to its virtual owner without restoring original size."""
        for slot, view in tuple(self._views.items()):
            if any(owner.cloid == cloid for owner in view.owners):
                self.simulate_fill(slot, size=size)
                return True
        return False

    def _view(self, key: QuoteSlotKey) -> QuoteSlotView:
        return self._views.get(key, QuoteSlotView(key=key, revision=0, plan_revision=0, owners=()))

    @staticmethod
    def _owner(
        plan: QuotePlan,
        slot: QuoteSlotKey,
        price: Price | None,
        size: Size | None,
        now: datetime,
    ) -> QuoteRiskOwner:
        if price is None or size is None:
            raise ValueError("shadow placement requires desired price and size")
        cloid = Cloid(f"shadow:{plan.session_id}:{plan.revision}:{slot.side.value}:{slot.level}")
        return QuoteRiskOwner(
            order_id=None,
            cloid=cloid,
            price=price,
            remaining_size=size,
            status=OrderStatus.ACKNOWLEDGED,
            plan_revision=plan.revision,
            live_since=now,
            exchange_order_id_known=False,
        )
