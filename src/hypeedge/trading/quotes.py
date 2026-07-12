"""Immutable desired-quote models shared by policy and coordinator."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from hypeedge.core.enums import ActionBudgetMode, OrderStatus, QuoteAction, QuoteDecision, Side
from hypeedge.core.types import Cloid, OrderId, Price, Size, StrategyId, Symbol, Usd


@dataclass(frozen=True, slots=True)
class QuoteSlotKey:
    strategy_id: StrategyId
    symbol: Symbol
    side: Side
    level: int = 0

    def __post_init__(self) -> None:
        if self.level < 0:
            raise ValueError("quote level cannot be negative")


@dataclass(frozen=True, slots=True)
class DesiredQuote:
    slot: QuoteSlotKey
    decision: QuoteDecision
    price: Price | None
    size: Size | None
    gross_edge_usdc: Usd
    reason: str

    def __post_init__(self) -> None:
        has_quote = self.decision == QuoteDecision.QUOTE
        if has_quote != (self.price is not None and self.size is not None):
            raise ValueError("QUOTE requires price and size; non-QUOTE decisions must omit them")
        if self.size is not None and self.size <= 0:
            raise ValueError("quote size must be positive")
        if self.price is not None and self.price <= 0:
            raise ValueError("quote price must be positive")


@dataclass(frozen=True, slots=True)
class DesiredQuoteSet:
    strategy_id: StrategyId
    symbol: Symbol
    session_id: str
    config_version: int
    model_version: str
    market_version: int
    connection_generation: int
    current_slot_revision: int
    revision: int
    fair_price: Price
    reservation_price: Price
    inventory_notional: Usd
    expected_utility_usdc: Usd
    budget_mode: ActionBudgetMode
    bid: DesiredQuote
    ask: DesiredQuote
    created_at: datetime
    valid_until: datetime
    feature_values: tuple[tuple[str, Decimal], ...] = ()

    def __post_init__(self) -> None:
        if self.revision < 0 or self.current_slot_revision < 0 or self.market_version < 0:
            raise ValueError("quote and market revisions cannot be negative")
        if self.config_version <= 0:
            raise ValueError("config version must be positive")
        if self.valid_until <= self.created_at:
            raise ValueError("quote set validity deadline must be after creation")
        for quote, side in ((self.bid, Side.BUY), (self.ask, Side.SELL)):
            if quote.slot.strategy_id != self.strategy_id or quote.slot.symbol != self.symbol:
                raise ValueError("quote slot does not belong to quote set")
            if quote.slot.side != side:
                raise ValueError("quote slot side does not match quote-set side")


@dataclass(frozen=True, slots=True)
class QuoteRiskOwner:
    """An order which can still fill and therefore remains a risk owner.

    ``remaining_size`` is authoritative remaining quantity, not original order
    quantity.  This prevents partial fills from being mechanically replenished.
    """

    order_id: OrderId | None
    cloid: Cloid
    price: Price
    remaining_size: Size
    status: OrderStatus
    plan_revision: int
    live_since: datetime
    exchange_order_id_known: bool = True

    def __post_init__(self) -> None:
        if self.price <= 0 or self.remaining_size <= 0:
            raise ValueError("risk-owner price and remaining size must be positive")
        if self.plan_revision < 0:
            raise ValueError("risk-owner plan revision cannot be negative")

    @property
    def is_unknown(self) -> bool:
        return self.status in {OrderStatus.SUBMIT_UNKNOWN, OrderStatus.CANCEL_UNKNOWN}

    @property
    def is_inflight(self) -> bool:
        return self.status in {OrderStatus.PENDING, OrderStatus.SUBMITTED}


@dataclass(frozen=True, slots=True)
class QuoteSlotView:
    """Authoritative slot projection including every possible live owner."""

    key: QuoteSlotKey
    revision: int
    plan_revision: int
    owners: tuple[QuoteRiskOwner, ...]
    last_transition_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.revision < 0 or self.plan_revision < 0:
            raise ValueError("slot revisions cannot be negative")
        if len({owner.cloid for owner in self.owners}) != len(self.owners):
            raise ValueError("a risk owner may appear only once in a slot")

    @property
    def has_unknown(self) -> bool:
        return any(owner.is_unknown for owner in self.owners)

    @property
    def has_inflight(self) -> bool:
        return any(owner.is_inflight for owner in self.owners)

    @property
    def has_orphaned_owner(self) -> bool:
        return any(owner.plan_revision != self.plan_revision for owner in self.owners)

    @property
    def current_owner(self) -> QuoteRiskOwner | None:
        matching = [owner for owner in self.owners if owner.plan_revision == self.plan_revision]
        if len(matching) > 1:
            raise ValueError("slot has more than one current desired owner")
        return matching[0] if matching else None


@dataclass(frozen=True, slots=True)
class QuoteDiff:
    """One minimal slot transition and its real exchange-child cost."""

    slot: QuoteSlotKey
    action: QuoteAction
    source: QuoteRiskOwner | None
    desired: DesiredQuote
    child_actions: tuple[str, ...]
    reason: str
    gross_edge_usdc: Usd
    transition_cost_usdc: Usd
    net_incremental_utility_usdc: Usd

    @property
    def estimated_incremental_actions(self) -> int:
        return len(self.child_actions)


@dataclass(frozen=True, slots=True)
class QuotePlan:
    strategy_id: StrategyId
    symbol: Symbol
    session_id: str
    config_version: int
    revision: int
    market_version: int
    connection_generation: int
    valid_until: datetime
    diffs: tuple[QuoteDiff, ...]
    fair_price: Price | None = None
    reservation_price: Price | None = None
    inventory_notional: Usd = Usd("0")
    budget_mode: ActionBudgetMode = ActionBudgetMode.CANCEL_ONLY
    fenced: bool = False
    fence_reason: str | None = None

    @property
    def estimated_incremental_actions(self) -> int:
        return sum(diff.estimated_incremental_actions for diff in self.diffs)
