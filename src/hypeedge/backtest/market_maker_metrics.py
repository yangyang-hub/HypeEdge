"""Exact market-making accounting and non-accounting execution diagnostics."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from hypeedge.core.enums import Side
from hypeedge.core.types import Price, Size, Usd

ZERO = Decimal("0")


@dataclass(frozen=True, slots=True)
class AccountingFill:
    """An immutable ledger input. Fees are signed: rebates are positive."""

    side: Side
    price: Price
    size: Size
    net_fee_rebate: Usd = Usd(ZERO)


@dataclass(frozen=True, slots=True)
class AccountingPnL:
    """Accounting identity derived only from ledger inputs, never markouts."""

    realized_trading: Usd
    unrealized_inventory_change: Usd
    net_fee_rebate: Usd
    funding: Usd
    paid_action: Usd
    ending_inventory: Size
    ending_inventory_cost: Price | None

    @property
    def net(self) -> Usd:
        return Usd(
            self.realized_trading
            + self.unrealized_inventory_change
            + self.net_fee_rebate
            + self.funding
            - self.paid_action
        )

    def assert_ledger_identity(self, ledger_net: Usd) -> None:
        if Decimal(self.net) != Decimal(ledger_net):
            raise ValueError(f"accounting PnL does not equal ledger: calculated={self.net}, ledger={ledger_net}")


@dataclass(frozen=True, slots=True)
class FillMarkout:
    fill_id: str
    horizon_ms: int
    value: Usd


@dataclass(frozen=True, slots=True)
class ExecutionQuality:
    """Research diagnostics. These values must never enter AccountingPnL."""

    quoted_spread_bps: tuple[Decimal, ...] = ()
    realized_spread: Usd = Usd(ZERO)
    markouts: tuple[FillMarkout, ...] = ()
    queue_ahead_consumed: Size = Size(ZERO)
    fills: int = 0
    partial_fills: int = 0


@dataclass(slots=True)
class AccountingLedger:
    """Average-cost Decimal ledger supporting partial fills and open inventory."""

    _quantity: Decimal = ZERO
    _average_cost: Decimal | None = None
    _realized: Decimal = ZERO
    _fees: Decimal = ZERO
    _funding: Decimal = ZERO
    _paid_action: Decimal = ZERO
    _fills: list[AccountingFill] = field(default_factory=list)

    def record_fill(self, fill: AccountingFill) -> None:
        quantity = Decimal(fill.size) if fill.side == Side.BUY else -Decimal(fill.size)
        if quantity == ZERO:
            raise ValueError("fill size must be positive")
        price = Decimal(fill.price)
        old_quantity = self._quantity
        same_direction = old_quantity == ZERO or (old_quantity > ZERO) == (quantity > ZERO)
        if same_direction:
            total = abs(old_quantity) + abs(quantity)
            old_cost = self._average_cost or ZERO
            self._average_cost = ((abs(old_quantity) * old_cost) + (abs(quantity) * price)) / total
            self._quantity += quantity
        else:
            closing = min(abs(old_quantity), abs(quantity))
            average = self._average_cost
            if average is None:
                raise AssertionError("non-flat inventory requires an average cost")
            direction = Decimal("1") if old_quantity > ZERO else Decimal("-1")
            self._realized += closing * (price - average) * direction
            self._quantity += quantity
            if self._quantity == ZERO:
                self._average_cost = None
            elif (self._quantity > ZERO) != (old_quantity > ZERO):
                self._average_cost = price
        self._fees += Decimal(fill.net_fee_rebate)
        self._fills.append(fill)

    def record_funding(self, amount: Usd) -> None:
        """Record signed funding income (payment is negative)."""
        self._funding += Decimal(amount)

    def record_paid_action(self, amount: Usd) -> None:
        if amount < ZERO:
            raise ValueError("paid action cost cannot be negative")
        self._paid_action += Decimal(amount)

    def close(self, mark_price: Price) -> AccountingPnL:
        unrealized = ZERO
        if self._quantity != ZERO:
            if self._average_cost is None:
                raise AssertionError("non-flat inventory requires an average cost")
            unrealized = self._quantity * (Decimal(mark_price) - self._average_cost)
        return AccountingPnL(
            realized_trading=Usd(self._realized),
            unrealized_inventory_change=Usd(unrealized),
            net_fee_rebate=Usd(self._fees),
            funding=Usd(self._funding),
            paid_action=Usd(self._paid_action),
            ending_inventory=Size(self._quantity),
            ending_inventory_cost=Price(self._average_cost) if self._average_cost is not None else None,
        )
