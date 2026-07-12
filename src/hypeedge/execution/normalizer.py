"""Exact instrument-aware order normalization for every trading entry point."""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import ROUND_DOWN, Decimal
from typing import NoReturn, Protocol

from hypeedge.core.enums import Side, TimeInForce
from hypeedge.core.exceptions import OrderNormalizationError
from hypeedge.core.models import OrderIntent
from hypeedge.core.types import Price, Size, Symbol


@dataclass(frozen=True, slots=True)
class InstrumentSpec:
    """Exchange rules required to construct an exact order."""

    symbol: Symbol
    tick_size: Decimal
    lot_size: Decimal
    min_size: Decimal
    min_notional: Decimal | None = None

    def __post_init__(self) -> None:
        for name in ("tick_size", "lot_size", "min_size"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.min_notional is not None and self.min_notional <= 0:
            raise ValueError("min_notional must be positive when configured")


class InstrumentSpecLike(Protocol):
    @property
    def symbol(self) -> Symbol: ...

    @property
    def tick_size(self) -> Decimal: ...

    @property
    def lot_size(self) -> Decimal: ...

    @property
    def min_size(self) -> Decimal: ...

    @property
    def min_notional(self) -> Decimal | None: ...


class InstrumentSpecProvider(Protocol):
    """Synchronous instrument-rule lookup used on the trading hot path."""

    def get(self, symbol: Symbol) -> InstrumentSpecLike | None: ...


class OrderNormalizer:
    """Quantize and validate order intents before risk admission and signing."""

    def __init__(self, instruments: InstrumentSpecProvider) -> None:
        self._instruments = instruments

    def normalize(
        self,
        intent: OrderIntent,
        *,
        best_bid: Price | None = None,
        best_ask: Price | None = None,
    ) -> OrderIntent:
        spec = self._instruments.get(intent.symbol)
        if spec is None:
            self._reject(intent.symbol, "instrument_meta_unavailable", "Instrument metadata is unavailable")

        size = self._floor_to_step(Decimal(intent.size), spec.lot_size)
        if size < spec.min_size:
            self._reject(intent.symbol, "size_below_minimum", f"Normalized size {size} is below {spec.min_size}")

        price: Decimal | None = None
        if intent.price is not None:
            price = self._floor_to_step(Decimal(intent.price), spec.tick_size)
            if price <= 0:
                self._reject(intent.symbol, "price_not_positive", "Normalized price must be positive")

        if spec.min_notional is not None:
            if price is None:
                self._reject(
                    intent.symbol,
                    "reference_price_required",
                    "A reference price is required to validate minimum notional",
                )
            if size * price < spec.min_notional:
                self._reject(
                    intent.symbol,
                    "notional_below_minimum",
                    f"Normalized notional {size * price} is below {spec.min_notional}",
                )

        if intent.time_in_force in {TimeInForce.ALO, TimeInForce.GTX} and price is not None:
            if intent.side == Side.BUY and best_ask is not None and price >= best_ask:
                self._reject(intent.symbol, "post_only_would_cross", "Post-only buy would cross the best ask")
            if intent.side == Side.SELL and best_bid is not None and price <= best_bid:
                self._reject(intent.symbol, "post_only_would_cross", "Post-only sell would cross the best bid")

        return replace(intent, size=Size(size), price=Price(price) if price is not None else None)

    @staticmethod
    def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
        units = (value / step).to_integral_value(rounding=ROUND_DOWN)
        return units * step

    @staticmethod
    def _reject(symbol: Symbol, reason: str, message: str) -> NoReturn:
        raise OrderNormalizationError(message, symbol=str(symbol), reason=reason)
