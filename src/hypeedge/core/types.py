"""Shared semantic types used across HypeEdge modules."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, NewType, Self

# Semantic types for clarity and type-safety
Symbol = NewType("Symbol", str)  # e.g. "BTC", "ETH"
Timestamp = NewType("Timestamp", int)  # Unix millis
OrderId = NewType("OrderId", str)  # Exchange order ID
Cloid = NewType("Cloid", str)  # Client order ID
StrategyId = NewType("StrategyId", str)  # Strategy identifier
SubAccount = NewType("SubAccount", str)  # Sub-account name


class DecimalValue(Decimal):
    """Exact decimal domain value with safe construction from floats.

    ``Decimal(float)`` preserves the float's binary approximation. Trading
    values instead pass through ``str`` so API/SDK float compatibility does
    not silently inject binary rounding noise into the domain model.
    """

    def __new__(cls, value: Decimal | int | float | str = "0") -> Self:
        if isinstance(value, bool):
            raise TypeError("boolean is not a valid decimal domain value")
        decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
        if not decimal_value.is_finite():
            raise ValueError("decimal domain value must be finite")
        return super().__new__(cls, str(decimal_value))

    @staticmethod
    def _coerce_operand(value: Any) -> Any:
        if isinstance(value, float):
            return Decimal(str(value))
        return value

    def __add__(self, other: Any) -> DecimalValue:
        return DecimalValue(super().__add__(self._coerce_operand(other)))

    def __radd__(self, other: Any) -> DecimalValue:
        return DecimalValue(super().__radd__(self._coerce_operand(other)))

    def __sub__(self, other: Any) -> DecimalValue:
        return DecimalValue(super().__sub__(self._coerce_operand(other)))

    def __rsub__(self, other: Any) -> DecimalValue:
        return DecimalValue(super().__rsub__(self._coerce_operand(other)))

    def __mul__(self, other: Any) -> DecimalValue:
        return DecimalValue(super().__mul__(self._coerce_operand(other)))

    def __rmul__(self, other: Any) -> DecimalValue:
        return DecimalValue(super().__rmul__(self._coerce_operand(other)))

    def __truediv__(self, other: Any) -> DecimalValue:
        return DecimalValue(super().__truediv__(self._coerce_operand(other)))

    def __rtruediv__(self, other: Any) -> DecimalValue:
        return DecimalValue(super().__rtruediv__(self._coerce_operand(other)))

    def __pow__(self, other: Any, modulo: Any = None) -> DecimalValue:
        coerced = self._coerce_operand(other)
        if modulo is not None:
            return DecimalValue(super().__pow__(coerced, modulo))
        return DecimalValue(super().__pow__(coerced))


class Price(DecimalValue):
    """Exact instrument price."""


class Size(DecimalValue):
    """Exact contract quantity."""


class Usd(DecimalValue):
    """Exact USDC-denominated monetary amount."""


class Pct(DecimalValue):
    """Exact percentage represented as a fraction in the range chosen by the caller."""


Leverage = NewType("Leverage", int)  # e.g. 1, 2, 5
