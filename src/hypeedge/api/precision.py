"""Instrument-aware decimal validation at the HTTP trading boundary."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from typing import Any

from hypeedge.api.errors import ApiProblem
from hypeedge.api.schemas import decimal_string
from hypeedge.core.types import Symbol


@dataclass(frozen=True, slots=True)
class InstrumentRules:
    symbol: Symbol
    tick_size: Decimal
    lot_size: Decimal
    min_size: Decimal


def require_instrument_rules(app: Any, raw_symbol: str) -> InstrumentRules:  # noqa: ANN401
    symbol = Symbol(raw_symbol.strip().upper())
    cache = getattr(app, "_instrument_cache", None)
    info = cache.get(symbol) if cache is not None else None
    if info is None:
        raise ApiProblem(
            503,
            "INSTRUMENT_META_UNAVAILABLE",
            "Instrument metadata is required before accepting a trading command",
            retryable=True,
            context={"symbol": str(symbol)},
        )
    return InstrumentRules(
        symbol=symbol,
        tick_size=Decimal(str(info.tick_size)),
        lot_size=Decimal(str(info.lot_size)),
        min_size=Decimal(str(info.min_size)),
    )


def validate_size(size: Decimal, rules: InstrumentRules) -> None:
    if size < rules.min_size:
        raise ApiProblem(
            422,
            "ORDER_SIZE_BELOW_MINIMUM",
            "Order size is below the instrument minimum",
            context={
                "symbol": str(rules.symbol),
                "size": decimal_string(size),
                "min_order_size": decimal_string(rules.min_size),
            },
        )
    if size % rules.lot_size != 0:
        raise ApiProblem(
            422,
            "ORDER_SIZE_NOT_ON_LOT",
            "Order size must be an exact multiple of the instrument lot size",
            context={
                "symbol": str(rules.symbol),
                "size": decimal_string(size),
                "lot_size": decimal_string(rules.lot_size),
            },
        )


def validate_price(price: Decimal | None, rules: InstrumentRules) -> None:
    if price is not None and price % rules.tick_size != 0:
        raise ApiProblem(
            422,
            "ORDER_PRICE_NOT_ON_TICK",
            "Limit price must be an exact multiple of the instrument tick size",
            context={
                "symbol": str(rules.symbol),
                "price": decimal_string(price),
                "tick_size": decimal_string(rules.tick_size),
            },
        )


def floor_to_lot(size: Decimal, rules: InstrumentRules) -> Decimal:
    """Round a server-derived partial close down so it can never over-close."""
    lots = (size / rules.lot_size).to_integral_value(rounding=ROUND_DOWN)
    return lots * rules.lot_size
