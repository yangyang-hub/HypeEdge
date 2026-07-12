"""Tests for exact order normalization."""

from __future__ import annotations

from decimal import Decimal

import pytest

from hypeedge.core.enums import Side, TimeInForce
from hypeedge.core.exceptions import OrderNormalizationError
from hypeedge.core.models import OrderIntent
from hypeedge.core.types import Price, Size, Symbol
from hypeedge.execution.normalizer import InstrumentSpec, OrderNormalizer


class _Specs:
    def __init__(self, spec: InstrumentSpec | None) -> None:
        self._spec = spec

    def get(self, symbol: Symbol) -> InstrumentSpec | None:
        if self._spec is None or self._spec.symbol != symbol:
            return None
        return self._spec


def _normalizer(*, min_notional: str | None = None) -> OrderNormalizer:
    return OrderNormalizer(
        _Specs(
            InstrumentSpec(
                symbol=Symbol("BTC"),
                tick_size=Decimal("0.1"),
                lot_size=Decimal("0.001"),
                min_size=Decimal("0.002"),
                min_notional=Decimal(min_notional) if min_notional else None,
            )
        )
    )


def test_normalizer_floors_price_and_size_exactly() -> None:
    normalized = _normalizer().normalize(
        OrderIntent(symbol=Symbol("BTC"), side=Side.BUY, size=Size("0.0029"), price=Price("100.19"))
    )

    assert normalized.size == Decimal("0.002")
    assert normalized.price == Decimal("100.1")


def test_normalizer_rejects_size_below_minimum_after_flooring() -> None:
    with pytest.raises(OrderNormalizationError, match="below") as exc_info:
        _normalizer().normalize(OrderIntent(symbol=Symbol("BTC"), side=Side.BUY, size=Size("0.0019")))

    assert exc_info.value.reason == "size_below_minimum"


def test_normalizer_rejects_post_only_cross() -> None:
    with pytest.raises(OrderNormalizationError) as exc_info:
        _normalizer().normalize(
            OrderIntent(
                symbol=Symbol("BTC"),
                side=Side.BUY,
                size=Size("0.002"),
                price=Price("100.1"),
                time_in_force=TimeInForce.ALO,
            ),
            best_ask=Price("100.0"),
        )

    assert exc_info.value.reason == "post_only_would_cross"


def test_normalizer_enforces_minimum_notional() -> None:
    with pytest.raises(OrderNormalizationError) as exc_info:
        _normalizer(min_notional="10").normalize(
            OrderIntent(symbol=Symbol("BTC"), side=Side.BUY, size=Size("0.002"), price=Price("100"))
        )

    assert exc_info.value.reason == "notional_below_minimum"


def test_normalizer_fails_closed_without_metadata() -> None:
    with pytest.raises(OrderNormalizationError) as exc_info:
        OrderNormalizer(_Specs(None)).normalize(OrderIntent(symbol=Symbol("BTC"), side=Side.BUY, size=Size("0.002")))

    assert exc_info.value.reason == "instrument_meta_unavailable"
