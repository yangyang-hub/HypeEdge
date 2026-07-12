"""Tests for exact decimal domain values."""

from __future__ import annotations

from decimal import Decimal

import pytest

from hypeedge.core.types import Price, Size, Usd


def test_decimal_domain_values_construct_floats_via_string() -> None:
    assert Price(0.1) == Decimal("0.1")
    assert Size("0.00001") == Decimal("0.00001")
    assert Usd(Decimal("12.34")) == Decimal("12.34")


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_decimal_domain_values_reject_non_finite_values(value: str) -> None:
    with pytest.raises(ValueError, match="finite"):
        Price(value)


def test_decimal_domain_values_reject_boolean() -> None:
    with pytest.raises(TypeError, match="boolean"):
        Size(True)  # type: ignore[arg-type]
