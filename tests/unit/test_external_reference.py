"""Tests for external reference normalization and safety degradation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from hypeedge.config.settings import ExternalReferenceSettings
from hypeedge.core.types import Price, Symbol, Timestamp
from hypeedge.market_data.external_reference import ExternalVenueQuote, LatestExternalReferenceProvider


def _quote(
    market: str,
    bid: str,
    ask: str,
    *,
    received_at: datetime | None = None,
    sequence: int = 1,
    generation: int = 1,
) -> ExternalVenueQuote:
    return ExternalVenueQuote(
        symbol=Symbol("BTC"),
        venue_symbol="BTCUSDT",
        market=market,  # type: ignore[arg-type]
        bid=Price(bid),
        ask=Price(ask),
        exchange_ts=Timestamp(1_700_000_000_000),
        received_at=received_at or datetime.now(UTC),
        sequence=sequence,
        connection_generation=generation,
    )


def _provider(**overrides: object) -> LatestExternalReferenceProvider:
    return LatestExternalReferenceProvider(ExternalReferenceSettings(external_reference_enabled=True, **overrides))


def test_disabled_provider_is_zero_weight() -> None:
    provider = LatestExternalReferenceProvider(ExternalReferenceSettings())

    snapshot = provider.get_external_reference(Symbol("BTC"))

    assert snapshot.quality == "disabled"
    assert snapshot.raw_price is None
    assert snapshot.effective_weight == 0


def test_weighted_spot_perpetual_composite_uses_decimal() -> None:
    provider = _provider()
    provider.update_quote(_quote("spot", "100", "102"))
    snapshot = provider.update_quote(_quote("perpetual", "100.1", "102.1"))

    assert snapshot.quality == "healthy"
    assert snapshot.spot_mid == Price("101")
    assert snapshot.perpetual_mid == Price("101.1")
    assert snapshot.raw_price == Price("101.06")
    assert snapshot.adjusted_price == Price("101.06")
    assert Decimal("0") < snapshot.effective_weight <= Decimal("0.35")


def test_outlier_and_crossed_quotes_have_zero_weight() -> None:
    provider = _provider(max_perp_spot_divergence_bps=Decimal("10"))
    provider.update_quote(_quote("spot", "100", "101"))
    outlier = provider.update_quote(_quote("perpetual", "110", "111"))
    assert outlier.quality == "degraded"
    assert outlier.effective_weight == 0
    assert "perpetual_spot_outlier" in outlier.quality_reasons

    crossed = provider.update_quote(_quote("perpetual", "112", "111", sequence=2))
    assert crossed.effective_weight == 0
    assert "perpetual_crossed" in crossed.quality_reasons


def test_mark_book_outlier_has_zero_weight() -> None:
    provider = _provider(max_mark_book_divergence_bps=Decimal("10"))
    provider.update_quote(_quote("spot", "100", "102"))
    provider.update_quote(_quote("perpetual", "100", "102"))
    mark = ExternalVenueQuote(
        symbol=Symbol("BTC"),
        venue_symbol="BTCUSDT",
        market="perpetual_mark",
        mark_price=Price("110"),
        exchange_ts=Timestamp(1_700_000_000_000),
        received_at=datetime.now(UTC),
        sequence=2,
        connection_generation=1,
    )

    snapshot = provider.update_quote(mark)

    assert snapshot.effective_weight == 0
    assert "perpetual_mark_outlier" in snapshot.quality_reasons


def test_stale_data_loses_all_weight() -> None:
    provider = _provider(stale_after_ms=100)
    old = datetime.now(UTC) - timedelta(seconds=1)
    provider.update_quote(_quote("spot", "100", "101", received_at=old))
    snapshot = provider.update_quote(_quote("perpetual", "100", "101", received_at=old))

    assert snapshot.quality == "stale"
    assert snapshot.effective_weight == 0
    assert snapshot.raw_price is None


def test_freshness_weight_decays_monotonically() -> None:
    recent = _provider(stale_after_ms=2000)
    older = _provider(stale_after_ms=2000)
    now = datetime.now(UTC)
    for provider, age_ms in ((recent, 100), (older, 1000)):
        observed = now - timedelta(milliseconds=age_ms)
        provider.update_quote(_quote("spot", "100", "102", received_at=observed))
        provider.update_quote(_quote("perpetual", "100", "102", received_at=observed))

    assert (
        recent.get_external_reference(Symbol("BTC")).effective_weight
        > older.get_external_reference(Symbol("BTC")).effective_weight
    )


def test_ewma_log_basis_maps_external_price_into_hl_domain() -> None:
    provider = _provider()
    provider.update_quote(_quote("spot", "99", "101"))
    provider.update_quote(_quote("perpetual", "99", "101"))

    snapshot = provider.update_hyperliquid_mid(Symbol("BTC"), Price("101"))

    assert snapshot.adjusted_price == Price("101.0000000000000000000000000")
    assert snapshot.basis_bps > 0


def test_sequence_and_generation_fencing() -> None:
    provider = _provider()
    provider.update_quote(_quote("spot", "100", "102", sequence=5, generation=2))
    provider.update_quote(_quote("spot", "90", "92", sequence=4, generation=2))
    provider.update_quote(_quote("spot", "80", "82", sequence=99, generation=1))

    assert provider.get_external_reference(Symbol("BTC")).spot_mid == Price("101")


def test_external_reference_settings_enforce_global_caps() -> None:
    with pytest.raises(ValueError, match="weights must sum to 1"):
        ExternalReferenceSettings(spot_weight=Decimal("0.5"), perpetual_weight=Decimal("0.6"))
    with pytest.raises(ValueError, match="exceeds max_symbols"):
        ExternalReferenceSettings(symbol_map={"BTC": "BTCUSDT", "ETH": "ETHUSDT"}, max_symbols=1)
