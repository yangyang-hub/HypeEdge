"""External-reference, latency and mature-markout model tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from hypeedge.core.enums import Side
from hypeedge.core.models import L2BookSnapshot, L2Level
from hypeedge.core.types import Cloid, OrderId, Price, Size, StrategyId, Symbol, Timestamp, Usd
from hypeedge.market_data.external_reference import ExternalReferenceSnapshot
from hypeedge.market_data.features import MarketFeatureEngine
from hypeedge.storage.mm_analytics import MarketMakerFillMarkout
from hypeedge.strategy.market_maker import AdverseMarkoutEstimator, DecisionLatencyEstimator, FairValueModel
from hypeedge.strategy.market_maker.models import MarketMakerConfig

NOW = datetime(2026, 7, 12, tzinfo=UTC)
SYMBOL = Symbol("BTC")
STRATEGY = StrategyId("mm-btc")


def _book(*, bid: str = "99.9", ask: str = "100.1", version: int = 1) -> L2BookSnapshot:
    return L2BookSnapshot(
        symbol=SYMBOL,
        bids=(L2Level(Price(bid), Size("5")),),
        asks=(L2Level(Price(ask), Size("5")),),
        timestamp=Timestamp(version),
        local_ts=NOW,
        version=version,
        connection_generation=1,
    )


def _config(**overrides: object) -> MarketMakerConfig:
    values: dict[str, object] = {
        "version": 1,
        "model_version": "external-v1",
        "tick_size": Decimal("0.1"),
        "lot_size": Decimal("0.001"),
        "min_size": Decimal("0.001"),
        "soft_inventory_notional": Usd("100"),
        "hard_inventory_notional": Usd("150"),
        "emergency_inventory_notional": Usd("200"),
        "quote_size": Size("0.1"),
        "max_depth_participation": Decimal("0.1"),
        "beta_microprice": Decimal(0),
        "beta_ofi_ticks": Decimal(0),
        "beta_trade_flow_ticks": Decimal(0),
        "beta_short_return_ticks": Decimal(0),
    }
    values.update(overrides)
    return MarketMakerConfig(**values)  # type: ignore[arg-type]


def _reference(
    *,
    adjusted: str = "100.2",
    observed_at: datetime = NOW,
    quality: str = "healthy",
    confidence: str = "1",
    weight: str = "1",
    basis_bps: str = "10",
) -> ExternalReferenceSnapshot:
    return ExternalReferenceSnapshot(
        source="binance_spot_perpetual",
        symbol=SYMBOL,
        raw_price=Price("100.1"),
        adjusted_price=Price(adjusted),
        basis_bps=Decimal(basis_bps),
        effective_weight=Decimal(weight),
        confidence=Decimal(confidence),
        age_ms=0,
        quality=quality,  # type: ignore[arg-type]
        observed_at=observed_at,
    )


def test_basis_adjusted_external_price_moves_fair_value_in_both_directions() -> None:
    engine = MarketFeatureEngine()
    config = _config(external_reference_weight=Decimal("0.5"))
    upward = engine.build(_book(), healthy=True, external_reference=_reference(), config=config, decision_at=NOW)
    downward = engine.build(
        _book(version=2),
        healthy=True,
        external_reference=_reference(adjusted="99.8", basis_bps="-10"),
        config=config,
        decision_at=NOW,
    )

    assert upward.external_basis_bps == Decimal("10")
    assert upward.external_effective_weight == Decimal("0.5")
    assert FairValueModel().calculate(upward, config) > upward.mid_price
    assert FairValueModel().calculate(downward, config) < downward.mid_price


def test_stale_and_outlier_external_references_have_zero_weight() -> None:
    config = _config(external_max_age_seconds=Decimal("0.2"), external_outlier_bps=Decimal("20"))
    engine = MarketFeatureEngine()
    stale = engine.build(
        _book(),
        healthy=True,
        external_reference=_reference(observed_at=NOW - timedelta(seconds=1)),
        config=config,
        decision_at=NOW,
    )
    outlier = engine.build(
        _book(version=2),
        healthy=True,
        external_reference=_reference(adjusted="101"),
        config=config,
        decision_at=NOW,
    )

    assert (stale.external_quality, stale.external_effective_weight) == ("stale", Decimal(0))
    assert (outlier.external_quality, outlier.external_effective_weight) == ("outlier", Decimal(0))
    assert FairValueModel().calculate(outlier, config) == outlier.mid_price


def test_external_and_total_prediction_caps_are_independent() -> None:
    config = _config(
        external_reference_weight=Decimal(1),
        external_outlier_bps=Decimal("10000"),
        max_external_shift_ticks=Decimal(2),
        max_total_fair_shift_ticks=Decimal("1.5"),
    )
    features = MarketFeatureEngine().build(
        _book(),
        healthy=True,
        external_reference=_reference(adjusted="110"),
        config=config,
        decision_at=NOW,
    )
    assert FairValueModel().calculate(features, config) == Price("100.15")


def test_latency_buffer_uses_observed_ewma_and_realized_variance() -> None:
    estimator = DecisionLatencyEstimator(
        alpha=Decimal("0.5"),
        conservative_default_seconds=Decimal("0.2"),
        min_samples=2,
    )
    assert estimator.quality == "conservative_default"
    estimator.observe(Decimal("0.04"))
    estimator.observe(Decimal("0.08"))
    assert estimator.seconds == Decimal("0.060")
    assert estimator.quality == "observed"

    engine = MarketFeatureEngine()
    engine.observe_book(_book(bid="99.9", ask="100.1"))
    engine.observe_book(_book(bid="100.0", ask="100.2", version=2))
    moved = _book(bid="100.9", ask="101.1", version=3)
    features = engine.build(
        moved,
        healthy=True,
        latency_seconds=estimator.seconds,
        latency_quality=estimator.quality,
        config=_config(),
    )
    assert features.latency_buffer_bps > 0
    assert features.latency_quality == "observed"


def _markout(*, signed_bps: str, horizon_ts: datetime, ts: datetime, maker: bool = True) -> MarketMakerFillMarkout:
    return MarketMakerFillMarkout(
        ts=ts,
        strategy_id=STRATEGY,
        symbol=SYMBOL,
        session_id="session-1",
        fill_id=f"fill-{signed_bps}",
        order_id=OrderId("1"),
        cloid=Cloid("0x" + "1" * 32),
        fill_ts=NOW - timedelta(seconds=2),
        side=Side.BUY,
        fill_px=Price("100"),
        fill_size=Size("0.1"),
        reference="mid",
        reference_px=Price("100"),
        horizon_ms=1000,
        horizon_ts=horizon_ts,
        mark_px=Price("99.99"),
        signed_markout_bps=Decimal(signed_bps),
        signed_markout_usdc=Usd("-0.001"),
        spread_capture_usdc=Usd("0.001"),
        maker=maker,
        queue_ahead_size=None,
        fill_probability=None,
        calculation_version="v1",
    )


def test_markout_estimator_ignores_immature_samples_and_uses_safe_default() -> None:
    estimator = AdverseMarkoutEstimator(min_samples=2, conservative_default_bps=Decimal("1.5"))
    immature = _markout(signed_bps="-9", horizon_ts=NOW + timedelta(seconds=1), ts=NOW)
    assert estimator.observe(immature, now=NOW) is False
    assert estimator.estimate(STRATEGY, SYMBOL).adverse_bps == Decimal("1.5")

    assert estimator.observe(_markout(signed_bps="-2", horizon_ts=NOW, ts=NOW), now=NOW)
    assert estimator.observe(_markout(signed_bps="1", horizon_ts=NOW, ts=NOW), now=NOW)
    estimate = estimator.estimate(STRATEGY, SYMBOL)
    assert estimate.quality == "mature"
    assert estimate.adverse_bps == Decimal("2")
