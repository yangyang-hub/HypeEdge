"""Operational metric, alert, and release-gate contracts for market making."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import yaml
from prometheus_client import CollectorRegistry, generate_latest

from hypeedge.core.types import Price, Size, StrategyId, Symbol, Usd
from hypeedge.monitor.alerts import AlertPayload, AlertSeverity
from hypeedge.monitor.market_making import (
    ExecutionOutcome,
    ExternalReferenceQuality,
    FreshnessSource,
    InventoryBand,
    LatencyStage,
    MarketMakingMetrics,
)
from hypeedge.monitor.release_gates import (
    CanaryLaunchArtifacts,
    ExpansionChange,
    ExpansionDimension,
    OperationalGateChecker,
)
from hypeedge.risk.canary import (
    CanaryDirective,
    CanaryRiskEnvelope,
    ExpansionEvidence,
    ReleaseEvidence,
)

ROOT = Path(__file__).resolve().parents[2]
STRATEGY = StrategyId("mm-btc")
SYMBOL = Symbol("BTC")


def _envelope() -> CanaryRiskEnvelope:
    return CanaryRiskEnvelope(
        version=3,
        max_deployed_equity=Usd("1000"),
        max_quote_notional=Usd("100"),
        max_daily_loss=Usd("20"),
        max_cumulative_loss=Usd("50"),
        max_daily_actions=100,
        max_total_actions=1000,
        min_action_credits=2000,
        min_cancel_headroom=100,
        max_forced_flatten_count=0,
        max_forced_flatten_cost=Usd("0"),
        unknown_sla=timedelta(seconds=15),
        max_duration=timedelta(days=7),
        max_filled_volume=Usd("5000"),
    )


def _release_evidence(*, shadow_days: int = 14) -> ReleaseEvidence:
    return ReleaseEvidence(
        shadow_complete_utc_days=shadow_days,
        testnet_clean_utc_days=14,
        reconciliation_diff_count=0,
        duplicate_order_count=0,
        risk_bypass_count=0,
        hard_inventory_breach_count=0,
        unresolved_unknown_count=0,
        pessimistic_net_edge_usdc=Usd("0"),
        projected_runway_hours=Decimal("336"),
        required_runway_hours=Decimal("336"),
    )


def _artifacts() -> CanaryLaunchArtifacts:
    return CanaryLaunchArtifacts(
        envelope=_envelope(),
        shadow_report_id="shadow-2026-07-v1",
        testnet_report_id="testnet-2026-08-v1",
        failure_injection_report_id="fi-2026-08-v2",
        statistical_plan_version="sap-v1",
        approved_by="operator@example.invalid",
    )


def _expansion_evidence() -> ExpansionEvidence:
    return ExpansionEvidence(
        complete_utc_days=30,
        independent_inventory_episodes=30,
        regime_coverage_complete=True,
        accounting_edge_ci95_lower=Usd("0.01"),
        marginal_usdc_per_action=Usd("1.25"),
        critical_reconciliation_diff_count=0,
        duplicate_order_count=0,
        hard_inventory_breach_count=0,
        unknown_with_terminal_fact_count=4,
        unknown_total_count=4,
        directional_concentration=Decimal("0.40"),
    )


def test_market_making_metrics_expose_bounded_operational_projection() -> None:
    registry = CollectorRegistry()
    metrics = MarketMakingMetrics(registry)
    metrics.observe_freshness(
        STRATEGY,
        SYMBOL,
        FreshnessSource.USER_STREAM,
        age=timedelta(seconds=2),
        max_age=timedelta(seconds=5),
        healthy=True,
    )
    metrics.set_reference_prices(STRATEGY, SYMBOL, fair=Price("60000.1"), reservation=Price("59999.9"))
    metrics.set_external_reference(
        STRATEGY,
        SYMBOL,
        source="binance_perp",
        raw_price=Price("60010"),
        adjusted_price=Price("60001"),
        basis_bps=Decimal("-1.5"),
        basis_limit_bps=Decimal("10"),
        divergence_bps=Decimal("0.15"),
        divergence_limit_bps=Decimal("5"),
        configured_weight=Decimal("0.25"),
        effective_weight=Decimal("0.20"),
        confidence=Decimal("0.8"),
        age=timedelta(milliseconds=25),
        max_age=timedelta(milliseconds=500),
        quality=ExternalReferenceQuality.HEALTHY,
    )
    metrics.set_quote(
        STRATEGY,
        SYMBOL,
        view="desired",
        side="buy",
        price=Price("59999"),
        size=Size("0.001"),
        age=timedelta(milliseconds=50),
    )
    metrics.set_quote_uptime(STRATEGY, SYMBOL, window="5m", ratio=Decimal("0.98"))
    metrics.set_inventory(
        STRATEGY,
        SYMBOL,
        notional=Usd("-75"),
        hard_limit=Usd("100"),
        band=InventoryBand.SOFT,
        margin_utilization=Decimal("0.1"),
        liquidation_distance=Decimal("0.5"),
        funding_carry=Usd("-0.02"),
    )
    metrics.set_action_budget(
        STRATEGY,
        SYMBOL,
        address_remaining=9000,
        cancel_headroom=1000,
        ip_weight_remaining=1100,
        burn_per_hour=Decimal("20"),
        earn_per_hour=Decimal("2"),
        marginal_usdc_per_action=Decimal("1.30"),
        runway_hours=Decimal("500"),
        emergency_reserve=200,
    )
    metrics.record_execution_outcome(STRATEGY, SYMBOL, ExecutionOutcome.UNKNOWN)
    metrics.set_unknown_orders(
        STRATEGY,
        SYMBOL,
        count=1,
        oldest_age=timedelta(seconds=10),
        sla=timedelta(seconds=15),
    )
    metrics.observe_latency(STRATEGY, SYMBOL, LatencyStage.RECEIPT_TO_DECISION, Decimal("0.002"))
    metrics.set_reconciliation_diff(STRATEGY, SYMBOL, severity="critical", count=0)
    metrics.set_runtime(STRATEGY, SYMBOL, state="running", config_version=7)
    metrics.set_canary_directive(STRATEGY, envelope_version=3, directive=CanaryDirective.CANCEL_ONLY)
    metrics.set_postgres_available(True)
    metrics.record_emergency_cancel_failure("mm-isolated")

    payload = generate_latest(registry).decode()
    assert 'hype_mm_freshness_healthy{source="user_stream",strategy_id="mm-btc",symbol="BTC"} 1.0' in payload
    assert 'hype_mm_reference_price{kind="fair",strategy_id="mm-btc",symbol="BTC"} 60000.1' in payload
    assert (
        'hype_mm_external_reference_price{kind="adjusted",source="binance_perp",strategy_id="mm-btc",symbol="BTC"} '
        "60001.0"
    ) in payload
    assert (
        'hype_mm_external_effective_weight{kind="effective",source="binance_perp",strategy_id="mm-btc",symbol="BTC"} '
        "0.2"
    ) in payload
    assert (
        'hype_mm_external_quality{quality="healthy",source="binance_perp",strategy_id="mm-btc",symbol="BTC"} 1.0'
    ) in payload
    assert 'hype_mm_inventory_band{band="soft",strategy_id="mm-btc",symbol="BTC"} 1.0' in payload
    assert 'hype_mm_action_budget{kind="runway_hours",strategy_id="mm-btc",symbol="BTC"} 500.0' in payload
    assert 'hype_mm_execution_outcomes_total{outcome="unknown",strategy_id="mm-btc",symbol="BTC"} 1.0' in payload
    assert 'hype_mm_canary_directive{directive="cancel_only",strategy_id="mm-btc"} 1.0' in payload
    assert "hype_mm_postgres_available 1.0" in payload


def test_metrics_fail_closed_for_unknown_freshness_and_clear_one_hot_state() -> None:
    registry = CollectorRegistry()
    metrics = MarketMakingMetrics(registry)
    metrics.observe_freshness(
        STRATEGY,
        SYMBOL,
        FreshnessSource.CREDIT,
        age=None,
        max_age=timedelta(seconds=10),
        healthy=True,
    )
    metrics.set_inventory(
        STRATEGY,
        SYMBOL,
        notional=Usd("10"),
        hard_limit=Usd("100"),
        band=InventoryBand.NORMAL,
        margin_utilization=Decimal("0"),
        liquidation_distance=Decimal("1"),
        funding_carry=Usd("0"),
    )
    metrics.set_inventory(
        STRATEGY,
        SYMBOL,
        notional=Usd("110"),
        hard_limit=Usd("100"),
        band=InventoryBand.HARD,
        margin_utilization=Decimal("0.5"),
        liquidation_distance=Decimal("0.1"),
        funding_carry=Usd("0"),
    )

    payload = generate_latest(registry).decode()
    assert 'hype_mm_freshness_healthy{source="credit",strategy_id="mm-btc",symbol="BTC"} 0.0' in payload
    assert 'hype_mm_inventory_band{band="normal",strategy_id="mm-btc",symbol="BTC"} 0.0' in payload
    assert 'hype_mm_inventory_band{band="hard",strategy_id="mm-btc",symbol="BTC"} 1.0' in payload


def test_external_reference_metrics_validate_weight_and_clear_quality() -> None:
    registry = CollectorRegistry()
    metrics = MarketMakingMetrics(registry)
    common = {
        "source": "binance_spot",
        "raw_price": Price("60000"),
        "adjusted_price": Price("59999"),
        "basis_bps": Decimal("-0.2"),
        "basis_limit_bps": Decimal("10"),
        "divergence_bps": Decimal("0.1"),
        "divergence_limit_bps": Decimal("5"),
        "configured_weight": Decimal("0.2"),
        "confidence": Decimal("0.9"),
        "max_age": timedelta(milliseconds=500),
    }
    metrics.set_external_reference(
        STRATEGY,
        SYMBOL,
        **common,
        effective_weight=Decimal("0.18"),
        age=timedelta(milliseconds=20),
        quality=ExternalReferenceQuality.HEALTHY,
    )
    metrics.set_external_reference(
        STRATEGY,
        SYMBOL,
        **common,
        effective_weight=Decimal("0"),
        age=None,
        quality=ExternalReferenceQuality.STALE,
    )

    payload = generate_latest(registry).decode()
    assert (
        'hype_mm_external_quality{quality="healthy",source="binance_spot",strategy_id="mm-btc",symbol="BTC"} 0.0'
    ) in payload
    assert (
        'hype_mm_external_quality{quality="stale",source="binance_spot",strategy_id="mm-btc",symbol="BTC"} 1.0'
    ) in payload

    try:
        metrics.set_external_reference(
            STRATEGY,
            SYMBOL,
            **common,
            effective_weight=Decimal("0.3"),
            age=timedelta(milliseconds=20),
            quality=ExternalReferenceQuality.HEALTHY,
        )
    except ValueError as error:
        assert "effective external weight" in str(error)
    else:
        raise AssertionError("effective weight above configured weight must be rejected")


def test_structured_alert_payload_is_stable_and_utc() -> None:
    payload = AlertPayload(
        rule_id="mm.unknown_sla",
        title="UNKNOWN exceeded SLA",
        message="Keep slot blocked and reconcile.",
        severity=AlertSeverity.CRITICAL,
        observed_at=datetime(2026, 7, 11, tzinfo=UTC),
        labels={"symbol": "BTC", "strategy_id": "mm-btc"},
        runbook_url="docs/deployment.md#p0-alert",
    )
    assert payload.to_dict() == {
        "rule_id": "mm.unknown_sla",
        "title": "UNKNOWN exceeded SLA",
        "message": "Keep slot blocked and reconcile.",
        "severity": "critical",
        "observed_at": "2026-07-11T00:00:00+00:00",
        "labels": {"strategy_id": "mm-btc", "symbol": "BTC"},
        "runbook_url": "docs/deployment.md#p0-alert",
    }
    assert "strategy_id=mm-btc" in payload.render_text()


def test_canary_start_report_never_infers_missing_shadow_time() -> None:
    report = OperationalGateChecker().canary_start_report(
        _release_evidence(shadow_days=13),
        _artifacts(),
        generated_at=datetime(2026, 7, 11, tzinfo=UTC),
    )
    assert report.allowed is False
    assert report.directive == CanaryDirective.HALTED
    assert report.reasons == ("shadow_observation_incomplete",)
    decoded = json.loads(report.to_json())
    assert decoded["allowed"] is False
    assert "does not prove that a real-time soak occurred" in decoded["disclaimer"]
    assert decoded["metadata"]["envelope_version"] == "3"


def test_expansion_report_requires_one_dimension_and_rollback() -> None:
    report = OperationalGateChecker().expansion_report(
        _expansion_evidence(),
        ExpansionChange(
            current_config_version=7,
            target_config_version=8,
            rollback_config_version=None,
            changed_dimensions=(ExpansionDimension.QUOTE_SIZE, ExpansionDimension.SYMBOL),
            observation_window_id="window-2026-09",
        ),
        generated_at=datetime(2026, 9, 1, tzinfo=UTC),
    )
    assert report.allowed is False
    assert set(report.reasons) == {"multiple_expansion_dimensions", "rollback_version_missing"}

    passed = OperationalGateChecker().expansion_report(
        _expansion_evidence(),
        ExpansionChange(
            current_config_version=7,
            target_config_version=8,
            rollback_config_version=7,
            changed_dimensions=(ExpansionDimension.QUOTE_SIZE,),
            observation_window_id="window-2026-09",
        ),
    )
    assert passed.allowed is True


def test_observability_and_runbook_assets_are_machine_readable() -> None:
    dashboard = json.loads(
        (ROOT / "configs/grafana/dashboards/hypeedge-market-making.json").read_text(encoding="utf-8")
    )
    alert_rules = yaml.safe_load((ROOT / "configs/prometheus/market_making_alerts.yml").read_text(encoding="utf-8"))
    release_gates = yaml.safe_load(
        (ROOT / "configs/operations/market_making_release_gates.yaml").read_text(encoding="utf-8")
    )
    assert dashboard["uid"] == "hypeedge-mm-ops"
    assert len(dashboard["panels"]) >= 10
    assert alert_rules["groups"][0]["name"] == "hypeedge-market-making-p0"
    dashboard_json = json.dumps(dashboard)
    alert_json = json.dumps(alert_rules)
    assert "hype_mm_external_effective_weight" in dashboard_json
    assert "HypeEdgeExternalReferenceStaleWithWeight" in alert_json
    assert "HypeEdgeExternalBasisUnstable" in alert_json
    assert release_gates["completion_claims"]["shadow_soak_completed"] is False
    assert release_gates["completion_claims"]["testnet_clean_soak_completed"] is False
