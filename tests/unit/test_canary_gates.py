"""Tests for versioned market-making deployment gates."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from hypeedge.core.types import Usd
from hypeedge.risk.canary import (
    CanaryDirective,
    CanaryGateEvaluator,
    CanaryObservation,
    CanaryRiskEnvelope,
    ExpansionEvidence,
    ReleaseEvidence,
)


def envelope() -> CanaryRiskEnvelope:
    return CanaryRiskEnvelope(
        version=1,
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


def observation(**changes: object) -> CanaryObservation:
    now = datetime(2026, 1, 2, tzinfo=UTC)
    values: dict[str, object] = {
        "observed_at": now,
        "started_at": now - timedelta(days=1),
        "deployed_equity": Usd("500"),
        "live_quote_notional": Usd("50"),
        "daily_pnl": Usd("1"),
        "cumulative_pnl": Usd("2"),
        "daily_actions": 10,
        "total_actions": 20,
        "action_credits": 3000,
        "cancel_headroom": 500,
        "forced_flatten_count": 0,
        "forced_flatten_cost": Usd("0"),
        "oldest_unknown_age": None,
        "filled_volume": Usd("100"),
        "reconciliation_healthy": True,
        "market_data_healthy": True,
        "account_healthy": True,
    }
    values.update(changes)
    return CanaryObservation(**values)  # type: ignore[arg-type]


def test_canary_envelope_rejects_invalid_action_limits() -> None:
    with pytest.raises(ValueError, match="daily actions"):
        CanaryRiskEnvelope(
            version=1,
            max_deployed_equity=Usd("1"),
            max_quote_notional=Usd("1"),
            max_daily_loss=Usd("1"),
            max_cumulative_loss=Usd("1"),
            max_daily_actions=2,
            max_total_actions=1,
            min_action_credits=0,
            min_cancel_headroom=0,
            max_forced_flatten_count=0,
            max_forced_flatten_cost=Usd("0"),
            unknown_sla=timedelta(seconds=1),
            max_duration=timedelta(seconds=1),
            max_filled_volume=Usd("1"),
        )


def test_release_gate_is_fail_closed_until_shadow_and_testnet_are_clean() -> None:
    decision = CanaryGateEvaluator().can_start_canary(
        ReleaseEvidence(
            shadow_complete_utc_days=13,
            testnet_clean_utc_days=14,
            reconciliation_diff_count=0,
            duplicate_order_count=0,
            risk_bypass_count=0,
            hard_inventory_breach_count=0,
            unresolved_unknown_count=0,
            pessimistic_net_edge_usdc=Usd("1"),
            projected_runway_hours=Decimal("24"),
            required_runway_hours=Decimal("24"),
        )
    )
    assert decision.allowed is False
    assert decision.directive == CanaryDirective.HALTED
    assert decision.reasons == ("shadow_observation_incomplete",)


def test_live_gate_prioritizes_halt_over_cancel_only_and_pause() -> None:
    decision = CanaryGateEvaluator().evaluate_live(
        envelope(),
        observation(
            reconciliation_healthy=False,
            oldest_unknown_age=timedelta(seconds=30),
            live_quote_notional=Usd("101"),
        ),
    )
    assert decision.directive == CanaryDirective.HALTED
    assert "reconciliation_unhealthy" in decision.reasons
    assert "unknown_sla_exceeded" in decision.reasons
    assert "quote_notional_limit" in decision.reasons


def test_live_gate_keeps_cancel_available_when_unknown_or_quota_is_unsafe() -> None:
    decision = CanaryGateEvaluator().evaluate_live(
        envelope(),
        observation(oldest_unknown_age=timedelta(seconds=16), action_credits=1999),
    )
    assert decision.allowed is False
    assert decision.directive == CanaryDirective.CANCEL_ONLY


def test_live_gate_pauses_on_daily_economic_limit() -> None:
    decision = CanaryGateEvaluator().evaluate_live(envelope(), observation(daily_pnl=Usd("-21")))
    assert decision.directive == CanaryDirective.PAUSED
    assert decision.reasons == ("daily_loss_limit",)


def test_expansion_requires_positive_independent_accounting_evidence() -> None:
    evaluator = CanaryGateEvaluator(expansion_min_episodes=20)
    passed = evaluator.can_expand(
        ExpansionEvidence(
            complete_utc_days=30,
            independent_inventory_episodes=20,
            regime_coverage_complete=True,
            accounting_edge_ci95_lower=Usd("0.01"),
            marginal_usdc_per_action=Usd("1.25"),
            critical_reconciliation_diff_count=0,
            duplicate_order_count=0,
            hard_inventory_breach_count=0,
            unknown_with_terminal_fact_count=3,
            unknown_total_count=3,
            directional_concentration=Decimal("0.40"),
        )
    )
    assert passed.allowed is True
    assert passed.directive == CanaryDirective.RUNNING

    failed = evaluator.can_expand(
        ExpansionEvidence(
            complete_utc_days=30,
            independent_inventory_episodes=20,
            regime_coverage_complete=True,
            accounting_edge_ci95_lower=Usd("0"),
            marginal_usdc_per_action=Usd("1.24"),
            critical_reconciliation_diff_count=0,
            duplicate_order_count=0,
            hard_inventory_breach_count=0,
            unknown_with_terminal_fact_count=2,
            unknown_total_count=3,
            directional_concentration=Decimal("0.60"),
        )
    )
    assert set(failed.reasons) == {
        "accounting_edge_ci_not_positive",
        "marginal_usdc_per_action_below_gate",
        "unknown_without_terminal_fact",
        "directional_pnl_concentration",
    }
