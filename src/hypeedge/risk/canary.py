"""Versioned shadow, testnet, and mainnet-canary release gates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from hypeedge.core.types import Usd


class CanaryDirective(StrEnum):
    """Most permissive runtime directive allowed by the current evidence."""

    RUNNING = "running"
    PAUSED = "paused"
    CANCEL_ONLY = "cancel_only"
    HALTED = "halted"


@dataclass(frozen=True, slots=True)
class CanaryRiskEnvelope:
    """Immutable, versioned limits activated before a mainnet canary starts."""

    version: int
    max_deployed_equity: Usd
    max_quote_notional: Usd
    max_daily_loss: Usd
    max_cumulative_loss: Usd
    max_daily_actions: int
    max_total_actions: int
    min_action_credits: int
    min_cancel_headroom: int
    max_forced_flatten_count: int
    max_forced_flatten_cost: Usd
    unknown_sla: timedelta
    max_duration: timedelta
    max_filled_volume: Usd

    def __post_init__(self) -> None:
        if self.version <= 0:
            raise ValueError("canary envelope version must be positive")
        non_negative = (
            self.max_deployed_equity,
            self.max_quote_notional,
            self.max_daily_loss,
            self.max_cumulative_loss,
            self.max_forced_flatten_cost,
            self.max_filled_volume,
        )
        if any(value < 0 for value in non_negative):
            raise ValueError("canary monetary limits cannot be negative")
        if (
            min(
                self.max_daily_actions,
                self.max_total_actions,
                self.min_action_credits,
                self.min_cancel_headroom,
                self.max_forced_flatten_count,
            )
            < 0
        ):
            raise ValueError("canary count limits cannot be negative")
        if self.max_daily_actions > self.max_total_actions:
            raise ValueError("daily actions cannot exceed total actions")
        if self.unknown_sla <= timedelta(0) or self.max_duration <= timedelta(0):
            raise ValueError("canary time limits must be positive")


@dataclass(frozen=True, slots=True)
class ReleaseEvidence:
    """Auditable evidence required before moving between deployment stages."""

    shadow_complete_utc_days: int
    testnet_clean_utc_days: int
    reconciliation_diff_count: int
    duplicate_order_count: int
    risk_bypass_count: int
    hard_inventory_breach_count: int
    unresolved_unknown_count: int
    pessimistic_net_edge_usdc: Usd
    projected_runway_hours: Decimal
    required_runway_hours: Decimal


@dataclass(frozen=True, slots=True)
class CanaryObservation:
    """Authoritative Postgres-derived live canary state."""

    observed_at: datetime
    started_at: datetime
    deployed_equity: Usd
    live_quote_notional: Usd
    daily_pnl: Usd
    cumulative_pnl: Usd
    daily_actions: int
    total_actions: int
    action_credits: int
    cancel_headroom: int
    forced_flatten_count: int
    forced_flatten_cost: Usd
    oldest_unknown_age: timedelta | None
    filled_volume: Usd
    reconciliation_healthy: bool
    market_data_healthy: bool
    account_healthy: bool


@dataclass(frozen=True, slots=True)
class ExpansionEvidence:
    """Statistical and operational evidence required before risk expansion."""

    complete_utc_days: int
    independent_inventory_episodes: int
    regime_coverage_complete: bool
    accounting_edge_ci95_lower: Usd
    marginal_usdc_per_action: Usd
    critical_reconciliation_diff_count: int
    duplicate_order_count: int
    hard_inventory_breach_count: int
    unknown_with_terminal_fact_count: int
    unknown_total_count: int
    directional_concentration: Decimal


@dataclass(frozen=True, slots=True)
class GateDecision:
    allowed: bool
    directive: CanaryDirective
    reasons: tuple[str, ...]


class CanaryGateEvaluator:
    """Pure fail-closed evaluator used by API, supervisor, and deployment checks."""

    def __init__(
        self,
        *,
        shadow_min_days: int = 14,
        testnet_min_days: int = 14,
        expansion_min_days: int = 30,
        expansion_min_episodes: int = 30,
        max_directional_concentration: Decimal = Decimal("0.50"),
    ) -> None:
        if min(shadow_min_days, testnet_min_days, expansion_min_days, expansion_min_episodes) <= 0:
            raise ValueError("release-gate sample minimums must be positive")
        if not Decimal(0) < max_directional_concentration <= Decimal(1):
            raise ValueError("directional concentration limit must be in (0, 1]")
        self._shadow_min_days = shadow_min_days
        self._testnet_min_days = testnet_min_days
        self._expansion_min_days = expansion_min_days
        self._expansion_min_episodes = expansion_min_episodes
        self._max_directional_concentration = max_directional_concentration

    def can_start_canary(self, evidence: ReleaseEvidence) -> GateDecision:
        reasons: list[str] = []
        if evidence.shadow_complete_utc_days < self._shadow_min_days:
            reasons.append("shadow_observation_incomplete")
        if evidence.testnet_clean_utc_days < self._testnet_min_days:
            reasons.append("testnet_soak_incomplete")
        if evidence.reconciliation_diff_count:
            reasons.append("reconciliation_diff_present")
        if evidence.duplicate_order_count:
            reasons.append("duplicate_orders_present")
        if evidence.risk_bypass_count:
            reasons.append("risk_bypass_present")
        if evidence.hard_inventory_breach_count:
            reasons.append("hard_inventory_breach_present")
        if evidence.unresolved_unknown_count:
            reasons.append("unresolved_unknown_present")
        if evidence.pessimistic_net_edge_usdc < 0:
            reasons.append("pessimistic_edge_negative")
        if evidence.projected_runway_hours < evidence.required_runway_hours:
            reasons.append("action_runway_insufficient")
        directive = CanaryDirective.RUNNING if not reasons else CanaryDirective.HALTED
        return GateDecision(not reasons, directive, tuple(reasons))

    def evaluate_live(
        self,
        envelope: CanaryRiskEnvelope,
        observation: CanaryObservation,
    ) -> GateDecision:
        halted: list[str] = []
        cancel_only: list[str] = []
        paused: list[str] = []
        if not observation.reconciliation_healthy:
            halted.append("reconciliation_unhealthy")
        if observation.deployed_equity > envelope.max_deployed_equity:
            halted.append("deployed_equity_limit")
        if -observation.cumulative_pnl > envelope.max_cumulative_loss:
            halted.append("cumulative_loss_limit")
        if observation.total_actions > envelope.max_total_actions:
            halted.append("total_action_limit")
        if observation.observed_at - observation.started_at > envelope.max_duration:
            halted.append("maximum_duration")
        if observation.filled_volume > envelope.max_filled_volume:
            halted.append("filled_volume_limit")
        if observation.forced_flatten_count > envelope.max_forced_flatten_count:
            halted.append("forced_flatten_count_limit")
        if observation.forced_flatten_cost > envelope.max_forced_flatten_cost:
            halted.append("forced_flatten_cost_limit")

        if observation.oldest_unknown_age is not None and observation.oldest_unknown_age > envelope.unknown_sla:
            cancel_only.append("unknown_sla_exceeded")
        if observation.action_credits < envelope.min_action_credits:
            cancel_only.append("action_credits_below_minimum")
        if observation.cancel_headroom < envelope.min_cancel_headroom:
            cancel_only.append("cancel_headroom_below_minimum")
        if not observation.market_data_healthy or not observation.account_healthy:
            cancel_only.append("runtime_data_unhealthy")

        if observation.live_quote_notional > envelope.max_quote_notional:
            paused.append("quote_notional_limit")
        if -observation.daily_pnl > envelope.max_daily_loss:
            paused.append("daily_loss_limit")
        if observation.daily_actions > envelope.max_daily_actions:
            paused.append("daily_action_limit")

        if halted:
            return GateDecision(False, CanaryDirective.HALTED, tuple(halted + cancel_only + paused))
        if cancel_only:
            return GateDecision(False, CanaryDirective.CANCEL_ONLY, tuple(cancel_only + paused))
        if paused:
            return GateDecision(False, CanaryDirective.PAUSED, tuple(paused))
        return GateDecision(True, CanaryDirective.RUNNING, ())

    def can_expand(self, evidence: ExpansionEvidence) -> GateDecision:
        reasons: list[str] = []
        if evidence.complete_utc_days < self._expansion_min_days:
            reasons.append("observation_window_incomplete")
        if evidence.independent_inventory_episodes < self._expansion_min_episodes:
            reasons.append("inventory_episode_sample_insufficient")
        if not evidence.regime_coverage_complete:
            reasons.append("regime_coverage_incomplete")
        if evidence.accounting_edge_ci95_lower <= 0:
            reasons.append("accounting_edge_ci_not_positive")
        if evidence.marginal_usdc_per_action < Usd("1.25"):
            reasons.append("marginal_usdc_per_action_below_gate")
        if evidence.critical_reconciliation_diff_count:
            reasons.append("critical_reconciliation_diff_present")
        if evidence.duplicate_order_count:
            reasons.append("duplicate_orders_present")
        if evidence.hard_inventory_breach_count:
            reasons.append("hard_inventory_breach_present")
        if evidence.unknown_with_terminal_fact_count != evidence.unknown_total_count:
            reasons.append("unknown_without_terminal_fact")
        if evidence.directional_concentration > self._max_directional_concentration:
            reasons.append("directional_pnl_concentration")
        directive = CanaryDirective.RUNNING if not reasons else CanaryDirective.HALTED
        return GateDecision(not reasons, directive, tuple(reasons))
