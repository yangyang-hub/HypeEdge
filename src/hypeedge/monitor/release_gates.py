"""Machine-readable operational checks for shadow, testnet, and canary releases.

The checker evaluates supplied evidence and artifact references. It does not
discover or infer soak completion, and therefore cannot silently turn missing
evidence into a pass.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from hypeedge.risk.canary import (
    CanaryDirective,
    CanaryGateEvaluator,
    CanaryRiskEnvelope,
    ExpansionEvidence,
    ReleaseEvidence,
)


class ReleaseStage(StrEnum):
    CANARY_START = "canary_start"
    EXPANSION = "expansion"


class ExpansionDimension(StrEnum):
    QUOTE_SIZE = "quote_size"
    SYMBOL = "symbol"
    QUOTE_LEVEL = "quote_level"
    RUST_HOT_PATH = "rust_hot_path"


@dataclass(frozen=True, slots=True)
class CanaryLaunchArtifacts:
    """Versioned references reviewed by an operator before canary activation."""

    envelope: CanaryRiskEnvelope
    shadow_report_id: str
    testnet_report_id: str
    failure_injection_report_id: str
    statistical_plan_version: str
    approved_by: str

    def __post_init__(self) -> None:
        references = (
            self.shadow_report_id,
            self.testnet_report_id,
            self.failure_injection_report_id,
            self.statistical_plan_version,
            self.approved_by,
        )
        if any(not value.strip() for value in references):
            raise ValueError("canary launch artifact references cannot be empty")


@dataclass(frozen=True, slots=True)
class ExpansionChange:
    """One reversible expansion request; only one dimension may change."""

    current_config_version: int
    target_config_version: int
    rollback_config_version: int | None
    changed_dimensions: tuple[ExpansionDimension, ...]
    observation_window_id: str

    def __post_init__(self) -> None:
        if self.current_config_version <= 0 or self.target_config_version <= self.current_config_version:
            raise ValueError("target config version must be newer than the positive current version")
        if self.rollback_config_version is not None and self.rollback_config_version <= 0:
            raise ValueError("rollback config version must be positive")
        if len(set(self.changed_dimensions)) != len(self.changed_dimensions):
            raise ValueError("expansion dimensions cannot be duplicated")


@dataclass(frozen=True, slots=True)
class GateCheck:
    check_id: str
    passed: bool
    actual: str
    requirement: str
    evidence_source: str

    def to_dict(self) -> dict[str, str | bool]:
        return {
            "check_id": self.check_id,
            "passed": self.passed,
            "actual": self.actual,
            "requirement": self.requirement,
            "evidence_source": self.evidence_source,
        }


@dataclass(frozen=True, slots=True)
class OperationalGateReport:
    stage: ReleaseStage
    generated_at: datetime
    allowed: bool
    directive: CanaryDirective
    reasons: tuple[str, ...]
    checks: tuple[GateCheck, ...]
    metadata: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if self.generated_at.tzinfo is None:
            raise ValueError("gate report timestamp must be timezone-aware")
        if self.allowed != all(check.passed for check in self.checks):
            raise ValueError("gate report allowed flag must match all checks")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "stage": self.stage.value,
            "generated_at": self.generated_at.astimezone(UTC).isoformat(),
            "allowed": self.allowed,
            "directive": self.directive.value,
            "reasons": list(self.reasons),
            "checks": [check.to_dict() for check in self.checks],
            "metadata": dict(self.metadata),
            "disclaimer": "This report evaluates supplied evidence; it does not prove that a real-time soak occurred.",
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))


class OperationalGateChecker:
    """Fail-closed deployment checker that mirrors the frozen release ladder."""

    def __init__(
        self,
        *,
        shadow_min_days: int = 14,
        testnet_min_days: int = 14,
        expansion_min_days: int = 30,
        expansion_min_episodes: int = 30,
        max_directional_concentration: Decimal = Decimal("0.50"),
    ) -> None:
        self._shadow_min_days = shadow_min_days
        self._testnet_min_days = testnet_min_days
        self._expansion_min_days = expansion_min_days
        self._expansion_min_episodes = expansion_min_episodes
        self._max_directional_concentration = max_directional_concentration
        self._evaluator = CanaryGateEvaluator(
            shadow_min_days=shadow_min_days,
            testnet_min_days=testnet_min_days,
            expansion_min_days=expansion_min_days,
            expansion_min_episodes=expansion_min_episodes,
            max_directional_concentration=max_directional_concentration,
        )

    def canary_start_report(
        self,
        evidence: ReleaseEvidence,
        artifacts: CanaryLaunchArtifacts,
        *,
        generated_at: datetime | None = None,
    ) -> OperationalGateReport:
        decision = self._evaluator.can_start_canary(evidence)
        checks = (
            _minimum(
                "shadow_complete_utc_days",
                evidence.shadow_complete_utc_days,
                self._shadow_min_days,
                "shadow_report",
            ),
            _minimum(
                "testnet_clean_utc_days",
                evidence.testnet_clean_utc_days,
                self._testnet_min_days,
                "testnet_report",
            ),
            _zero("reconciliation_diff_count", evidence.reconciliation_diff_count, "postgres_reconciliation_facts"),
            _zero("duplicate_order_count", evidence.duplicate_order_count, "postgres_execution_facts"),
            _zero("risk_bypass_count", evidence.risk_bypass_count, "postgres_risk_facts"),
            _zero("hard_inventory_breach_count", evidence.hard_inventory_breach_count, "postgres_risk_facts"),
            _zero("unresolved_unknown_count", evidence.unresolved_unknown_count, "postgres_execution_facts"),
            GateCheck(
                "pessimistic_net_edge_non_negative",
                evidence.pessimistic_net_edge_usdc >= 0,
                str(evidence.pessimistic_net_edge_usdc),
                ">= 0 USDC",
                "versioned_shadow_report",
            ),
            GateCheck(
                "projected_action_runway",
                evidence.projected_runway_hours >= evidence.required_runway_hours,
                str(evidence.projected_runway_hours),
                f">= {evidence.required_runway_hours} hours",
                "action_budget_projection",
            ),
            GateCheck(
                "versioned_canary_envelope",
                artifacts.envelope.version > 0,
                str(artifacts.envelope.version),
                "active version > 0",
                "postgres_config_fact",
            ),
        )
        allowed = decision.allowed and all(check.passed for check in checks)
        reasons = decision.reasons + (() if artifacts.envelope.version > 0 else ("canary_envelope_missing",))
        return OperationalGateReport(
            stage=ReleaseStage.CANARY_START,
            generated_at=generated_at or datetime.now(UTC),
            allowed=allowed,
            directive=CanaryDirective.RUNNING if allowed else CanaryDirective.HALTED,
            reasons=reasons,
            checks=checks,
            metadata=(
                ("envelope_version", str(artifacts.envelope.version)),
                ("shadow_report_id", artifacts.shadow_report_id),
                ("testnet_report_id", artifacts.testnet_report_id),
                ("failure_injection_report_id", artifacts.failure_injection_report_id),
                ("statistical_plan_version", artifacts.statistical_plan_version),
                ("approved_by", artifacts.approved_by),
            ),
        )

    def expansion_report(
        self,
        evidence: ExpansionEvidence,
        change: ExpansionChange,
        *,
        generated_at: datetime | None = None,
    ) -> OperationalGateReport:
        decision = self._evaluator.can_expand(evidence)
        checks = (
            _minimum("complete_utc_days", evidence.complete_utc_days, self._expansion_min_days, "accounting_report"),
            _minimum(
                "independent_inventory_episodes",
                evidence.independent_inventory_episodes,
                self._expansion_min_episodes,
                "preregistered_episode_report",
            ),
            GateCheck(
                "regime_coverage_complete",
                evidence.regime_coverage_complete,
                str(evidence.regime_coverage_complete).lower(),
                "true",
                "preregistered_regime_report",
            ),
            GateCheck(
                "accounting_edge_ci95_lower",
                evidence.accounting_edge_ci95_lower > 0,
                str(evidence.accounting_edge_ci95_lower),
                "> 0 USDC",
                "postgres_ledger_block_bootstrap",
            ),
            GateCheck(
                "marginal_usdc_per_action",
                evidence.marginal_usdc_per_action >= Decimal("1.25"),
                str(evidence.marginal_usdc_per_action),
                ">= 1.25 USDC/action",
                "postgres_ledger_and_action_facts",
            ),
            _zero(
                "critical_reconciliation_diff_count",
                evidence.critical_reconciliation_diff_count,
                "postgres_reconciliation_facts",
            ),
            _zero("duplicate_order_count", evidence.duplicate_order_count, "postgres_execution_facts"),
            _zero("hard_inventory_breach_count", evidence.hard_inventory_breach_count, "postgres_risk_facts"),
            GateCheck(
                "unknown_terminal_facts_complete",
                evidence.unknown_with_terminal_fact_count == evidence.unknown_total_count,
                f"{evidence.unknown_with_terminal_fact_count}/{evidence.unknown_total_count}",
                "all UNKNOWN/orphan records have terminal audit facts",
                "postgres_execution_facts",
            ),
            GateCheck(
                "directional_concentration",
                evidence.directional_concentration <= self._max_directional_concentration,
                str(evidence.directional_concentration),
                f"<= {self._max_directional_concentration}",
                "postgres_accounting_attribution",
            ),
            GateCheck(
                "single_expansion_dimension",
                len(change.changed_dimensions) == 1,
                ",".join(item.value for item in change.changed_dimensions) or "none",
                "exactly one dimension",
                "versioned_config_diff",
            ),
            GateCheck(
                "rollback_version_present",
                change.rollback_config_version is not None,
                str(change.rollback_config_version),
                "previous config version is recorded",
                "postgres_config_fact",
            ),
            GateCheck(
                "independent_observation_window",
                bool(change.observation_window_id.strip()),
                change.observation_window_id or "missing",
                "non-empty preregistered window id",
                "deployment_change_record",
            ),
        )
        extra_reasons: list[str] = []
        if len(change.changed_dimensions) != 1:
            extra_reasons.append("multiple_expansion_dimensions")
        if change.rollback_config_version is None:
            extra_reasons.append("rollback_version_missing")
        if not change.observation_window_id.strip():
            extra_reasons.append("observation_window_missing")
        allowed = decision.allowed and all(check.passed for check in checks)
        return OperationalGateReport(
            stage=ReleaseStage.EXPANSION,
            generated_at=generated_at or datetime.now(UTC),
            allowed=allowed,
            directive=CanaryDirective.RUNNING if allowed else CanaryDirective.HALTED,
            reasons=decision.reasons + tuple(extra_reasons),
            checks=checks,
            metadata=(
                ("current_config_version", str(change.current_config_version)),
                ("target_config_version", str(change.target_config_version)),
                ("rollback_config_version", str(change.rollback_config_version)),
                ("observation_window_id", change.observation_window_id),
            ),
        )


def _minimum(check_id: str, actual: int, minimum: int, source: str) -> GateCheck:
    return GateCheck(check_id, actual >= minimum, str(actual), f">= {minimum}", source)


def _zero(check_id: str, actual: int, source: str) -> GateCheck:
    return GateCheck(check_id, actual == 0, str(actual), "= 0", source)
