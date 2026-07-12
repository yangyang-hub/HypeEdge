"""Bounded-cardinality Prometheus projection for market-making health.

The metrics in this module are operational projections only.  Callers must
derive values from authoritative runtime/Postgres facts; Prometheus and
ClickHouse are never used as order, PnL, quota, or configuration truth.
"""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from enum import StrEnum

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram

from hypeedge.core.types import Price, Size, StrategyId, Symbol, Usd
from hypeedge.risk.canary import CanaryDirective


class FreshnessSource(StrEnum):
    FEED = "feed"
    USER_STREAM = "user_stream"
    ACCOUNT = "account"
    CREDIT = "credit"
    EXTERNAL_REFERENCE = "external_reference"


class ExternalReferenceQuality(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    STALE = "stale"
    DISABLED = "disabled"


class InventoryBand(StrEnum):
    NORMAL = "normal"
    SOFT = "soft"
    HARD = "hard"
    EMERGENCY = "emergency"


class ExecutionOutcome(StrEnum):
    SUBMIT = "submit"
    CANCEL = "cancel"
    MODIFY = "modify"
    REJECT = "reject"
    UNKNOWN = "unknown"
    BATCH_PARTIAL = "batch_partial"


class LatencyStage(StrEnum):
    RECEIPT_TO_DECISION = "receipt_to_decision"
    DECISION_TO_SEND = "decision_to_send"
    ACK = "ack"
    CANCEL = "cancel"
    EVENT_LOOP_LAG = "event_loop_lag"


class MarketMakingMetrics:
    """Explicit write interface for market-making operational telemetry."""

    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self._freshness_age = Gauge(
            "hype_mm_freshness_age_seconds",
            "Age of the latest authoritative market-making fact",
            ["strategy_id", "symbol", "source"],
            registry=registry,
        )
        self._freshness_limit = Gauge(
            "hype_mm_freshness_limit_seconds",
            "Configured maximum age for an authoritative market-making fact",
            ["strategy_id", "symbol", "source"],
            registry=registry,
        )
        self._freshness_healthy = Gauge(
            "hype_mm_freshness_healthy",
            "Whether an authoritative market-making fact is healthy and fresh",
            ["strategy_id", "symbol", "source"],
            registry=registry,
        )
        self._reference_price = Gauge(
            "hype_mm_reference_price",
            "Current fair or reservation price projected from strategy runtime",
            ["strategy_id", "symbol", "kind"],
            registry=registry,
        )
        self._external_reference_price = Gauge(
            "hype_mm_external_reference_price",
            "External reference raw or basis-adjusted price; never an order or oracle fact",
            ["strategy_id", "symbol", "source", "kind"],
            registry=registry,
        )
        self._external_basis_bps = Gauge(
            "hype_mm_external_basis_bps",
            "Basis adjustment between the external reference and Hyperliquid local anchor",
            ["strategy_id", "symbol", "source"],
            registry=registry,
        )
        self._external_basis_limit_bps = Gauge(
            "hype_mm_external_basis_limit_bps",
            "Configured absolute external basis alert threshold",
            ["strategy_id", "symbol", "source"],
            registry=registry,
        )
        self._external_divergence_bps = Gauge(
            "hype_mm_external_divergence_bps",
            "Adjusted external reference divergence from the Hyperliquid local anchor",
            ["strategy_id", "symbol", "source"],
            registry=registry,
        )
        self._external_divergence_limit_bps = Gauge(
            "hype_mm_external_divergence_limit_bps",
            "Configured absolute adjusted external divergence alert threshold",
            ["strategy_id", "symbol", "source"],
            registry=registry,
        )
        self._external_weight = Gauge(
            "hype_mm_external_effective_weight",
            "Configured or effective bounded contribution of an external reference",
            ["strategy_id", "symbol", "source", "kind"],
            registry=registry,
        )
        self._external_confidence = Gauge(
            "hype_mm_external_confidence",
            "External reference confidence after feed and basis quality checks",
            ["strategy_id", "symbol", "source"],
            registry=registry,
        )
        self._external_freshness_age = Gauge(
            "hype_mm_external_freshness_age_seconds",
            "Age of the latest external reference observation",
            ["strategy_id", "symbol", "source"],
            registry=registry,
        )
        self._external_freshness_limit = Gauge(
            "hype_mm_external_freshness_limit_seconds",
            "Configured maximum age of an external reference observation",
            ["strategy_id", "symbol", "source"],
            registry=registry,
        )
        self._external_quality = Gauge(
            "hype_mm_external_quality",
            "One-hot external reference quality; the source is reference-only and not an oracle",
            ["strategy_id", "symbol", "source", "quality"],
            registry=registry,
        )
        self._quote_present = Gauge(
            "hype_mm_quote_present",
            "Whether a desired or authoritative live quote is present",
            ["strategy_id", "symbol", "view", "side"],
            registry=registry,
        )
        self._quote_price = Gauge(
            "hype_mm_quote_price",
            "Desired or authoritative live quote price",
            ["strategy_id", "symbol", "view", "side"],
            registry=registry,
        )
        self._quote_size = Gauge(
            "hype_mm_quote_size",
            "Desired or authoritative live quote size",
            ["strategy_id", "symbol", "view", "side"],
            registry=registry,
        )
        self._quote_age = Gauge(
            "hype_mm_quote_age_seconds",
            "Age of a desired or authoritative live quote",
            ["strategy_id", "symbol", "view", "side"],
            registry=registry,
        )
        self._quote_uptime = Gauge(
            "hype_mm_quote_uptime_ratio",
            "Fraction of an observation window with at least one valid maker quote",
            ["strategy_id", "symbol", "window"],
            registry=registry,
        )
        self._inventory_notional = Gauge(
            "hype_mm_inventory_notional_usdc",
            "Signed authoritative inventory notional",
            ["strategy_id", "symbol"],
            registry=registry,
        )
        self._inventory_utilization = Gauge(
            "hype_mm_inventory_hard_limit_utilization_ratio",
            "Absolute inventory notional divided by the hard inventory limit",
            ["strategy_id", "symbol"],
            registry=registry,
        )
        self._inventory_band = Gauge(
            "hype_mm_inventory_band",
            "One-hot authoritative inventory risk band",
            ["strategy_id", "symbol", "band"],
            registry=registry,
        )
        self._margin_utilization = Gauge(
            "hype_mm_margin_utilization_ratio",
            "Authoritative margin utilization ratio",
            ["strategy_id", "symbol"],
            registry=registry,
        )
        self._liquidation_distance = Gauge(
            "hype_mm_liquidation_distance_ratio",
            "Absolute mark-to-liquidation distance divided by mark price",
            ["strategy_id", "symbol"],
            registry=registry,
        )
        self._funding_carry = Gauge(
            "hype_mm_funding_carry_usdc",
            "Authoritative or accrued funding carry for the active session",
            ["strategy_id", "symbol"],
            registry=registry,
        )
        self._action_value = Gauge(
            "hype_mm_action_budget",
            "Current address, cancel, IP, burn, earn, runway, and reserve projection",
            ["strategy_id", "symbol", "kind"],
            registry=registry,
        )
        self._execution_outcomes = Counter(
            "hype_mm_execution_outcomes_total",
            "Actual network-boundary execution outcomes",
            ["strategy_id", "symbol", "outcome"],
            registry=registry,
        )
        self._unknown_orders = Gauge(
            "hype_mm_unknown_orders",
            "Number of authoritative unresolved UNKNOWN orders",
            ["strategy_id", "symbol"],
            registry=registry,
        )
        self._oldest_unknown_age = Gauge(
            "hype_mm_oldest_unknown_age_seconds",
            "Age of the oldest unresolved UNKNOWN order",
            ["strategy_id", "symbol"],
            registry=registry,
        )
        self._unknown_sla = Gauge(
            "hype_mm_unknown_sla_seconds",
            "Configured UNKNOWN reconciliation SLA",
            ["strategy_id", "symbol"],
            registry=registry,
        )
        self._latency = Histogram(
            "hype_mm_latency_seconds",
            "Market-making critical-path latency",
            ["strategy_id", "symbol", "stage"],
            buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
            registry=registry,
        )
        self._reconciliation_diff = Gauge(
            "hype_mm_reconciliation_diff_count",
            "Unresolved authoritative reconciliation differences",
            ["strategy_id", "symbol", "severity"],
            registry=registry,
        )
        self._runtime_info = Gauge(
            "hype_mm_runtime_info",
            "One-hot market-maker lifecycle state and active config version",
            ["strategy_id", "symbol", "state", "config_version"],
            registry=registry,
        )
        self._canary_directive = Gauge(
            "hype_mm_canary_directive",
            "One-hot directive allowed by the active versioned canary envelope",
            ["strategy_id", "directive"],
            registry=registry,
        )
        self._canary_envelope_version = Gauge(
            "hype_mm_canary_envelope_version",
            "Active canary risk-envelope version",
            ["strategy_id"],
            registry=registry,
        )
        self._postgres_available = Gauge(
            "hype_mm_postgres_available",
            "Whether the authoritative Postgres transaction store is available",
            registry=registry,
        )
        self._emergency_cancel_failures = Counter(
            "hype_mm_emergency_cancel_failures_total",
            "Failed emergency cancel-all attempts",
            ["sub_account"],
            registry=registry,
        )
        self._last_inventory_band: dict[tuple[str, str], InventoryBand] = {}
        self._last_runtime: dict[tuple[str, str], tuple[str, int]] = {}
        self._last_external_quality: dict[tuple[str, str, str], ExternalReferenceQuality] = {}

    def observe_freshness(
        self,
        strategy_id: StrategyId,
        symbol: Symbol,
        source: FreshnessSource,
        *,
        age: timedelta | None,
        max_age: timedelta,
        healthy: bool,
    ) -> None:
        if max_age <= timedelta(0):
            raise ValueError("freshness max_age must be positive")
        labels = (str(strategy_id), str(symbol), source.value)
        age_seconds = max_age.total_seconds() + 1 if age is None else max(0.0, age.total_seconds())
        self._freshness_age.labels(*labels).set(age_seconds)
        self._freshness_limit.labels(*labels).set(max_age.total_seconds())
        self._freshness_healthy.labels(*labels).set(1 if healthy and age is not None and age <= max_age else 0)

    def set_reference_prices(self, strategy_id: StrategyId, symbol: Symbol, *, fair: Price, reservation: Price) -> None:
        if fair <= 0 or reservation <= 0:
            raise ValueError("reference prices must be positive")
        labels = (str(strategy_id), str(symbol))
        self._reference_price.labels(*labels, "fair").set(_metric_number(fair))
        self._reference_price.labels(*labels, "reservation").set(_metric_number(reservation))

    def set_external_reference(
        self,
        strategy_id: StrategyId,
        symbol: Symbol,
        *,
        source: str,
        raw_price: Price | None,
        adjusted_price: Price | None,
        basis_bps: Decimal,
        basis_limit_bps: Decimal,
        divergence_bps: Decimal,
        divergence_limit_bps: Decimal,
        configured_weight: Decimal,
        effective_weight: Decimal,
        confidence: Decimal,
        age: timedelta | None,
        max_age: timedelta,
        quality: ExternalReferenceQuality,
    ) -> None:
        """Project bounded external-reference health without making it authoritative."""

        if not source.strip():
            raise ValueError("external reference source cannot be empty")
        if max_age <= timedelta(0) or basis_limit_bps <= 0 or divergence_limit_bps <= 0:
            raise ValueError("external reference limits must be positive")
        if not Decimal(0) <= configured_weight <= Decimal(1):
            raise ValueError("configured external weight must be in [0, 1]")
        if not Decimal(0) <= effective_weight <= configured_weight:
            raise ValueError("effective external weight must be in [0, configured_weight]")
        if not Decimal(0) <= confidence <= Decimal(1):
            raise ValueError("external confidence must be in [0, 1]")
        if (raw_price is None) != (adjusted_price is None):
            raise ValueError("external raw and adjusted prices must both be present or absent")
        if raw_price is not None and (raw_price <= 0 or adjusted_price is None or adjusted_price <= 0):
            raise ValueError("external reference prices must be positive")
        if quality is ExternalReferenceQuality.DISABLED and effective_weight != 0:
            raise ValueError("disabled external reference must have zero effective weight")

        labels = (str(strategy_id), str(symbol), source)
        self._external_reference_price.labels(*labels, "raw").set(_metric_number(raw_price) if raw_price else 0)
        self._external_reference_price.labels(*labels, "adjusted").set(
            _metric_number(adjusted_price) if adjusted_price else 0
        )
        self._external_basis_bps.labels(*labels).set(_metric_number(basis_bps))
        self._external_basis_limit_bps.labels(*labels).set(_metric_number(basis_limit_bps))
        self._external_divergence_bps.labels(*labels).set(_metric_number(divergence_bps))
        self._external_divergence_limit_bps.labels(*labels).set(_metric_number(divergence_limit_bps))
        self._external_weight.labels(*labels, "configured").set(_metric_number(configured_weight))
        self._external_weight.labels(*labels, "effective").set(_metric_number(effective_weight))
        self._external_confidence.labels(*labels).set(_metric_number(confidence))
        age_seconds = max_age.total_seconds() + 1 if age is None else max(0.0, age.total_seconds())
        self._external_freshness_age.labels(*labels).set(age_seconds)
        self._external_freshness_limit.labels(*labels).set(max_age.total_seconds())

        previous = self._last_external_quality.get(labels)
        if previous is not None and previous is not quality:
            self._external_quality.labels(*labels, previous.value).set(0)
        self._external_quality.labels(*labels, quality.value).set(1)
        self._last_external_quality[labels] = quality

    def set_quote(
        self,
        strategy_id: StrategyId,
        symbol: Symbol,
        *,
        view: str,
        side: str,
        price: Price | None,
        size: Size | None,
        age: timedelta | None,
    ) -> None:
        if view not in {"desired", "live"} or side not in {"buy", "sell"}:
            raise ValueError("quote labels must use desired/live and buy/sell")
        if (price is None) != (size is None):
            raise ValueError("quote price and size must both be present or absent")
        labels = (str(strategy_id), str(symbol), view, side)
        present = price is not None
        self._quote_present.labels(*labels).set(1 if present else 0)
        self._quote_price.labels(*labels).set(_metric_number(price) if price is not None else 0)
        self._quote_size.labels(*labels).set(_metric_number(size) if size is not None else 0)
        self._quote_age.labels(*labels).set(max(0.0, age.total_seconds()) if age is not None else 0)

    def set_quote_uptime(self, strategy_id: StrategyId, symbol: Symbol, *, window: str, ratio: Decimal) -> None:
        if not Decimal(0) <= ratio <= Decimal(1):
            raise ValueError("quote uptime ratio must be in [0, 1]")
        self._quote_uptime.labels(str(strategy_id), str(symbol), window).set(_metric_number(ratio))

    def set_inventory(
        self,
        strategy_id: StrategyId,
        symbol: Symbol,
        *,
        notional: Usd,
        hard_limit: Usd,
        band: InventoryBand,
        margin_utilization: Decimal,
        liquidation_distance: Decimal,
        funding_carry: Usd,
    ) -> None:
        if hard_limit <= 0:
            raise ValueError("hard inventory limit must be positive")
        if margin_utilization < 0 or liquidation_distance < 0:
            raise ValueError("margin utilization and liquidation distance cannot be negative")
        key = (str(strategy_id), str(symbol))
        self._inventory_notional.labels(*key).set(_metric_number(notional))
        self._inventory_utilization.labels(*key).set(_metric_number(abs(notional) / hard_limit))
        previous = self._last_inventory_band.get(key)
        if previous is not None and previous != band:
            self._inventory_band.labels(*key, previous.value).set(0)
        self._inventory_band.labels(*key, band.value).set(1)
        self._last_inventory_band[key] = band
        self._margin_utilization.labels(*key).set(_metric_number(margin_utilization))
        self._liquidation_distance.labels(*key).set(_metric_number(liquidation_distance))
        self._funding_carry.labels(*key).set(_metric_number(funding_carry))

    def set_action_budget(
        self,
        strategy_id: StrategyId,
        symbol: Symbol,
        *,
        address_remaining: int,
        cancel_headroom: int,
        ip_weight_remaining: int,
        burn_per_hour: Decimal,
        earn_per_hour: Decimal,
        marginal_usdc_per_action: Decimal | None,
        runway_hours: Decimal | None,
        emergency_reserve: int,
    ) -> None:
        integer_values = (address_remaining, cancel_headroom, ip_weight_remaining, emergency_reserve)
        if min(integer_values) < 0 or burn_per_hour < 0 or earn_per_hour < 0:
            raise ValueError("action-budget values cannot be negative")
        values: dict[str, Decimal | int] = {
            "address_remaining": address_remaining,
            "cancel_headroom": cancel_headroom,
            "ip_weight_remaining": ip_weight_remaining,
            "burn_per_hour": burn_per_hour,
            "earn_per_hour": earn_per_hour,
            "marginal_usdc_per_action": marginal_usdc_per_action or Decimal(0),
            "runway_hours": runway_hours or Decimal(0),
            "emergency_reserve": emergency_reserve,
        }
        labels = (str(strategy_id), str(symbol))
        for kind, value in values.items():
            self._action_value.labels(*labels, kind).set(_metric_number(value))

    def record_execution_outcome(
        self, strategy_id: StrategyId, symbol: Symbol, outcome: ExecutionOutcome, *, count: int = 1
    ) -> None:
        if count <= 0:
            raise ValueError("execution outcome count must be positive")
        self._execution_outcomes.labels(str(strategy_id), str(symbol), outcome.value).inc(count)

    def set_unknown_orders(
        self,
        strategy_id: StrategyId,
        symbol: Symbol,
        *,
        count: int,
        oldest_age: timedelta | None,
        sla: timedelta,
    ) -> None:
        if count < 0 or sla <= timedelta(0):
            raise ValueError("UNKNOWN count must be non-negative and SLA positive")
        if count == 0 and oldest_age is not None:
            raise ValueError("oldest UNKNOWN age requires at least one UNKNOWN order")
        labels = (str(strategy_id), str(symbol))
        self._unknown_orders.labels(*labels).set(count)
        self._oldest_unknown_age.labels(*labels).set(max(0.0, oldest_age.total_seconds()) if oldest_age else 0)
        self._unknown_sla.labels(*labels).set(sla.total_seconds())

    def observe_latency(
        self, strategy_id: StrategyId, symbol: Symbol, stage: LatencyStage, seconds: Decimal | float
    ) -> None:
        if seconds < 0:
            raise ValueError("latency cannot be negative")
        self._latency.labels(str(strategy_id), str(symbol), stage.value).observe(_metric_number(seconds))

    def set_reconciliation_diff(self, strategy_id: StrategyId, symbol: Symbol, *, severity: str, count: int) -> None:
        if severity not in {"info", "warning", "critical"} or count < 0:
            raise ValueError("reconciliation severity/count is invalid")
        self._reconciliation_diff.labels(str(strategy_id), str(symbol), severity).set(count)

    def set_runtime(self, strategy_id: StrategyId, symbol: Symbol, *, state: str, config_version: int) -> None:
        if not state or config_version <= 0:
            raise ValueError("runtime state and config version must be valid")
        key = (str(strategy_id), str(symbol))
        previous = self._last_runtime.get(key)
        if previous is not None and previous != (state, config_version):
            self._runtime_info.labels(*key, previous[0], str(previous[1])).set(0)
        self._runtime_info.labels(*key, state, str(config_version)).set(1)
        self._last_runtime[key] = (state, config_version)

    def set_canary_directive(
        self, strategy_id: StrategyId, *, envelope_version: int, directive: CanaryDirective
    ) -> None:
        if envelope_version <= 0:
            raise ValueError("canary envelope version must be positive")
        strategy = str(strategy_id)
        for candidate in CanaryDirective:
            self._canary_directive.labels(strategy, candidate.value).set(1 if candidate == directive else 0)
        self._canary_envelope_version.labels(strategy).set(envelope_version)

    def set_postgres_available(self, available: bool) -> None:
        self._postgres_available.set(1 if available else 0)

    def record_emergency_cancel_failure(self, sub_account: str) -> None:
        if not sub_account:
            raise ValueError("sub_account cannot be empty")
        self._emergency_cancel_failures.labels(sub_account).inc()


def _metric_number(value: Decimal | float | int) -> float:
    """Convert at the Prometheus boundary; never feed this value back into trading."""
    return float(value)
