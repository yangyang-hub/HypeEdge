"""Layered account freshness and adaptive clearinghouse-state polling.

Market-making safety cannot collapse account health into one timestamp.  The
authenticated inventory stream, clearinghouse REST snapshot, user-stream
connection, and full reconciliation each have different update rates and
failure semantics.  This module keeps those facts separate and fails closed
when any required dimension is unknown, unhealthy, or stale.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol

import structlog

from hypeedge.account.tracker import AccountTracker
from hypeedge.core.models import AccountState, Position
from hypeedge.core.types import Price, Size, SubAccount, Symbol, Usd

logger = structlog.get_logger(__name__)


class AccountHealthDimension(StrEnum):
    """Independent account facts required before increasing risk."""

    INVENTORY = "inventory"
    CLEARINGHOUSE = "clearinghouse"
    USER_STREAM = "user_stream"
    RECONCILIATION = "reconciliation"


class FreshnessStatus(StrEnum):
    """Evaluated state of one account-health dimension."""

    UNKNOWN = "unknown"
    FRESH = "fresh"
    STALE = "stale"
    UNHEALTHY = "unhealthy"


@dataclass(frozen=True)
class AccountFreshnessThresholds:
    """Maximum ages for account facts with conservative production defaults."""

    inventory: timedelta = timedelta(seconds=5)
    clearinghouse: timedelta = timedelta(seconds=6)
    user_stream: timedelta = timedelta(seconds=5)
    reconciliation: timedelta = timedelta(minutes=10)
    max_future_skew: timedelta = timedelta(seconds=1)

    def __post_init__(self) -> None:
        for name in ("inventory", "clearinghouse", "user_stream", "reconciliation"):
            if getattr(self, name) <= timedelta(0):
                raise ValueError(f"{name} freshness threshold must be positive")
        if self.max_future_skew < timedelta(0):
            raise ValueError("max_future_skew must be non-negative")

    def for_dimension(self, dimension: AccountHealthDimension) -> timedelta:
        return {
            AccountHealthDimension.INVENTORY: self.inventory,
            AccountHealthDimension.CLEARINGHOUSE: self.clearinghouse,
            AccountHealthDimension.USER_STREAM: self.user_stream,
            AccountHealthDimension.RECONCILIATION: self.reconciliation,
        }[dimension]


@dataclass(frozen=True)
class FreshnessObservation:
    """Last locally received observation for one health dimension."""

    dimension: AccountHealthDimension
    observed_at: datetime | None
    healthy: bool
    reason: str | None = None


@dataclass(frozen=True)
class FreshnessResult:
    """Freshness evaluation at a specific point in time."""

    dimension: AccountHealthDimension
    status: FreshnessStatus
    observed_at: datetime | None
    age_seconds: float | None
    max_age_seconds: float
    reason: str | None

    @property
    def is_fresh(self) -> bool:
        return self.status == FreshnessStatus.FRESH


@dataclass(frozen=True)
class AccountHealthSnapshot:
    """Immutable, point-in-time evaluation of all account safety facts."""

    evaluated_at: datetime
    inventory: FreshnessResult
    clearinghouse: FreshnessResult
    user_stream: FreshnessResult
    reconciliation: FreshnessResult

    @property
    def dimensions(self) -> tuple[FreshnessResult, ...]:
        return (self.inventory, self.clearinghouse, self.user_stream, self.reconciliation)

    @property
    def allows_risk_increase(self) -> bool:
        return all(item.is_fresh for item in self.dimensions)

    @property
    def requires_cancel(self) -> bool:
        """Any missing critical account fact requires maker quotes to be removed."""
        return not self.allows_risk_increase

    @property
    def blocking_reasons(self) -> tuple[str, ...]:
        return tuple(
            f"{item.dimension.value}:{item.reason or item.status.value}"
            for item in self.dimensions
            if not item.is_fresh
        )


class AccountHealthProvider(Protocol):
    """Read boundary used by risk and dispatch-time account freshness gates."""

    def get_account_health(self, *, now: datetime | None = None) -> AccountHealthSnapshot:
        """Return a freshly evaluated immutable account-health snapshot."""
        ...


class MutableAccountHealthProvider(AccountHealthProvider, Protocol):
    """Write boundary for the stream, poller, and reconciler owners."""

    def record_success(
        self,
        dimension: AccountHealthDimension,
        *,
        observed_at: datetime | None = None,
    ) -> None: ...

    def record_failure(
        self,
        dimension: AccountHealthDimension,
        reason: str,
        *,
        observed_at: datetime | None = None,
    ) -> None: ...


class LayeredAccountHealthProvider:
    """In-memory account health projection with no implicit timestamp refresh."""

    def __init__(self, thresholds: AccountFreshnessThresholds | None = None) -> None:
        self._thresholds = thresholds or AccountFreshnessThresholds()
        self._observations = {
            dimension: FreshnessObservation(dimension, None, False, "not_observed")
            for dimension in AccountHealthDimension
        }

    def record_success(
        self,
        dimension: AccountHealthDimension,
        *,
        observed_at: datetime | None = None,
    ) -> None:
        timestamp = _require_aware(observed_at or datetime.now(UTC))
        self._observations[dimension] = FreshnessObservation(dimension, timestamp, True)

    def record_failure(
        self,
        dimension: AccountHealthDimension,
        reason: str,
        *,
        observed_at: datetime | None = None,
    ) -> None:
        if not reason:
            raise ValueError("account health failure reason must not be empty")
        timestamp = _require_aware(observed_at or datetime.now(UTC))
        self._observations[dimension] = FreshnessObservation(dimension, timestamp, False, reason)

    def get_account_health(self, *, now: datetime | None = None) -> AccountHealthSnapshot:
        evaluated_at = _require_aware(now or datetime.now(UTC))
        results = {
            dimension: self._evaluate(self._observations[dimension], evaluated_at)
            for dimension in AccountHealthDimension
        }
        return AccountHealthSnapshot(
            evaluated_at=evaluated_at,
            inventory=results[AccountHealthDimension.INVENTORY],
            clearinghouse=results[AccountHealthDimension.CLEARINGHOUSE],
            user_stream=results[AccountHealthDimension.USER_STREAM],
            reconciliation=results[AccountHealthDimension.RECONCILIATION],
        )

    def _evaluate(self, observation: FreshnessObservation, now: datetime) -> FreshnessResult:
        max_age = self._thresholds.for_dimension(observation.dimension)
        if observation.observed_at is None:
            return FreshnessResult(
                dimension=observation.dimension,
                status=FreshnessStatus.UNKNOWN,
                observed_at=None,
                age_seconds=None,
                max_age_seconds=max_age.total_seconds(),
                reason=observation.reason or "not_observed",
            )

        age = now - observation.observed_at
        if age < -self._thresholds.max_future_skew:
            return FreshnessResult(
                observation.dimension,
                FreshnessStatus.UNHEALTHY,
                observation.observed_at,
                age.total_seconds(),
                max_age.total_seconds(),
                "observed_at_in_future",
            )
        if not observation.healthy:
            return FreshnessResult(
                observation.dimension,
                FreshnessStatus.UNHEALTHY,
                observation.observed_at,
                max(0.0, age.total_seconds()),
                max_age.total_seconds(),
                observation.reason or "source_unhealthy",
            )
        if age > max_age:
            return FreshnessResult(
                observation.dimension,
                FreshnessStatus.STALE,
                observation.observed_at,
                age.total_seconds(),
                max_age.total_seconds(),
                "observation_stale",
            )
        return FreshnessResult(
            observation.dimension,
            FreshnessStatus.FRESH,
            observation.observed_at,
            max(0.0, age.total_seconds()),
            max_age.total_seconds(),
            None,
        )


@dataclass(frozen=True)
class PolledAccountSnapshot:
    """One authoritative clearinghouse-state response."""

    account_state: AccountState
    positions: tuple[Position, ...]
    received_at: datetime


class AccountStateSource(Protocol):
    """Async clearinghouse-state source used by :class:`AccountStatePoller`."""

    async def fetch_account_state(self) -> PolledAccountSnapshot: ...


RiskProximityEvaluator = Callable[[PolledAccountSnapshot], bool]
HealthFailureCallback = Callable[[str], Awaitable[None]]


class AccountStatePoller:
    """Poll clearinghouse state at an adaptive, rate-budget-friendly cadence."""

    def __init__(
        self,
        source: AccountStateSource,
        tracker: AccountTracker,
        health: MutableAccountHealthProvider,
        *,
        normal_interval_seconds: float = 3.0,
        near_risk_interval_seconds: float = 1.0,
        risk_proximity_evaluator: RiskProximityEvaluator | None = None,
        on_health_failure: HealthFailureCallback | None = None,
    ) -> None:
        if not 2.0 <= normal_interval_seconds <= 5.0:
            raise ValueError("normal account poll interval must be between 2 and 5 seconds")
        if not 0.5 <= near_risk_interval_seconds <= 2.0:
            raise ValueError("near-risk account poll interval must be between 0.5 and 2 seconds")
        if near_risk_interval_seconds >= normal_interval_seconds:
            raise ValueError("near-risk account poll interval must be lower than normal interval")
        self._source = source
        self._tracker = tracker
        self._health = health
        self._normal_interval = normal_interval_seconds
        self._near_risk_interval = near_risk_interval_seconds
        self._risk_proximity_evaluator = risk_proximity_evaluator or _default_near_risk
        self._on_health_failure = on_health_failure
        self._stop_event = asyncio.Event()
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    async def poll_once(self) -> float:
        """Fetch and apply one snapshot; return the next adaptive interval."""
        try:
            snapshot = await self._source.fetch_account_state()
            received_at = _require_aware(snapshot.received_at)
            self._apply_snapshot(snapshot)
            self._health.record_success(AccountHealthDimension.CLEARINGHOUSE, observed_at=received_at)
            near_risk = self._risk_proximity_evaluator(snapshot)
            interval = self._near_risk_interval if near_risk else self._normal_interval
            logger.debug(
                "account_state_poll_succeeded",
                equity=float(snapshot.account_state.equity),
                available_balance=float(snapshot.account_state.available_balance),
                positions=len(snapshot.positions),
                near_risk=near_risk,
                next_interval_seconds=interval,
            )
            return interval
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            reason = f"clearinghouse_poll_failed:{type(exc).__name__}"
            self._health.record_failure(AccountHealthDimension.CLEARINGHOUSE, reason)
            logger.exception("account_state_poll_failed", reason=reason)
            if self._on_health_failure is not None:
                await self._on_health_failure(reason)
            return self._near_risk_interval

    async def run(self) -> None:
        """Poll immediately, then sleep interruptibly until stopped."""
        if self._running:
            raise RuntimeError("account state poller is already running")
        self._running = True
        self._stop_event.clear()
        logger.info("account_state_poller_started")
        try:
            while not self._stop_event.is_set():
                interval = await self.poll_once()
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                except TimeoutError:
                    continue
        except asyncio.CancelledError:
            raise
        finally:
            self._running = False
            logger.info("account_state_poller_stopped")

    async def stop(self) -> None:
        self._stop_event.set()

    def _apply_snapshot(self, snapshot: PolledAccountSnapshot) -> None:
        self._tracker.update_account_state(snapshot.account_state)
        current_symbols = set(self._tracker.get_all_positions())
        exchange_symbols: set[Symbol] = set()
        for position in snapshot.positions:
            exchange_symbols.add(position.symbol)
            if position.is_flat:
                self._tracker.remove_position(position.symbol)
            else:
                self._tracker.update_position_from_exchange(position.symbol, position)
        for missing_symbol in current_symbols - exchange_symbols:
            self._tracker.remove_position(missing_symbol)


class ClearinghouseRestClient(Protocol):
    """Narrow REST client boundary needed by the account-state adapter."""

    async def get_clearinghouse_state(self, user: str) -> dict[str, object]: ...


class RestAccountStateSource:
    """Parse Hyperliquid clearinghouse state without using the signing SDK."""

    def __init__(self, client: ClearinghouseRestClient, account_address: str, tracker: AccountTracker) -> None:
        if not account_address:
            raise ValueError("account_address must not be empty")
        self._client = client
        self._account_address = account_address
        self._tracker = tracker

    async def fetch_account_state(self) -> PolledAccountSnapshot:
        raw = await self._client.get_clearinghouse_state(self._account_address)
        margin_summary = raw.get("marginSummary")
        asset_positions = raw.get("assetPositions")
        if not isinstance(margin_summary, dict) or not isinstance(asset_positions, list):
            raise ValueError("invalid_clearinghouse_state_response")

        account_value = _as_float(margin_summary.get("accountValue"), "accountValue")
        available_raw = raw.get("withdrawable", margin_summary.get("totalMarginAvailable"))
        available = _as_float(available_raw, "withdrawable")
        margin_used_raw = margin_summary.get("totalMarginUsed", max(0.0, account_value - available))
        margin_used = _as_float(margin_used_raw, "totalMarginUsed")
        positions = tuple(self._parse_position(item) for item in asset_positions)
        unrealized = sum(float(position.unrealized_pnl or Usd(0.0)) for position in positions)
        state = AccountState(
            equity=Usd(account_value),
            available_balance=Usd(available),
            total_margin_used=Usd(margin_used),
            total_unrealized_pnl=Usd(unrealized),
            peak_equity=max(self._tracker.peak_equity, Usd(account_value)),
            sub_account=SubAccount(self._account_address.lower()),
        )
        return PolledAccountSnapshot(state, positions, datetime.now(UTC))

    def _parse_position(self, item: object) -> Position:
        if not isinstance(item, dict) or not isinstance(item.get("position"), dict):
            raise ValueError("invalid_asset_position")
        raw = item["position"]
        coin = raw.get("coin")
        if not isinstance(coin, str) or not coin:
            raise ValueError("asset_position_missing_coin")
        size = _as_float(raw.get("szi"), "szi")
        position_value = abs(_as_float(raw.get("positionValue", 0.0), "positionValue"))
        mark_price = position_value / abs(size) if size != 0 else None
        leverage_raw = raw.get("leverage", {})
        leverage_value = leverage_raw.get("value", 1) if isinstance(leverage_raw, dict) else 1
        return Position(
            symbol=Symbol(coin),
            size=Size(size),
            entry_price=_optional_price(raw.get("entryPx")),
            mark_price=Price(mark_price) if mark_price is not None else None,
            unrealized_pnl=Usd(_as_float(raw.get("unrealizedPnl", 0.0), "unrealizedPnl")),
            leverage=max(1, int(float(leverage_value))),
            liquidation_price=_optional_price(raw.get("liquidationPx")),
            sub_account=SubAccount(self._account_address.lower()),
        )


def _default_near_risk(snapshot: PolledAccountSnapshot) -> bool:
    equity = float(snapshot.account_state.equity)
    if equity <= 0:
        return True
    available_ratio = float(snapshot.account_state.available_balance) / equity
    if available_ratio <= 0.25:
        return True
    for position in snapshot.positions:
        if position.mark_price is None or position.liquidation_price is None or position.mark_price <= 0:
            continue
        distance = abs(float(position.mark_price) - float(position.liquidation_price)) / float(position.mark_price)
        if distance <= 0.10:
            return True
    return False


def _require_aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("account health timestamps must be timezone-aware")
    return value


def _as_float(value: object, field_name: str) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid numeric field: {field_name}") from exc


def _optional_price(value: object) -> Price | None:
    if value in (None, ""):
        return None
    return Price(_as_float(value, "price"))
