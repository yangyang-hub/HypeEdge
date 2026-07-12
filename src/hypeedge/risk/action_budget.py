"""Conservative action-quota controller for market-making execution.

The controller deliberately keeps three independent ledgers:

* address actions are reconciled to ``userRateLimit``;
* cancel headroom is reconciled to its own cumulative exchange limit;
* IP weight is a local one-minute sliding window.

It has no database dependency.  Durable execution attempts can be replayed
through :meth:`restore` after the last authoritative remote snapshot.
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any

import structlog

from hypeedge.config.settings import ActionBudgetSettings
from hypeedge.core.enums import ActionBudgetMode
from hypeedge.core.types import StrategyId, Symbol, Usd

logger = structlog.get_logger(__name__)

_ADDRESS_PATTERN = re.compile(r"^0x[0-9a-f]{40}$")
_STAT_WINDOWS_HOURS = (1, 6, 24)


class BudgetAction(StrEnum):
    """Exchange child action classes relevant to quota policy."""

    PLACE = "place"
    CANCEL = "cancel"
    MODIFY = "modify"
    CLOSE = "close"


@dataclass(frozen=True, slots=True)
class RemoteActionSnapshot:
    """Authoritative address quota snapshot returned by ``userRateLimit``."""

    quota_owner_address: str
    cap: int
    used: int
    observed_at: datetime

    def __post_init__(self) -> None:
        normalized = self.quota_owner_address.lower()
        if not _ADDRESS_PATTERN.fullmatch(normalized):
            raise ValueError("quota_owner_address must be a canonical 20-byte hex address")
        if self.cap < 0 or self.used < 0 or self.used > self.cap:
            raise ValueError("remote action quota must satisfy 0 <= used <= cap")
        object.__setattr__(self, "quota_owner_address", normalized)
        object.__setattr__(self, "observed_at", _as_utc(self.observed_at))

    @property
    def remaining(self) -> int:
        return self.cap - self.used

    @classmethod
    def from_user_rate_limit(
        cls,
        quota_owner_address: str,
        payload: dict[str, Any],
        *,
        observed_at: datetime,
    ) -> RemoteActionSnapshot:
        """Parse the stable Hyperliquid ``userRateLimit`` quota fields."""
        try:
            cap = int(payload["nRequestsCap"])
            used = int(payload["nRequestsUsed"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("userRateLimit response lacks valid nRequestsCap/nRequestsUsed") from exc
        return cls(quota_owner_address=quota_owner_address, cap=cap, used=used, observed_at=observed_at)


@dataclass(frozen=True, slots=True)
class CancelHeadroomSnapshot:
    """Authoritative cumulative cancel-limit projection."""

    cap: int
    used: int
    observed_at: datetime

    def __post_init__(self) -> None:
        if self.cap < 0 or self.used < 0 or self.used > self.cap:
            raise ValueError("cancel headroom must satisfy 0 <= used <= cap")
        object.__setattr__(self, "observed_at", _as_utc(self.observed_at))

    @property
    def remaining(self) -> int:
        return self.cap - self.used


@dataclass(frozen=True, slots=True)
class NetworkAttemptDebit:
    """One request that actually crossed the network boundary.

    ``attempt_id`` is the durable idempotency key.  ``child_actions`` counts
    address quota per child, while ``ip_weight`` is charged once per request.
    """

    attempt_id: str
    child_actions: tuple[BudgetAction, ...]
    ip_weight: int
    occurred_at: datetime
    strategy_id: StrategyId | None = None
    symbol: Symbol | None = None

    def __post_init__(self) -> None:
        if not self.attempt_id:
            raise ValueError("attempt_id cannot be empty")
        if self.ip_weight < 0:
            raise ValueError("ip_weight cannot be negative")
        if not self.child_actions and self.ip_weight == 0:
            raise ValueError("a network attempt must debit an address action or IP weight")
        if (self.strategy_id is None) != (self.symbol is None):
            raise ValueError("strategy_id and symbol must either both be set or both be omitted")
        object.__setattr__(self, "occurred_at", _as_utc(self.occurred_at))

    @property
    def address_cost(self) -> int:
        return len(self.child_actions)

    @property
    def cancel_cost(self) -> int:
        return sum(action == BudgetAction.CANCEL for action in self.child_actions)


@dataclass(frozen=True, slots=True)
class FillCredit:
    """Organic filled volume that earns address quota; grants are excluded."""

    volume_usdc: Usd
    occurred_at: datetime

    def __post_init__(self) -> None:
        if self.volume_usdc < 0:
            raise ValueError("fill volume cannot be negative")
        object.__setattr__(self, "occurred_at", _as_utc(self.occurred_at))


@dataclass(frozen=True, slots=True)
class BudgetAllocation:
    strategy_id: StrategyId
    symbol: Symbol
    soft_limit: int
    hard_limit: int

    def __post_init__(self) -> None:
        if self.soft_limit < 0 or self.hard_limit < self.soft_limit:
            raise ValueError("allocation must satisfy 0 <= soft_limit <= hard_limit")


@dataclass(frozen=True, slots=True)
class BudgetWindowStats:
    window_hours: int
    burned_actions: int
    earned_actions: Decimal
    fills: int
    actions_per_fill: Decimal | None
    marginal_usdc_per_action: Decimal | None
    net_burn_per_hour: Decimal
    runway_hours: float


@dataclass(frozen=True, slots=True)
class BudgetPermission:
    allowed: bool
    mode: ActionBudgetMode
    reason: str


@dataclass(frozen=True, slots=True)
class ActionBudgetView:
    quota_owner_address: str
    mode: ActionBudgetMode
    remote_cap: int
    remote_used: int
    address_remaining: int
    required_cancel_reserve: int
    close_action_reserve: int
    placement_actions_available: int
    cancel_headroom_remaining: int
    ip_weight_remaining: int
    possible_live_orders: int
    remote_fresh: bool
    cancel_headroom_fresh: bool
    restored_conservatively: bool
    windows: tuple[BudgetWindowStats, ...]


@dataclass(frozen=True, slots=True)
class ActionBudgetRecoveryState:
    """Serializable inputs needed for conservative process restart."""

    remote_snapshot: RemoteActionSnapshot | None
    cancel_snapshot: CancelHeadroomSnapshot | None
    attempts_after_snapshot: tuple[NetworkAttemptDebit, ...]
    fills: tuple[FillCredit, ...] = ()
    allocations: tuple[BudgetAllocation, ...] = ()
    possible_live_orders: int = 0


class ActionBudgetController:
    """Scope-level owner for action, cancel, and IP budgets."""

    def __init__(
        self,
        quota_owner_address: str,
        settings: ActionBudgetSettings,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        owner = quota_owner_address.lower()
        if not _ADDRESS_PATTERN.fullmatch(owner):
            raise ValueError("quota_owner_address must be a canonical 20-byte hex address")
        self._quota_owner_address = owner
        self._settings = settings
        self._clock = clock or (lambda: datetime.now(UTC))
        self._remote_snapshot: RemoteActionSnapshot | None = None
        self._cancel_snapshot: CancelHeadroomSnapshot | None = None
        self._attempts: dict[str, NetworkAttemptDebit] = {}
        self._fills: list[FillCredit] = []
        self._allocations: dict[tuple[StrategyId, Symbol], BudgetAllocation] = {}
        self._possible_live_orders = 0
        self._forced_cancel_only = True
        self._restored_conservatively = False
        self._paid_reserve_spend: list[tuple[datetime, Decimal]] = []

    @property
    def quota_owner_address(self) -> str:
        return self._quota_owner_address

    def set_allocation(self, allocation: BudgetAllocation) -> None:
        self._allocations[(allocation.strategy_id, allocation.symbol)] = allocation

    def release_allocation(self, strategy_id: StrategyId, symbol: Symbol) -> None:
        self._allocations.pop((strategy_id, symbol), None)

    def update_possible_live_orders(self, count: int) -> None:
        if count < 0:
            raise ValueError("possible live order count cannot be negative")
        self._possible_live_orders = count

    def reconcile_remote(self, snapshot: RemoteActionSnapshot) -> None:
        """Accept an authoritative address snapshot and correct shadow use."""
        if snapshot.quota_owner_address != self._quota_owner_address:
            raise ValueError("remote snapshot belongs to a different quota owner")
        previous = self._remote_snapshot
        self._remote_snapshot = snapshot
        self._forced_cancel_only = False
        logger.info(
            "action_budget_remote_reconciled",
            quota_owner=self._quota_owner_address,
            remote_cap=snapshot.cap,
            remote_used=snapshot.used,
            previous_used=previous.used if previous else None,
            shadow_after_snapshot=self._shadow_address_debit(snapshot.observed_at),
        )

    def reconcile_cancel_headroom(self, snapshot: CancelHeadroomSnapshot) -> None:
        self._cancel_snapshot = snapshot

    def debit_network_attempt(self, debit: NetworkAttemptDebit) -> bool:
        """Shadow-debit one actual request, idempotently by durable attempt id.

        The outcome is intentionally irrelevant: reject and timeout still burn
        their conservative debit and are never charged a second time.
        """
        existing = self._attempts.get(debit.attempt_id)
        if existing is not None:
            if existing != debit:
                raise ValueError("attempt_id was reused with different budget facts")
            return False
        self._attempts[debit.attempt_id] = debit
        return True

    def record_fill(self, volume_usdc: Usd, *, occurred_at: datetime | None = None) -> None:
        self._fills.append(FillCredit(volume_usdc=volume_usdc, occurred_at=occurred_at or self._now()))

    def restore(self, state: ActionBudgetRecoveryState) -> bool:
        """Rebuild shadow state from a remote snapshot plus later durable facts.

        Any missing snapshot, duplicate conflict, pre-snapshot attempt, or
        owner mismatch leaves the controller in ``CANCEL_ONLY``.
        """
        self._remote_snapshot = None
        self._cancel_snapshot = None
        self._attempts.clear()
        self._fills.clear()
        self._allocations.clear()
        self._possible_live_orders = max(0, state.possible_live_orders)
        self._forced_cancel_only = True
        self._restored_conservatively = True
        if state.remote_snapshot is None or state.cancel_snapshot is None:
            return False
        try:
            if state.remote_snapshot.quota_owner_address != self._quota_owner_address:
                raise ValueError("recovery snapshot belongs to another quota owner")
            for attempt in state.attempts_after_snapshot:
                if attempt.occurred_at < state.remote_snapshot.observed_at:
                    raise ValueError("recovery attempt predates the remote snapshot")
                self.debit_network_attempt(attempt)
            for fill in state.fills:
                self._fills.append(fill)
            for allocation in state.allocations:
                self.set_allocation(allocation)
        except ValueError:
            logger.exception("action_budget_recovery_unexplained", quota_owner=self._quota_owner_address)
            self._attempts.clear()
            return False
        self._remote_snapshot = state.remote_snapshot
        self._cancel_snapshot = state.cancel_snapshot
        self._forced_cancel_only = False
        return True

    def export_recovery_state(self) -> ActionBudgetRecoveryState:
        remote_at = self._remote_snapshot.observed_at if self._remote_snapshot else datetime.min.replace(tzinfo=UTC)
        attempts = tuple(attempt for attempt in self._attempts.values() if attempt.occurred_at >= remote_at)
        return ActionBudgetRecoveryState(
            remote_snapshot=self._remote_snapshot,
            cancel_snapshot=self._cancel_snapshot,
            attempts_after_snapshot=attempts,
            fills=tuple(self._fills),
            allocations=tuple(self._allocations.values()),
            possible_live_orders=self._possible_live_orders,
        )

    def permission(
        self,
        action: BudgetAction,
        *,
        strategy_id: StrategyId | None = None,
        symbol: Symbol | None = None,
        child_actions: int = 1,
        ip_weight: int = 1,
        risk_reducing: bool = False,
        emergency: bool = False,
    ) -> BudgetPermission:
        """Return dispatch permission without mutating quota state.

        Cancels intentionally bypass every placement budget gate.  They still
        appear in telemetry and are shadow-debited once sent.
        """
        if child_actions <= 0 or ip_weight < 0:
            raise ValueError("child_actions must be positive and ip_weight non-negative")
        current_mode = self.mode
        if action == BudgetAction.CANCEL:
            return BudgetPermission(True, current_mode, "cancel bypasses placement budget gates")

        view = self.snapshot()
        if action == BudgetAction.CLOSE and emergency:
            allowed = view.address_remaining >= child_actions and view.ip_weight_remaining >= ip_weight
            return BudgetPermission(allowed, current_mode, "emergency close reserve" if allowed else "quota exhausted")

        if current_mode in (ActionBudgetMode.CANCEL_ONLY, ActionBudgetMode.EXHAUSTED):
            return BudgetPermission(False, current_mode, "budget mode forbids placement")
        if current_mode == ActionBudgetMode.CRITICAL and not risk_reducing:
            return BudgetPermission(False, current_mode, "critical mode permits only risk reduction")
        if view.placement_actions_available < child_actions:
            return BudgetPermission(False, current_mode, "address action reserve would be consumed")
        if view.ip_weight_remaining - ip_weight < self._settings.ip_emergency_reserve:
            return BudgetPermission(False, current_mode, "IP emergency reserve would be consumed")
        if strategy_id is not None or symbol is not None:
            if strategy_id is None or symbol is None:
                raise ValueError("strategy_id and symbol must be supplied together")
            allocation = self._allocations.get((strategy_id, symbol))
            if allocation is None:
                return BudgetPermission(False, current_mode, "no active strategy/symbol allocation")
            consumed = self._allocation_consumed(strategy_id, symbol)
            if consumed + child_actions > allocation.hard_limit:
                return BudgetPermission(False, current_mode, "strategy/symbol hard allocation exhausted")
        return BudgetPermission(True, current_mode, "budget available")

    def ip_request_permission(self, ip_weight: int, *, emergency: bool = False) -> BudgetPermission:
        """Gate info requests without mixing them into address-action quota.

        Recovery and quota-refresh requests may consume the emergency reserve,
        but ordinary polling/backfill must leave it untouched.
        """
        if ip_weight <= 0:
            raise ValueError("ip_weight must be positive")
        current_mode = self.mode
        remaining = self._ip_remaining(self._now())
        required_after = 0 if emergency else self._settings.ip_emergency_reserve
        allowed = remaining >= ip_weight and remaining - ip_weight >= required_after
        reason = "IP weight available" if allowed else "IP emergency reserve would be consumed"
        return BudgetPermission(allowed, current_mode, reason)

    @property
    def mode(self) -> ActionBudgetMode:
        return self._calculate_mode(self._now())

    @property
    def next_remote_poll_interval_seconds(self) -> float:
        mode = self.mode
        if mode in (ActionBudgetMode.CRITICAL, ActionBudgetMode.CANCEL_ONLY, ActionBudgetMode.EXHAUSTED):
            return self._settings.remote_poll_interval_critical_seconds
        if mode == ActionBudgetMode.CONSERVE:
            return self._settings.remote_poll_interval_conserve_seconds
        return self._settings.remote_poll_interval_normal_seconds

    def snapshot(self, *, now: datetime | None = None) -> ActionBudgetView:
        current = _as_utc(now) if now is not None else self._now()
        remote = self._remote_snapshot
        cancel = self._cancel_snapshot
        address_remaining = self._address_remaining()
        required_cancel = self.required_cancel_reserve
        placement_available = max(
            0,
            address_remaining - required_cancel - self._settings.close_action_reserve,
        )
        windows = tuple(self._window_stats(hours, current) for hours in _STAT_WINDOWS_HOURS)
        return ActionBudgetView(
            quota_owner_address=self._quota_owner_address,
            mode=self._calculate_mode(current),
            remote_cap=remote.cap if remote else 0,
            remote_used=remote.used if remote else 0,
            address_remaining=address_remaining,
            required_cancel_reserve=required_cancel,
            close_action_reserve=self._settings.close_action_reserve,
            placement_actions_available=placement_available,
            cancel_headroom_remaining=self._cancel_remaining(),
            ip_weight_remaining=self._ip_remaining(current),
            possible_live_orders=self._possible_live_orders,
            remote_fresh=self._is_fresh(remote.observed_at, current) if remote else False,
            cancel_headroom_fresh=self._is_fresh(cancel.observed_at, current) if cancel else False,
            restored_conservatively=self._restored_conservatively,
            windows=windows,
        )

    @property
    def required_cancel_reserve(self) -> int:
        return self._possible_live_orders + self._settings.cancel_retry_buffer

    def paid_reserve_permission(
        self,
        requests: int,
        *,
        purpose: str,
        admin_authorized: bool,
        now: datetime | None = None,
    ) -> BudgetPermission:
        current = _as_utc(now) if now else self._now()
        mode = self._calculate_mode(current)
        if purpose not in {"close", "unknown_recovery"}:
            return BudgetPermission(False, mode, "paid reserve is limited to close and UNKNOWN recovery")
        if not self._settings.paid_reserve_enabled or not admin_authorized:
            return BudgetPermission(False, mode, "paid reserve is disabled or lacks admin authorization")
        if requests <= 0:
            raise ValueError("paid reserve requests must be positive")
        cost = Decimal(str(self._settings.paid_reserve_cost_per_request_usdc)) * requests
        single = Decimal(str(self._settings.paid_reserve_max_single_usdc))
        daily = Decimal(str(self._settings.paid_reserve_max_daily_usdc))
        monthly = Decimal(str(self._settings.paid_reserve_max_monthly_usdc))
        day_spend = sum(
            amount for occurred, amount in self._paid_reserve_spend if occurred >= current - timedelta(days=1)
        )
        month_spend = sum(
            amount for occurred, amount in self._paid_reserve_spend if occurred >= current - timedelta(days=30)
        )
        allowed = cost <= single and day_spend + cost <= daily and month_spend + cost <= monthly
        reason = "paid reserve within limits" if allowed else "paid reserve cost cap exceeded"
        return BudgetPermission(allowed, mode, reason)

    def record_paid_reserve(
        self,
        requests: int,
        *,
        purpose: str,
        admin_authorized: bool,
        now: datetime | None = None,
    ) -> Usd:
        current = _as_utc(now) if now else self._now()
        permission = self.paid_reserve_permission(
            requests,
            purpose=purpose,
            admin_authorized=admin_authorized,
            now=current,
        )
        if not permission.allowed:
            raise PermissionError(permission.reason)
        cost = Decimal(str(self._settings.paid_reserve_cost_per_request_usdc)) * requests
        self._paid_reserve_spend.append((current, cost))
        logger.warning(
            "paid_action_reserve_used",
            quota_owner=self._quota_owner_address,
            purpose=purpose,
            requests=requests,
            cost_usdc=str(cost),
        )
        return Usd(cost)

    def _calculate_mode(self, now: datetime) -> ActionBudgetMode:
        if self._forced_cancel_only or self._remote_snapshot is None or self._cancel_snapshot is None:
            return ActionBudgetMode.CANCEL_ONLY
        if not self._is_fresh(self._remote_snapshot.observed_at, now) or not self._is_fresh(
            self._cancel_snapshot.observed_at, now
        ):
            return ActionBudgetMode.CANCEL_ONLY
        address = self._address_remaining()
        cancel = self._cancel_remaining()
        ip = self._ip_remaining(now)
        if address <= 0 or cancel <= 0 or ip <= 0:
            return ActionBudgetMode.EXHAUSTED
        if (
            address <= self.required_cancel_reserve + self._settings.close_action_reserve
            or cancel <= self.required_cancel_reserve
            or ip <= self._settings.ip_emergency_reserve
        ):
            return ActionBudgetMode.CANCEL_ONLY

        placement = address - self.required_cancel_reserve - self._settings.close_action_reserve
        stats = self._window_stats(1, now)
        runway = stats.runway_hours
        if (
            placement <= self._settings.address_cancel_only_threshold
            or runway <= self._settings.runway_cancel_only_hours
        ):
            return ActionBudgetMode.CANCEL_ONLY
        if placement <= self._settings.address_critical_threshold or runway <= self._settings.runway_critical_hours:
            return ActionBudgetMode.CRITICAL
        if placement <= self._settings.address_conserve_threshold or runway <= self._settings.runway_conserve_hours:
            return ActionBudgetMode.CONSERVE
        if (
            stats.burned_actions >= self._settings.minimum_actions_for_economic_gate
            and stats.marginal_usdc_per_action is not None
            and stats.marginal_usdc_per_action < Decimal(str(self._settings.minimum_marginal_usdc_per_action))
        ):
            return ActionBudgetMode.CONSERVE
        return ActionBudgetMode.NORMAL

    def _window_stats(self, hours: int, now: datetime) -> BudgetWindowStats:
        cutoff = now - timedelta(hours=hours)
        attempts = [attempt for attempt in self._attempts.values() if cutoff < attempt.occurred_at <= now]
        fills = [fill for fill in self._fills if cutoff < fill.occurred_at <= now]
        burned = sum(attempt.address_cost for attempt in attempts)
        earned = sum((Decimal(fill.volume_usdc) for fill in fills), start=Decimal(0))
        net_burn = max(Decimal(0), Decimal(burned) - earned) / Decimal(hours)
        actions_per_fill = Decimal(burned) / len(fills) if fills else None
        marginal = earned / burned if burned else None
        runway = math.inf if net_burn <= 0 else float(Decimal(self._placement_remaining_raw()) / net_burn)
        return BudgetWindowStats(
            window_hours=hours,
            burned_actions=burned,
            earned_actions=earned,
            fills=len(fills),
            actions_per_fill=actions_per_fill,
            marginal_usdc_per_action=marginal,
            net_burn_per_hour=net_burn,
            runway_hours=runway,
        )

    def _address_remaining(self) -> int:
        if self._remote_snapshot is None:
            return 0
        return max(0, self._remote_snapshot.remaining - self._shadow_address_debit(self._remote_snapshot.observed_at))

    def _cancel_remaining(self) -> int:
        if self._cancel_snapshot is None:
            return 0
        shadow = sum(
            attempt.cancel_cost
            for attempt in self._attempts.values()
            if attempt.occurred_at > self._cancel_snapshot.observed_at
        )
        return max(0, self._cancel_snapshot.remaining - shadow)

    def _ip_remaining(self, now: datetime) -> int:
        cutoff = now - timedelta(minutes=1)
        used = sum(attempt.ip_weight for attempt in self._attempts.values() if cutoff < attempt.occurred_at <= now)
        return max(0, self._settings.ip_weight_limit_per_minute - used)

    def _shadow_address_debit(self, observed_at: datetime) -> int:
        return sum(attempt.address_cost for attempt in self._attempts.values() if attempt.occurred_at > observed_at)

    def _placement_remaining_raw(self) -> int:
        return max(0, self._address_remaining() - self.required_cancel_reserve - self._settings.close_action_reserve)

    def _allocation_consumed(self, strategy_id: StrategyId, symbol: Symbol) -> int:
        return sum(
            attempt.address_cost
            for attempt in self._attempts.values()
            if attempt.strategy_id == strategy_id and attempt.symbol == symbol
        )

    def _is_fresh(self, observed_at: datetime, now: datetime) -> bool:
        age = (now - observed_at).total_seconds()
        return 0 <= age <= self._settings.remote_snapshot_max_age_seconds

    def _now(self) -> datetime:
        return _as_utc(self._clock())


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("budget timestamps must be timezone-aware")
    return value.astimezone(UTC)
