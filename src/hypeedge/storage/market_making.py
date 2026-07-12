"""Postgres repositories and authoritative read models for market making.

The repositories deliberately keep lifecycle/configuration facts in Postgres
and never synthesize missing position, quote, or budget state.  High-frequency
research projections remain ClickHouse concerns.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, NoReturn

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hypeedge.core.enums import MarketMakerLifecycle, OrderStatus, QuoteAction, Side
from hypeedge.core.exceptions import StrategyLifecycleError, StrategyRegistrationError
from hypeedge.core.types import Cloid, OrderId, Price, Size, StrategyId, SubAccount, Symbol
from hypeedge.execution.cloid import CloidGenerator
from hypeedge.storage.postgres import (
    AccountStateRecord,
    ActionBudgetAllocationRecord,
    ActionBudgetEventRecord,
    ActionBudgetScopeRecord,
    ExecutionActionRecord,
    ExecutionCommandItemRecord,
    ExecutionCommandRecord,
    MarketMakerConfigVersionRecord,
    MarketMakingSessionRecord,
    OrderRecord,
    PositionRecord,
    QuotePlanItemRecord,
    QuotePlanRecord,
    QuoteSlotRecord,
    RiskReservationRecord,
    StrategyAllocationRecord,
    StrategyConfigVersionRecord,
    StrategyInstanceRecord,
    StrategyRuntimeStateRecord,
    StrategyStateEventRecord,
)
from hypeedge.strategy.registry import StrategyConfigSnapshot, StrategyInstanceDefinition
from hypeedge.strategy.supervisor import StrategyAllocation, StrategyRuntimeState
from hypeedge.trading.quotes import QuotePlan, QuoteRiskOwner, QuoteSlotKey
from hypeedge.trading.quotes import QuoteSlotView as DomainQuoteSlotView

_MM_DECIMAL_FIELDS = (
    "soft_inventory_notional",
    "hard_inventory_notional",
    "emergency_inventory_notional",
    "quote_size",
    "max_depth_participation",
    "inventory_skew_bps",
    "max_inventory_shift_bps",
    "min_half_spread_bps",
    "toxicity_spread_bps",
    "min_expected_pnl_usdc",
    "external_reference_weight",
    "external_max_age_seconds",
    "external_outlier_bps",
    "max_external_shift_ticks",
    "max_total_fair_shift_ticks",
    "latency_risk_multiplier",
    "conservative_latency_seconds",
    "conservative_markout_bps",
)
_MM_INTEGER_FIELDS = (
    "min_quote_lifetime_ms",
    "refresh_cooldown_ms",
    "max_quote_age_ms",
    "market_stale_after_ms",
    "account_stale_after_ms",
    "min_markout_samples",
)
_MM_CONFIG_FIELDS = frozenset((*_MM_DECIMAL_FIELDS, *_MM_INTEGER_FIELDS))
_MM_NEW_FIELD_DEFAULTS: dict[str, Decimal | int] = {
    "external_reference_weight": Decimal("0.25"),
    "external_max_age_seconds": Decimal("0.5"),
    "external_outlier_bps": Decimal("75"),
    "max_external_shift_ticks": Decimal("2"),
    "max_total_fair_shift_ticks": Decimal("3"),
    "latency_risk_multiplier": Decimal("1"),
    "conservative_latency_seconds": Decimal("0.1"),
    "conservative_markout_bps": Decimal("1"),
    "min_markout_samples": 20,
}


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _uuid_or_none(value: object | None) -> uuid.UUID | None:
    if value is None:
        return None
    try:
        return uuid.UUID(str(value))
    except ValueError as exc:
        raise StrategyLifecycleError(f"Quote source order ID is not a UUID: {value}") from exc


def _quote_plan_command_payload(plan: QuotePlan, plan_id: uuid.UUID) -> dict[str, Any]:
    return {
        "plan_id": str(plan_id),
        "strategy_id": str(plan.strategy_id),
        "symbol": str(plan.symbol),
        "runtime_session_id": plan.session_id,
        "config_version": plan.config_version,
        "revision": plan.revision,
        "market_version": plan.market_version,
        "connection_generation": plan.connection_generation,
        "valid_until": plan.valid_until.isoformat(),
        "fair_price": str(plan.fair_price),
        "reservation_price": str(plan.reservation_price),
        "inventory_notional": str(plan.inventory_notional),
        "budget_mode": plan.budget_mode.value,
        "children": [
            {
                "side": diff.slot.side.value,
                "level": diff.slot.level,
                "actions": list(diff.child_actions),
                "source_cloid": str(diff.source.cloid) if diff.source is not None else None,
                "desired_price": str(diff.desired.price) if diff.desired.price is not None else None,
                "desired_size": str(diff.desired.size) if diff.desired.size is not None else None,
            }
            for diff in plan.diffs
        ],
    }


def _decimal_text(value: object) -> str:
    if isinstance(value, bool):
        raise TypeError("boolean is not a decimal configuration value")
    decimal = value if isinstance(value, Decimal) else Decimal(str(value))
    if not decimal.is_finite():
        raise ValueError("configuration decimals must be finite")
    if decimal == 0:
        return "0"
    return format(decimal.normalize(), "f")


def normalize_market_maker_config(values: Mapping[str, Any]) -> dict[str, Decimal | int]:
    """Validate and normalize the complete typed Postgres config contract."""

    supplied = dict(values)
    keys = frozenset(supplied)
    required_fields = _MM_CONFIG_FIELDS - _MM_NEW_FIELD_DEFAULTS.keys()
    if not required_fields <= keys or not keys <= _MM_CONFIG_FIELDS:
        missing = sorted(required_fields - keys)
        extra = sorted(keys - _MM_CONFIG_FIELDS)
        raise StrategyRegistrationError(f"Invalid market-maker config fields: missing={missing} extra={extra}")
    for name, default in _MM_NEW_FIELD_DEFAULTS.items():
        supplied.setdefault(name, default)
    normalized: dict[str, Decimal | int] = {name: Decimal(_decimal_text(supplied[name])) for name in _MM_DECIMAL_FIELDS}
    for name in _MM_INTEGER_FIELDS:
        value = supplied[name]
        if isinstance(value, bool) or int(value) != value:
            raise StrategyRegistrationError(f"Market-maker config field must be an integer: {name}")
        normalized[name] = int(value)
    return normalized


def market_maker_config_hash(values: Mapping[str, Any]) -> str:
    """Return a stable semantic hash; Decimal scale and key order do not affect it."""

    normalized = normalize_market_maker_config(values)
    canonical = {
        key: _decimal_text(value) if key in _MM_DECIMAL_FIELDS else value for key, value in sorted(normalized.items())
    }
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class StrategyInstanceView:
    definition: StrategyInstanceDefinition
    metadata: Mapping[str, Any]
    archived_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class AuthoritativeRead[T]:
    data: T | None
    as_of: datetime | None
    stale: bool
    reason: str | None = None
    source: str = "postgres"


@dataclass(frozen=True, slots=True)
class MarketMakingStateView:
    instance: StrategyInstanceView
    runtime: StrategyRuntimeState
    runtime_heartbeat_at: datetime | None


@dataclass(frozen=True, slots=True)
class QuoteSlotView:
    symbol: Symbol
    side: str
    level: int
    state: str
    plan_revision: int
    slot_revision: int
    owner_order_id: uuid.UUID | None
    owner_plan_id: uuid.UUID | None
    price: Decimal | None
    size: Decimal | None
    filled_size: Decimal | None
    order_status: str | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class InventoryView:
    symbol: Symbol
    size: Decimal
    entry_price: Decimal | None
    mark_price: Decimal | None
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    liquidation_price: Decimal | None
    equity: Decimal | None
    available_balance: Decimal | None
    margin_used: Decimal | None
    position_revision: int
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class ActionBudgetView:
    quota_owner_address: str
    mode: str
    remote_cap: int
    remote_used: int
    remote_remaining: int
    shadow_used: int
    emergency_reserve: int
    soft_allocation: int
    hard_allocation: int
    scope_revision: int
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class MarketMakingEventView:
    event_id: int
    event_type: str
    from_state: str | None
    to_state: str
    reason: str | None
    actor: str
    desired_config_version_id: int | None
    effective_config_version_id: int | None
    occurred_at: datetime


def _definition(record: StrategyInstanceRecord, desired_version: int | None) -> StrategyInstanceDefinition:
    if record.sub_account is None:
        raise StrategyRegistrationError(f"Strategy instance has no sub-account: {record.strategy_id}")
    if desired_version is None:
        raise StrategyRegistrationError(f"Strategy instance has no desired config: {record.strategy_id}")
    return StrategyInstanceDefinition(
        strategy_id=StrategyId(record.strategy_id),
        strategy_type=record.strategy_type,
        sub_account=SubAccount(record.sub_account),
        symbol=Symbol(record.symbol),
        desired_state=MarketMakerLifecycle(record.desired_state),
        desired_config_revision=desired_version,
        revision=record.revision,
    )


def _instance_view(record: StrategyInstanceRecord, desired_version: int | None) -> StrategyInstanceView:
    return StrategyInstanceView(
        definition=_definition(record, desired_version),
        metadata=dict(record.metadata_),
        archived_at=record.archived_at,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


class PostgresStrategyStateStore:
    """Durable implementation of ``StrategyStateStore`` plus API commands."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    @staticmethod
    def _instance_statement() -> Any:
        return select(StrategyInstanceRecord, StrategyConfigVersionRecord.version).outerjoin(
            StrategyConfigVersionRecord,
            StrategyConfigVersionRecord.id == StrategyInstanceRecord.desired_config_version_id,
        )

    async def list_strategy_instances(self, *, include_archived: bool = False) -> list[StrategyInstanceView]:
        statement = self._instance_statement()
        if not include_archived:
            statement = statement.where(StrategyInstanceRecord.archived_at.is_(None))
        statement = statement.order_by(StrategyInstanceRecord.created_at, StrategyInstanceRecord.strategy_id)
        async with self._session_factory() as session:
            rows = (await session.execute(statement)).all()
        return [_instance_view(record, version) for record, version in rows]

    async def list_instances(self) -> list[StrategyInstanceDefinition]:
        return [view.definition for view in await self.list_strategy_instances()]

    async def get_strategy_instance(self, strategy_id: StrategyId) -> StrategyInstanceView:
        statement = self._instance_statement().where(StrategyInstanceRecord.strategy_id == str(strategy_id))
        async with self._session_factory() as session:
            row = (await session.execute(statement)).one_or_none()
        if row is None:
            raise StrategyRegistrationError(f"Unknown strategy instance: {strategy_id}")
        return _instance_view(row[0], row[1])

    async def get_instance(self, strategy_id: StrategyId) -> StrategyInstanceDefinition:
        return (await self.get_strategy_instance(strategy_id)).definition

    async def create_strategy_instance(
        self,
        instance: StrategyInstanceDefinition,
        initial_config: Mapping[str, Any],
        *,
        created_by: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> StrategyInstanceView:
        if instance.strategy_type != "market_maker":
            raise StrategyRegistrationError("Postgres typed creation currently requires strategy_type=market_maker")
        if instance.desired_config_revision != 1:
            raise StrategyRegistrationError("Initial config revision must be 1")
        normalized = normalize_market_maker_config(initial_config)
        config_hash = market_maker_config_hash(normalized)
        async with self._session_factory() as session, session.begin():
            record = StrategyInstanceRecord(
                strategy_id=str(instance.strategy_id),
                strategy_type=instance.strategy_type,
                sub_account=str(instance.sub_account),
                symbol=str(instance.symbol),
                desired_state=instance.desired_state.value,
                revision=instance.revision,
                metadata_=dict(metadata or {}),
            )
            session.add(record)
            await session.flush()
            config_record = StrategyConfigVersionRecord(
                strategy_id=str(instance.strategy_id), version=1, config_hash=config_hash, created_by=created_by
            )
            session.add(config_record)
            await session.flush()
            session.add(MarketMakerConfigVersionRecord(config_version_id=config_record.id, **normalized))
            record.desired_config_version_id = config_record.id
            session.add(StrategyRuntimeStateRecord(strategy_id=str(instance.strategy_id)))
            await session.flush()
            view = _instance_view(record, 1)
        return view

    async def update_strategy_metadata(
        self,
        strategy_id: StrategyId,
        metadata: Mapping[str, Any],
        *,
        expected_revision: int,
    ) -> StrategyInstanceView:
        async with self._session_factory() as session, session.begin():
            result = await session.execute(
                update(StrategyInstanceRecord)
                .where(
                    StrategyInstanceRecord.strategy_id == str(strategy_id),
                    StrategyInstanceRecord.revision == expected_revision,
                    StrategyInstanceRecord.archived_at.is_(None),
                )
                .values(metadata_=dict(metadata), revision=StrategyInstanceRecord.revision + 1, updated_at=_utcnow())
                .returning(StrategyInstanceRecord)
            )
            record = result.scalar_one_or_none()
            if record is None:
                await self._raise_instance_conflict(session, strategy_id, expected_revision)
            desired_version = await self._config_version_for_id(session, record.desired_config_version_id)
            return _instance_view(record, desired_version)

    async def archive_strategy_instance(
        self,
        strategy_id: StrategyId,
        *,
        expected_revision: int,
    ) -> StrategyInstanceView:
        now = _utcnow()
        async with self._session_factory() as session, session.begin():
            runtime = await session.get(StrategyRuntimeStateRecord, str(strategy_id))
            if runtime is None:
                raise StrategyRegistrationError(f"Unknown strategy instance: {strategy_id}")
            if runtime.actual_state != MarketMakerLifecycle.STOPPED.value:
                raise StrategyLifecycleError("Only a stopped strategy can be archived")
            result = await session.execute(
                update(StrategyInstanceRecord)
                .where(
                    StrategyInstanceRecord.strategy_id == str(strategy_id),
                    StrategyInstanceRecord.revision == expected_revision,
                    StrategyInstanceRecord.archived_at.is_(None),
                )
                .values(
                    archived_at=now,
                    desired_state=MarketMakerLifecycle.STOPPED.value,
                    revision=StrategyInstanceRecord.revision + 1,
                    updated_at=now,
                )
                .returning(StrategyInstanceRecord)
            )
            record = result.scalar_one_or_none()
            if record is None:
                await self._raise_instance_conflict(session, strategy_id, expected_revision)
            await session.execute(
                delete(StrategyAllocationRecord).where(StrategyAllocationRecord.strategy_id == str(strategy_id))
            )
            desired_version = await self._config_version_for_id(session, record.desired_config_version_id)
            return _instance_view(record, desired_version)

    async def list_config_versions(self, strategy_id: StrategyId) -> list[StrategyConfigSnapshot]:
        statement = (
            select(StrategyConfigVersionRecord, MarketMakerConfigVersionRecord)
            .join(
                MarketMakerConfigVersionRecord,
                MarketMakerConfigVersionRecord.config_version_id == StrategyConfigVersionRecord.id,
            )
            .where(StrategyConfigVersionRecord.strategy_id == str(strategy_id))
            .order_by(StrategyConfigVersionRecord.version)
        )
        async with self._session_factory() as session:
            rows = (await session.execute(statement)).all()
        return [self._config_snapshot(meta, typed) for meta, typed in rows]

    async def create_market_maker_config_version(
        self,
        strategy_id: StrategyId,
        values: Mapping[str, Any],
        *,
        created_by: str,
        expected_revision: int | None = None,
    ) -> StrategyConfigSnapshot:
        normalized = normalize_market_maker_config(values)
        config_hash = market_maker_config_hash(normalized)
        async with self._session_factory() as session, session.begin():
            instance = (
                await session.execute(
                    select(StrategyInstanceRecord)
                    .where(StrategyInstanceRecord.strategy_id == str(strategy_id))
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if instance is None or instance.archived_at is not None:
                raise StrategyRegistrationError(f"Unknown active strategy instance: {strategy_id}")
            if expected_revision is not None and instance.revision != expected_revision:
                raise StrategyLifecycleError(
                    f"Strategy revision conflict: expected={expected_revision} actual={instance.revision}"
                )
            existing = (
                await session.execute(
                    select(StrategyConfigVersionRecord, MarketMakerConfigVersionRecord)
                    .join(
                        MarketMakerConfigVersionRecord,
                        MarketMakerConfigVersionRecord.config_version_id == StrategyConfigVersionRecord.id,
                    )
                    .where(
                        StrategyConfigVersionRecord.strategy_id == str(strategy_id),
                        StrategyConfigVersionRecord.config_hash == config_hash,
                    )
                )
            ).one_or_none()
            if existing is not None:
                return self._config_snapshot(existing[0], existing[1])
            latest = await session.scalar(
                select(func.max(StrategyConfigVersionRecord.version)).where(
                    StrategyConfigVersionRecord.strategy_id == str(strategy_id)
                )
            )
            meta = StrategyConfigVersionRecord(
                strategy_id=str(strategy_id),
                version=int(latest or 0) + 1,
                config_hash=config_hash,
                created_by=created_by,
            )
            session.add(meta)
            await session.flush()
            typed = MarketMakerConfigVersionRecord(config_version_id=meta.id, **normalized)
            session.add(typed)
            instance.revision += 1
            instance.updated_at = _utcnow()
            await session.flush()
            return self._config_snapshot(meta, typed)

    async def get_config(self, strategy_id: StrategyId, revision: int) -> StrategyConfigSnapshot:
        statement = (
            select(StrategyConfigVersionRecord, MarketMakerConfigVersionRecord)
            .join(
                MarketMakerConfigVersionRecord,
                MarketMakerConfigVersionRecord.config_version_id == StrategyConfigVersionRecord.id,
            )
            .where(
                StrategyConfigVersionRecord.strategy_id == str(strategy_id),
                StrategyConfigVersionRecord.version == revision,
            )
        )
        async with self._session_factory() as session:
            row = (await session.execute(statement)).one_or_none()
        if row is None:
            raise StrategyRegistrationError(f"Unknown config revision: strategy_id={strategy_id} revision={revision}")
        return self._config_snapshot(row[0], row[1])

    async def get_runtime(self, strategy_id: StrategyId) -> StrategyRuntimeState:
        statement = (
            select(StrategyRuntimeStateRecord, StrategyConfigVersionRecord.version)
            .outerjoin(
                StrategyConfigVersionRecord,
                StrategyConfigVersionRecord.id == StrategyRuntimeStateRecord.effective_config_version_id,
            )
            .where(StrategyRuntimeStateRecord.strategy_id == str(strategy_id))
        )
        async with self._session_factory() as session:
            row = (await session.execute(statement)).one_or_none()
        if row is None:
            raise StrategyRegistrationError(f"Unknown strategy instance: {strategy_id}")
        return StrategyRuntimeState(
            strategy_id=strategy_id,
            actual_state=MarketMakerLifecycle(row[0].actual_state),
            effective_config_revision=row[1],
            revision=row[0].revision,
            reason=row[0].reason,
        )

    async def set_desired(
        self,
        strategy_id: StrategyId,
        *,
        state: MarketMakerLifecycle | None = None,
        config_revision: int | None = None,
        expected_revision: int | None = None,
    ) -> StrategyInstanceDefinition:
        async with self._session_factory() as session, session.begin():
            current_row = (
                await session.execute(
                    self._instance_statement()
                    .where(StrategyInstanceRecord.strategy_id == str(strategy_id))
                    .with_for_update()
                )
            ).one_or_none()
            if current_row is None or current_row[0].archived_at is not None:
                raise StrategyRegistrationError(f"Unknown active strategy instance: {strategy_id}")
            record, current_config = current_row
            if expected_revision is not None and record.revision != expected_revision:
                raise StrategyLifecycleError(
                    f"Strategy revision conflict: expected={expected_revision} actual={record.revision}"
                )
            target_state = state.value if state is not None else record.desired_state
            target_config = config_revision if config_revision is not None else current_config
            config_id = await self._config_id_for_version(session, strategy_id, target_config)
            if target_state == record.desired_state and target_config == current_config:
                return _definition(record, current_config)
            old_state = record.desired_state
            record.desired_state = target_state
            record.desired_config_version_id = config_id
            record.revision += 1
            record.updated_at = _utcnow()
            session.add(
                StrategyStateEventRecord(
                    strategy_id=str(strategy_id),
                    from_state=old_state,
                    to_state=target_state,
                    desired_config_version_id=config_id,
                    reason="desired_state_or_config_changed",
                    actor="strategy_supervisor",
                )
            )
            await session.flush()
            return _definition(record, target_config)

    async def set_runtime(
        self,
        strategy_id: StrategyId,
        *,
        actual_state: MarketMakerLifecycle | None = None,
        effective_config_revision: int | None = None,
        set_effective_config: bool = False,
        reason: str | None = None,
        expected_revision: int | None = None,
    ) -> StrategyRuntimeState:
        async with self._session_factory() as session, session.begin():
            runtime = (
                await session.execute(
                    select(StrategyRuntimeStateRecord)
                    .where(StrategyRuntimeStateRecord.strategy_id == str(strategy_id))
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if runtime is None:
                raise StrategyRegistrationError(f"Unknown strategy instance: {strategy_id}")
            if expected_revision is not None and runtime.revision != expected_revision:
                raise StrategyLifecycleError(
                    f"Runtime revision conflict: expected={expected_revision} actual={runtime.revision}"
                )
            current_effective = await self._config_version_for_id(session, runtime.effective_config_version_id)
            target_effective = effective_config_revision if set_effective_config else current_effective
            effective_id = (
                await self._config_id_for_version(session, strategy_id, target_effective)
                if target_effective is not None
                else None
            )
            target_state = actual_state.value if actual_state is not None else runtime.actual_state
            if (
                target_state == runtime.actual_state
                and target_effective == current_effective
                and reason == runtime.reason
            ):
                return StrategyRuntimeState(
                    strategy_id,
                    MarketMakerLifecycle(target_state),
                    target_effective,
                    runtime.revision,
                    reason,
                )
            old_state = runtime.actual_state
            runtime.actual_state = target_state
            runtime.effective_config_version_id = effective_id
            runtime.reason = reason
            runtime.heartbeat_at = _utcnow()
            runtime.revision += 1
            session.add(
                StrategyStateEventRecord(
                    strategy_id=str(strategy_id),
                    from_state=old_state,
                    to_state=target_state,
                    effective_config_version_id=effective_id,
                    reason=reason,
                    actor="strategy_supervisor",
                )
            )
            await session.flush()
            return StrategyRuntimeState(
                strategy_id,
                MarketMakerLifecycle(target_state),
                target_effective,
                runtime.revision,
                reason,
            )

    async def _raise_instance_conflict(
        self, session: AsyncSession, strategy_id: StrategyId, expected_revision: int
    ) -> NoReturn:
        actual = await session.scalar(
            select(StrategyInstanceRecord.revision).where(StrategyInstanceRecord.strategy_id == str(strategy_id))
        )
        if actual is None:
            raise StrategyRegistrationError(f"Unknown strategy instance: {strategy_id}")
        raise StrategyLifecycleError(f"Strategy revision conflict: expected={expected_revision} actual={actual}")

    @staticmethod
    async def _config_version_for_id(session: AsyncSession, config_id: int | None) -> int | None:
        if config_id is None:
            return None
        version = await session.scalar(
            select(StrategyConfigVersionRecord.version).where(StrategyConfigVersionRecord.id == config_id)
        )
        return int(version) if version is not None else None

    @staticmethod
    async def _config_id_for_version(session: AsyncSession, strategy_id: StrategyId, revision: int | None) -> int:
        if revision is None:
            raise StrategyRegistrationError(f"Strategy has no desired config: {strategy_id}")
        config_id = await session.scalar(
            select(StrategyConfigVersionRecord.id).where(
                StrategyConfigVersionRecord.strategy_id == str(strategy_id),
                StrategyConfigVersionRecord.version == revision,
            )
        )
        if config_id is None:
            raise StrategyRegistrationError(f"Unknown config revision: strategy_id={strategy_id} revision={revision}")
        return int(config_id)

    @staticmethod
    def _config_snapshot(
        meta: StrategyConfigVersionRecord, typed: MarketMakerConfigVersionRecord
    ) -> StrategyConfigSnapshot:
        values = {name: getattr(typed, name) for name in _MM_CONFIG_FIELDS}
        return StrategyConfigSnapshot(StrategyId(meta.strategy_id), int(meta.version), values)


class PostgresStrategyAllocationManager:
    """Exclusive allocation manager whose identity key is the fencing token."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def acquire(self, strategy_id: StrategyId, sub_account: SubAccount, symbol: Symbol) -> StrategyAllocation:
        values = {
            "strategy_id": str(strategy_id),
            "sub_account": str(sub_account),
            "symbol": str(symbol),
        }
        async with self._session_factory() as session, session.begin():
            inserted = await session.execute(
                pg_insert(StrategyAllocationRecord)
                .values(**values)
                .on_conflict_do_nothing()
                .returning(StrategyAllocationRecord.id)
            )
            fence = inserted.scalar_one_or_none()
            if fence is not None:
                return StrategyAllocation(strategy_id, sub_account, symbol, int(fence))
            existing = (
                await session.execute(
                    select(StrategyAllocationRecord).where(StrategyAllocationRecord.strategy_id == str(strategy_id))
                )
            ).scalar_one_or_none()
            if existing is not None:
                if existing.sub_account == str(sub_account) and existing.symbol == str(symbol):
                    return StrategyAllocation(strategy_id, sub_account, symbol, int(existing.id))
                raise StrategyLifecycleError(f"Strategy {strategy_id} already owns a different allocation")
            owner = (
                await session.execute(
                    select(StrategyAllocationRecord.strategy_id).where(
                        StrategyAllocationRecord.sub_account == str(sub_account),
                        StrategyAllocationRecord.symbol == str(symbol),
                    )
                )
            ).scalar_one_or_none()
            raise StrategyLifecycleError(
                f"Allocation is already owned: sub_account={sub_account} symbol={symbol} owner={owner}"
            )

    async def release(self, strategy_id: StrategyId) -> None:
        async with self._session_factory() as session, session.begin():
            await session.execute(
                delete(StrategyAllocationRecord).where(StrategyAllocationRecord.strategy_id == str(strategy_id))
            )

    async def get(self, strategy_id: StrategyId) -> StrategyAllocation | None:
        async with self._session_factory() as session:
            record = (
                await session.execute(
                    select(StrategyAllocationRecord).where(StrategyAllocationRecord.strategy_id == str(strategy_id))
                )
            ).scalar_one_or_none()
        if record is None:
            return None
        if record.sub_account is None:
            raise StrategyLifecycleError(f"Allocation has no sub-account: {strategy_id}")
        return StrategyAllocation(
            strategy_id,
            SubAccount(record.sub_account),
            Symbol(record.symbol),
            int(record.id),
        )


class PostgresMarketMakingReadRepository:
    """Authoritative, gap-explicit REST query repository."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        stale_after: timedelta = timedelta(seconds=5),
    ) -> None:
        if stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")
        self._session_factory = session_factory
        self._state_store = PostgresStrategyStateStore(session_factory)
        self._stale_after = stale_after

    def _is_stale(self, observed_at: datetime) -> bool:
        return _utcnow() - observed_at > self._stale_after

    async def get_market_making_state(self, strategy_id: StrategyId) -> AuthoritativeRead[MarketMakingStateView]:
        instance = await self._state_store.get_strategy_instance(strategy_id)
        async with self._session_factory() as session:
            runtime = await session.get(StrategyRuntimeStateRecord, str(strategy_id))
        if runtime is None:
            return AuthoritativeRead(None, None, True, "runtime_state_missing")
        effective = await self._state_store.get_runtime(strategy_id)
        as_of = runtime.heartbeat_at or runtime.updated_at
        stale = runtime.heartbeat_at is None or self._is_stale(as_of)
        return AuthoritativeRead(
            MarketMakingStateView(instance, effective, runtime.heartbeat_at),
            as_of,
            stale,
            "runtime_heartbeat_missing" if runtime.heartbeat_at is None else ("runtime_state_stale" if stale else None),
        )

    async def get_market_making_quotes(self, strategy_id: StrategyId) -> AuthoritativeRead[tuple[QuoteSlotView, ...]]:
        await self._state_store.get_strategy_instance(strategy_id)
        statement = (
            select(QuoteSlotRecord, OrderRecord)
            .outerjoin(OrderRecord, OrderRecord.order_id == QuoteSlotRecord.owner_order_id)
            .where(QuoteSlotRecord.strategy_id == str(strategy_id))
            .order_by(QuoteSlotRecord.symbol, QuoteSlotRecord.side, QuoteSlotRecord.level)
        )
        async with self._session_factory() as session:
            rows = (await session.execute(statement)).all()
        views = tuple(
            QuoteSlotView(
                Symbol(slot.symbol),
                slot.side,
                slot.level,
                slot.state,
                slot.plan_revision,
                slot.revision,
                slot.owner_order_id,
                slot.owner_plan_id,
                order.price if order else None,
                order.size if order else None,
                order.filled_size if order else None,
                order.status if order else None,
                slot.updated_at,
            )
            for slot, order in rows
        )
        as_of = max((view.updated_at for view in views), default=None)
        return AuthoritativeRead(views, as_of, False)

    async def get_market_making_inventory(self, strategy_id: StrategyId) -> AuthoritativeRead[InventoryView]:
        instance = await self._state_store.get_strategy_instance(strategy_id)
        definition = instance.definition
        statement = (
            select(PositionRecord, AccountStateRecord)
            .outerjoin(
                AccountStateRecord,
                AccountStateRecord.sub_account == PositionRecord.sub_account,
            )
            .where(
                PositionRecord.sub_account == str(definition.sub_account),
                PositionRecord.symbol == str(definition.symbol),
            )
        )
        async with self._session_factory() as session:
            row = (await session.execute(statement)).one_or_none()
        if row is None:
            return AuthoritativeRead(None, None, True, "position_projection_missing")
        position, account = row
        observed_at = position.exchange_updated_at or position.updated_at
        view = InventoryView(
            definition.symbol,
            position.size,
            position.entry_price,
            position.mark_price,
            position.unrealized_pnl,
            position.realized_pnl,
            position.liquidation_price,
            account.equity if account else None,
            account.available_balance if account else None,
            account.total_margin_used if account else None,
            position.revision,
            observed_at,
        )
        reason = "account_projection_missing" if account is None else None
        stale = account is None or self._is_stale(observed_at)
        if reason is None and stale:
            reason = "position_projection_stale"
        return AuthoritativeRead(view, observed_at, stale, reason)

    async def get_market_making_action_budget(self, strategy_id: StrategyId) -> AuthoritativeRead[ActionBudgetView]:
        await self._state_store.get_strategy_instance(strategy_id)
        statement = (
            select(ActionBudgetAllocationRecord, ActionBudgetScopeRecord)
            .join(
                ActionBudgetScopeRecord,
                ActionBudgetScopeRecord.quota_owner_address == ActionBudgetAllocationRecord.quota_owner_address,
            )
            .where(
                ActionBudgetAllocationRecord.strategy_id == str(strategy_id),
                ActionBudgetAllocationRecord.status == "active",
            )
        )
        async with self._session_factory() as session:
            row = (await session.execute(statement)).one_or_none()
        if row is None:
            return AuthoritativeRead(None, None, True, "active_action_budget_allocation_missing")
        allocation, scope = row
        stale = self._is_stale(scope.observed_at)
        return AuthoritativeRead(
            ActionBudgetView(
                scope.quota_owner_address,
                scope.mode,
                scope.remote_cap,
                scope.remote_used,
                scope.remote_remaining,
                scope.shadow_used,
                scope.emergency_reserve,
                allocation.soft_allocation,
                allocation.hard_allocation,
                scope.revision,
                scope.observed_at,
            ),
            scope.observed_at,
            stale,
            "action_budget_projection_stale" if stale else None,
        )

    async def get_market_making_performance(self, strategy_id: StrategyId) -> AuthoritativeRead[None]:
        """Make analytical unavailability explicit without affecting control reads."""

        await self._state_store.get_strategy_instance(strategy_id)
        return AuthoritativeRead(
            None,
            None,
            True,
            "clickhouse_performance_projection_unavailable",
            source="clickhouse",
        )

    async def get_market_making_events(
        self, strategy_id: StrategyId, *, limit: int = 100
    ) -> AuthoritativeRead[tuple[MarketMakingEventView, ...]]:
        if not 1 <= limit <= 1_000:
            raise ValueError("limit must be between 1 and 1000")
        await self._state_store.get_strategy_instance(strategy_id)
        statement = (
            select(StrategyStateEventRecord)
            .where(StrategyStateEventRecord.strategy_id == str(strategy_id))
            .order_by(StrategyStateEventRecord.created_at.desc(), StrategyStateEventRecord.id.desc())
            .limit(limit)
        )
        async with self._session_factory() as session:
            records = (await session.execute(statement)).scalars().all()
        events = tuple(
            MarketMakingEventView(
                record.id,
                "strategy_state",
                record.from_state,
                record.to_state,
                record.reason,
                record.actor,
                record.desired_config_version_id,
                record.effective_config_version_id,
                record.created_at,
            )
            for record in records
        )
        return AuthoritativeRead(events, events[0].occurred_at if events else None, False)

    async def get_quote_slots(
        self, strategy_id: StrategyId, symbol: Symbol
    ) -> tuple[DomainQuoteSlotView, DomainQuoteSlotView]:
        """Adapt the Postgres slot/order projection to the coordinator domain view."""

        statement = (
            select(QuoteSlotRecord, OrderRecord)
            .outerjoin(OrderRecord, OrderRecord.order_id == QuoteSlotRecord.owner_order_id)
            .where(
                QuoteSlotRecord.strategy_id == str(strategy_id),
                QuoteSlotRecord.symbol == str(symbol),
            )
        )
        async with self._session_factory() as session:
            rows = (await session.execute(statement)).all()
        by_side: dict[Side, DomainQuoteSlotView] = {}
        for slot, order in rows:
            side = Side(slot.side)
            owners: tuple[QuoteRiskOwner, ...] = ()
            if order is not None and order.price is not None:
                remaining = order.size - order.filled_size
                if remaining > 0:
                    status = (
                        OrderStatus.CANCEL_UNKNOWN if order.status == "cancel_pending" else OrderStatus(order.status)
                    )
                    owners = (
                        QuoteRiskOwner(
                            OrderId(str(order.order_id)),
                            Cloid(order.cloid),
                            Price(order.price),
                            Size(remaining),
                            status,
                            slot.plan_revision,
                            order.submitted_at or order.created_at,
                            order.exchange_oid is not None,
                        ),
                    )
            by_side[side] = DomainQuoteSlotView(
                QuoteSlotKey(strategy_id, symbol, side, slot.level),
                slot.revision,
                slot.plan_revision,
                owners,
                slot.updated_at,
            )
        bid = by_side.get(
            Side.BUY,
            DomainQuoteSlotView(QuoteSlotKey(strategy_id, symbol, Side.BUY), 0, 0, ()),
        )
        ask = by_side.get(
            Side.SELL,
            DomainQuoteSlotView(QuoteSlotKey(strategy_id, symbol, Side.SELL), 0, 0, ()),
        )
        return bid, ask

    # Singular aliases match the API design wording while plural names remain explicit.
    get_market_making_quote = get_market_making_quotes


class MarketMakingTransactionRepository:
    """One-session primitives used inside an existing durable transaction."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add_quote_plan(self, plan: QuotePlanRecord, items: Sequence[QuotePlanItemRecord]) -> None:
        self._session.add(plan)
        self._session.add_all(items)
        await self._session.flush()

    async def persist_quote_plan(self, plan: QuotePlanRecord, items: Sequence[QuotePlanItemRecord]) -> None:
        await self.add_quote_plan(plan, items)

    async def update_quote_slot(
        self,
        strategy_id: StrategyId,
        symbol: Symbol,
        side: str,
        level: int,
        *,
        owner_order_id: uuid.UUID | None,
        owner_plan_id: uuid.UUID | None,
        plan_revision: int,
        state: str,
        expected_revision: int,
    ) -> QuoteSlotRecord:
        result = await self._session.execute(
            update(QuoteSlotRecord)
            .where(
                QuoteSlotRecord.strategy_id == str(strategy_id),
                QuoteSlotRecord.symbol == str(symbol),
                QuoteSlotRecord.side == side,
                QuoteSlotRecord.level == level,
                QuoteSlotRecord.revision == expected_revision,
                QuoteSlotRecord.plan_revision <= plan_revision,
            )
            .values(
                owner_order_id=owner_order_id,
                owner_plan_id=owner_plan_id,
                plan_revision=plan_revision,
                state=state,
                revision=QuoteSlotRecord.revision + 1,
                updated_at=_utcnow(),
            )
            .returning(QuoteSlotRecord)
        )
        slot = result.scalar_one_or_none()
        if slot is None:
            raise StrategyLifecycleError("Quote slot revision or plan fence conflict")
        return slot

    async def add_execution_command_items(self, items: Sequence[ExecutionCommandItemRecord]) -> None:
        self._session.add_all(items)
        await self._session.flush()

    async def append_execution_action(self, action: ExecutionActionRecord) -> bool:
        statement = (
            pg_insert(ExecutionActionRecord)
            .values(
                command_item_id=action.command_item_id,
                attempt=action.attempt,
                action_type=action.action_type,
                request_hash=action.request_hash,
                sent_at=action.sent_at,
                responded_at=action.responded_at,
                outcome=action.outcome,
                response_code=action.response_code,
                estimated_credit_cost=action.estimated_credit_cost,
                reconciled_credit_cost=action.reconciled_credit_cost,
            )
            .on_conflict_do_nothing(
                index_elements=[ExecutionActionRecord.command_item_id, ExecutionActionRecord.attempt]
            )
            .returning(ExecutionActionRecord.id)
        )
        inserted = (await self._session.execute(statement)).scalar_one_or_none()
        if inserted is not None:
            return True
        existing_hash = await self._session.scalar(
            select(ExecutionActionRecord.request_hash).where(
                ExecutionActionRecord.command_item_id == action.command_item_id,
                ExecutionActionRecord.attempt == action.attempt,
            )
        )
        if existing_hash != action.request_hash:
            raise StrategyLifecycleError(
                "Execution attempt conflict: an attempt key was reused with a different request hash"
            )
        return False

    async def upsert_action_budget_scope(
        self, scope: ActionBudgetScopeRecord, *, expected_revision: int | None
    ) -> ActionBudgetScopeRecord:
        if expected_revision is None:
            self._session.add(scope)
            await self._session.flush()
            return scope
        result = await self._session.execute(
            update(ActionBudgetScopeRecord)
            .where(
                ActionBudgetScopeRecord.quota_owner_address == scope.quota_owner_address,
                ActionBudgetScopeRecord.revision == expected_revision,
            )
            .values(
                remote_cap=scope.remote_cap,
                remote_used=scope.remote_used,
                remote_remaining=scope.remote_remaining,
                shadow_used=scope.shadow_used,
                emergency_reserve=scope.emergency_reserve,
                mode=scope.mode,
                observed_at=scope.observed_at,
                revision=ActionBudgetScopeRecord.revision + 1,
                updated_at=_utcnow(),
            )
            .returning(ActionBudgetScopeRecord)
        )
        updated = result.scalar_one_or_none()
        if updated is None:
            raise StrategyLifecycleError("Action budget scope revision conflict")
        return updated

    async def add_action_budget_allocation(self, allocation: ActionBudgetAllocationRecord) -> None:
        self._session.add(allocation)
        await self._session.flush()

    async def append_action_budget_event(self, event: ActionBudgetEventRecord) -> None:
        self._session.add(event)
        await self._session.flush()


class PostgresMarketMakingRepository:
    """API-friendly facade over lifecycle commands and authoritative queries."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        stale_after: timedelta = timedelta(seconds=5),
        session_mode: str = "testnet",
    ) -> None:
        if session_mode not in {"shadow", "testnet", "mainnet"}:
            raise ValueError("invalid market-making session mode")
        self._session_factory = session_factory
        self._session_mode = session_mode
        self.state_store = PostgresStrategyStateStore(session_factory)
        self.allocations = PostgresStrategyAllocationManager(session_factory)
        self.reads = PostgresMarketMakingReadRepository(session_factory, stale_after=stale_after)

    async def get_quote_slots(
        self, strategy_id: StrategyId, symbol: Symbol
    ) -> tuple[DomainQuoteSlotView, DomainQuoteSlotView]:
        return await self.reads.get_quote_slots(strategy_id, symbol)

    async def submit_quote_plan(self, plan: QuotePlan) -> None:
        """Atomically persist one quote revision and every recoverable child.

        The runtime session token is retained in the parent command payload;
        ``market_making_sessions.id`` remains the database-local session key.
        A repeated (strategy, runtime-session, revision) submission is a no-op
        only when its immutable payload hash matches.
        """

        if plan.fenced:
            raise StrategyLifecycleError(f"Cannot submit fenced quote plan: {plan.fence_reason}")
        if plan.fair_price is None or plan.reservation_price is None:
            raise StrategyLifecycleError("Quote plan is missing its pricing snapshot")
        plan_key = f"hypeedge:quote-plan:{plan.strategy_id}:{plan.session_id}:{plan.revision}"
        plan_id = uuid.uuid5(uuid.NAMESPACE_URL, plan_key)
        command_id = uuid.uuid5(plan_id, "execution-command")
        payload = _quote_plan_command_payload(plan, plan_id)
        payload_hash = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

        async with self._session_factory() as session, session.begin():
            existing = await session.scalar(select(QuotePlanRecord).where(QuotePlanRecord.plan_id == plan_id))
            if existing is not None:
                command = await session.scalar(
                    select(ExecutionCommandRecord).where(ExecutionCommandRecord.command_id == command_id)
                )
                if command is None or command.payload.get("plan_hash") != payload_hash:
                    raise StrategyLifecycleError("Quote plan revision was reused with a different payload")
                return

            instance = await session.scalar(
                select(StrategyInstanceRecord)
                .where(StrategyInstanceRecord.strategy_id == str(plan.strategy_id))
                .with_for_update()
            )
            if instance is None or instance.archived_at is not None:
                raise StrategyLifecycleError("Quote plan strategy is missing or archived")
            runtime = await session.scalar(
                select(StrategyRuntimeStateRecord)
                .where(StrategyRuntimeStateRecord.strategy_id == str(plan.strategy_id))
                .with_for_update()
            )
            if runtime is None or runtime.actual_state != MarketMakerLifecycle.RUNNING.value:
                raise StrategyLifecycleError("Live quote plan requires RUNNING runtime state")
            config = await session.scalar(
                select(StrategyConfigVersionRecord).where(
                    StrategyConfigVersionRecord.strategy_id == str(plan.strategy_id),
                    StrategyConfigVersionRecord.version == plan.config_version,
                )
            )
            if config is None or runtime.effective_config_version_id != config.id:
                raise StrategyLifecycleError("Quote plan config is not the effective runtime config")
            db_session = await session.scalar(
                select(MarketMakingSessionRecord)
                .where(
                    MarketMakingSessionRecord.strategy_id == str(plan.strategy_id),
                    MarketMakingSessionRecord.ended_at.is_(None),
                )
                .with_for_update()
            )
            if db_session is None:
                db_session = MarketMakingSessionRecord(
                    strategy_id=str(plan.strategy_id), config_version_id=config.id, mode=self._session_mode
                )
                session.add(db_session)
                await session.flush()
            elif db_session.config_version_id != config.id:
                raise StrategyLifecycleError("Active market-making session belongs to another config")

            record = QuotePlanRecord(
                plan_id=plan_id,
                strategy_id=str(plan.strategy_id),
                session_id=db_session.id,
                config_version_id=config.id,
                revision=plan.revision,
                market_version=plan.market_version,
                fair_price=Decimal(plan.fair_price),
                reservation_price=Decimal(plan.reservation_price),
                inventory_size=Decimal(plan.inventory_notional),
                budget_mode=plan.budget_mode.value,
                status="planned",
                valid_until=plan.valid_until,
            )
            session.add(record)
            parent_payload = {**payload, "plan_hash": payload_hash, "sub_account": instance.sub_account}
            session.add(
                ExecutionCommandRecord(
                    command_id=command_id,
                    command_type="quote_plan",
                    actor_type="strategy",
                    actor_id=str(plan.strategy_id),
                    idempotency_key=f"{plan.session_id}:{plan.revision}",
                    priority=20,
                    status="pending",
                    payload=parent_payload,
                    available_at=_utcnow(),
                )
            )
            await session.flush()
            await self._persist_plan_children(session, plan, plan_id, command_id, instance.sub_account)

    async def _persist_plan_children(
        self,
        session: AsyncSession,
        plan: QuotePlan,
        plan_id: uuid.UUID,
        command_id: uuid.UUID,
        sub_account: str | None,
    ) -> None:
        ordinal = 0
        for item_ordinal, diff in enumerate(plan.diffs):
            if not diff.child_actions and diff.action not in {QuoteAction.BLOCKED_UNKNOWN}:
                continue
            source_id = _uuid_or_none(diff.source.order_id if diff.source is not None else None)
            target_id: uuid.UUID | None = None
            target_cloid: str | None = None
            if "place" in diff.child_actions:
                if diff.desired.price is None or diff.desired.size is None:
                    raise StrategyLifecycleError("Placement child lacks desired price or size")
                order_key = uuid.uuid5(plan_id, f"order:{diff.slot.side.value}:{diff.slot.level}")
                target_cloid = CloidGenerator.to_hl_cloid(Cloid(str(order_key)))
            plan_item = QuotePlanItemRecord(
                plan_id=plan_id,
                ordinal=item_ordinal,
                symbol=str(plan.symbol),
                side=diff.slot.side.value,
                level=diff.slot.level,
                decision=("blocked_unknown" if diff.action == QuoteAction.BLOCKED_UNKNOWN else diff.child_actions[-1]),
                source_order_id=source_id,
                target_order_id=target_id,
                source_cloid=str(diff.source.cloid) if diff.source is not None else None,
                target_cloid=target_cloid,
                desired_price=Decimal(diff.desired.price) if diff.desired.price is not None else None,
                desired_size=Decimal(diff.desired.size) if diff.desired.size is not None else None,
            )
            session.add(plan_item)
            await session.flush()
            child_records: list[ExecutionCommandItemRecord] = []
            for action in diff.child_actions:
                child_records.append(
                    ExecutionCommandItemRecord(
                        command_id=command_id,
                        plan_item_id=plan_item.id,
                        ordinal=ordinal,
                        action_type=action,
                        source_order_id=source_id,
                        target_order_id=target_id,
                        status="pending",
                        available_at=_utcnow(),
                    )
                )
                ordinal += 1
            session.add_all(child_records)
            await session.flush()
            for child in child_records:
                if child.action_type == "place":
                    assert diff.desired.price is not None and diff.desired.size is not None
                    session.add(
                        RiskReservationRecord(
                            command_id=command_id,
                            command_item_id=child.id,
                            risk_owner_type="new_quote",
                            risk_owner_key=f"{plan_id}:{plan_item.id}",
                            order_id=None,
                            sub_account=sub_account,
                            strategy_id=str(plan.strategy_id),
                            symbol=str(plan.symbol),
                            side=diff.slot.side.value,
                            reduce_only=False,
                            reserved_size=Decimal(diff.desired.size),
                            reserved_notional=Decimal(diff.desired.size) * Decimal(diff.desired.price),
                            expires_at=plan.valid_until,
                        )
                    )
            owner_id = source_id if "cancel" in diff.child_actions else None
            slot_state = "recovery_required" if diff.action == QuoteAction.BLOCKED_UNKNOWN else "inflight"
            await session.execute(
                pg_insert(QuoteSlotRecord)
                .values(
                    strategy_id=str(plan.strategy_id),
                    symbol=str(plan.symbol),
                    side=diff.slot.side.value,
                    level=diff.slot.level,
                    owner_order_id=owner_id,
                    owner_plan_id=plan_id,
                    plan_revision=plan.revision,
                    state=slot_state,
                    revision=1,
                    updated_at=_utcnow(),
                )
                .on_conflict_do_update(
                    index_elements=[
                        QuoteSlotRecord.strategy_id,
                        QuoteSlotRecord.symbol,
                        QuoteSlotRecord.side,
                        QuoteSlotRecord.level,
                    ],
                    set_={
                        "owner_order_id": owner_id,
                        "owner_plan_id": plan_id,
                        "plan_revision": plan.revision,
                        "state": slot_state,
                        "revision": QuoteSlotRecord.revision + 1,
                        "updated_at": _utcnow(),
                    },
                    where=QuoteSlotRecord.plan_revision < plan.revision,
                )
            )

    async def create_strategy_instance(
        self,
        *,
        strategy_id: StrategyId,
        sub_account: SubAccount,
        symbol: Symbol,
        initial_config: Mapping[str, Any],
        created_by: str,
        metadata: Mapping[str, Any] | None = None,
        desired_state: MarketMakerLifecycle = MarketMakerLifecycle.STOPPED,
    ) -> StrategyInstanceView:
        definition = StrategyInstanceDefinition(
            strategy_id,
            "market_maker",
            sub_account,
            symbol,
            desired_state,
            1,
            0,
        )
        return await self.state_store.create_strategy_instance(
            definition,
            initial_config,
            created_by=created_by,
            metadata=metadata,
        )

    async def list_strategy_instances(self, *, include_archived: bool = False) -> list[StrategyInstanceView]:
        return await self.state_store.list_strategy_instances(include_archived=include_archived)

    async def get_strategy_instance(self, strategy_id: StrategyId) -> StrategyInstanceView:
        return await self.state_store.get_strategy_instance(strategy_id)

    async def update_strategy_metadata(
        self,
        strategy_id: StrategyId,
        metadata: Mapping[str, Any],
        *,
        expected_revision: int,
    ) -> StrategyInstanceView:
        return await self.state_store.update_strategy_metadata(
            strategy_id, metadata, expected_revision=expected_revision
        )

    async def archive_strategy_instance(
        self, strategy_id: StrategyId, *, expected_revision: int
    ) -> StrategyInstanceView:
        return await self.state_store.archive_strategy_instance(strategy_id, expected_revision=expected_revision)

    async def list_config_versions(self, strategy_id: StrategyId) -> list[StrategyConfigSnapshot]:
        return await self.state_store.list_config_versions(strategy_id)

    async def create_market_maker_config_version(
        self,
        strategy_id: StrategyId,
        values: Mapping[str, Any],
        *,
        created_by: str,
        expected_revision: int | None = None,
    ) -> StrategyConfigSnapshot:
        return await self.state_store.create_market_maker_config_version(
            strategy_id,
            values,
            created_by=created_by,
            expected_revision=expected_revision,
        )

    async def get_market_making_state(self, strategy_id: StrategyId) -> AuthoritativeRead[MarketMakingStateView]:
        return await self.reads.get_market_making_state(strategy_id)

    async def get_market_making_quotes(self, strategy_id: StrategyId) -> AuthoritativeRead[tuple[QuoteSlotView, ...]]:
        return await self.reads.get_market_making_quotes(strategy_id)

    async def get_market_making_inventory(self, strategy_id: StrategyId) -> AuthoritativeRead[InventoryView]:
        return await self.reads.get_market_making_inventory(strategy_id)

    async def get_market_making_action_budget(self, strategy_id: StrategyId) -> AuthoritativeRead[ActionBudgetView]:
        return await self.reads.get_market_making_action_budget(strategy_id)

    async def get_market_making_performance(self, strategy_id: StrategyId) -> AuthoritativeRead[None]:
        return await self.reads.get_market_making_performance(strategy_id)

    async def get_market_making_events(
        self, strategy_id: StrategyId, *, limit: int = 100
    ) -> AuthoritativeRead[tuple[MarketMakingEventView, ...]]:
        return await self.reads.get_market_making_events(strategy_id, limit=limit)
