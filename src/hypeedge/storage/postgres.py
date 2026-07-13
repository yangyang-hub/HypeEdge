"""Postgres V2 transactional models, repositories, and unit of work.

Postgres is the local system of record for trading state.  Monetary values
are stored as exact decimals, timestamps are timezone-aware, and schema
creation is exclusively managed by Alembic (design document section 17.5).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import TracebackType
from typing import TYPE_CHECKING, Any

import structlog
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import INET, JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from hypeedge.config.settings import PostgresSettings
from hypeedge.core.enums import OrderStatus, OrderType, Side, TimeInForce
from hypeedge.core.events import (
    EVENT_ORDER_CANCELLED,
    EVENT_ORDER_FILLED,
    EVENT_ORDER_REJECTED,
    EVENT_ORDER_SUBMITTED,
    Event,
    EventBus,
)
from hypeedge.core.models import Order, RiskCheckResult
from hypeedge.core.types import Cloid, OrderId, Price, Size, StrategyId, SubAccount, Symbol
from hypeedge.execution.durable import DurableExecutionCommand

if TYPE_CHECKING:
    from hypeedge.risk.checker import RiskLimits

logger = structlog.get_logger(__name__)

DECIMAL_PRECISION = 38
DECIMAL_SCALE = 18
MONEY = Numeric(DECIMAL_PRECISION, DECIMAL_SCALE)
UTC_TIMESTAMP = DateTime(timezone=True)

ORDER_STATUSES = (
    "pending",
    "submitted",
    "submit_unknown",
    "acknowledged",
    "partial_fill",
    "filled",
    "cancel_pending",
    "cancel_unknown",
    "cancelled",
    "rejected",
    "expired",
)
SYSTEM_STATES = (
    "starting",
    "reconciling",
    "normal",
    "reduce_only",
    "cancel_only",
    "halting",
    "halted",
    "recovering",
    "stopping",
)
COMMAND_STATUSES = ("pending", "processing", "succeeded", "failed", "unknown", "cancelled")
RESERVATION_STATUSES = ("active", "consumed", "released", "expired")
RECONCILIATION_STATUSES = ("running", "succeeded", "failed")
STRATEGY_DESIRED_STATES = ("stopped", "warming", "shadow", "running", "paused", "draining", "faulted")
STRATEGY_RUNTIME_STATES = ("stopped", "warming", "shadow", "running", "paused", "draining", "faulted")
STRATEGY_TYPES = ("trend_follow", "market_maker", "legacy")
SESSION_MODES = ("shadow", "testnet", "mainnet")
QUOTE_PLAN_STATUSES = ("planned", "dispatching", "succeeded", "partial", "unknown", "cancelled", "superseded")
QUOTE_DECISIONS = ("place", "cancel", "modify", "blocked_unknown")
QUOTE_SLOT_STATES = ("empty", "live", "inflight", "unknown", "orphaned_live", "recovery_required")
EXECUTION_ITEM_STATUSES = (
    "pending",
    "processing",
    "succeeded",
    "failed",
    "unknown",
    "cancelled",
    "superseded",
    "expired",
    "blocked",
)
EXECUTION_ACTION_OUTCOMES = ("succeeded", "rejected", "timeout", "unknown", "transport_error")
BUDGET_MODES = ("normal", "conserve", "critical", "cancel_only", "exhausted")
BUDGET_ALLOCATION_STATUSES = ("active", "released")
RISK_OWNER_TYPES = ("legacy", "live_order", "inflight_place", "unknown", "new_quote")


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _uuid() -> uuid.UUID:
    return uuid.uuid4()


def _enum_check(column: str, values: tuple[str, ...], name: str) -> CheckConstraint:
    rendered = ", ".join(f"'{value}'" for value in values)
    return CheckConstraint(f"{column} IN ({rendered})", name=name)


class Base(DeclarativeBase):
    """SQLAlchemy declarative base for all Postgres tables."""


class OrderRecord(Base):
    """Current order projection; immutable transitions live in ``order_events``."""

    __tablename__ = "orders"
    __table_args__ = (
        _enum_check("status", ORDER_STATUSES, "ck_orders_status"),
        CheckConstraint("size > 0", name="ck_orders_size_positive"),
        CheckConstraint("filled_size >= 0 AND filled_size <= size", name="ck_orders_filled_size"),
        CheckConstraint("price IS NULL OR price > 0", name="ck_orders_price_positive"),
        CheckConstraint("cloid ~ '^0x[0-9a-f]{32}$'", name="ck_orders_cloid_format"),
        Index("ix_orders_account_status_created", "sub_account", "status", "created_at"),
        Index("ix_orders_strategy_created", "strategy_id", "created_at"),
        Index(
            "ix_orders_open",
            "sub_account",
            "symbol",
            postgresql_where="status IN ('pending', 'submitted', 'submit_unknown', 'acknowledged', "
            "'partial_fill', 'cancel_pending', 'cancel_unknown')",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    order_id: Mapped[uuid.UUID] = mapped_column(unique=True, nullable=False, default=_uuid)
    cloid: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    legacy_cloid: Mapped[str | None] = mapped_column(Text)
    exchange_oid: Mapped[str | None] = mapped_column(Text, index=True)
    command_id: Mapped[uuid.UUID | None] = mapped_column(index=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    side: Mapped[str] = mapped_column(Text, nullable=False)
    order_type: Mapped[str] = mapped_column(Text, nullable=False)
    time_in_force: Mapped[str] = mapped_column(Text, nullable=False, default="Gtc", server_default="Gtc")
    size: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    price: Mapped[Decimal | None] = mapped_column(MONEY)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending", server_default="pending")
    strategy_id: Mapped[str | None] = mapped_column(Text, index=True)
    sub_account: Mapped[str | None] = mapped_column(Text, index=True)
    client_id: Mapped[str | None] = mapped_column(Text)
    reduce_only: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    filled_size: Mapped[Decimal] = mapped_column(MONEY, nullable=False, default=Decimal(0), server_default="0")
    avg_fill_price: Mapped[Decimal | None] = mapped_column(MONEY)
    revision: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    error_code: Mapped[str | None] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(Text)
    submitted_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    acknowledged_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    filled_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP,
        nullable=False,
        default=_utcnow,
        server_default=func.now(),
        onupdate=_utcnow,
    )


class OrderEvent(Base):
    """Append-only order transition history."""

    __tablename__ = "order_events"
    __table_args__ = (
        UniqueConstraint("order_id", "revision", name="uq_order_events_order_revision"),
        CheckConstraint("revision >= 0", name="ck_order_events_revision"),
        Index("ix_order_events_order_created", "order_id", "created_at"),
        Index("ix_order_events_type_created", "event_type", "created_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(unique=True, nullable=False, default=_uuid)
    order_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("orders.order_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    cloid: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[str | None] = mapped_column(Text)
    size: Mapped[Decimal | None] = mapped_column(MONEY)
    price: Mapped[Decimal | None] = mapped_column(MONEY)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    error_code: Mapped[str | None] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(Text)
    strategy_id: Mapped[str | None] = mapped_column(Text, index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    # Kept for compatibility with the Phase 2 writer; new code uses payload.
    extra_data: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )


class FillRecord(Base):
    """Immutable, idempotently ingested exchange fill."""

    __tablename__ = "fills"
    __table_args__ = (
        UniqueConstraint("source", "exchange_fill_id", name="uq_fills_source_exchange_fill"),
        CheckConstraint("price > 0", name="ck_fills_price_positive"),
        CheckConstraint("size > 0", name="ck_fills_size_positive"),
        Index("ix_fills_account_occurred", "sub_account", "occurred_at"),
        Index("ix_fills_symbol_occurred", "symbol", "occurred_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    fill_id: Mapped[uuid.UUID] = mapped_column(unique=True, nullable=False, default=_uuid)
    source: Mapped[str] = mapped_column(Text, nullable=False, default="hyperliquid", server_default="hyperliquid")
    exchange_fill_id: Mapped[str] = mapped_column(Text, nullable=False)
    order_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("orders.order_id", ondelete="RESTRICT"), index=True)
    cloid: Mapped[str | None] = mapped_column(Text, index=True)
    exchange_oid: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    side: Mapped[str] = mapped_column(Text, nullable=False)
    price: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    size: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    fee: Mapped[Decimal] = mapped_column(MONEY, nullable=False, default=Decimal(0), server_default="0")
    realized_pnl: Mapped[Decimal] = mapped_column(MONEY, nullable=False, default=Decimal(0), server_default="0")
    is_maker: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    strategy_id: Mapped[str | None] = mapped_column(Text, index=True)
    sub_account: Mapped[str | None] = mapped_column(Text, index=True)
    occurred_at: Mapped[datetime] = mapped_column(UTC_TIMESTAMP, nullable=False)
    # Legacy alias persisted during migration; new readers use occurred_at.
    timestamp: Mapped[datetime] = mapped_column(UTC_TIMESTAMP, nullable=False)
    raw_event: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )


class PositionRecord(Base):
    """Current authoritative local position projection."""

    __tablename__ = "positions"
    __table_args__ = (
        CheckConstraint("leverage >= 1", name="ck_positions_leverage"),
        Index(
            "uq_positions_scope_symbol",
            "sub_account",
            "symbol",
            unique=True,
            postgresql_nulls_not_distinct=True,
        ),
        Index("ix_positions_account_updated", "sub_account", "updated_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    position_id: Mapped[uuid.UUID] = mapped_column(unique=True, nullable=False, default=_uuid)
    sub_account: Mapped[str | None] = mapped_column(Text, index=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    size: Mapped[Decimal] = mapped_column(MONEY, nullable=False, default=Decimal(0), server_default="0")
    entry_price: Mapped[Decimal | None] = mapped_column(MONEY)
    mark_price: Mapped[Decimal | None] = mapped_column(MONEY)
    unrealized_pnl: Mapped[Decimal] = mapped_column(MONEY, nullable=False, default=Decimal(0), server_default="0")
    realized_pnl: Mapped[Decimal] = mapped_column(MONEY, nullable=False, default=Decimal(0), server_default="0")
    leverage: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    liquidation_price: Mapped[Decimal | None] = mapped_column(MONEY)
    exchange_updated_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    revision: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now(), onupdate=_utcnow
    )


class AccountStateRecord(Base):
    """Hot account projection used by transactional risk checks."""

    __tablename__ = "account_state"
    __table_args__ = (Index("uq_account_state_scope", "sub_account", unique=True, postgresql_nulls_not_distinct=True),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    sub_account: Mapped[str | None] = mapped_column(Text)
    equity: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    available_balance: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    total_margin_used: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    total_unrealized_pnl: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    peak_equity: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    action_credits_remaining: Mapped[int | None] = mapped_column(BigInteger)
    exchange_updated_at: Mapped[datetime] = mapped_column(UTC_TIMESTAMP, nullable=False)
    reconciled_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    revision: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    updated_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now(), onupdate=_utcnow
    )


class SystemStateRecord(Base):
    """Durable trading lifecycle and Kill Switch state."""

    __tablename__ = "system_state"
    __table_args__ = (_enum_check("state", SYSTEM_STATES, "ck_system_state_state"),)

    state_key: Mapped[str] = mapped_column(Text, primary_key=True, default="trading", server_default="trading")
    state: Mapped[str] = mapped_column(Text, nullable=False, default="starting", server_default="starting")
    kill_switch_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    reason: Mapped[str | None] = mapped_column(Text)
    triggered_by: Mapped[str | None] = mapped_column(Text)
    triggered_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    last_reconciliation_id: Mapped[uuid.UUID | None] = mapped_column(index=True)
    revision: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    metadata_: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, default=dict, server_default="{}"
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now(), onupdate=_utcnow
    )


class StrategyInstanceRecord(Base):
    """Registered strategy identity and operator desired state."""

    __tablename__ = "strategy_instances"
    __table_args__ = (
        _enum_check("strategy_type", STRATEGY_TYPES, "ck_strategy_instances_type"),
        _enum_check("desired_state", STRATEGY_DESIRED_STATES, "ck_strategy_instances_desired_state"),
        CheckConstraint("revision >= 0", name="ck_strategy_instances_revision"),
        CheckConstraint("archived_at IS NULL OR archived_at >= created_at", name="ck_strategy_instances_archive_time"),
        ForeignKeyConstraint(
            ["desired_config_version_id", "strategy_id"],
            ["strategy_config_versions.id", "strategy_config_versions.strategy_id"],
            name="fk_strategy_instances_desired_config",
            ondelete="RESTRICT",
        ),
        Index("ix_strategy_instances_scope", "sub_account", "symbol"),
        Index("ix_strategy_instances_desired_config", "desired_config_version_id"),
    )

    strategy_id: Mapped[str] = mapped_column(Text, primary_key=True)
    strategy_type: Mapped[str] = mapped_column(Text, nullable=False)
    sub_account: Mapped[str | None] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    desired_state: Mapped[str] = mapped_column(Text, nullable=False, default="stopped", server_default="stopped")
    desired_config_version_id: Mapped[int | None] = mapped_column(BigInteger)
    revision: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    metadata_: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, default=dict, server_default="{}"
    )
    archived_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now(), onupdate=_utcnow
    )


class StrategyAllocationRecord(Base):
    """Exclusive current lease for one account and symbol."""

    __tablename__ = "strategy_allocations"
    __table_args__ = (
        Index(
            "uq_strategy_allocations_scope",
            "sub_account",
            "symbol",
            unique=True,
            postgresql_nulls_not_distinct=True,
        ),
        CheckConstraint("revision >= 0", name="ck_strategy_allocations_revision"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    strategy_id: Mapped[str] = mapped_column(
        ForeignKey("strategy_instances.strategy_id", ondelete="RESTRICT"), nullable=False, unique=True, index=True
    )
    sub_account: Mapped[str | None] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    allocated_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )
    revision: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")


class StrategyConfigVersionRecord(Base):
    """Immutable metadata for a strategy configuration version."""

    __tablename__ = "strategy_config_versions"
    __table_args__ = (
        UniqueConstraint("strategy_id", "version", name="uq_strategy_config_versions_version"),
        UniqueConstraint("strategy_id", "config_hash", name="uq_strategy_config_versions_hash"),
        UniqueConstraint("id", "strategy_id", name="uq_strategy_config_versions_id_strategy"),
        CheckConstraint("version > 0", name="ck_strategy_config_versions_version"),
        CheckConstraint("length(config_hash) = 64", name="ck_strategy_config_versions_hash"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    strategy_id: Mapped[str] = mapped_column(
        ForeignKey("strategy_instances.strategy_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    config_hash: Mapped[str] = mapped_column(Text, nullable=False)
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )


class MarketMakerConfigVersionRecord(Base):
    """Typed, immutable market-making parameters for one config version."""

    __tablename__ = "market_maker_config_versions"
    __table_args__ = (
        CheckConstraint("soft_inventory_notional > 0", name="ck_mm_config_soft_inventory"),
        CheckConstraint("hard_inventory_notional >= soft_inventory_notional", name="ck_mm_config_hard_inventory"),
        CheckConstraint(
            "emergency_inventory_notional >= hard_inventory_notional", name="ck_mm_config_emergency_inventory"
        ),
        CheckConstraint("quote_size > 0", name="ck_mm_config_quote_size"),
        CheckConstraint("max_depth_participation > 0 AND max_depth_participation <= 1", name="ck_mm_config_depth"),
        CheckConstraint("min_quote_lifetime_ms >= 0", name="ck_mm_config_min_lifetime"),
        CheckConstraint("refresh_cooldown_ms >= 0", name="ck_mm_config_cooldown"),
        CheckConstraint("max_quote_age_ms > 0", name="ck_mm_config_max_age"),
        CheckConstraint("market_stale_after_ms > 0", name="ck_mm_config_market_stale"),
        CheckConstraint("account_stale_after_ms > 0", name="ck_mm_config_account_stale"),
        CheckConstraint("min_expected_pnl_usdc >= 0", name="ck_mm_config_expected_pnl"),
        CheckConstraint(
            "external_reference_weight >= 0 AND external_reference_weight <= 1",
            name="ck_mm_config_external_weight",
        ),
        CheckConstraint("external_max_age_seconds > 0", name="ck_mm_config_external_max_age"),
        CheckConstraint("external_outlier_bps > 0", name="ck_mm_config_external_outlier"),
        CheckConstraint("max_external_shift_ticks >= 0", name="ck_mm_config_external_shift"),
        CheckConstraint("max_total_fair_shift_ticks >= 0", name="ck_mm_config_total_shift"),
        CheckConstraint("latency_risk_multiplier >= 0", name="ck_mm_config_latency_multiplier"),
        CheckConstraint("conservative_latency_seconds >= 0", name="ck_mm_config_latency_default"),
        CheckConstraint("conservative_markout_bps >= 0", name="ck_mm_config_markout_default"),
        CheckConstraint("min_markout_samples > 0", name="ck_mm_config_markout_samples"),
    )

    config_version_id: Mapped[int] = mapped_column(
        ForeignKey("strategy_config_versions.id", ondelete="RESTRICT"), primary_key=True
    )
    soft_inventory_notional: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    hard_inventory_notional: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    emergency_inventory_notional: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    quote_size: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    max_depth_participation: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    inventory_skew_bps: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    max_inventory_shift_bps: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    min_half_spread_bps: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    toxicity_spread_bps: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    min_expected_pnl_usdc: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    external_reference_weight: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    external_max_age_seconds: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    external_outlier_bps: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    max_external_shift_ticks: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    max_total_fair_shift_ticks: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    latency_risk_multiplier: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    conservative_latency_seconds: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    conservative_markout_bps: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    min_markout_samples: Mapped[int] = mapped_column(BigInteger, nullable=False)
    min_quote_lifetime_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    refresh_cooldown_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_quote_age_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    market_stale_after_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    account_stale_after_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)


class TrendFollowConfigVersionRecord(Base):
    """Typed, immutable trend-follow parameters for one config version."""

    __tablename__ = "trend_follow_config_versions"
    __table_args__ = (
        CheckConstraint("fast_ema_period > 0", name="ck_tf_config_fast_ema"),
        CheckConstraint("slow_ema_period > 0", name="ck_tf_config_slow_ema"),
        CheckConstraint("fast_ema_period < slow_ema_period", name="ck_tf_config_ema_order"),
        CheckConstraint("signal_ema_period > 0", name="ck_tf_config_signal_ema"),
        CheckConstraint("momentum_period > 0", name="ck_tf_config_momentum_period"),
        CheckConstraint("atr_period > 0", name="ck_tf_config_atr_period"),
        CheckConstraint("atr_position_multiplier > 0", name="ck_tf_config_atr_pos_mult"),
        CheckConstraint("atr_stop_multiplier > 0", name="ck_tf_config_atr_stop_mult"),
        CheckConstraint("max_position_pct > 0 AND max_position_pct <= 1", name="ck_tf_config_max_pos"),
        CheckConstraint("risk_per_trade_pct > 0 AND risk_per_trade_pct <= 1", name="ck_tf_config_risk"),
    )

    config_version_id: Mapped[int] = mapped_column(
        ForeignKey("strategy_config_versions.id", ondelete="RESTRICT"), primary_key=True
    )
    fast_ema_period: Mapped[int] = mapped_column(BigInteger, nullable=False)
    slow_ema_period: Mapped[int] = mapped_column(BigInteger, nullable=False)
    signal_ema_period: Mapped[int] = mapped_column(BigInteger, nullable=False)
    momentum_period: Mapped[int] = mapped_column(BigInteger, nullable=False)
    momentum_threshold: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    atr_period: Mapped[int] = mapped_column(BigInteger, nullable=False)
    atr_position_multiplier: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    atr_stop_multiplier: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    max_position_pct: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    risk_per_trade_pct: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    macd_cross_threshold: Mapped[Decimal] = mapped_column(MONEY, nullable=False)


class StrategyRuntimeStateRecord(Base):
    """Recoverable hot state acknowledged by the active supervisor."""

    __tablename__ = "strategy_runtime_state"
    __table_args__ = (
        _enum_check("actual_state", STRATEGY_RUNTIME_STATES, "ck_strategy_runtime_state_actual"),
        CheckConstraint("revision >= 0", name="ck_strategy_runtime_state_revision"),
        ForeignKeyConstraint(
            ["effective_config_version_id", "strategy_id"],
            ["strategy_config_versions.id", "strategy_config_versions.strategy_id"],
            name="fk_strategy_runtime_state_effective_config",
            ondelete="RESTRICT",
        ),
        Index("ix_strategy_runtime_state_effective_config", "effective_config_version_id"),
    )

    strategy_id: Mapped[str] = mapped_column(
        ForeignKey("strategy_instances.strategy_id", ondelete="RESTRICT"), primary_key=True
    )
    actual_state: Mapped[str] = mapped_column(Text, nullable=False, default="stopped", server_default="stopped")
    effective_config_version_id: Mapped[int | None] = mapped_column(BigInteger)
    heartbeat_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    revision: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    reason: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now(), onupdate=_utcnow
    )


class StrategyStateEventRecord(Base):
    """Append-only strategy lifecycle and config activation audit."""

    __tablename__ = "strategy_state_events"
    __table_args__ = (
        CheckConstraint(
            "from_state IS NULL OR from_state IN "
            "('stopped','warming','shadow','running','paused','draining','faulted')",
            name="ck_strategy_state_events_from_state",
        ),
        CheckConstraint(
            "to_state IN ('stopped','warming','shadow','running','paused','draining','faulted')",
            name="ck_strategy_state_events_to_state",
        ),
        Index("ix_strategy_state_events_strategy_created", "strategy_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    strategy_id: Mapped[str] = mapped_column(
        ForeignKey("strategy_instances.strategy_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    from_state: Mapped[str | None] = mapped_column(Text)
    to_state: Mapped[str] = mapped_column(Text, nullable=False)
    desired_config_version_id: Mapped[int | None] = mapped_column(
        ForeignKey("strategy_config_versions.id", ondelete="RESTRICT"), index=True
    )
    effective_config_version_id: Mapped[int | None] = mapped_column(
        ForeignKey("strategy_config_versions.id", ondelete="RESTRICT"), index=True
    )
    reason: Mapped[str | None] = mapped_column(Text)
    actor: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )


class MarketMakingSessionRecord(Base):
    """An explicit shadow/testnet/mainnet market-making session boundary."""

    __tablename__ = "market_making_sessions"
    __table_args__ = (
        _enum_check("mode", SESSION_MODES, "ck_market_making_sessions_mode"),
        CheckConstraint("ended_at IS NULL OR ended_at >= started_at", name="ck_market_making_sessions_time"),
        ForeignKeyConstraint(
            ["config_version_id", "strategy_id"],
            ["strategy_config_versions.id", "strategy_config_versions.strategy_id"],
            name="fk_market_making_sessions_config",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("id", "strategy_id", name="uq_market_making_sessions_id_strategy"),
        Index("ix_market_making_sessions_strategy_started", "strategy_id", "started_at"),
        Index(
            "uq_market_making_sessions_active_strategy",
            "strategy_id",
            unique=True,
            postgresql_where="ended_at IS NULL",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    strategy_id: Mapped[str] = mapped_column(
        ForeignKey("strategy_instances.strategy_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    config_version_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    mode: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )
    ended_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    stop_reason: Mapped[str | None] = mapped_column(Text)


class QuotePlanRecord(Base):
    """Durable desired quote revision that crossed a transactional boundary."""

    __tablename__ = "quote_plans"
    __table_args__ = (
        _enum_check("budget_mode", BUDGET_MODES, "ck_quote_plans_budget_mode"),
        _enum_check("status", QUOTE_PLAN_STATUSES, "ck_quote_plans_status"),
        ForeignKeyConstraint(
            ["session_id", "strategy_id"],
            ["market_making_sessions.id", "market_making_sessions.strategy_id"],
            name="fk_quote_plans_session",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("strategy_id", "session_id", "revision", name="uq_quote_plans_revision"),
        UniqueConstraint("plan_id", "strategy_id", name="uq_quote_plans_id_strategy"),
        CheckConstraint("revision >= 0", name="ck_quote_plans_revision"),
        CheckConstraint("market_version >= 0", name="ck_quote_plans_market_version"),
        CheckConstraint("fair_price > 0", name="ck_quote_plans_fair_price"),
        CheckConstraint("reservation_price > 0", name="ck_quote_plans_reservation_price"),
        CheckConstraint("valid_until >= created_at", name="ck_quote_plans_valid_until"),
        ForeignKeyConstraint(
            ["config_version_id", "strategy_id"],
            ["strategy_config_versions.id", "strategy_config_versions.strategy_id"],
            name="fk_quote_plans_config",
            ondelete="RESTRICT",
        ),
        Index("ix_quote_plans_session_created", "session_id", "created_at"),
        Index("ix_quote_plans_config", "config_version_id"),
    )

    plan_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=_uuid)
    strategy_id: Mapped[str] = mapped_column(
        ForeignKey("strategy_instances.strategy_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    session_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    config_version_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    market_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    fair_price: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    reservation_price: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    inventory_size: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    budget_mode: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="planned", server_default="planned")
    valid_until: Mapped[datetime] = mapped_column(UTC_TIMESTAMP, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )


class QuotePlanItemRecord(Base):
    """One durable transition in a quote plan."""

    __tablename__ = "quote_plan_items"
    __table_args__ = (
        _enum_check("decision", QUOTE_DECISIONS, "ck_quote_plan_items_decision"),
        UniqueConstraint("plan_id", "ordinal", name="uq_quote_plan_items_ordinal"),
        CheckConstraint("ordinal >= 0", name="ck_quote_plan_items_ordinal"),
        CheckConstraint("level >= 0", name="ck_quote_plan_items_level"),
        CheckConstraint("side IN ('buy','sell')", name="ck_quote_plan_items_side"),
        CheckConstraint("desired_price IS NULL OR desired_price > 0", name="ck_quote_plan_items_price"),
        CheckConstraint("desired_size IS NULL OR desired_size > 0", name="ck_quote_plan_items_size"),
        Index("ix_quote_plan_items_slot", "symbol", "side", "level"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    plan_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("quote_plans.plan_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[str] = mapped_column(Text, nullable=False)
    level: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    decision: Mapped[str] = mapped_column(Text, nullable=False)
    source_order_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("orders.order_id", ondelete="RESTRICT"), index=True
    )
    target_order_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("orders.order_id", ondelete="RESTRICT"), index=True
    )
    source_cloid: Mapped[str | None] = mapped_column(Text)
    target_cloid: Mapped[str | None] = mapped_column(Text)
    desired_price: Mapped[Decimal | None] = mapped_column(MONEY)
    desired_size: Mapped[Decimal | None] = mapped_column(MONEY)
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )


class QuoteSlotRecord(Base):
    """Current quote-slot projection; live risk owners remain separate facts."""

    __tablename__ = "quote_slots"
    __table_args__ = (
        _enum_check("state", QUOTE_SLOT_STATES, "ck_quote_slots_state"),
        ForeignKeyConstraint(
            ["owner_plan_id", "strategy_id"],
            ["quote_plans.plan_id", "quote_plans.strategy_id"],
            name="fk_quote_slots_owner_plan",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("strategy_id", "symbol", "side", "level", name="uq_quote_slots_key"),
        CheckConstraint("level >= 0", name="ck_quote_slots_level"),
        CheckConstraint("side IN ('buy','sell')", name="ck_quote_slots_side"),
        CheckConstraint("plan_revision >= 0", name="ck_quote_slots_plan_revision"),
        CheckConstraint("revision >= 0", name="ck_quote_slots_revision"),
        Index("ix_quote_slots_owner_order", "owner_order_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    strategy_id: Mapped[str] = mapped_column(
        ForeignKey("strategy_instances.strategy_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[str] = mapped_column(Text, nullable=False)
    level: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    owner_order_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("orders.order_id", ondelete="RESTRICT"))
    owner_plan_id: Mapped[uuid.UUID | None] = mapped_column(index=True)
    plan_revision: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    state: Mapped[str] = mapped_column(Text, nullable=False, default="empty", server_default="empty")
    revision: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    updated_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now(), onupdate=_utcnow
    )


class ExecutionCommandItemRecord(Base):
    """Recoverable child command belonging to a durable parent batch."""

    __tablename__ = "execution_command_items"
    __table_args__ = (
        _enum_check("action_type", ("place", "cancel", "modify"), "ck_execution_command_items_action"),
        _enum_check("status", EXECUTION_ITEM_STATUSES, "ck_execution_command_items_status"),
        UniqueConstraint("command_id", "ordinal", name="uq_execution_command_items_ordinal"),
        UniqueConstraint("id", "command_id", name="uq_execution_command_items_id_command"),
        CheckConstraint("ordinal >= 0", name="ck_execution_command_items_ordinal"),
        CheckConstraint("attempt_count >= 0", name="ck_execution_command_items_attempts"),
        Index("ix_execution_command_items_ready", "status", "available_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    command_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("execution_commands.command_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    plan_item_id: Mapped[int | None] = mapped_column(ForeignKey("quote_plan_items.id", ondelete="RESTRICT"), index=True)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    action_type: Mapped[str] = mapped_column(Text, nullable=False)
    source_order_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("orders.order_id", ondelete="RESTRICT"), index=True
    )
    target_order_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("orders.order_id", ondelete="RESTRICT"), index=True
    )
    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending", server_default="pending")
    resolution: Mapped[str | None] = mapped_column(Text)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    available_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )
    locked_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    locked_by: Mapped[str | None] = mapped_column(Text)
    completed_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now(), onupdate=_utcnow
    )


class ExecutionActionRecord(Base):
    """Immutable fact for one actual child network attempt."""

    __tablename__ = "execution_actions"
    __table_args__ = (
        _enum_check("action_type", ("place", "cancel", "modify"), "ck_execution_actions_action"),
        _enum_check("outcome", EXECUTION_ACTION_OUTCOMES, "ck_execution_actions_outcome"),
        UniqueConstraint("command_item_id", "attempt", name="uq_execution_actions_attempt"),
        CheckConstraint("attempt > 0", name="ck_execution_actions_attempt"),
        CheckConstraint("length(request_hash) = 64", name="ck_execution_actions_request_hash"),
        CheckConstraint("estimated_credit_cost >= 0", name="ck_execution_actions_estimated_cost"),
        CheckConstraint(
            "reconciled_credit_cost IS NULL OR reconciled_credit_cost >= 0",
            name="ck_execution_actions_reconciled_cost",
        ),
        CheckConstraint("responded_at IS NULL OR responded_at >= sent_at", name="ck_execution_actions_time"),
        Index("ix_execution_actions_sent", "sent_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    command_item_id: Mapped[int] = mapped_column(
        ForeignKey("execution_command_items.id", ondelete="RESTRICT"), nullable=False, index=True
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    action_type: Mapped[str] = mapped_column(Text, nullable=False)
    request_hash: Mapped[str] = mapped_column(Text, nullable=False)
    sent_at: Mapped[datetime] = mapped_column(UTC_TIMESTAMP, nullable=False)
    responded_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    outcome: Mapped[str] = mapped_column(Text, nullable=False)
    response_code: Mapped[str | None] = mapped_column(Text)
    estimated_credit_cost: Mapped[int] = mapped_column(BigInteger, nullable=False)
    reconciled_credit_cost: Mapped[int | None] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )


class ActionBudgetScopeRecord(Base):
    """Address-level action quota projection."""

    __tablename__ = "action_budget_scopes"
    __table_args__ = (
        _enum_check("mode", BUDGET_MODES, "ck_action_budget_scopes_mode"),
        CheckConstraint(
            "remote_cap >= 0 AND remote_used >= 0 AND remote_remaining >= 0 AND shadow_used >= 0",
            name="ck_action_budget_scopes_nonnegative",
        ),
        CheckConstraint("emergency_reserve >= 0", name="ck_action_budget_scopes_emergency_reserve"),
        CheckConstraint("remote_used <= remote_cap", name="ck_action_budget_scopes_used_cap"),
        CheckConstraint("remote_remaining = remote_cap - remote_used", name="ck_action_budget_scopes_balance"),
        CheckConstraint("emergency_reserve <= remote_cap", name="ck_action_budget_scopes_reserve_cap"),
        CheckConstraint("quota_owner_address ~ '^0x[0-9a-f]{40}$'", name="ck_action_budget_scopes_address"),
        CheckConstraint("revision >= 0", name="ck_action_budget_scopes_revision"),
    )

    quota_owner_address: Mapped[str] = mapped_column(Text, primary_key=True)
    remote_cap: Mapped[int] = mapped_column(BigInteger, nullable=False)
    remote_used: Mapped[int] = mapped_column(BigInteger, nullable=False)
    remote_remaining: Mapped[int] = mapped_column(BigInteger, nullable=False)
    shadow_used: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    emergency_reserve: Mapped[int] = mapped_column(BigInteger, nullable=False)
    mode: Mapped[str] = mapped_column(Text, nullable=False, default="cancel_only", server_default="cancel_only")
    observed_at: Mapped[datetime] = mapped_column(UTC_TIMESTAMP, nullable=False)
    revision: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    updated_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now(), onupdate=_utcnow
    )


class ActionBudgetAllocationRecord(Base):
    """Strategy allocation inside an address-level action quota."""

    __tablename__ = "action_budget_allocations"
    __table_args__ = (
        _enum_check("status", BUDGET_ALLOCATION_STATUSES, "ck_action_budget_allocations_status"),
        Index(
            "uq_action_budget_allocations_active_scope",
            "quota_owner_address",
            "strategy_id",
            "symbol",
            unique=True,
            postgresql_where="status = 'active'",
        ),
        CheckConstraint("soft_allocation >= 0", name="ck_action_budget_allocations_soft"),
        CheckConstraint("hard_allocation >= soft_allocation", name="ck_action_budget_allocations_hard"),
        CheckConstraint("released_at IS NULL OR released_at >= created_at", name="ck_action_budget_allocations_time"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    quota_owner_address: Mapped[str] = mapped_column(
        ForeignKey("action_budget_scopes.quota_owner_address", ondelete="RESTRICT"), nullable=False, index=True
    )
    strategy_id: Mapped[str] = mapped_column(
        ForeignKey("strategy_instances.strategy_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    soft_allocation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    hard_allocation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="active", server_default="active")
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )
    released_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)


class ActionBudgetEventRecord(Base):
    """Append-only budget debit, correction, reserve, or manual adjustment."""

    __tablename__ = "action_budget_events"
    __table_args__ = (
        CheckConstraint("remote_before IS NULL OR remote_before >= 0", name="ck_action_budget_events_before"),
        CheckConstraint("remote_after IS NULL OR remote_after >= 0", name="ck_action_budget_events_after"),
        Index("ix_action_budget_events_scope_created", "quota_owner_address", "created_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    quota_owner_address: Mapped[str] = mapped_column(
        ForeignKey("action_budget_scopes.quota_owner_address", ondelete="RESTRICT"), nullable=False, index=True
    )
    strategy_id: Mapped[str | None] = mapped_column(
        ForeignKey("strategy_instances.strategy_id", ondelete="RESTRICT"), index=True
    )
    command_item_id: Mapped[int | None] = mapped_column(
        ForeignKey("execution_command_items.id", ondelete="RESTRICT"), index=True
    )
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    estimated_delta: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    remote_before: Mapped[int | None] = mapped_column(BigInteger)
    remote_after: Mapped[int | None] = mapped_column(BigInteger)
    details: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )


class RiskEventRecord(Base):
    """Immutable decision produced for every placement command."""

    __tablename__ = "risk_events"
    __table_args__ = (Index("ix_risk_events_account_created", "sub_account", "created_at"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    risk_event_id: Mapped[uuid.UUID] = mapped_column(unique=True, nullable=False, default=_uuid)
    command_id: Mapped[uuid.UUID] = mapped_column(nullable=False, index=True)
    order_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("orders.order_id", ondelete="RESTRICT"), index=True)
    sub_account: Mapped[str | None] = mapped_column(Text, index=True)
    strategy_id: Mapped[str | None] = mapped_column(Text, index=True)
    passed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    reason_code: Mapped[str | None] = mapped_column(Text)
    reason: Mapped[str | None] = mapped_column(Text)
    checked_limits: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list, server_default="[]")
    snapshot: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    duration_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )


class RiskReservationRecord(Base):
    """Risk exposure reserved between command acceptance and terminal outcome."""

    __tablename__ = "risk_reservations"
    __table_args__ = (
        _enum_check("status", RESERVATION_STATUSES, "ck_risk_reservations_status"),
        _enum_check("risk_owner_type", RISK_OWNER_TYPES, "ck_risk_reservations_owner_type"),
        CheckConstraint("reserved_notional >= 0", name="ck_risk_reservations_notional"),
        CheckConstraint("reserved_size >= 0", name="ck_risk_reservations_size"),
        ForeignKeyConstraint(
            ["command_item_id", "command_id"],
            ["execution_command_items.id", "execution_command_items.command_id"],
            name="fk_risk_reservations_command_item",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("command_id", "risk_owner_key", name="uq_risk_reservations_command_owner"),
        Index("ix_risk_reservations_active", "sub_account", "expires_at", postgresql_where="status = 'active'"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    reservation_id: Mapped[uuid.UUID] = mapped_column(unique=True, nullable=False, default=_uuid)
    command_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    command_item_id: Mapped[int | None] = mapped_column(BigInteger, index=True)
    risk_owner_type: Mapped[str] = mapped_column(Text, nullable=False, default="legacy", server_default="legacy")
    risk_owner_key: Mapped[str] = mapped_column(
        Text, nullable=False, default=lambda: str(_uuid()), server_default=text("gen_random_uuid()::text")
    )
    order_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("orders.order_id", ondelete="RESTRICT"), index=True)
    sub_account: Mapped[str | None] = mapped_column(Text, index=True)
    strategy_id: Mapped[str | None] = mapped_column(Text, index=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[str] = mapped_column(Text, nullable=False)
    reduce_only: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    reserved_size: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    reserved_notional: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="active", server_default="active")
    expires_at: Mapped[datetime] = mapped_column(UTC_TIMESTAMP, nullable=False)
    released_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now(), onupdate=_utcnow
    )


class ReconciliationRunRecord(Base):
    """A complete reconciliation attempt; failure is explicit, never an empty success."""

    __tablename__ = "reconciliation_runs"
    __table_args__ = (
        _enum_check("status", RECONCILIATION_STATUSES, "ck_reconciliation_runs_status"),
        Index("ix_reconciliation_runs_scope_started", "sub_account", "started_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    run_id: Mapped[uuid.UUID] = mapped_column(unique=True, nullable=False, default=_uuid)
    sub_account: Mapped[str | None] = mapped_column(Text, index=True)
    trigger: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="running", server_default="running")
    required_queries: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list, server_default="[]")
    completed_queries: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list, server_default="[]")
    error_code: Mapped[str | None] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)


class ReconciliationDiffRecord(Base):
    """An individual local/exchange difference found by reconciliation."""

    __tablename__ = "reconciliation_diffs"
    __table_args__ = (Index("ix_reconciliation_diffs_run_severity", "run_id", "severity"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    diff_id: Mapped[uuid.UUID] = mapped_column(unique=True, nullable=False, default=_uuid)
    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("reconciliation_runs.run_id", ondelete="CASCADE"), nullable=False, index=True
    )
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    entity_key: Mapped[str] = mapped_column(Text, nullable=False)
    difference_type: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(Text, nullable=False)
    local_value: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    exchange_value: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    resolution: Mapped[str | None] = mapped_column(Text)
    resolved: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    resolved_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )


class ExecutionCommandRecord(Base):
    """Durable command queue consumed by the signed action executor."""

    __tablename__ = "execution_commands"
    __table_args__ = (
        _enum_check("status", COMMAND_STATUSES, "ck_execution_commands_status"),
        UniqueConstraint("actor_id", "idempotency_key", name="uq_execution_commands_actor_idempotency"),
        CheckConstraint("attempt_count >= 0", name="ck_execution_commands_attempt_count"),
        Index("ix_execution_commands_ready", "priority", "created_at", postgresql_where="status = 'pending'"),
        Index("ix_execution_commands_status_updated", "status", "updated_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    command_id: Mapped[uuid.UUID] = mapped_column(unique=True, nullable=False, default=_uuid)
    order_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("orders.order_id", ondelete="RESTRICT"), index=True)
    command_type: Mapped[str] = mapped_column(Text, nullable=False)
    actor_type: Mapped[str] = mapped_column(Text, nullable=False)
    actor_id: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100, server_default="100")
    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending", server_default="pending")
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    available_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )
    locked_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    locked_by: Mapped[str | None] = mapped_column(Text)
    completed_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    last_error_code: Mapped[str | None] = mapped_column(Text)
    last_error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now(), onupdate=_utcnow
    )


class InboxEventRecord(Base):
    """External event idempotency ledger."""

    __tablename__ = "inbox_events"
    __table_args__ = (
        UniqueConstraint("source", "external_event_id", name="uq_inbox_events_source_external"),
        Index("ix_inbox_events_received", "received_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(unique=True, nullable=False, default=_uuid)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    external_event_id: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    payload_hash: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)


class OutboxEventRecord(Base):
    """Transactionally committed event stream backing EventBus and SSE."""

    __tablename__ = "outbox_events"
    __table_args__ = (
        Index("ix_outbox_events_unpublished", "sequence", postgresql_where="published_at IS NULL"),
        Index(
            "ix_outbox_events_dispatch",
            "claimed_at",
            "sequence",
            postgresql_where="published_at IS NULL",
        ),
        Index("ix_outbox_events_aggregate", "aggregate_type", "aggregate_id", "sequence"),
        CheckConstraint("publish_attempts >= 0", name="ck_outbox_events_publish_attempts"),
    )

    sequence: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(unique=True, nullable=False, default=_uuid)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    aggregate_type: Mapped[str] = mapped_column(Text, nullable=False)
    aggregate_id: Mapped[str] = mapped_column(Text, nullable=False)
    aggregate_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    correlation_id: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    claimed_at: Mapped[datetime | None] = mapped_column(UTC_TIMESTAMP)
    claimed_by: Mapped[str | None] = mapped_column(Text)
    publish_attempts: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    last_publish_error: Mapped[str | None] = mapped_column(Text)


class LedgerEntryRecord(Base):
    """Immutable double-entry-compatible account fact linked to an exchange fill."""

    __tablename__ = "ledger_entries"
    __table_args__ = (
        UniqueConstraint("fill_id", "entry_type", name="uq_ledger_entries_fill_type"),
        Index("ix_ledger_entries_account_occurred", "sub_account", "occurred_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    entry_id: Mapped[uuid.UUID] = mapped_column(unique=True, nullable=False, default=_uuid)
    fill_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("fills.fill_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    entry_type: Mapped[str] = mapped_column(Text, nullable=False)
    asset: Mapped[str] = mapped_column(Text, nullable=False, default="USDC", server_default="USDC")
    amount: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    sub_account: Mapped[str | None] = mapped_column(Text, index=True)
    strategy_id: Mapped[str | None] = mapped_column(Text, index=True)
    occurred_at: Mapped[datetime] = mapped_column(UTC_TIMESTAMP, nullable=False)
    metadata_: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, default=dict, server_default="{}"
    )
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )


class ExchangeSyncCursorRecord(Base):
    """Durable high-water mark for incremental exchange history recovery."""

    __tablename__ = "exchange_sync_cursors"
    __table_args__ = (UniqueConstraint("source", "sub_account", "stream", name="uq_exchange_sync_cursor_scope"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    sub_account: Mapped[str] = mapped_column(Text, nullable=False)
    stream: Mapped[str] = mapped_column(Text, nullable=False)
    last_exchange_timestamp_ms: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    last_external_event_id: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now(), onupdate=_utcnow
    )


class ApiAuditRecord(Base):
    """Immutable audit trail for authenticated API actions."""

    __tablename__ = "api_audit"
    __table_args__ = (Index("ix_api_audit_actor_created", "actor_id", "created_at"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    audit_id: Mapped[uuid.UUID] = mapped_column(unique=True, nullable=False, default=_uuid)
    request_id: Mapped[uuid.UUID] = mapped_column(nullable=False, index=True)
    actor_type: Mapped[str] = mapped_column(Text, nullable=False)
    actor_id: Mapped[str] = mapped_column(Text, nullable=False)
    role: Mapped[str] = mapped_column(Text, nullable=False)
    action: Mapped[str] = mapped_column(Text, nullable=False)
    resource_type: Mapped[str | None] = mapped_column(Text)
    resource_id: Mapped[str | None] = mapped_column(Text)
    outcome: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str | None] = mapped_column(Text)
    ip_address: Mapped[str | None] = mapped_column(INET)
    user_agent: Mapped[str | None] = mapped_column(Text)
    details: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(
        UTC_TIMESTAMP, nullable=False, default=_utcnow, server_default=func.now()
    )


# Phase 2 compatibility tables.  New trading code must use positions/current
# ledger projections above; these remain mapped until the legacy reader is removed.
class PositionSnapshot(Base):
    __tablename__ = "position_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    size: Mapped[float] = mapped_column(nullable=False)
    entry_price: Mapped[float | None] = mapped_column()
    mark_price: Mapped[float | None] = mapped_column()
    unrealized_pnl: Mapped[float | None] = mapped_column()
    leverage: Mapped[int] = mapped_column(Integer, default=1)
    strategy_id: Mapped[str | None] = mapped_column(String(50), index=True)
    sub_account: Mapped[str | None] = mapped_column(String(50))
    snapshot_at: Mapped[datetime] = mapped_column(UTC_TIMESTAMP, server_default=func.now())


class PnLRecord(Base):
    __tablename__ = "pnl"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    realized_pnl: Mapped[float] = mapped_column(nullable=False, default=0.0)
    fees: Mapped[float] = mapped_column(nullable=False, default=0.0)
    funding: Mapped[float] = mapped_column(nullable=False, default=0.0)
    trade_count: Mapped[int] = mapped_column(Integer, default=0)
    period_start: Mapped[datetime] = mapped_column(UTC_TIMESTAMP, nullable=False)
    period_end: Mapped[datetime] = mapped_column(UTC_TIMESTAMP, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTC_TIMESTAMP, server_default=func.now())


def create_pg_session_factory(settings: PostgresSettings) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    """Create the async engine and session factory without mutating schema."""

    engine = create_async_engine(settings.url, pool_size=settings.pool_size, pool_pre_ping=True, echo=False)
    return engine, async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


async def create_pg_engine(settings: PostgresSettings) -> async_sessionmaker[AsyncSession]:
    """Compatibility factory; schema must already be at the Alembic head."""

    _, session_factory = create_pg_session_factory(settings)
    return session_factory


async def verify_postgres_schema(engine: AsyncEngine) -> None:
    """Fail unless Postgres is reachable and its Alembic revision is at head."""

    from alembic.config import Config
    from alembic.script import ScriptDirectory

    expected_heads = set(ScriptDirectory.from_config(Config("alembic.ini")).get_heads())
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT version_num FROM alembic_version"))
        current_heads = {str(row[0]) for row in result}
    if current_heads != expected_heads:
        raise RuntimeError(
            f"postgres_schema_not_at_head: current={sorted(current_heads)}, expected={sorted(expected_heads)}"
        )


class Repository[ModelT: Base]:
    """Small async repository foundation scoped to one UoW session."""

    def __init__(self, session: AsyncSession, model: type[ModelT]) -> None:
        self._session = session
        self._model = model

    async def get(self, identity: Any) -> ModelT | None:
        return await self._session.get(self._model, identity)

    def add(self, entity: ModelT) -> None:
        self._session.add(entity)

    async def flush(self) -> None:
        await self._session.flush()


class OrderRepository(Repository[OrderRecord]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, OrderRecord)

    async def get_by_order_id(self, order_id: uuid.UUID, *, for_update: bool = False) -> OrderRecord | None:
        statement = select(OrderRecord).where(OrderRecord.order_id == order_id)
        if for_update:
            statement = statement.with_for_update()
        return (await self._session.execute(statement)).scalar_one_or_none()

    async def get_by_cloid(self, cloid: str, *, for_update: bool = False) -> OrderRecord | None:
        statement = select(OrderRecord).where(OrderRecord.cloid == cloid)
        if for_update:
            statement = statement.with_for_update()
        return (await self._session.execute(statement)).scalar_one_or_none()

    async def list_open(self, sub_account: str | None = None) -> Sequence[OrderRecord]:
        statement = select(OrderRecord).where(
            OrderRecord.status.in_(
                {
                    "pending",
                    "submitted",
                    "submit_unknown",
                    "acknowledged",
                    "partial_fill",
                    "cancel_pending",
                    "cancel_unknown",
                }
            )
        )
        if sub_account is None:
            statement = statement.where(OrderRecord.sub_account.is_(None))
        else:
            statement = statement.where(OrderRecord.sub_account == sub_account)
        return (await self._session.execute(statement.order_by(OrderRecord.created_at))).scalars().all()

    async def list_all_open(self) -> Sequence[OrderRecord]:
        statement = (
            select(OrderRecord)
            .where(
                OrderRecord.status.in_(
                    {
                        "pending",
                        "submitted",
                        "submit_unknown",
                        "acknowledged",
                        "partial_fill",
                        "cancel_pending",
                        "cancel_unknown",
                    }
                )
            )
            .order_by(OrderRecord.created_at)
        )
        return (await self._session.execute(statement)).scalars().all()


class ExecutionCommandRepository(Repository[ExecutionCommandRecord]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, ExecutionCommandRecord)

    async def claim_ready(self, worker_id: str, limit: int = 1) -> Sequence[ExecutionCommandRecord]:
        """Claim ready commands without blocking another executor."""

        statement = (
            select(ExecutionCommandRecord)
            .where(
                ExecutionCommandRecord.status == "pending",
                ExecutionCommandRecord.available_at <= func.now(),
            )
            .order_by(ExecutionCommandRecord.priority, ExecutionCommandRecord.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        commands = (await self._session.execute(statement)).scalars().all()
        now = _utcnow()
        for command in commands:
            command.status = "processing"
            command.locked_at = now
            command.locked_by = worker_id
        return commands


class OutboxRepository(Repository[OutboxEventRecord]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, OutboxEventRecord)

    async def list_unpublished(self, after_sequence: int = 0, limit: int = 100) -> Sequence[OutboxEventRecord]:
        statement = (
            select(OutboxEventRecord)
            .where(OutboxEventRecord.sequence > after_sequence, OutboxEventRecord.published_at.is_(None))
            .order_by(OutboxEventRecord.sequence)
            .limit(limit)
        )
        return (await self._session.execute(statement)).scalars().all()


class PostgresUnitOfWork:
    """Explicit transaction boundary exposing repositories on one session."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self.session: AsyncSession | None = None
        self.orders: OrderRepository
        self.commands: ExecutionCommandRepository
        self.outbox: OutboxRepository

    async def __aenter__(self) -> PostgresUnitOfWork:
        self.session = self._session_factory()
        self.orders = OrderRepository(self.session)
        self.commands = ExecutionCommandRepository(self.session)
        self.outbox = OutboxRepository(self.session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self.session is None:
            return
        try:
            if exc_type is not None:
                await self.session.rollback()
        finally:
            await self.session.close()

    async def commit(self) -> None:
        if self.session is None:
            raise RuntimeError("PostgresUnitOfWork is not active")
        await self.session.commit()

    async def rollback(self) -> None:
        if self.session is None:
            raise RuntimeError("PostgresUnitOfWork is not active")
        await self.session.rollback()


class PostgresProjectionReader:
    """Read-only access to the durable API projections."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def list_orders(self, status: str, limit: int) -> Sequence[OrderRecord]:
        statement = select(OrderRecord)
        if status == "active":
            statement = statement.where(
                OrderRecord.status.in_(
                    (
                        "pending",
                        "submitted",
                        "submit_unknown",
                        "acknowledged",
                        "partial_fill",
                        "cancel_pending",
                        "cancel_unknown",
                    )
                )
            )
        elif status == "terminal":
            statement = statement.where(OrderRecord.status.in_(("filled", "cancelled", "rejected", "expired")))
        else:
            requested = {item for item in status.split(",") if item in ORDER_STATUSES}
            statement = statement.where(OrderRecord.status.in_(requested))
        async with self._session_factory() as session:
            return (
                (await session.execute(statement.order_by(OrderRecord.created_at.desc()).limit(limit))).scalars().all()
            )

    async def list_positions(self) -> Sequence[PositionRecord]:
        async with self._session_factory() as session:
            return (
                (
                    await session.execute(
                        select(PositionRecord).where(PositionRecord.size != 0).order_by(PositionRecord.symbol)
                    )
                )
                .scalars()
                .all()
            )

    async def get_account(self) -> AccountStateRecord | None:
        async with self._session_factory() as session:
            return (
                await session.execute(
                    select(AccountStateRecord).order_by(AccountStateRecord.updated_at.desc()).limit(1)
                )
            ).scalar_one_or_none()

    async def get_account_metrics(self) -> dict[str, Decimal | int]:
        async with self._session_factory() as session:
            fill_count = (await session.execute(select(func.count(FillRecord.id)))).scalar_one()
            total_fees = (
                await session.execute(select(func.coalesce(func.sum(FillRecord.fee), Decimal(0))))
            ).scalar_one()
            position_count = (
                await session.execute(select(func.count(PositionRecord.id)).where(PositionRecord.size != 0))
            ).scalar_one()
            max_leverage = (
                await session.execute(
                    select(func.coalesce(func.max(PositionRecord.leverage), 0)).where(PositionRecord.size != 0)
                )
            ).scalar_one()
        return {
            "fill_count": int(fill_count),
            "total_fees": Decimal(total_fees),
            "position_count": int(position_count),
            "leverage": int(max_leverage),
        }


class PostgresReconciliationStore:
    """Persist reconciliation attempts and exchange-authoritative projections."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], sub_account: str | None) -> None:
        self._session_factory = session_factory
        self._sub_account = sub_account.lower() if sub_account else None

    async def start(self, trigger: str = "scheduled") -> uuid.UUID:
        run_id = _uuid()
        async with self._session_factory() as session, session.begin():
            session.add(
                ReconciliationRunRecord(
                    run_id=run_id,
                    sub_account=self._sub_account,
                    trigger=trigger,
                    status="running",
                    required_queries=["open_orders", "historical_order_status", "positions", "account"],
                )
            )
        return run_id

    async def finish(
        self,
        run_id: uuid.UUID,
        *,
        success: bool,
        errors: Sequence[str],
        diffs: Sequence[dict[str, Any]],
        exchange_positions: dict[str, dict[str, Any]],
        exchange_account: Any | None,
    ) -> None:
        now = _utcnow()
        async with self._session_factory() as session, session.begin():
            run = (
                await session.execute(
                    select(ReconciliationRunRecord).where(ReconciliationRunRecord.run_id == run_id).with_for_update()
                )
            ).scalar_one()
            run.status = "succeeded" if success else "failed"
            run.completed_queries = (
                ["open_orders", "historical_order_status", "positions", "account"] if success else []
            )
            run.error_code = "reconciliation_failed" if errors else None
            run.error_message = "; ".join(errors) if errors else None
            run.finished_at = now
            for diff in diffs:
                session.add(
                    ReconciliationDiffRecord(
                        run_id=run_id,
                        entity_type=str(diff["entity_type"]),
                        entity_key=str(diff["entity_key"]),
                        difference_type=str(diff["difference_type"]),
                        severity=str(diff.get("severity", "warning")),
                        local_value=diff.get("local_value"),
                        exchange_value=diff.get("exchange_value"),
                        resolution="exchange_projection_applied" if success else None,
                        resolved=success,
                        resolved_at=now if success else None,
                    )
                )
            session.add(
                OutboxEventRecord(
                    event_type="reconciliation.completed",
                    aggregate_type="reconciliation",
                    aggregate_id=str(run_id),
                    aggregate_revision=1,
                    correlation_id=str(run_id),
                    payload={
                        "run_id": str(run_id),
                        "success": success,
                        "errors": list(errors),
                        "diff_count": len(diffs),
                    },
                    occurred_at=now,
                )
            )
            if not success or exchange_account is None:
                return

            active_symbols = set(exchange_positions)
            for symbol, raw in exchange_positions.items():
                leverage_raw = raw.get("leverage", {})
                leverage = int(float(leverage_raw.get("value", 1))) if isinstance(leverage_raw, dict) else 1
                values = {
                    "sub_account": self._sub_account,
                    "symbol": symbol,
                    "size": _to_decimal(raw.get("szi", 0)),
                    "entry_price": _nullable_decimal(raw.get("entryPx")),
                    "mark_price": _nullable_decimal(raw.get("markPx")),
                    "unrealized_pnl": _to_decimal(raw.get("unrealizedPnl", 0)),
                    "realized_pnl": Decimal(0),
                    "leverage": max(1, leverage),
                    "liquidation_price": _nullable_decimal(raw.get("liquidationPx")),
                    "exchange_updated_at": now,
                    "revision": 1,
                }
                statement = pg_insert(PositionRecord).values(position_id=_uuid(), **values)
                await session.execute(
                    statement.on_conflict_do_update(
                        index_elements=["sub_account", "symbol"],
                        set_={
                            **values,
                            "revision": PositionRecord.revision + 1,
                            "updated_at": now,
                        },
                    )
                )
            stale = select(PositionRecord).where(
                PositionRecord.sub_account == self._sub_account,
                PositionRecord.size != 0,
            )
            if active_symbols:
                stale = stale.where(PositionRecord.symbol.not_in(active_symbols))
            for position in (await session.execute(stale.with_for_update())).scalars():
                position.size = Decimal(0)
                position.entry_price = None
                position.unrealized_pnl = Decimal(0)
                position.exchange_updated_at = now
                position.revision += 1

            account_values = {
                "equity": _to_decimal(exchange_account.equity),
                "available_balance": _to_decimal(exchange_account.available_balance),
                "total_margin_used": _to_decimal(exchange_account.total_margin_used),
                "total_unrealized_pnl": _to_decimal(exchange_account.total_unrealized_pnl),
                "peak_equity": _to_decimal(exchange_account.peak_equity),
                "exchange_updated_at": now,
                "reconciled_at": now,
                "revision": 1,
            }
            account_statement = pg_insert(AccountStateRecord).values(sub_account=self._sub_account, **account_values)
            await session.execute(
                account_statement.on_conflict_do_update(
                    index_elements=["sub_account"],
                    set_={
                        **account_values,
                        "revision": AccountStateRecord.revision + 1,
                        "updated_at": now,
                    },
                )
            )


def _to_decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _nullable_decimal(value: Any) -> Decimal | None:
    return None if value is None or value == "" else _to_decimal(value)


class PostgresDurableOrderStore:
    """Transactional implementation of the execution durable-order boundary."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        risk_limits: RiskLimits | None = None,
        account_stale_seconds: float = 360.0,
        reservation_ttl_seconds: int = 86400,
    ) -> None:
        self._session_factory = session_factory
        self._risk_limits = risk_limits
        self._account_stale_seconds = account_stale_seconds
        self._reservation_ttl_seconds = reservation_ttl_seconds

    async def persist_placement(
        self,
        order: Order,
        risk_result: RiskCheckResult,
        *,
        command_id: uuid.UUID,
        dispatch: bool,
        reference_price: float | None = None,
        price_observed_at: datetime | None = None,
    ) -> RiskCheckResult | None:
        order_id = _uuid()
        revision = 1
        async with self._session_factory() as session, session.begin():
            effective_risk = risk_result
            if dispatch and risk_result.passed and self._risk_limits is not None:
                effective_risk = await self._check_and_lock_risk_scope(session, order, reference_price)
                if not effective_risk.passed:
                    dispatch = False
                    order.status = OrderStatus.REJECTED
                    order.error_message = effective_risk.reason

            event_type = "submitted" if dispatch else "rejected"
            command_status = "pending" if dispatch else "failed"
            payload = self._order_payload(order)
            session.add(
                OrderRecord(
                    order_id=order_id,
                    command_id=command_id,
                    cloid=str(order.cloid),
                    symbol=str(order.symbol),
                    side=str(order.side),
                    order_type=str(order.order_type),
                    time_in_force=str(order.time_in_force),
                    size=Decimal(str(order.size)),
                    price=Decimal(str(order.price)) if order.price is not None else None,
                    status=str(order.status),
                    strategy_id=str(order.strategy_id) if order.strategy_id else None,
                    sub_account=str(order.sub_account) if order.sub_account else None,
                    reduce_only=order.reduce_only,
                    error_message=order.error_message,
                    submitted_at=order.submitted_at,
                    revision=revision,
                )
            )
            session.add(
                RiskEventRecord(
                    command_id=command_id,
                    order_id=order_id,
                    sub_account=str(order.sub_account) if order.sub_account else None,
                    strategy_id=str(order.strategy_id) if order.strategy_id else None,
                    passed=effective_risk.passed,
                    reason_code=effective_risk.reason,
                    reason=effective_risk.reason,
                    checked_limits=list(effective_risk.checked_limits),
                    snapshot={
                        "reservation_included": dispatch,
                        "reference_price": reference_price,
                        "price_observed_at": price_observed_at.isoformat() if price_observed_at else None,
                    },
                    duration_ms=0,
                )
            )
            session.add(
                ExecutionCommandRecord(
                    command_id=command_id,
                    order_id=order_id,
                    command_type="place_order",
                    actor_type="strategy" if order.strategy_id else "system",
                    actor_id=str(order.strategy_id or "execution_engine"),
                    idempotency_key=str(order.cloid),
                    status=command_status,
                    payload=payload,
                    completed_at=_utcnow() if not dispatch else None,
                    last_error_code=effective_risk.reason if not dispatch else None,
                    last_error_message=effective_risk.reason if not dispatch else None,
                )
            )
            if dispatch:
                locked_reference_price = await self._reference_price(session, order, reference_price)
                session.add(
                    RiskReservationRecord(
                        command_id=command_id,
                        order_id=order_id,
                        sub_account=str(order.sub_account) if order.sub_account else None,
                        strategy_id=str(order.strategy_id) if order.strategy_id else None,
                        symbol=str(order.symbol),
                        side=str(order.side),
                        reduce_only=order.reduce_only,
                        reserved_size=Decimal(str(order.size)),
                        reserved_notional=Decimal(str(order.size)) * locked_reference_price,
                        expires_at=_utcnow() + timedelta(seconds=self._reservation_ttl_seconds),
                    )
                )
            self._append_event_rows(
                session,
                order,
                order_id=order_id,
                revision=revision,
                event_type=event_type,
                payload=payload,
            )
        return effective_risk

    async def _reference_price(
        self, session: AsyncSession, order: Order, supplied_reference_price: float | None = None
    ) -> Decimal:
        if order.price is not None and Decimal(str(order.price)) > 0:
            return Decimal(str(order.price))
        if supplied_reference_price is not None and supplied_reference_price > 0:
            return Decimal(str(supplied_reference_price))
        statement = select(PositionRecord.mark_price).where(PositionRecord.symbol == str(order.symbol))
        if order.sub_account is None:
            statement = statement.where(PositionRecord.sub_account.is_(None))
        else:
            statement = statement.where(PositionRecord.sub_account == str(order.sub_account))
        mark = (await session.execute(statement.limit(1))).scalar_one_or_none()
        return mark or Decimal(0)

    async def _check_and_lock_risk_scope(
        self, session: AsyncSession, order: Order, supplied_reference_price: float | None
    ) -> RiskCheckResult:
        """Serialize exposure admission for one account and include active reservations."""
        limits = self._risk_limits
        assert limits is not None
        account_statement = select(AccountStateRecord).with_for_update()
        if order.sub_account is None:
            account_statement = account_statement.where(AccountStateRecord.sub_account.is_(None))
        else:
            account_statement = account_statement.where(AccountStateRecord.sub_account == str(order.sub_account))
        account = (await session.execute(account_statement)).scalar_one_or_none()
        checked = ["postgres_account_scope_locked"]
        if account is None:
            return RiskCheckResult(False, "account_state_not_available", checked)
        if (_utcnow() - account.exchange_updated_at).total_seconds() > self._account_stale_seconds:
            return RiskCheckResult(False, "account_state_stale", checked)

        positions_statement = select(PositionRecord).with_for_update()
        reservations_statement = (
            select(RiskReservationRecord).where(RiskReservationRecord.status == "active").with_for_update()
        )
        if order.sub_account is None:
            positions_statement = positions_statement.where(PositionRecord.sub_account.is_(None))
            reservations_statement = reservations_statement.where(RiskReservationRecord.sub_account.is_(None))
        else:
            scope = str(order.sub_account)
            positions_statement = positions_statement.where(PositionRecord.sub_account == scope)
            reservations_statement = reservations_statement.where(RiskReservationRecord.sub_account == scope)
        positions = (await session.execute(positions_statement)).scalars().all()
        expired_statement = (
            select(RiskReservationRecord)
            .join(OrderRecord, OrderRecord.order_id == RiskReservationRecord.order_id)
            .where(
                RiskReservationRecord.status == "active",
                RiskReservationRecord.expires_at <= func.now(),
                OrderRecord.status.in_(("filled", "cancelled", "rejected", "expired")),
            )
            .with_for_update()
        )
        if order.sub_account is None:
            expired_statement = expired_statement.where(RiskReservationRecord.sub_account.is_(None))
        else:
            expired_statement = expired_statement.where(RiskReservationRecord.sub_account == str(order.sub_account))
        for expired in (await session.execute(expired_statement)).scalars().all():
            expired.status = "expired"
            expired.released_at = _utcnow()
        reservations = (await session.execute(reservations_statement)).scalars().all()
        checked.append("active_reservations_included")

        reference_price = await self._reference_price(session, order, supplied_reference_price)
        if reference_price <= 0:
            return RiskCheckResult(False, "market_price_not_available", checked)
        symbol = str(order.symbol)
        existing = next((position for position in positions if position.symbol == symbol), None)
        existing_size = existing.size if existing is not None else Decimal(0)
        requested_delta = Decimal(str(order.size)) if order.side == Side.BUY else -Decimal(str(order.size))
        if order.reduce_only:
            resulting_size = existing_size + requested_delta
            reduces = existing_size != 0 and abs(resulting_size) < abs(existing_size)
            no_flip = resulting_size == 0 or (resulting_size > 0) == (existing_size > 0)
            if not reduces or not no_flip:
                return RiskCheckResult(False, "invalid_reduce_only_order", checked)

        opening_reservations = [
            reservation for reservation in reservations if not getattr(reservation, "reduce_only", False)
        ]
        symbol_buys = sum(
            (
                reservation.reserved_size
                for reservation in opening_reservations
                if reservation.symbol == symbol and reservation.side == "buy"
            ),
            start=Decimal(0),
        )
        symbol_sells = sum(
            (
                reservation.reserved_size
                for reservation in opening_reservations
                if reservation.symbol == symbol and reservation.side == "sell"
            ),
            start=Decimal(0),
        )
        if not order.reduce_only:
            if order.side == Side.BUY:
                symbol_buys += Decimal(str(order.size))
            else:
                symbol_sells += Decimal(str(order.size))
        worst_symbol_size = max(abs(existing_size + symbol_buys), abs(existing_size - symbol_sells))

        equity = account.equity
        if equity <= 0:
            return RiskCheckResult(False, "non_positive_equity", checked)
        max_symbol_notional = equity * Decimal(str(limits.max_position_pct))
        if not order.reduce_only and worst_symbol_size * reference_price > max_symbol_notional:
            return RiskCheckResult(False, "position_limit_exceeded_with_reservations", checked)

        positions_by_symbol = {position.symbol: position for position in positions}
        exposure_symbols = (
            set(positions_by_symbol) | {reservation.symbol for reservation in opening_reservations} | {symbol}
        )
        resulting_notional = Decimal(0)
        for exposure_symbol in exposure_symbols:
            position = positions_by_symbol.get(exposure_symbol)
            position_size = position.size if position is not None else Decimal(0)
            buys = sum(
                (
                    reservation.reserved_size
                    for reservation in opening_reservations
                    if reservation.symbol == exposure_symbol and reservation.side == "buy"
                ),
                start=Decimal(0),
            )
            sells = sum(
                (
                    reservation.reserved_size
                    for reservation in opening_reservations
                    if reservation.symbol == exposure_symbol and reservation.side == "sell"
                ),
                start=Decimal(0),
            )
            if exposure_symbol == symbol and not order.reduce_only:
                if order.side == Side.BUY:
                    buys += Decimal(str(order.size))
                else:
                    sells += Decimal(str(order.size))
            worst_size = max(abs(position_size + buys), abs(position_size - sells))
            price_candidates = [position.mark_price or Decimal(0)] if position is not None else []
            price_candidates.extend(
                reservation.reserved_notional / reservation.reserved_size
                for reservation in opening_reservations
                if reservation.symbol == exposure_symbol and reservation.reserved_size > 0
            )
            if exposure_symbol == symbol:
                price_candidates.append(reference_price)
            exposure_price = max(price_candidates, default=Decimal(0))
            if worst_size > 0 and exposure_price <= 0:
                return RiskCheckResult(False, "market_price_not_available", checked)
            resulting_notional += worst_size * exposure_price
        if not order.reduce_only and resulting_notional / equity > Decimal(str(limits.max_leverage)):
            return RiskCheckResult(False, "leverage_exceeded_with_reservations", checked)
        checked.extend(["max_position", "max_leverage"])
        return RiskCheckResult(True, checked_limits=checked)

    async def persist_transition(
        self,
        order: Order,
        event_type: str,
        *,
        command_id: uuid.UUID | None = None,
        command_status: str | None = None,
    ) -> None:
        async with self._session_factory() as session, session.begin():
            record = (
                await session.execute(
                    select(OrderRecord).where(OrderRecord.cloid == str(order.cloid)).with_for_update()
                )
            ).scalar_one()
            self._update_order_record(record, order)
            record.revision += 1
            if command_id is not None and command_status is not None:
                command = (
                    await session.execute(
                        select(ExecutionCommandRecord)
                        .where(ExecutionCommandRecord.command_id == command_id)
                        .with_for_update()
                    )
                ).scalar_one()
                command.status = command_status
                command.completed_at = _utcnow() if command_status in {"succeeded", "failed", "cancelled"} else None
                command.locked_at = None
                command.locked_by = None
                command.last_error_message = order.error_message
            reservation = (
                await session.execute(
                    select(RiskReservationRecord)
                    .where(RiskReservationRecord.order_id == record.order_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if reservation is not None and reservation.status == "active":
                if order.status == OrderStatus.FILLED:
                    reservation.status = "consumed"
                    reservation.released_at = _utcnow()
                elif order.status in {OrderStatus.REJECTED, OrderStatus.CANCELLED, OrderStatus.EXPIRED}:
                    reservation.status = "released"
                    reservation.released_at = _utcnow()
            payload = self._order_payload(order)
            self._append_event_rows(
                session,
                order,
                order_id=record.order_id,
                revision=record.revision,
                event_type=event_type,
                payload=payload,
            )
            # The synchronous placement response updates only the order
            # projection. Immutable fill/account facts are accepted solely via
            # ExchangeEventIngestor, which owns exchange fill idempotency.

    async def persist_cancel_requested(self, order: Order, *, command_id: uuid.UUID) -> None:
        async with self._session_factory() as session, session.begin():
            record = (
                await session.execute(
                    select(OrderRecord).where(OrderRecord.cloid == str(order.cloid)).with_for_update()
                )
            ).scalar_one()
            record.revision += 1
            payload = {"cloid": str(order.cloid), "symbol": str(order.symbol)}
            session.add(
                ExecutionCommandRecord(
                    command_id=command_id,
                    order_id=record.order_id,
                    command_type="cancel_order",
                    actor_type="system",
                    actor_id="execution_engine",
                    idempotency_key=f"{order.cloid}:cancel:{command_id}",
                    priority=0,
                    # The request coroutine owns the first attempt. If it
                    # crashes, the durable worker recovers this processing
                    # lease and resolves/retries the idempotent cancel.
                    status="processing",
                    payload=payload,
                    attempt_count=1,
                    locked_at=_utcnow(),
                    locked_by="request",
                )
            )
            self._append_event_rows(
                session,
                order,
                order_id=record.order_id,
                revision=record.revision,
                event_type="cancel_requested",
                payload=payload,
            )

    async def persist_reconciled_order(self, order: Order) -> None:
        """Upsert an exchange-discovered order before any cancel side effect."""
        async with self._session_factory() as session, session.begin():
            record = (
                await session.execute(
                    select(OrderRecord).where(OrderRecord.cloid == str(order.cloid)).with_for_update()
                )
            ).scalar_one_or_none()
            if record is None:
                record = OrderRecord(
                    order_id=_uuid(),
                    cloid=str(order.cloid),
                    exchange_oid=str(order.exchange_oid) if order.exchange_oid else None,
                    symbol=str(order.symbol),
                    side=str(order.side),
                    order_type=str(order.order_type),
                    time_in_force=str(order.time_in_force),
                    size=Decimal(str(order.size)),
                    price=Decimal(str(order.price)) if order.price is not None else None,
                    status=str(order.status),
                    strategy_id=str(order.strategy_id) if order.strategy_id else None,
                    sub_account=str(order.sub_account) if order.sub_account else None,
                    reduce_only=order.reduce_only,
                    filled_size=Decimal(str(order.filled_size)),
                    acknowledged_at=order.acknowledged_at or _utcnow(),
                    revision=1,
                    created_at=order.created_at,
                )
                session.add(record)
            else:
                self._update_order_record(record, order)
                record.revision += 1

            payload = self._order_payload(order)
            self._append_event_rows(
                session,
                order,
                order_id=record.order_id,
                revision=record.revision,
                event_type="reconciled_import",
                payload=payload,
            )
            if order.is_terminal:
                reservation = (
                    await session.execute(
                        select(RiskReservationRecord)
                        .where(
                            RiskReservationRecord.order_id == record.order_id,
                            RiskReservationRecord.status == "active",
                        )
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if reservation is not None:
                    reservation.status = "consumed" if order.status == OrderStatus.FILLED else "released"
                    reservation.released_at = _utcnow()

    async def load_open_orders(self) -> list[Order]:
        async with self._session_factory() as session:
            records = await OrderRepository(session).list_all_open()
            return [self._to_domain(record) for record in records]

    async def get_order(self, cloid: str) -> Order | None:
        async with self._session_factory() as session:
            record = (await session.execute(select(OrderRecord).where(OrderRecord.cloid == cloid))).scalar_one_or_none()
            return self._to_domain(record) if record is not None else None

    @staticmethod
    def _update_order_record(record: OrderRecord, order: Order) -> None:
        record.exchange_oid = str(order.exchange_oid) if order.exchange_oid else None
        record.status = str(order.status)
        record.filled_size = Decimal(str(order.filled_size))
        record.avg_fill_price = Decimal(str(order.avg_fill_price)) if order.avg_fill_price is not None else None
        record.error_message = order.error_message
        record.submitted_at = order.submitted_at
        record.acknowledged_at = order.acknowledged_at
        record.filled_at = order.filled_at

    @staticmethod
    def _order_payload(order: Order) -> dict[str, Any]:
        return {
            "cloid": str(order.cloid),
            "exchange_oid": str(order.exchange_oid) if order.exchange_oid else None,
            "symbol": str(order.symbol),
            "side": str(order.side),
            "size": str(order.size),
            "price": str(order.price) if order.price is not None else None,
            "order_type": str(order.order_type),
            "time_in_force": str(order.time_in_force),
            "status": str(order.status),
            "strategy_id": str(order.strategy_id) if order.strategy_id else None,
            "sub_account": str(order.sub_account) if order.sub_account else None,
            "reduce_only": order.reduce_only,
            "filled_size": str(order.filled_size),
            "avg_fill_price": str(order.avg_fill_price) if order.avg_fill_price is not None else None,
            "error_message": order.error_message,
        }

    @staticmethod
    def _append_event_rows(
        session: AsyncSession,
        order: Order,
        *,
        order_id: uuid.UUID,
        revision: int,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        session.add(
            OrderEvent(
                order_id=order_id,
                cloid=str(order.cloid),
                revision=revision,
                event_type=event_type,
                symbol=str(order.symbol),
                side=str(order.side),
                size=Decimal(str(order.size)),
                price=Decimal(str(order.price)) if order.price is not None else None,
                status=str(order.status),
                error_message=order.error_message,
                strategy_id=str(order.strategy_id) if order.strategy_id else None,
                payload=payload,
            )
        )
        session.add(
            OutboxEventRecord(
                event_type=f"order.{event_type}",
                aggregate_type="order",
                aggregate_id=str(order_id),
                aggregate_revision=revision,
                correlation_id=str(order.cloid),
                payload=payload,
            )
        )

    @staticmethod
    def _to_domain(record: OrderRecord) -> Order:
        return Order(
            cloid=Cloid(record.cloid),
            symbol=Symbol(record.symbol),
            side=Side(record.side),
            size=Size(record.size),
            price=Price(record.price) if record.price is not None else None,
            order_type=OrderType(record.order_type),
            time_in_force=TimeInForce(record.time_in_force),
            status=OrderStatus(record.status),
            strategy_id=StrategyId(record.strategy_id) if record.strategy_id else None,
            sub_account=SubAccount(record.sub_account) if record.sub_account else None,
            reduce_only=record.reduce_only,
            exchange_oid=OrderId(record.exchange_oid) if record.exchange_oid else None,
            filled_size=Size(record.filled_size),
            avg_fill_price=Price(record.avg_fill_price) if record.avg_fill_price is not None else None,
            submitted_at=record.submitted_at,
            acknowledged_at=record.acknowledged_at,
            filled_at=record.filled_at,
            error_message=record.error_message,
            created_at=record.created_at,
        )


class PostgresExecutionCommandQueue:
    """Postgres lease queue for the single signed-action executor."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        lease_seconds: int = 15,
        unknown_recheck_seconds: int = 5,
    ) -> None:
        self._session_factory = session_factory
        self._lease_seconds = lease_seconds
        self._unknown_recheck_seconds = unknown_recheck_seconds

    async def claim(self, worker_id: str) -> DurableExecutionCommand | None:
        now = _utcnow()
        lease_cutoff = now - timedelta(seconds=self._lease_seconds)
        async with self._session_factory() as session, session.begin():
            expired = (
                (
                    await session.execute(
                        select(ExecutionCommandRecord)
                        .where(
                            ExecutionCommandRecord.command_type.in_(("place_order", "cancel_order")),
                            ExecutionCommandRecord.status == "processing",
                            ExecutionCommandRecord.locked_at < lease_cutoff,
                        )
                        .with_for_update(skip_locked=True)
                    )
                )
                .scalars()
                .all()
            )
            for command in expired:
                command.status = "unknown"
                command.locked_at = None
                command.locked_by = None
                command.available_at = now
                command.last_error_code = "processing_lease_expired"
                command.last_error_message = "Worker lease expired; exchange outcome must be queried by cloid"

            record = (
                await session.execute(
                    select(ExecutionCommandRecord)
                    .where(
                        ExecutionCommandRecord.command_type.in_(("place_order", "cancel_order")),
                        ExecutionCommandRecord.status.in_({"pending", "unknown"}),
                        ExecutionCommandRecord.available_at <= now,
                    )
                    .order_by(ExecutionCommandRecord.priority, ExecutionCommandRecord.created_at)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
            ).scalar_one_or_none()
            if record is None:
                return None
            requires_resolution = record.status == "unknown"
            record.status = "processing"
            record.locked_at = now
            record.locked_by = worker_id
            record.attempt_count += 1
            return DurableExecutionCommand(
                command_id=record.command_id,
                command_type=record.command_type,
                payload=dict(record.payload),
                attempt_count=record.attempt_count,
                requires_resolution=requires_resolution,
            )

    async def defer_unknown(self, command_id: uuid.UUID, reason: str) -> None:
        async with self._session_factory() as session, session.begin():
            command = (
                await session.execute(
                    select(ExecutionCommandRecord)
                    .where(ExecutionCommandRecord.command_id == command_id)
                    .with_for_update()
                )
            ).scalar_one()
            command.status = "unknown"
            command.locked_at = None
            command.locked_by = None
            command.completed_at = None
            command.available_at = _utcnow() + timedelta(seconds=self._unknown_recheck_seconds)
            command.last_error_code = "exchange_outcome_unknown"
            command.last_error_message = reason


@dataclass(frozen=True)
class DurableSystemState:
    state: str
    kill_switch_active: bool
    reason: str | None


class PostgresSystemStateStore:
    """Durable safety latch with an outbox event for every transition."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def load(self) -> DurableSystemState | None:
        async with self._session_factory() as session:
            record = await session.get(SystemStateRecord, "trading")
            if record is None:
                return None
            return DurableSystemState(record.state, record.kill_switch_active, record.reason)

    async def transition(
        self,
        state: str,
        reason: str | None,
        *,
        kill_switch_active: bool,
        triggered_by: str = "application",
    ) -> None:
        now = _utcnow()
        async with self._session_factory() as session, session.begin():
            record = (
                await session.execute(
                    select(SystemStateRecord).where(SystemStateRecord.state_key == "trading").with_for_update()
                )
            ).scalar_one_or_none()
            if record is None:
                record = SystemStateRecord(state_key="trading", state=state, revision=1)
                session.add(record)
            else:
                record.state = state
                record.revision += 1
            record.kill_switch_active = kill_switch_active
            record.reason = reason
            record.triggered_by = triggered_by
            record.triggered_at = now if kill_switch_active else None
            record.updated_at = now
            session.add(
                OutboxEventRecord(
                    event_type="system.safety.transitioned",
                    aggregate_type="system_state",
                    aggregate_id="trading",
                    aggregate_revision=record.revision,
                    payload={
                        "state": state,
                        "kill_switch_active": kill_switch_active,
                        "reason": reason,
                    },
                )
            )


class PostgresWriter:
    """Legacy EventBus adapter retained while producers migrate to durable commands."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], event_bus: EventBus) -> None:
        self._session_factory = session_factory
        self._event_bus = event_bus
        self._running = False
        self._total_written = 0

    async def run(self) -> None:
        self._running = True
        queues = (
            self._event_bus.subscribe(EVENT_ORDER_SUBMITTED),
            self._event_bus.subscribe(EVENT_ORDER_FILLED),
            self._event_bus.subscribe(EVENT_ORDER_CANCELLED),
            self._event_bus.subscribe(EVENT_ORDER_REJECTED),
        )
        queue_tasks = {asyncio.create_task(queue.get()): queue for queue in queues}
        logger.info("pg_writer_started")
        try:
            while self._running:
                done, _ = await asyncio.wait(queue_tasks, timeout=1.0, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    queue = queue_tasks.pop(task)
                    try:
                        await self._handle_event(task.result())
                    except Exception:
                        logger.exception("pg_writer_event_error")
                    queue_tasks[asyncio.create_task(queue.get())] = queue
        except asyncio.CancelledError:
            logger.debug("pg_writer_cancelled")
        finally:
            for task in queue_tasks:
                task.cancel()
            logger.info("pg_writer_stopped", total_written=self._total_written)

    async def stop(self) -> None:
        self._running = False

    async def _handle_event(self, event: Event) -> None:
        if event.event_type == EVENT_ORDER_SUBMITTED:
            await self._persist_order_submitted(event.payload)
        elif event.event_type == EVENT_ORDER_FILLED:
            await self._persist_order_filled(event.payload)
        elif event.event_type in (EVENT_ORDER_CANCELLED, EVENT_ORDER_REJECTED):
            await self._persist_order_terminal(event)

    async def _persist_order_submitted(self, order: Order) -> None:
        order_id = _uuid()
        revision = 1
        cloid = str(order.cloid)
        canonical_cloid = cloid if cloid.startswith("0x") and len(cloid) == 34 else f"0x{order_id.hex}"
        async with self._session_factory() as session, session.begin():
            session.add(
                OrderRecord(
                    order_id=order_id,
                    cloid=canonical_cloid,
                    legacy_cloid=cloid if canonical_cloid != cloid else None,
                    exchange_oid=str(order.exchange_oid) if order.exchange_oid else None,
                    symbol=str(order.symbol),
                    side=str(order.side),
                    order_type=str(order.order_type),
                    time_in_force=str(order.time_in_force),
                    size=Decimal(str(order.size)),
                    price=Decimal(str(order.price)) if order.price is not None else None,
                    status=str(order.status),
                    strategy_id=str(order.strategy_id) if order.strategy_id else None,
                    sub_account=str(order.sub_account) if order.sub_account else None,
                    reduce_only=order.reduce_only,
                    filled_size=Decimal(str(order.filled_size)),
                    avg_fill_price=Decimal(str(order.avg_fill_price)) if order.avg_fill_price is not None else None,
                    error_message=order.error_message,
                    revision=revision,
                )
            )
            session.add(
                OrderEvent(
                    order_id=order_id,
                    cloid=canonical_cloid,
                    revision=revision,
                    event_type="submitted",
                    symbol=str(order.symbol),
                    side=str(order.side),
                    size=Decimal(str(order.size)),
                    price=Decimal(str(order.price)) if order.price is not None else None,
                    status=str(order.status),
                    strategy_id=str(order.strategy_id) if order.strategy_id else None,
                )
            )
        self._total_written += 2

    async def _persist_order_filled(self, order_or_fill: Any) -> None:
        legacy_cloid = str(order_or_fill.cloid)
        async with self._session_factory() as session, session.begin():
            order_record = (
                await session.execute(
                    select(OrderRecord).where(
                        (OrderRecord.cloid == legacy_cloid) | (OrderRecord.legacy_cloid == legacy_cloid)
                    )
                )
            ).scalar_one_or_none()
            if order_record is None:
                logger.warning("pg_fill_order_not_found", cloid=legacy_cloid)
                return
            fill_size = Decimal(str(getattr(order_or_fill, "filled_size", 0) or getattr(order_or_fill, "size", 0)))
            fill_price = Decimal(
                str(getattr(order_or_fill, "avg_fill_price", None) or getattr(order_or_fill, "price", 0))
            )
            order_record.status = "filled"
            order_record.filled_size = fill_size
            order_record.avg_fill_price = fill_price
            order_record.revision += 1
            session.add(
                OrderEvent(
                    order_id=order_record.order_id,
                    cloid=order_record.cloid,
                    revision=order_record.revision,
                    event_type="filled",
                    symbol=str(order_or_fill.symbol),
                    side=str(getattr(order_or_fill, "side", "")),
                    size=Decimal(str(getattr(order_or_fill, "size", 0))),
                    price=Decimal(str(getattr(order_or_fill, "price", 0))),
                    status="filled",
                    strategy_id=str(getattr(order_or_fill, "strategy_id", None) or "") or None,
                )
            )
            if hasattr(order_or_fill, "fee") and hasattr(order_or_fill, "is_maker"):
                occurred_at = datetime.fromtimestamp(order_or_fill.timestamp / 1000, tz=UTC)
                exchange_fill_id = f"{order_or_fill.exchange_oid}:{order_or_fill.timestamp}:{order_or_fill.size}"
                session.add(
                    FillRecord(
                        exchange_fill_id=exchange_fill_id,
                        order_id=order_record.order_id,
                        cloid=order_record.cloid,
                        exchange_oid=str(order_or_fill.exchange_oid),
                        symbol=str(order_or_fill.symbol),
                        side=str(order_or_fill.side),
                        price=Decimal(str(order_or_fill.price)),
                        size=Decimal(str(order_or_fill.size)),
                        fee=Decimal(str(order_or_fill.fee)),
                        is_maker=bool(order_or_fill.is_maker),
                        strategy_id=str(order_or_fill.strategy_id) if order_or_fill.strategy_id else None,
                        sub_account=str(order_or_fill.sub_account) if order_or_fill.sub_account else None,
                        occurred_at=occurred_at,
                        timestamp=occurred_at,
                    )
                )
        self._total_written += 2

    async def _persist_order_terminal(self, event: Event) -> None:
        order = event.payload
        legacy_cloid = str(getattr(order, "cloid", ""))
        status = "cancelled" if event.event_type == EVENT_ORDER_CANCELLED else "rejected"
        async with self._session_factory() as session, session.begin():
            order_record = (
                await session.execute(
                    select(OrderRecord).where(
                        (OrderRecord.cloid == legacy_cloid) | (OrderRecord.legacy_cloid == legacy_cloid)
                    )
                )
            ).scalar_one_or_none()
            if order_record is None:
                logger.warning("pg_terminal_order_not_found", cloid=legacy_cloid, status=status)
                return
            order_record.status = status
            order_record.error_message = getattr(order, "error_message", None)
            order_record.revision += 1
            session.add(
                OrderEvent(
                    order_id=order_record.order_id,
                    cloid=order_record.cloid,
                    revision=order_record.revision,
                    event_type=status,
                    symbol=str(getattr(order, "symbol", "")),
                    status=status,
                    error_message=getattr(order, "error_message", None),
                    strategy_id=str(getattr(order, "strategy_id", None) or "") or None,
                )
            )
        self._total_written += 2
