"""Add the transactional market-making foundation.

Revision ID: 007_market_making_foundation
Revises: 006_account_position_projection
Create Date: 2026-07-11
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "007_market_making_foundation"
down_revision: str | None = "006_account_position_projection"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

MONEY = sa.Numeric(38, 18)
TSTZ = sa.DateTime(timezone=True)
UUID = sa.Uuid()
JSONB = postgresql.JSONB(astext_type=sa.Text())


def _id() -> sa.Column[int]:
    return sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True)


def upgrade() -> None:
    _create_strategy_tables()
    _create_quote_tables()
    _create_execution_child_tables()
    _create_budget_tables()
    _expand_risk_reservations()


def _create_strategy_tables() -> None:
    # desired_config_version_id is fenced with a composite FK after config versions exist.
    op.create_table(
        "strategy_instances",
        sa.Column("strategy_id", sa.Text(), primary_key=True),
        sa.Column("strategy_type", sa.Text(), nullable=False),
        sa.Column("sub_account", sa.Text(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("desired_state", sa.Text(), server_default="stopped", nullable=False),
        sa.Column("desired_config_version_id", sa.BigInteger(), nullable=True),
        sa.Column("archived_at", TSTZ, nullable=True),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "strategy_type IN ('trend_follow','market_maker','legacy')", name="ck_strategy_instances_type"
        ),
        sa.CheckConstraint(
            "desired_state IN ('stopped','shadow','running','paused')",
            name="ck_strategy_instances_desired_state",
        ),
        sa.CheckConstraint(
            "archived_at IS NULL OR archived_at >= created_at", name="ck_strategy_instances_archive_time"
        ),
    )
    op.create_index("ix_strategy_instances_scope", "strategy_instances", ["sub_account", "symbol"])
    op.create_index("ix_strategy_instances_desired_config", "strategy_instances", ["desired_config_version_id"])

    op.create_table(
        "strategy_config_versions",
        _id(),
        sa.Column("strategy_id", sa.Text(), nullable=False),
        sa.Column("version", sa.BigInteger(), nullable=False),
        sa.Column("config_hash", sa.Text(), nullable=False),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategy_instances.strategy_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("strategy_id", "version", name="uq_strategy_config_versions_version"),
        sa.UniqueConstraint("strategy_id", "config_hash", name="uq_strategy_config_versions_hash"),
        sa.UniqueConstraint("id", "strategy_id", name="uq_strategy_config_versions_id_strategy"),
        sa.CheckConstraint("version > 0", name="ck_strategy_config_versions_version"),
        sa.CheckConstraint("length(config_hash) = 64", name="ck_strategy_config_versions_hash"),
    )
    op.create_index("ix_strategy_config_versions_strategy_id", "strategy_config_versions", ["strategy_id"])
    op.create_foreign_key(
        "fk_strategy_instances_desired_config",
        "strategy_instances",
        "strategy_config_versions",
        ["desired_config_version_id", "strategy_id"],
        ["id", "strategy_id"],
        ondelete="RESTRICT",
    )

    op.create_table(
        "strategy_allocations",
        _id(),
        sa.Column("strategy_id", sa.Text(), nullable=False),
        sa.Column("sub_account", sa.Text(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("allocated_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.Column("revision", sa.BigInteger(), server_default="0", nullable=False),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategy_instances.strategy_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("strategy_id", name="uq_strategy_allocations_strategy_id"),
        sa.CheckConstraint("revision >= 0", name="ck_strategy_allocations_revision"),
    )
    op.create_index("ix_strategy_allocations_strategy_id", "strategy_allocations", ["strategy_id"])
    op.create_index(
        "uq_strategy_allocations_scope",
        "strategy_allocations",
        ["sub_account", "symbol"],
        unique=True,
        postgresql_nulls_not_distinct=True,
    )

    op.create_table(
        "market_maker_config_versions",
        sa.Column("config_version_id", sa.BigInteger(), primary_key=True),
        sa.Column("soft_inventory_notional", MONEY, nullable=False),
        sa.Column("hard_inventory_notional", MONEY, nullable=False),
        sa.Column("emergency_inventory_notional", MONEY, nullable=False),
        sa.Column("quote_size", MONEY, nullable=False),
        sa.Column("max_depth_participation", MONEY, nullable=False),
        sa.Column("inventory_skew_bps", MONEY, nullable=False),
        sa.Column("max_inventory_shift_bps", MONEY, nullable=False),
        sa.Column("min_half_spread_bps", MONEY, nullable=False),
        sa.Column("toxicity_spread_bps", MONEY, nullable=False),
        sa.Column("min_expected_pnl_usdc", MONEY, nullable=False),
        sa.Column("min_quote_lifetime_ms", sa.BigInteger(), nullable=False),
        sa.Column("refresh_cooldown_ms", sa.BigInteger(), nullable=False),
        sa.Column("max_quote_age_ms", sa.BigInteger(), nullable=False),
        sa.Column("market_stale_after_ms", sa.BigInteger(), nullable=False),
        sa.Column("account_stale_after_ms", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(["config_version_id"], ["strategy_config_versions.id"], ondelete="RESTRICT"),
        sa.CheckConstraint("soft_inventory_notional > 0", name="ck_mm_config_soft_inventory"),
        sa.CheckConstraint("hard_inventory_notional >= soft_inventory_notional", name="ck_mm_config_hard_inventory"),
        sa.CheckConstraint(
            "emergency_inventory_notional >= hard_inventory_notional",
            name="ck_mm_config_emergency_inventory",
        ),
        sa.CheckConstraint("quote_size > 0", name="ck_mm_config_quote_size"),
        sa.CheckConstraint("max_depth_participation > 0 AND max_depth_participation <= 1", name="ck_mm_config_depth"),
        sa.CheckConstraint("min_quote_lifetime_ms >= 0", name="ck_mm_config_min_lifetime"),
        sa.CheckConstraint("refresh_cooldown_ms >= 0", name="ck_mm_config_cooldown"),
        sa.CheckConstraint("max_quote_age_ms > 0", name="ck_mm_config_max_age"),
        sa.CheckConstraint("market_stale_after_ms > 0", name="ck_mm_config_market_stale"),
        sa.CheckConstraint("account_stale_after_ms > 0", name="ck_mm_config_account_stale"),
        sa.CheckConstraint("min_expected_pnl_usdc >= 0", name="ck_mm_config_expected_pnl"),
    )

    op.create_table(
        "strategy_runtime_state",
        sa.Column("strategy_id", sa.Text(), primary_key=True),
        sa.Column("actual_state", sa.Text(), server_default="stopped", nullable=False),
        sa.Column("effective_config_version_id", sa.BigInteger(), nullable=True),
        sa.Column("heartbeat_at", TSTZ, nullable=True),
        sa.Column("revision", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("updated_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategy_instances.strategy_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["effective_config_version_id", "strategy_id"],
            ["strategy_config_versions.id", "strategy_config_versions.strategy_id"],
            name="fk_strategy_runtime_state_effective_config",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "actual_state IN ('stopped','warming','shadow','running','paused','draining','faulted')",
            name="ck_strategy_runtime_state_actual",
        ),
        sa.CheckConstraint("revision >= 0", name="ck_strategy_runtime_state_revision"),
    )
    op.execute("ALTER TABLE strategy_runtime_state SET (fillfactor = 90)")
    op.create_index(
        "ix_strategy_runtime_state_effective_config",
        "strategy_runtime_state",
        ["effective_config_version_id"],
    )

    op.create_table(
        "strategy_state_events",
        _id(),
        sa.Column("strategy_id", sa.Text(), nullable=False),
        sa.Column("from_state", sa.Text(), nullable=True),
        sa.Column("to_state", sa.Text(), nullable=False),
        sa.Column("desired_config_version_id", sa.BigInteger(), nullable=True),
        sa.Column("effective_config_version_id", sa.BigInteger(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("actor", sa.Text(), nullable=False),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategy_instances.strategy_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["desired_config_version_id"], ["strategy_config_versions.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["effective_config_version_id"], ["strategy_config_versions.id"], ondelete="RESTRICT"),
        sa.CheckConstraint(
            "from_state IS NULL OR from_state IN "
            "('stopped','warming','shadow','running','paused','draining','faulted')",
            name="ck_strategy_state_events_from_state",
        ),
        sa.CheckConstraint(
            "to_state IN ('stopped','warming','shadow','running','paused','draining','faulted')",
            name="ck_strategy_state_events_to_state",
        ),
    )
    op.create_index("ix_strategy_state_events_strategy_id", "strategy_state_events", ["strategy_id"])
    op.create_index("ix_strategy_state_events_desired_config", "strategy_state_events", ["desired_config_version_id"])
    op.create_index(
        "ix_strategy_state_events_effective_config", "strategy_state_events", ["effective_config_version_id"]
    )
    op.create_index("ix_strategy_state_events_strategy_created", "strategy_state_events", ["strategy_id", "created_at"])

    op.create_table(
        "market_making_sessions",
        _id(),
        sa.Column("strategy_id", sa.Text(), nullable=False),
        sa.Column("config_version_id", sa.BigInteger(), nullable=False),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("started_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.Column("ended_at", TSTZ, nullable=True),
        sa.Column("stop_reason", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategy_instances.strategy_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["config_version_id", "strategy_id"],
            ["strategy_config_versions.id", "strategy_config_versions.strategy_id"],
            name="fk_market_making_sessions_config",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("id", "strategy_id", name="uq_market_making_sessions_id_strategy"),
        sa.CheckConstraint("mode IN ('shadow','testnet','mainnet')", name="ck_market_making_sessions_mode"),
        sa.CheckConstraint("ended_at IS NULL OR ended_at >= started_at", name="ck_market_making_sessions_time"),
    )
    op.create_index("ix_market_making_sessions_strategy_id", "market_making_sessions", ["strategy_id"])
    op.create_index("ix_market_making_sessions_config_version_id", "market_making_sessions", ["config_version_id"])
    op.create_index(
        "ix_market_making_sessions_strategy_started",
        "market_making_sessions",
        ["strategy_id", "started_at"],
    )
    op.create_index(
        "uq_market_making_sessions_active_strategy",
        "market_making_sessions",
        ["strategy_id"],
        unique=True,
        postgresql_where=sa.text("ended_at IS NULL"),
    )


def _create_quote_tables() -> None:
    op.create_table(
        "quote_plans",
        sa.Column("plan_id", UUID, primary_key=True),
        sa.Column("strategy_id", sa.Text(), nullable=False),
        sa.Column("session_id", sa.BigInteger(), nullable=False),
        sa.Column("config_version_id", sa.BigInteger(), nullable=False),
        sa.Column("revision", sa.BigInteger(), nullable=False),
        sa.Column("market_version", sa.BigInteger(), nullable=False),
        sa.Column("fair_price", MONEY, nullable=False),
        sa.Column("reservation_price", MONEY, nullable=False),
        sa.Column("inventory_size", MONEY, nullable=False),
        sa.Column("budget_mode", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default="planned", nullable=False),
        sa.Column("valid_until", TSTZ, nullable=False),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategy_instances.strategy_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["session_id", "strategy_id"],
            ["market_making_sessions.id", "market_making_sessions.strategy_id"],
            name="fk_quote_plans_session",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["config_version_id", "strategy_id"],
            ["strategy_config_versions.id", "strategy_config_versions.strategy_id"],
            name="fk_quote_plans_config",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("strategy_id", "session_id", "revision", name="uq_quote_plans_revision"),
        sa.UniqueConstraint("plan_id", "strategy_id", name="uq_quote_plans_id_strategy"),
        sa.CheckConstraint("revision >= 0", name="ck_quote_plans_revision"),
        sa.CheckConstraint("market_version >= 0", name="ck_quote_plans_market_version"),
        sa.CheckConstraint("fair_price > 0", name="ck_quote_plans_fair_price"),
        sa.CheckConstraint("reservation_price > 0", name="ck_quote_plans_reservation_price"),
        sa.CheckConstraint("valid_until >= created_at", name="ck_quote_plans_valid_until"),
        sa.CheckConstraint(
            "budget_mode IN ('normal','conserve','critical','cancel_only','exhausted')",
            name="ck_quote_plans_budget_mode",
        ),
        sa.CheckConstraint(
            "status IN ('planned','dispatching','succeeded','partial','unknown','cancelled','superseded')",
            name="ck_quote_plans_status",
        ),
    )
    op.create_index("ix_quote_plans_strategy_id", "quote_plans", ["strategy_id"])
    op.create_index("ix_quote_plans_session_id", "quote_plans", ["session_id"])
    op.create_index("ix_quote_plans_config", "quote_plans", ["config_version_id"])
    op.create_index("ix_quote_plans_session_created", "quote_plans", ["session_id", "created_at"])

    op.create_table(
        "quote_plan_items",
        _id(),
        sa.Column("plan_id", UUID, nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("level", sa.Integer(), server_default="0", nullable=False),
        sa.Column("decision", sa.Text(), nullable=False),
        sa.Column("source_order_id", UUID, nullable=True),
        sa.Column("target_order_id", UUID, nullable=True),
        sa.Column("source_cloid", sa.Text(), nullable=True),
        sa.Column("target_cloid", sa.Text(), nullable=True),
        sa.Column("desired_price", MONEY, nullable=True),
        sa.Column("desired_size", MONEY, nullable=True),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["plan_id"], ["quote_plans.plan_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["source_order_id"], ["orders.order_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["target_order_id"], ["orders.order_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("plan_id", "ordinal", name="uq_quote_plan_items_ordinal"),
        sa.CheckConstraint("ordinal >= 0", name="ck_quote_plan_items_ordinal"),
        sa.CheckConstraint("level >= 0", name="ck_quote_plan_items_level"),
        sa.CheckConstraint("side IN ('buy','sell')", name="ck_quote_plan_items_side"),
        sa.CheckConstraint(
            "decision IN ('place','cancel','modify','blocked_unknown')",
            name="ck_quote_plan_items_decision",
        ),
        sa.CheckConstraint("desired_price IS NULL OR desired_price > 0", name="ck_quote_plan_items_price"),
        sa.CheckConstraint("desired_size IS NULL OR desired_size > 0", name="ck_quote_plan_items_size"),
    )
    for column in ("plan_id", "source_order_id", "target_order_id"):
        op.create_index(f"ix_quote_plan_items_{column}", "quote_plan_items", [column])
    op.create_index("ix_quote_plan_items_slot", "quote_plan_items", ["symbol", "side", "level"])

    op.create_table(
        "quote_slots",
        _id(),
        sa.Column("strategy_id", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("level", sa.Integer(), server_default="0", nullable=False),
        sa.Column("owner_order_id", UUID, nullable=True),
        sa.Column("owner_plan_id", UUID, nullable=True),
        sa.Column("plan_revision", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("state", sa.Text(), server_default="empty", nullable=False),
        sa.Column("revision", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("updated_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategy_instances.strategy_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["owner_order_id"], ["orders.order_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["owner_plan_id", "strategy_id"],
            ["quote_plans.plan_id", "quote_plans.strategy_id"],
            name="fk_quote_slots_owner_plan",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("strategy_id", "symbol", "side", "level", name="uq_quote_slots_key"),
        sa.CheckConstraint("level >= 0", name="ck_quote_slots_level"),
        sa.CheckConstraint("side IN ('buy','sell')", name="ck_quote_slots_side"),
        sa.CheckConstraint("plan_revision >= 0", name="ck_quote_slots_plan_revision"),
        sa.CheckConstraint("revision >= 0", name="ck_quote_slots_revision"),
        sa.CheckConstraint(
            "state IN ('empty','live','inflight','unknown','orphaned_live','recovery_required')",
            name="ck_quote_slots_state",
        ),
    )
    op.execute("ALTER TABLE quote_slots SET (fillfactor = 90)")
    op.create_index("ix_quote_slots_strategy_id", "quote_slots", ["strategy_id"])
    op.create_index("ix_quote_slots_owner_order", "quote_slots", ["owner_order_id"])
    op.create_index("ix_quote_slots_owner_plan_id", "quote_slots", ["owner_plan_id"])


def _create_execution_child_tables() -> None:
    op.create_table(
        "execution_command_items",
        _id(),
        sa.Column("command_id", UUID, nullable=False),
        sa.Column("plan_item_id", sa.BigInteger(), nullable=True),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("action_type", sa.Text(), nullable=False),
        sa.Column("source_order_id", UUID, nullable=True),
        sa.Column("target_order_id", UUID, nullable=True),
        sa.Column("status", sa.Text(), server_default="pending", nullable=False),
        sa.Column("resolution", sa.Text(), nullable=True),
        sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("available_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.Column("locked_at", TSTZ, nullable=True),
        sa.Column("locked_by", sa.Text(), nullable=True),
        sa.Column("completed_at", TSTZ, nullable=True),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["command_id"], ["execution_commands.command_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["plan_item_id"], ["quote_plan_items.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["source_order_id"], ["orders.order_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["target_order_id"], ["orders.order_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("command_id", "ordinal", name="uq_execution_command_items_ordinal"),
        sa.UniqueConstraint("id", "command_id", name="uq_execution_command_items_id_command"),
        sa.CheckConstraint("ordinal >= 0", name="ck_execution_command_items_ordinal"),
        sa.CheckConstraint("attempt_count >= 0", name="ck_execution_command_items_attempts"),
        sa.CheckConstraint("action_type IN ('place','cancel','modify')", name="ck_execution_command_items_action"),
        sa.CheckConstraint(
            "status IN ('pending','processing','succeeded','failed','unknown','cancelled',"
            "'superseded','expired','blocked')",
            name="ck_execution_command_items_status",
        ),
    )
    for column in ("command_id", "plan_item_id", "source_order_id", "target_order_id"):
        op.create_index(f"ix_execution_command_items_{column}", "execution_command_items", [column])
    op.create_index("ix_execution_command_items_ready", "execution_command_items", ["status", "available_at"])

    op.create_table(
        "execution_actions",
        _id(),
        sa.Column("command_item_id", sa.BigInteger(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("action_type", sa.Text(), nullable=False),
        sa.Column("request_hash", sa.Text(), nullable=False),
        sa.Column("sent_at", TSTZ, nullable=False),
        sa.Column("responded_at", TSTZ, nullable=True),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("response_code", sa.Text(), nullable=True),
        sa.Column("estimated_credit_cost", sa.BigInteger(), nullable=False),
        sa.Column("reconciled_credit_cost", sa.BigInteger(), nullable=True),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["command_item_id"], ["execution_command_items.id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("command_item_id", "attempt", name="uq_execution_actions_attempt"),
        sa.CheckConstraint("attempt > 0", name="ck_execution_actions_attempt"),
        sa.CheckConstraint("length(request_hash) = 64", name="ck_execution_actions_request_hash"),
        sa.CheckConstraint("action_type IN ('place','cancel','modify')", name="ck_execution_actions_action"),
        sa.CheckConstraint(
            "outcome IN ('succeeded','rejected','timeout','unknown','transport_error')",
            name="ck_execution_actions_outcome",
        ),
        sa.CheckConstraint("estimated_credit_cost >= 0", name="ck_execution_actions_estimated_cost"),
        sa.CheckConstraint(
            "reconciled_credit_cost IS NULL OR reconciled_credit_cost >= 0",
            name="ck_execution_actions_reconciled_cost",
        ),
        sa.CheckConstraint("responded_at IS NULL OR responded_at >= sent_at", name="ck_execution_actions_time"),
    )
    op.create_index("ix_execution_actions_command_item_id", "execution_actions", ["command_item_id"])
    op.create_index("ix_execution_actions_sent", "execution_actions", ["sent_at"])


def _create_budget_tables() -> None:
    op.create_table(
        "action_budget_scopes",
        sa.Column("quota_owner_address", sa.Text(), primary_key=True),
        sa.Column("remote_cap", sa.BigInteger(), nullable=False),
        sa.Column("remote_used", sa.BigInteger(), nullable=False),
        sa.Column("remote_remaining", sa.BigInteger(), nullable=False),
        sa.Column("shadow_used", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("emergency_reserve", sa.BigInteger(), nullable=False),
        sa.Column("mode", sa.Text(), server_default="cancel_only", nullable=False),
        sa.Column("observed_at", TSTZ, nullable=False),
        sa.Column("revision", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("updated_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "remote_cap >= 0 AND remote_used >= 0 AND remote_remaining >= 0 AND shadow_used >= 0",
            name="ck_action_budget_scopes_nonnegative",
        ),
        sa.CheckConstraint("emergency_reserve >= 0", name="ck_action_budget_scopes_emergency_reserve"),
        sa.CheckConstraint("remote_used <= remote_cap", name="ck_action_budget_scopes_used_cap"),
        sa.CheckConstraint("remote_remaining = remote_cap - remote_used", name="ck_action_budget_scopes_balance"),
        sa.CheckConstraint("emergency_reserve <= remote_cap", name="ck_action_budget_scopes_reserve_cap"),
        sa.CheckConstraint("quota_owner_address ~ '^0x[0-9a-f]{40}$'", name="ck_action_budget_scopes_address"),
        sa.CheckConstraint("revision >= 0", name="ck_action_budget_scopes_revision"),
        sa.CheckConstraint(
            "mode IN ('normal','conserve','critical','cancel_only','exhausted')",
            name="ck_action_budget_scopes_mode",
        ),
    )

    op.create_table(
        "action_budget_allocations",
        _id(),
        sa.Column("quota_owner_address", sa.Text(), nullable=False),
        sa.Column("strategy_id", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("soft_allocation", sa.BigInteger(), nullable=False),
        sa.Column("hard_allocation", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.Text(), server_default="active", nullable=False),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.Column("released_at", TSTZ, nullable=True),
        sa.ForeignKeyConstraint(
            ["quota_owner_address"], ["action_budget_scopes.quota_owner_address"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategy_instances.strategy_id"], ondelete="RESTRICT"),
        sa.CheckConstraint("soft_allocation >= 0", name="ck_action_budget_allocations_soft"),
        sa.CheckConstraint("hard_allocation >= soft_allocation", name="ck_action_budget_allocations_hard"),
        sa.CheckConstraint("status IN ('active','released')", name="ck_action_budget_allocations_status"),
        sa.CheckConstraint(
            "released_at IS NULL OR released_at >= created_at", name="ck_action_budget_allocations_time"
        ),
    )
    op.create_index(
        "ix_action_budget_allocations_quota_owner_address",
        "action_budget_allocations",
        ["quota_owner_address"],
    )
    op.create_index("ix_action_budget_allocations_strategy_id", "action_budget_allocations", ["strategy_id"])
    op.create_index(
        "uq_action_budget_allocations_active_scope",
        "action_budget_allocations",
        ["quota_owner_address", "strategy_id", "symbol"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )

    op.create_table(
        "action_budget_events",
        _id(),
        sa.Column("quota_owner_address", sa.Text(), nullable=False),
        sa.Column("strategy_id", sa.Text(), nullable=True),
        sa.Column("command_item_id", sa.BigInteger(), nullable=True),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("estimated_delta", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("remote_before", sa.BigInteger(), nullable=True),
        sa.Column("remote_after", sa.BigInteger(), nullable=True),
        sa.Column("details", JSONB, server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["quota_owner_address"], ["action_budget_scopes.quota_owner_address"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategy_instances.strategy_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["command_item_id"], ["execution_command_items.id"], ondelete="RESTRICT"),
        sa.CheckConstraint("remote_before IS NULL OR remote_before >= 0", name="ck_action_budget_events_before"),
        sa.CheckConstraint("remote_after IS NULL OR remote_after >= 0", name="ck_action_budget_events_after"),
    )
    for column in ("quota_owner_address", "strategy_id", "command_item_id"):
        op.create_index(f"ix_action_budget_events_{column}", "action_budget_events", [column])
    op.create_index(
        "ix_action_budget_events_scope_created",
        "action_budget_events",
        ["quota_owner_address", "created_at"],
    )


def _expand_risk_reservations() -> None:
    op.drop_constraint("uq_risk_reservations_command", "risk_reservations", type_="unique")
    op.add_column("risk_reservations", sa.Column("command_item_id", sa.BigInteger(), nullable=True))
    op.add_column("risk_reservations", sa.Column("risk_owner_type", sa.Text(), server_default="legacy", nullable=False))
    op.add_column("risk_reservations", sa.Column("risk_owner_key", sa.Text(), nullable=True))
    op.execute("UPDATE risk_reservations SET risk_owner_key = reservation_id::text WHERE risk_owner_key IS NULL")
    op.alter_column(
        "risk_reservations",
        "risk_owner_key",
        nullable=False,
        server_default=sa.text("gen_random_uuid()::text"),
    )
    op.create_foreign_key(
        "fk_risk_reservations_command_item",
        "risk_reservations",
        "execution_command_items",
        ["command_item_id", "command_id"],
        ["id", "command_id"],
        ondelete="RESTRICT",
    )
    op.create_index("ix_risk_reservations_command_item_id", "risk_reservations", ["command_item_id"])
    op.create_unique_constraint(
        "uq_risk_reservations_command_owner", "risk_reservations", ["command_id", "risk_owner_key"]
    )
    op.create_check_constraint(
        "ck_risk_reservations_owner_type",
        "risk_reservations",
        "risk_owner_type IN ('legacy','live_order','inflight_place','unknown','new_quote')",
    )
    op.create_check_constraint("ck_risk_reservations_size", "risk_reservations", "reserved_size >= 0")


def downgrade() -> None:
    # Contracting to the legacy one-reservation-per-command model is intentionally
    # blocked if child reservations have already been materialized.
    op.execute(
        """
        DELETE FROM risk_reservations newer
        USING risk_reservations older
        WHERE newer.command_id = older.command_id AND newer.id > older.id
        """
    )
    op.drop_constraint("ck_risk_reservations_size", "risk_reservations", type_="check")
    op.drop_constraint("ck_risk_reservations_owner_type", "risk_reservations", type_="check")
    op.drop_constraint("uq_risk_reservations_command_owner", "risk_reservations", type_="unique")
    op.drop_index("ix_risk_reservations_command_item_id", table_name="risk_reservations")
    op.drop_constraint("fk_risk_reservations_command_item", "risk_reservations", type_="foreignkey")
    op.drop_column("risk_reservations", "risk_owner_key")
    op.drop_column("risk_reservations", "risk_owner_type")
    op.drop_column("risk_reservations", "command_item_id")
    op.create_unique_constraint("uq_risk_reservations_command", "risk_reservations", ["command_id"])

    for table in (
        "action_budget_events",
        "action_budget_allocations",
        "action_budget_scopes",
        "execution_actions",
        "execution_command_items",
        "quote_slots",
        "quote_plan_items",
        "quote_plans",
        "market_making_sessions",
        "strategy_state_events",
        "strategy_runtime_state",
        "market_maker_config_versions",
        "strategy_allocations",
    ):
        op.drop_table(table)
    op.drop_constraint("fk_strategy_instances_desired_config", "strategy_instances", type_="foreignkey")
    op.drop_table("strategy_config_versions")
    op.drop_table("strategy_instances")
