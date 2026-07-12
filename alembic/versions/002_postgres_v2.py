"""Postgres V2 durable trading state.

Revision ID: 002_postgres_v2
Revises: 001_initial
Create Date: 2026-07-11
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "002_postgres_v2"
down_revision: str | None = "001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

MONEY = sa.Numeric(38, 18)
TSTZ = sa.DateTime(timezone=True)
UUID = sa.Uuid()
JSONB = postgresql.JSONB(astext_type=sa.Text())


def _identity_pk(name: str = "id") -> sa.Column[int]:
    return sa.Column(name, sa.BigInteger(), sa.Identity(always=True), primary_key=True)


def upgrade() -> None:
    """Expand and backfill the Phase 2 schema into V2."""

    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    # Orders: keep legacy cloids addressable while making the exchange cloid canonical.
    op.add_column("orders", sa.Column("order_id", UUID, nullable=True))
    op.add_column("orders", sa.Column("legacy_cloid", sa.Text(), nullable=True))
    op.add_column("orders", sa.Column("command_id", UUID, nullable=True))
    op.add_column("orders", sa.Column("time_in_force", sa.Text(), server_default="Gtc", nullable=False))
    op.add_column("orders", sa.Column("client_id", sa.Text(), nullable=True))
    op.add_column("orders", sa.Column("reduce_only", sa.Boolean(), server_default=sa.false(), nullable=False))
    op.add_column("orders", sa.Column("revision", sa.BigInteger(), server_default="0", nullable=False))
    op.add_column("orders", sa.Column("error_code", sa.Text(), nullable=True))
    op.add_column("orders", sa.Column("submitted_at", TSTZ, nullable=True))
    op.add_column("orders", sa.Column("acknowledged_at", TSTZ, nullable=True))
    op.add_column("orders", sa.Column("filled_at", TSTZ, nullable=True))
    op.execute("UPDATE orders SET order_id = gen_random_uuid() WHERE order_id IS NULL")
    op.execute(
        "UPDATE orders SET legacy_cloid = cloid, cloid = '0x' || replace(order_id::text, '-', '') "
        "WHERE cloid !~ '^0x[0-9a-f]{32}$'"
    )
    op.alter_column("orders", "order_id", nullable=False)
    for column in ("size", "price", "filled_size", "avg_fill_price"):
        op.alter_column(
            "orders",
            column,
            existing_type=sa.Float(),
            type_=MONEY,
            postgresql_using=f"{column}::numeric(38,18)",
        )
    for column in ("created_at", "updated_at"):
        op.alter_column(
            "orders",
            column,
            existing_type=sa.DateTime(),
            type_=TSTZ,
            postgresql_using=f"{column} AT TIME ZONE 'UTC'",
            nullable=False,
        )
    for column in (
        "cloid",
        "exchange_oid",
        "symbol",
        "side",
        "order_type",
        "status",
        "strategy_id",
        "sub_account",
        "error_message",
    ):
        op.alter_column("orders", column, type_=sa.Text(), existing_type=sa.String())
    op.alter_column("orders", "id", existing_type=sa.Integer(), type_=sa.BigInteger())
    op.create_unique_constraint("uq_orders_order_id", "orders", ["order_id"])
    op.create_index("ix_orders_command_id", "orders", ["command_id"])
    op.create_index("ix_orders_sub_account", "orders", ["sub_account"])
    op.create_index("ix_orders_account_status_created", "orders", ["sub_account", "status", "created_at"])
    op.create_index("ix_orders_strategy_created", "orders", ["strategy_id", "created_at"])
    op.create_index(
        "ix_orders_open",
        "orders",
        ["sub_account", "symbol"],
        postgresql_where=sa.text(
            "status IN ('pending','submitted','submit_unknown','acknowledged','partial_fill',"
            "'cancel_pending','cancel_unknown')"
        ),
    )
    op.create_check_constraint("ck_orders_size_positive", "orders", "size > 0")
    op.create_check_constraint("ck_orders_filled_size", "orders", "filled_size >= 0 AND filled_size <= size")
    op.create_check_constraint("ck_orders_price_positive", "orders", "price IS NULL OR price > 0")
    op.create_check_constraint("ck_orders_cloid_format", "orders", "cloid ~ '^0x[0-9a-f]{32}$'")
    op.create_check_constraint(
        "ck_orders_status",
        "orders",
        "status IN ('pending','submitted','submit_unknown','acknowledged','partial_fill','filled',"
        "'cancel_pending','cancel_unknown','cancelled','rejected','expired')",
    )

    # Existing append-only facts are backfilled with opaque IDs and exact values.
    op.add_column("order_events", sa.Column("event_id", UUID, nullable=True))
    op.add_column("order_events", sa.Column("order_id", UUID, nullable=True))
    op.add_column("order_events", sa.Column("revision", sa.BigInteger(), nullable=True))
    op.add_column("order_events", sa.Column("error_code", sa.Text(), nullable=True))
    op.add_column("order_events", sa.Column("payload", JSONB, server_default=sa.text("'{}'::jsonb"), nullable=False))
    op.execute("UPDATE order_events SET event_id = gen_random_uuid() WHERE event_id IS NULL")
    op.execute(
        "UPDATE order_events oe SET order_id = o.order_id, cloid = o.cloid FROM orders o "
        "WHERE oe.cloid = o.cloid OR oe.cloid = o.legacy_cloid"
    )
    # Orphan audit rows must not block migration; preserve them via a deterministic imported order.
    op.execute(
        "INSERT INTO orders (order_id, cloid, legacy_cloid, symbol, side, order_type, size, status) "
        "SELECT gen_random_uuid(), '0x' || md5(oe.cloid), oe.cloid, max(oe.symbol), "
        "coalesce(max(oe.side), 'buy'), 'limit', greatest(coalesce(max(oe.size), 0.000000000000000001), "
        "0.000000000000000001), max(oe.status) FROM order_events oe WHERE oe.order_id IS NULL GROUP BY oe.cloid"
    )
    op.execute(
        "UPDATE order_events oe SET order_id = o.order_id, cloid = o.cloid FROM orders o "
        "WHERE oe.order_id IS NULL AND oe.cloid = o.legacy_cloid"
    )
    op.execute(
        "WITH ranked AS (SELECT id, row_number() OVER (PARTITION BY order_id ORDER BY created_at, id) AS rev "
        "FROM order_events) UPDATE order_events oe SET revision = ranked.rev FROM ranked WHERE oe.id = ranked.id"
    )
    op.alter_column("order_events", "event_id", nullable=False)
    op.alter_column("order_events", "order_id", nullable=False)
    op.alter_column("order_events", "revision", nullable=False)
    for column in ("size", "price"):
        op.alter_column(
            "order_events",
            column,
            existing_type=sa.Float(),
            type_=MONEY,
            postgresql_using=f"{column}::numeric(38,18)",
        )
    op.alter_column(
        "order_events",
        "created_at",
        existing_type=sa.DateTime(),
        type_=TSTZ,
        postgresql_using="created_at AT TIME ZONE 'UTC'",
        nullable=False,
    )
    op.alter_column("order_events", "id", existing_type=sa.Integer(), type_=sa.BigInteger())
    op.create_unique_constraint("uq_order_events_event_id", "order_events", ["event_id"])
    op.create_unique_constraint("uq_order_events_order_revision", "order_events", ["order_id", "revision"])
    op.create_foreign_key(
        "fk_order_events_order_id_orders", "order_events", "orders", ["order_id"], ["order_id"], ondelete="RESTRICT"
    )
    op.create_index("ix_order_events_order_id", "order_events", ["order_id"])
    op.create_index("ix_order_events_order_created", "order_events", ["order_id", "created_at"])
    op.create_index("ix_order_events_type_created", "order_events", ["event_type", "created_at"])
    op.create_check_constraint("ck_order_events_revision", "order_events", "revision >= 0")

    op.add_column("fills", sa.Column("fill_id", UUID, nullable=True))
    op.add_column("fills", sa.Column("source", sa.Text(), server_default="hyperliquid", nullable=False))
    op.add_column("fills", sa.Column("exchange_fill_id", sa.Text(), nullable=True))
    op.add_column("fills", sa.Column("order_id", UUID, nullable=True))
    op.add_column("fills", sa.Column("realized_pnl", MONEY, server_default="0", nullable=False))
    op.add_column("fills", sa.Column("occurred_at", TSTZ, nullable=True))
    op.add_column("fills", sa.Column("raw_event", JSONB, server_default=sa.text("'{}'::jsonb"), nullable=False))
    op.execute(
        "UPDATE fills f SET fill_id = gen_random_uuid(), exchange_fill_id = f.exchange_oid || ':' || f.id::text, "
        "order_id = o.order_id, occurred_at = f.timestamp AT TIME ZONE 'UTC', cloid = o.cloid "
        "FROM orders o WHERE f.cloid = o.cloid OR f.cloid = o.legacy_cloid"
    )
    op.execute(
        "UPDATE fills SET fill_id = coalesce(fill_id, gen_random_uuid()), "
        "exchange_fill_id = coalesce(exchange_fill_id, exchange_oid || ':' || id::text), "
        "occurred_at = coalesce(occurred_at, timestamp AT TIME ZONE 'UTC')"
    )
    op.alter_column("fills", "fill_id", nullable=False)
    op.alter_column("fills", "exchange_fill_id", nullable=False)
    op.alter_column("fills", "occurred_at", nullable=False)
    for column in ("price", "size", "fee"):
        op.alter_column(
            "fills", column, existing_type=sa.Float(), type_=MONEY, postgresql_using=f"{column}::numeric(38,18)"
        )
    op.alter_column(
        "fills",
        "timestamp",
        existing_type=sa.DateTime(),
        type_=TSTZ,
        postgresql_using="timestamp AT TIME ZONE 'UTC'",
        nullable=False,
    )
    op.alter_column(
        "fills",
        "created_at",
        existing_type=sa.DateTime(),
        type_=TSTZ,
        postgresql_using="created_at AT TIME ZONE 'UTC'",
        nullable=False,
    )
    # PostgreSQL cannot cast INTEGER→BOOLEAN while a default expression remains.
    op.execute(sa.text("ALTER TABLE fills ALTER COLUMN is_maker DROP DEFAULT"))
    op.alter_column(
        "fills",
        "is_maker",
        existing_type=sa.Integer(),
        type_=sa.Boolean(),
        postgresql_using="is_maker <> 0",
        server_default=sa.false(),
        existing_nullable=True,
    )
    op.alter_column("fills", "id", existing_type=sa.Integer(), type_=sa.BigInteger())
    op.create_unique_constraint("uq_fills_fill_id", "fills", ["fill_id"])
    op.create_unique_constraint("uq_fills_source_exchange_fill", "fills", ["source", "exchange_fill_id"])
    op.create_foreign_key(
        "fk_fills_order_id_orders", "fills", "orders", ["order_id"], ["order_id"], ondelete="RESTRICT"
    )
    op.create_index("ix_fills_order_id", "fills", ["order_id"])
    op.create_index("ix_fills_sub_account", "fills", ["sub_account"])
    op.create_index("ix_fills_account_occurred", "fills", ["sub_account", "occurred_at"])
    op.create_index("ix_fills_symbol_occurred", "fills", ["symbol", "occurred_at"])
    op.create_check_constraint("ck_fills_price_positive", "fills", "price > 0")
    op.create_check_constraint("ck_fills_size_positive", "fills", "size > 0")

    _create_current_projections()
    _create_risk_tables()
    _create_reconciliation_tables()
    _create_delivery_tables()


def _create_current_projections() -> None:
    op.create_table(
        "positions",
        _identity_pk(),
        sa.Column("position_id", UUID, nullable=False),
        sa.Column("sub_account", sa.Text(), nullable=True),
        sa.Column("strategy_id", sa.Text(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("size", MONEY, server_default="0", nullable=False),
        sa.Column("entry_price", MONEY, nullable=True),
        sa.Column("mark_price", MONEY, nullable=True),
        sa.Column("unrealized_pnl", MONEY, server_default="0", nullable=False),
        sa.Column("realized_pnl", MONEY, server_default="0", nullable=False),
        sa.Column("leverage", sa.Integer(), server_default="1", nullable=False),
        sa.Column("liquidation_price", MONEY, nullable=True),
        sa.Column("exchange_updated_at", TSTZ, nullable=True),
        sa.Column("revision", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("leverage >= 1", name="ck_positions_leverage"),
        sa.UniqueConstraint("position_id", name="uq_positions_position_id"),
    )
    op.create_index(
        "uq_positions_scope_symbol",
        "positions",
        ["sub_account", "strategy_id", "symbol"],
        unique=True,
        postgresql_nulls_not_distinct=True,
    )
    op.create_index("ix_positions_account_updated", "positions", ["sub_account", "updated_at"])
    op.create_index("ix_positions_strategy_id", "positions", ["strategy_id"])
    op.create_index("ix_positions_symbol", "positions", ["symbol"])

    op.create_table(
        "account_state",
        _identity_pk(),
        sa.Column("sub_account", sa.Text(), nullable=True),
        sa.Column("equity", MONEY, nullable=False),
        sa.Column("available_balance", MONEY, nullable=False),
        sa.Column("total_margin_used", MONEY, nullable=False),
        sa.Column("total_unrealized_pnl", MONEY, nullable=False),
        sa.Column("peak_equity", MONEY, nullable=False),
        sa.Column("action_credits_remaining", sa.BigInteger(), nullable=True),
        sa.Column("exchange_updated_at", TSTZ, nullable=False),
        sa.Column("reconciled_at", TSTZ, nullable=True),
        sa.Column("revision", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("updated_at", TSTZ, server_default=sa.func.now(), nullable=False),
    )
    op.create_index(
        "uq_account_state_scope",
        "account_state",
        ["sub_account"],
        unique=True,
        postgresql_nulls_not_distinct=True,
    )

    op.create_table(
        "system_state",
        sa.Column("state_key", sa.Text(), server_default="trading", primary_key=True),
        sa.Column("state", sa.Text(), server_default="starting", nullable=False),
        sa.Column("kill_switch_active", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("triggered_by", sa.Text(), nullable=True),
        sa.Column("triggered_at", TSTZ, nullable=True),
        sa.Column("last_reconciliation_id", UUID, nullable=True),
        sa.Column("revision", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("metadata", JSONB, server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("updated_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "state IN ('starting','reconciling','normal','reduce_only','cancel_only','halting','halted',"
            "'recovering','stopping')",
            name="ck_system_state_state",
        ),
    )
    op.create_index("ix_system_state_last_reconciliation_id", "system_state", ["last_reconciliation_id"])
    op.execute("INSERT INTO system_state (state_key, state) VALUES ('trading', 'starting')")


def _create_risk_tables() -> None:
    op.create_table(
        "risk_events",
        _identity_pk(),
        sa.Column("risk_event_id", UUID, nullable=False, unique=True),
        sa.Column("command_id", UUID, nullable=False),
        sa.Column("order_id", UUID, nullable=True),
        sa.Column("sub_account", sa.Text(), nullable=True),
        sa.Column("strategy_id", sa.Text(), nullable=True),
        sa.Column("passed", sa.Boolean(), nullable=False),
        sa.Column("reason_code", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("checked_limits", JSONB, server_default=sa.text("'[]'::jsonb"), nullable=False),
        sa.Column("snapshot", JSONB, server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("duration_ms", sa.BigInteger(), nullable=False),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["order_id"], ["orders.order_id"], ondelete="RESTRICT"),
    )
    op.create_index("ix_risk_events_command_id", "risk_events", ["command_id"])
    op.create_index("ix_risk_events_order_id", "risk_events", ["order_id"])
    op.create_index("ix_risk_events_sub_account", "risk_events", ["sub_account"])
    op.create_index("ix_risk_events_strategy_id", "risk_events", ["strategy_id"])
    op.create_index("ix_risk_events_account_created", "risk_events", ["sub_account", "created_at"])

    op.create_table(
        "risk_reservations",
        _identity_pk(),
        sa.Column("reservation_id", UUID, nullable=False, unique=True),
        sa.Column("command_id", UUID, nullable=False),
        sa.Column("order_id", UUID, nullable=True),
        sa.Column("sub_account", sa.Text(), nullable=True),
        sa.Column("strategy_id", sa.Text(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("reserved_size", MONEY, nullable=False),
        sa.Column("reserved_notional", MONEY, nullable=False),
        sa.Column("status", sa.Text(), server_default="active", nullable=False),
        sa.Column("expires_at", TSTZ, nullable=False),
        sa.Column("released_at", TSTZ, nullable=True),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["order_id"], ["orders.order_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("command_id", name="uq_risk_reservations_command"),
        sa.CheckConstraint("reserved_notional >= 0", name="ck_risk_reservations_notional"),
        sa.CheckConstraint("status IN ('active','consumed','released','expired')", name="ck_risk_reservations_status"),
    )
    op.create_index("ix_risk_reservations_order_id", "risk_reservations", ["order_id"])
    op.create_index("ix_risk_reservations_sub_account", "risk_reservations", ["sub_account"])
    op.create_index("ix_risk_reservations_strategy_id", "risk_reservations", ["strategy_id"])
    op.create_index(
        "ix_risk_reservations_active",
        "risk_reservations",
        ["sub_account", "expires_at"],
        postgresql_where=sa.text("status = 'active'"),
    )


def _create_reconciliation_tables() -> None:
    op.create_table(
        "reconciliation_runs",
        _identity_pk(),
        sa.Column("run_id", UUID, nullable=False, unique=True),
        sa.Column("sub_account", sa.Text(), nullable=True),
        sa.Column("trigger", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default="running", nullable=False),
        sa.Column("required_queries", JSONB, server_default=sa.text("'[]'::jsonb"), nullable=False),
        sa.Column("completed_queries", JSONB, server_default=sa.text("'[]'::jsonb"), nullable=False),
        sa.Column("error_code", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("started_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.Column("finished_at", TSTZ, nullable=True),
        sa.CheckConstraint("status IN ('running','succeeded','failed')", name="ck_reconciliation_runs_status"),
    )
    op.create_index("ix_reconciliation_runs_sub_account", "reconciliation_runs", ["sub_account"])
    op.create_index("ix_reconciliation_runs_scope_started", "reconciliation_runs", ["sub_account", "started_at"])

    op.create_table(
        "reconciliation_diffs",
        _identity_pk(),
        sa.Column("diff_id", UUID, nullable=False, unique=True),
        sa.Column("run_id", UUID, nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("entity_key", sa.Text(), nullable=False),
        sa.Column("difference_type", sa.Text(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("local_value", JSONB, nullable=True),
        sa.Column("exchange_value", JSONB, nullable=True),
        sa.Column("resolution", sa.Text(), nullable=True),
        sa.Column("resolved", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("resolved_at", TSTZ, nullable=True),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["reconciliation_runs.run_id"], ondelete="CASCADE"),
    )
    op.create_index("ix_reconciliation_diffs_run_id", "reconciliation_diffs", ["run_id"])
    op.create_index("ix_reconciliation_diffs_run_severity", "reconciliation_diffs", ["run_id", "severity"])


def _create_delivery_tables() -> None:
    op.create_table(
        "execution_commands",
        _identity_pk(),
        sa.Column("command_id", UUID, nullable=False, unique=True),
        sa.Column("order_id", UUID, nullable=True),
        sa.Column("command_type", sa.Text(), nullable=False),
        sa.Column("actor_type", sa.Text(), nullable=False),
        sa.Column("actor_id", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("priority", sa.Integer(), server_default="100", nullable=False),
        sa.Column("status", sa.Text(), server_default="pending", nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("available_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.Column("locked_at", TSTZ, nullable=True),
        sa.Column("locked_by", sa.Text(), nullable=True),
        sa.Column("completed_at", TSTZ, nullable=True),
        sa.Column("last_error_code", sa.Text(), nullable=True),
        sa.Column("last_error_message", sa.Text(), nullable=True),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["order_id"], ["orders.order_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("actor_id", "idempotency_key", name="uq_execution_commands_actor_idempotency"),
        sa.CheckConstraint("attempt_count >= 0", name="ck_execution_commands_attempt_count"),
        sa.CheckConstraint(
            "status IN ('pending','processing','succeeded','failed','unknown','cancelled')",
            name="ck_execution_commands_status",
        ),
    )
    op.create_index("ix_execution_commands_order_id", "execution_commands", ["order_id"])
    op.create_index(
        "ix_execution_commands_ready",
        "execution_commands",
        ["priority", "created_at"],
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index("ix_execution_commands_status_updated", "execution_commands", ["status", "updated_at"])

    op.create_table(
        "inbox_events",
        _identity_pk(),
        sa.Column("event_id", UUID, nullable=False, unique=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("external_event_id", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("payload_hash", sa.Text(), nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("received_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.Column("processed_at", TSTZ, nullable=True),
        sa.UniqueConstraint("source", "external_event_id", name="uq_inbox_events_source_external"),
    )
    op.create_index("ix_inbox_events_received", "inbox_events", ["received_at"])

    op.create_table(
        "outbox_events",
        sa.Column("sequence", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column("event_id", UUID, nullable=False, unique=True),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("schema_version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("aggregate_type", sa.Text(), nullable=False),
        sa.Column("aggregate_id", sa.Text(), nullable=False),
        sa.Column("aggregate_revision", sa.BigInteger(), nullable=False),
        sa.Column("correlation_id", sa.Text(), nullable=True),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("occurred_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.Column("published_at", TSTZ, nullable=True),
    )
    op.create_index(
        "ix_outbox_events_unpublished",
        "outbox_events",
        ["sequence"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_index("ix_outbox_events_aggregate", "outbox_events", ["aggregate_type", "aggregate_id", "sequence"])

    op.create_table(
        "api_audit",
        _identity_pk(),
        sa.Column("audit_id", UUID, nullable=False, unique=True),
        sa.Column("request_id", UUID, nullable=False),
        sa.Column("actor_type", sa.Text(), nullable=False),
        sa.Column("actor_id", sa.Text(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("resource_type", sa.Text(), nullable=True),
        sa.Column("resource_id", sa.Text(), nullable=True),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("ip_address", postgresql.INET(), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column("details", JSONB, server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_api_audit_request_id", "api_audit", ["request_id"])
    op.create_index("ix_api_audit_actor_created", "api_audit", ["actor_id", "created_at"])


def downgrade() -> None:
    """Remove V2 tables and restore the Phase 2 column shapes."""

    for table in (
        "api_audit",
        "outbox_events",
        "inbox_events",
        "execution_commands",
        "reconciliation_diffs",
        "reconciliation_runs",
        "risk_reservations",
        "risk_events",
        "system_state",
        "account_state",
        "positions",
    ):
        op.drop_table(table)

    op.drop_constraint("fk_fills_order_id_orders", "fills", type_="foreignkey")
    op.drop_constraint("uq_fills_source_exchange_fill", "fills", type_="unique")
    op.drop_constraint("uq_fills_fill_id", "fills", type_="unique")
    for name in (
        "ck_fills_size_positive",
        "ck_fills_price_positive",
    ):
        op.drop_constraint(name, "fills", type_="check")
    for column in ("raw_event", "occurred_at", "realized_pnl", "order_id", "exchange_fill_id", "source", "fill_id"):
        op.drop_column("fills", column)

    op.drop_constraint("fk_order_events_order_id_orders", "order_events", type_="foreignkey")
    op.drop_constraint("uq_order_events_order_revision", "order_events", type_="unique")
    op.drop_constraint("uq_order_events_event_id", "order_events", type_="unique")
    op.drop_constraint("ck_order_events_revision", "order_events", type_="check")
    for column in ("payload", "error_code", "revision", "order_id", "event_id"):
        op.drop_column("order_events", column)

    for name in (
        "ck_orders_status",
        "ck_orders_cloid_format",
        "ck_orders_price_positive",
        "ck_orders_filled_size",
        "ck_orders_size_positive",
    ):
        op.drop_constraint(name, "orders", type_="check")
    op.drop_constraint("uq_orders_order_id", "orders", type_="unique")
    for column in (
        "filled_at",
        "acknowledged_at",
        "submitted_at",
        "error_code",
        "revision",
        "reduce_only",
        "client_id",
        "time_in_force",
        "command_id",
        "legacy_cloid",
        "order_id",
    ):
        op.drop_column("orders", column)
