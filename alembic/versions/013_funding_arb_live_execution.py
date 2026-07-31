"""Add testnet funding-arbitrage execution facts and spot projections.

Revision ID: 013_funding_arb_live_execution
Revises: 012_funding_arb_safety_fixes
Create Date: 2026-07-31
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "013_funding_arb_live_execution"
down_revision: str | None = "012_funding_arb_safety_fixes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

MONEY = sa.Numeric(38, 18)
TSTZ = sa.DateTime(timezone=True)
UUID = sa.Uuid()
JSONB = postgresql.JSONB(astext_type=sa.Text())


def upgrade() -> None:
    _extend_orders_and_fills()
    _extend_funding_config()
    _create_spot_balances()
    _create_funding_cycles()


def _extend_orders_and_fills() -> None:
    op.add_column("orders", sa.Column("is_spot", sa.Boolean(), server_default="false", nullable=False))
    op.add_column("orders", sa.Column("risk_reducing", sa.Boolean(), server_default="false", nullable=False))
    op.add_column("orders", sa.Column("max_slippage_bps", sa.Integer(), server_default="50", nullable=False))
    op.create_check_constraint("ck_orders_max_slippage_bps", "orders", "max_slippage_bps BETWEEN 1 AND 500")
    op.create_check_constraint("ck_orders_spot_not_reduce_only", "orders", "NOT (is_spot AND reduce_only)")
    op.add_column("fills", sa.Column("is_spot", sa.Boolean(), server_default="false", nullable=False))


def _extend_funding_config() -> None:
    columns = (
        sa.Column("max_slippage_bps", sa.BigInteger(), server_default="50", nullable=False),
        sa.Column("max_basis_bps", sa.BigInteger(), server_default="500", nullable=False),
        sa.Column("min_expected_edge_bps", MONEY, server_default="5", nullable=False),
        sa.Column("expected_hold_hours", sa.BigInteger(), server_default="8", nullable=False),
        sa.Column("round_trip_fee_bps", MONEY, server_default="20", nullable=False),
        sa.Column("max_unhedged_seconds", sa.BigInteger(), server_default="15", nullable=False),
    )
    for column in columns:
        op.add_column("funding_arb_config_versions", column)
    op.create_check_constraint(
        "ck_fa_config_max_slippage",
        "funding_arb_config_versions",
        "max_slippage_bps BETWEEN 1 AND 500",
    )
    op.create_check_constraint("ck_fa_config_max_basis", "funding_arb_config_versions", "max_basis_bps > 0")
    op.create_check_constraint(
        "ck_fa_config_min_edge", "funding_arb_config_versions", "min_expected_edge_bps >= 0"
    )
    op.create_check_constraint(
        "ck_fa_config_hold_hours",
        "funding_arb_config_versions",
        "expected_hold_hours BETWEEN 1 AND 168",
    )
    op.create_check_constraint(
        "ck_fa_config_round_trip_fee", "funding_arb_config_versions", "round_trip_fee_bps >= 0"
    )
    op.create_check_constraint(
        "ck_fa_config_unhedged_seconds",
        "funding_arb_config_versions",
        "max_unhedged_seconds BETWEEN 1 AND 60",
    )


def _create_spot_balances() -> None:
    op.create_table(
        "spot_balances",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column("balance_id", UUID, nullable=False, unique=True),
        sa.Column("sub_account", sa.Text(), nullable=True),
        sa.Column("token", sa.Text(), nullable=False),
        sa.Column("total", MONEY, server_default="0", nullable=False),
        sa.Column("hold", MONEY, server_default="0", nullable=False),
        sa.Column("entry_ntl", MONEY, server_default="0", nullable=False),
        sa.Column("exchange_updated_at", TSTZ, nullable=False),
        sa.Column("revision", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("total >= 0", name="ck_spot_balances_total"),
        sa.CheckConstraint("hold >= 0 AND hold <= total", name="ck_spot_balances_hold"),
    )
    op.create_index("ix_spot_balances_sub_account", "spot_balances", ["sub_account"])
    op.create_index("ix_spot_balances_token", "spot_balances", ["token"])
    op.create_index("ix_spot_balances_account_updated", "spot_balances", ["sub_account", "updated_at"])
    op.create_index(
        "uq_spot_balances_scope_token",
        "spot_balances",
        ["sub_account", "token"],
        unique=True,
        postgresql_nulls_not_distinct=True,
    )


def _create_funding_cycles() -> None:
    states = (
        "'entering_spot','entering_perp','compensating_entry','open','rebalancing',"
        "'exiting_perp','exiting_spot','closed','faulted'"
    )
    op.create_table(
        "funding_arb_cycles",
        sa.Column("cycle_id", UUID, primary_key=True),
        sa.Column("strategy_id", sa.Text(), nullable=False),
        sa.Column("config_version_id", sa.BigInteger(), nullable=False),
        sa.Column("config_revision", sa.BigInteger(), nullable=False),
        sa.Column("sub_account", sa.Text(), nullable=False),
        sa.Column("perp_symbol", sa.Text(), nullable=False),
        sa.Column("spot_symbol", sa.Text(), nullable=False),
        sa.Column("spot_display", sa.Text(), nullable=False),
        sa.Column("base_token", sa.Text(), nullable=False),
        sa.Column("quote_token", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("target_perp_size", MONEY, nullable=False),
        sa.Column("target_spot_size", MONEY, nullable=False),
        sa.Column("perp_open_size", MONEY, server_default="0", nullable=False),
        sa.Column("spot_open_size", MONEY, server_default="0", nullable=False),
        sa.Column("baseline_spot_size", MONEY, server_default="0", nullable=False),
        sa.Column("spot_entry_cloid", sa.Text(), nullable=True),
        sa.Column("perp_entry_cloid", sa.Text(), nullable=True),
        sa.Column("compensation_cloid", sa.Text(), nullable=True),
        sa.Column("perp_exit_cloid", sa.Text(), nullable=True),
        sa.Column("spot_exit_cloid", sa.Text(), nullable=True),
        sa.Column("entry_funding_rate", MONEY, nullable=False),
        sa.Column("entry_basis_bps", MONEY, nullable=False),
        sa.Column("error_code", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("revision", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("opened_at", TSTZ, nullable=True),
        sa.Column("closed_at", TSTZ, nullable=True),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategy_instances.strategy_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["config_version_id", "strategy_id"],
            ["strategy_config_versions.id", "strategy_config_versions.strategy_id"],
            name="fk_funding_arb_cycles_config",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(f"state IN ({states})", name="ck_funding_arb_cycles_state"),
        sa.CheckConstraint("target_perp_size > 0", name="ck_funding_arb_cycles_target_perp"),
        sa.CheckConstraint("target_spot_size > 0", name="ck_funding_arb_cycles_target_spot"),
        sa.CheckConstraint("perp_open_size >= 0", name="ck_funding_arb_cycles_perp_open"),
        sa.CheckConstraint("spot_open_size >= 0", name="ck_funding_arb_cycles_spot_open"),
        sa.CheckConstraint("baseline_spot_size >= 0", name="ck_funding_arb_cycles_spot_baseline"),
        sa.CheckConstraint("revision >= 0", name="ck_funding_arb_cycles_revision"),
    )
    op.create_index("ix_funding_arb_cycles_strategy_id", "funding_arb_cycles", ["strategy_id"])
    op.create_index("ix_funding_arb_cycles_config", "funding_arb_cycles", ["config_version_id"])
    op.create_index("ix_funding_arb_cycles_sub_account", "funding_arb_cycles", ["sub_account"])
    op.create_index("ix_funding_arb_cycles_strategy_created", "funding_arb_cycles", ["strategy_id", "created_at"])
    op.create_index(
        "uq_funding_arb_cycles_active_strategy",
        "funding_arb_cycles",
        ["strategy_id"],
        unique=True,
        postgresql_where=sa.text("state <> 'closed'"),
    )

    op.create_table(
        "funding_arb_cycle_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column("event_id", UUID, nullable=False, unique=True),
        sa.Column("cycle_id", UUID, nullable=False),
        sa.Column("revision", sa.BigInteger(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("from_state", sa.Text(), nullable=True),
        sa.Column("to_state", sa.Text(), nullable=False),
        sa.Column("payload", JSONB, server_default="{}", nullable=False),
        sa.Column("occurred_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["cycle_id"], ["funding_arb_cycles.cycle_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("cycle_id", "revision", name="uq_funding_arb_cycle_events_revision"),
        sa.CheckConstraint("revision > 0", name="ck_funding_arb_cycle_events_revision"),
    )
    op.create_index("ix_funding_arb_cycle_events_cycle_id", "funding_arb_cycle_events", ["cycle_id"])
    op.create_index(
        "ix_funding_arb_cycle_events_cycle_created",
        "funding_arb_cycle_events",
        ["cycle_id", "occurred_at"],
    )

    op.create_table(
        "funding_payments",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column("payment_id", UUID, nullable=False, unique=True),
        sa.Column("source", sa.Text(), server_default="hyperliquid", nullable=False),
        sa.Column("external_event_id", sa.Text(), nullable=False),
        sa.Column("sub_account", sa.Text(), nullable=False),
        sa.Column("cycle_id", UUID, nullable=True),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("amount", MONEY, nullable=False),
        sa.Column("funding_rate", MONEY, nullable=False),
        sa.Column("position_size", MONEY, nullable=False),
        sa.Column("occurred_at", TSTZ, nullable=False),
        sa.Column("raw_event", JSONB, server_default="{}", nullable=False),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["cycle_id"], ["funding_arb_cycles.cycle_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("source", "external_event_id", name="uq_funding_payments_source_event"),
    )
    op.create_index("ix_funding_payments_sub_account", "funding_payments", ["sub_account"])
    op.create_index("ix_funding_payments_cycle_id", "funding_payments", ["cycle_id"])
    op.create_index("ix_funding_payments_symbol", "funding_payments", ["symbol"])
    op.create_index("ix_funding_payments_symbol_occurred", "funding_payments", ["symbol", "occurred_at"])


def downgrade() -> None:
    op.drop_table("funding_payments")
    op.drop_table("funding_arb_cycle_events")
    op.drop_table("funding_arb_cycles")
    op.drop_table("spot_balances")

    for name in (
        "ck_fa_config_unhedged_seconds",
        "ck_fa_config_round_trip_fee",
        "ck_fa_config_hold_hours",
        "ck_fa_config_min_edge",
        "ck_fa_config_max_basis",
        "ck_fa_config_max_slippage",
    ):
        op.drop_constraint(name, "funding_arb_config_versions", type_="check")
    for name in (
        "max_unhedged_seconds",
        "round_trip_fee_bps",
        "expected_hold_hours",
        "min_expected_edge_bps",
        "max_basis_bps",
        "max_slippage_bps",
    ):
        op.drop_column("funding_arb_config_versions", name)

    op.drop_column("fills", "is_spot")
    op.drop_constraint("ck_orders_spot_not_reduce_only", "orders", type_="check")
    op.drop_constraint("ck_orders_max_slippage_bps", "orders", type_="check")
    op.drop_column("orders", "max_slippage_bps")
    op.drop_column("orders", "risk_reducing")
    op.drop_column("orders", "is_spot")
