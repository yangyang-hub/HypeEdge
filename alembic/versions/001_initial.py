"""Initial schema — orders, fills, position_snapshots, order_events, pnl.

Revision ID: 001_initial
Revises: None
Create Date: 2026-06-02
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # orders table
    op.create_table(
        "orders",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("cloid", sa.String(64), nullable=False),
        sa.Column("exchange_oid", sa.String(64), nullable=True),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("side", sa.String(10), nullable=False),
        sa.Column("order_type", sa.String(20), nullable=False),
        sa.Column("size", sa.Float(), nullable=False),
        sa.Column("price", sa.Float(), nullable=True),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("strategy_id", sa.String(50), nullable=True),
        sa.Column("sub_account", sa.String(50), nullable=True),
        sa.Column("filled_size", sa.Float(), server_default="0.0"),
        sa.Column("avg_fill_price", sa.Float(), nullable=True),
        sa.Column("error_message", sa.String(500), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_orders_cloid", "orders", ["cloid"], unique=True)
    op.create_index("ix_orders_symbol", "orders", ["symbol"])
    op.create_index("ix_orders_status", "orders", ["status"])
    op.create_index("ix_orders_strategy_id", "orders", ["strategy_id"])

    # fills table
    op.create_table(
        "fills",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("cloid", sa.String(64), nullable=False),
        sa.Column("exchange_oid", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("side", sa.String(10), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("size", sa.Float(), nullable=False),
        sa.Column("fee", sa.Float(), nullable=False),
        sa.Column("is_maker", sa.Integer(), server_default="0"),
        sa.Column("strategy_id", sa.String(50), nullable=True),
        sa.Column("sub_account", sa.String(50), nullable=True),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_fills_cloid", "fills", ["cloid"])
    op.create_index("ix_fills_symbol", "fills", ["symbol"])
    op.create_index("ix_fills_strategy_id", "fills", ["strategy_id"])

    # position_snapshots table
    op.create_table(
        "position_snapshots",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("size", sa.Float(), nullable=False),
        sa.Column("entry_price", sa.Float(), nullable=True),
        sa.Column("mark_price", sa.Float(), nullable=True),
        sa.Column("unrealized_pnl", sa.Float(), nullable=True),
        sa.Column("leverage", sa.Integer(), server_default="1"),
        sa.Column("strategy_id", sa.String(50), nullable=True),
        sa.Column("sub_account", sa.String(50), nullable=True),
        sa.Column("snapshot_at", sa.DateTime(), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_position_snapshots_symbol", "position_snapshots", ["symbol"])
    op.create_index("ix_position_snapshots_strategy_id", "position_snapshots", ["strategy_id"])

    # order_events table (append-only audit log)
    op.create_table(
        "order_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("cloid", sa.String(64), nullable=False),
        sa.Column("event_type", sa.String(30), nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("side", sa.String(10), nullable=True),
        sa.Column("size", sa.Float(), nullable=True),
        sa.Column("price", sa.Float(), nullable=True),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("error_message", sa.String(500), nullable=True),
        sa.Column("strategy_id", sa.String(50), nullable=True),
        sa.Column("extra_data", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_order_events_cloid", "order_events", ["cloid"])
    op.create_index("ix_order_events_event_type", "order_events", ["event_type"])
    op.create_index("ix_order_events_strategy_id", "order_events", ["strategy_id"])

    # pnl table
    op.create_table(
        "pnl",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("strategy_id", sa.String(50), nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("realized_pnl", sa.Float(), server_default="0.0"),
        sa.Column("fees", sa.Float(), server_default="0.0"),
        sa.Column("funding", sa.Float(), server_default="0.0"),
        sa.Column("trade_count", sa.Integer(), server_default="0"),
        sa.Column("period_start", sa.DateTime(), nullable=False),
        sa.Column("period_end", sa.DateTime(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_pnl_strategy_id", "pnl", ["strategy_id"])
    op.create_index("ix_pnl_symbol", "pnl", ["symbol"])


def downgrade() -> None:
    op.drop_table("pnl")
    op.drop_table("order_events")
    op.drop_table("position_snapshots")
    op.drop_table("fills")
    op.drop_table("orders")
