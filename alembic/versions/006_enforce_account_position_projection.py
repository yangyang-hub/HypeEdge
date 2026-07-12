"""Enforce one account-level current position projection per symbol.

Revision ID: 006_account_position_projection
Revises: 005_position_projection_scope
Create Date: 2026-07-11
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "006_account_position_projection"
down_revision: str | None = "005_position_projection_scope"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # positions is an exchange-authoritative account projection. Strategy
    # attribution remains in immutable fills and ledger_entries.
    op.execute("DELETE FROM positions WHERE strategy_id IS NOT NULL")
    op.drop_index("uq_positions_scope_symbol", table_name="positions")
    op.drop_index("ix_positions_strategy_id", table_name="positions")
    op.drop_column("positions", "strategy_id")
    op.create_index(
        "uq_positions_scope_symbol",
        "positions",
        ["sub_account", "symbol"],
        unique=True,
        postgresql_nulls_not_distinct=True,
    )
    op.add_column(
        "risk_reservations",
        sa.Column("reduce_only", sa.Boolean(), server_default=sa.false(), nullable=False),
    )
    op.execute(
        """
        UPDATE risk_reservations AS reservation
        SET reduce_only = orders.reduce_only
        FROM orders
        WHERE orders.order_id = reservation.order_id
        """
    )


def downgrade() -> None:
    op.drop_column("risk_reservations", "reduce_only")
    op.drop_index("uq_positions_scope_symbol", table_name="positions")
    op.add_column("positions", sa.Column("strategy_id", sa.Text(), nullable=True))
    op.create_index("ix_positions_strategy_id", "positions", ["strategy_id"])
    op.create_index(
        "uq_positions_scope_symbol",
        "positions",
        ["sub_account", "strategy_id", "symbol"],
        unique=True,
        postgresql_nulls_not_distinct=True,
    )
