"""Add optimistic revision and operator metadata to strategy instances.

Revision ID: 008_strategy_instance_revision
Revises: 007_market_making_foundation
Create Date: 2026-07-11
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "008_strategy_instance_revision"
down_revision: str | None = "007_market_making_foundation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint("ck_strategy_instances_desired_state", "strategy_instances", type_="check")
    op.create_check_constraint(
        "ck_strategy_instances_desired_state",
        "strategy_instances",
        "desired_state IN ('stopped','warming','shadow','running','paused','draining','faulted')",
    )
    op.add_column(
        "strategy_instances",
        sa.Column("revision", sa.BigInteger(), server_default="0", nullable=False),
    )
    op.add_column(
        "strategy_instances",
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
    )
    op.create_check_constraint(
        "ck_strategy_instances_revision",
        "strategy_instances",
        "revision >= 0",
    )


def downgrade() -> None:
    op.drop_constraint("ck_strategy_instances_revision", "strategy_instances", type_="check")
    op.drop_column("strategy_instances", "metadata")
    op.drop_column("strategy_instances", "revision")
    op.drop_constraint("ck_strategy_instances_desired_state", "strategy_instances", type_="check")
    op.create_check_constraint(
        "ck_strategy_instances_desired_state",
        "strategy_instances",
        "desired_state IN ('stopped','shadow','running','paused')",
    )
