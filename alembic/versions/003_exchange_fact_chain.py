"""Add immutable ledger facts and durable exchange history cursors.

Revision ID: 003_exchange_fact_chain
Revises: 002_postgres_v2
Create Date: 2026-07-11
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "003_exchange_fact_chain"
down_revision: str | None = "002_postgres_v2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

MONEY = sa.Numeric(38, 18)
TSTZ = sa.DateTime(timezone=True)
UUID = sa.Uuid()
JSONB = postgresql.JSONB(astext_type=sa.Text())


def upgrade() -> None:
    op.create_table(
        "ledger_entries",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column("entry_id", UUID, nullable=False, unique=True),
        sa.Column("fill_id", UUID, nullable=False),
        sa.Column("entry_type", sa.Text(), nullable=False),
        sa.Column("asset", sa.Text(), server_default="USDC", nullable=False),
        sa.Column("amount", MONEY, nullable=False),
        sa.Column("sub_account", sa.Text(), nullable=True),
        sa.Column("strategy_id", sa.Text(), nullable=True),
        sa.Column("occurred_at", TSTZ, nullable=False),
        sa.Column("metadata", JSONB, server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("created_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["fill_id"], ["fills.fill_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("fill_id", "entry_type", name="uq_ledger_entries_fill_type"),
    )
    op.create_index("ix_ledger_entries_fill_id", "ledger_entries", ["fill_id"])
    op.create_index("ix_ledger_entries_sub_account", "ledger_entries", ["sub_account"])
    op.create_index("ix_ledger_entries_strategy_id", "ledger_entries", ["strategy_id"])
    op.create_index(
        "ix_ledger_entries_account_occurred", "ledger_entries", ["sub_account", "occurred_at"]
    )

    op.create_table(
        "exchange_sync_cursors",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("sub_account", sa.Text(), nullable=False),
        sa.Column("stream", sa.Text(), nullable=False),
        sa.Column("last_exchange_timestamp_ms", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("last_external_event_id", sa.Text(), nullable=True),
        sa.Column("updated_at", TSTZ, server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("source", "sub_account", "stream", name="uq_exchange_sync_cursor_scope"),
    )


def downgrade() -> None:
    op.drop_table("exchange_sync_cursors")
    op.drop_table("ledger_entries")
