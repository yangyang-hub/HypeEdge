"""Add recoverable delivery leases to the durable outbox.

Revision ID: 004_durable_outbox
Revises: 003_exchange_fact_chain
Create Date: 2026-07-11
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "004_durable_outbox"
down_revision: str | None = "003_exchange_fact_chain"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("outbox_events", sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("outbox_events", sa.Column("claimed_by", sa.Text(), nullable=True))
    op.add_column(
        "outbox_events",
        sa.Column("publish_attempts", sa.BigInteger(), server_default="0", nullable=False),
    )
    op.add_column("outbox_events", sa.Column("last_publish_error", sa.Text(), nullable=True))
    op.create_check_constraint(
        "ck_outbox_events_publish_attempts",
        "outbox_events",
        "publish_attempts >= 0",
    )
    op.create_index(
        "ix_outbox_events_dispatch",
        "outbox_events",
        ["claimed_at", "sequence"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_events_dispatch", table_name="outbox_events")
    op.drop_constraint("ck_outbox_events_publish_attempts", "outbox_events", type_="check")
    op.drop_column("outbox_events", "last_publish_error")
    op.drop_column("outbox_events", "publish_attempts")
    op.drop_column("outbox_events", "claimed_by")
    op.drop_column("outbox_events", "claimed_at")
