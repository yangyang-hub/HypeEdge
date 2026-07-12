"""Keep one account-level current position projection per symbol.

Revision ID: 005_position_projection_scope
Revises: 004_durable_outbox
Create Date: 2026-07-11
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "005_position_projection_scope"
down_revision: str | None = "004_durable_outbox"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Strategy attribution remains in fills/ledger. Current positions are an
    # exchange-authoritative account projection and must not duplicate exposure.
    op.execute("DELETE FROM positions WHERE strategy_id IS NOT NULL")


def downgrade() -> None:
    # Deleted derived projections can be rebuilt only from immutable facts.
    pass
