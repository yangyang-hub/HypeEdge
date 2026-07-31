"""Add typed funding-rate-arbitrage configuration versions.

Revision ID: 011_funding_arb_config_versions
Revises: 010_trend_follow_config_versions
Create Date: 2026-07-30
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "011_funding_arb_config_versions"
down_revision: str | None = "010_trend_follow_config_versions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

MONEY = sa.Numeric(38, 18)


def upgrade() -> None:
    op.create_table(
        "funding_arb_config_versions",
        sa.Column(
            "config_version_id",
            sa.BigInteger(),
            sa.ForeignKey("strategy_config_versions.id", ondelete="RESTRICT"),
            primary_key=True,
        ),
        sa.Column("entry_funding_rate", MONEY, nullable=False),
        sa.Column("exit_funding_rate", MONEY, nullable=False),
        sa.Column("max_notional_usd", MONEY, nullable=False),
        sa.Column("hedge_ratio", MONEY, nullable=False),
        sa.Column("rebalance_threshold_bps", sa.BigInteger(), nullable=False),
        sa.Column("leverage", MONEY, nullable=False),
        sa.Column("spot_coin", sa.String(length=64), nullable=False),
        sa.CheckConstraint("length(spot_coin) > 0", name="ck_fa_config_spot_coin"),
        sa.CheckConstraint("entry_funding_rate >= 0", name="ck_fa_config_entry_funding"),
        sa.CheckConstraint("exit_funding_rate >= 0", name="ck_fa_config_exit_funding"),
        sa.CheckConstraint("max_notional_usd > 0", name="ck_fa_config_max_notional"),
        sa.CheckConstraint("hedge_ratio > 0 AND hedge_ratio <= 1", name="ck_fa_config_hedge_ratio"),
        sa.CheckConstraint("rebalance_threshold_bps > 0", name="ck_fa_config_rebalance_bps"),
        sa.CheckConstraint("leverage > 0", name="ck_fa_config_leverage"),
    )


def downgrade() -> None:
    op.drop_table("funding_arb_config_versions")
