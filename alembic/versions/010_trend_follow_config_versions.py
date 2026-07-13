"""Add typed trend-follow configuration versions.

Revision ID: 010_trend_follow_config_versions
Revises: 009_mm_external_risk_config
Create Date: 2026-07-13
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "010_trend_follow_config_versions"
down_revision: str | None = "009_mm_external_risk_config"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

MONEY = sa.Numeric(38, 18)


def upgrade() -> None:
    op.create_table(
        "trend_follow_config_versions",
        sa.Column(
            "config_version_id",
            sa.BigInteger(),
            sa.ForeignKey("strategy_config_versions.id", ondelete="RESTRICT"),
            primary_key=True,
        ),
        sa.Column("fast_ema_period", sa.BigInteger(), nullable=False),
        sa.Column("slow_ema_period", sa.BigInteger(), nullable=False),
        sa.Column("signal_ema_period", sa.BigInteger(), nullable=False),
        sa.Column("momentum_period", sa.BigInteger(), nullable=False),
        sa.Column("momentum_threshold", MONEY, nullable=False),
        sa.Column("atr_period", sa.BigInteger(), nullable=False),
        sa.Column("atr_position_multiplier", MONEY, nullable=False),
        sa.Column("atr_stop_multiplier", MONEY, nullable=False),
        sa.Column("max_position_pct", MONEY, nullable=False),
        sa.Column("risk_per_trade_pct", MONEY, nullable=False),
        sa.Column("macd_cross_threshold", MONEY, nullable=False),
        sa.CheckConstraint("fast_ema_period > 0", name="ck_tf_config_fast_ema"),
        sa.CheckConstraint("slow_ema_period > 0", name="ck_tf_config_slow_ema"),
        sa.CheckConstraint("fast_ema_period < slow_ema_period", name="ck_tf_config_ema_order"),
        sa.CheckConstraint("signal_ema_period > 0", name="ck_tf_config_signal_ema"),
        sa.CheckConstraint("momentum_period > 0", name="ck_tf_config_momentum_period"),
        sa.CheckConstraint("atr_period > 0", name="ck_tf_config_atr_period"),
        sa.CheckConstraint("atr_position_multiplier > 0", name="ck_tf_config_atr_pos_mult"),
        sa.CheckConstraint("atr_stop_multiplier > 0", name="ck_tf_config_atr_stop_mult"),
        sa.CheckConstraint("max_position_pct > 0 AND max_position_pct <= 1", name="ck_tf_config_max_pos"),
        sa.CheckConstraint("risk_per_trade_pct > 0 AND risk_per_trade_pct <= 1", name="ck_tf_config_risk"),
    )


def downgrade() -> None:
    op.drop_table("trend_follow_config_versions")
