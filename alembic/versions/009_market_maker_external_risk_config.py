"""Persist external-reference, latency and markout strategy parameters.

Revision ID: 009_mm_external_risk_config
Revises: 008_strategy_instance_revision
Create Date: 2026-07-12
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "009_mm_external_risk_config"
down_revision: str | None = "008_strategy_instance_revision"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_DECIMAL_COLUMNS = {
    "external_reference_weight": "0.25",
    "external_max_age_seconds": "0.5",
    "external_outlier_bps": "75",
    "max_external_shift_ticks": "2",
    "max_total_fair_shift_ticks": "3",
    "latency_risk_multiplier": "1",
    "conservative_latency_seconds": "0.1",
    "conservative_markout_bps": "1",
}

_CHECKS = {
    "ck_mm_config_external_weight": "external_reference_weight >= 0 AND external_reference_weight <= 1",
    "ck_mm_config_external_max_age": "external_max_age_seconds > 0",
    "ck_mm_config_external_outlier": "external_outlier_bps > 0",
    "ck_mm_config_external_shift": "max_external_shift_ticks >= 0",
    "ck_mm_config_total_shift": "max_total_fair_shift_ticks >= 0",
    "ck_mm_config_latency_multiplier": "latency_risk_multiplier >= 0",
    "ck_mm_config_latency_default": "conservative_latency_seconds >= 0",
    "ck_mm_config_markout_default": "conservative_markout_bps >= 0",
    "ck_mm_config_markout_samples": "min_markout_samples > 0",
}


def upgrade() -> None:
    for name, default in _DECIMAL_COLUMNS.items():
        op.add_column(
            "market_maker_config_versions",
            sa.Column(name, sa.Numeric(38, 18), nullable=False, server_default=sa.text(default)),
        )
    op.add_column(
        "market_maker_config_versions",
        sa.Column("min_markout_samples", sa.BigInteger(), nullable=False, server_default=sa.text("20")),
    )
    for name, expression in _CHECKS.items():
        op.create_check_constraint(name, "market_maker_config_versions", expression)
    for name in (*_DECIMAL_COLUMNS, "min_markout_samples"):
        op.alter_column("market_maker_config_versions", name, server_default=None)


def downgrade() -> None:
    for name in reversed(tuple(_CHECKS)):
        op.drop_constraint(name, "market_maker_config_versions", type_="check")
    op.drop_column("market_maker_config_versions", "min_markout_samples")
    for name in reversed(tuple(_DECIMAL_COLUMNS)):
        op.drop_column("market_maker_config_versions", name)
