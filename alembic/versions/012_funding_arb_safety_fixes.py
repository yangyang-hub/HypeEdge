"""Repair funding-arbitrage strategy and config constraints.

Revision ID: 012_funding_arb_safety_fixes
Revises: 011_funding_arb_config_versions
Create Date: 2026-07-31
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "012_funding_arb_safety_fixes"
down_revision: str | None = "011_funding_arb_config_versions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint("ck_strategy_instances_type", "strategy_instances", type_="check")
    op.create_check_constraint(
        "ck_strategy_instances_type",
        "strategy_instances",
        "strategy_type IN ('funding_arb','trend_follow','market_maker','legacy')",
    )

    op.drop_constraint("ck_fa_config_entry_funding", "funding_arb_config_versions", type_="check")
    # Historical config versions are immutable and their parent rows contain a
    # content hash. NOT VALID preserves any legacy rows without rewriting/hash
    # drift while still enforcing every new insert or update fail-closed.
    op.create_check_constraint(
        "ck_fa_config_entry_funding",
        "funding_arb_config_versions",
        "entry_funding_rate > 0",
        postgresql_not_valid=True,
    )
    op.create_check_constraint(
        "ck_fa_config_rate_hysteresis",
        "funding_arb_config_versions",
        "exit_funding_rate < entry_funding_rate",
        postgresql_not_valid=True,
    )
    op.create_check_constraint(
        "ck_fa_config_spot_market",
        "funding_arb_config_versions",
        "spot_coin ~ '^(@[0-9]+|[A-Za-z0-9_.:-]+/[A-Za-z0-9_.:-]+)$'",
        postgresql_not_valid=True,
    )


def downgrade() -> None:
    op.drop_constraint("ck_fa_config_spot_market", "funding_arb_config_versions", type_="check")
    op.drop_constraint("ck_fa_config_rate_hysteresis", "funding_arb_config_versions", type_="check")
    op.drop_constraint("ck_fa_config_entry_funding", "funding_arb_config_versions", type_="check")
    op.create_check_constraint(
        "ck_fa_config_entry_funding",
        "funding_arb_config_versions",
        "entry_funding_rate >= 0",
    )

    op.drop_constraint("ck_strategy_instances_type", "strategy_instances", type_="check")
    op.create_check_constraint(
        "ck_strategy_instances_type",
        "strategy_instances",
        "strategy_type IN ('trend_follow','market_maker','legacy')",
    )
