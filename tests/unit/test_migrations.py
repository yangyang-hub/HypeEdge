"""Alembic migration contract tests for safety-critical schema changes."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from alembic.config import Config
from alembic.script import ScriptDirectory


def test_funding_arb_safety_migration_repairs_strategy_type_constraint() -> None:
    revision = ScriptDirectory.from_config(Config("alembic.ini")).get_revision("012_funding_arb_safety_fixes")
    assert revision is not None
    assert revision.down_revision == "011_funding_arb_config_versions"

    operations = MagicMock()
    with patch.object(revision.module, "op", operations):
        revision.module.upgrade()

    operations.drop_constraint.assert_any_call(
        "ck_strategy_instances_type",
        "strategy_instances",
        type_="check",
    )
    checks = {call.args[0]: call.args[2] for call in operations.create_check_constraint.call_args_list}
    assert "funding_arb" in checks["ck_strategy_instances_type"]
    assert checks["ck_fa_config_entry_funding"] == "entry_funding_rate > 0"
    assert checks["ck_fa_config_rate_hysteresis"] == "exit_funding_rate < entry_funding_rate"
    strict_checks = {
        call.args[0]: call.kwargs
        for call in operations.create_check_constraint.call_args_list
        if call.args[0].startswith("ck_fa_config_")
    }
    assert strict_checks["ck_fa_config_entry_funding"]["postgresql_not_valid"] is True
    assert strict_checks["ck_fa_config_rate_hysteresis"]["postgresql_not_valid"] is True
    assert strict_checks["ck_fa_config_spot_market"]["postgresql_not_valid"] is True
    # Config versions are immutable and their parent hash must remain stable.
    operations.execute.assert_not_called()


def test_funding_arb_live_migration_adds_asset_class_and_cycle_facts() -> None:
    revision = ScriptDirectory.from_config(Config("alembic.ini")).get_revision("013_funding_arb_live_execution")
    assert revision is not None
    assert revision.down_revision == "012_funding_arb_safety_fixes"

    operations = MagicMock()
    with patch.object(revision.module, "op", operations):
        revision.module.upgrade()

    added_order_columns = {
        call.args[1].name for call in operations.add_column.call_args_list if call.args[0] == "orders"
    }
    assert {"is_spot", "risk_reducing", "max_slippage_bps"} <= added_order_columns
    created_tables = {call.args[0] for call in operations.create_table.call_args_list}
    assert {"spot_balances", "funding_arb_cycles", "funding_arb_cycle_events", "funding_payments"} <= created_tables
