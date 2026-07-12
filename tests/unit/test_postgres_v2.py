"""Unit tests for the Postgres V2 schema and transaction foundation."""

from __future__ import annotations

from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import DateTime, Numeric, Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateTable

from hypeedge.config.settings import PostgresSettings
from hypeedge.storage.postgres import (
    Base,
    ExecutionCommandRecord,
    OrderRecord,
    OutboxEventRecord,
    PositionRecord,
    PostgresUnitOfWork,
    create_pg_engine,
    create_pg_session_factory,
)

V2_TABLES = {
    "orders",
    "order_events",
    "fills",
    "positions",
    "account_state",
    "system_state",
    "risk_events",
    "risk_reservations",
    "reconciliation_runs",
    "reconciliation_diffs",
    "execution_commands",
    "inbox_events",
    "outbox_events",
    "api_audit",
    "strategy_instances",
    "strategy_allocations",
    "strategy_config_versions",
    "market_maker_config_versions",
    "strategy_runtime_state",
    "strategy_state_events",
    "market_making_sessions",
    "quote_plans",
    "quote_plan_items",
    "quote_slots",
    "execution_command_items",
    "execution_actions",
    "action_budget_scopes",
    "action_budget_allocations",
    "action_budget_events",
}


class TestPostgresV2Metadata:
    def test_all_transactional_tables_are_registered(self) -> None:
        assert set(Base.metadata.tables) >= V2_TABLES

    def test_exact_values_use_numeric_38_18(self) -> None:
        exact_columns = (
            OrderRecord.__table__.c.size,
            OrderRecord.__table__.c.price,
            OrderRecord.__table__.c.filled_size,
            Base.metadata.tables["fills"].c.fee,
            Base.metadata.tables["positions"].c.realized_pnl,
            Base.metadata.tables["account_state"].c.equity,
            Base.metadata.tables["risk_reservations"].c.reserved_notional,
            Base.metadata.tables["market_maker_config_versions"].c.soft_inventory_notional,
            Base.metadata.tables["quote_plans"].c.fair_price,
            Base.metadata.tables["quote_plan_items"].c.desired_size,
        )
        for column in exact_columns:
            assert isinstance(column.type, Numeric)
            assert column.type.precision == 38
            assert column.type.scale == 18

    def test_v2_timestamps_are_timezone_aware(self) -> None:
        for table_name in V2_TABLES:
            table = Base.metadata.tables[table_name]
            for column in table.columns:
                if isinstance(column.type, DateTime):
                    assert column.type.timezone, f"{table_name}.{column.name} must be TIMESTAMPTZ"

    def test_order_constraints_protect_core_invariants(self) -> None:
        order_table = cast(Table, OrderRecord.__table__)
        constraint_names = {constraint.name for constraint in order_table.constraints}
        assert {
            "ck_orders_status",
            "ck_orders_size_positive",
            "ck_orders_filled_size",
            "ck_orders_price_positive",
            "ck_orders_cloid_format",
        } <= constraint_names
        assert OrderRecord.__table__.c.order_id.unique
        assert OrderRecord.__table__.c.cloid.unique

    def test_delivery_tables_have_idempotency_and_replay_indexes(self) -> None:
        command_table = cast(Table, ExecutionCommandRecord.__table__)
        command_constraints = {constraint.name for constraint in command_table.constraints}
        assert "uq_execution_commands_actor_idempotency" in command_constraints
        outbox_table = cast(Table, OutboxEventRecord.__table__)
        outbox_indexes = {index.name for index in outbox_table.indexes}
        assert "ix_outbox_events_unpublished" in outbox_indexes
        assert OutboxEventRecord.__table__.c.sequence.primary_key

    def test_position_projection_is_account_aggregate_only(self) -> None:
        position_table = cast(Table, PositionRecord.__table__)
        assert "strategy_id" not in position_table.c
        unique_scope = next(index for index in position_table.indexes if index.name == "uq_positions_scope_symbol")
        assert [column.name for column in unique_scope.columns] == ["sub_account", "symbol"]
        assert "reduce_only" in Base.metadata.tables["risk_reservations"].c

    def test_market_making_config_and_runtime_are_version_fenced(self) -> None:
        config_table = Base.metadata.tables["strategy_config_versions"]
        config_constraints = {constraint.name for constraint in config_table.constraints}
        assert {
            "uq_strategy_config_versions_version",
            "uq_strategy_config_versions_hash",
            "uq_strategy_config_versions_id_strategy",
        } <= config_constraints

        instance_fks = {
            constraint.name for constraint in Base.metadata.tables["strategy_instances"].foreign_key_constraints
        }
        runtime_fks = {
            constraint.name for constraint in Base.metadata.tables["strategy_runtime_state"].foreign_key_constraints
        }
        assert "fk_strategy_instances_desired_config" in instance_fks
        assert "fk_strategy_runtime_state_effective_config" in runtime_fks

    def test_quote_and_execution_children_have_revision_idempotency(self) -> None:
        expected_constraints = {
            "quote_plans": "uq_quote_plans_revision",
            "quote_plan_items": "uq_quote_plan_items_ordinal",
            "quote_slots": "uq_quote_slots_key",
            "execution_command_items": "uq_execution_command_items_ordinal",
            "execution_actions": "uq_execution_actions_attempt",
        }
        for table_name, expected in expected_constraints.items():
            names = {constraint.name for constraint in Base.metadata.tables[table_name].constraints}
            assert expected in names

    def test_risk_reservations_support_multiple_child_risk_owners(self) -> None:
        table = Base.metadata.tables["risk_reservations"]
        constraints = {constraint.name for constraint in table.constraints}
        assert "uq_risk_reservations_command" not in constraints
        assert "uq_risk_reservations_command_owner" in constraints
        assert {"command_item_id", "risk_owner_type", "risk_owner_key"} <= set(table.c.keys())
        assert table.c.risk_owner_key.nullable is False

    def test_new_foreign_keys_use_restrict_and_are_indexed(self) -> None:
        new_tables = V2_TABLES - {
            "orders",
            "order_events",
            "fills",
            "positions",
            "account_state",
            "system_state",
            "risk_events",
            "risk_reservations",
            "reconciliation_runs",
            "reconciliation_diffs",
            "execution_commands",
            "inbox_events",
            "outbox_events",
            "api_audit",
        }
        for table_name in new_tables:
            table = Base.metadata.tables[table_name]
            indexed_columns = {column.name for index in table.indexes for column in index.columns} | {
                column.name for column in table.primary_key.columns
            }
            for foreign_key in table.foreign_keys:
                assert foreign_key.ondelete == "RESTRICT"
                assert foreign_key.parent.name in indexed_columns, (
                    f"missing FK index: {table_name}.{foreign_key.parent.name}"
                )

    def test_metadata_compiles_for_postgresql(self) -> None:
        dialect = postgresql.dialect()  # type: ignore[no-untyped-call]
        for table_name in V2_TABLES:
            table = Base.metadata.tables[table_name]
            assert f"CREATE TABLE {table_name}" in str(CreateTable(table).compile(dialect=dialect))
            for index in table.indexes:
                assert index.name is not None
                assert "CREATE" in str(CreateIndex(index).compile(dialect=dialect))


class TestPostgresEngineFactory:
    def test_factory_does_not_create_schema(self) -> None:
        settings = PostgresSettings(url="postgresql+asyncpg://user:pass@localhost/hypeedge")
        engine = MagicMock()
        with patch("hypeedge.storage.postgres.create_async_engine", return_value=engine) as create_engine:
            returned_engine, session_factory = create_pg_session_factory(settings)
        assert returned_engine is engine
        assert session_factory.kw["expire_on_commit"] is False
        create_engine.assert_called_once_with(
            settings.url,
            pool_size=settings.pool_size,
            pool_pre_ping=True,
            echo=False,
        )
        assert not hasattr(engine, "begin") or not engine.begin.called

    async def test_compatibility_factory_does_not_run_create_all(self) -> None:
        settings = PostgresSettings(url="postgresql+asyncpg://user:pass@localhost/hypeedge")
        session_factory = MagicMock()
        with patch(
            "hypeedge.storage.postgres.create_pg_session_factory",
            return_value=(MagicMock(), session_factory),
        ):
            result = await create_pg_engine(settings)
        assert result is session_factory


class TestPostgresUnitOfWork:
    @pytest.fixture
    def session(self) -> MagicMock:
        session = MagicMock()
        session.commit = AsyncMock()
        session.rollback = AsyncMock()
        session.close = AsyncMock()
        session.flush = AsyncMock()
        return session

    async def test_commit_and_close_share_one_session(self, session: MagicMock) -> None:
        session_factory = MagicMock(return_value=session)
        async with PostgresUnitOfWork(session_factory) as uow:
            assert uow.orders._session is session
            assert uow.commands._session is session
            assert uow.outbox._session is session
            await uow.commit()
        session.commit.assert_awaited_once()
        session.rollback.assert_not_awaited()
        session.close.assert_awaited_once()

    async def test_exception_rolls_back_and_propagates(self, session: MagicMock) -> None:
        session_factory = MagicMock(return_value=session)
        with pytest.raises(ValueError, match="abort"):
            async with PostgresUnitOfWork(session_factory):
                raise ValueError("abort")
        session.rollback.assert_awaited_once()
        session.commit.assert_not_awaited()
        session.close.assert_awaited_once()

    async def test_methods_require_active_context(self) -> None:
        uow = PostgresUnitOfWork(MagicMock())
        with pytest.raises(RuntimeError, match="not active"):
            await uow.commit()
        with pytest.raises(RuntimeError, match="not active"):
            await uow.rollback()
