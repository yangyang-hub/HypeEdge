"""Focused tests for durable market-making repositories."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.dialects import postgresql

from hypeedge.api.schemas import MarketMakerConfigCreateRequest
from hypeedge.core.exceptions import StrategyLifecycleError, StrategyRegistrationError
from hypeedge.core.types import StrategyId, SubAccount, Symbol
from hypeedge.storage.market_making import (
    MarketMakingTransactionRepository,
    PostgresMarketMakingReadRepository,
    PostgresStrategyAllocationManager,
    PostgresStrategyStateStore,
    market_maker_config_hash,
    normalize_market_maker_config,
)
from hypeedge.storage.postgres import Base, ExecutionActionRecord
from hypeedge.strategy.supervisor import InMemoryStrategyAllocationManager

SID = StrategyId("mm_btc")
SUB = SubAccount("agent_mm")
BTC = Symbol("BTC")


def _config(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "soft_inventory_notional": Decimal("100.00"),
        "hard_inventory_notional": Decimal("150"),
        "emergency_inventory_notional": Decimal("200"),
        "quote_size": Decimal("0.0010"),
        "max_depth_participation": Decimal("0.1"),
        "inventory_skew_bps": Decimal("5"),
        "max_inventory_shift_bps": Decimal("20"),
        "min_half_spread_bps": Decimal("1"),
        "toxicity_spread_bps": Decimal("10"),
        "min_expected_pnl_usdc": Decimal("0.01"),
        "min_quote_lifetime_ms": 500,
        "refresh_cooldown_ms": 250,
        "max_quote_age_ms": 10_000,
        "market_stale_after_ms": 1_000,
        "account_stale_after_ms": 5_000,
    }
    values.update(overrides)
    return values


def _session_factory(session: MagicMock) -> MagicMock:
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock(return_value=transaction)
    transaction.__aexit__ = AsyncMock(return_value=None)
    session.begin.return_value = transaction
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    return MagicMock(return_value=session)


def _result(value: object) -> MagicMock:
    result = MagicMock()
    result.scalar_one_or_none.return_value = value
    return result


def test_config_hash_is_stable_across_decimal_scale_and_key_order() -> None:
    first = _config()
    second = dict(reversed(list(_config(quote_size="0.00100", soft_inventory_notional="100").items())))
    assert market_maker_config_hash(first) == market_maker_config_hash(second)
    assert normalize_market_maker_config(first)["quote_size"] == Decimal("0.0010")
    assert normalize_market_maker_config(first)["external_reference_weight"] == Decimal("0.25")


def test_config_hash_includes_external_latency_and_markout_parameters() -> None:
    baseline = market_maker_config_hash(_config())
    assert market_maker_config_hash(_config(external_reference_weight="0.5")) != baseline
    assert market_maker_config_hash(_config(conservative_latency_seconds="0.2")) != baseline
    assert market_maker_config_hash(_config(min_markout_samples=50)) != baseline


def test_api_config_defaults_round_trip_into_typed_storage_contract() -> None:
    payload = {key: str(value) if isinstance(value, Decimal) else value for key, value in _config().items()}
    request = MarketMakerConfigCreateRequest.model_validate(payload)
    normalized = normalize_market_maker_config(request.model_dump())

    assert normalized["external_reference_weight"] == Decimal("0.25")
    assert normalized["external_max_age_seconds"] == Decimal("0.5")
    assert normalized["conservative_markout_bps"] == Decimal("1")
    assert normalized["min_markout_samples"] == 20


def test_config_contract_rejects_unpersisted_or_missing_fields() -> None:
    with pytest.raises(StrategyRegistrationError, match="missing=.*quote_size"):
        normalize_market_maker_config({key: value for key, value in _config().items() if key != "quote_size"})
    with pytest.raises(StrategyRegistrationError, match="extra=.*secret_knob"):
        normalize_market_maker_config(_config(secret_knob=1))


def test_strategy_instance_schema_has_real_optimistic_revision_and_metadata() -> None:
    table = Base.metadata.tables["strategy_instances"]
    assert table.c.revision.nullable is False
    assert table.c.metadata.nullable is False
    assert "ck_strategy_instances_revision" in {constraint.name for constraint in table.constraints}


async def test_allocation_insert_is_idempotent_and_returns_identity_fence() -> None:
    session = MagicMock()
    session.execute = AsyncMock(return_value=_result(41))
    manager = PostgresStrategyAllocationManager(_session_factory(session))

    allocation = await manager.acquire(SID, SUB, BTC)

    assert allocation.fence == 41
    statement = session.execute.await_args.args[0]
    sql = str(statement.compile(dialect=postgresql.dialect()))  # type: ignore[no-untyped-call]
    assert "ON CONFLICT DO NOTHING" in sql
    assert "RETURNING strategy_allocations.id" in sql


async def test_allocation_retry_returns_existing_fence_without_reallocation() -> None:
    existing = MagicMock(id=42, strategy_id=str(SID), sub_account=str(SUB), symbol=str(BTC))
    session = MagicMock()
    session.execute = AsyncMock(side_effect=[_result(None), _result(existing)])
    manager = PostgresStrategyAllocationManager(_session_factory(session))

    allocation = await manager.acquire(SID, SUB, BTC)

    assert allocation.fence == 42
    assert session.execute.await_count == 2


async def test_allocation_conflict_reports_authoritative_owner() -> None:
    session = MagicMock()
    session.execute = AsyncMock(side_effect=[_result(None), _result(None), _result("other_mm")])
    manager = PostgresStrategyAllocationManager(_session_factory(session))

    with pytest.raises(StrategyLifecycleError, match="owner=other_mm"):
        await manager.acquire(SID, SUB, BTC)


async def test_in_memory_protocol_concurrency_has_single_scope_owner() -> None:
    manager = InMemoryStrategyAllocationManager()

    async def acquire(strategy_id: str) -> object:
        return await manager.acquire(StrategyId(strategy_id), SUB, BTC)

    results = await __import__("asyncio").gather(acquire("mm_one"), acquire("mm_two"), return_exceptions=True)
    assert sum(not isinstance(result, Exception) for result in results) == 1
    assert sum(isinstance(result, StrategyLifecycleError) for result in results) == 1


async def test_metadata_update_uses_compare_and_swap_and_reports_actual_revision() -> None:
    session = MagicMock()
    session.execute = AsyncMock(return_value=_result(None))
    session.scalar = AsyncMock(return_value=7)
    store = PostgresStrategyStateStore(_session_factory(session))

    with pytest.raises(StrategyLifecycleError, match="expected=6 actual=7"):
        await store.update_strategy_metadata(SID, {"label": "BTC maker"}, expected_revision=6)

    statement = session.execute.await_args.args[0]
    sql = str(statement.compile(dialect=postgresql.dialect()))  # type: ignore[no-untyped-call]
    assert "strategy_instances.revision" in sql
    assert "RETURNING strategy_instances" in sql


async def test_execution_attempt_append_is_database_idempotent() -> None:
    session = MagicMock()
    session.execute = AsyncMock(return_value=_result(9))
    repository = MarketMakingTransactionRepository(session)
    action = ExecutionActionRecord(
        command_item_id=3,
        attempt=1,
        action_type="place",
        request_hash="a" * 64,
        sent_at=datetime.now(UTC),
        outcome="succeeded",
        estimated_credit_cost=1,
    )

    assert await repository.append_execution_action(action)
    statement = session.execute.await_args.args[0]
    sql = str(statement.compile(dialect=postgresql.dialect()))  # type: ignore[no-untyped-call]
    assert "ON CONFLICT (command_item_id, attempt) DO NOTHING" in sql


async def test_quotes_query_returns_authoritative_empty_instead_of_fabricating_orders() -> None:
    session = MagicMock()
    empty = MagicMock()
    empty.all.return_value = []
    session.execute = AsyncMock(return_value=empty)
    repository = PostgresMarketMakingReadRepository(_session_factory(session))
    repository._state_store = MagicMock()  # noqa: SLF001
    repository._state_store.get_strategy_instance = AsyncMock()  # noqa: SLF001

    result = await repository.get_market_making_quotes(SID)

    assert result.data == ()
    assert result.stale is False
    assert result.source == "postgres"
