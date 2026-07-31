"""Durability and optimistic-fencing tests for funding-arbitrage cycles."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from hypeedge.core.enums import FundingArbCycleState
from hypeedge.core.exceptions import StrategyLifecycleError
from hypeedge.storage.funding_arb import PostgresFundingArbCycleStore
from hypeedge.strategy.funding_arb.models import FundingArbCycle


class _Result:
    def __init__(self, value: Any) -> None:
        self._value = value

    def scalar_one_or_none(self) -> Any:
        return self._value


class _Transaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


class _Session:
    def __init__(self, record: Any) -> None:
        self._record = record

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    def begin(self) -> _Transaction:
        return _Transaction()

    async def execute(self, statement: object) -> _Result:
        del statement
        return _Result(self._record)


def _cycle(*, revision: int) -> FundingArbCycle:
    return FundingArbCycle(
        cycle_id=uuid.uuid4(),
        strategy_id="fa-1",
        config_revision=1,
        sub_account="0xabc",
        perp_symbol="HYPE",
        spot_symbol="@1035",
        spot_display="HYPE/USDC",
        base_token="HYPE",
        quote_token="USDC",
        state=FundingArbCycleState.OPEN,
        target_perp_size=Decimal("0.2"),
        target_spot_size=Decimal("0.2"),
        perp_open_size=Decimal("0.2"),
        spot_open_size=Decimal("0.2"),
        baseline_spot_size=Decimal(0),
        entry_funding_rate=Decimal("0.0002"),
        entry_basis_bps=Decimal("10"),
        revision=revision,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )


async def test_cycle_transition_rejects_stale_revision() -> None:
    cycle = _cycle(revision=1)
    record = SimpleNamespace(cycle_id=cycle.cycle_id, revision=2)
    session = _Session(record)
    store = PostgresFundingArbCycleStore(lambda: session)  # type: ignore[arg-type]

    with pytest.raises(StrategyLifecycleError, match="revision conflict"):
        await store.transition(cycle, FundingArbCycleState.EXITING_PERP, "exit_started")
