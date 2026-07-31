"""Durable funding-arbitrage cycle repository."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hypeedge.core.enums import FundingArbCycleState
from hypeedge.core.exceptions import StrategyLifecycleError, StrategyRegistrationError
from hypeedge.storage.postgres import (
    FundingArbCycleEventRecord,
    FundingArbCycleRecord,
    OutboxEventRecord,
    StrategyConfigVersionRecord,
)
from hypeedge.strategy.funding_arb.models import FundingArbCycle


class FundingArbCycleStore(Protocol):
    async def create(self, cycle: FundingArbCycle) -> FundingArbCycle: ...

    async def get_active(self, strategy_id: str) -> FundingArbCycle | None: ...

    async def transition(
        self,
        cycle: FundingArbCycle,
        state: FundingArbCycleState,
        event_type: str,
        *,
        payload: Mapping[str, Any] | None = None,
        **updates: Any,
    ) -> FundingArbCycle: ...


_UPDATE_FIELDS = frozenset(
    {
        "perp_open_size",
        "spot_open_size",
        "spot_entry_cloid",
        "perp_entry_cloid",
        "compensation_cloid",
        "perp_exit_cloid",
        "spot_exit_cloid",
        "error_code",
        "error_message",
    }
)
_DECIMAL_FIELDS = frozenset({"perp_open_size", "spot_open_size"})


class PostgresFundingArbCycleStore:
    """Optimistically fenced cycle projection plus append-only transition facts."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def create(self, cycle: FundingArbCycle) -> FundingArbCycle:
        async with self._session_factory() as session, session.begin():
            config_id = await session.scalar(
                select(StrategyConfigVersionRecord.id).where(
                    StrategyConfigVersionRecord.strategy_id == cycle.strategy_id,
                    StrategyConfigVersionRecord.version == cycle.config_revision,
                )
            )
            if config_id is None:
                raise StrategyRegistrationError(
                    f"Unknown funding-arb config: strategy_id={cycle.strategy_id} revision={cycle.config_revision}"
                )
            record = FundingArbCycleRecord(
                cycle_id=cycle.cycle_id,
                strategy_id=cycle.strategy_id,
                config_version_id=int(config_id),
                config_revision=cycle.config_revision,
                sub_account=cycle.sub_account,
                perp_symbol=cycle.perp_symbol,
                spot_symbol=cycle.spot_symbol,
                spot_display=cycle.spot_display,
                base_token=cycle.base_token,
                quote_token=cycle.quote_token,
                state=cycle.state.value,
                target_perp_size=cycle.target_perp_size,
                target_spot_size=cycle.target_spot_size,
                perp_open_size=cycle.perp_open_size,
                spot_open_size=cycle.spot_open_size,
                baseline_spot_size=cycle.baseline_spot_size,
                spot_entry_cloid=cycle.spot_entry_cloid,
                perp_entry_cloid=cycle.perp_entry_cloid,
                compensation_cloid=cycle.compensation_cloid,
                perp_exit_cloid=cycle.perp_exit_cloid,
                spot_exit_cloid=cycle.spot_exit_cloid,
                entry_funding_rate=cycle.entry_funding_rate,
                entry_basis_bps=cycle.entry_basis_bps,
                error_code=cycle.error_code,
                error_message=cycle.error_message,
                revision=1,
            )
            session.add(record)
            await session.flush()
            self._append_event(session, record, None, "cycle_created", {})
            return self._to_domain(record)

    async def get_active(self, strategy_id: str) -> FundingArbCycle | None:
        async with self._session_factory() as session:
            record = (
                await session.execute(
                    select(FundingArbCycleRecord)
                    .where(
                        FundingArbCycleRecord.strategy_id == strategy_id,
                        FundingArbCycleRecord.state != FundingArbCycleState.CLOSED.value,
                    )
                    .order_by(FundingArbCycleRecord.created_at.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()
            return self._to_domain(record) if record is not None else None

    async def transition(
        self,
        cycle: FundingArbCycle,
        state: FundingArbCycleState,
        event_type: str,
        *,
        payload: Mapping[str, Any] | None = None,
        **updates: Any,
    ) -> FundingArbCycle:
        unknown = set(updates) - _UPDATE_FIELDS
        if unknown:
            raise ValueError(f"Unsupported funding-arb cycle update fields: {sorted(unknown)}")
        async with self._session_factory() as session, session.begin():
            record = (
                await session.execute(
                    select(FundingArbCycleRecord)
                    .where(FundingArbCycleRecord.cycle_id == cycle.cycle_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if record is None:
                raise StrategyLifecycleError(f"Unknown funding-arb cycle: {cycle.cycle_id}")
            if record.revision != cycle.revision:
                raise StrategyLifecycleError(
                    f"Funding-arb cycle revision conflict: expected={cycle.revision} actual={record.revision}"
                )
            previous = record.state
            for name, value in updates.items():
                setattr(record, name, Decimal(str(value)) if name in _DECIMAL_FIELDS else value)
            record.state = state.value
            record.revision += 1
            now = datetime.now(UTC)
            if state == FundingArbCycleState.OPEN and record.opened_at is None:
                record.opened_at = now
            if state == FundingArbCycleState.CLOSED:
                record.closed_at = now
            self._append_event(session, record, previous, event_type, dict(payload or {}))
            await session.flush()
            return self._to_domain(record)

    @staticmethod
    def _append_event(
        session: AsyncSession,
        record: FundingArbCycleRecord,
        previous_state: str | None,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        occurred_at = datetime.now(UTC)
        session.add(
            FundingArbCycleEventRecord(
                event_id=uuid.uuid4(),
                cycle_id=record.cycle_id,
                revision=record.revision,
                event_type=event_type,
                from_state=previous_state,
                to_state=record.state,
                payload=payload,
                occurred_at=occurred_at,
            )
        )
        session.add(
            OutboxEventRecord(
                event_type=f"funding_arb.{event_type}",
                aggregate_type="funding_arb_cycle",
                aggregate_id=str(record.cycle_id),
                aggregate_revision=record.revision,
                correlation_id=str(record.cycle_id),
                payload={
                    "cycle_id": str(record.cycle_id),
                    "strategy_id": record.strategy_id,
                    "from_state": previous_state,
                    "to_state": record.state,
                    **payload,
                },
                occurred_at=occurred_at,
            )
        )

    @staticmethod
    def _to_domain(record: FundingArbCycleRecord) -> FundingArbCycle:
        return FundingArbCycle(
            cycle_id=record.cycle_id,
            strategy_id=record.strategy_id,
            config_revision=record.config_revision,
            sub_account=record.sub_account,
            perp_symbol=record.perp_symbol,
            spot_symbol=record.spot_symbol,
            spot_display=record.spot_display,
            base_token=record.base_token,
            quote_token=record.quote_token,
            state=FundingArbCycleState(record.state),
            target_perp_size=record.target_perp_size,
            target_spot_size=record.target_spot_size,
            perp_open_size=record.perp_open_size,
            spot_open_size=record.spot_open_size,
            baseline_spot_size=record.baseline_spot_size,
            entry_funding_rate=record.entry_funding_rate,
            entry_basis_bps=record.entry_basis_bps,
            revision=record.revision,
            spot_entry_cloid=record.spot_entry_cloid,
            perp_entry_cloid=record.perp_entry_cloid,
            compensation_cloid=record.compensation_cloid,
            perp_exit_cloid=record.perp_exit_cloid,
            spot_exit_cloid=record.spot_exit_cloid,
            error_code=record.error_code,
            error_message=record.error_message,
            opened_at=record.opened_at,
            closed_at=record.closed_at,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )


__all__ = ["FundingArbCycleStore", "PostgresFundingArbCycleStore"]
