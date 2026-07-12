"""Durable order journal boundary used by the execution engine."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from hypeedge.core.models import Order, RiskCheckResult


class DurableOrderStore(Protocol):
    """Persist trading intent before exchange side effects and journal outcomes."""

    async def persist_placement(
        self,
        order: Order,
        risk_result: RiskCheckResult,
        *,
        command_id: uuid.UUID,
        dispatch: bool,
        reference_price: float | None = None,
        price_observed_at: datetime | None = None,
    ) -> RiskCheckResult | None:
        """Atomically persist an order, risk decision, command, event, and outbox event."""
        ...

    async def persist_transition(
        self,
        order: Order,
        event_type: str,
        *,
        command_id: uuid.UUID | None = None,
        command_status: str | None = None,
    ) -> None:
        """Persist the current order projection and append its transition events."""
        ...

    async def persist_cancel_requested(self, order: Order, *, command_id: uuid.UUID) -> None:
        """Persist a cancellation command before calling the exchange."""
        ...

    async def persist_reconciled_order(self, order: Order) -> None:
        """Ensure an exchange-authoritative order exists before mutating it."""
        ...

    async def load_open_orders(self) -> list[Order]:
        """Load non-terminal orders during startup recovery."""
        ...

    async def get_order(self, cloid: str) -> Order | None:
        """Look up any durable order, including terminal states, for idempotency."""
        ...


@dataclass(frozen=True)
class DurableExecutionCommand:
    """A command claimed by the sole signed-action worker."""

    command_id: uuid.UUID
    command_type: str
    payload: dict[str, Any]
    attempt_count: int
    requires_resolution: bool


class DurableCommandQueue(Protocol):
    """Lease-based command queue; implementations must claim with SKIP LOCKED."""

    async def claim(self, worker_id: str) -> DurableExecutionCommand | None:
        """Claim one command, recovering expired processing leases as UNKNOWN."""
        ...

    async def defer_unknown(self, command_id: uuid.UUID, reason: str) -> None:
        """Keep an ambiguous command UNKNOWN and schedule a later cloid lookup."""
        ...
