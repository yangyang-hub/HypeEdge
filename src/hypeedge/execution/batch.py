"""Pure durable-batch state and dispatch guard models."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum


class ChildActionType(StrEnum):
    PLACE = "place"
    CANCEL = "cancel"
    MODIFY = "modify"


class ChildOutcome(StrEnum):
    PENDING = "pending"
    DISPATCHING = "dispatching"
    SUCCEEDED = "succeeded"
    REJECTED = "rejected"
    UNKNOWN = "unknown"
    SUPERSEDED = "superseded"
    EXPIRED = "expired"
    BLOCKED = "blocked"


TERMINAL_CHILD_OUTCOMES = {
    ChildOutcome.SUCCEEDED,
    ChildOutcome.REJECTED,
    ChildOutcome.SUPERSEDED,
    ChildOutcome.EXPIRED,
    ChildOutcome.BLOCKED,
}


class BatchOutcome(StrEnum):
    PENDING = "pending"
    DISPATCHING = "dispatching"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    UNKNOWN = "unknown"
    COMPLETED = "completed"


class GuardDecision(StrEnum):
    ALLOW = "allow"
    SUPERSEDED = "superseded"
    EXPIRED = "expired"
    BLOCKED = "blocked"


@dataclass(frozen=True, slots=True)
class DispatchGuardContext:
    now: datetime
    deadline: datetime
    expected_session_id: str
    active_session_id: str
    expected_config_version: int
    active_config_version: int
    expected_plan_revision: int
    active_plan_revision: int
    expected_connection_generation: int
    active_connection_generation: int
    market_fresh: bool
    account_fresh: bool
    user_stream_fresh: bool
    postgres_fresh: bool
    safety_allows_place: bool
    lifecycle_allows_place: bool
    budget_allows_place: bool
    reservation_valid: bool
    alo_valid: bool


def evaluate_dispatch_guard(action: ChildActionType, context: DispatchGuardContext) -> GuardDecision:
    """Cancel is unconditional; risk-increasing children fail closed."""

    if action == ChildActionType.CANCEL:
        return GuardDecision.ALLOW
    if context.now >= context.deadline:
        return GuardDecision.EXPIRED
    if (
        context.expected_session_id != context.active_session_id
        or context.expected_config_version != context.active_config_version
        or context.expected_plan_revision != context.active_plan_revision
        or context.expected_connection_generation != context.active_connection_generation
    ):
        return GuardDecision.SUPERSEDED
    if not all(
        (
            context.market_fresh,
            context.account_fresh,
            context.user_stream_fresh,
            context.postgres_fresh,
            context.safety_allows_place,
            context.lifecycle_allows_place,
            context.budget_allows_place,
            context.reservation_valid,
            context.alo_valid,
        )
    ):
        return GuardDecision.BLOCKED
    return GuardDecision.ALLOW


@dataclass(frozen=True, slots=True)
class NetworkAttempt:
    attempt_id: uuid.UUID
    request_hash: str
    sent_at: datetime
    responded_at: datetime | None = None

    @classmethod
    def sent(cls, payload: bytes, *, sent_at: datetime, attempt_id: uuid.UUID | None = None) -> NetworkAttempt:
        return cls(
            attempt_id=attempt_id or uuid.uuid4(),
            request_hash=hashlib.sha256(payload).hexdigest(),
            sent_at=sent_at,
        )


@dataclass(frozen=True, slots=True)
class BatchChild:
    child_id: uuid.UUID
    ordinal: int
    action: ChildActionType
    plan_revision: int
    outcome: ChildOutcome = ChildOutcome.PENDING
    attempts: tuple[NetworkAttempt, ...] = ()
    depends_on: uuid.UUID | None = None
    resolution: str | None = None

    @property
    def actual_child_action_cost(self) -> int:
        """One debit per unique attempt that crossed the network boundary."""

        return len({attempt.attempt_id for attempt in self.attempts})

    def record_attempt(self, attempt: NetworkAttempt) -> BatchChild:
        if any(existing.attempt_id == attempt.attempt_id for existing in self.attempts):
            return self
        if self.outcome in TERMINAL_CHILD_OUTCOMES or self.outcome == ChildOutcome.UNKNOWN:
            raise ValueError("cannot resend a terminal or UNKNOWN child")
        return replace(self, outcome=ChildOutcome.DISPATCHING, attempts=(*self.attempts, attempt))

    def resolve(self, outcome: ChildOutcome, resolution: str | None = None) -> BatchChild:
        if outcome in {ChildOutcome.PENDING, ChildOutcome.DISPATCHING}:
            raise ValueError("resolve requires a result outcome")
        if self.outcome in TERMINAL_CHILD_OUTCOMES:
            if self.outcome == outcome:
                return self
            raise ValueError("conflicting result for terminal child")
        return replace(self, outcome=outcome, resolution=resolution)


@dataclass(frozen=True, slots=True)
class BatchExecutionCommand:
    command_id: uuid.UUID
    plan_revision: int
    children: tuple[BatchChild, ...]

    def __post_init__(self) -> None:
        if self.plan_revision < 0:
            raise ValueError("plan revision cannot be negative")
        if len({child.child_id for child in self.children}) != len(self.children):
            raise ValueError("batch child IDs must be unique")
        if sorted(child.ordinal for child in self.children) != list(range(len(self.children))):
            raise ValueError("batch child ordinals must be contiguous")
        child_ids = {child.child_id for child in self.children}
        if any(child.depends_on is not None and child.depends_on not in child_ids for child in self.children):
            raise ValueError("batch child dependency must belong to the same command")

    @property
    def actual_child_action_cost(self) -> int:
        return sum(child.actual_child_action_cost for child in self.children)

    @property
    def outcome(self) -> BatchOutcome:
        outcomes = {child.outcome for child in self.children}
        if not self.children or outcomes <= TERMINAL_CHILD_OUTCOMES:
            if outcomes == {ChildOutcome.SUCCEEDED} or not outcomes:
                return BatchOutcome.SUCCEEDED
            if ChildOutcome.SUCCEEDED in outcomes:
                return BatchOutcome.PARTIAL
            return BatchOutcome.COMPLETED
        if ChildOutcome.UNKNOWN in outcomes:
            return BatchOutcome.UNKNOWN
        if ChildOutcome.DISPATCHING in outcomes:
            return BatchOutcome.DISPATCHING
        return BatchOutcome.PENDING

    def replace_child(self, updated: BatchChild) -> BatchExecutionCommand:
        children = tuple(updated if child.child_id == updated.child_id else child for child in self.children)
        if children == self.children and all(child.child_id != updated.child_id for child in self.children):
            raise KeyError(updated.child_id)
        return replace(self, children=children)

    def dispatchable_children(self) -> tuple[BatchChild, ...]:
        by_id = {child.child_id: child for child in self.children}
        result: list[BatchChild] = []
        for child in self.children:
            if child.outcome != ChildOutcome.PENDING:
                continue
            if child.depends_on is None or by_id[child.depends_on].outcome == ChildOutcome.SUCCEEDED:
                result.append(child)
        return tuple(result)
