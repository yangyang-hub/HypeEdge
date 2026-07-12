"""Durable batch child-result and dispatch guard tests."""

from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from hypeedge.execution.batch import (
    BatchChild,
    BatchExecutionCommand,
    BatchOutcome,
    ChildActionType,
    ChildOutcome,
    DispatchGuardContext,
    GuardDecision,
    NetworkAttempt,
    evaluate_dispatch_guard,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def context(**overrides: object) -> DispatchGuardContext:
    values: dict[str, object] = {
        "now": NOW,
        "deadline": NOW + timedelta(seconds=1),
        "expected_session_id": "s1",
        "active_session_id": "s1",
        "expected_config_version": 2,
        "active_config_version": 2,
        "expected_plan_revision": 4,
        "active_plan_revision": 4,
        "expected_connection_generation": 7,
        "active_connection_generation": 7,
        "market_fresh": True,
        "account_fresh": True,
        "user_stream_fresh": True,
        "postgres_fresh": True,
        "safety_allows_place": True,
        "lifecycle_allows_place": True,
        "budget_allows_place": True,
        "reservation_valid": True,
        "alo_valid": True,
    }
    values.update(overrides)
    return DispatchGuardContext(**values)  # type: ignore[arg-type]


def child(ordinal: int, action: ChildActionType, *, depends_on: uuid.UUID | None = None) -> BatchChild:
    return BatchChild(uuid.uuid4(), ordinal, action, 4, depends_on=depends_on)


def test_cancel_always_passes_dispatch_guard() -> None:
    failed = context(
        deadline=NOW - timedelta(seconds=1),
        active_session_id="different",
        market_fresh=False,
        safety_allows_place=False,
        alo_valid=False,
    )
    assert evaluate_dispatch_guard(ChildActionType.CANCEL, failed) == GuardDecision.ALLOW


@pytest.mark.parametrize(
    ("override", "expected"),
    [
        ({"deadline": NOW}, GuardDecision.EXPIRED),
        ({"active_plan_revision": 5}, GuardDecision.SUPERSEDED),
        ({"active_connection_generation": 8}, GuardDecision.SUPERSEDED),
        ({"active_config_version": 3}, GuardDecision.SUPERSEDED),
        ({"market_fresh": False}, GuardDecision.BLOCKED),
        ({"reservation_valid": False}, GuardDecision.BLOCKED),
        ({"alo_valid": False}, GuardDecision.BLOCKED),
    ],
)
def test_place_dispatch_guard_fails_closed(override: dict[str, object], expected: GuardDecision) -> None:
    assert evaluate_dispatch_guard(ChildActionType.PLACE, context(**override)) == expected


def test_each_unique_network_attempt_costs_one_even_timeout_or_reject() -> None:
    quote = child(0, ChildActionType.PLACE)
    attempt_id = uuid.uuid4()
    attempt = NetworkAttempt.sent(b"payload", sent_at=NOW, attempt_id=attempt_id)
    quote = quote.record_attempt(attempt).record_attempt(attempt)
    assert quote.actual_child_action_cost == 1
    quote = quote.resolve(ChildOutcome.UNKNOWN, "timeout")
    assert quote.actual_child_action_cost == 1
    with pytest.raises(ValueError, match="UNKNOWN"):
        quote.record_attempt(NetworkAttempt.sent(b"retry", sent_at=NOW))


def test_child_resolution_rejects_non_results_and_terminal_resend() -> None:
    quote = child(0, ChildActionType.PLACE)
    with pytest.raises(ValueError, match="result outcome"):
        quote.resolve(ChildOutcome.PENDING)
    succeeded = quote.resolve(ChildOutcome.SUCCEEDED)
    assert succeeded.resolve(ChildOutcome.SUCCEEDED) is succeeded
    with pytest.raises(ValueError, match="terminal"):
        succeeded.record_attempt(NetworkAttempt.sent(b"retry", sent_at=NOW))


def test_cancel_then_place_dependency_does_not_release_before_cancel_success() -> None:
    cancel = child(0, ChildActionType.CANCEL)
    place = child(1, ChildActionType.PLACE, depends_on=cancel.child_id)
    batch = BatchExecutionCommand(uuid.uuid4(), 4, (cancel, place))
    assert batch.dispatchable_children() == (cancel,)

    cancel_unknown = cancel.resolve(ChildOutcome.UNKNOWN, "timeout")
    batch = batch.replace_child(cancel_unknown)
    assert batch.outcome == BatchOutcome.UNKNOWN
    assert batch.dispatchable_children() == ()

    cancel_success = replace(cancel_unknown, outcome=ChildOutcome.SUCCEEDED)
    batch = batch.replace_child(cancel_success)
    assert batch.dispatchable_children() == (place,)


def test_partial_and_out_of_order_results_are_aggregated_per_child() -> None:
    first = child(0, ChildActionType.CANCEL).resolve(ChildOutcome.SUCCEEDED)
    second = child(1, ChildActionType.CANCEL).resolve(ChildOutcome.REJECTED)
    batch = BatchExecutionCommand(uuid.uuid4(), 4, (first, second))
    assert batch.outcome == BatchOutcome.PARTIAL
    assert batch.replace_child(first) == batch  # duplicate identical result is idempotent

    with pytest.raises(ValueError, match="conflicting"):
        first.resolve(ChildOutcome.REJECTED)


def test_batch_outcomes_cover_pending_dispatching_success_and_completed() -> None:
    pending = child(0, ChildActionType.PLACE)
    assert BatchExecutionCommand(uuid.uuid4(), 4, ()).outcome == BatchOutcome.SUCCEEDED
    assert BatchExecutionCommand(uuid.uuid4(), 4, (pending,)).outcome == BatchOutcome.PENDING
    dispatching = pending.record_attempt(NetworkAttempt.sent(b"place", sent_at=NOW))
    assert BatchExecutionCommand(uuid.uuid4(), 4, (dispatching,)).outcome == BatchOutcome.DISPATCHING
    succeeded = pending.resolve(ChildOutcome.SUCCEEDED)
    assert BatchExecutionCommand(uuid.uuid4(), 4, (succeeded,)).outcome == BatchOutcome.SUCCEEDED
    rejected = pending.resolve(ChildOutcome.REJECTED)
    assert BatchExecutionCommand(uuid.uuid4(), 4, (rejected,)).outcome == BatchOutcome.COMPLETED


def test_invalid_batch_shape_and_missing_replacement_are_rejected() -> None:
    first = child(0, ChildActionType.CANCEL)
    with pytest.raises(ValueError, match="revision"):
        BatchExecutionCommand(uuid.uuid4(), -1, ())
    with pytest.raises(ValueError, match="unique"):
        BatchExecutionCommand(uuid.uuid4(), 1, (first, replace(first, ordinal=1)))
    with pytest.raises(ValueError, match="contiguous"):
        BatchExecutionCommand(uuid.uuid4(), 1, (replace(first, ordinal=2),))
    missing = child(1, ChildActionType.PLACE, depends_on=uuid.uuid4())
    with pytest.raises(ValueError, match="dependency"):
        BatchExecutionCommand(uuid.uuid4(), 1, (first, missing))
    batch = BatchExecutionCommand(uuid.uuid4(), 1, (first,))
    with pytest.raises(KeyError):
        batch.replace_child(child(0, ChildActionType.CANCEL))


def test_default_attempt_id_and_all_guard_freshness_dimensions() -> None:
    assert NetworkAttempt.sent(b"x", sent_at=NOW).attempt_id
    assert evaluate_dispatch_guard(ChildActionType.PLACE, context()) == GuardDecision.ALLOW
    for field in (
        "account_fresh",
        "user_stream_fresh",
        "postgres_fresh",
        "safety_allows_place",
        "lifecycle_allows_place",
        "budget_allows_place",
    ):
        assert evaluate_dispatch_guard(ChildActionType.PLACE, context(**{field: False})) == GuardDecision.BLOCKED


def test_batch_action_cost_is_children_not_http_request_count() -> None:
    children = tuple(child(index, ChildActionType.CANCEL) for index in range(3))
    attempt_id = uuid.uuid4()
    sent = tuple(
        item.record_attempt(
            NetworkAttempt.sent(f"item-{index}".encode(), sent_at=NOW, attempt_id=uuid.uuid5(attempt_id, str(index)))
        )
        for index, item in enumerate(children)
    )
    batch = BatchExecutionCommand(uuid.uuid4(), 4, sent)
    assert batch.actual_child_action_cost == 3
