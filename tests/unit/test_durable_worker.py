"""Failure-boundary tests for the durable signed-action worker."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from hypeedge.execution.durable import DurableExecutionCommand
from hypeedge.execution.worker import SignedActionExecutor


class FakeQueue:
    def __init__(self, command: DurableExecutionCommand | None) -> None:
        self.command = command
        self.claimed = 0
        self.deferred: list[tuple[uuid.UUID, str]] = []

    async def claim(self, worker_id: str) -> DurableExecutionCommand | None:
        del worker_id
        self.claimed += 1
        command, self.command = self.command, None
        return command

    async def defer_unknown(self, command_id: uuid.UUID, reason: str) -> None:
        self.deferred.append((command_id, reason))


def _command(*, requires_resolution: bool = False) -> DurableExecutionCommand:
    return DurableExecutionCommand(
        command_id=uuid.uuid4(),
        command_type="place_order",
        payload={"cloid": "0x" + "a" * 32},
        attempt_count=2 if requires_resolution else 1,
        requires_resolution=requires_resolution,
    )


@pytest.mark.asyncio
async def test_worker_executes_claimed_command_once() -> None:
    command = _command()
    queue = FakeQueue(command)
    engine = AsyncMock()
    engine.execute_durable_command.return_value = True
    worker = SignedActionExecutor(queue, engine, worker_id="worker-a")

    assert await worker.run_once() is True
    engine.execute_durable_command.assert_awaited_once()
    assert queue.deferred == []


@pytest.mark.asyncio
async def test_crash_before_send_leaves_claim_for_lease_recovery() -> None:
    command = _command()
    queue = FakeQueue(command)
    engine = AsyncMock()

    def crash(phase: str, claimed: DurableExecutionCommand) -> None:
        assert claimed == command
        if phase == "before_send":
            raise RuntimeError("crash-before-send")

    worker = SignedActionExecutor(queue, engine, worker_id="worker-a", fault_injector=crash)
    with pytest.raises(RuntimeError, match="crash-before-send"):
        await worker.run_once()
    engine.execute_durable_command.assert_not_awaited()
    assert queue.deferred == []


@pytest.mark.asyncio
async def test_crash_after_send_does_not_requeue_for_blind_resend() -> None:
    command = _command()
    queue = FakeQueue(command)
    send_count = 0

    class Engine:
        async def execute_durable_command(self, claimed, *, after_send_hook=None):  # type: ignore[no-untyped-def]
            nonlocal send_count
            send_count += 1
            assert claimed == command
            assert after_send_hook is not None
            after_send_hook(claimed)
            return True

    def crash(phase: str, claimed: DurableExecutionCommand) -> None:
        if phase == "after_send":
            assert claimed == command
            raise RuntimeError("crash-after-send")

    worker = SignedActionExecutor(queue, Engine(), worker_id="worker-a", fault_injector=crash)  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="crash-after-send"):
        await worker.run_once()
    assert send_count == 1
    assert queue.deferred == []


@pytest.mark.asyncio
async def test_unknown_result_remains_durable_and_is_rechecked_later() -> None:
    command = _command(requires_resolution=True)
    queue = FakeQueue(command)
    engine = AsyncMock()
    engine.execute_durable_command.return_value = False
    worker = SignedActionExecutor(queue, engine, worker_id="worker-a")

    assert await worker.run_once() is True
    assert queue.deferred[0][0] == command.command_id
