"""Recoverable worker for durable signed exchange commands."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable

import structlog

from hypeedge.execution.durable import DurableCommandQueue, DurableExecutionCommand
from hypeedge.execution.engine import ExecutionEngine

logger = structlog.get_logger(__name__)


class SignedActionExecutor:
    """Claim durable commands and send them through the one NonceManager queue.

    A recovered/UNKNOWN command is never resent. It is resolved exclusively by
    querying Hyperliquid with its cloid; absence keeps the reservation active.
    """

    def __init__(
        self,
        queue: DurableCommandQueue,
        engine: ExecutionEngine,
        *,
        poll_interval_ms: int = 100,
        worker_id: str | None = None,
        fault_injector: Callable[[str, DurableExecutionCommand], None] | None = None,
    ) -> None:
        self._queue = queue
        self._engine = engine
        self._poll_interval = poll_interval_ms / 1000
        self._worker_id = worker_id or f"signed-action-{uuid.uuid4()}"
        self._fault_injector = fault_injector
        self._running = False

    async def run(self) -> None:
        self._running = True
        logger.info("signed_action_executor_started", worker_id=self._worker_id)
        try:
            while self._running:
                processed = await self.run_once()
                if not processed:
                    await asyncio.sleep(self._poll_interval)
        except asyncio.CancelledError:
            raise
        finally:
            self._running = False
            logger.info("signed_action_executor_stopped", worker_id=self._worker_id)

    async def run_once(self) -> bool:
        command = await self._queue.claim(self._worker_id)
        if command is None:
            return False
        if self._fault_injector is not None and not command.requires_resolution:
            self._fault_injector("before_send", command)

        def after_send(claimed: DurableExecutionCommand) -> None:
            if self._fault_injector is not None:
                self._fault_injector("after_send", claimed)

        if command.command_type == "cancel_order":
            resolved = await self._engine.execute_durable_cancel_command(command)
        else:
            resolved = await self._engine.execute_durable_command(command, after_send_hook=after_send)
        if not resolved:
            await self._queue.defer_unknown(command.command_id, "cloid lookup did not prove a terminal outcome")
        return True

    async def stop(self) -> None:
        self._running = False
