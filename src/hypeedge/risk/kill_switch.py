"""Kill switch — global emergency stop (design doc §8.2, §9.1).

When triggered:
1. Stops all strategies
2. Cancels all open orders (via registered callback)
3. Optionally closes all positions
4. Prevents any new orders from being submitted
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

import structlog

from hypeedge.core.events import EVENT_KILL_SWITCH_TRIGGERED, Event, EventBus
from hypeedge.core.exceptions import KillSwitchTriggeredError

if TYPE_CHECKING:
    from hypeedge.risk.safety import SafetyController

logger = structlog.get_logger(__name__)


class KillSwitch:
    """Global emergency stop.

    Triggered by:
    - Max drawdown exceeded
    - Manual trigger (API/CLI)
    - Risk checker crash
    - Reconciliation failure
    - Action credits exhausted
    """

    def __init__(self, event_bus: EventBus, safety_controller: SafetyController | None = None) -> None:
        self._event_bus = event_bus
        self._active = False
        self._reason: str | None = None
        self._cancel_all_fn: Callable[[], Awaitable[int]] | None = None
        self._verify_no_open_orders_fn: Callable[[], Awaitable[bool]] | None = None
        self._cancel_task: asyncio.Task[None] | None = None
        self._safety = safety_controller

    def register_cancel_all(
        self,
        fn: Callable[[], Awaitable[int]],
        verify_no_open_orders: Callable[[], Awaitable[bool]] | None = None,
    ) -> None:
        """Register the cancel-all-orders function (called during init).

        Args:
            fn: An async callable that cancels all open orders.
                Typically ``engine.cancel_all_orders``.
        """
        self._cancel_all_fn = fn
        self._verify_no_open_orders_fn = verify_no_open_orders

    @property
    def is_active(self) -> bool:
        return self._active

    @property
    def reason(self) -> str | None:
        return self._reason

    @property
    def cancellation_task(self) -> asyncio.Task[None] | None:
        """The single in-flight authoritative cancel-all task, if any."""
        return self._cancel_task

    def trigger(self, reason: str) -> None:
        """Trigger the kill switch.

        1. Set active flag (blocks all future orders via check())
        2. Publish EVENT_KILL_SWITCH_TRIGGERED
        3. Schedule cancel-all-orders as a background task
        """
        self._active = True
        self._reason = reason
        if self._safety is not None:
            from hypeedge.core.enums import SafetyMode

            self._safety.transition(SafetyMode.HALTING, reason)
        logger.critical("kill_switch_triggered", reason=reason)
        self._event_bus.publish_sync(Event(event_type=EVENT_KILL_SWITCH_TRIGGERED, payload={"reason": reason}))

        # Cancel all open orders (design doc §8.2)
        if self._cancel_all_fn:
            try:
                loop = asyncio.get_running_loop()
                if self._cancel_task is None or self._cancel_task.done():
                    self._cancel_task = loop.create_task(
                        self._cancel_all_and_close(),
                        name="kill_switch_cancel_all",
                    )
                else:
                    logger.warning("kill_switch_cancel_all_already_running")
            except RuntimeError:
                # No event loop running — fire and forget won't work
                logger.error("kill_switch_no_event_loop_cancel_failed")

    async def _cancel_all_and_close(self) -> None:
        """Cancel and verify against exchange truth before entering HALTED."""
        if self._cancel_all_fn is None:
            return
        for attempt in range(1, 4):
            try:
                cancelled = await self._cancel_all_fn()
                if self._verify_no_open_orders_fn is None:
                    logger.error("kill_switch_authoritative_verifier_missing", cancelled=cancelled)
                    return
                no_open_orders = await self._verify_no_open_orders_fn()
                logger.info(
                    "kill_switch_cancel_attempt_complete",
                    attempt=attempt,
                    cancelled=cancelled,
                    exchange_open_orders_empty=no_open_orders,
                )
                if no_open_orders:
                    if self._safety is not None:
                        from hypeedge.core.enums import SafetyMode

                        self._safety.transition(SafetyMode.HALTED, "exchange_open_orders_cleared")
                    return
            except Exception:
                logger.exception("kill_switch_cancel_all_failed", attempt=attempt)
            if attempt < 3:
                await asyncio.sleep(0.25 * attempt)

        logger.critical("kill_switch_halt_incomplete_open_orders_unverified")

    async def wait_until_halted(self) -> bool:
        """Wait for the tracked cancellation task and report authoritative halt."""
        task = self._cancel_task
        if task is not None and not task.done():
            await asyncio.shield(task)
        if self._safety is None:
            return False
        from hypeedge.core.enums import SafetyMode

        return self._safety.mode == SafetyMode.HALTED

    def reset(self, *, recovery_confirmed: bool = False) -> None:
        """Clear the latch only after the application completed recovery gates."""
        if not recovery_confirmed:
            raise RuntimeError("kill_switch_reset_requires_confirmed_recovery")
        self._active = False
        self._reason = None
        logger.info("kill_switch_reset")

    def restore_active(self, reason: str | None) -> None:
        """Restore a durable latch without claiming cancellation completed."""
        self._active = True
        self._reason = reason or "restored_durable_kill_switch"
        if self._safety is not None:
            from hypeedge.core.enums import SafetyMode

            self._safety.transition(SafetyMode.HALTED, self._reason)

    def check(self) -> None:
        """Raise KillSwitchTriggeredError if active. Call before every order."""
        if self._active:
            raise KillSwitchTriggeredError(reason=self._reason)
