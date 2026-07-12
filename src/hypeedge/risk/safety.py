"""Central trading lifecycle and permission controller."""

from __future__ import annotations

from dataclasses import dataclass

import structlog

from hypeedge.core.enums import SafetyMode
from hypeedge.core.exceptions import KillSwitchTriggeredError, OrderRejectedError
from hypeedge.core.models import OrderIntent

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class SafetyState:
    mode: SafetyMode
    reason: str | None = None


class SafetyController:
    """Single source of truth for whether an action may increase risk.

    Cancellation is deliberately not gated here: a degraded system must always
    retain the ability to remove working orders.
    """

    def __init__(self, initial_mode: SafetyMode = SafetyMode.STARTING) -> None:
        self._state = SafetyState(initial_mode)

    @property
    def mode(self) -> SafetyMode:
        return self._state.mode

    @property
    def reason(self) -> str | None:
        return self._state.reason

    @property
    def state(self) -> SafetyState:
        return self._state

    def transition(self, mode: SafetyMode, reason: str | None = None) -> None:
        previous = self._state
        self._state = SafetyState(mode=mode, reason=reason)
        logger.warning(
            "safety_mode_changed",
            old_mode=previous.mode.value,
            new_mode=mode.value,
            reason=reason,
        )

    def enter_cancel_only(self, reason: str) -> None:
        if self.mode not in {SafetyMode.HALTING, SafetyMode.HALTED}:
            self.transition(SafetyMode.CANCEL_ONLY, reason)

    def check_placement(self, intent: OrderIntent) -> None:
        """Reject placements not permitted by the current lifecycle mode."""
        if self.mode == SafetyMode.NORMAL:
            return
        if self.mode == SafetyMode.REDUCE_ONLY and intent.reduce_only:
            return
        if self.mode in {SafetyMode.HALTING, SafetyMode.HALTED}:
            raise KillSwitchTriggeredError(reason=self.reason)
        raise OrderRejectedError(
            f"Trading mode {self.mode.value} does not permit order placement",
            cloid=str(intent.cloid) if intent.cloid else None,
            reason=f"safety_mode_{self.mode.value}",
        )

    def check_emergency_close(self) -> None:
        """Allow emergency close unless the system is only reconciling/starting."""
        if self.mode in {SafetyMode.STARTING, SafetyMode.RECONCILING}:
            raise OrderRejectedError(
                f"Trading mode {self.mode.value} does not permit emergency close",
                reason=f"safety_mode_{self.mode.value}",
            )
