"""Order state machine — enforces legal order lifecycle transitions."""

from __future__ import annotations

import structlog

from hypeedge.core.enums import ORDER_TRANSITIONS, TERMINAL_STATES, OrderStatus
from hypeedge.core.exceptions import InvalidStateTransition
from hypeedge.core.models import Order

logger = structlog.get_logger(__name__)


class OrderStateMachine:
    """Manages order state transitions (design doc §9.2).

    Enforces legal transitions:
    pending → submitted → acknowledged → {filled | partial_fill → filled | cancelled | expired}
    pending → rejected

    Every transition is logged and validated.
    """

    def transition(self, order: Order, new_status: OrderStatus, reason: str | None = None) -> None:
        """Attempt to transition an order to a new status.

        Raises InvalidStateTransition if the transition is illegal.
        """
        current = order.status

        if new_status not in ORDER_TRANSITIONS.get(current, set()):
            raise InvalidStateTransition(
                from_status=current.value,
                to_status=new_status.value,
                cloid=str(order.cloid),
            )

        old_status = order.status
        order.status = new_status

        logger.info(
            "order_state_transition",
            cloid=str(order.cloid),
            from_status=old_status.value,
            to_status=new_status.value,
            reason=reason,
        )

    def is_terminal(self, order: Order) -> bool:
        """Check if order is in a terminal state."""
        return order.status in TERMINAL_STATES

    def can_transition(self, order: Order, new_status: OrderStatus) -> bool:
        """Check if a transition is legal without performing it."""
        return new_status in ORDER_TRANSITIONS.get(order.status, set())
