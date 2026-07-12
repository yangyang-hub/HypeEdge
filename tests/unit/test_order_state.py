"""Tests for the order state machine."""

import pytest

from hypeedge.core.enums import OrderStatus
from hypeedge.core.exceptions import InvalidStateTransition
from hypeedge.core.models import Order
from hypeedge.core.types import Cloid, Price, Size, Symbol
from hypeedge.execution.order_state import OrderStateMachine


def make_order(status: OrderStatus = OrderStatus.PENDING) -> Order:
    """Create a test order with given status."""
    order = Order(
        cloid=Cloid("test_123"),
        symbol=Symbol("BTC"),
        side="buy",
        size=Size(1.0),
        price=Price(50000.0),
        order_type="limit",
        time_in_force="Gtc",
        status=status,
    )
    return order


class TestOrderStateMachine:
    def test_pending_to_submitted(self):
        sm = OrderStateMachine()
        order = make_order(OrderStatus.PENDING)

        sm.transition(order, OrderStatus.SUBMITTED)
        assert order.status == OrderStatus.SUBMITTED

    def test_pending_to_rejected(self):
        sm = OrderStateMachine()
        order = make_order(OrderStatus.PENDING)

        sm.transition(order, OrderStatus.REJECTED)
        assert order.status == OrderStatus.REJECTED

    def test_submitted_to_acknowledged(self):
        sm = OrderStateMachine()
        order = make_order(OrderStatus.SUBMITTED)

        sm.transition(order, OrderStatus.ACKNOWLEDGED)
        assert order.status == OrderStatus.ACKNOWLEDGED

    def test_acknowledged_to_partial_fill(self):
        sm = OrderStateMachine()
        order = make_order(OrderStatus.ACKNOWLEDGED)

        sm.transition(order, OrderStatus.PARTIAL_FILL)
        assert order.status == OrderStatus.PARTIAL_FILL

    def test_partial_fill_to_filled(self):
        sm = OrderStateMachine()
        order = make_order(OrderStatus.PARTIAL_FILL)

        sm.transition(order, OrderStatus.FILLED)
        assert order.status == OrderStatus.FILLED

    def test_acknowledged_to_expired(self):
        sm = OrderStateMachine()
        order = make_order(OrderStatus.ACKNOWLEDGED)

        sm.transition(order, OrderStatus.EXPIRED)
        assert order.status == OrderStatus.EXPIRED

    def test_invalid_transition_filled_to_cancelled(self):
        sm = OrderStateMachine()
        order = make_order(OrderStatus.FILLED)

        with pytest.raises(InvalidStateTransition) as exc_info:
            sm.transition(order, OrderStatus.CANCELLED)

        assert "filled" in str(exc_info.value)
        assert "cancelled" in str(exc_info.value)

    def test_invalid_transition_rejected_to_acknowledged(self):
        sm = OrderStateMachine()
        order = make_order(OrderStatus.REJECTED)

        with pytest.raises(InvalidStateTransition):
            sm.transition(order, OrderStatus.ACKNOWLEDGED)

    def test_is_terminal(self):
        sm = OrderStateMachine()

        for status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED]:
            order = make_order(status)
            assert sm.is_terminal(order), f"{status} should be terminal"

        for status in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.ACKNOWLEDGED, OrderStatus.PARTIAL_FILL]:
            order = make_order(status)
            assert not sm.is_terminal(order), f"{status} should not be terminal"

    def test_can_transition(self):
        sm = OrderStateMachine()
        order = make_order(OrderStatus.PENDING)

        assert sm.can_transition(order, OrderStatus.SUBMITTED) is True
        assert sm.can_transition(order, OrderStatus.FILLED) is False

    def test_full_lifecycle(self):
        """Test a complete order lifecycle: pending → submitted → acknowledged → partial → filled."""
        sm = OrderStateMachine()
        order = make_order(OrderStatus.PENDING)

        sm.transition(order, OrderStatus.SUBMITTED)
        sm.transition(order, OrderStatus.ACKNOWLEDGED)
        sm.transition(order, OrderStatus.PARTIAL_FILL)
        sm.transition(order, OrderStatus.FILLED)

        assert order.status == OrderStatus.FILLED
        assert sm.is_terminal(order)
