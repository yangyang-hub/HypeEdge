//! Order state machine, port of `src/hypeedge/execution/order_state.py`.
//!
//! Enforces the legal lifecycle transitions:
//! `pending → submitted → acknowledged → {filled | partial_fill → filled | cancelled | expired}`
//! and `pending → rejected`. The transition legality table lives on
//! [`OrderStatus::can_transition`] in `domain`; this module adds the checked
//! transition helper with logging and terminal-state queries.

use hypeedge_domain::enums::OrderStatus;
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::models::Order;

/// Order lifecycle state machine (design doc §9.2).
#[derive(Debug, Clone, Default)]
pub struct OrderStateMachine;

impl OrderStateMachine {
    pub fn new() -> Self {
        Self
    }

    /// Attempt to transition `order` to `new_status`.
    ///
    /// Returns `InvalidStateTransition` when the move is illegal. On success
    /// the order's `status` is replaced in place (mirrors the Python
    /// `transition` which mutates `order.status`).
    pub fn transition(
        &self,
        order: &mut Order,
        new_status: OrderStatus,
        reason: Option<&str>,
    ) -> Result<(), HypeEdgeError> {
        let current = order.status;
        if !current.can_transition(new_status) {
            return Err(HypeEdgeError::InvalidStateTransition {
                from: current,
                to: new_status,
                cloid: Some(order.cloid.clone()),
            });
        }
        tracing::info!(
            cloid = %order.cloid,
            from_status = %current.as_str(),
            to_status = %new_status.as_str(),
            reason = reason.unwrap_or_default(),
            "order_state_transition"
        );
        order.status = new_status;
        Ok(())
    }

    pub fn is_terminal(&self, order: &Order) -> bool {
        order.status.is_terminal()
    }

    pub fn can_transition(&self, order: &Order, new_status: OrderStatus) -> bool {
        order.status.can_transition(new_status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::enums::{OrderType, Side, TimeInForce};

    fn order(status: OrderStatus) -> Order {
        Order::new(
            "0xdead".into(),
            "BTC".into(),
            Side::Buy,
            hypeedge_domain::decimal::Size::new(hypeedge_domain::decimal::Decimal::from_str_strict(
                "1.0",
            )
            .unwrap()),
            None,
            OrderType::Limit,
            TimeInForce::Gtc,
        )
        .into_status(status)
    }

    trait IntoStatus {
        fn into_status(self, status: OrderStatus) -> Order;
    }
    impl IntoStatus for Order {
        fn into_status(mut self, status: OrderStatus) -> Order {
            self.status = status;
            self
        }
    }

    #[test]
    fn legal_transition_chain() {
        let sm = OrderStateMachine::new();
        let mut o = order(OrderStatus::Pending);
        sm.transition(&mut o, OrderStatus::Submitted, Some("submit")).unwrap();
        assert_eq!(o.status, OrderStatus::Submitted);
        sm.transition(&mut o, OrderStatus::Acknowledged, Some("ack")).unwrap();
        sm.transition(&mut o, OrderStatus::Filled, Some("fill")).unwrap();
        assert!(sm.is_terminal(&o));
    }

    #[test]
    fn pending_can_reject_directly() {
        let sm = OrderStateMachine::new();
        let mut o = order(OrderStatus::Pending);
        sm.transition(&mut o, OrderStatus::Rejected, Some("risk")).unwrap();
        assert!(sm.is_terminal(&o));
    }

    #[test]
    fn illegal_transition_returns_error_and_preserves_state() {
        let sm = OrderStateMachine::new();
        let mut o = order(OrderStatus::Pending);
        let err = sm
            .transition(&mut o, OrderStatus::Filled, Some("skip"))
            .unwrap_err();
        assert!(matches!(
            err,
            HypeEdgeError::InvalidStateTransition { from: OrderStatus::Pending, to: OrderStatus::Filled, .. }
        ));
        assert_eq!(o.status, OrderStatus::Pending, "status must not change on error");
    }

    #[test]
    fn terminal_orders_reject_further_transitions() {
        let sm = OrderStateMachine::new();
        let mut o = order(OrderStatus::Filled);
        assert!(sm.is_terminal(&o));
        assert!(
            sm.transition(&mut o, OrderStatus::Cancelled, None).is_err(),
            "a terminal order must not accept further transitions"
        );
    }
}
