//! Central trading lifecycle and permission controller, port of
//! `src/hypeedge/risk/safety.py`.
//!
//! Cancellation is deliberately not gated here: a degraded system must always
//! retain the ability to remove working orders.

use hypeedge_domain::enums::SafetyMode;
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::models::OrderIntent;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SafetyState {
    pub mode: SafetyMode,
    pub reason: Option<String>,
}

/// Single source of truth for whether an action may increase risk.
pub struct SafetyController {
    state: SafetyState,
}

impl Default for SafetyController {
    fn default() -> Self {
        Self::new(SafetyMode::Starting)
    }
}

impl SafetyController {
    pub fn new(initial_mode: SafetyMode) -> Self {
        Self {
            state: SafetyState {
                mode: initial_mode,
                reason: None,
            },
        }
    }

    pub fn mode(&self) -> SafetyMode {
        self.state.mode
    }

    pub fn reason(&self) -> Option<&str> {
        self.state.reason.as_deref()
    }

    pub fn state(&self) -> &SafetyState {
        &self.state
    }

    pub fn transition(&mut self, mode: SafetyMode, reason: Option<&str>) {
        let previous = self.state.mode;
        self.state = SafetyState {
            mode,
            reason: reason.map(|s| s.to_string()),
        };
        tracing::warn!(
            old_mode = previous.as_str(),
            new_mode = mode.as_str(),
            reason,
            "safety_mode_changed"
        );
    }

    pub fn enter_cancel_only(&mut self, reason: &str) {
        if !matches!(self.state.mode, SafetyMode::Halting | SafetyMode::Halted) {
            self.transition(SafetyMode::CancelOnly, Some(reason));
        }
    }

    /// Reject placements not permitted by the current lifecycle mode.
    pub fn check_placement(&self, intent: &OrderIntent) -> Result<(), HypeEdgeError> {
        match self.state.mode {
            SafetyMode::Normal => Ok(()),
            SafetyMode::ReduceOnly if intent.reduce_only || intent.risk_reducing => Ok(()),
            SafetyMode::Halting | SafetyMode::Halted => Err(HypeEdgeError::kill_switch_triggered(
                "kill switch triggered",
                self.state.reason.clone(),
            )),
            mode => Err(HypeEdgeError::order_rejected(
                format!(
                    "Trading mode {} does not permit order placement",
                    mode.as_str()
                ),
                intent.cloid.clone(),
                Some(format!("safety_mode_{}", mode.as_str())),
            )),
        }
    }

    /// Allow emergency close unless the system is only reconciling/starting.
    pub fn check_emergency_close(&self) -> Result<(), HypeEdgeError> {
        match self.state.mode {
            SafetyMode::Starting | SafetyMode::Reconciling => Err(HypeEdgeError::order_rejected(
                format!(
                    "Trading mode {} does not permit emergency close",
                    self.state.mode.as_str()
                ),
                None,
                Some(format!("safety_mode_{}", self.state.mode.as_str())),
            )),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::decimal::{Decimal, Size};

    fn intent(reduce_only: bool, risk_reducing: bool) -> OrderIntent {
        OrderIntent {
            symbol: "BTC".into(),
            side: hypeedge_domain::enums::Side::Buy,
            size: Size::new(Decimal::from_str_strict("0.1").unwrap()),
            price: None,
            order_type: hypeedge_domain::enums::OrderType::Limit,
            time_in_force: hypeedge_domain::enums::TimeInForce::Gtc,
            strategy_id: None,
            sub_account: None,
            reduce_only,
            cloid: Some("c".into()),
            client_id: None,
            is_spot: false,
            risk_reducing,
            max_slippage_bps: 50,
        }
    }

    #[test]
    fn normal_allows_all() {
        let ctrl = SafetyController::new(SafetyMode::Normal);
        assert!(ctrl.check_placement(&intent(false, false)).is_ok());
    }

    #[test]
    fn reduce_only_permits_risk_reducing() {
        let ctrl = SafetyController::new(SafetyMode::ReduceOnly);
        assert!(ctrl.check_placement(&intent(true, false)).is_ok());
        assert!(ctrl.check_placement(&intent(false, true)).is_ok());
        let err = ctrl.check_placement(&intent(false, false)).unwrap_err();
        assert!(err.to_string().contains("safety_mode_reduce_only"));
    }

    #[test]
    fn halted_raises_kill_switch() {
        let mut ctrl = SafetyController::new(SafetyMode::Halted);
        ctrl.transition(SafetyMode::Halted, Some("test"));
        let err = ctrl.check_placement(&intent(false, false)).unwrap_err();
        assert!(matches!(err, HypeEdgeError::KillSwitchTriggered { .. }));
    }

    #[test]
    fn enter_cancel_only_guards_halting() {
        let mut ctrl = SafetyController::new(SafetyMode::Halted);
        ctrl.enter_cancel_only("x");
        assert_eq!(ctrl.mode(), SafetyMode::Halted);
        let mut ctrl2 = SafetyController::new(SafetyMode::Normal);
        ctrl2.enter_cancel_only("degraded");
        assert_eq!(ctrl2.mode(), SafetyMode::CancelOnly);
    }

    #[test]
    fn emergency_close_blocked_in_reconciling() {
        let ctrl = SafetyController::new(SafetyMode::Reconciling);
        assert!(ctrl.check_emergency_close().is_err());
        let ctrl2 = SafetyController::new(SafetyMode::Normal);
        assert!(ctrl2.check_emergency_close().is_ok());
    }
}
