//! Risk controls: safety lifecycle, kill switch, the risk checker, the
//! action-quota controller, and the canary release gates.
//!
//! Ports `src/hypeedge/risk/`.

pub mod action_budget;
pub mod canary;
pub mod checker;
pub mod kill_switch;
pub mod safety;

pub use action_budget::{
    ActionBudgetController, ActionBudgetRecoveryState, ActionBudgetView, BudgetAction,
    BudgetAllocation, BudgetPermission, BudgetWindowStats, CancelHeadroomSnapshot, FillCredit,
    NetworkAttemptDebit, PermissionRequest, RemoteActionSnapshot,
};
pub use canary::{
    CanaryDirective, CanaryGateEvaluator, CanaryObservation, CanaryRiskEnvelope, ExpansionEvidence,
    GateDecision, ReleaseEvidence,
};
pub use checker::{AccountView, RiskChecker};
pub use kill_switch::KillSwitch;
pub use safety::{SafetyController, SafetyState};
