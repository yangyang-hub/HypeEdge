//! Pure durable-batch state and dispatch guard models, port of
//! `src/hypeedge/execution/batch.py`.
//!
//! These are the immutable child/batch state machines used by the
//! quote-plan worker and the durable batch executor. The dispatch guard is
//! fail-closed: any missing or unknown admission fact blocks the placement.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;

/// A child action within a durable batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChildActionType {
    Place,
    Cancel,
    Modify,
}

impl ChildActionType {
    pub fn as_str(self) -> &'static str {
        match self {
            ChildActionType::Place => "place",
            ChildActionType::Cancel => "cancel",
            ChildActionType::Modify => "modify",
        }
    }
}

/// The evaluated state of one child.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChildOutcome {
    Pending,
    Dispatching,
    Succeeded,
    Rejected,
    Unknown,
    Superseded,
    Expired,
    Blocked,
}

impl ChildOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            ChildOutcome::Pending => "pending",
            ChildOutcome::Dispatching => "dispatching",
            ChildOutcome::Succeeded => "succeeded",
            ChildOutcome::Rejected => "rejected",
            ChildOutcome::Unknown => "unknown",
            ChildOutcome::Superseded => "superseded",
            ChildOutcome::Expired => "expired",
            ChildOutcome::Blocked => "blocked",
        }
    }
}

/// Outcomes after which a child may not be resent.
pub const TERMINAL_CHILD_OUTCOMES: &[ChildOutcome] = &[
    ChildOutcome::Succeeded,
    ChildOutcome::Rejected,
    ChildOutcome::Superseded,
    ChildOutcome::Expired,
    ChildOutcome::Blocked,
];

fn is_terminal(outcome: ChildOutcome) -> bool {
    TERMINAL_CHILD_OUTCOMES.contains(&outcome)
}

/// The aggregate outcome of a batch command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchOutcome {
    Pending,
    Dispatching,
    Succeeded,
    Partial,
    Unknown,
    Completed,
}

impl BatchOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            BatchOutcome::Pending => "pending",
            BatchOutcome::Dispatching => "dispatching",
            BatchOutcome::Succeeded => "succeeded",
            BatchOutcome::Partial => "partial",
            BatchOutcome::Unknown => "unknown",
            BatchOutcome::Completed => "completed",
        }
    }
}

/// Dispatch admission decision for one child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardDecision {
    Allow,
    Superseded,
    Expired,
    Blocked,
}

impl GuardDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            GuardDecision::Allow => "allow",
            GuardDecision::Superseded => "superseded",
            GuardDecision::Expired => "expired",
            GuardDecision::Blocked => "blocked",
        }
    }
}

/// The full admission context for one quote-plan child dispatch.
#[derive(Debug, Clone, PartialEq)]
pub struct DispatchGuardContext {
    pub now: DateTime<Utc>,
    pub deadline: DateTime<Utc>,
    pub expected_session_id: String,
    pub active_session_id: String,
    pub expected_config_version: i64,
    pub active_config_version: i64,
    pub expected_plan_revision: i64,
    pub active_plan_revision: i64,
    pub expected_connection_generation: i64,
    pub active_connection_generation: i64,
    pub market_fresh: bool,
    pub account_fresh: bool,
    pub user_stream_fresh: bool,
    pub postgres_fresh: bool,
    pub safety_allows_place: bool,
    pub lifecycle_allows_place: bool,
    pub budget_allows_place: bool,
    pub reservation_valid: bool,
    pub alo_valid: bool,
}

/// Cancel is unconditional; risk-increasing children fail closed.
pub fn evaluate_dispatch_guard(
    action: ChildActionType,
    context: &DispatchGuardContext,
) -> GuardDecision {
    if action == ChildActionType::Cancel {
        return GuardDecision::Allow;
    }
    if context.now >= context.deadline {
        return GuardDecision::Expired;
    }
    if context.expected_session_id != context.active_session_id
        || context.expected_config_version != context.active_config_version
        || context.expected_plan_revision != context.active_plan_revision
        || context.expected_connection_generation != context.active_connection_generation
    {
        return GuardDecision::Superseded;
    }
    if !(context.market_fresh
        && context.account_fresh
        && context.user_stream_fresh
        && context.postgres_fresh
        && context.safety_allows_place
        && context.lifecycle_allows_place
        && context.budget_allows_place
        && context.reservation_valid
        && context.alo_valid)
    {
        return GuardDecision::Blocked;
    }
    GuardDecision::Allow
}

/// One network attempt, identified by its request payload hash.
#[derive(Debug, Clone, PartialEq)]
pub struct NetworkAttempt {
    pub attempt_id: Uuid,
    pub request_hash: String,
    pub sent_at: DateTime<Utc>,
    pub responded_at: Option<DateTime<Utc>>,
}

impl NetworkAttempt {
    /// SHA-256 of the raw request payload (mirrors `NetworkAttempt.sent`).
    pub fn sent(payload: &[u8], sent_at: DateTime<Utc>, attempt_id: Option<Uuid>) -> Self {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(payload);
        Self {
            attempt_id: attempt_id.unwrap_or_else(Uuid::new_v4),
            request_hash: format!("{:x}", hasher.finalize()),
            sent_at,
            responded_at: None,
        }
    }
}

/// One child within a batch command.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchChild {
    pub child_id: Uuid,
    pub ordinal: u32,
    pub action: ChildActionType,
    pub plan_revision: i64,
    pub outcome: ChildOutcome,
    pub attempts: Vec<NetworkAttempt>,
    pub depends_on: Option<Uuid>,
    pub resolution: Option<String>,
}

impl BatchChild {
    pub fn new(
        child_id: Uuid,
        ordinal: u32,
        action: ChildActionType,
        plan_revision: i64,
        depends_on: Option<Uuid>,
    ) -> Self {
        Self {
            child_id,
            ordinal,
            action,
            plan_revision,
            outcome: ChildOutcome::Pending,
            attempts: Vec::new(),
            depends_on,
            resolution: None,
        }
    }

    /// One debit per unique attempt that crossed the network boundary.
    pub fn actual_child_action_cost(&self) -> usize {
        self.attempts
            .iter()
            .map(|a| a.attempt_id)
            .collect::<HashSet<_>>()
            .len()
    }

    pub fn record_attempt(&self, attempt: NetworkAttempt) -> Result<Self, String> {
        if self
            .attempts
            .iter()
            .any(|a| a.attempt_id == attempt.attempt_id)
        {
            return Ok(self.clone());
        }
        if is_terminal(self.outcome) || self.outcome == ChildOutcome::Unknown {
            return Err("cannot resend a terminal or UNKNOWN child".into());
        }
        let mut updated = self.clone();
        updated.outcome = ChildOutcome::Dispatching;
        updated.attempts.push(attempt);
        Ok(updated)
    }

    pub fn resolve(
        &self,
        outcome: ChildOutcome,
        resolution: Option<String>,
    ) -> Result<Self, String> {
        if matches!(outcome, ChildOutcome::Pending | ChildOutcome::Dispatching) {
            return Err("resolve requires a result outcome".into());
        }
        if is_terminal(self.outcome) {
            if self.outcome == outcome {
                return Ok(self.clone());
            }
            return Err("conflicting result for terminal child".into());
        }
        let mut updated = self.clone();
        updated.outcome = outcome;
        updated.resolution = resolution;
        Ok(updated)
    }
}

/// A durable batch execution command with ordered children.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchExecutionCommand {
    pub command_id: Uuid,
    pub plan_revision: i64,
    pub children: Vec<BatchChild>,
}

impl BatchExecutionCommand {
    pub fn new(
        command_id: Uuid,
        plan_revision: i64,
        children: Vec<BatchChild>,
    ) -> Result<Self, String> {
        if plan_revision < 0 {
            return Err("plan revision cannot be negative".into());
        }
        let child_ids: HashSet<Uuid> = children.iter().map(|c| c.child_id).collect();
        if child_ids.len() != children.len() {
            return Err("batch child IDs must be unique".into());
        }
        let mut ordinals: Vec<u32> = children.iter().map(|c| c.ordinal).collect();
        ordinals.sort_unstable();
        let expected: Vec<u32> = (0..children.len() as u32).collect();
        if ordinals != expected {
            return Err("batch child ordinals must be contiguous".into());
        }
        for child in &children {
            if let Some(dep) = child.depends_on
                && !child_ids.contains(&dep)
            {
                return Err("batch child dependency must belong to the same command".into());
            }
        }
        Ok(Self {
            command_id,
            plan_revision,
            children,
        })
    }

    pub fn actual_child_action_cost(&self) -> usize {
        self.children
            .iter()
            .map(|c| c.actual_child_action_cost())
            .sum()
    }

    pub fn outcome(&self) -> BatchOutcome {
        if self.children.is_empty() {
            return BatchOutcome::Succeeded;
        }
        let outcomes: HashSet<ChildOutcome> = self.children.iter().map(|c| c.outcome).collect();
        if outcomes.iter().all(|o| is_terminal(*o)) {
            if outcomes == HashSet::from([ChildOutcome::Succeeded]) {
                return BatchOutcome::Succeeded;
            }
            if outcomes.contains(&ChildOutcome::Succeeded) {
                return BatchOutcome::Partial;
            }
            return BatchOutcome::Completed;
        }
        if outcomes.contains(&ChildOutcome::Unknown) {
            return BatchOutcome::Unknown;
        }
        if outcomes.contains(&ChildOutcome::Dispatching) {
            return BatchOutcome::Dispatching;
        }
        BatchOutcome::Pending
    }

    pub fn replace_child(&self, updated: BatchChild) -> Result<Self, String> {
        let mut replaced = false;
        let children = self
            .children
            .iter()
            .map(|child| {
                if child.child_id == updated.child_id {
                    replaced = true;
                    updated.clone()
                } else {
                    child.clone()
                }
            })
            .collect::<Vec<_>>();
        if !replaced {
            return Err(format!("unknown child id {}", updated.child_id));
        }
        Ok(Self {
            command_id: self.command_id,
            plan_revision: self.plan_revision,
            children,
        })
    }

    /// Children that are pending and unblocked by an unmet dependency.
    pub fn dispatchable_children(&self) -> Vec<BatchChild> {
        let by_id: HashMap<Uuid, BatchChild> = self
            .children
            .iter()
            .map(|c| (c.child_id, c.clone()))
            .collect();
        self.children
            .iter()
            .filter(|child| child.outcome == ChildOutcome::Pending)
            .filter(|child| match child.depends_on {
                None => true,
                Some(dep) => by_id
                    .get(&dep)
                    .is_some_and(|d| d.outcome == ChildOutcome::Succeeded),
            })
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000, 0).unwrap()
    }

    fn context(now: DateTime<Utc>) -> DispatchGuardContext {
        DispatchGuardContext {
            now,
            deadline: now + chrono::Duration::seconds(10),
            expected_session_id: "s1".into(),
            active_session_id: "s1".into(),
            expected_config_version: 1,
            active_config_version: 1,
            expected_plan_revision: 2,
            active_plan_revision: 2,
            expected_connection_generation: 3,
            active_connection_generation: 3,
            market_fresh: true,
            account_fresh: true,
            user_stream_fresh: true,
            postgres_fresh: true,
            safety_allows_place: true,
            lifecycle_allows_place: true,
            budget_allows_place: true,
            reservation_valid: true,
            alo_valid: true,
        }
    }

    #[test]
    fn cancel_is_always_allowed() {
        let ctx = context(now());
        assert_eq!(
            evaluate_dispatch_guard(ChildActionType::Cancel, &ctx),
            GuardDecision::Allow
        );
    }

    #[test]
    fn expired_past_deadline() {
        let ctx = context(now());
        let mut expired = ctx.clone();
        expired.deadline = now() - chrono::Duration::seconds(1);
        assert_eq!(
            evaluate_dispatch_guard(ChildActionType::Place, &expired),
            GuardDecision::Expired
        );
    }

    #[test]
    fn superseded_on_stale_revision() {
        let ctx = context(now());
        let mut stale = ctx.clone();
        stale.active_plan_revision = 1;
        assert_eq!(
            evaluate_dispatch_guard(ChildActionType::Place, &stale),
            GuardDecision::Superseded
        );
    }

    #[test]
    fn blocked_on_missing_admission_fact() {
        let ctx = context(now());
        let mut blocked = ctx.clone();
        blocked.market_fresh = false;
        assert_eq!(
            evaluate_dispatch_guard(ChildActionType::Place, &blocked),
            GuardDecision::Blocked
        );
    }

    #[test]
    fn allow_when_everything_fresh() {
        let ctx = context(now());
        assert_eq!(
            evaluate_dispatch_guard(ChildActionType::Place, &ctx),
            GuardDecision::Allow
        );
    }

    #[test]
    fn network_attempt_hash_is_stable() {
        let a1 = NetworkAttempt::sent(b"payload", now(), None);
        let a2 = NetworkAttempt::sent(b"payload", now(), None);
        assert_eq!(a1.request_hash, a2.request_hash);
        assert_ne!(a1.attempt_id, a2.attempt_id);
    }

    #[test]
    fn child_attempt_and_resolve_state_machine() {
        let child = BatchChild::new(Uuid::new_v4(), 0, ChildActionType::Place, 1, None);
        let attempt = NetworkAttempt::sent(b"x", now(), None);
        let dispatching = child.record_attempt(attempt.clone()).unwrap();
        assert_eq!(dispatching.outcome, ChildOutcome::Dispatching);
        assert_eq!(dispatching.actual_child_action_cost(), 1);
        // Duplicate attempt id is a no-op.
        let again = dispatching.record_attempt(attempt.clone()).unwrap();
        assert_eq!(again.attempts.len(), 1);
        let resolved = dispatching.resolve(ChildOutcome::Succeeded, None).unwrap();
        assert_eq!(resolved.outcome, ChildOutcome::Succeeded);
        // Resolving a terminal child with a conflicting outcome errors.
        assert!(resolved.resolve(ChildOutcome::Rejected, None).is_err());
        // Sending after terminal errors.
        assert!(
            resolved
                .record_attempt(NetworkAttempt::sent(b"y", now(), None))
                .is_err()
        );
    }

    #[test]
    fn child_requires_result_outcome_on_resolve() {
        let child = BatchChild::new(Uuid::new_v4(), 0, ChildActionType::Cancel, 1, None);
        assert!(child.resolve(ChildOutcome::Pending, None).is_err());
        assert!(child.resolve(ChildOutcome::Dispatching, None).is_err());
    }

    #[test]
    fn batch_ordinals_must_be_contiguous() {
        let a = BatchChild::new(Uuid::new_v4(), 0, ChildActionType::Place, 1, None);
        let b = BatchChild::new(Uuid::new_v4(), 5, ChildActionType::Cancel, 1, None);
        assert!(BatchExecutionCommand::new(Uuid::new_v4(), 1, vec![a, b]).is_err());
    }

    #[test]
    fn batch_dependency_must_be_internal() {
        let a = BatchChild::new(Uuid::new_v4(), 0, ChildActionType::Place, 1, None);
        let b = BatchChild::new(
            Uuid::new_v4(),
            1,
            ChildActionType::Cancel,
            1,
            Some(Uuid::new_v4()),
        );
        assert!(BatchExecutionCommand::new(Uuid::new_v4(), 1, vec![a, b]).is_err());
    }

    #[test]
    fn batch_outcome_aggregation() {
        let mut a = BatchChild::new(Uuid::new_v4(), 0, ChildActionType::Place, 1, None);
        a.outcome = ChildOutcome::Succeeded;
        let mut b = BatchChild::new(Uuid::new_v4(), 1, ChildActionType::Cancel, 1, None);
        b.outcome = ChildOutcome::Succeeded;
        let batch = BatchExecutionCommand::new(Uuid::new_v4(), 1, vec![a, b]).unwrap();
        assert_eq!(batch.outcome(), BatchOutcome::Succeeded);

        let mut a = BatchChild::new(Uuid::new_v4(), 0, ChildActionType::Place, 1, None);
        a.outcome = ChildOutcome::Succeeded;
        let mut b = BatchChild::new(Uuid::new_v4(), 1, ChildActionType::Cancel, 1, None);
        b.outcome = ChildOutcome::Rejected;
        let batch = BatchExecutionCommand::new(Uuid::new_v4(), 1, vec![a, b]).unwrap();
        assert_eq!(batch.outcome(), BatchOutcome::Partial);

        let mut a = BatchChild::new(Uuid::new_v4(), 0, ChildActionType::Place, 1, None);
        a.outcome = ChildOutcome::Unknown;
        let batch = BatchExecutionCommand::new(Uuid::new_v4(), 1, vec![a]).unwrap();
        assert_eq!(batch.outcome(), BatchOutcome::Unknown);
    }

    #[test]
    fn dispatchable_children_respect_dependencies() {
        let dep = BatchChild::new(Uuid::new_v4(), 0, ChildActionType::Cancel, 1, None);
        let dependent = BatchChild::new(
            Uuid::new_v4(),
            1,
            ChildActionType::Place,
            1,
            Some(dep.child_id),
        );
        let batch =
            BatchExecutionCommand::new(Uuid::new_v4(), 1, vec![dep.clone(), dependent.clone()])
                .unwrap();
        // Dependent blocked until the cancel succeeds.
        let dispatchable = batch.dispatchable_children();
        assert_eq!(dispatchable.len(), 1);
        assert_eq!(dispatchable[0].child_id, dep.child_id);
        let dep_succeeded = dep.resolve(ChildOutcome::Succeeded, None).unwrap();
        let batch = batch.replace_child(dep_succeeded).unwrap();
        // Only the now-unblocked dependent remains pending and dispatchable.
        let dispatchable = batch.dispatchable_children();
        assert_eq!(dispatchable.len(), 1);
        assert_eq!(dispatchable[0].child_id, dependent.child_id);
    }
}
