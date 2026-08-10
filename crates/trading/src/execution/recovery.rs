//! UNKNOWN and orphan recovery projections, port of
//! `src/hypeedge/execution/recovery.py`.
//!
//! Pure logic over the trading quote models: a [`RecoveryOwner`] marks a live
//! risk owner that still may fill, and a [`RecoveryRegistry`] tracks how long
//! such possible-live facts have been unresolved so CANCEL_ONLY/FAULTED
//! supervisors can act on SLA breaches.

use std::time::Duration;

use chrono::{DateTime, Utc};
use hypeedge_domain::enums::OrderStatus;

use crate::trading::quotes::{QuoteRiskOwner, QuoteSlotKey};

/// Why a live order became a recovery fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryReason {
    SubmitUnknown,
    CancelUnknown,
    ModifyUnknown,
    LateOldRevision,
    UnattributedLive,
}

impl RecoveryReason {
    pub fn as_str(self) -> &'static str {
        match self {
            RecoveryReason::SubmitUnknown => "submit_unknown",
            RecoveryReason::CancelUnknown => "cancel_unknown",
            RecoveryReason::ModifyUnknown => "modify_unknown",
            RecoveryReason::LateOldRevision => "late_old_revision",
            RecoveryReason::UnattributedLive => "unattributed_live",
        }
    }
}

/// Lifecycle of one recovery fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryStatus {
    Required,
    CancelPending,
    ResolvedTerminal,
}

impl RecoveryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            RecoveryStatus::Required => "required",
            RecoveryStatus::CancelPending => "cancel_pending",
            RecoveryStatus::ResolvedTerminal => "resolved_terminal",
        }
    }
}

/// One possible-live order tracked for recovery.
#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryOwner {
    pub slot: QuoteSlotKey,
    pub owner: QuoteRiskOwner,
    pub reason: RecoveryReason,
    pub discovered_at: DateTime<Utc>,
    pub status: RecoveryStatus,
}

impl RecoveryOwner {
    pub fn new(
        slot: QuoteSlotKey,
        owner: QuoteRiskOwner,
        reason: RecoveryReason,
        discovered_at: DateTime<Utc>,
    ) -> Self {
        Self {
            slot,
            owner,
            reason,
            discovered_at,
            status: RecoveryStatus::Required,
        }
    }

    /// Whether this owner still blocks placement on its slot.
    pub fn blocks_placement(&self) -> bool {
        self.status != RecoveryStatus::ResolvedTerminal
    }

    /// Mark the cancellation as in flight. Fails for a terminal owner.
    pub fn mark_cancel_pending(&self) -> Result<Self, String> {
        if self.status == RecoveryStatus::ResolvedTerminal {
            return Err("terminal recovery owner cannot be cancelled again".into());
        }
        let mut updated = self.clone();
        updated.status = RecoveryStatus::CancelPending;
        Ok(updated)
    }

    /// Reconcile against exchange-authoritative order status.
    pub fn reconcile(&self, authoritative_status: OrderStatus) -> Self {
        let mut updated = self.clone();
        if matches!(
            authoritative_status,
            OrderStatus::Filled
                | OrderStatus::Cancelled
                | OrderStatus::Rejected
                | OrderStatus::Expired
        ) {
            updated.status = RecoveryStatus::ResolvedTerminal;
        }
        updated
    }
}

/// An immutable set of recovery owners with unique cloids.
#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryRegistry {
    owners: Vec<RecoveryOwner>,
}

impl RecoveryRegistry {
    /// Build a registry, rejecting duplicate owner cloids (mirrors
    /// `__post_init__`).
    pub fn new(owners: Vec<RecoveryOwner>) -> Result<Self, String> {
        let mut seen = std::collections::HashSet::new();
        for owner in &owners {
            if !seen.insert(owner.owner.cloid.as_str()) {
                return Err("recovery owner cloids must be unique".into());
            }
        }
        Ok(Self { owners })
    }

    pub fn empty() -> Self {
        Self { owners: Vec::new() }
    }

    pub fn owners(&self) -> &[RecoveryOwner] {
        &self.owners
    }

    /// Register a recovery fact; idempotent for an already-present cloid.
    pub fn register(&self, recovery: RecoveryOwner) -> Self {
        if self
            .owners
            .iter()
            .any(|existing| existing.owner.cloid == recovery.owner.cloid)
        {
            return self.clone();
        }
        let mut owners = self.owners.clone();
        owners.push(recovery);
        Self { owners }
    }

    /// Whether any unresolved owner blocks placement on the given slot.
    pub fn placement_blocked(&self, slot: &QuoteSlotKey) -> bool {
        self.owners
            .iter()
            .any(|owner| owner.slot == *slot && owner.blocks_placement())
    }

    /// Durable recovery facts which still represent possible live risk.
    pub fn unresolved(&self) -> Vec<&RecoveryOwner> {
        self.owners
            .iter()
            .filter(|owner| owner.blocks_placement())
            .collect()
    }

    /// Age of the oldest possible-live owner, for SLA and lifecycle gates.
    pub fn oldest_unresolved_age(&self, now: DateTime<Utc>) -> Option<Duration> {
        let unresolved = self.unresolved();
        if unresolved.is_empty() {
            return None;
        }
        let discovered_at = unresolved.iter().map(|owner| owner.discovered_at).min()?;
        let age = (now - discovered_at).to_std().unwrap_or(Duration::ZERO);
        Some(age)
    }

    /// Fail-safe signal consumed by CANCEL_ONLY/FAULTED supervisors. An SLA at
    /// or below zero is invalid.
    pub fn sla_exceeded(&self, now: DateTime<Utc>, sla: Duration) -> Result<bool, String> {
        if sla.is_zero() {
            return Err("recovery SLA must be positive".into());
        }
        let age = self.oldest_unresolved_age(now);
        Ok(age.is_some_and(|age| age > sla))
    }

    /// Reconcile one owner by cloid against exchange truth.
    pub fn reconcile(&self, cloid: &str, status: OrderStatus) -> Result<Self, String> {
        let mut found = false;
        let owners = self
            .owners
            .iter()
            .map(|owner| {
                if owner.owner.cloid == cloid {
                    found = true;
                    owner.reconcile(status)
                } else {
                    owner.clone()
                }
            })
            .collect();
        if !found {
            return Err(format!("recovery owner not found for cloid {cloid}"));
        }
        Ok(Self { owners })
    }
}

/// Classify a live risk owner that is not part of the active plan.
pub fn classify_orphan(
    owner: &QuoteRiskOwner,
    active_plan_revision: i64,
) -> Option<RecoveryReason> {
    if owner.status == OrderStatus::SubmitUnknown {
        return Some(RecoveryReason::SubmitUnknown);
    }
    if owner.status == OrderStatus::CancelUnknown {
        return Some(RecoveryReason::CancelUnknown);
    }
    if owner.plan_revision < active_plan_revision {
        return Some(RecoveryReason::LateOldRevision);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use hypeedge_domain::decimal::{Decimal, Price, Size};
    use hypeedge_domain::enums::Side;

    fn slot() -> QuoteSlotKey {
        QuoteSlotKey {
            strategy_id: "mm-btc".into(),
            symbol: "BTC".into(),
            side: Side::Buy,
            level: 1,
        }
    }

    fn owner(cloid: &str, status: OrderStatus, plan_revision: i64) -> QuoteRiskOwner {
        QuoteRiskOwner {
            order_id: Some(cloid.into()),
            cloid: cloid.into(),
            price: Price::new(Decimal::ONE),
            remaining_size: Size::new(Decimal::ONE),
            status,
            plan_revision,
            live_since: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            exchange_order_id_known: true,
        }
    }

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    #[test]
    fn recovery_reason_as_str() {
        assert_eq!(RecoveryReason::SubmitUnknown.as_str(), "submit_unknown");
        assert_eq!(
            RecoveryReason::LateOldRevision.as_str(),
            "late_old_revision"
        );
    }

    #[test]
    fn terminal_owner_cannot_be_cancelled_again() {
        let mut reg = RecoveryRegistry::new(vec![RecoveryOwner::new(
            slot(),
            owner("c1", OrderStatus::Filled, 1),
            RecoveryReason::LateOldRevision,
            at(100),
        )])
        .unwrap();
        reg = reg.reconcile("c1", OrderStatus::Filled).unwrap();
        let resolved = reg.owners()[0].clone();
        assert_eq!(resolved.status, RecoveryStatus::ResolvedTerminal);
        assert!(!resolved.blocks_placement());
        assert!(resolved.mark_cancel_pending().is_err());
    }

    #[test]
    fn register_is_idempotent_by_cloid() {
        let reg = RecoveryRegistry::empty();
        let o1 = RecoveryOwner::new(
            slot(),
            owner("c1", OrderStatus::Acknowledged, 1),
            RecoveryReason::LateOldRevision,
            at(100),
        );
        let reg = reg.register(o1.clone());
        let reg = reg.register(o1);
        assert_eq!(reg.owners().len(), 1);
    }

    #[test]
    fn duplicate_cloids_rejected() {
        let owners = vec![
            RecoveryOwner::new(
                slot(),
                owner("c1", OrderStatus::Acknowledged, 1),
                RecoveryReason::LateOldRevision,
                at(100),
            ),
            RecoveryOwner::new(
                slot(),
                owner("c1", OrderStatus::Acknowledged, 1),
                RecoveryReason::SubmitUnknown,
                at(100),
            ),
        ];
        assert!(RecoveryRegistry::new(owners).is_err());
    }

    #[test]
    fn placement_blocked_until_terminal() {
        let mut reg = RecoveryRegistry::new(vec![RecoveryOwner::new(
            slot(),
            owner("c1", OrderStatus::Acknowledged, 1),
            RecoveryReason::LateOldRevision,
            at(100),
        )])
        .unwrap();
        let s = slot();
        assert!(reg.placement_blocked(&s));
        reg = reg.reconcile("c1", OrderStatus::Cancelled).unwrap();
        assert!(!reg.placement_blocked(&s));
    }

    #[test]
    fn unresolved_filters_terminal() {
        let reg = RecoveryRegistry::new(vec![
            RecoveryOwner::new(
                slot(),
                owner("c1", OrderStatus::Acknowledged, 1),
                RecoveryReason::LateOldRevision,
                at(100),
            ),
            RecoveryOwner::new(
                slot(),
                owner("c2", OrderStatus::Acknowledged, 1),
                RecoveryReason::UnattributedLive,
                at(100),
            ),
        ])
        .unwrap();
        // Both are fresh (RecoveryStatus::Required) so both count as unresolved.
        assert_eq!(reg.unresolved().len(), 2);
        // Reconciling one to a terminal exchange status removes it.
        let reg = reg.reconcile("c1", OrderStatus::Filled).unwrap();
        assert_eq!(reg.unresolved().len(), 1);
        assert_eq!(reg.unresolved()[0].owner.cloid, "c2");
    }

    #[test]
    fn oldest_unresolved_age_and_sla() {
        let reg = RecoveryRegistry::new(vec![
            RecoveryOwner::new(
                slot(),
                owner("c1", OrderStatus::Acknowledged, 1),
                RecoveryReason::LateOldRevision,
                at(1_700_000_000),
            ),
            RecoveryOwner::new(
                slot(),
                owner("c2", OrderStatus::Acknowledged, 1),
                RecoveryReason::SubmitUnknown,
                at(1_700_000_100),
            ),
        ])
        .unwrap();
        let now = at(1_700_000_300);
        let age = reg.oldest_unresolved_age(now).unwrap();
        assert_eq!(age.as_secs(), 300);
        // age (300s) > sla (299s) => exceeded; age (300s) < sla (301s) => not.
        assert!(reg.sla_exceeded(now, Duration::from_secs(299)).unwrap());
        assert!(!reg.sla_exceeded(now, Duration::from_secs(301)).unwrap());
        assert!(reg.sla_exceeded(now, Duration::ZERO).is_err());

        // Once reconciled to terminal, no unresolved owners remain.
        let reg = reg.reconcile("c1", OrderStatus::Filled).unwrap();
        let reg = reg.reconcile("c2", OrderStatus::Cancelled).unwrap();
        assert!(reg.oldest_unresolved_age(now).is_none());
        assert!(!reg.sla_exceeded(now, Duration::from_secs(1)).unwrap());
    }

    #[test]
    fn sla_absent_when_no_unresolved() {
        let reg = RecoveryRegistry::empty();
        assert!(reg.oldest_unresolved_age(at(200)).is_none());
        assert!(!reg.sla_exceeded(at(200), Duration::from_secs(1)).unwrap());
    }

    #[test]
    fn reconcile_unknown_cloid_is_error() {
        let reg = RecoveryRegistry::empty();
        assert!(reg.reconcile("missing", OrderStatus::Filled).is_err());
    }

    #[test]
    fn classify_orphan_reasons() {
        let unknown = owner("c1", OrderStatus::SubmitUnknown, 5);
        assert_eq!(
            classify_orphan(&unknown, 5),
            Some(RecoveryReason::SubmitUnknown)
        );
        let cancel_unknown = owner("c2", OrderStatus::CancelUnknown, 5);
        assert_eq!(
            classify_orphan(&cancel_unknown, 5),
            Some(RecoveryReason::CancelUnknown)
        );
        let late = owner("c3", OrderStatus::Acknowledged, 3);
        assert_eq!(
            classify_orphan(&late, 5),
            Some(RecoveryReason::LateOldRevision)
        );
        let current = owner("c4", OrderStatus::Acknowledged, 5);
        assert_eq!(classify_orphan(&current, 5), None);
    }

    #[test]
    fn mark_cancel_pending_transitions() {
        let o = RecoveryOwner::new(
            slot(),
            owner("c1", OrderStatus::Acknowledged, 1),
            RecoveryReason::LateOldRevision,
            at(100),
        );
        let pending = o.mark_cancel_pending().unwrap();
        assert_eq!(pending.status, RecoveryStatus::CancelPending);
        assert!(pending.blocks_placement());
        assert!(o.mark_cancel_pending().is_ok());
    }
}
