//! Conservative action-quota controller for market-making execution, port of
//! `src/hypeedge/risk/action_budget.py`.
//!
//! Keeps three independent ledgers: address actions (reconciled to
//! `userRateLimit`), cancel headroom (a conservative cumulative projection),
//! and IP weight (a local one-minute sliding window). No database dependency —
//! durable attempts can be replayed through `restore` after the last
//! authoritative remote snapshot.

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use hypeedge_domain::decimal::Decimal;
use hypeedge_domain::enums::ActionBudgetMode;

/// Conservative action-budget tunables. Mirrors `ActionBudgetSettings` in
/// `config/settings.py`; the config crate maps its YAML onto this struct so the
/// trading crate stays free of a config dependency (design rule: `trading`
/// depends only on `domain`/`infra`).
#[derive(Debug, Clone)]
pub struct ActionBudgetSettings {
    pub remote_snapshot_max_age_seconds: f64,
    pub remote_poll_interval_normal_seconds: f64,
    pub remote_poll_interval_conserve_seconds: f64,
    pub remote_poll_interval_critical_seconds: f64,
    pub address_conserve_threshold: u64,
    pub address_critical_threshold: u64,
    pub address_cancel_only_threshold: u64,
    pub cancel_retry_buffer: u64,
    pub close_action_reserve: u64,
    pub cancel_headroom_initial: u64,
    pub ip_weight_limit_per_minute: u64,
    pub ip_emergency_reserve: u64,
    pub runway_conserve_hours: f64,
    pub runway_critical_hours: f64,
    pub runway_cancel_only_hours: f64,
    pub minimum_marginal_usdc_per_action: f64,
    pub minimum_actions_for_economic_gate: u32,
}

impl Default for ActionBudgetSettings {
    fn default() -> Self {
        // Values mirrored from `ActionBudgetSettings::default()` in settings.rs.
        Self {
            remote_snapshot_max_age_seconds: 60.0,
            remote_poll_interval_normal_seconds: 30.0,
            remote_poll_interval_conserve_seconds: 15.0,
            remote_poll_interval_critical_seconds: 5.0,
            address_conserve_threshold: 3000,
            address_critical_threshold: 1500,
            address_cancel_only_threshold: 500,
            cancel_retry_buffer: 10,
            close_action_reserve: 5,
            cancel_headroom_initial: 10_000,
            ip_weight_limit_per_minute: 1200,
            ip_emergency_reserve: 100,
            runway_conserve_hours: 24.0,
            runway_critical_hours: 6.0,
            runway_cancel_only_hours: 1.0,
            minimum_marginal_usdc_per_action: 1.25,
            minimum_actions_for_economic_gate: 20,
        }
    }
}

/// Exchange child action classes relevant to quota policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BudgetAction {
    Place,
    Cancel,
    Modify,
    Close,
}

impl BudgetAction {
    pub fn as_str(self) -> &'static str {
        match self {
            BudgetAction::Place => "place",
            BudgetAction::Cancel => "cancel",
            BudgetAction::Modify => "modify",
            BudgetAction::Close => "close",
        }
    }
}

fn is_canonical_address(s: &str) -> bool {
    s.len() == 42 && s.starts_with("0x") && s[2..].bytes().all(|b| b.is_ascii_hexdigit())
}

/// Authoritative address quota snapshot returned by `userRateLimit`.
#[derive(Debug, Clone, PartialEq)]
pub struct RemoteActionSnapshot {
    pub quota_owner_address: String,
    pub cap: i64,
    pub used: i64,
    pub observed_at: DateTime<Utc>,
}

impl RemoteActionSnapshot {
    pub fn remaining(&self) -> i64 {
        self.cap - self.used
    }

    pub fn from_user_rate_limit(
        quota_owner_address: &str,
        payload: &serde_json::Value,
        observed_at: DateTime<Utc>,
    ) -> Result<Self, String> {
        let cap = payload
            .get("nRequestsCap")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| {
                "userRateLimit response lacks valid nRequestsCap/nRequestsUsed".to_string()
            })?;
        let used = payload
            .get("nRequestsUsed")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| {
                "userRateLimit response lacks valid nRequestsCap/nRequestsUsed".to_string()
            })?;
        let normalized = quota_owner_address.to_lowercase();
        if !is_canonical_address(&normalized) {
            return Err("quota_owner_address must be a canonical 20-byte hex address".into());
        }
        if cap < 0 || used < 0 || used > cap {
            return Err("remote action quota must satisfy 0 <= used <= cap".into());
        }
        Ok(Self {
            quota_owner_address: normalized,
            cap,
            used,
            observed_at,
        })
    }
}

/// Conservative cumulative cancel-limit projection.
#[derive(Debug, Clone, PartialEq)]
pub struct CancelHeadroomSnapshot {
    pub cap: i64,
    pub used: i64,
    pub observed_at: DateTime<Utc>,
}

impl CancelHeadroomSnapshot {
    pub fn remaining(&self) -> i64 {
        self.cap - self.used
    }
}

/// One request that actually crossed the network boundary.
#[derive(Debug, Clone, PartialEq)]
pub struct NetworkAttemptDebit {
    pub attempt_id: String,
    pub child_actions: Vec<BudgetAction>,
    pub ip_weight: i64,
    pub occurred_at: DateTime<Utc>,
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
}

impl NetworkAttemptDebit {
    pub fn address_cost(&self) -> i64 {
        self.child_actions.len() as i64
    }
    pub fn cancel_cost(&self) -> i64 {
        self.child_actions
            .iter()
            .filter(|a| **a == BudgetAction::Cancel)
            .count() as i64
    }
}

/// Organic filled volume that earns address quota; grants are excluded.
#[derive(Debug, Clone, PartialEq)]
pub struct FillCredit {
    pub volume_usdc: Decimal,
    pub occurred_at: DateTime<Utc>,
}

/// Per-strategy/symbol action allocation.
#[derive(Debug, Clone, PartialEq)]
pub struct BudgetAllocation {
    pub strategy_id: String,
    pub symbol: String,
    pub soft_limit: i64,
    pub hard_limit: i64,
}

/// A permission verdict without mutating quota state.
#[derive(Debug, Clone, PartialEq)]
pub struct BudgetPermission {
    pub allowed: bool,
    pub mode: ActionBudgetMode,
    pub reason: String,
}

/// Snapshot of the full budget view for telemetry / routes.
#[derive(Debug, Clone)]
pub struct ActionBudgetView {
    pub quota_owner_address: String,
    pub mode: ActionBudgetMode,
    pub remote_cap: i64,
    pub remote_used: i64,
    pub address_remaining: i64,
    pub required_cancel_reserve: i64,
    pub close_action_reserve: i64,
    pub placement_actions_available: i64,
    pub cancel_headroom_remaining: i64,
    pub ip_weight_remaining: i64,
    pub possible_live_orders: i64,
    pub remote_fresh: bool,
    pub cancel_headroom_fresh: bool,
    pub restored_conservatively: bool,
    pub windows: Vec<BudgetWindowStats>,
}

#[derive(Debug, Clone)]
pub struct BudgetWindowStats {
    pub window_hours: i64,
    pub burned_actions: i64,
    pub earned_actions: Decimal,
    pub fills: usize,
    pub actions_per_fill: Option<Decimal>,
    pub marginal_usdc_per_action: Option<Decimal>,
    pub net_burn_per_hour: Decimal,
    pub runway_hours: f64,
}

/// Serializable inputs needed for conservative process restart.
#[derive(Debug, Clone, Default)]
pub struct ActionBudgetRecoveryState {
    pub remote_snapshot: Option<RemoteActionSnapshot>,
    pub cancel_snapshot: Option<CancelHeadroomSnapshot>,
    pub attempts_after_snapshot: Vec<NetworkAttemptDebit>,
    pub fills: Vec<FillCredit>,
    pub allocations: Vec<BudgetAllocation>,
    pub possible_live_orders: i64,
}

/// Dispatch permission request (mirrors the `*`-keyword arguments of the
/// Python `permission`; defaults keep placement simple).
#[derive(Debug, Clone)]
pub struct PermissionRequest {
    pub action: BudgetAction,
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
    pub child_actions: i64,
    pub ip_weight: i64,
    pub risk_reducing: bool,
    pub emergency: bool,
}

impl Default for PermissionRequest {
    fn default() -> Self {
        Self {
            action: BudgetAction::Place,
            strategy_id: None,
            symbol: None,
            child_actions: 1,
            ip_weight: 1,
            risk_reducing: false,
            emergency: false,
        }
    }
}

impl PermissionRequest {
    pub fn new(action: BudgetAction) -> Self {
        Self {
            action,
            ..Default::default()
        }
    }
}

const STAT_WINDOWS_HOURS: [i64; 3] = [1, 6, 24];

/// Scope-level owner for action, cancel, and IP budgets. `Mutex`-guarded so the
/// execution engine, strategy runtimes, and telemetry routes share one ledger.
pub struct ActionBudgetController {
    quota_owner_address: String,
    settings: ActionBudgetSettings,
    remote_snapshot: Option<RemoteActionSnapshot>,
    cancel_snapshot: Option<CancelHeadroomSnapshot>,
    attempts: HashMap<String, NetworkAttemptDebit>,
    fills: Vec<FillCredit>,
    allocations: HashMap<(String, String), BudgetAllocation>,
    possible_live_orders: i64,
    forced_cancel_only: bool,
    restored_conservatively: bool,
    #[allow(dead_code)] // paid-reserve spend limits land with the admin close/recovery flow
    paid_reserve_spend: Vec<(DateTime<Utc>, Decimal)>,
}

impl ActionBudgetController {
    pub fn new(quota_owner_address: &str, settings: ActionBudgetSettings) -> Result<Self, String> {
        let owner = quota_owner_address.to_lowercase();
        if !is_canonical_address(&owner) {
            return Err("quota_owner_address must be a canonical 20-byte hex address".into());
        }
        Ok(Self {
            quota_owner_address: owner,
            settings,
            remote_snapshot: None,
            cancel_snapshot: None,
            attempts: HashMap::new(),
            fills: Vec::new(),
            allocations: HashMap::new(),
            possible_live_orders: 0,
            forced_cancel_only: true,
            restored_conservatively: false,
            paid_reserve_spend: Vec::new(),
        })
    }

    pub fn quota_owner(&self) -> &str {
        &self.quota_owner_address
    }

    pub fn set_allocation(&mut self, allocation: BudgetAllocation) {
        if allocation.soft_limit < 0 || allocation.hard_limit < allocation.soft_limit {
            return; // invalid allocation ignored
        }
        self.allocations.insert(
            (allocation.strategy_id.clone(), allocation.symbol.clone()),
            allocation,
        );
    }

    pub fn release_allocation(&mut self, strategy_id: &str, symbol: &str) {
        self.allocations
            .remove(&(strategy_id.to_string(), symbol.to_string()));
    }

    pub fn update_possible_live_orders(&mut self, count: i64) {
        if count < 0 {
            return;
        }
        self.possible_live_orders = count;
    }

    /// Accept an authoritative address snapshot and advance conservative quota facts.
    pub fn reconcile_remote(&mut self, snapshot: RemoteActionSnapshot) -> Result<(), String> {
        if snapshot.quota_owner_address != self.quota_owner_address {
            return Err("remote snapshot belongs to a different quota owner".into());
        }
        if let Some(previous) = &self.remote_snapshot {
            if snapshot.observed_at < previous.observed_at {
                return Err("remote action snapshot timestamp regressed".into());
            }
            if snapshot.used < previous.used {
                // B5: usage legitimately regresses when filled volume replenishes
                // the address quota. Accept it as a reset point rather than
                // erroring and permanently stalling the controller in CancelOnly.
                tracing::warn!(
                    prev_used = previous.used,
                    new_used = snapshot.used,
                    "action_budget_usage_regressed_resetting"
                );
            }
        }
        self.advance_cancel_headroom(&snapshot);
        self.remote_snapshot = Some(snapshot);
        self.forced_cancel_only = false;
        tracing::info!(
            quota_owner = %self.quota_owner_address,
            remote_cap = self.remote_snapshot.as_ref().map(|s| s.cap).unwrap_or(0),
            remote_used = self.remote_snapshot.as_ref().map(|s| s.used).unwrap_or(0),
            "action_budget_remote_reconciled"
        );
        Ok(())
    }

    pub fn reconcile_cancel_headroom(&mut self, snapshot: CancelHeadroomSnapshot) {
        self.cancel_snapshot = Some(snapshot);
    }

    /// Roll a configured headroom floor forward without inventing remote
    /// capacity: fold in local durable cancel debits and pessimistically charge
    /// every newly observed remote action as possible cancel usage.
    fn advance_cancel_headroom(&mut self, snapshot: &RemoteActionSnapshot) {
        let Some(cancel) = self.cancel_snapshot.clone() else {
            return;
        };
        let previous_used = self.remote_snapshot.as_ref().map(|s| s.used).unwrap_or(0);
        let remote_delta = snapshot.used - previous_used;
        // B4: start from the raw snapshot remaining, not `cancel_remaining()`.
        // `cancel_remaining()` already subtracts the local shadow cancel debits;
        // `remote_delta` includes the same cancels' address cost, so subtracting
        // both double-counts and exhausts the cancel headroom ~2-3x too fast.
        let remaining = (cancel.cap - cancel.used - remote_delta).max(0);
        self.cancel_snapshot = Some(CancelHeadroomSnapshot {
            cap: cancel.cap,
            used: cancel.cap - remaining,
            observed_at: snapshot.observed_at,
        });
    }

    /// Shadow-debit one actual request, idempotently by durable attempt id.
    /// Rejects and timeouts still burn their conservative debit, never twice.
    pub fn debit_network_attempt(&mut self, debit: NetworkAttemptDebit) -> Result<bool, String> {
        if let Some(existing) = self.attempts.get(&debit.attempt_id) {
            if existing != &debit {
                return Err("attempt_id was reused with different budget facts".into());
            }
            return Ok(false);
        }
        self.attempts.insert(debit.attempt_id.clone(), debit);
        Ok(true)
    }

    pub fn record_fill(&mut self, volume_usdc: Decimal, occurred_at: Option<DateTime<Utc>>) {
        if volume_usdc.is_negative() {
            return;
        }
        self.fills.push(FillCredit {
            volume_usdc,
            occurred_at: occurred_at.unwrap_or_else(Utc::now),
        });
    }

    /// Rebuild shadow state from a remote snapshot plus later durable facts.
    /// Any missing snapshot, conflict, or owner mismatch leaves the controller
    /// in `CancelOnly`.
    pub fn restore(&mut self, state: &ActionBudgetRecoveryState) -> bool {
        self.remote_snapshot = None;
        self.cancel_snapshot = None;
        self.attempts.clear();
        self.fills.clear();
        self.allocations.clear();
        self.possible_live_orders = state.possible_live_orders.max(0);
        self.forced_cancel_only = true;
        self.restored_conservatively = true;
        let (Some(remote), Some(cancel)) = (&state.remote_snapshot, &state.cancel_snapshot) else {
            return false;
        };
        if remote.quota_owner_address != self.quota_owner_address {
            return false;
        }
        for attempt in &state.attempts_after_snapshot {
            if attempt.occurred_at < remote.observed_at {
                return false;
            }
            if self.debit_network_attempt(attempt.clone()).is_err() {
                self.attempts.clear();
                return false;
            }
        }
        self.fills = state.fills.clone();
        for allocation in &state.allocations {
            self.set_allocation(allocation.clone());
        }
        self.remote_snapshot = Some(remote.clone());
        self.cancel_snapshot = Some(cancel.clone());
        self.forced_cancel_only = false;
        true
    }

    pub fn export_recovery_state(&self) -> ActionBudgetRecoveryState {
        let remote_at = self
            .remote_snapshot
            .as_ref()
            .map(|s| s.observed_at)
            .unwrap_or(DateTime::<Utc>::MIN_UTC);
        ActionBudgetRecoveryState {
            remote_snapshot: self.remote_snapshot.clone(),
            cancel_snapshot: self.cancel_snapshot.clone(),
            attempts_after_snapshot: self
                .attempts
                .values()
                .filter(|a| a.occurred_at >= remote_at)
                .cloned()
                .collect(),
            fills: self.fills.clone(),
            allocations: self.allocations.values().cloned().collect(),
            possible_live_orders: self.possible_live_orders,
        }
    }

    /// Return dispatch permission without mutating quota state. Cancels
    /// intentionally bypass every placement budget gate.
    pub fn permission(&self, req: &PermissionRequest) -> Result<BudgetPermission, String> {
        let action = req.action;
        let strategy_id = req.strategy_id.as_deref();
        let symbol = req.symbol.as_deref();
        let child_actions = req.child_actions;
        let ip_weight = req.ip_weight;
        let risk_reducing = req.risk_reducing;
        let emergency = req.emergency;
        if child_actions <= 0 || ip_weight < 0 {
            return Err("child_actions must be positive and ip_weight non-negative".into());
        }
        let current_mode = self.mode();
        if action == BudgetAction::Cancel {
            return Ok(BudgetPermission {
                allowed: true,
                mode: current_mode,
                reason: "cancel bypasses placement budget gates".into(),
            });
        }

        let view = self.snapshot();
        if action == BudgetAction::Close && emergency {
            let allowed =
                view.address_remaining >= child_actions && view.ip_weight_remaining >= ip_weight;
            return Ok(BudgetPermission {
                allowed,
                mode: current_mode,
                reason: if allowed {
                    "emergency close reserve".into()
                } else {
                    "quota exhausted".into()
                },
            });
        }

        if matches!(
            current_mode,
            ActionBudgetMode::CancelOnly | ActionBudgetMode::Exhausted
        ) {
            return Ok(BudgetPermission {
                allowed: false,
                mode: current_mode,
                reason: "budget mode forbids placement".into(),
            });
        }
        if current_mode == ActionBudgetMode::Critical && !risk_reducing {
            return Ok(BudgetPermission {
                allowed: false,
                mode: current_mode,
                reason: "critical mode permits only risk reduction".into(),
            });
        }
        if view.placement_actions_available < child_actions {
            return Ok(BudgetPermission {
                allowed: false,
                mode: current_mode,
                reason: "address action reserve would be consumed".into(),
            });
        }
        if view.ip_weight_remaining - ip_weight < self.settings.ip_emergency_reserve as i64 {
            return Ok(BudgetPermission {
                allowed: false,
                mode: current_mode,
                reason: "IP emergency reserve would be consumed".into(),
            });
        }
        if strategy_id.is_some() || symbol.is_some() {
            let (Some(sid), Some(sym)) = (strategy_id, symbol) else {
                return Err("strategy_id and symbol must be supplied together".into());
            };
            let Some(allocation) = self.allocations.get(&(sid.to_string(), sym.to_string())) else {
                return Ok(BudgetPermission {
                    allowed: false,
                    mode: current_mode,
                    reason: "no active strategy/symbol allocation".into(),
                });
            };
            let consumed = self.allocation_consumed(sid, sym);
            if consumed + child_actions > allocation.hard_limit {
                return Ok(BudgetPermission {
                    allowed: false,
                    mode: current_mode,
                    reason: "strategy/symbol hard allocation exhausted".into(),
                });
            }
        }
        Ok(BudgetPermission {
            allowed: true,
            mode: current_mode,
            reason: "budget available".into(),
        })
    }

    /// Gate info requests without mixing them into address-action quota.
    pub fn ip_request_permission(
        &self,
        ip_weight: i64,
        emergency: bool,
    ) -> Result<BudgetPermission, String> {
        if ip_weight <= 0 {
            return Err("ip_weight must be positive".into());
        }
        let current_mode = self.mode();
        let remaining = self.ip_remaining(Utc::now());
        let required_after = if emergency {
            0
        } else {
            self.settings.ip_emergency_reserve as i64
        };
        let allowed = remaining >= ip_weight && remaining - ip_weight >= required_after;
        Ok(BudgetPermission {
            allowed,
            mode: current_mode,
            reason: if allowed {
                "IP weight available".into()
            } else {
                "IP emergency reserve would be consumed".into()
            },
        })
    }

    pub fn mode(&self) -> ActionBudgetMode {
        self.calculate_mode(Utc::now())
    }

    pub fn next_remote_poll_interval_seconds(&self) -> f64 {
        match self.mode() {
            ActionBudgetMode::Critical
            | ActionBudgetMode::CancelOnly
            | ActionBudgetMode::Exhausted => self.settings.remote_poll_interval_critical_seconds,
            ActionBudgetMode::Conserve => self.settings.remote_poll_interval_conserve_seconds,
            ActionBudgetMode::Normal => self.settings.remote_poll_interval_normal_seconds,
        }
    }

    pub fn snapshot(&self) -> ActionBudgetView {
        let remote = &self.remote_snapshot;
        let cancel = &self.cancel_snapshot;
        let address_remaining = self.address_remaining();
        let required_cancel = self.required_cancel_reserve();
        let placement_available =
            (address_remaining - required_cancel - self.settings.close_action_reserve as i64)
                .max(0);
        let now = Utc::now();
        let windows = STAT_WINDOWS_HOURS
            .iter()
            .map(|h| self.window_stats(*h, now))
            .collect();
        ActionBudgetView {
            quota_owner_address: self.quota_owner_address.clone(),
            mode: self.calculate_mode(now),
            remote_cap: remote.as_ref().map(|s| s.cap).unwrap_or(0),
            remote_used: remote.as_ref().map(|s| s.used).unwrap_or(0),
            address_remaining,
            required_cancel_reserve: required_cancel,
            close_action_reserve: self.settings.close_action_reserve as i64,
            placement_actions_available: placement_available,
            cancel_headroom_remaining: self.cancel_remaining(),
            ip_weight_remaining: self.ip_remaining(now),
            possible_live_orders: self.possible_live_orders,
            remote_fresh: remote
                .as_ref()
                .map(|s| self.is_fresh(s.observed_at, now))
                .unwrap_or(false),
            cancel_headroom_fresh: cancel
                .as_ref()
                .map(|s| self.is_fresh(s.observed_at, now))
                .unwrap_or(false),
            restored_conservatively: self.restored_conservatively,
            windows,
        }
    }

    pub fn required_cancel_reserve(&self) -> i64 {
        self.possible_live_orders + self.settings.cancel_retry_buffer as i64
    }

    // --- Internal math (mirrors the Python private helpers) ---

    fn calculate_mode(&self, now: DateTime<Utc>) -> ActionBudgetMode {
        if self.forced_cancel_only
            || self.remote_snapshot.is_none()
            || self.cancel_snapshot.is_none()
        {
            return ActionBudgetMode::CancelOnly;
        }
        let (Some(remote), Some(cancel)) = (&self.remote_snapshot, &self.cancel_snapshot) else {
            return ActionBudgetMode::CancelOnly;
        };
        if !self.is_fresh(remote.observed_at, now) || !self.is_fresh(cancel.observed_at, now) {
            return ActionBudgetMode::CancelOnly;
        }
        let address = self.address_remaining();
        let cancel_remaining = self.cancel_remaining();
        let ip = self.ip_remaining(now);
        if address <= 0 || cancel_remaining <= 0 || ip <= 0 {
            return ActionBudgetMode::Exhausted;
        }
        let required_cancel = self.required_cancel_reserve();
        if address <= required_cancel + self.settings.close_action_reserve as i64
            || cancel_remaining <= required_cancel
            || ip <= self.settings.ip_emergency_reserve as i64
        {
            return ActionBudgetMode::CancelOnly;
        }

        let placement = address - required_cancel - self.settings.close_action_reserve as i64;
        let stats = self.window_stats(1, now);
        if placement <= self.settings.address_cancel_only_threshold as i64
            || stats.runway_hours <= self.settings.runway_cancel_only_hours
        {
            return ActionBudgetMode::CancelOnly;
        }
        if placement <= self.settings.address_critical_threshold as i64
            || stats.runway_hours <= self.settings.runway_critical_hours
        {
            return ActionBudgetMode::Critical;
        }
        if placement <= self.settings.address_conserve_threshold as i64
            || stats.runway_hours <= self.settings.runway_conserve_hours
        {
            return ActionBudgetMode::Conserve;
        }
        if stats.burned_actions >= self.settings.minimum_actions_for_economic_gate as i64
            && stats.marginal_usdc_per_action.is_some()
            && stats.marginal_usdc_per_action.unwrap()
                < Decimal::from_f64(self.settings.minimum_marginal_usdc_per_action)
                    .unwrap_or(Decimal::ZERO)
        {
            return ActionBudgetMode::Conserve;
        }
        ActionBudgetMode::Normal
    }

    fn window_stats(&self, hours: i64, now: DateTime<Utc>) -> BudgetWindowStats {
        let cutoff = now - Duration::hours(hours);
        let attempts: Vec<&NetworkAttemptDebit> = self
            .attempts
            .values()
            .filter(|a| cutoff < a.occurred_at && a.occurred_at <= now)
            .collect();
        let fills: Vec<&FillCredit> = self
            .fills
            .iter()
            .filter(|f| cutoff < f.occurred_at && f.occurred_at <= now)
            .collect();
        let burned: i64 = attempts.iter().map(|a| a.address_cost()).sum();
        let earned: Decimal = fills
            .iter()
            .fold(Decimal::ZERO, |acc, f| acc + f.volume_usdc);
        let net_burn = (Decimal::from_i128(burned as i128) - earned).max(Decimal::ZERO)
            / Decimal::from_i128(hours as i128);
        let actions_per_fill = if fills.is_empty() {
            None
        } else {
            Some(Decimal::from_i128(burned as i128) / Decimal::from_i128(fills.len() as i128))
        };
        let marginal = if burned == 0 {
            None
        } else {
            Some(earned / Decimal::from_i128(burned as i128))
        };
        let runway = if net_burn <= Decimal::ZERO {
            f64::INFINITY
        } else {
            self.placement_remaining_raw() as f64
                / net_burn.to_string().parse::<f64>().unwrap_or(1.0)
        };
        BudgetWindowStats {
            window_hours: hours,
            burned_actions: burned,
            earned_actions: earned,
            fills: fills.len(),
            actions_per_fill,
            marginal_usdc_per_action: marginal,
            net_burn_per_hour: net_burn,
            runway_hours: runway,
        }
    }

    fn address_remaining(&self) -> i64 {
        let Some(remote) = &self.remote_snapshot else {
            return 0;
        };
        (remote.remaining() - self.shadow_address_debit(remote.observed_at)).max(0)
    }

    fn cancel_remaining(&self) -> i64 {
        let Some(cancel) = &self.cancel_snapshot else {
            return 0;
        };
        let shadow: i64 = self
            .attempts
            .values()
            .filter(|a| a.occurred_at > cancel.observed_at)
            .map(|a| a.cancel_cost())
            .sum();
        (cancel.remaining() - shadow).max(0)
    }

    fn ip_remaining(&self, now: DateTime<Utc>) -> i64 {
        let cutoff = now - Duration::minutes(1);
        let used: i64 = self
            .attempts
            .values()
            .filter(|a| cutoff < a.occurred_at && a.occurred_at <= now)
            .map(|a| a.ip_weight)
            .sum();
        (self.settings.ip_weight_limit_per_minute as i64 - used).max(0)
    }

    fn shadow_address_debit(&self, observed_at: DateTime<Utc>) -> i64 {
        self.attempts
            .values()
            .filter(|a| a.occurred_at > observed_at)
            .map(|a| a.address_cost())
            .sum()
    }

    fn placement_remaining_raw(&self) -> i64 {
        (self.address_remaining()
            - self.required_cancel_reserve()
            - self.settings.close_action_reserve as i64)
            .max(0)
    }

    fn allocation_consumed(&self, strategy_id: &str, symbol: &str) -> i64 {
        self.attempts
            .values()
            .filter(|a| {
                a.strategy_id.as_deref() == Some(strategy_id) && a.symbol.as_deref() == Some(symbol)
            })
            .map(|a| a.address_cost())
            .sum()
    }

    fn is_fresh(&self, observed_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        let age = (now - observed_at).num_milliseconds() as f64 / 1000.0;
        (0.0..=self.settings.remote_snapshot_max_age_seconds).contains(&age)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(used: i64, cap: i64, at: DateTime<Utc>) -> RemoteActionSnapshot {
        RemoteActionSnapshot {
            quota_owner_address: "0x1111111111111111111111111111111111111111".into(),
            cap,
            used,
            observed_at: at,
        }
    }

    fn cancel_snapshot(used: i64, cap: i64, at: DateTime<Utc>) -> CancelHeadroomSnapshot {
        CancelHeadroomSnapshot {
            cap,
            used,
            observed_at: at,
        }
    }

    fn controller() -> ActionBudgetController {
        let addr = "0x1111111111111111111111111111111111111111";
        ActionBudgetController::new(addr, ActionBudgetSettings::default()).unwrap()
    }

    fn debit(
        id: &str,
        actions: &[BudgetAction],
        weight: i64,
        at: DateTime<Utc>,
    ) -> NetworkAttemptDebit {
        NetworkAttemptDebit {
            attempt_id: id.into(),
            child_actions: actions.to_vec(),
            ip_weight: weight,
            occurred_at: at,
            strategy_id: None,
            symbol: None,
        }
    }

    #[test]
    fn fresh_state_is_cancel_only_until_reconciled() {
        let c = controller();
        assert_eq!(c.mode(), ActionBudgetMode::CancelOnly);
        let view = c.snapshot();
        assert_eq!(view.placement_actions_available, 0);
        // Cancel is always permitted.
        let p = c
            .permission(&PermissionRequest::new(BudgetAction::Cancel))
            .unwrap();
        assert!(p.allowed);
    }

    #[test]
    fn reconcile_advances_to_normal() {
        let mut c = controller();
        let now = Utc::now();
        c.reconcile_remote(snapshot(100, 10_000, now)).unwrap();
        c.reconcile_cancel_headroom(cancel_snapshot(0, 10_000, now));
        assert_eq!(c.mode(), ActionBudgetMode::Normal);
        let p = c
            .permission(&PermissionRequest::new(BudgetAction::Place))
            .unwrap();
        assert!(p.allowed, "{}", p.reason);
    }

    #[test]
    fn stale_remote_snapshot_forces_cancel_only() {
        let mut c = controller();
        let stale = Utc::now() - Duration::seconds(120);
        c.reconcile_remote(snapshot(100, 10_000, stale)).unwrap();
        c.reconcile_cancel_headroom(cancel_snapshot(0, 10_000, stale));
        assert_eq!(c.mode(), ActionBudgetMode::CancelOnly);
    }

    #[test]
    fn cancel_headroom_not_double_counted_after_shadow_and_remote_delta() {
        // B4 regression: a cancel is shadow-debited locally AND its address cost
        // appears in the remote `used` delta — the headroom must charge it once.
        let mut c = controller();
        let t0 = Utc::now() - Duration::minutes(10);
        let t1 = Utc::now();
        c.reconcile_remote(snapshot(0, 10_000, t0)).unwrap();
        c.reconcile_cancel_headroom(cancel_snapshot(0, 1000, t0));
        // One cancel crosses the wire after the snapshot (shadow debit).
        c.debit_network_attempt(debit(
            "c1",
            &[BudgetAction::Cancel],
            1,
            t0 + Duration::seconds(1),
        ))
        .unwrap();
        // Reconcile a remote snapshot whose `used` advanced by that same cancel.
        c.reconcile_remote(snapshot(1, 10_000, t1)).unwrap();
        assert_eq!(
            c.cancel_remaining(),
            999,
            "cancel headroom must be charged exactly once (B4), not 998"
        );
    }

    #[test]
    fn usage_regression_resets_instead_of_stalling() {
        // B5 regression: filled volume replenishes the address quota, so a
        // lower `used` is legitimate. It must reset the ledger, not error and
        // permanently force CancelOnly.
        let mut c = controller();
        let t0 = Utc::now() - Duration::minutes(10);
        c.reconcile_remote(snapshot(100, 10_000, t0)).unwrap();
        assert!(!c.forced_cancel_only);
        let result = c.reconcile_remote(snapshot(50, 10_000, Utc::now()));
        assert!(result.is_ok(), "usage regression must not error (B5)");
        assert!(
            !c.forced_cancel_only,
            "controller must recover from a usage regression (B5)"
        );
    }

    #[test]
    fn exhausted_remote_quota_forbids_placement() {
        let mut c = controller();
        let now = Utc::now();
        c.reconcile_remote(snapshot(10_000, 10_000, now)).unwrap();
        c.reconcile_cancel_headroom(cancel_snapshot(0, 10_000, now));
        assert_eq!(c.mode(), ActionBudgetMode::Exhausted);
        let p = c
            .permission(&PermissionRequest::new(BudgetAction::Place))
            .unwrap();
        assert!(!p.allowed);
    }

    #[test]
    fn idempotent_debit_and_conflict_detection() {
        let mut c = controller();
        let now = Utc::now();
        let d = debit("att-1", &[BudgetAction::Place], 1, now);
        assert!(c.debit_network_attempt(d.clone()).unwrap());
        assert!(
            !c.debit_network_attempt(d.clone()).unwrap(),
            "replay is a no-op"
        );
        let conflict = debit("att-1", &[BudgetAction::Cancel], 2, now);
        assert!(
            c.debit_network_attempt(conflict).is_err(),
            "same id, different facts"
        );
    }

    #[test]
    fn restore_fails_without_snapshots() {
        let mut c = controller();
        assert!(!c.restore(&ActionBudgetRecoveryState::default()));
        assert_eq!(c.mode(), ActionBudgetMode::CancelOnly);
    }

    #[test]
    fn restore_roundtrips_recovery_state() {
        let mut c = controller();
        let now = Utc::now();
        let remote = snapshot(100, 10_000, now);
        let cancel = cancel_snapshot(0, 10_000, now);
        c.reconcile_remote(remote.clone()).unwrap();
        c.reconcile_cancel_headroom(cancel.clone());
        c.debit_network_attempt(debit(
            "att-1",
            &[BudgetAction::Place],
            1,
            now + Duration::seconds(1),
        ))
        .unwrap();

        let exported = c.export_recovery_state();
        let mut c2 = controller();
        assert!(c2.restore(&exported));
        assert_eq!(c2.mode(), ActionBudgetMode::Normal);
        assert_eq!(c2.address_remaining(), c.address_remaining());
    }

    #[test]
    fn allocation_hard_limit_blocks() {
        let mut c = controller();
        let now = Utc::now();
        c.reconcile_remote(snapshot(100, 10_000, now)).unwrap();
        c.reconcile_cancel_headroom(cancel_snapshot(0, 10_000, now));
        c.set_allocation(BudgetAllocation {
            strategy_id: "mm_1".into(),
            symbol: "BTC".into(),
            soft_limit: 2,
            hard_limit: 5,
        });
        for i in 0..5 {
            let at = now + Duration::milliseconds(100 * (i as i64 + 1));
            let mut d = debit(&format!("att-{i}"), &[BudgetAction::Place], 1, at);
            d.strategy_id = Some("mm_1".into());
            d.symbol = Some("BTC".into());
            c.debit_network_attempt(d).unwrap();
        }
        let p = c
            .permission(&PermissionRequest {
                action: BudgetAction::Place,
                strategy_id: Some("mm_1".into()),
                symbol: Some("BTC".into()),
                child_actions: 1,
                ip_weight: 1,
                risk_reducing: false,
                emergency: false,
            })
            .unwrap();
        assert!(!p.allowed, "hard allocation exhausted: {}", p.reason);
    }

    #[test]
    fn window_stats_compute_burn_and_runway() {
        let mut c = controller();
        let now = Utc::now();
        c.reconcile_remote(snapshot(100, 10_000, now)).unwrap();
        c.reconcile_cancel_headroom(cancel_snapshot(0, 10_000, now));
        // 10 placements in the last minute → high burn, finite runway.
        for i in 0..10 {
            let at = now - Duration::seconds(30 - i as i64);
            c.debit_network_attempt(debit(&format!("att-{i}"), &[BudgetAction::Place], 1, at))
                .unwrap();
        }
        let view = c.snapshot();
        let h1 = &view.windows[0];
        assert_eq!(h1.burned_actions, 10);
        assert!(
            h1.runway_hours.is_finite(),
            "burned 10 in 1h window → finite runway"
        );
        assert!(h1.runway_hours > 0.0);
    }

    #[test]
    fn from_user_rate_limit_parses_and_validates() {
        let now = Utc::now();
        let payload = serde_json::json!({"nRequestsCap": 10000, "nRequestsUsed": 250});
        let snap = RemoteActionSnapshot::from_user_rate_limit(
            "0x1111111111111111111111111111111111111111",
            &payload,
            now,
        )
        .unwrap();
        assert_eq!(snap.remaining(), 9750);
        // Invalid address rejected.
        assert!(
            RemoteActionSnapshot::from_user_rate_limit("not-an-address", &payload, now).is_err()
        );
        // used > cap rejected.
        let bad = serde_json::json!({"nRequestsCap": 100, "nRequestsUsed": 200});
        assert!(
            RemoteActionSnapshot::from_user_rate_limit(
                "0x1111111111111111111111111111111111111111",
                &bad,
                now,
            )
            .is_err()
        );
    }

    #[test]
    fn critical_mode_permits_only_risk_reduction() {
        let mut c = controller();
        let now = Utc::now();
        // Force critical: low placement headroom but above cancel-only.
        // address_remaining = 10000 - 9400 = 600; placement ≈ 600 - 10 - 5 = 585,
        // which is > cancel_only(500) and ≤ critical(1500).
        c.reconcile_remote(snapshot(9400, 10_000, now)).unwrap();
        c.reconcile_cancel_headroom(cancel_snapshot(0, 10_000, now));
        let view = c.snapshot();
        assert_eq!(
            view.mode,
            ActionBudgetMode::Critical,
            "mode: {:?}",
            view.mode
        );
        let p = c
            .permission(&PermissionRequest::new(BudgetAction::Place))
            .unwrap();
        assert!(!p.allowed, "critical blocks ordinary placement");
        let p = c
            .permission(&PermissionRequest {
                action: BudgetAction::Place,
                risk_reducing: true,
                child_actions: 1,
                ip_weight: 1,
                ..Default::default()
            })
            .unwrap();
        assert!(p.allowed, "critical permits risk-reducing placement");
    }
}
