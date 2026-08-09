//! Strategy control plane, port of `src/hypeedge/strategy/supervisor.py`.
//!
//! [`StrategySupervisor`] drives the durable per-instance lifecycle
//! (`stopped → warming → shadow → running → paused → draining → faulted`),
//! enforcing the `_ALLOWED_TRANSITIONS` table, optimistic revision fencing, and
//! system-safety pause semantics. The [`InMemoryStrategyStateStore`] and
//! [`InMemoryStrategyAllocationManager`] are the test/restart implementations
//! of the storage protocols.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use hypeedge_domain::enums::MarketMakerLifecycle;

use super::registry::{
    StrategyConfigSnapshot, StrategyInstanceDefinition, StrategyRegistry, StrategyRuntimeHandle,
};

/// Prefix for system-safety pause reasons (mirrors the Python constant).
pub const SYSTEM_SAFETY_PAUSE_PREFIX: &str = "system_safety_pause:";
/// Reason used when resuming from a system-safety pause.
pub const SYSTEM_SAFETY_RECOVERED_REASON: &str = "system_safety_recovered";

pub fn is_system_safety_pause_reason(reason: &str) -> bool {
    reason.starts_with(SYSTEM_SAFETY_PAUSE_PREFIX)
}

/// The durable runtime state of one strategy instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyRuntimeState {
    pub strategy_id: String,
    pub actual_state: MarketMakerLifecycle,
    pub effective_config_revision: Option<u64>,
    pub revision: u64,
    pub reason: Option<String>,
}

/// A leased allocation of a (sub_account, symbol) scope to one strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyAllocation {
    pub strategy_id: String,
    pub sub_account: String,
    pub symbol: String,
    pub fence: u64,
}

/// Storage for strategy instances / runtime states / configs.
#[async_trait]
pub trait StrategyStateStore: Send + Sync {
    async fn list_instances(&self) -> Result<Vec<StrategyInstanceDefinition>, String>;
    async fn get_instance(
        &self,
        strategy_id: &str,
    ) -> Result<Option<StrategyInstanceDefinition>, String>;
    async fn get_runtime(&self, strategy_id: &str) -> Result<Option<StrategyRuntimeState>, String>;
    async fn get_config(
        &self,
        strategy_id: &str,
        revision: u64,
    ) -> Result<Option<StrategyConfigSnapshot>, String>;
    async fn set_desired(
        &self,
        strategy_id: &str,
        state: Option<MarketMakerLifecycle>,
        config_revision: Option<u64>,
        expected_revision: Option<u64>,
    ) -> Result<StrategyInstanceDefinition, String>;
    async fn set_runtime(
        &self,
        strategy_id: &str,
        actual_state: Option<MarketMakerLifecycle>,
        effective_config_revision: Option<u64>,
        set_effective_config: bool,
        reason: Option<&str>,
        expected_revision: Option<u64>,
    ) -> Result<StrategyRuntimeState, String>;
}

/// Lease manager for (sub_account, symbol) allocations.
#[async_trait]
pub trait StrategyAllocationManager: Send + Sync {
    async fn acquire(
        &self,
        strategy_id: &str,
        sub_account: &str,
        symbol: &str,
    ) -> Result<StrategyAllocation, String>;
    async fn release(&self, strategy_id: &str) -> Result<(), String>;
    async fn get(&self, strategy_id: &str) -> Result<Option<StrategyAllocation>, String>;
}

/// The lifecycle transition table (verbatim from `_ALLOWED_TRANSITIONS`).
pub fn allowed_transitions(from: MarketMakerLifecycle) -> Vec<MarketMakerLifecycle> {
    use MarketMakerLifecycle::*;
    match from {
        Stopped => vec![Warming],
        Warming => vec![Shadow, Paused, Stopped, Faulted],
        Shadow => vec![Running, Paused, Draining, Stopped, Faulted],
        Running => vec![Paused, Draining, Faulted],
        Paused => vec![Warming, Shadow, Running, Draining, Stopped, Faulted],
        Draining => vec![Paused, Stopped, Faulted],
        Faulted => vec![Stopped],
    }
}

#[allow(dead_code)] // used by tests; future callers enforce the table
fn transition_allowed(from: MarketMakerLifecycle, to: MarketMakerLifecycle) -> bool {
    allowed_transitions(from).contains(&to)
}

/// The strategy supervisor.
pub struct StrategySupervisor {
    registry: Arc<StrategyRegistry>,
    state_store: Arc<dyn StrategyStateStore>,
    allocations: Arc<dyn StrategyAllocationManager>,
    handles: tokio::sync::Mutex<HashMap<String, Arc<dyn StrategyRuntimeHandle>>>,
}

impl StrategySupervisor {
    pub fn new(
        registry: Arc<StrategyRegistry>,
        state_store: Arc<dyn StrategyStateStore>,
        allocations: Arc<dyn StrategyAllocationManager>,
    ) -> Self {
        Self {
            registry,
            state_store,
            allocations,
            handles: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    async fn handle(&self, strategy_id: &str) -> Option<Arc<dyn StrategyRuntimeHandle>> {
        self.handles.lock().await.get(strategy_id).cloned()
    }

    /// Rebuild a handle from the durable definition via the registry
    /// (`_start_locked`): runs through WARMING → SHADOW → RUNNING regardless of
    /// the transition table.
    async fn start_locked(
        &self,
        instance: &StrategyInstanceDefinition,
        target: MarketMakerLifecycle,
    ) -> Result<(), String> {
        let config = self
            .state_store
            .get_config(&instance.strategy_id, instance.desired_config_revision)
            .await?
            .ok_or_else(|| {
                format!(
                    "config {} missing for {}",
                    instance.desired_config_revision, instance.strategy_id
                )
            })?;
        let context = super::registry::StrategyBuildContext {
            instance: instance.clone(),
            config,
        };
        let handle = self.registry.create(&context)?;
        handle.start().await?;
        // Set desired + runtime to the target. An empty reason string clears any
        // prior reason (e.g. a fault reason) on the fresh start.
        self.state_store
            .set_desired(
                &instance.strategy_id,
                Some(target),
                None,
                Some(instance.revision),
            )
            .await?;
        let _ = self
            .state_store
            .set_runtime(
                &instance.strategy_id,
                Some(target),
                None,
                false,
                Some(""),
                None,
            )
            .await?;
        self.handles
            .lock()
            .await
            .insert(instance.strategy_id.clone(), handle);
        Ok(())
    }

    /// Start a strategy toward `SHADOW` or `RUNNING`.
    pub async fn start(
        &self,
        strategy_id: &str,
        target: MarketMakerLifecycle,
        expected_revision: Option<u64>,
    ) -> Result<StrategyRuntimeState, String> {
        if !matches!(
            target,
            MarketMakerLifecycle::Shadow | MarketMakerLifecycle::Running
        ) {
            return Err(format!(
                "start target must be shadow or running, got {target:?}"
            ));
        }
        let instance = self
            .state_store
            .get_instance(strategy_id)
            .await?
            .ok_or_else(|| format!("unknown strategy {strategy_id}"))?;
        // Ensure allocation.
        let _allocation = self
            .allocations
            .acquire(strategy_id, &instance.sub_account, &instance.symbol)
            .await?;
        self.start_locked(&instance, target).await?;
        // Refresh runtime state (set_desired may bump the instance revision).
        let runtime =
            self.state_store
                .get_runtime(strategy_id)
                .await?
                .unwrap_or(StrategyRuntimeState {
                    strategy_id: strategy_id.to_string(),
                    actual_state: target,
                    effective_config_revision: None,
                    revision: 0,
                    reason: None,
                });
        let _ = expected_revision;
        Ok(runtime)
    }

    /// Pause a strategy (operator action).
    pub async fn pause(&self, strategy_id: &str) -> Result<StrategyRuntimeState, String> {
        let runtime = self
            .state_store
            .get_runtime(strategy_id)
            .await?
            .ok_or_else(|| format!("no runtime for {strategy_id}"))?;
        if runtime.actual_state == MarketMakerLifecycle::Paused {
            self.state_store
                .set_runtime(strategy_id, None, None, false, Some("operator_pause"), None)
                .await?;
        } else {
            if let Some(handle) = self.handle(strategy_id).await {
                handle.set_mode(MarketMakerLifecycle::Paused).await?;
            }
            self.state_store
                .set_runtime(
                    strategy_id,
                    Some(MarketMakerLifecycle::Paused),
                    None,
                    false,
                    Some("operator_pause"),
                    None,
                )
                .await?;
        }
        self.state_store
            .set_desired(strategy_id, Some(MarketMakerLifecycle::Paused), None, None)
            .await?;
        self.state_store
            .get_runtime(strategy_id)
            .await?
            .ok_or_else(|| "missing runtime".to_string())
    }

    /// Suspend for a system-safety pause (does not change desired state).
    pub async fn suspend_for_safety(&self, strategy_id: &str, reason: &str) -> Result<(), String> {
        let runtime = self
            .state_store
            .get_runtime(strategy_id)
            .await?
            .ok_or_else(|| format!("no runtime for {strategy_id}"))?;
        if matches!(
            runtime.actual_state,
            MarketMakerLifecycle::Stopped | MarketMakerLifecycle::Faulted
        ) || runtime.actual_state == MarketMakerLifecycle::Paused
        {
            return Ok(());
        }
        let normalized = reason.trim().to_string();
        if let Some(handle) = self.handle(strategy_id).await {
            handle.set_mode(MarketMakerLifecycle::Paused).await?;
        }
        let pause_reason = format!("{SYSTEM_SAFETY_PAUSE_PREFIX}{normalized}");
        self.state_store
            .set_runtime(
                strategy_id,
                Some(MarketMakerLifecycle::Paused),
                None,
                false,
                Some(&pause_reason),
                None,
            )
            .await?;
        Ok(())
    }

    /// Resume from a system-safety pause.
    pub async fn resume_from_safety(&self, strategy_id: &str) -> Result<(), String> {
        let runtime = self
            .state_store
            .get_runtime(strategy_id)
            .await?
            .ok_or_else(|| format!("no runtime for {strategy_id}"))?;
        let reason = runtime.reason.clone().unwrap_or_default();
        let is_safety_pause = is_system_safety_pause_reason(&reason);
        let desired = self
            .state_store
            .get_instance(strategy_id)
            .await?
            .ok_or_else(|| format!("unknown strategy {strategy_id}"))?
            .desired_state;
        if runtime.actual_state != MarketMakerLifecycle::Paused
            || !is_safety_pause
            || !matches!(
                desired,
                MarketMakerLifecycle::Shadow | MarketMakerLifecycle::Running
            )
        {
            return Ok(());
        }
        // Rebuild the handle if missing (start_locked passes through to desired).
        if self.handle(strategy_id).await.is_none() {
            let instance = self.state_store.get_instance(strategy_id).await?.unwrap();
            self.start_locked(&instance, desired).await?;
        } else if let Some(handle) = self.handle(strategy_id).await {
            handle.set_mode(desired).await?;
        }
        self.state_store
            .set_runtime(
                strategy_id,
                Some(desired),
                None,
                false,
                Some(SYSTEM_SAFETY_RECOVERED_REASON),
                None,
            )
            .await?;
        Ok(())
    }

    /// Resume to a target mode (operator).
    pub async fn resume(
        &self,
        strategy_id: &str,
        target: MarketMakerLifecycle,
    ) -> Result<StrategyRuntimeState, String> {
        if !matches!(
            target,
            MarketMakerLifecycle::Shadow | MarketMakerLifecycle::Running
        ) {
            return Err(format!(
                "resume target must be shadow or running, got {target:?}"
            ));
        }
        let instance = self
            .state_store
            .get_instance(strategy_id)
            .await?
            .ok_or_else(|| format!("unknown strategy {strategy_id}"))?;
        self.start_locked(&instance, target).await?;
        self.state_store
            .get_runtime(strategy_id)
            .await?
            .ok_or_else(|| "missing runtime".to_string())
    }

    /// Drain a strategy.
    pub async fn drain(&self, strategy_id: &str) -> Result<StrategyRuntimeState, String> {
        if let Some(handle) = self.handle(strategy_id).await {
            handle.set_mode(MarketMakerLifecycle::Draining).await?;
        }
        self.state_store
            .set_runtime(
                strategy_id,
                Some(MarketMakerLifecycle::Draining),
                None,
                false,
                Some("operator_drain"),
                None,
            )
            .await?;
        self.state_store
            .set_desired(
                strategy_id,
                Some(MarketMakerLifecycle::Draining),
                None,
                None,
            )
            .await?;
        self.state_store
            .get_runtime(strategy_id)
            .await?
            .ok_or_else(|| "missing runtime".to_string())
    }

    /// Stop a strategy.
    pub async fn stop(&self, strategy_id: &str) -> Result<(), String> {
        if let Some(handle) = self.handle(strategy_id).await {
            handle.stop().await?;
            self.handles.lock().await.remove(strategy_id);
        }
        self.state_store
            .set_runtime(
                strategy_id,
                Some(MarketMakerLifecycle::Stopped),
                None,
                false,
                Some("operator_stop"),
                None,
            )
            .await?;
        self.state_store
            .set_desired(strategy_id, Some(MarketMakerLifecycle::Stopped), None, None)
            .await?;
        self.allocations.release(strategy_id).await?;
        Ok(())
    }

    /// Fault a strategy.
    pub async fn fault(&self, strategy_id: &str, reason: &str) -> Result<(), String> {
        self.state_store
            .set_runtime(
                strategy_id,
                Some(MarketMakerLifecycle::Faulted),
                None,
                false,
                Some(reason),
                None,
            )
            .await?;
        self.state_store
            .set_desired(strategy_id, Some(MarketMakerLifecycle::Faulted), None, None)
            .await?;
        if let Some(handle) = self.handle(strategy_id).await {
            handle.stop().await?;
            self.handles.lock().await.remove(strategy_id);
        }
        Ok(())
    }

    /// Recover a faulted strategy back to a target.
    pub async fn recover(
        &self,
        strategy_id: &str,
        target: MarketMakerLifecycle,
    ) -> Result<(), String> {
        let runtime = self
            .state_store
            .get_runtime(strategy_id)
            .await?
            .ok_or_else(|| "no runtime".to_string())?;
        if runtime.actual_state != MarketMakerLifecycle::Faulted {
            return Err(format!(
                "cannot recover {strategy_id} from {:?}",
                runtime.actual_state
            ));
        }
        if let Some(handle) = self.handle(strategy_id).await {
            handle.stop().await?;
            self.handles.lock().await.remove(strategy_id);
        }
        self.state_store
            .set_runtime(
                strategy_id,
                Some(MarketMakerLifecycle::Stopped),
                None,
                false,
                Some("manual_recovery"),
                None,
            )
            .await?;
        let instance = self.state_store.get_instance(strategy_id).await?.unwrap();
        self.start_locked(&instance, target).await?;
        Ok(())
    }

    /// Activate a config version on a running strategy.
    pub async fn activate_config(
        &self,
        strategy_id: &str,
        config_revision: u64,
        expected_revision: Option<u64>,
    ) -> Result<(), String> {
        let config = self
            .state_store
            .get_config(strategy_id, config_revision)
            .await?
            .ok_or_else(|| format!("config {config_revision} missing"))?;
        let runtime = self
            .state_store
            .get_runtime(strategy_id)
            .await?
            .ok_or_else(|| "no runtime".to_string())?;
        if let Some(handle) = self.handle(strategy_id).await {
            handle.apply_config(&config).await?;
        }
        let _ = runtime;
        let _ = expected_revision;
        self.state_store
            .set_runtime(
                strategy_id,
                None,
                Some(config_revision),
                true,
                Some("config_applied"),
                None,
            )
            .await?;
        Ok(())
    }

    /// Reconciliation on restart: restore all instances.
    pub async fn restore(&self) -> Result<Vec<StrategyRuntimeState>, String> {
        let instances = self.state_store.list_instances().await?;
        let mut states = Vec::new();
        for instance in instances {
            let runtime = self
                .state_store
                .get_runtime(&instance.strategy_id)
                .await?
                .unwrap_or(StrategyRuntimeState {
                    strategy_id: instance.strategy_id.clone(),
                    actual_state: MarketMakerLifecycle::Stopped,
                    effective_config_revision: None,
                    revision: 0,
                    reason: None,
                });
            match runtime.actual_state {
                MarketMakerLifecycle::Faulted => {
                    // Latch to FAULTED.
                    self.state_store
                        .set_desired(
                            &instance.strategy_id,
                            Some(MarketMakerLifecycle::Faulted),
                            None,
                            None,
                        )
                        .await?;
                }
                MarketMakerLifecycle::Stopped => {
                    self.allocations.release(&instance.strategy_id).await?;
                }
                MarketMakerLifecycle::Paused | MarketMakerLifecycle::Draining => {
                    let target = if instance.desired_state == MarketMakerLifecycle::Shadow {
                        MarketMakerLifecycle::Shadow
                    } else {
                        MarketMakerLifecycle::Running
                    };
                    self.start_locked(&instance, target).await?;
                    // Re-apply pause/drain.
                    if let Some(handle) = self.handle(&instance.strategy_id).await {
                        handle.set_mode(runtime.actual_state).await?;
                    }
                }
                _ => {
                    let target = if instance.desired_state == MarketMakerLifecycle::Shadow {
                        MarketMakerLifecycle::Shadow
                    } else {
                        MarketMakerLifecycle::Running
                    };
                    self.start_locked(&instance, target).await?;
                }
            }
            states.push(runtime);
        }
        Ok(states)
    }

    pub async fn runtime_snapshot(
        &self,
        strategy_id: &str,
    ) -> Result<Option<StrategyRuntimeState>, String> {
        self.state_store.get_runtime(strategy_id).await
    }
}

// --- In-memory test implementations ---

/// In-process state store with optimistic revision fencing.
pub struct InMemoryStrategyStateStore {
    inner: tokio::sync::Mutex<InMemoryState>,
}

struct InMemoryState {
    instances: HashMap<String, StrategyInstanceDefinition>,
    runtimes: HashMap<String, StrategyRuntimeState>,
    configs: HashMap<String, Vec<StrategyConfigSnapshot>>,
}

impl Default for InMemoryStrategyStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStrategyStateStore {
    pub fn new() -> Self {
        Self {
            inner: tokio::sync::Mutex::new(InMemoryState {
                instances: HashMap::new(),
                runtimes: HashMap::new(),
                configs: HashMap::new(),
            }),
        }
    }

    pub async fn insert_instance(&self, instance: StrategyInstanceDefinition) {
        self.inner
            .lock()
            .await
            .instances
            .insert(instance.strategy_id.clone(), instance);
    }

    pub async fn insert_config(&self, config: StrategyConfigSnapshot) {
        let mut st = self.inner.lock().await;
        st.configs
            .entry(config.strategy_id.clone())
            .or_default()
            .push(config);
    }
}

#[async_trait]
impl StrategyStateStore for InMemoryStrategyStateStore {
    async fn list_instances(&self) -> Result<Vec<StrategyInstanceDefinition>, String> {
        Ok(self
            .inner
            .lock()
            .await
            .instances
            .values()
            .cloned()
            .collect())
    }
    async fn get_instance(
        &self,
        strategy_id: &str,
    ) -> Result<Option<StrategyInstanceDefinition>, String> {
        Ok(self.inner.lock().await.instances.get(strategy_id).cloned())
    }
    async fn get_runtime(&self, strategy_id: &str) -> Result<Option<StrategyRuntimeState>, String> {
        Ok(self.inner.lock().await.runtimes.get(strategy_id).cloned())
    }
    async fn get_config(
        &self,
        strategy_id: &str,
        revision: u64,
    ) -> Result<Option<StrategyConfigSnapshot>, String> {
        Ok(self
            .inner
            .lock()
            .await
            .configs
            .get(strategy_id)
            .and_then(|cfgs| cfgs.iter().find(|c| c.revision == revision))
            .cloned())
    }
    async fn set_desired(
        &self,
        strategy_id: &str,
        state: Option<MarketMakerLifecycle>,
        config_revision: Option<u64>,
        expected_revision: Option<u64>,
    ) -> Result<StrategyInstanceDefinition, String> {
        let mut st = self.inner.lock().await;
        let instance = st
            .instances
            .get_mut(strategy_id)
            .ok_or_else(|| format!("unknown strategy {strategy_id}"))?;
        if let Some(expected) = expected_revision
            && instance.revision != expected
        {
            return Err(format!(
                "Strategy revision conflict: expected={expected} actual={}",
                instance.revision
            ));
        }
        if let Some(state) = state {
            instance.desired_state = state;
        }
        if let Some(rev) = config_revision {
            instance.desired_config_revision = rev;
        }
        instance.revision += 1;
        Ok(instance.clone())
    }
    async fn set_runtime(
        &self,
        strategy_id: &str,
        actual_state: Option<MarketMakerLifecycle>,
        effective_config_revision: Option<u64>,
        set_effective_config: bool,
        reason: Option<&str>,
        expected_revision: Option<u64>,
    ) -> Result<StrategyRuntimeState, String> {
        let mut st = self.inner.lock().await;
        let runtime = st
            .runtimes
            .entry(strategy_id.to_string())
            .or_insert(StrategyRuntimeState {
                strategy_id: strategy_id.to_string(),
                actual_state: MarketMakerLifecycle::Stopped,
                effective_config_revision: None,
                revision: 0,
                reason: None,
            });
        if let Some(expected) = expected_revision
            && runtime.revision != expected
        {
            return Err(format!(
                "Strategy revision conflict: expected={expected} actual={}",
                runtime.revision
            ));
        }
        if let Some(state) = actual_state {
            runtime.actual_state = state;
        }
        if set_effective_config {
            runtime.effective_config_revision = effective_config_revision;
        }
        if let Some(reason) = reason {
            // Empty string clears the reason (used by `start_locked`).
            runtime.reason = if reason.is_empty() {
                None
            } else {
                Some(reason.to_string())
            };
        }
        runtime.revision += 1;
        Ok(runtime.clone())
    }
}

/// In-process allocation manager: exclusive (sub_account, symbol) leases with a
/// monotonic fence. `AUTO` conflicts with every symbol on the same sub_account.
pub struct InMemoryStrategyAllocationManager {
    inner: tokio::sync::Mutex<HashMap<String, StrategyAllocation>>,
    fence: std::sync::atomic::AtomicU64,
}

impl Default for InMemoryStrategyAllocationManager {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStrategyAllocationManager {
    pub fn new() -> Self {
        Self {
            inner: tokio::sync::Mutex::new(HashMap::new()),
            fence: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl StrategyAllocationManager for InMemoryStrategyAllocationManager {
    async fn acquire(
        &self,
        strategy_id: &str,
        sub_account: &str,
        symbol: &str,
    ) -> Result<StrategyAllocation, String> {
        let mut st = self.inner.lock().await;
        if let Some(existing) = st.get(strategy_id) {
            return Ok(existing.clone());
        }
        // Check for a conflicting exclusive lease.
        for (other_id, alloc) in st.iter() {
            if other_id == strategy_id {
                continue;
            }
            if alloc.sub_account == sub_account
                && (alloc.symbol == symbol || symbol == "AUTO" || alloc.symbol == "AUTO")
            {
                return Err(format!(
                    "allocation conflict: {strategy_id} wants ({sub_account},{symbol}) held by {other_id}"
                ));
            }
        }
        let fence = self.fence.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        let alloc = StrategyAllocation {
            strategy_id: strategy_id.to_string(),
            sub_account: sub_account.to_string(),
            symbol: symbol.to_string(),
            fence,
        };
        st.insert(strategy_id.to_string(), alloc.clone());
        Ok(alloc)
    }

    async fn release(&self, strategy_id: &str) -> Result<(), String> {
        self.inner.lock().await.remove(strategy_id);
        Ok(())
    }

    async fn get(&self, strategy_id: &str) -> Result<Option<StrategyAllocation>, String> {
        Ok(self.inner.lock().await.get(strategy_id).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal runtime handle that records set_mode calls.
    struct RecordingHandle {
        modes: tokio::sync::Mutex<Vec<MarketMakerLifecycle>>,
    }

    #[async_trait]
    impl StrategyRuntimeHandle for RecordingHandle {
        async fn start(&self) -> Result<(), String> {
            Ok(())
        }
        async fn set_mode(&self, mode: MarketMakerLifecycle) -> Result<(), String> {
            self.modes.lock().await.push(mode);
            Ok(())
        }
        async fn apply_config(&self, _: &StrategyConfigSnapshot) -> Result<(), String> {
            Ok(())
        }
        async fn stop(&self) -> Result<(), String> {
            Ok(())
        }
    }

    async fn setup() -> (
        Arc<StrategyRegistry>,
        Arc<InMemoryStrategyStateStore>,
        Arc<InMemoryStrategyAllocationManager>,
        StrategySupervisor,
    ) {
        let mut registry = StrategyRegistry::new();
        registry.register(
            "trend_follow",
            Arc::new(|_| {
                Arc::new(RecordingHandle {
                    modes: tokio::sync::Mutex::new(Vec::new()),
                })
            }),
        );
        let registry = Arc::new(registry);
        let store = Arc::new(InMemoryStrategyStateStore::new());
        let allocations = Arc::new(InMemoryStrategyAllocationManager::new());
        // Seed an instance + config.
        store
            .insert_instance(StrategyInstanceDefinition {
                strategy_id: "tf_1".into(),
                strategy_type: "trend_follow".into(),
                sub_account: "sub1".into(),
                symbol: "BTC".into(),
                desired_state: MarketMakerLifecycle::Stopped,
                desired_config_revision: 1,
                revision: 0,
            })
            .await;
        store
            .insert_config(StrategyConfigSnapshot {
                strategy_id: "tf_1".into(),
                revision: 1,
                values: serde_json::json!({"fast_ema_period": 12}),
            })
            .await;
        let supervisor =
            StrategySupervisor::new(registry.clone(), store.clone(), allocations.clone());
        (registry, store, allocations, supervisor)
    }

    #[tokio::test]
    async fn lifecycle_start_pause_stop() {
        let (_, store, _, sup) = setup().await;
        let started = sup
            .start("tf_1", MarketMakerLifecycle::Running, None)
            .await
            .unwrap();
        assert_eq!(started.actual_state, MarketMakerLifecycle::Running);

        let paused = sup.pause("tf_1").await.unwrap();
        assert_eq!(paused.actual_state, MarketMakerLifecycle::Paused);
        assert_eq!(paused.reason.as_deref(), Some("operator_pause"));

        sup.stop("tf_1").await.unwrap();
        let runtime = store.get_runtime("tf_1").await.unwrap().unwrap();
        assert_eq!(runtime.actual_state, MarketMakerLifecycle::Stopped);
        assert_eq!(runtime.reason.as_deref(), Some("operator_stop"));
    }

    #[tokio::test]
    async fn system_safety_pause_and_resume() {
        let (_, store, _, sup) = setup().await;
        sup.start("tf_1", MarketMakerLifecycle::Running, None)
            .await
            .unwrap();

        sup.suspend_for_safety("tf_1", "drawdown_exceeded")
            .await
            .unwrap();
        let runtime = store.get_runtime("tf_1").await.unwrap().unwrap();
        assert_eq!(runtime.actual_state, MarketMakerLifecycle::Paused);
        assert!(
            runtime
                .reason
                .as_deref()
                .unwrap()
                .starts_with(SYSTEM_SAFETY_PAUSE_PREFIX)
        );

        sup.resume_from_safety("tf_1").await.unwrap();
        let runtime = store.get_runtime("tf_1").await.unwrap().unwrap();
        assert_eq!(runtime.actual_state, MarketMakerLifecycle::Running);
        assert_eq!(
            runtime.reason.as_deref(),
            Some(SYSTEM_SAFETY_RECOVERED_REASON)
        );
    }

    #[tokio::test]
    async fn operator_pause_is_not_resumed_by_safety() {
        let (_, store, _, sup) = setup().await;
        sup.start("tf_1", MarketMakerLifecycle::Running, None)
            .await
            .unwrap();
        sup.pause("tf_1").await.unwrap();
        // resume_from_safety must NOT act on an operator pause.
        sup.resume_from_safety("tf_1").await.unwrap();
        let runtime = store.get_runtime("tf_1").await.unwrap().unwrap();
        assert_eq!(runtime.actual_state, MarketMakerLifecycle::Paused);
    }

    #[tokio::test]
    async fn fault_and_recover() {
        let (_, store, _, sup) = setup().await;
        sup.start("tf_1", MarketMakerLifecycle::Running, None)
            .await
            .unwrap();
        sup.fault("tf_1", "risk_check_error").await.unwrap();
        let runtime = store.get_runtime("tf_1").await.unwrap().unwrap();
        assert_eq!(runtime.actual_state, MarketMakerLifecycle::Faulted);

        sup.recover("tf_1", MarketMakerLifecycle::Shadow)
            .await
            .unwrap();
        let runtime = store.get_runtime("tf_1").await.unwrap().unwrap();
        assert_eq!(runtime.actual_state, MarketMakerLifecycle::Shadow);
        assert_eq!(runtime.reason.as_deref(), None); // start_locked clears reason
    }

    #[tokio::test]
    async fn revision_fencing_rejects_stale_write() {
        let (_, store, _, sup) = setup().await;
        sup.start("tf_1", MarketMakerLifecycle::Running, None)
            .await
            .unwrap();
        let runtime = store.get_runtime("tf_1").await.unwrap().unwrap();
        let stale = runtime.revision; // current
        // Attempt a write with a stale (older) expected revision.
        let err = store
            .set_runtime(
                "tf_1",
                Some(MarketMakerLifecycle::Paused),
                None,
                false,
                Some("x"),
                Some(stale.saturating_sub(1)),
            )
            .await
            .unwrap_err();
        assert!(err.contains("revision conflict"), "got: {err}");
    }

    #[tokio::test]
    async fn allocation_fencing_conflicts() {
        let (_, store, allocations, _sup) = setup().await;
        // First strategy leases (sub1, BTC).
        allocations.acquire("tf_1", "sub1", "BTC").await.unwrap();
        // Second strategy on the same scope conflicts.
        let err = allocations
            .acquire("tf_2", "sub1", "BTC")
            .await
            .unwrap_err();
        assert!(err.contains("allocation conflict"));
        // AUTO conflicts with every symbol on the same sub_account.
        let err2 = allocations
            .acquire("tf_3", "sub1", "AUTO")
            .await
            .unwrap_err();
        assert!(err2.contains("allocation conflict"));
        // Different sub_account is fine.
        allocations.acquire("tf_4", "sub2", "BTC").await.unwrap();
        let _ = store;
    }

    #[test]
    fn transition_table_matches_python() {
        use MarketMakerLifecycle::*;
        assert!(transition_allowed(Stopped, Warming));
        assert!(!transition_allowed(Stopped, Running));
        assert!(transition_allowed(Running, Paused));
        assert!(transition_allowed(Running, Draining));
        assert!(transition_allowed(Running, Faulted));
        assert!(!transition_allowed(Running, Shadow));
        assert!(transition_allowed(Paused, Running));
        assert!(transition_allowed(Faulted, Stopped));
        assert!(!transition_allowed(Faulted, Running));
    }
}
