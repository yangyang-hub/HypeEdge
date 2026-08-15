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
    async fn upsert_instance(&self, instance: &StrategyInstanceDefinition) -> Result<(), String>;
    async fn upsert_config(&self, config: &StrategyConfigSnapshot) -> Result<(), String>;
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

#[cfg(test)]
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
    /// the transition table. `expected_revision` is the caller's optimistic
    /// concurrency token forwarded to the `set_desired` fence (M-ST8); when
    /// absent, the just-fetched instance revision is used.
    async fn start_locked(
        &self,
        instance: &StrategyInstanceDefinition,
        target: MarketMakerLifecycle,
        expected_revision: Option<u64>,
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
        handle.set_mode(target).await?;
        // Set desired + runtime to the target. An empty reason string clears any
        // prior reason (e.g. a fault reason) on the fresh start.
        self.state_store
            .set_desired(
                &instance.strategy_id,
                Some(target),
                None,
                expected_revision.or(Some(instance.revision)),
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

    /// Capability gate (H-ST2): a target state must be within the type's
    /// declared capabilities. Shadow is rejected for types that declare
    /// `supports_shadow == false`; types without a plugin declaration are
    /// treated as legacy/unmanaged and allowed through.
    fn check_target_capability(
        &self,
        instance: &StrategyInstanceDefinition,
        target: MarketMakerLifecycle,
    ) -> Result<(), String> {
        if target == MarketMakerLifecycle::Shadow
            && let Some(caps) = self.registry.capabilities(&instance.strategy_type)
            && !caps.supports_shadow
        {
            return Err(format!(
                "strategy type {} does not support shadow mode (capability gate)",
                instance.strategy_type
            ));
        }
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
        self.check_target_capability(&instance, target)?;
        // Ensure allocation.
        let _allocation = self
            .allocations
            .acquire(strategy_id, &instance.sub_account, &instance.symbol)
            .await?;
        if let Err(e) = self.start_locked(&instance, target, expected_revision).await {
            // M-ST4: a failed start must not leak the (sub_account, symbol)
            // lease.
            let _ = self.allocations.release(strategy_id).await;
            return Err(e);
        }
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
            self.start_locked(&instance, desired, None).await?;
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
        self.check_target_capability(&instance, target)?;
        // M-ST4: resume is a start-like path and must hold the lease.
        let _allocation = self
            .allocations
            .acquire(strategy_id, &instance.sub_account, &instance.symbol)
            .await?;
        if let Err(e) = self.start_locked(&instance, target, None).await {
            let _ = self.allocations.release(strategy_id).await;
            return Err(e);
        }
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
        self.start_locked(&instance, target, None).await?;
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
        self.state_store
            .set_runtime(
                strategy_id,
                None,
                Some(config_revision),
                true,
                Some("config_applied"),
                expected_revision,
            )
            .await?;
        // H-ST3: keep desired/effective consistent. restore() (re)builds the
        // runtime from `desired_config_revision`, so an activation that only
        // bumps the effective revision would silently roll back to the old
        // config on the next restart.
        self.state_store
            .set_desired(strategy_id, None, Some(config_revision), expected_revision)
            .await?;
        Ok(())
    }

    /// Reconciliation on restart: restore all instances. One broken instance
    /// (M-ST7) is latched to FAULTED and logged; the remaining instances are
    /// still restored.
    pub async fn restore(&self) -> Result<Vec<StrategyRuntimeState>, String> {
        let instances = self.state_store.list_instances().await?;
        let mut states = Vec::new();
        for instance in instances {
            match self.restore_one(&instance).await {
                Ok(runtime) => states.push(runtime),
                Err(e) => {
                    tracing::error!(
                        strategy_id = %instance.strategy_id,
                        error = %e,
                        "strategy_restore_instance_failed"
                    );
                    // Do not let one bad instance abort the restore of the rest.
                    let _ = self
                        .state_store
                        .set_desired(
                            &instance.strategy_id,
                            Some(MarketMakerLifecycle::Faulted),
                            None,
                            None,
                        )
                        .await;
                    let _ = self
                        .state_store
                        .set_runtime(
                            &instance.strategy_id,
                            Some(MarketMakerLifecycle::Faulted),
                            None,
                            false,
                            Some(&format!("restore_failed:{e}")),
                            None,
                        )
                        .await;
                    let _ = self.allocations.release(&instance.strategy_id).await;
                    // Keep the one-state-per-instance contract for callers.
                    states.push(StrategyRuntimeState {
                        strategy_id: instance.strategy_id.clone(),
                        actual_state: MarketMakerLifecycle::Faulted,
                        effective_config_revision: None,
                        revision: 0,
                        reason: Some(format!("restore_failed:{e}")),
                    });
                }
            }
        }
        Ok(states)
    }

    /// Restore a single instance to its desired lifecycle, holding the
    /// (sub_account, symbol) lease while it runs (M-ST4).
    async fn restore_one(
        &self,
        instance: &StrategyInstanceDefinition,
    ) -> Result<StrategyRuntimeState, String> {
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
                self.allocations.release(&instance.strategy_id).await?;
            }
            MarketMakerLifecycle::Stopped => {
                self.allocations.release(&instance.strategy_id).await?;
            }
            MarketMakerLifecycle::Paused | MarketMakerLifecycle::Draining => {
                let target = self.resolve_restore_target(instance);
                let _allocation = self
                    .allocations
                    .acquire(
                        &instance.strategy_id,
                        &instance.sub_account,
                        &instance.symbol,
                    )
                    .await?;
                self.start_locked(instance, target, None).await?;
                // Re-apply pause/drain.
                if let Some(handle) = self.handle(&instance.strategy_id).await {
                    handle.set_mode(runtime.actual_state).await?;
                }
            }
            _ => {
                let target = self.resolve_restore_target(instance);
                let _allocation = self
                    .allocations
                    .acquire(
                        &instance.strategy_id,
                        &instance.sub_account,
                        &instance.symbol,
                    )
                    .await?;
                self.start_locked(instance, target, None).await?;
            }
        }
        Ok(runtime)
    }

    /// Map a persisted `desired_state` of SHADOW to the restored target. Types
    /// that do not support shadow (H-ST2) come back up as running; everything
    /// else restores to the desired state.
    fn resolve_restore_target(&self, instance: &StrategyInstanceDefinition) -> MarketMakerLifecycle {
        if instance.desired_state == MarketMakerLifecycle::Shadow {
            let supports = self
                .registry
                .capabilities(&instance.strategy_type)
                .map(|caps| caps.supports_shadow)
                .unwrap_or(false);
            if supports {
                return MarketMakerLifecycle::Shadow;
            }
        }
        MarketMakerLifecycle::Running
    }

    pub async fn runtime_snapshot(
        &self,
        strategy_id: &str,
    ) -> Result<Option<StrategyRuntimeState>, String> {
        // M-ST2 lightweight supervision: a lifecycle query is a cheap chance to
        // notice a crashed runner task. If the handle's worker died without a
        // supervised stop, fault the instance (and log loudly) so the operator
        // sees it instead of a silently dead strategy.
        if let Some(handle) = self.handle(strategy_id).await
            && !handle.is_healthy()
        {
            tracing::error!(
                strategy_id,
                "strategy_runner_unhealthy_faulting"
            );
            let reason = "runner_crashed";
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
            handle.stop().await?;
            self.handles.lock().await.remove(strategy_id);
        }
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
    async fn upsert_instance(&self, instance: &StrategyInstanceDefinition) -> Result<(), String> {
        self.insert_instance(instance.clone()).await;
        Ok(())
    }
    async fn upsert_config(&self, config: &StrategyConfigSnapshot) -> Result<(), String> {
        self.insert_config(config.clone()).await;
        Ok(())
    }
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

    /// A registry with real plugin capabilities (trend_follow: no shadow;
    /// market_maker: shadow) whose factory records the config revision each
    /// built handle was created from (for restart-load assertions).
    async fn setup_with_plugins() -> (
        Arc<StrategyRegistry>,
        Arc<InMemoryStrategyStateStore>,
        Arc<InMemoryStrategyAllocationManager>,
        StrategySupervisor,
        Arc<std::sync::Mutex<Vec<u64>>>,
    ) {
        use crate::strategy::registry::{
            StrategyBuildContext, StrategyTypePlugin, market_maker_capabilities,
            trend_follow_capabilities,
        };
        let mut registry = StrategyRegistry::new();
        let built = Arc::new(std::sync::Mutex::new(Vec::new()));
        let tf_built = built.clone();
        registry.register_plugin(StrategyTypePlugin {
            strategy_type: "trend_follow".to_string(),
            capabilities: trend_follow_capabilities(),
            factory: Arc::new(move |ctx: &StrategyBuildContext| {
                tf_built.lock().unwrap().push(ctx.config.revision);
                Arc::new(RecordingHandle {
                    modes: tokio::sync::Mutex::new(Vec::new()),
                })
            }),
        });
        registry.register_plugin(StrategyTypePlugin {
            strategy_type: "market_maker".to_string(),
            capabilities: market_maker_capabilities(),
            factory: Arc::new(|_| {
                Arc::new(RecordingHandle {
                    modes: tokio::sync::Mutex::new(Vec::new()),
                })
            }),
        });
        let registry = Arc::new(registry);
        let store = Arc::new(InMemoryStrategyStateStore::new());
        let allocations = Arc::new(InMemoryStrategyAllocationManager::new());
        let supervisor =
            StrategySupervisor::new(registry.clone(), store.clone(), allocations.clone());
        (registry, store, allocations, supervisor, built)
    }

    async fn seed_plugin_instance(
        store: &Arc<InMemoryStrategyStateStore>,
        strategy_id: &str,
        strategy_type: &str,
        symbol: &str,
        config_revision: u64,
    ) {
        store
            .insert_instance(StrategyInstanceDefinition {
                strategy_id: strategy_id.into(),
                strategy_type: strategy_type.into(),
                sub_account: "sub1".into(),
                symbol: symbol.into(),
                desired_state: MarketMakerLifecycle::Stopped,
                desired_config_revision: config_revision,
                revision: 0,
            })
            .await;
        store
            .insert_config(StrategyConfigSnapshot {
                strategy_id: strategy_id.into(),
                revision: config_revision,
                values: serde_json::json!({"fast_ema_period": 12}),
            })
            .await;
    }

    #[tokio::test]
    async fn start_shadow_rejected_for_trend_follow() {
        // H-ST2: the capability gate must reject Shadow for trend_follow
        // (accepting it would silently run the strategy for real).
        let (_, store, allocations, sup, _) = setup_with_plugins().await;
        seed_plugin_instance(&store, "tf_1", "trend_follow", "BTC", 1).await;

        let err = sup
            .start("tf_1", MarketMakerLifecycle::Shadow, None)
            .await
            .unwrap_err();
        assert!(err.contains("shadow"), "got: {err}");
        // The gate must reject before acquiring the lease.
        assert!(
            allocations.get("tf_1").await.unwrap().is_none(),
            "a rejected start must not hold an allocation"
        );

        // Running is still allowed.
        sup.start("tf_1", MarketMakerLifecycle::Running, None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn start_shadow_allowed_for_market_maker() {
        // H-ST2: market_maker declares supports_shadow and is unaffected.
        let (_, store, _, sup, _) = setup_with_plugins().await;
        seed_plugin_instance(&store, "mm_1", "market_maker", "ETH", 1).await;

        let started = sup
            .start("mm_1", MarketMakerLifecycle::Shadow, None)
            .await
            .unwrap();
        assert_eq!(started.actual_state, MarketMakerLifecycle::Shadow);
    }

    #[tokio::test]
    async fn activate_config_updates_desired_and_survives_restart() {
        // H-ST3: activation must keep desired_config_revision == effective so a
        // restart reloads the new config instead of rolling back.
        let (registry, store, allocations, sup, built) = setup_with_plugins().await;
        seed_plugin_instance(&store, "tf_1", "trend_follow", "BTC", 1).await;
        sup.start("tf_1", MarketMakerLifecycle::Running, None)
            .await
            .unwrap();
        store
            .insert_config(StrategyConfigSnapshot {
                strategy_id: "tf_1".into(),
                revision: 2,
                values: serde_json::json!({"fast_ema_period": 21}),
            })
            .await;

        sup.activate_config("tf_1", 2, None).await.unwrap();

        let instance = store.get_instance("tf_1").await.unwrap().unwrap();
        assert_eq!(
            instance.desired_config_revision, 2,
            "desired must track the activated config"
        );
        let runtime = store.get_runtime("tf_1").await.unwrap().unwrap();
        assert_eq!(runtime.effective_config_revision, Some(2));

        // Simulate a restart: a fresh supervisor over the same durable store
        // must rebuild the handle from config revision 2.
        let restarted = StrategySupervisor::new(registry, store.clone(), allocations.clone());
        restarted.restore().await.unwrap();
        assert_eq!(
            *built.lock().unwrap(),
            vec![1, 2],
            "restore must rebuild with the activated config revision"
        );
    }

    #[tokio::test]
    async fn restore_acquires_allocation_and_failure_does_not_leak() {
        // M-ST4: restore acquires the lease for a running instance; a start
        // that fails after acquiring must release it again.
        let (_, store, allocations, sup, _) = setup_with_plugins().await;
        seed_plugin_instance(&store, "tf_1", "trend_follow", "BTC", 1).await;
        store
            .set_runtime(
                "tf_1",
                Some(MarketMakerLifecycle::Running),
                None,
                false,
                Some("pre_restart"),
                None,
            )
            .await
            .unwrap();

        sup.restore().await.unwrap();
        assert!(
            allocations.get("tf_1").await.unwrap().is_some(),
            "restore must hold the (sub_account, symbol) lease"
        );

        // A second instance whose config is missing → start fails → lease must
        // not leak.
        store
            .insert_instance(StrategyInstanceDefinition {
                strategy_id: "tf_bad".into(),
                strategy_type: "trend_follow".into(),
                sub_account: "sub1".into(),
                symbol: "SOL".into(),
                desired_state: MarketMakerLifecycle::Stopped,
                desired_config_revision: 99,
                revision: 0,
            })
            .await;
        let err = sup
            .start("tf_bad", MarketMakerLifecycle::Running, None)
            .await
            .unwrap_err();
        assert!(err.contains("config 99 missing"), "got: {err}");
        assert!(
            allocations.get("tf_bad").await.unwrap().is_none(),
            "a failed start must release its lease"
        );
    }

    #[tokio::test]
    async fn restore_continues_after_instance_failure() {
        // M-ST7: one instance with a broken config must be latched to FAULTED
        // while the remaining instances are still restored.
        let (_, store, _, sup, _) = setup_with_plugins().await;
        // Good instance.
        seed_plugin_instance(&store, "tf_good", "trend_follow", "BTC", 1).await;
        store
            .set_runtime(
                "tf_good",
                Some(MarketMakerLifecycle::Running),
                None,
                false,
                Some("pre_restart"),
                None,
            )
            .await
            .unwrap();
        // Broken instance: runtime says running but its config revision is gone.
        store
            .insert_instance(StrategyInstanceDefinition {
                strategy_id: "tf_bad".into(),
                strategy_type: "trend_follow".into(),
                sub_account: "sub1".into(),
                symbol: "SOL".into(),
                desired_state: MarketMakerLifecycle::Stopped,
                desired_config_revision: 99,
                revision: 0,
            })
            .await;
        store
            .set_runtime(
                "tf_bad",
                Some(MarketMakerLifecycle::Running),
                None,
                false,
                Some("pre_restart"),
                None,
            )
            .await
            .unwrap();

        let states = sup.restore().await.expect("restore must not abort");

        let good = store.get_runtime("tf_good").await.unwrap().unwrap();
        assert_eq!(good.actual_state, MarketMakerLifecycle::Running);
        let bad = store.get_runtime("tf_bad").await.unwrap().unwrap();
        assert_eq!(bad.actual_state, MarketMakerLifecycle::Faulted);
        assert!(
            bad.reason.as_deref().unwrap_or("").contains("restore_failed"),
            "fault reason must explain the restore failure, got {:?}",
            bad.reason
        );
        assert_eq!(states.len(), 2);
    }

    #[tokio::test]
    async fn start_honors_expected_revision() {
        // M-ST8: start() must forward the caller's expected_revision into the
        // set_desired fence (stale callers get a revision conflict).
        let (_, store, _, sup, _) = setup_with_plugins().await;
        seed_plugin_instance(&store, "tf_1", "trend_follow", "BTC", 1).await;

        // Instance revision is 0 at seed → a start with expected=0 succeeds.
        sup.start("tf_1", MarketMakerLifecycle::Running, Some(0))
            .await
            .unwrap();

        // The start bumped the revision; the same expected token is now stale.
        let err = sup
            .start("tf_1", MarketMakerLifecycle::Running, Some(0))
            .await
            .unwrap_err();
        assert!(err.contains("revision conflict"), "got: {err}");
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
