//! Shared API state: the components handlers read, plus the sliding-window
//! rate limiter. Port of the FastAPI `app.state` wiring + `security.py`.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use hypeedge_config::settings::AppSettings;
use hypeedge_domain::traits::ExecutionClient;
use hypeedge_infra::event_bus::EventBus;
use hypeedge_storage::strategy_state_store::PostgresStrategyStateStore;
use hypeedge_trading::market_data::{BookManager, InstrumentMetaCache};
use hypeedge_trading::market_maker::{MarketMakerRuntime, MarketMakerRuntimeFactory};
use hypeedge_trading::risk::ActionBudgetController;
use hypeedge_trading::risk::KillSwitch;
use hypeedge_trading::strategy::{
    InMemoryStrategyAllocationManager, InMemoryStrategyStateStore, StrategyRegistry,
    StrategyStateStore, StrategySupervisor,
};

use crate::auth::RoleTokens;
use crate::sse_broker::SseBroker;

type MmSnapshotProvider = Arc<
    dyn Fn(
            &str,
        ) -> futures::future::BoxFuture<
            'static,
            Option<hypeedge_trading::market_maker::MarketMakerRuntimeSnapshot>,
        > + Send
        + Sync,
>;

/// The strategy control plane the API drives (from `HypeEdgeApp`).
#[derive(Clone)]
pub struct StrategyControlPlane {
    pub supervisor: Arc<StrategySupervisor>,
    pub registry: Arc<StrategyRegistry>,
    pub state_store: Arc<dyn StrategyStateStore>,
    pub allocations: Arc<InMemoryStrategyAllocationManager>,
}

/// The shared application context the API reads (a subset of `HypeEdgeApp`).
#[derive(Clone)]
pub struct AppState {
    pub settings: Arc<AppSettings>,
    pub role_tokens: RoleTokens,
    pub kill_switch: Arc<KillSwitch>,
    pub event_bus: Arc<EventBus>,
    pub books: Arc<tokio::sync::Mutex<BookManager>>,
    /// The durable SSE broker (outbox-backed) when a Postgres store is wired.
    pub sse_broker: Arc<SseBroker>,
    /// The strategy control plane (supervisor + registry + in-memory stores).
    pub strategies: StrategyControlPlane,
    /// Latest market-maker runtime snapshot provider (wired by the app).
    pub mm_snapshot_provider: Option<MmSnapshotProvider>,
    /// Per-strategy live market-maker runtimes built by the plugin factory.
    pub mm_runtimes: Arc<std::sync::Mutex<HashMap<String, Arc<MarketMakerRuntime>>>>,
    /// Optional trading-enabled flag (from the app lifecycle).
    pub trading_enabled: Arc<tokio::sync::RwLock<bool>>,
    pub safety_mode: Arc<tokio::sync::RwLock<String>>,
    /// The live account tracker (positions/equity/PnL). Wired by the app; the
    /// in-memory default is empty until the clearinghouse poller runs.
    pub account_tracker: Arc<hypeedge_trading::account::AccountTracker>,
    /// The canary release gate evaluator (pure fail-closed decisions for the
    /// deployment stages). Constructed from defaults; wired by the app.
    pub canary_gate: Arc<hypeedge_trading::risk::canary::CanaryGateEvaluator>,
    pub action_budget: Option<Arc<tokio::sync::Mutex<ActionBudgetController>>>,
    /// Durable config-version repository (Postgres-backed). `None` when the
    /// app runs without a database → config-version routes return 503.
    pub config_versions: Option<Arc<dyn hypeedge_storage::ConfigVersionStore>>,
    /// The wired execution engine (6e). `None` in control-plane-only mode.
    pub execution: Option<Arc<hypeedge_trading::execution::ExecutionEngine>>,
    /// The wired live market-data provider (6c). `None` in control-plane-only mode.
    pub market_data: Option<Arc<hypeedge_trading::market_data::LiveMarketDataProvider>>,
    /// The wired instrument metadata cache.
    pub instrument_meta: Option<Arc<InstrumentMetaCache>>,
    pub request_limiter: SlidingWindowLimiter,
    pub mutation_limiter: SlidingWindowLimiter,
    pub auth_failure_limiter: SlidingWindowLimiter,
}

impl AppState {
    pub fn new(
        settings: Arc<AppSettings>,
        kill_switch: Arc<KillSwitch>,
        event_bus: Arc<EventBus>,
        books: Arc<tokio::sync::Mutex<BookManager>>,
    ) -> Self {
        let role_tokens = RoleTokens::from_settings(&settings.api);
        let sse_broker = Arc::new(SseBroker::new(event_bus.clone(), None, None, 1000, 256));
        // C2: without a durable outbox publisher wired, drive the SSE broker
        // from the in-process bus so `/api/v1/events` actually delivers (the
        // broker's mailbox was never fed, so the endpoint blocked on an empty
        // queue). The outbox publisher replaces this in the app wiring.
        if !sse_broker.has_durable_store() {
            let broker = sse_broker.clone();
            tokio::spawn(async move {
                let (stop_tx, stop_rx) = tokio::sync::mpsc::channel(1);
                // Leak the sender: the loop runs for the bus's lifetime and
                // exits when the bus mailbox closes at shutdown.
                std::mem::forget(stop_tx);
                broker.run_legacy(stop_rx).await;
            });
        }
        let mut registry = StrategyRegistry::new();
        // Register a no-op runtime handle factory so the supervisor can drive
        // strategy lifecycle even without a live strategy runtime wired. The
        // app wiring replaces this with real strategy runtimes.
        register_noop_plugins(&mut registry);
        let registry = Arc::new(registry);
        let state_store: Arc<dyn StrategyStateStore> = Arc::new(InMemoryStrategyStateStore::new());
        let allocations = Arc::new(InMemoryStrategyAllocationManager::new());
        let supervisor = Arc::new(StrategySupervisor::new(
            registry.clone(),
            state_store.clone(),
            allocations.clone(),
        ));
        let strategies = StrategyControlPlane {
            supervisor,
            registry,
            state_store,
            allocations,
        };
        let account_tracker = Arc::new(hypeedge_trading::account::AccountTracker::new());
        Self {
            settings,
            role_tokens,
            kill_switch,
            event_bus,
            books,
            sse_broker,
            strategies,
            mm_snapshot_provider: None,
            mm_runtimes: Arc::new(std::sync::Mutex::new(HashMap::new())),
            trading_enabled: Arc::new(tokio::sync::RwLock::new(false)),
            safety_mode: Arc::new(tokio::sync::RwLock::new("starting".into())),
            account_tracker,
            canary_gate: Arc::new(hypeedge_trading::risk::CanaryGateEvaluator::new()),
            action_budget: None,
            config_versions: None,
            execution: None,
            market_data: None,
            instrument_meta: None,
            request_limiter: SlidingWindowLimiter::new(),
            mutation_limiter: SlidingWindowLimiter::new(),
            auth_failure_limiter: SlidingWindowLimiter::new(),
        }
    }

    pub fn environment(&self) -> &str {
        &self.settings.environment
    }

    pub fn is_mainnet(&self) -> bool {
        self.settings.is_mainnet()
    }

    /// Build `AppState` from a wired runtime (6d/6e/6f): execution engine,
    /// market-data provider, account tracker, durable config versions, and the
    /// durable SSE outbox. The app crate calls this with the pieces it built.
    #[allow(clippy::too_many_arguments)]
    pub fn from_wiring(
        mut state: Self,
        execution: Option<Arc<hypeedge_trading::execution::ExecutionEngine>>,
        market_data: Option<Arc<hypeedge_trading::market_data::LiveMarketDataProvider>>,
        instrument_meta: Option<Arc<InstrumentMetaCache>>,
        config_versions: Option<Arc<dyn hypeedge_storage::ConfigVersionStore>>,
        trading_enabled: Arc<tokio::sync::RwLock<bool>>,
        safety_mode: Arc<tokio::sync::RwLock<String>>,
        sse_outbox: Option<Arc<hypeedge_storage::outbox::PostgresOutboxStore>>,
        sse_pool: Option<sqlx::PgPool>,
        funding_arb_deps: Option<
            Arc<hypeedge_trading::funding_arb::runtime::FundingArbRuntimeDependencies>,
        >,
        mm_runtime: Option<Arc<MarketMakerRuntimeFactory>>,
        action_budget: Option<Arc<tokio::sync::Mutex<ActionBudgetController>>>,
        account_tracker: Arc<hypeedge_trading::account::AccountTracker>,
    ) -> Self {
        // The wired account tracker is the one the clearinghouse poller fills;
        // AppState::new created an empty one that would otherwise shadow it.
        state.account_tracker = account_tracker;
        state.action_budget = action_budget;
        state.execution = execution.clone();
        state.market_data = market_data;
        state.instrument_meta = instrument_meta;
        state.config_versions = config_versions;
        state.trading_enabled = trading_enabled;
        state.safety_mode = safety_mode;
        let mm_runtimes = Arc::new(std::sync::Mutex::new(HashMap::new()));
        state.mm_runtimes = mm_runtimes.clone();
        state.mm_snapshot_provider = mm_runtime.as_ref().map(|_factory| {
            let runtimes = mm_runtimes.clone();
            let provider: MmSnapshotProvider = Arc::new(move |strategy_id: &str| {
                let runtimes = runtimes.clone();
                let strategy_id = strategy_id.to_string();
                Box::pin(async move {
                    let runtime = runtimes.lock().unwrap().get(&strategy_id).cloned()?;
                    Some(runtime.snapshot().await)
                })
            });
            provider
        });
        if let (Some(outbox), Some(pool)) = (&sse_outbox, &sse_pool) {
            let broker = Arc::new(crate::sse_broker::SseBroker::new(
                state.event_bus.clone(),
                Some(outbox.clone()),
                Some(pool.clone()),
                1000,
                256,
            ));
            state.sse_broker = broker.clone();
            // Outbox → SSE relay (wiring follow-up): poll the durable outbox
            // and publish committed events to the broker so `/api/v1/events`
            // delivers the durable stream.
            let outbox = outbox.clone();
            let pool = pool.clone();
            tokio::spawn(async move {
                loop {
                    let events = match outbox.claim_batch(&pool, "sse-relay", 200).await {
                        Ok(events) => events,
                        Err(e) => {
                            tracing::warn!(error = %e, "sse_relay_claim_failed");
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                            continue;
                        }
                    };
                    if events.is_empty() {
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                        continue;
                    }
                    for event in &events {
                        broker.publish(event).await;
                        let _ = outbox.mark_published(&pool, event, "sse-relay").await;
                    }
                }
            });
        }
        // 6f: when the execution engine is wired, register the real trend-follow
        // plugin (replacing the noop) so strategy lifecycle drives actual orders.
        if let Some(engine) = &execution {
            state.strategies = StrategyControlPlane::with_real_plugins(
                state.event_bus.clone(),
                state.account_tracker.clone(),
                engine.clone(),
                funding_arb_deps,
                mm_runtime,
                mm_runtimes,
            );
        }
        // Durable strategy control plane: replace the in-memory store with the
        // Postgres one whenever a pool is wired, then restore persisted
        // instances so a restart resumes (or faults) them.
        if let Some(pool) = &sse_pool {
            let store: Arc<dyn StrategyStateStore> =
                Arc::new(PostgresStrategyStateStore::new(pool.clone()));
            let allocations = Arc::new(InMemoryStrategyAllocationManager::new());
            let supervisor = Arc::new(StrategySupervisor::new(
                state.strategies.registry.clone(),
                store.clone(),
                allocations.clone(),
            ));
            state.strategies = StrategyControlPlane {
                supervisor: supervisor.clone(),
                registry: state.strategies.registry.clone(),
                state_store: store,
                allocations,
            };
            let restore = supervisor.clone();
            tokio::spawn(async move {
                if let Err(e) = restore.restore().await {
                    tracing::warn!(error = %e, "strategy_restore_failed");
                }
            });
        }
        state
    }
}

impl StrategyControlPlane {
    /// Build a control plane whose registry registers the real strategy
    /// runtimes (6f). Falls back to noop plugins for strategy types without a
    /// wired runtime yet.
    #[allow(clippy::too_many_arguments)]
    pub fn with_real_plugins(
        event_bus: Arc<EventBus>,
        tracker: Arc<hypeedge_trading::account::AccountTracker>,
        execution: Arc<hypeedge_trading::execution::ExecutionEngine>,
        funding_arb_deps: Option<
            Arc<hypeedge_trading::funding_arb::runtime::FundingArbRuntimeDependencies>,
        >,
        mm_runtime: Option<Arc<MarketMakerRuntimeFactory>>,
        mm_runtimes: Arc<std::sync::Mutex<HashMap<String, Arc<MarketMakerRuntime>>>>,
    ) -> Self {
        let mut registry = StrategyRegistry::new();
        registry.register_plugin(hypeedge_trading::strategy::build_trend_follow_plugin(
            event_bus.clone(),
            Some(tracker as Arc<dyn hypeedge_trading::strategy::StrategyAccountView>),
            execution as Arc<dyn ExecutionClient>,
        ));
        // funding_arb real plugin when its deps are wired.
        if let Some(deps) = funding_arb_deps {
            registry.register_plugin(
                hypeedge_trading::funding_arb::runtime::build_funding_arb_plugin(Some(deps)),
            );
        }
        // market_maker real plugin when its factory is wired.
        if let Some(factory) = mm_runtime {
            registry.register_plugin(hypeedge_trading::strategy::StrategyTypePlugin {
                strategy_type: "market_maker".to_string(),
                capabilities: hypeedge_trading::strategy::market_maker_capabilities(),
                factory: Arc::new(move |ctx| {
                    let Some(runtime) = factory.build(ctx) else {
                        return Arc::new(hypeedge_trading::strategy::FaultedRuntimeHandle {
                            message: format!(
                                "market-maker runtime construction failed for {}",
                                ctx.instance.strategy_id
                            ),
                        });
                    };
                    mm_runtimes
                        .lock()
                        .unwrap()
                        .insert(ctx.instance.strategy_id.clone(), runtime.clone());
                    Arc::new(hypeedge_trading::market_maker::MarketMakerRuntimeHandle::new(runtime))
                }),
            });
        }
        // Remaining types without a wired runtime keep a noop handle.
        for strategy_type in ["trend_follow", "market_maker", "funding_arb"] {
            if registry.contains(strategy_type) {
                continue;
            }
            let capabilities = match strategy_type {
                "market_maker" => hypeedge_trading::strategy::market_maker_capabilities(),
                "funding_arb" => hypeedge_trading::strategy::funding_arb_capabilities(),
                _ => hypeedge_trading::strategy::trend_follow_capabilities(),
            };
            registry.register_plugin(hypeedge_trading::strategy::StrategyTypePlugin {
                strategy_type: strategy_type.to_string(),
                capabilities,
                factory: Arc::new(|_ctx| {
                    struct Noop;
                    #[async_trait::async_trait]
                    impl hypeedge_trading::strategy::StrategyRuntimeHandle for Noop {
                        async fn start(&self) -> Result<(), String> {
                            Ok(())
                        }
                        async fn set_mode(
                            &self,
                            _: hypeedge_domain::enums::MarketMakerLifecycle,
                        ) -> Result<(), String> {
                            Ok(())
                        }
                        async fn apply_config(
                            &self,
                            _: &hypeedge_trading::strategy::StrategyConfigSnapshot,
                        ) -> Result<(), String> {
                            Ok(())
                        }
                        async fn stop(&self) -> Result<(), String> {
                            Ok(())
                        }
                    }
                    Arc::new(Noop)
                }),
            });
        }
        let registry = Arc::new(registry);
        let state_store = Arc::new(InMemoryStrategyStateStore::new());
        let allocations = Arc::new(InMemoryStrategyAllocationManager::new());
        let supervisor = Arc::new(StrategySupervisor::new(
            registry.clone(),
            state_store.clone(),
            allocations.clone(),
        ));
        Self {
            supervisor,
            registry,
            state_store,
            allocations,
        }
    }
}

/// Register no-op runtime handles for the supported strategy types so the
/// supervisor can drive lifecycle. The app replaces these with real runtimes.
fn register_noop_plugins(registry: &mut StrategyRegistry) {
    for (strategy_type, capabilities) in [
        (
            "trend_follow",
            hypeedge_trading::strategy::trend_follow_capabilities(),
        ),
        (
            "market_maker",
            hypeedge_trading::strategy::market_maker_capabilities(),
        ),
        (
            "funding_arb",
            hypeedge_trading::strategy::funding_arb_capabilities(),
        ),
    ] {
        registry.register_plugin(hypeedge_trading::strategy::StrategyTypePlugin {
            strategy_type: strategy_type.to_string(),
            capabilities,
            factory: Arc::new(|_ctx| {
                struct Noop;
                #[async_trait::async_trait]
                impl hypeedge_trading::strategy::StrategyRuntimeHandle for Noop {
                    async fn start(&self) -> Result<(), String> {
                        Ok(())
                    }
                    async fn set_mode(
                        &self,
                        _mode: hypeedge_domain::enums::MarketMakerLifecycle,
                    ) -> Result<(), String> {
                        Ok(())
                    }
                    async fn apply_config(
                        &self,
                        _config: &hypeedge_trading::strategy::StrategyConfigSnapshot,
                    ) -> Result<(), String> {
                        Ok(())
                    }
                    async fn stop(&self) -> Result<(), String> {
                        Ok(())
                    }
                }
                Arc::new(Noop)
            }),
        });
    }
}

/// Sliding-window rate limiter keyed by a string (IP or actor).
#[derive(Clone)]
pub struct SlidingWindowLimiter {
    windows: Arc<tokio::sync::Mutex<std::collections::HashMap<String, VecDeque<Instant>>>>,
}

impl Default for SlidingWindowLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl SlidingWindowLimiter {
    pub fn new() -> Self {
        Self {
            windows: Arc::new(tokio::sync::Mutex::new(Default::default())),
        }
    }

    /// Whether a request under `key` is allowed within `limit` per 60s.
    pub async fn allow(&self, key: &str, limit: u64) -> bool {
        let mut windows = self.windows.lock().await;
        let window = windows.entry(key.to_string()).or_default();
        let cutoff = Instant::now() - std::time::Duration::from_secs(60);
        while let Some(front) = window.front() {
            if *front < cutoff {
                window.pop_front();
            } else {
                break;
            }
        }
        if window.len() as u64 >= limit {
            return false;
        }
        window.push_back(Instant::now());
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn limiter_enforces_limit() {
        let limiter = SlidingWindowLimiter::new();
        for _ in 0..3 {
            assert!(limiter.allow("ip:1", 3).await);
        }
        assert!(!limiter.allow("ip:1", 3).await);
        // Different key is unaffected.
        assert!(limiter.allow("ip:2", 3).await);
    }
}
