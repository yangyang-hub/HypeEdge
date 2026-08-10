//! Shared API state: the components handlers read, plus the sliding-window
//! rate limiter. Port of the FastAPI `app.state` wiring + `security.py`.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use hypeedge_config::settings::AppSettings;
use hypeedge_infra::event_bus::EventBus;
use hypeedge_trading::market_data::BookManager;
use hypeedge_trading::risk::KillSwitch;
use hypeedge_trading::strategy::{
    InMemoryStrategyAllocationManager, InMemoryStrategyStateStore, StrategyRegistry,
    StrategySupervisor,
};

use crate::auth::RoleTokens;
use crate::sse_broker::SseBroker;

/// The strategy control plane the API drives (from `HypeEdgeApp`).
#[derive(Clone)]
pub struct StrategyControlPlane {
    pub supervisor: Arc<StrategySupervisor>,
    pub registry: Arc<StrategyRegistry>,
    pub state_store: Arc<InMemoryStrategyStateStore>,
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
    pub mm_snapshot_provider: Option<
        Arc<
            dyn Fn() -> Option<hypeedge_trading::market_maker::MarketMakerRuntimeSnapshot>
                + Send
                + Sync,
        >,
    >,
    /// Optional trading-enabled flag (from the app lifecycle).
    pub trading_enabled: Arc<tokio::sync::RwLock<bool>>,
    pub safety_mode: Arc<tokio::sync::RwLock<String>>,
    /// The live account tracker (positions/equity/PnL). Wired by the app; the
    /// in-memory default is empty until the clearinghouse poller runs.
    pub account_tracker: Arc<hypeedge_trading::account::AccountTracker>,
    /// The canary release gate evaluator (pure fail-closed decisions for the
    /// deployment stages). Constructed from defaults; wired by the app.
    pub canary_gate: Arc<hypeedge_trading::risk::canary::CanaryGateEvaluator>,
    /// Durable config-version repository (Postgres-backed). `None` when the
    /// app runs without a database → config-version routes return 503.
    pub config_versions: Option<Arc<dyn hypeedge_storage::ConfigVersionStore>>,
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
        let state_store = Arc::new(InMemoryStrategyStateStore::new());
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
            trading_enabled: Arc::new(tokio::sync::RwLock::new(false)),
            safety_mode: Arc::new(tokio::sync::RwLock::new("starting".into())),
            account_tracker,
            canary_gate: Arc::new(hypeedge_trading::risk::CanaryGateEvaluator::new()),
            config_versions: None,
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
