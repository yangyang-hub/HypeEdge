//! Trading-runtime assembly (wiring, 6d/6e/6f): constructs the live market-data
//! chain, the account poller/ingestor/reconciler, the execution engine + durable
//! worker, the risk stack, and the strategy runtimes in dependency order. Gated
//! on the feature flags so a control-plane-only deployment still works.
//!
//! The `HypeEdgeApp` routes the assembled pieces into `AppState` (which the API
//! routes read instead of stubs).

use std::sync::Arc;

use hypeedge_config::settings::AppSettings;
use hypeedge_domain::enums::SafetyMode;
use hypeedge_domain::models::RiskLimits;
use hypeedge_domain::traits::{DurableOrderStore, ExecutionClient, SystemStateStore};
use hypeedge_infra::event_bus::EventBus;
use hypeedge_storage::adapters::{PooledDurableOrderStore, PooledExecutionCommandQueue};
use hypeedge_storage::exchange_ingestor_store::PostgresExchangeFactProjector;
use hypeedge_storage::system_state_store::PooledSystemStateStore;
use hypeedge_trading::account::{
    AccountStatePoller, AccountTracker, ExchangeEventIngestor, InfoClient,
    LayeredAccountHealthProvider, RestAccountStateSource,
};
use hypeedge_trading::execution::{
    ExecutionEngine, ExecutionEngineConfig, HyperliquidExchangeClient, NonceQueue, OrderNormalizer,
    SignedActionExecutor,
};
use hypeedge_trading::market_data::{
    BookManager, InstrumentMetaCache, LiveMarketDataProvider, RateLimiter, RestClient,
    WebSocketFeed, WsFeedConfig,
};
use hypeedge_trading::risk::{ActionBudgetController, KillSwitch, RiskChecker, SafetyController};

/// What a fully wired runtime hands to the app / API state.
pub struct RuntimeWiring {
    pub settings: Arc<AppSettings>,
    pub event_bus: Arc<EventBus>,
    pub kill_switch: Arc<KillSwitch>,
    pub books: Arc<tokio::sync::Mutex<BookManager>>,
    pub account_tracker: Arc<AccountTracker>,
    pub execution: Option<Arc<ExecutionEngine>>,
    pub market_data: Option<Arc<LiveMarketDataProvider>>,
    pub config_versions: Option<Arc<dyn hypeedge_storage::ConfigVersionStore>>,
    pub sse_outbox: Option<Arc<hypeedge_storage::outbox::PostgresOutboxStore>>,
    pub sse_pool: Option<sqlx::PgPool>,
    pub trading_enabled: Arc<tokio::sync::RwLock<bool>>,
    pub safety_mode: Arc<tokio::sync::RwLock<String>>,
    pub action_budget: Option<Arc<tokio::sync::Mutex<ActionBudgetController>>>,
    /// Funding-arb runtime dependencies (wiring follow-up), when a store is wired.
    pub funding_arb_deps:
        Option<Arc<hypeedge_trading::funding_arb::runtime::FundingArbRuntimeDependencies>>,
    /// The live market-maker runtime (wiring follow-up) for the WS snapshot provider.
    pub mm_runtime: Option<Arc<hypeedge_trading::market_maker::MarketMakerRuntime>>,
}

/// Build the full runtime in dependency order. When the V2 trading chain is not
/// enabled, returns an API-only wiring (matching the previous skeleton).
pub async fn build_runtime(
    settings: &AppSettings,
    event_bus: Arc<EventBus>,
) -> Result<RuntimeWiring, String> {
    let rate_limiter = Arc::new(RateLimiter::new(
        hypeedge_trading::market_data::IP_WEIGHT_LIMIT_PER_MIN,
        settings.risk.action_credits_low_watermark as i64,
    ));
    let api_url = settings.exchange.api_url.trim_end_matches('/').to_string();
    let rest = Arc::new(
        RestClient::new(
            &api_url,
            rate_limiter.clone(),
            settings.market_data.backfill_batch_size,
        )
        .map_err(|e| format!("rest client: {e}"))?,
    );
    let meta_cache = Arc::new(InstrumentMetaCache::new(
        rest.clone(),
        Some(hypeedge_trading::market_data::META_REFRESH_INTERVAL_HOURS),
    ));
    let books = Arc::new(tokio::sync::Mutex::new(BookManager::new(
        settings.market_data.l2_book_depth as usize,
    )));
    let account_tracker = Arc::new(AccountTracker::new());
    let safety_mode = Arc::new(tokio::sync::RwLock::new("starting".into()));
    let trading_enabled = Arc::new(tokio::sync::RwLock::new(false));

    if !settings.features.v2_trading_enabled() {
        return Ok(RuntimeWiring {
            settings: Arc::new(settings.clone()),
            event_bus: event_bus.clone(),
            kill_switch: Arc::new(KillSwitch::new(
                event_bus.clone(),
                settings.risk.kill_switch_enabled,
            )),
            books,
            account_tracker,
            execution: None,
            market_data: None,
            config_versions: None,
            sse_outbox: None,
            sse_pool: None,
            trading_enabled,
            safety_mode,
            action_budget: None,
            funding_arb_deps: None,
            mm_runtime: None,
        });
    }

    // Mainnet hard-disable (design doc §7): trading components are refused on
    // mainnet unless the operator explicitly opts in via the env flag. Without
    // this, a `HYPE_ENV=mainnet` boot with credentials would place real orders.
    if settings.is_mainnet()
        && !std::env::var("HYPE_MAINNET_TRADING_ENABLED")
            .map(|v| v == "1")
            .unwrap_or(false)
    {
        return Err(
            "mainnet live trading is hard-disabled (design doc §7); set HYPE_MAINNET_TRADING_ENABLED=1 to override".into(),
        );
    }

    // Exchange credentials gate (never trade without them).
    if !settings.exchange.is_configured() {
        return Err(
            "trading enabled but exchange account_address / agent_private_key are unset".into(),
        );
    }
    let account = settings.exchange.account_address.to_lowercase();
    let is_mainnet = settings.is_mainnet();
    let private_key = parse_private_key(&settings.exchange.agent_private_key)?;
    let exchange = Arc::new(HyperliquidExchangeClient::new(
        private_key,
        is_mainnet,
        format!("{api_url}/exchange"),
        format!("{api_url}/info"),
        account.clone(),
    ));

    // ---------- Market data chain (6c/6d) ----------
    let market_data = {
        let provider = Arc::new(LiveMarketDataProvider::new(
            event_bus.clone(),
            rest.clone(),
            books.clone(),
        ));
        provider.start();
        Some(provider)
    };

    // WS feed owns its own book manager; run() needs the Arc alone.
    {
        let feed = Arc::new(WebSocketFeed::from_config(WsFeedConfig {
            url: settings.exchange.ws_url.clone(),
            coins: settings.market_data.coins.clone(),
            spot_coins: settings.market_data.spot_coins.clone(),
            channels: settings.market_data.ws_subscriptions.clone(),
            candle_intervals: settings.market_data.candle_intervals.clone(),
            book_depth: settings.market_data.l2_book_depth as usize,
            reconnect_delay_min: settings.market_data.ws_reconnect_delay_min,
            reconnect_delay_max: settings.market_data.ws_reconnect_delay_max,
        }));
        let bus = event_bus.clone();
        tokio::spawn(async move { feed.run(bus).await });
    }

    // Instrument metadata background refresh.
    {
        let meta = meta_cache.clone();
        tokio::spawn(async move {
            if let Err(e) = meta.ensure_loaded().await {
                tracing::warn!(error = %e, "instrument_meta_load_failed");
            }
        });
    }

    // ---------- Account chain (6d) ----------
    let health = Arc::new(LayeredAccountHealthProvider::default());
    let state_source = Arc::new(RestAccountStateSource::new(
        rest.clone(),
        &account,
        account_tracker.clone(),
    )?);
    let poller = AccountStatePoller::new(
        state_source,
        account_tracker.clone(),
        health.clone(),
        settings.market_making.account_poll_interval_seconds,
        settings
            .market_making
            .near_risk_account_poll_interval_seconds,
        None,
        None,
    )?;
    {
        let poller = Arc::new(poller);
        tokio::spawn(async move {
            if let Err(e) = poller.run().await {
                tracing::error!(error = %e, "account_state_poller_exited");
            }
        });
    }

    // Ingestor + reconciler require Postgres (durable projector). A Postgres
    // failure degrades to a no-DB wiring (trading still possible without the
    // durable ledger, matching the Python-era fallback) rather than killing the
    // whole runtime build.
    let (_pg, config_versions, sse_outbox, sse_pool, durable_order_store) =
        if settings.postgres.url.trim().is_empty() {
            (None, None, None, None, None)
        } else {
            match hypeedge_storage::Postgres::connect(
                &settings.postgres.url,
                settings.postgres.pool_size,
            )
            .await
            {
                Ok(storage) => {
                    let pool = storage.pool.clone();
                    let projector =
                        Arc::new(PostgresExchangeFactProjector::new(pool.clone(), &account));
                    let info: Arc<dyn InfoClient> = rest.clone();
                    let mut ingestor = ExchangeEventIngestor::new(
                        &account,
                        projector.clone(),
                        info,
                        Some(account_tracker.clone()),
                        settings.market_making.account_poll_interval_seconds,
                    );
                    if let Err(e) = ingestor.recover_history().await {
                        tracing::warn!(error = %e, "ingestor_history_recovery_failed");
                    }
                    tokio::spawn(async move {
                        ingestor.run_until_closed().await;
                    });
                    let cfg_versions = Arc::new(hypeedge_storage::PostgresConfigVersionStore::new(
                        pool.clone(),
                    ))
                        as Arc<dyn hypeedge_storage::ConfigVersionStore>;
                    let outbox = Arc::new(hypeedge_storage::outbox::PostgresOutboxStore::new(
                        settings.postgres.command_lease_seconds as i64,
                    ));
                    let order_store = Arc::new(PooledDurableOrderStore::new(
                        pool.clone(),
                        None,
                        30.0,
                        settings.postgres.risk_reservation_ttl_seconds as i64,
                    )) as Arc<dyn DurableOrderStore>;
                    (
                        Some(storage),
                        Some(cfg_versions),
                        Some(outbox),
                        Some(pool),
                        Some(order_store),
                    )
                }
                Err(e) => {
                    tracing::error!(error = %e, "postgres_connect_failed_degrading_no_db");
                    (None, None, None, None, None)
                }
            }
        };

    // ---------- Risk (6e) ----------
    let risk_limits = RiskLimits {
        max_position_pct: settings.risk.max_position_pct,
        max_strategy_loss_pct: settings.risk.max_strategy_loss_pct,
        max_drawdown_pct: settings.risk.max_drawdown_pct,
        max_leverage: settings.risk.max_leverage,
        timeout_ms: settings.risk.risk_check_timeout_ms as u64,
        account_stale_seconds: (settings.market_making.account_poll_interval_seconds * 2.0)
            .max(5.0),
    };
    let risk_checker = Arc::new(RiskChecker::new(account_tracker.clone(), risk_limits));
    let safety = Arc::new(tokio::sync::Mutex::new(SafetyController::new(
        SafetyMode::Starting,
    )));
    let action_budget = Arc::new(tokio::sync::Mutex::new(ActionBudgetController::new(
        &account,
        map_action_budget_settings(&settings.action_budget),
    )?));

    // Kill switch: cancel-all hook + durable state store. The engine is
    // constructed after; the closure reads it via the shared cell.
    let engine_cell: Arc<std::sync::OnceLock<Arc<ExecutionEngine>>> =
        Arc::new(std::sync::OnceLock::new());
    let mut kill_switch = KillSwitch::new(event_bus.clone(), settings.risk.kill_switch_enabled);
    {
        let cell = engine_cell.clone();
        kill_switch = kill_switch.with_cancel_all(move || {
            let cell = cell.clone();
            Box::pin(async move {
                if let Some(engine) = cell.get() {
                    let _ = engine.cancel_all_orders(None).await;
                }
            })
        });
    }
    if let Some(pool) = &sse_pool {
        kill_switch =
            kill_switch.with_state_store(Arc::new(PooledSystemStateStore::new(pool.clone())));
    }
    // Restore a persisted kill switch latch across restarts (A15).
    if let Some(pool) = &sse_pool {
        let store = Arc::new(PooledSystemStateStore::new(pool.clone()));
        if let Ok(Some(state)) = store.load().await
            && state.kill_switch_active
        {
            tracing::warn!("kill_switch_restored_from_durable_state");
            kill_switch.restore_active(state.reason).await;
        }
    }
    let kill_switch = Arc::new(kill_switch);

    // ---------- Execution (6e) ----------
    let nonce = Arc::new(NonceQueue::new());
    let engine = Arc::new(ExecutionEngine::new(ExecutionEngineConfig {
        nonce: nonce.clone(),
        event_bus: event_bus.clone(),
        kill_switch: kill_switch.clone(),
        exchange: exchange.clone(),
        account_address: account.clone(),
        safety: Some(safety.clone()),
        risk_checker: Some(risk_checker.clone()),
        rate_limiter: Some(rate_limiter.clone()),
        durable_store: durable_order_store,
        market_data_provider: market_data
            .clone()
            .map(|p| p as Arc<dyn hypeedge_domain::traits::MarketDataProvider>),
        order_normalizer: Some(Arc::new(OrderNormalizer::new(meta_cache.clone()))),
        asset_index_provider: Some(meta_cache.clone()),
        deferred_execution: true,
        market_price_stale_seconds: settings.risk.market_price_stale_seconds,
        durable_kill_trigger: None,
        action_budget: Some(action_budget.clone()),
    }));
    let _ = engine_cell.set(engine.clone());

    // Durable worker: claims commands and dispatches through the engine.
    if let Some(pool) = &sse_pool {
        let queue = Arc::new(PooledExecutionCommandQueue::new(
            pool.clone(),
            settings.postgres.command_lease_seconds as i64,
            settings.postgres.unknown_recheck_seconds as i64,
        ));
        let worker = SignedActionExecutor::new(
            queue,
            engine.clone(),
            settings.postgres.command_poll_interval_ms as u64,
            Some("signed-action-1".into()),
        );
        tokio::spawn(async move {
            if let Err(e) = worker.run().await {
                tracing::error!(error = %e, "signed_action_executor_exited");
            }
        });
    }

    // Outbox → SSE relay is wired by AppState::from_wiring (the durable broker
    // is constructed there from sse_outbox + sse_pool).

    // ---------- Strategy runtimes (wiring follow-up) ----------
    // Funding-arb deps: live scanner + instrument meta + durable cycle store.
    let funding_arb_deps = sse_pool.as_ref().and_then(|pool| {
        if !settings.features.funding_arb_execution_enabled {
            return None;
        }
        let scanner = Arc::new(
            hypeedge_trading::funding_arb::live_scanner::LiveFundingArbScanner::new(
                market_data.clone()?,
                rest.clone(),
            ),
        );
        let meta = Arc::new(
            hypeedge_trading::funding_arb::live_scanner::InstrumentCacheFundingArbMeta::new(
                meta_cache.clone(),
            ),
        );
        let cycles = Arc::new(
            hypeedge_storage::funding_arb_store::PostgresFundingArbCycleStore::new(pool.clone()),
        );
        let tracker_ref = account_tracker.clone();
        let fa = settings.funding_arb.clone();
        Some(Arc::new(
            hypeedge_trading::funding_arb::runtime::FundingArbRuntimeDependencies {
                execution: engine.clone(),
                scanner,
                tracker: tracker_ref,
                cycles,
                meta,
                trading_ready: Box::new(|| true),
                kill_switch_active: Box::new({
                    let ks = kill_switch.clone();
                    move || {
                        let ks = ks.clone();
                        tokio::runtime::Handle::current()
                            .block_on(async move { ks.is_active().await })
                    }
                }),
                account_allows_risk_increase: Box::new(|| true),
                reconcile: Box::new(|| Box::pin(async { true })),
                deployment: hypeedge_trading::funding_arb::runtime::FundingArbDeployment {
                    max_notional_usd: fa.max_notional_usd.0,
                    poll_interval_seconds: fa.poll_interval_seconds,
                    order_status_poll_interval_seconds: fa.order_status_poll_interval_seconds,
                    max_leg_attempts: fa.max_leg_attempts,
                    market_stale_seconds: fa.market_stale_seconds,
                    min_spot_24h_volume_usd: fa.min_spot_24h_volume_usd.0,
                    min_perp_24h_volume_usd: fa.min_perp_24h_volume_usd.0,
                    min_top_book_depth_usd: fa.min_top_book_depth_usd.0,
                    max_combined_spread_bps: fa.max_combined_spread_bps.0,
                },
                account_address: account.clone(),
            },
        ))
    });

    // Market-maker runtime (only when the MM feature is enabled and there is a
    // live engine + provider).
    let mm_runtime = if settings.features.market_making_enabled {
        build_market_maker_runtime(
            event_bus.clone(),
            account_tracker.clone(),
            action_budget.clone(),
            market_data.clone(),
            engine.clone(),
        )
    } else {
        None
    };

    Ok(RuntimeWiring {
        settings: Arc::new(settings.clone()),
        event_bus: event_bus.clone(),
        kill_switch,
        books,
        account_tracker,
        execution: Some(engine),
        market_data,
        config_versions,
        sse_outbox,
        sse_pool,
        trading_enabled: {
            *trading_enabled.write().await = true;
            trading_enabled
        },
        safety_mode: {
            *safety_mode.write().await = "running".into();
            safety_mode
        },
        action_budget: Some(action_budget),
        funding_arb_deps,
        mm_runtime,
    })
}

/// A minimal control-plane-only wiring (no trading). Used when the V2 chain is
/// disabled or the runtime build fails — the API still serves status/health.
pub fn build_control_plane(settings: &AppSettings, event_bus: Arc<EventBus>) -> RuntimeWiring {
    RuntimeWiring {
        settings: Arc::new(settings.clone()),
        event_bus: event_bus.clone(),
        kill_switch: Arc::new(KillSwitch::new(
            event_bus.clone(),
            settings.risk.kill_switch_enabled,
        )),
        books: Arc::new(tokio::sync::Mutex::new(BookManager::new(
            settings.market_data.l2_book_depth as usize,
        ))),
        account_tracker: Arc::new(AccountTracker::new()),
        execution: None,
        market_data: None,
        config_versions: None,
        sse_outbox: None,
        sse_pool: None,
        trading_enabled: Arc::new(tokio::sync::RwLock::new(false)),
        safety_mode: Arc::new(tokio::sync::RwLock::new("starting".into())),
        action_budget: None,
        funding_arb_deps: None,
        mm_runtime: None,
    }
}

/// Parse a hex agent private key into 32 bytes.
fn parse_private_key(hex_key: &str) -> Result<[u8; 32], String> {
    let raw = hex_key.strip_prefix("0x").unwrap_or(hex_key);
    let bytes = hex::decode(raw).map_err(|e| format!("private key hex: {e}"))?;
    bytes
        .try_into()
        .map_err(|_| "private key must be 32 bytes".into())
}

/// Map the config action-budget settings onto the trading crate's struct.
fn map_action_budget_settings(
    s: &hypeedge_config::settings::ActionBudgetSettings,
) -> hypeedge_trading::risk::ActionBudgetSettings {
    use hypeedge_trading::risk::ActionBudgetSettings as T;
    T {
        remote_snapshot_max_age_seconds: s.remote_snapshot_max_age_seconds,
        remote_poll_interval_normal_seconds: s.remote_poll_interval_normal_seconds,
        remote_poll_interval_conserve_seconds: s.remote_poll_interval_conserve_seconds,
        remote_poll_interval_critical_seconds: s.remote_poll_interval_critical_seconds,
        address_conserve_threshold: s.address_conserve_threshold,
        address_critical_threshold: s.address_critical_threshold,
        address_cancel_only_threshold: s.address_cancel_only_threshold,
        cancel_retry_buffer: s.cancel_retry_buffer,
        close_action_reserve: s.close_action_reserve,
        cancel_headroom_initial: s.cancel_headroom_initial,
        ip_weight_limit_per_minute: s.ip_weight_limit_per_minute,
        ip_emergency_reserve: s.ip_emergency_reserve,
        runway_conserve_hours: s.runway_conserve_hours,
        runway_critical_hours: s.runway_critical_hours,
        runway_cancel_only_hours: s.runway_cancel_only_hours,
        minimum_marginal_usdc_per_action: s.minimum_marginal_usdc_per_action,
        minimum_actions_for_economic_gate: s.minimum_actions_for_economic_gate,
    }
}

/// Build the live market-maker runtime with provider adapters (wiring follow-up).
/// Returns `None` when a required live dependency is missing.
fn build_market_maker_runtime(
    event_bus: Arc<EventBus>,
    tracker: Arc<AccountTracker>,
    budget: Arc<tokio::sync::Mutex<ActionBudgetController>>,
    market_data: Option<Arc<LiveMarketDataProvider>>,
    engine: Arc<ExecutionEngine>,
) -> Option<Arc<hypeedge_trading::market_maker::MarketMakerRuntime>> {
    use hypeedge_trading::market_maker::adapters::{
        ControllerBudgetProvider, EngineQuotePlanClient, EngineSlotProvider,
        ProviderFundingProvider, TrackerHealthProvider, TrackerInventoryProvider,
    };
    use hypeedge_trading::market_maker::runtime::MarketMakerRuntime;
    use hypeedge_trading::trading::quote_coordinator::{QuoteCoordinator, QuoteCoordinatorConfig};

    let market_data = market_data?;
    let feature_engine = Arc::new(tokio::sync::Mutex::new(
        hypeedge_trading::market_data::MarketFeatureEngine::new(20, 60.0, 10_000).ok()?,
    ));
    let inventory = Arc::new(TrackerInventoryProvider::new(tracker.clone()));
    let budget_provider = Arc::new(ControllerBudgetProvider::new(budget));
    let health = Arc::new(TrackerHealthProvider::new(tracker));
    let slots = Arc::new(EngineSlotProvider::new(engine.clone()));
    let commands = Arc::new(EngineQuotePlanClient::new(engine));
    let funding = Some(Arc::new(ProviderFundingProvider::new(market_data))
        as Arc<
            dyn hypeedge_trading::market_maker::runtime::FundingSnapshotProvider,
        >);
    let coordinator = QuoteCoordinator::new(QuoteCoordinatorConfig::default()).ok()?;

    // The runtime requires a strategy_id/session_id/symbol; the supervisor
    // re-binds per instance, so this is a placeholder identity the handle
    // factory overrides.
    MarketMakerRuntime::new(
        "mm_wiring".into(),
        "mm_session".into(),
        String::new(),
        "BTC".into(),
        event_bus,
        feature_engine,
        hypeedge_trading::market_maker::MarketMakerPolicy::new(),
        coordinator,
        inventory,
        budget_provider,
        health,
        slots,
        commands,
        funding,
        chrono::Duration::seconds(5),
    )
    .ok()
    .map(Arc::new)
}
