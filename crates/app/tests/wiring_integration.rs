//! Assembly-level integration test (P0-1 / P0-3 regression net).
//!
//! The app wiring starts the [`SafetyController`] in `Starting` — which
//! rejects every placement — and the previous code never moved it to `Normal`,
//! silently refusing to trade while reporting a healthy `"running"` status.
//! This test drives the wiring's [`SafetyLifecycle`] against a real
//! [`ExecutionEngine`] gate path (no network, no Postgres):
//!
//! 1. `Starting` → `submit_order_impl` is rejected;
//! 2. after the startup-completion logic (`mark_startup_complete`) → the same
//!    submit passes the gates and returns a `Submitted` order;
//! 3. after the kill switch triggers (and the lifecycle refreshes) → the
//!    submit is rejected again and the API mirrors report `halted` / disabled.
//!
//! It also covers the account-readiness gate the startup transition waits on,
//! and the kill-switch reset path restoring `Normal`.

use std::sync::Arc;

use hypeedge_app::runtime::SafetyLifecycle;
use hypeedge_domain::decimal::{Decimal, Size, Usd};
use hypeedge_domain::enums::{OrderStatus, OrderType, SafetyMode, Side, TimeInForce};
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::models::{AccountState, OrderIntent};
use hypeedge_infra::event_bus::EventBus;
use hypeedge_trading::account::AccountTracker;
use hypeedge_trading::execution::{
    CancelByCloidWire, CancelWire, ExchangeClient, ExecutionEngine, ExecutionEngineConfig,
    NonceQueue, OrderWire,
};
use hypeedge_trading::risk::{KillSwitch, SafetyController};
use serde_json::Value;
use tokio::sync::RwLock;

/// A stub exchange that is never reached: the test engine runs with
/// `deferred_execution = true`, so submission stops at the gate + persistence
/// boundary. Every method fails loudly if it is ever called.
///
/// The [`ExchangeClient`] trait is `#[async_trait]`-based, and `async_trait`
/// is not a dependency of this crate, so the impl is written in the macro's
/// expanded form (methods return boxed futures).
struct StubExchange;

impl ExchangeClient for StubExchange {
    fn order<'l, 'f>(
        &'l self,
        _orders: Vec<OrderWire>,
        _nonce: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'f>>
    where
        'l: 'f,
        Self: 'f,
    {
        Box::pin(async {
            Err("stub exchange: order() must not be called in the deferred test path".into())
        })
    }
    fn cancel<'l, 'f>(
        &'l self,
        _cancels: Vec<CancelWire>,
        _nonce: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'f>>
    where
        'l: 'f,
        Self: 'f,
    {
        Box::pin(async { Err("stub exchange: cancel() must not be called".into()) })
    }
    fn cancel_by_cloid<'l, 'f>(
        &'l self,
        _cancels: Vec<CancelByCloidWire>,
        _nonce: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'f>>
    where
        'l: 'f,
        Self: 'f,
    {
        Box::pin(async { Err("stub exchange: cancel_by_cloid() must not be called".into()) })
    }
    fn update_leverage<'l, 'f>(
        &'l self,
        _asset: i64,
        _is_cross: bool,
        _leverage: i64,
        _nonce: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'f>>
    where
        'l: 'f,
        Self: 'f,
    {
        Box::pin(async { Err("stub exchange: update_leverage() must not be called".into()) })
    }
    fn query_order_by_cloid<'l, 'c, 'f>(
        &'l self,
        _cloid: &'c str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<Value>, String>> + Send + 'f>,
    >
    where
        'l: 'f,
        'c: 'f,
        Self: 'f,
    {
        Box::pin(async { Err("stub exchange: query_order_by_cloid() must not be called".into()) })
    }
}

fn intent(cloid: &str) -> OrderIntent {
    OrderIntent {
        symbol: "BTC".into(),
        side: Side::Buy,
        size: Size::new(Decimal::from_str_strict("0.1").unwrap()),
        price: Some(hypeedge_domain::decimal::Price::new(
            Decimal::from_str_strict("50000").unwrap(),
        )),
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::Gtc,
        strategy_id: Some("wiring-test".into()),
        sub_account: None,
        reduce_only: false,
        cloid: Some(cloid.into()),
        client_id: None,
        is_spot: false,
        risk_reducing: false,
        max_slippage_bps: 50,
    }
}

/// Build the same wiring the app assembles: `Starting` safety controller,
/// enabled kill switch, disabled mirrors, and an engine whose placement gate
/// consults exactly those two components.
struct Wiring {
    safety: Arc<tokio::sync::Mutex<SafetyController>>,
    kill_switch: Arc<KillSwitch>,
    trading_enabled: Arc<RwLock<bool>>,
    safety_mode: Arc<RwLock<String>>,
    engine: ExecutionEngine,
    lifecycle: Arc<SafetyLifecycle>,
}

fn wiring() -> Wiring {
    let bus = Arc::new(EventBus::new(64));
    let safety = Arc::new(tokio::sync::Mutex::new(SafetyController::new(
        SafetyMode::Starting,
    )));
    let kill_switch = Arc::new(KillSwitch::new(bus.clone(), true));
    let trading_enabled = Arc::new(RwLock::new(false));
    let safety_mode = Arc::new(RwLock::new("starting".into()));
    let lifecycle = Arc::new(SafetyLifecycle::new(
        safety.clone(),
        kill_switch.clone(),
        trading_enabled.clone(),
        safety_mode.clone(),
    ));
    let engine = ExecutionEngine::new(ExecutionEngineConfig {
        nonce: Arc::new(NonceQueue::new()),
        event_bus: bus,
        kill_switch: kill_switch.clone(),
        exchange: Arc::new(StubExchange),
        account_address: "0x0000000000000000000000000000000000000001".into(),
        safety: Some(safety.clone()),
        risk_checker: None,
        rate_limiter: None,
        durable_store: None,
        market_data_provider: None,
        order_normalizer: None,
        asset_index_provider: None,
        deferred_execution: true,
        market_price_stale_seconds: 5.0,
        durable_kill_trigger: None,
        action_budget: None,
    });
    Wiring {
        safety,
        kill_switch,
        trading_enabled,
        safety_mode,
        engine,
        lifecycle,
    }
}

#[tokio::test]
async fn starting_rejects_then_startup_completion_allows_then_kill_rejects() {
    let w = wiring();

    // Phase 1 — Starting: the placement gate rejects and the mirrors report
    // the fail-closed boot state (no more hardcoded "running"/true).
    let err = w.engine.submit_order_impl(intent("w1"), None).await;
    assert!(
        matches!(err, Err(HypeEdgeError::OrderRejected { .. })),
        "Starting must reject submissions, got: {err:?}"
    );
    assert!(
        !*w.trading_enabled.read().await,
        "trading must be disabled in Starting"
    );
    assert_eq!(*w.safety_mode.read().await, "starting");
    assert_eq!(w.safety.lock().await.mode(), SafetyMode::Starting);

    // Phase 2 — startup completion (poller first success + history recovery
    // done): the same submit passes the gates and reaches the engine as a
    // Submitted order.
    w.lifecycle.mark_startup_complete().await;
    assert_eq!(w.safety.lock().await.mode(), SafetyMode::Normal);
    let order = w.engine.submit_order_impl(intent("w1"), None).await;
    let order = order.expect("submit must pass after startup completion");
    assert_eq!(
        order.status,
        OrderStatus::Submitted,
        "order must reach the engine"
    );
    assert!(
        *w.trading_enabled.read().await,
        "trading must be enabled in Normal"
    );
    assert_eq!(*w.safety_mode.read().await, "normal");

    // Phase 3 — kill switch: the gate rejects again and the mirrors reflect
    // the halt (safety parked in Halted).
    w.kill_switch.trigger("integration-test").await;
    assert!(w.kill_switch.is_active().await);
    w.lifecycle.refresh().await;
    let err = w.engine.submit_order_impl(intent("w2"), None).await;
    assert!(
        matches!(err, Err(HypeEdgeError::KillSwitchTriggered { .. })),
        "kill switch must reject submissions, got: {err:?}"
    );
    assert_eq!(w.safety.lock().await.mode(), SafetyMode::Halted);
    assert!(
        !*w.trading_enabled.read().await,
        "trading must be disabled on kill"
    );
    assert_eq!(*w.safety_mode.read().await, "halted");

    // Phase 4 — reset: the operator cleared the latch; the lifecycle restores
    // Normal and placements resume.
    w.kill_switch.reset().await;
    assert!(!w.kill_switch.is_active().await);
    w.lifecycle.refresh().await;
    assert_eq!(w.safety.lock().await.mode(), SafetyMode::Normal);
    assert!(
        *w.trading_enabled.read().await,
        "trading must resume after reset"
    );
    assert_eq!(*w.safety_mode.read().await, "normal");
    let order = w.engine.submit_order_impl(intent("w3"), None).await;
    assert!(
        order.is_ok(),
        "submit must pass after a kill-switch reset, got: {order:?}"
    );
}

#[tokio::test]
async fn account_readiness_gates_startup_completion() {
    // The startup transition must wait for the account poller's first
    // authoritative clearinghouse snapshot — a tracker without data keeps the
    // system in Starting (fail-closed).
    let w = wiring();
    let tracker = Arc::new(AccountTracker::new());

    // No snapshot yet → not ready (bounded wait, no busy loop).
    let err = w
        .lifecycle
        .wait_for_account_ready(&tracker, std::time::Duration::from_millis(50))
        .await;
    assert!(err.is_err(), "must not be ready before the first snapshot");
    assert_eq!(w.safety.lock().await.mode(), SafetyMode::Starting);

    // First poller snapshot → ready.
    tracker.update_account_state(&AccountState {
        equity: Usd::new(Decimal::from_str_strict("10000").unwrap()),
        available_balance: Usd::new(Decimal::from_str_strict("9000").unwrap()),
        total_margin_used: Usd::ZERO,
        total_unrealized_pnl: Usd::ZERO,
        peak_equity: Usd::ZERO,
        sub_account: None,
    });
    w.lifecycle
        .wait_for_account_ready(&tracker, std::time::Duration::from_secs(2))
        .await
        .expect("ready once the poller has produced a snapshot");
}

#[tokio::test]
async fn funding_arb_entry_gates_reflect_real_state() {
    // P2-3: the funding-arb dependency gates wired by `build_runtime` must not
    // be `|| true`. This test re-creates the exact closures the runtime builds
    // (trading_ready / account_allows_risk_increase / reconcile) and asserts
    // they fail closed while the account is unknown or the kill switch is
    // latched.
    let bus = Arc::new(EventBus::new(64));
    let kill_switch = Arc::new(KillSwitch::new(bus.clone(), true));
    let trading_enabled = Arc::new(RwLock::new(false));
    let tracker = Arc::new(AccountTracker::new());
    let max_age_secs = 5.0;

    let trading_ready = {
        let ks = kill_switch.clone();
        let te = trading_enabled.clone();
        move || {
            let ks = ks.clone();
            let te = te.clone();
            Box::pin(async move { !ks.is_active().await && *te.read().await })
        }
    };
    let account_allows_risk_increase = {
        let tracker = tracker.clone();
        move || {
            let tracker = tracker.clone();
            Box::pin(async move {
                match tracker.last_update_ts() {
                    Some(ts) => {
                        let age_secs = (chrono::Utc::now() - ts).num_milliseconds() as f64 / 1000.0;
                        age_secs <= max_age_secs
                    }
                    None => false,
                }
            })
        }
    };
    let reconcile = {
        let tracker = tracker.clone();
        move || {
            let tracker = tracker.clone();
            Box::pin(async move { tracker.get_account_state().is_some() })
        }
    };

    // Unknown account + trading disabled → every gate fails closed.
    assert!(
        !trading_ready().await,
        "trading_ready must be false while disabled"
    );
    assert!(
        !account_allows_risk_increase().await,
        "risk-increase gate must fail closed without a tracker snapshot"
    );
    assert!(
        !reconcile().await,
        "reconcile must fail closed before any poll"
    );

    // Live account snapshot + trading enabled → gates open.
    *trading_enabled.write().await = true;
    tracker.update_account_state(&AccountState {
        equity: Usd::new(Decimal::from_str_strict("10000").unwrap()),
        available_balance: Usd::new(Decimal::from_str_strict("9000").unwrap()),
        total_margin_used: Usd::ZERO,
        total_unrealized_pnl: Usd::ZERO,
        peak_equity: Usd::ZERO,
        sub_account: None,
    });
    assert!(
        trading_ready().await,
        "trading_ready must open when enabled"
    );
    assert!(
        account_allows_risk_increase().await,
        "risk-increase gate must open with a fresh snapshot"
    );
    assert!(reconcile().await, "reconcile must open after a poll");

    // Kill switch latched → trading_ready closes again immediately.
    kill_switch.trigger("gate-test").await;
    assert!(!trading_ready().await, "trading_ready must close on kill");
}
