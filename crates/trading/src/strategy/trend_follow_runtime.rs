//! Trend-follow runtime handle + plugin factory, port of
//! `src/hypeedge/strategy/trend_follow_runtime.py`.
//!
//! Wraps a [`TrendFollowStrategy`] + [`StrategyRunner`] behind the
//! [`StrategyRuntimeHandle`] contract so the [`StrategySupervisor`] can drive
//! the strategy lifecycle.

use std::sync::Arc;

use async_trait::async_trait;
use hypeedge_domain::enums::{MarketMakerLifecycle, StrategyStatus};
use hypeedge_infra::event_bus::EventBus;
use tokio::sync::mpsc;

use super::base::Strategy;
use super::params::TrendParams;
use super::registry::{StrategyBuildContext, StrategyConfigSnapshot, StrategyRuntimeHandle};
use super::runner::StrategyRunner;
use super::trend_follow::{StrategyAccountView, TrendFollowStrategy};

/// The factory shape used by `build_trend_follow_factory`.
pub type TrendStrategyFactory =
    Box<dyn Fn(&StrategyBuildContext, TrendParams) -> TrendFollowStrategy + Send + Sync>;

/// The runtime handle wrapping a trend-follow strategy + runner.
pub struct TrendFollowRuntimeHandle {
    strategy_id: String,
    strategy: Arc<tokio::sync::Mutex<TrendFollowStrategy>>,
    config_error: Option<String>,
    stop_tx: Arc<tokio::sync::Mutex<Option<mpsc::Sender<()>>>>,
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<(), String>>>>,
    /// The process event bus the runner subscribes to (A4). Previously the
    /// runner was constructed with a private bus nothing ever published to, so
    /// the strategy silently never received any market data.
    event_bus: Arc<EventBus>,
}

impl TrendFollowRuntimeHandle {
    pub fn new(
        strategy_id: String,
        strategy: TrendFollowStrategy,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            strategy_id,
            strategy: Arc::new(tokio::sync::Mutex::new(strategy)),
            config_error: None,
            stop_tx: Arc::new(tokio::sync::Mutex::new(None)),
            task: tokio::sync::Mutex::new(None),
            event_bus,
        }
    }

    pub fn strategy_id(&self) -> &str {
        &self.strategy_id
    }
}

#[async_trait]
impl StrategyRuntimeHandle for TrendFollowRuntimeHandle {
    async fn start(&self) -> Result<(), String> {
        if let Some(e) = &self.config_error {
            return Err(format!("trend-follow config error: {e}"));
        }
        let mut task_guard = self.task.lock().await;
        if let Some(task) = task_guard.as_ref() {
            if !task.is_finished() {
                return Ok(()); // idempotent: runner already alive
            }
            // M-ST2: the runner task ended without a supervised stop (crashed
            // or its mailboxes closed). A finished JoinHandle can be dropped;
            // rebuild the task so the strategy keeps running.
            tracing::warn!(
                strategy_id = %self.strategy_id,
                "trend_follow_runner_ended_unexpectedly_restarting"
            );
            task_guard.take();
        }
        let (stop_tx, stop_rx) = mpsc::channel(1);
        *self.stop_tx.lock().await = Some(stop_tx);
        let adapter = LockedStrategyAdapter::new(self.strategy.clone());
        let mut runner = StrategyRunner::new(Box::new(adapter), self.event_bus.clone());
        let handle = tokio::spawn(async move { runner.run(stop_rx).await });
        *task_guard = Some(handle);
        Ok(())
    }

    /// The runner task is alive iff a task exists and has not finished.
    fn is_healthy(&self) -> bool {
        match self.task.try_lock() {
            Ok(guard) => guard
                .as_ref()
                .map(|task| !task.is_finished())
                .unwrap_or(false),
            // Locked by a concurrent lifecycle call; conservatively assume the
            // handle is healthy rather than faulting mid-operation.
            Err(_) => true,
        }
    }

    async fn set_mode(&self, mode: MarketMakerLifecycle) -> Result<(), String> {
        let mut strategy = self.strategy.lock().await;
        match mode {
            // H-ST2: trend-follow has no shadow product semantics; accepting it
            // would silently run the strategy for real. Reject explicitly so
            // the supervisor's capability gate and callers surface a clear
            // error (the API maps it to a lifecycle conflict).
            MarketMakerLifecycle::Warming => Ok(()),
            MarketMakerLifecycle::Shadow => {
                Err("strategy type trend_follow does not support shadow mode".to_string())
            }
            MarketMakerLifecycle::Running => {
                strategy.set_status(StrategyStatus::Running);
                Ok(())
            }
            MarketMakerLifecycle::Paused => {
                strategy.set_status(StrategyStatus::Paused);
                Ok(())
            }
            MarketMakerLifecycle::Stopped | MarketMakerLifecycle::Draining => {
                drop(strategy);
                self.stop().await
            }
            MarketMakerLifecycle::Faulted => {
                strategy.set_status(StrategyStatus::Error);
                drop(strategy);
                self.stop().await
            }
        }
    }

    async fn apply_config(&self, config: &StrategyConfigSnapshot) -> Result<(), String> {
        let params = decode_trend_follow_config(config)?;
        let mut strategy = self.strategy.lock().await;
        strategy.set_params(params);
        Ok(())
    }

    async fn stop(&self) -> Result<(), String> {
        if let Some(tx) = self.stop_tx.lock().await.take() {
            let _ = tx.send(()).await;
        }
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
        Ok(())
    }
}

/// A `Strategy` adapter that locks a shared `TrendFollowStrategy` per event.
///
/// M-ST6: the sync `Strategy` methods (`subscriptions`/`status`/`set_status`)
/// must never use `tokio::sync::Mutex::blocking_lock` — the runner invokes
/// them from an async runtime context, where `blocking_lock` panics. Instead
/// the subscriptions are cached once at construction (they are static for a
/// given strategy) and the status is mirrored in a `std::sync::Mutex`, kept in
/// sync whenever an async method holds the strategy lock.
struct LockedStrategyAdapter {
    inner: Arc<tokio::sync::Mutex<TrendFollowStrategy>>,
    subscriptions: Vec<hypeedge_domain::events::EventType>,
    status: std::sync::Mutex<StrategyStatus>,
}

impl LockedStrategyAdapter {
    fn new(inner: Arc<tokio::sync::Mutex<TrendFollowStrategy>>) -> Self {
        // `try_lock` never panics in an async context. At construction the
        // strategy lock is free (start() holds no lock), so the cache is the
        // strategy's real subscription set; the fallback keeps the adapter
        // usable if a lifecycle call races construction.
        let (subscriptions, status) = match inner.try_lock() {
            Ok(guard) => (guard.subscriptions(), guard.status()),
            Err(_) => (
                vec![
                    hypeedge_domain::events::EventType::CandleUpdate,
                    hypeedge_domain::events::EventType::OrderFilled,
                    hypeedge_domain::events::EventType::OrderCancelled,
                    hypeedge_domain::events::EventType::OrderRejected,
                    hypeedge_domain::events::EventType::OrderExpired,
                ],
                StrategyStatus::Stopped,
            ),
        };
        Self {
            inner,
            subscriptions,
            status: std::sync::Mutex::new(status),
        }
    }
}

#[async_trait]
impl Strategy for LockedStrategyAdapter {
    async fn on_start(&mut self) -> Result<(), String> {
        let mut strategy = self.inner.lock().await;
        let result = strategy.on_start().await;
        *self.status.lock().unwrap() = strategy.status();
        drop(strategy);
        result
    }
    async fn on_event(&mut self, event: &hypeedge_domain::events::Event) -> Result<(), String> {
        let mut strategy = self.inner.lock().await;
        let result = strategy.on_event(event).await;
        *self.status.lock().unwrap() = strategy.status();
        drop(strategy);
        result
    }
    async fn on_stop(&mut self) -> Result<(), String> {
        let mut strategy = self.inner.lock().await;
        let result = strategy.on_stop().await;
        *self.status.lock().unwrap() = strategy.status();
        drop(strategy);
        result
    }
    fn subscriptions(&self) -> Vec<hypeedge_domain::events::EventType> {
        self.subscriptions.clone()
    }
    fn status(&self) -> StrategyStatus {
        *self.status.lock().unwrap()
    }
    fn set_status(&mut self, status: StrategyStatus) {
        *self.status.lock().unwrap() = status;
    }
}

/// Decode a config snapshot into `TrendParams`.
pub fn decode_trend_follow_config(config: &StrategyConfigSnapshot) -> Result<TrendParams, String> {
    let v = &config.values;
    let get_u = |k: &str| -> Result<usize, String> {
        v.get(k)
            .and_then(|x| x.as_u64())
            .map(|x| x as usize)
            .ok_or_else(|| format!("missing integer field {k}"))
    };
    // A5: the canonical normalized config stores decimal fields as strings
    // (config_normalize.rs), so `as_f64()` alone would reject every real
    // config and silently fall back to defaults. Accept both forms.
    let get_f = |k: &str| -> Result<f64, String> {
        match v.get(k) {
            Some(serde_json::Value::String(s)) => s
                .parse::<f64>()
                .map_err(|_| format!("invalid numeric field {k}")),
            Some(serde_json::Value::Number(n)) => n
                .as_f64()
                .ok_or_else(|| format!("invalid numeric field {k}")),
            Some(_) | None => Err(format!("missing numeric field {k}")),
        }
    };
    let interval = match v.get("interval") {
        Some(serde_json::Value::String(s)) if !s.is_empty() => s.clone(),
        _ => "1m".to_string(),
    };
    let params = TrendParams {
        symbol: "BTC".to_string(),
        interval,
        fast_ema_period: get_u("fast_ema_period")?,
        slow_ema_period: get_u("slow_ema_period")?,
        signal_ema_period: get_u("signal_ema_period")?,
        momentum_period: get_u("momentum_period")?,
        momentum_threshold: get_f("momentum_threshold")?,
        atr_period: get_u("atr_period")?,
        atr_position_multiplier: get_f("atr_position_multiplier")?,
        max_position_pct: get_f("max_position_pct")?,
        risk_per_trade_pct: get_f("risk_per_trade_pct")?,
        atr_stop_multiplier: get_f("atr_stop_multiplier")?,
        macd_cross_threshold: get_f("macd_cross_threshold")?,
    };
    // M-ST3: a config snapshot that decodes into an invalid parameter set
    // (e.g. a zero period from a legacy row) must fail loudly, never trade on
    // out-of-range values.
    params
        .validate()
        .map_err(|e| format!("invalid trend-follow config: {e}"))?;
    Ok(params)
}

/// The default trend-follow config (mirrors `default_trend_follow_config`).
pub fn default_trend_follow_config() -> serde_json::Value {
    let p = TrendParams::default();
    serde_json::json!({
        "fast_ema_period": p.fast_ema_period,
        "slow_ema_period": p.slow_ema_period,
        "signal_ema_period": p.signal_ema_period,
        "momentum_period": p.momentum_period,
        "momentum_threshold": p.momentum_threshold,
        "atr_period": p.atr_period,
        "atr_position_multiplier": p.atr_position_multiplier,
        "atr_stop_multiplier": p.atr_stop_multiplier,
        "max_position_pct": p.max_position_pct,
        "risk_per_trade_pct": p.risk_per_trade_pct,
        "macd_cross_threshold": p.macd_cross_threshold,
    })
}

/// Build the trend-follow plugin (mirrors `build_trend_follow_plugin`).
pub fn build_trend_follow_plugin(
    event_bus: Arc<EventBus>,
    tracker: Option<Arc<dyn StrategyAccountView>>,
    execution: Arc<dyn hypeedge_domain::traits::ExecutionClient>,
) -> super::registry::StrategyTypePlugin {
    super::registry::StrategyTypePlugin {
        strategy_type: "trend_follow".to_string(),
        capabilities: super::registry::trend_follow_capabilities(),
        factory: Arc::new(move |ctx: &StrategyBuildContext| {
            let params = match decode_trend_follow_config(&ctx.config) {
                Ok(p) => p,
                Err(e) => {
                    // A5: never silently trade with defaults on a decode error.
                    // Registration normalizes config, so this is a genuine
                    // invariant violation — log loudly and fail the factory.
                    tracing::error!(
                        strategy_id = %ctx.instance.strategy_id,
                        error = %e,
                        "trend_follow_config_decode_failed"
                    );
                    let mut handle = TrendFollowRuntimeHandle::new(
                        ctx.instance.strategy_id.clone(),
                        TrendFollowStrategy::new(
                            ctx.instance.strategy_id.clone(),
                            TrendParams::default(),
                            execution.clone(),
                            tracker.clone(),
                        ),
                        event_bus.clone(),
                    );
                    handle.config_error = Some(e);
                    return Arc::new(handle);
                }
            };
            let mut params = params;
            params.symbol = if ctx.instance.symbol.is_empty() {
                "BTC".to_string()
            } else {
                ctx.instance.symbol.clone()
            };
            let strategy = TrendFollowStrategy::new(
                ctx.instance.strategy_id.clone(),
                params,
                execution.clone(),
                tracker.clone(),
            );
            let strategy_id = ctx.instance.strategy_id.clone();
            Arc::new(TrendFollowRuntimeHandle::new(
                strategy_id,
                strategy,
                event_bus.clone(),
            ))
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::decimal::{Decimal, Price, Size};
    use hypeedge_domain::error::HypeEdgeError;
    use hypeedge_domain::events::{DomainEvent, Event};
    use hypeedge_domain::models::{Candle, Order, OrderIntent};
    use hypeedge_domain::traits::ExecutionClient;

    /// Execution client that fails every submit (used to crash the runner).
    struct NoopExec;
    #[async_trait]
    impl ExecutionClient for NoopExec {
        async fn submit_order(
            &self,
            _: OrderIntent,
            _: Option<bool>,
        ) -> Result<Order, HypeEdgeError> {
            Err(HypeEdgeError::Execution {
                message: "boom".into(),
            })
        }
        async fn cancel_order(&self, _: &str) -> Result<bool, HypeEdgeError> {
            Ok(true)
        }
        async fn cancel_all_orders(&self, _: Option<&str>) -> Result<u64, HypeEdgeError> {
            Ok(0)
        }
        async fn get_order(&self, _: &str) -> Result<Option<Order>, HypeEdgeError> {
            Ok(None)
        }
        async fn get_open_orders(&self, _: Option<&str>) -> Result<Vec<Order>, HypeEdgeError> {
            Ok(vec![])
        }
    }

    #[test]
    fn decode_config_roundtrip() {
        let config = StrategyConfigSnapshot {
            strategy_id: "tf".into(),
            revision: 1,
            values: default_trend_follow_config(),
        };
        let params = decode_trend_follow_config(&config).unwrap();
        assert_eq!(params.fast_ema_period, 12);
        assert_eq!(params.slow_ema_period, 26);
        assert!(params.validate().is_ok());
    }

    #[test]
    fn decode_rejects_invalid_config() {
        // M-ST3: decoding must run TrendParams::validate() — a zero period
        // must be rejected, not silently traded on.
        let mut values = default_trend_follow_config();
        values["fast_ema_period"] = serde_json::json!(0);
        let config = StrategyConfigSnapshot {
            strategy_id: "tf".into(),
            revision: 1,
            values,
        };
        let err = decode_trend_follow_config(&config).unwrap_err();
        assert!(err.contains("fast_ema_period"), "got: {err}");

        let mut values = default_trend_follow_config();
        values["atr_stop_multiplier"] = serde_json::json!(0);
        let config = StrategyConfigSnapshot {
            strategy_id: "tf".into(),
            revision: 1,
            values,
        };
        assert!(decode_trend_follow_config(&config).is_err());
    }

    #[test]
    fn decode_reads_interval_with_default() {
        let config = StrategyConfigSnapshot {
            strategy_id: "tf".into(),
            revision: 1,
            values: default_trend_follow_config(),
        };
        assert_eq!(decode_trend_follow_config(&config).unwrap().interval, "1m");

        let mut values = default_trend_follow_config();
        values["interval"] = serde_json::json!("5m");
        let config = StrategyConfigSnapshot {
            strategy_id: "tf".into(),
            revision: 1,
            values,
        };
        assert_eq!(decode_trend_follow_config(&config).unwrap().interval, "5m");
    }

    #[tokio::test]
    async fn adapter_sync_methods_never_block_in_async_context() {
        // M-ST6: `subscriptions`/`status`/`set_status` are called from the
        // runner inside an async runtime context — `blocking_lock` on the
        // tokio Mutex would panic there. The adapter must serve them from its
        // cache / status mirror.
        let strategy = TrendFollowStrategy::new(
            "tf_1".into(),
            TrendParams::default(),
            Arc::new(NoopExec),
            None,
        );
        let mut adapter =
            LockedStrategyAdapter::new(Arc::new(tokio::sync::Mutex::new(strategy)));
        let subs = adapter.subscriptions();
        assert!(subs.contains(&hypeedge_domain::events::EventType::CandleUpdate));
        assert_eq!(adapter.status(), StrategyStatus::Stopped);
        adapter.set_status(StrategyStatus::Running);
        assert_eq!(adapter.status(), StrategyStatus::Running);
        // The mirrored status tracks what the strategy itself reports once the
        // lock is taken (async methods refresh the mirror).
        let mut strategy = adapter.inner.lock().await;
        strategy.set_status(StrategyStatus::Paused);
        assert_eq!(strategy.status(), StrategyStatus::Paused);
    }

    #[tokio::test]
    async fn set_mode_shadow_rejected() {
        // H-ST2: trend-follow must reject shadow mode — accepting it would run
        // the strategy for real with no shadow semantics.
        let bus = Arc::new(EventBus::new(16));
        let handle = TrendFollowRuntimeHandle::new(
            "tf_1".into(),
            TrendFollowStrategy::new("tf_1".into(), TrendParams::default(), Arc::new(NoopExec), None),
            bus,
        );
        let err = handle
            .set_mode(MarketMakerLifecycle::Shadow)
            .await
            .unwrap_err();
        assert!(err.contains("shadow"), "got: {err}");
        // Running is still accepted.
        handle.set_mode(MarketMakerLifecycle::Running).await.unwrap();
    }

    #[tokio::test]
    async fn start_restarts_runner_after_task_end() {
        // M-ST2: when the runner task ends without a supervised stop (here the
        // strategy errors on a stop-loss submit), start() must rebuild the task
        // instead of treating the dead JoinHandle as "already running".
        let bus = Arc::new(EventBus::new(16));
        let mut strategy = TrendFollowStrategy::new(
            "tf_1".into(),
            TrendParams::default(),
            Arc::new(NoopExec),
            None,
        );
        strategy.test_force_position_and_stop(1.0, 50000.0);
        let handle = TrendFollowRuntimeHandle::new("tf_1".into(), strategy, bus.clone());

        handle.start().await.unwrap();
        assert!(handle.is_healthy());
        tokio::time::sleep(std::time::Duration::from_millis(50)).await; // let it subscribe

        // A candle crossing the stop makes on_event error → runner ends.
        let px = Price::new(Decimal::from_f64(49000.0).unwrap());
        let candle = Candle {
            symbol: "BTC".into(),
            interval: "1m".into(),
            open: px,
            high: px,
            low: px,
            close: px,
            volume: Size::new(Decimal::ONE),
            timestamp: 1,
        };
        bus.publish(Arc::new(Event::new(DomainEvent::CandleUpdate(candle))))
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(!handle.is_healthy(), "runner task must have ended");

        handle.start().await.unwrap();
        assert!(handle.is_healthy(), "start() must restart a dead runner");
        handle.stop().await.unwrap();
    }
}
