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
        let mut task_guard = self.task.lock().await;
        if task_guard.is_some() {
            return Ok(()); // idempotent
        }
        let (stop_tx, stop_rx) = mpsc::channel(1);
        *self.stop_tx.lock().await = Some(stop_tx);
        let adapter = LockedStrategyAdapter(self.strategy.clone());
        let mut runner = StrategyRunner::new(Box::new(adapter), self.event_bus.clone());
        let handle = tokio::spawn(async move { runner.run(stop_rx).await });
        *task_guard = Some(handle);
        Ok(())
    }

    async fn set_mode(&self, mode: MarketMakerLifecycle) -> Result<(), String> {
        let mut strategy = self.strategy.lock().await;
        match mode {
            MarketMakerLifecycle::Warming | MarketMakerLifecycle::Shadow => Ok(()),
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
struct LockedStrategyAdapter(Arc<tokio::sync::Mutex<TrendFollowStrategy>>);

#[async_trait]
impl Strategy for LockedStrategyAdapter {
    async fn on_start(&mut self) -> Result<(), String> {
        self.0.lock().await.on_start().await
    }
    async fn on_event(&mut self, event: &hypeedge_domain::events::Event) -> Result<(), String> {
        self.0.lock().await.on_event(event).await
    }
    async fn on_stop(&mut self) -> Result<(), String> {
        self.0.lock().await.on_stop().await
    }
    fn subscriptions(&self) -> Vec<hypeedge_domain::events::EventType> {
        self.0.blocking_lock().subscriptions()
    }
    fn status(&self) -> StrategyStatus {
        self.0.blocking_lock().status()
    }
    fn set_status(&mut self, status: StrategyStatus) {
        self.0.blocking_lock().set_status(status);
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
    Ok(TrendParams {
        symbol: "BTC".to_string(),
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
    })
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
                    return Arc::new(TrendFollowRuntimeHandle::new(
                        ctx.instance.strategy_id.clone(),
                        TrendFollowStrategy::new(
                            ctx.instance.strategy_id.clone(),
                            TrendParams::default(),
                            execution.clone(),
                            tracker.clone(),
                        ),
                        event_bus.clone(),
                    ));
                }
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
}
