//! Trend-following strategy, port of `src/hypeedge/strategy/trend_follow.py`.
//!
//! MACD crossover + momentum confirmation + ATR-based sizing/stop-loss.
//! Submits orders through the injected `ExecutionClient`; uses an
//! [`AccountView`] for real equity-based sizing.

use std::sync::Arc;

use async_trait::async_trait;
use hypeedge_domain::decimal::{Decimal, Price, Size};
use hypeedge_domain::enums::{OrderType, Side, StrategyStatus};
use hypeedge_domain::events::{DomainEvent, Event, EventType};
use hypeedge_domain::models::{Candle, Order, OrderIntent, Position};
use hypeedge_domain::traits::ExecutionClient;

use super::base::Strategy;
use super::indicators::{atr, macd, momentum};
use super::params::TrendParams;

/// Minimum candle factor: need at least `slow_ema_period * 3` bars.
const MIN_CANDLES_FACTOR: usize = 3;

/// The account data the strategy reads (Rust analog of `AccountTracker`).
pub trait StrategyAccountView: Send + Sync {
    fn get_position(&self, symbol: &str) -> Option<Position>;
    fn current_equity(&self) -> f64;
}

/// The trend-following strategy.
pub struct TrendFollowStrategy {
    strategy_id: String,
    params: TrendParams,
    symbol: String,
    tracker: Option<Arc<dyn StrategyAccountView>>,
    execution: Arc<dyn ExecutionClient>,

    closes: Vec<f64>,
    highs: Vec<f64>,
    lows: Vec<f64>,

    position_size: f64,
    entry_price: Option<f64>,
    stop_price: Option<f64>,

    prev_macd_above_signal: Option<bool>,
    candle_count: usize,
    working_order_cloid: Option<String>,
    working_order_is_close: bool,
    status: StrategyStatus,
}

impl TrendFollowStrategy {
    pub fn new(
        strategy_id: String,
        params: TrendParams,
        execution: Arc<dyn ExecutionClient>,
        tracker: Option<Arc<dyn StrategyAccountView>>,
    ) -> Self {
        let symbol = params.symbol.clone();
        Self {
            strategy_id,
            params,
            symbol,
            tracker,
            execution,
            closes: Vec::new(),
            highs: Vec::new(),
            lows: Vec::new(),
            position_size: 0.0,
            entry_price: None,
            stop_price: None,
            prev_macd_above_signal: None,
            candle_count: 0,
            working_order_cloid: None,
            working_order_is_close: false,
            status: StrategyStatus::Stopped,
        }
    }

    pub fn position_size(&self) -> f64 {
        self.position_size
    }
    pub fn entry_price(&self) -> Option<f64> {
        self.entry_price
    }
    pub fn stop_price(&self) -> Option<f64> {
        self.stop_price
    }

    /// Hot-reload parameters (design doc §15.2).
    pub fn set_params(&mut self, params: TrendParams) {
        self.params = params;
    }

    fn sync_position_from_tracker(&mut self) {
        let Some(tracker) = &self.tracker else { return };
        match tracker.get_position(&self.symbol) {
            None => {
                self.position_size = 0.0;
                self.entry_price = None;
                self.stop_price = None;
            }
            Some(p) => {
                self.position_size = p.size.inner().to_string().parse().unwrap_or(0.0);
                self.entry_price = p
                    .entry_price
                    .map(|px| px.inner().to_string().parse().unwrap_or(0.0));
            }
        }
    }

    fn calculate_position_size(&self, price: f64, atr_val: f64) -> f64 {
        let equity = self
            .tracker
            .as_ref()
            .map(|t| t.current_equity())
            .unwrap_or(10_000.0);
        let p = &self.params;
        if atr_val <= 0.0 || price <= 0.0 || equity <= 0.0 {
            return 0.0;
        }
        let risk_amount = equity * p.risk_per_trade_pct;
        let size = risk_amount / (atr_val * p.atr_position_multiplier);
        let max_size = (equity * p.max_position_pct) / price;
        size.min(max_size)
    }

    async fn open_position(&mut self, side: Side, price: f64, atr_val: f64) -> Result<(), String> {
        let size = self.calculate_position_size(price, atr_val);
        if size <= 0.0 || self.working_order_cloid.is_some() {
            return Ok(());
        }
        let stop_distance = atr_val * self.params.atr_stop_multiplier;
        self.stop_price = Some(match side {
            Side::Buy => price - stop_distance,
            Side::Sell => price + stop_distance,
        });

        let intent = OrderIntent {
            symbol: self.symbol.clone(),
            side,
            size: Size::new(Decimal::from_f64(size).unwrap_or(Decimal::ZERO)),
            price: Some(Price::new(
                Decimal::from_f64(price).unwrap_or(Decimal::ZERO),
            )),
            order_type: OrderType::Limit,
            time_in_force: hypeedge_domain::enums::TimeInForce::Gtc,
            strategy_id: Some(self.strategy_id.clone()),
            sub_account: None,
            reduce_only: false,
            cloid: None,
            client_id: None,
            is_spot: false,
            risk_reducing: false,
            max_slippage_bps: 50,
        };
        let order = self
            .execution
            .submit_order(intent, None)
            .await
            .map_err(|e| e.to_string())?;
        if !order_is_terminal_failure(&order) {
            self.working_order_cloid = Some(order.cloid.clone());
            self.working_order_is_close = false;
            self.sync_position_from_tracker();
        }
        Ok(())
    }

    async fn close_position(&mut self, price: f64) -> Result<(), String> {
        self.sync_position_from_tracker();
        if self.position_size == 0.0 || self.working_order_cloid.is_some() {
            return Ok(());
        }
        let side = if self.position_size > 0.0 {
            Side::Sell
        } else {
            Side::Buy
        };
        let size = self.position_size.abs();
        let intent = OrderIntent {
            symbol: self.symbol.clone(),
            side,
            size: Size::new(Decimal::from_f64(size).unwrap_or(Decimal::ZERO)),
            price: None,
            order_type: OrderType::Market,
            time_in_force: hypeedge_domain::enums::TimeInForce::Gtc,
            strategy_id: Some(self.strategy_id.clone()),
            sub_account: None,
            reduce_only: true,
            cloid: None,
            client_id: None,
            is_spot: false,
            risk_reducing: false,
            max_slippage_bps: 50,
        };
        let order = self
            .execution
            .submit_order(intent, None)
            .await
            .map_err(|e| e.to_string())?;
        if !order_is_terminal_failure(&order) {
            self.working_order_cloid = Some(order.cloid.clone());
            self.working_order_is_close = true;
        }
        self.sync_position_from_tracker();
        let _ = price;
        Ok(())
    }

    async fn process_candle(&mut self, candle: &Candle) -> Result<(), String> {
        let p = &self.params;
        let (macd_line, signal_line, _hist) = macd(
            &self.closes,
            p.fast_ema_period,
            p.slow_ema_period,
            p.signal_ema_period,
        );
        let atr_values = atr(&self.highs, &self.lows, &self.closes, p.atr_period);
        let mom_values = momentum(&self.closes, p.momentum_period);

        let macd_val = macd_line.last().copied().unwrap_or(f64::NAN);
        let signal_val = signal_line.last().copied().unwrap_or(f64::NAN);
        let atr_val = atr_values.last().copied().unwrap_or(f64::NAN);
        let mom_val = mom_values.last().copied().unwrap_or(f64::NAN);

        if [macd_val, signal_val, atr_val, mom_val]
            .iter()
            .any(|v| v.is_nan())
        {
            return Ok(());
        }

        let macd_above = macd_val > signal_val;
        let prev_above = self.prev_macd_above_signal;
        self.prev_macd_above_signal = Some(macd_above);

        let current_price = candle
            .close
            .inner()
            .to_string()
            .parse::<f64>()
            .unwrap_or(0.0);

        // Stop-loss first.
        if let Some(stop) = self.stop_price {
            if self.position_size > 0.0 && current_price <= stop {
                return self.close_position(current_price).await;
            }
            if self.position_size < 0.0 && current_price >= stop {
                return self.close_position(current_price).await;
            }
        }

        if let Some(prev) = prev_above {
            let bullish_cross = macd_above && !prev;
            let bearish_cross = !macd_above && prev;
            if bullish_cross && mom_val > p.momentum_threshold {
                if self.position_size < 0.0 {
                    self.close_position(current_price).await?;
                }
                if self.position_size == 0.0 {
                    return self.open_position(Side::Buy, current_price, atr_val).await;
                }
            } else if bearish_cross && mom_val < -p.momentum_threshold {
                if self.position_size > 0.0 {
                    self.close_position(current_price).await?;
                }
                if self.position_size == 0.0 {
                    return self.open_position(Side::Sell, current_price, atr_val).await;
                }
            }
        }
        Ok(())
    }
}

fn order_is_terminal_failure(order: &Order) -> bool {
    matches!(
        order.status,
        hypeedge_domain::enums::OrderStatus::Rejected
            | hypeedge_domain::enums::OrderStatus::Cancelled
            | hypeedge_domain::enums::OrderStatus::Expired
    )
}

#[async_trait]
impl Strategy for TrendFollowStrategy {
    async fn on_start(&mut self) -> Result<(), String> {
        self.status = StrategyStatus::Running;
        self.sync_position_from_tracker();
        Ok(())
    }

    async fn on_event(&mut self, event: &Event) -> Result<(), String> {
        // Order lifecycle events for this strategy: clear the working order.
        let is_order_event = matches!(
            event.event_type(),
            EventType::OrderFilled
                | EventType::OrderCancelled
                | EventType::OrderRejected
                | EventType::OrderExpired
        );
        if is_order_event {
            if let DomainEvent::OrderFilled(o)
            | DomainEvent::OrderCancelled(o)
            | DomainEvent::OrderRejected(o)
            | DomainEvent::OrderExpired(o) = &event.payload
                && o.strategy_id.as_deref() == Some(&self.strategy_id)
                && self.working_order_cloid.as_deref() == Some(o.cloid.as_str())
            {
                self.sync_position_from_tracker();
                self.working_order_cloid = None;
                self.working_order_is_close = false;
            }
            return Ok(());
        }

        let DomainEvent::CandleUpdate(candle) = &event.payload else {
            return Ok(());
        };
        if candle.symbol != self.params.symbol {
            return Ok(());
        }
        self.candle_count += 1;
        self.sync_position_from_tracker();
        self.closes
            .push(candle.close.inner().to_string().parse().unwrap_or(0.0));
        self.highs
            .push(candle.high.inner().to_string().parse().unwrap_or(0.0));
        self.lows
            .push(candle.low.inner().to_string().parse().unwrap_or(0.0));

        let min_candles = self.params.slow_ema_period * MIN_CANDLES_FACTOR;
        if self.candle_count < min_candles {
            return Ok(());
        }
        if self.status == StrategyStatus::Paused {
            return Ok(());
        }
        self.process_candle(candle).await
    }

    async fn on_stop(&mut self) -> Result<(), String> {
        self.sync_position_from_tracker();
        if self.position_size != 0.0 && self.working_order_cloid.is_none() {
            let last_close = self.closes.last().copied().unwrap_or(0.0);
            let _ = self.close_position(last_close).await;
        }
        self.status = StrategyStatus::Stopped;
        Ok(())
    }

    fn subscriptions(&self) -> Vec<EventType> {
        vec![
            EventType::CandleUpdate,
            EventType::OrderFilled,
            EventType::OrderCancelled,
            EventType::OrderRejected,
            EventType::OrderExpired,
        ]
    }

    fn status(&self) -> StrategyStatus {
        self.status
    }

    fn set_status(&mut self, status: StrategyStatus) {
        self.status = status;
    }
}
