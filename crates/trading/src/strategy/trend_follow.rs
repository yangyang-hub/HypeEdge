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
    /// A reversal queued after a close order is submitted: the new position in
    /// the crossed direction opens once the close fill is reflected (A6).
    pending_reverse: Option<Side>,
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
            pending_reverse: None,
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

    /// Stop-loss check: closes the position when the market crosses the stop.
    /// Runs even while paused (A7) — a safety pause is exactly when an open
    /// position must still be stopped out. Returns `true` when it closed.
    async fn check_stop_loss(&mut self, current_price: f64) -> Result<bool, String> {
        if let Some(stop) = self.stop_price {
            if self.position_size > 0.0 && current_price <= stop {
                self.close_position(current_price).await?;
                return Ok(true);
            }
            if self.position_size < 0.0 && current_price >= stop {
                self.close_position(current_price).await?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn process_candle(&mut self, candle: &Candle) -> Result<(), String> {
        let (macd_line, signal_line, _hist) = macd(
            &self.closes,
            self.params.fast_ema_period,
            self.params.slow_ema_period,
            self.params.signal_ema_period,
        );
        let atr_values = atr(
            &self.highs,
            &self.lows,
            &self.closes,
            self.params.atr_period,
        );
        let mom_values = momentum(&self.closes, self.params.momentum_period);

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
        if self.check_stop_loss(current_price).await? {
            return Ok(());
        }

        // A pending reversal (the close order from a flip filled) opens once the
        // position is actually flat.
        if self.position_size == 0.0
            && let Some(side) = self.pending_reverse.take()
        {
            return self.open_position(side, current_price, atr_val).await;
        }

        if let Some(prev) = prev_above {
            let bullish_cross = macd_above && !prev;
            let bearish_cross = !macd_above && prev;
            if bullish_cross && mom_val > self.params.momentum_threshold {
                if self.position_size < 0.0 {
                    // A6: queue the reversal; the close order's working cloid
                    // blocks an immediate open, so it opens once the fill is
                    // reflected and this branch re-runs on a later candle.
                    self.close_position(current_price).await?;
                    self.pending_reverse = Some(Side::Buy);
                    return Ok(());
                }
                if self.position_size == 0.0 {
                    return self.open_position(Side::Buy, current_price, atr_val).await;
                }
            } else if bearish_cross && mom_val < -self.params.momentum_threshold {
                if self.position_size > 0.0 {
                    self.close_position(current_price).await?;
                    self.pending_reverse = Some(Side::Sell);
                    return Ok(());
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
                let was_close = self.working_order_is_close;
                self.working_order_cloid = None;
                self.working_order_is_close = false;
                // A6: only a *filled* close leaves the queued reversal active
                // (it opens on the next candle once the position is flat); a
                // rejected/cancelled/expired close means the flip did not happen.
                if was_close && !matches!(event.event_type(), EventType::OrderFilled) {
                    self.pending_reverse = None;
                }
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
        let current_price = candle
            .close
            .inner()
            .to_string()
            .parse::<f64>()
            .unwrap_or(0.0);

        // A7: the stop-loss always evaluates — even while paused and before the
        // indicator warmup completes — because a paused strategy holding an open
        // position must still be stopped out.
        if self.check_stop_loss(current_price).await? {
            return Ok(());
        }

        let min_candles = self.params.slow_ema_period * MIN_CANDLES_FACTOR;
        if self.candle_count < min_candles {
            return Ok(());
        }
        if self.status == StrategyStatus::Paused {
            return Ok(()); // paused: no new signals or entries
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

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::enums::OrderStatus;
    use hypeedge_domain::error::HypeEdgeError;

    fn candle(close: &str) -> Candle {
        let px = Price::new(Decimal::from_str_strict(close).unwrap());
        Candle {
            symbol: "BTC".into(),
            interval: "1m".into(),
            open: px,
            high: px,
            low: px,
            close: px,
            volume: Size::new(Decimal::ONE),
            timestamp: 1_700_000_000_000,
        }
    }

    fn order_for(cloid: &str, side: Side) -> Order {
        let mut order = Order::new(
            cloid.into(),
            "BTC".into(),
            side,
            Size::new(Decimal::ONE),
            None,
            OrderType::Market,
            hypeedge_domain::enums::TimeInForce::Ioc,
        );
        order.strategy_id = Some("tf_1".into());
        order
    }

    fn wrap(payload: DomainEvent) -> Event {
        Event::new(payload)
    }

    /// An execution client that records every intent; market orders fill
    /// immediately, limit orders ack.
    struct MockExecution {
        submitted: std::sync::Mutex<Vec<OrderIntent>>,
    }
    impl MockExecution {
        fn new() -> Self {
            Self {
                submitted: std::sync::Mutex::new(Vec::new()),
            }
        }
    }
    #[async_trait]
    impl ExecutionClient for MockExecution {
        async fn submit_order(
            &self,
            intent: OrderIntent,
            _deferred: Option<bool>,
        ) -> Result<Order, HypeEdgeError> {
            self.submitted.lock().unwrap().push(intent.clone());
            let mut order = Order::new(
                intent.cloid.clone().unwrap_or_default(),
                intent.symbol.clone(),
                intent.side,
                intent.size,
                intent.price,
                intent.order_type,
                intent.time_in_force,
            );
            order.strategy_id = intent.strategy_id.clone();
            if intent.order_type == OrderType::Market {
                order.status = OrderStatus::Filled;
                order.filled_size = intent.size;
            } else {
                order.status = OrderStatus::Acknowledged;
            }
            Ok(order)
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

    #[tokio::test]
    async fn paused_strategy_still_stops_out() {
        // A7 regression: a paused strategy holding an open position must still
        // run its stop-loss.
        let exec = Arc::new(MockExecution::new());
        let mut strategy =
            TrendFollowStrategy::new("tf_1".into(), TrendParams::default(), exec.clone(), None);
        strategy.status = StrategyStatus::Paused;
        strategy.position_size = 1.0;
        strategy.stop_price = Some(50000.0);

        strategy
            .on_event(&wrap(DomainEvent::CandleUpdate(candle("49000"))))
            .await
            .unwrap();

        let submitted = exec.submitted.lock().unwrap();
        assert!(
            !submitted.is_empty(),
            "stop-loss must fire while paused (A7)"
        );
        assert_eq!(
            submitted[0].side,
            Side::Sell,
            "long stop closes with a sell"
        );
    }

    #[tokio::test]
    async fn filled_close_keeps_queued_reversal_rejected_clears() {
        // A6 regression: a flip queues a reversal after the close order; only a
        // *filled* close keeps it active, a rejected/cancelled close cancels it.
        let exec = Arc::new(MockExecution::new());
        let mut strategy =
            TrendFollowStrategy::new("tf_1".into(), TrendParams::default(), exec.clone(), None);

        strategy.working_order_cloid = Some("close_c1".into());
        strategy.working_order_is_close = true;
        strategy.pending_reverse = Some(Side::Buy);
        strategy
            .on_event(&wrap(DomainEvent::OrderFilled(order_for(
                "close_c1",
                Side::Sell,
            ))))
            .await
            .unwrap();
        assert_eq!(
            strategy.pending_reverse,
            Some(Side::Buy),
            "filled close keeps the queued reversal (A6)"
        );
        assert_eq!(strategy.working_order_cloid, None);

        strategy.working_order_cloid = Some("close_c2".into());
        strategy.working_order_is_close = true;
        strategy.pending_reverse = Some(Side::Sell);
        strategy
            .on_event(&wrap(DomainEvent::OrderRejected(order_for(
                "close_c2",
                Side::Buy,
            ))))
            .await
            .unwrap();
        assert_eq!(
            strategy.pending_reverse, None,
            "a rejected close cancels the queued reversal (A6)"
        );
    }

    #[tokio::test]
    async fn flat_position_with_pending_reverse_opens_reversal() {
        // A6 regression: once the close fill is reflected (position flat), the
        // queued reversal position actually opens.
        let exec = Arc::new(MockExecution::new());
        let mut strategy =
            TrendFollowStrategy::new("tf_1".into(), TrendParams::default(), exec.clone(), None);
        // Warm up indicators with a mildly-varying series so ATR > 0.
        strategy.candle_count = 80;
        for i in 0..80 {
            let px = 100.0 + (i % 7) as f64 * 0.5;
            strategy.closes.push(px);
            strategy.highs.push(px + 0.5);
            strategy.lows.push(px - 0.5);
        }
        strategy.pending_reverse = Some(Side::Buy);
        strategy.status = StrategyStatus::Running;

        strategy.process_candle(&candle("100")).await.unwrap();

        let submitted = exec.submitted.lock().unwrap();
        assert!(!submitted.is_empty(), "pending reversal must open (A6)");
        assert_eq!(submitted[0].side, Side::Buy);
    }
}
