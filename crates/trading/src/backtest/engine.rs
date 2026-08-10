//! Backtest engine — orchestrates strategy, broker, and data feed, port of
//! `src/hypeedge/backtest/engine.py`.

use std::collections::HashMap;
use std::sync::Arc;

use hypeedge_domain::decimal::{Decimal, Price, Size, Usd};
use hypeedge_domain::enums::{OrderStatus, Side};
use hypeedge_domain::events::{DomainEvent, Event};
use hypeedge_domain::models::{Candle, Fill, FundingRate, Order, OrderIntent, Position};
use hypeedge_domain::traits::ExecutionClient;
use hypeedge_infra::event_bus::EventBus;

use super::broker::SimulatedBroker;
use super::metrics::MetricsCalculator;

/// Complete result of a single backtest run.
#[derive(Debug, Clone)]
pub struct BacktestResult {
    pub metrics: super::metrics::PerformanceMetrics,
    pub fills: Vec<Fill>,
    pub equity_curve: Vec<(i64, Usd)>,
}

/// The simulated execution client (implements `ExecutionClient`).
pub struct SimulatedExecutionClient {
    broker: Arc<std::sync::Mutex<SimulatedBroker>>,
    bus: Arc<EventBus>,
    open_orders: tokio::sync::Mutex<HashMap<String, Order>>,
    cloid_counter: std::sync::atomic::AtomicU64,
}

impl SimulatedExecutionClient {
    pub fn new(broker: Arc<std::sync::Mutex<SimulatedBroker>>, bus: Arc<EventBus>) -> Self {
        Self {
            broker,
            bus,
            open_orders: tokio::sync::Mutex::new(HashMap::new()),
            cloid_counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Attempt to fill open orders against the given candle. Filled orders are
    /// removed and appended to `fills_collector`.
    pub async fn try_fill_orders(&self, candle: &Candle, fills_collector: &mut Vec<Fill>) {
        let mut open = self.open_orders.lock().await;
        let filled_cloids: Vec<String> = {
            let mut broker = self.broker.lock().unwrap();
            let mut filled = Vec::new();
            for (cloid, order) in open.iter_mut() {
                let intent = OrderIntent {
                    symbol: order.symbol.clone(),
                    side: order.side,
                    size: order.remaining_size(),
                    price: order.price,
                    order_type: order.order_type,
                    time_in_force: order.time_in_force,
                    strategy_id: order.strategy_id.clone(),
                    sub_account: None,
                    reduce_only: order.reduce_only,
                    cloid: Some(cloid.clone()),
                    client_id: None,
                    is_spot: false,
                    risk_reducing: false,
                    max_slippage_bps: 50,
                };
                if let Some(fill) = broker.simulate_fill(&intent, candle, cloid) {
                    order.status = OrderStatus::Filled;
                    order.filled_size = order.size;
                    order.avg_fill_price = Some(fill.price);
                    order.filled_at = Some(chrono::Utc::now());
                    filled.push(cloid.clone());
                    fills_collector.push(fill.clone());
                    let _ = self.bus.publish_sync(Arc::new(
                        Event::new(DomainEvent::OrderFilled(order.clone()))
                            .with_correlation_id(cloid.clone()),
                    ));
                }
            }
            filled
        };
        for cloid in filled_cloids {
            open.remove(&cloid);
        }
    }

    fn generate_cloid(&self) -> String {
        let n = self
            .cloid_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        format!("bt_cloid_{n}")
    }
}

#[async_trait::async_trait]
impl ExecutionClient for SimulatedExecutionClient {
    async fn submit_order(
        &self,
        intent: OrderIntent,
        _deferred: Option<bool>,
    ) -> Result<Order, hypeedge_domain::error::HypeEdgeError> {
        let cloid = intent
            .cloid
            .clone()
            .unwrap_or_else(|| self.generate_cloid());
        let order = Order {
            cloid: cloid.clone(),
            symbol: intent.symbol.clone(),
            side: intent.side,
            size: intent.size,
            price: intent.price,
            order_type: intent.order_type,
            time_in_force: intent.time_in_force,
            status: OrderStatus::Submitted,
            strategy_id: intent.strategy_id.clone(),
            sub_account: None,
            reduce_only: intent.reduce_only,
            is_spot: false,
            risk_reducing: false,
            max_slippage_bps: 50,
            exchange_oid: None,
            filled_size: Size::ZERO,
            avg_fill_price: None,
            submitted_at: None,
            acknowledged_at: None,
            filled_at: None,
            error_message: None,
            created_at: chrono::Utc::now(),
        };
        self.open_orders
            .lock()
            .await
            .insert(cloid.clone(), order.clone());
        let _ = self.bus.publish_sync(Arc::new(
            Event::new(DomainEvent::OrderSubmitted(order.clone())).with_correlation_id(cloid),
        ));
        Ok(order)
    }

    async fn cancel_order(
        &self,
        cloid: &str,
    ) -> Result<bool, hypeedge_domain::error::HypeEdgeError> {
        let mut open = self.open_orders.lock().await;
        if let Some(mut order) = open.remove(cloid) {
            order.status = OrderStatus::Cancelled;
            let _ = self.bus.publish_sync(Arc::new(
                Event::new(DomainEvent::OrderCancelled(order.clone()))
                    .with_correlation_id(cloid.to_string()),
            ));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn cancel_all_orders(
        &self,
        symbol: Option<&str>,
    ) -> Result<u64, hypeedge_domain::error::HypeEdgeError> {
        let mut open = self.open_orders.lock().await;
        let keys: Vec<String> = open
            .iter()
            .filter(|(_, o)| symbol.is_none() || o.symbol == symbol.unwrap())
            .map(|(k, _)| k.clone())
            .collect();
        let count = keys.len() as u64;
        for k in keys {
            open.remove(&k);
        }
        Ok(count)
    }

    async fn get_order(
        &self,
        cloid: &str,
    ) -> Result<Option<Order>, hypeedge_domain::error::HypeEdgeError> {
        Ok(self.open_orders.lock().await.get(cloid).cloned())
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> Result<Vec<Order>, hypeedge_domain::error::HypeEdgeError> {
        let open = self.open_orders.lock().await;
        Ok(open
            .values()
            .filter(|o| symbol.is_none() || o.symbol == symbol.unwrap())
            .cloned()
            .collect())
    }
}

/// The main backtest orchestrator. Fee/slippage live on the caller-built
/// [`SimulatedBroker`]; the engine only replays candles, fills orders from the
/// injected [`SimulatedExecutionClient`], and computes metrics (A24).
pub struct BacktestEngine;

impl Default for BacktestEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl BacktestEngine {
    pub fn new() -> Self {
        BacktestEngine
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        &self,
        candles: Vec<Candle>,
        funding_rates: Option<Vec<FundingRate>>,
        execution: Arc<SimulatedExecutionClient>,
        initial_capital: Usd,
    ) -> BacktestResult {
        let mut equity = initial_capital;
        let mut peak_equity = initial_capital;
        let mut positions: HashMap<String, Position> = HashMap::new();
        let mut all_fills: Vec<Fill> = Vec::new();
        let mut equity_curve: Vec<(i64, Usd)> = Vec::new();
        let mut total_funding = Usd::ZERO;
        let mut applied_funding: std::collections::HashSet<(String, i64)> = Default::default();
        let mut realized_trade_pnls: Vec<Usd> = Vec::new();
        let mut total_fees = Usd::ZERO;

        // A24: fill orders from the caller-supplied simulated client — the same
        // one the strategy submits through. The pre-fix code built a private
        // client nothing ever submitted to, so run() always returned zero fills.
        for candle in &candles {
            // 1. Fill orders submitted on earlier candles.
            let prev_fill_count = all_fills.len();
            execution.try_fill_orders(candle, &mut all_fills).await;

            // 2. Apply fills to cash and positions.
            for fill in &all_fills[prev_fill_count..] {
                equity = Usd::new(equity.inner() - fill.fee.inner());
                total_fees = Usd::new(total_fees.inner() + fill.fee.inner().abs());
                let realized = Self::update_position(&mut positions, fill);
                equity = Usd::new(equity.inner() + realized.inner());
                if !realized.is_zero() {
                    realized_trade_pnls.push(realized);
                }
            }

            // 3. Mark positions to the current close.
            if let Some(position) = positions.get_mut(&candle.symbol) {
                position.mark_price = Some(candle.close);
            }

            // 4. Apply each funding record at most once.
            if let Some(rates) = &funding_rates {
                let funding_amount = Self::apply_funding_once(
                    &positions,
                    rates,
                    candle.timestamp,
                    &mut applied_funding,
                );
                if !funding_amount.is_zero() {
                    equity = Usd::new(equity.inner() - funding_amount.inner());
                    total_funding = Usd::new(total_funding.inner() + funding_amount.inner());
                }
            }

            let marked_equity = Usd::new(equity.inner() + Self::unrealized_pnl(&positions).inner());
            if marked_equity.inner() > peak_equity.inner() {
                peak_equity = marked_equity;
            }
            equity_curve.push((candle.timestamp, marked_equity));
        }

        // Calculate metrics.
        let calculator = MetricsCalculator::new(
            equity_curve.clone(),
            initial_capital,
            total_funding,
            total_fees,
            realized_trade_pnls,
        );
        let metrics = calculator.calculate();

        BacktestResult {
            metrics,
            fills: all_fills,
            equity_curve,
        }
    }

    fn update_position(positions: &mut HashMap<String, Position>, fill: &Fill) -> Usd {
        let key = fill.symbol.clone();
        let signed_fill = if fill.side == Side::Buy {
            fill.size.inner()
        } else {
            -fill.size.inner()
        };
        match positions.get_mut(&key) {
            None => {
                positions.insert(
                    key,
                    Position {
                        symbol: fill.symbol.clone(),
                        size: Size::new(signed_fill),
                        entry_price: Some(fill.price),
                        mark_price: Some(fill.price),
                        unrealized_pnl: None,
                        leverage: 1,
                        liquidation_price: None,
                        sub_account: None,
                        strategy_id: None,
                    },
                );
                Usd::ZERO
            }
            Some(pos) => {
                let old_size = pos.size.inner();
                let new_size = old_size + signed_fill;
                let entry = pos
                    .entry_price
                    .map(|p| p.inner())
                    .unwrap_or(fill.price.inner());

                // Same direction: increase using VWAP.
                if (old_size.is_positive() && signed_fill.is_positive())
                    || (old_size.is_negative() && signed_fill.is_negative())
                {
                    let new_entry = (old_size.abs() * entry
                        + signed_fill.abs() * fill.price.inner())
                        / new_size.abs();
                    pos.size = Size::new(new_size);
                    pos.entry_price = Some(Price::new(new_entry));
                    pos.mark_price = Some(fill.price);
                    return Usd::ZERO;
                }

                // Opposite direction: close all or part.
                let closing_size = old_size.abs().min(signed_fill.abs());
                let direction = if old_size > Decimal::ZERO {
                    Decimal::ONE
                } else {
                    -Decimal::ONE
                };
                let realized = (fill.price.inner() - entry) * closing_size * direction;

                if new_size.abs() < Decimal::from_str_lenient("0.000000000001").unwrap() {
                    positions.remove(&key);
                } else if old_size.is_positive() == new_size.is_positive() {
                    // Partial reduction: entry unchanged.
                    pos.size = Size::new(new_size);
                    pos.mark_price = Some(fill.price);
                } else {
                    // Flip: residual starts at the flip fill.
                    pos.size = Size::new(new_size);
                    pos.entry_price = Some(fill.price);
                    pos.mark_price = Some(fill.price);
                }
                Usd::new(realized)
            }
        }
    }

    fn apply_funding_once(
        positions: &HashMap<String, Position>,
        funding_rates: &[FundingRate],
        current_ts: i64,
        applied: &mut std::collections::HashSet<(String, i64)>,
    ) -> Usd {
        let mut total = Usd::ZERO;
        for position in positions.values() {
            if position.is_flat() || position.mark_price.is_none() {
                continue;
            }
            for rate in funding_rates {
                let key = (rate.symbol.clone(), rate.timestamp);
                if !applied.contains(&key)
                    && rate.timestamp <= current_ts
                    && rate.symbol == position.symbol
                {
                    let funding = SimulatedBroker::apply_hourly_funding(
                        position,
                        rate.funding_rate,
                        position.mark_price.unwrap(),
                    );
                    total = Usd::new(total.inner() + funding.inner());
                    applied.insert(key);
                }
            }
        }
        total
    }

    fn unrealized_pnl(positions: &HashMap<String, Position>) -> Usd {
        let mut total = Decimal::ZERO;
        for position in positions.values() {
            let (Some(entry), Some(mark)) = (position.entry_price, position.mark_price) else {
                continue;
            };
            total += position.size.inner() * (mark.inner() - entry.inner());
        }
        Usd::new(total)
    }
}
