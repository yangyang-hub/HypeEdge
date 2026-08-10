//! Deterministic event-time replay for research-only market-maker evaluation,
//! port of `src/hypeedge/backtest/market_maker_replay.py`.
//!
//! A scenario model with latency and queue-multiplier assumptions. It reuses
//! the [`AccountingLedger`] for exact PnL accounting. Equal timestamps retain
//! caller order, making runs reproducible.

use std::collections::HashMap;

use hypeedge_domain::decimal::{Decimal, Price, Size, Usd};
use hypeedge_domain::enums::Side;

use super::market_maker_metrics::{
    AccountingFill, AccountingLedger, AccountingPnL, ExecutionQuality,
};

/// The replay scenario determines latency and queue assumptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReplayScenario {
    Optimistic,
    Neutral,
    Pessimistic,
}

impl ReplayScenario {
    pub fn as_str(self) -> &'static str {
        match self {
            ReplayScenario::Optimistic => "optimistic",
            ReplayScenario::Neutral => "neutral",
            ReplayScenario::Pessimistic => "pessimistic",
        }
    }
}

/// Latency and queue-multiplier assumptions for a scenario.
#[derive(Debug, Clone, PartialEq)]
pub struct ScenarioAssumption {
    pub latency_ms: u64,
    pub queue_multiplier: Decimal,
}

pub const DEFAULT_ASSUMPTIONS: &[(ReplayScenario, ScenarioAssumption)] = &[
    (
        ReplayScenario::Optimistic,
        ScenarioAssumption {
            latency_ms: 0,
            queue_multiplier: Decimal::ZERO,
        },
    ),
    (
        ReplayScenario::Neutral,
        ScenarioAssumption {
            latency_ms: 25,
            queue_multiplier: Decimal::ONE,
        },
    ),
    (
        ReplayScenario::Pessimistic,
        ScenarioAssumption {
            latency_ms: 100,
            queue_multiplier: Decimal::from_scaled(2, 0),
        },
    ),
];

pub fn default_assumption(scenario: ReplayScenario) -> ScenarioAssumption {
    DEFAULT_ASSUMPTIONS
        .iter()
        .find(|(s, _)| *s == scenario)
        .map(|(_, a)| a.clone())
        .expect("known scenario")
}

/// A quote placed by the strategy.
#[derive(Debug, Clone, PartialEq)]
pub struct QuoteEvent {
    pub event_time_ms: u64,
    pub order_id: String,
    pub side: Side,
    pub price: Price,
    pub size: Size,
    pub queue_ahead: Size,
}

/// A cancellation (only effective once the order is active).
#[derive(Debug, Clone, PartialEq)]
pub struct CancelEvent {
    pub event_time_ms: u64,
    pub order_id: String,
}

/// An aggressive trade that can consume resting orders.
#[derive(Debug, Clone, PartialEq)]
pub struct TradeEvent {
    pub event_time_ms: u64,
    pub aggressor_side: Side,
    pub price: Price,
    pub size: Size,
}

/// A funding settlement (signed income; a payment is negative).
#[derive(Debug, Clone, PartialEq)]
pub struct FundingEvent {
    pub event_time_ms: u64,
    pub amount: Usd,
}

/// A paid action cost (e.g. reserve weight purchase).
#[derive(Debug, Clone, PartialEq)]
pub struct PaidActionEvent {
    pub event_time_ms: u64,
    pub cost: Usd,
}

/// One replay event (union of the scenario events).
#[derive(Debug, Clone, PartialEq)]
pub enum ReplayEvent {
    Quote(QuoteEvent),
    Cancel(CancelEvent),
    Trade(TradeEvent),
    Funding(FundingEvent),
    PaidAction(PaidActionEvent),
}

impl ReplayEvent {
    pub fn event_time_ms(&self) -> u64 {
        match self {
            ReplayEvent::Quote(e) => e.event_time_ms,
            ReplayEvent::Cancel(e) => e.event_time_ms,
            ReplayEvent::Trade(e) => e.event_time_ms,
            ReplayEvent::Funding(e) => e.event_time_ms,
            ReplayEvent::PaidAction(e) => e.event_time_ms,
        }
    }
}

/// A resting order being tracked through the replay.
#[derive(Debug, Clone, PartialEq)]
pub struct ShadowReplayOrder {
    pub order_id: String,
    pub side: Side,
    pub price: Price,
    pub remaining: Decimal,
    pub queue_ahead: Decimal,
    pub active_at_ms: u64,
    pub filled: Decimal,
    pub cancelled: bool,
}

/// One matched fill.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplayFill {
    pub fill_id: String,
    pub order_id: String,
    pub event_time_ms: u64,
    pub side: Side,
    pub price: Price,
    pub size: Size,
}

/// The full replay result.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketMakerReplayResult {
    pub scenario: ReplayScenario,
    pub fills: Vec<ReplayFill>,
    pub accounting_pnl: AccountingPnL,
    pub execution_quality: ExecutionQuality,
    pub shadow_orders: Vec<ShadowReplayOrder>,
}

/// Stable replay: equal timestamps retain caller order, making runs reproducible.
pub struct MarketMakerReplay {
    maker_rebate_rate: Decimal,
}

impl Default for MarketMakerReplay {
    fn default() -> Self {
        Self::new(Decimal::ZERO)
    }
}

impl MarketMakerReplay {
    pub fn new(maker_rebate_rate: Decimal) -> Self {
        Self { maker_rebate_rate }
    }

    #[allow(clippy::assign_op_pattern)] // Decimal has no SubAssign/MulAssign
    pub fn run(
        &self,
        events: &[ReplayEvent],
        scenario: ReplayScenario,
        ending_mark_price: Price,
        assumption: Option<&ScenarioAssumption>,
    ) -> Result<MarketMakerReplayResult, String> {
        let model = assumption.cloned().unwrap_or_else(|| default_assumption(scenario));
        let mut ledger = AccountingLedger::new();
        let mut orders: HashMap<String, ShadowReplayOrder> = HashMap::new();
        let mut fills: Vec<ReplayFill> = Vec::new();
        let mut queue_consumed = Decimal::ZERO;
        let mut partial_fills = 0u64;

        // Stable sort by (event_time_ms, original index) — equal timestamps
        // retain caller order.
        let mut indexed: Vec<(usize, &ReplayEvent)> = events.iter().enumerate().collect();
        indexed.sort_by_key(|(idx, event)| (event.event_time_ms(), *idx));

        for (_, event) in indexed {
            match event {
                ReplayEvent::Quote(e) => {
                    if let Some(existing) = orders.get(&e.order_id)
                        && !existing.cancelled
                    {
                        return Err(format!("duplicate live shadow order: {}", e.order_id));
                    }
                    orders.insert(
                        e.order_id.clone(),
                        ShadowReplayOrder {
                            order_id: e.order_id.clone(),
                            side: e.side,
                            price: e.price,
                            remaining: e.size.inner(),
                            queue_ahead: e.queue_ahead.inner() * model.queue_multiplier,
                            active_at_ms: e.event_time_ms + model.latency_ms,
                            filled: Decimal::ZERO,
                            cancelled: false,
                        },
                    );
                }
                ReplayEvent::Cancel(e) => {
                    if let Some(order) = orders.get_mut(&e.order_id)
                        && e.event_time_ms >= order.active_at_ms
                    {
                        order.cancelled = true;
                    }
                }
                ReplayEvent::Funding(e) => {
                    ledger.record_funding(e.amount);
                }
                ReplayEvent::PaidAction(e) => {
                    ledger.record_paid_action(e.cost)?;
                }
                ReplayEvent::Trade(e) => {
                    let mut available = e.size.inner();
                    let resting_side = if e.aggressor_side == Side::Buy {
                        Side::Sell
                    } else {
                        Side::Buy
                    };
                    let mut eligible: Vec<&mut ShadowReplayOrder> = orders
                        .values_mut()
                        .filter(|order| {
                            !order.cancelled
                                && order.side == resting_side
                                && e.event_time_ms >= order.active_at_ms
                                && ((order.side == Side::Buy && order.price.inner() >= e.price.inner())
                                    || (order.side == Side::Sell && order.price.inner() <= e.price.inner()))
                        })
                        .collect();
                    eligible.sort_by(|a, b| {
                        a.active_at_ms
                            .cmp(&b.active_at_ms)
                            .then_with(|| a.order_id.cmp(&b.order_id))
                    });
                    for order in eligible {
                        if available <= Decimal::ZERO {
                            break;
                        }
                        let queued = order.queue_ahead.min(available);
                        order.queue_ahead = order.queue_ahead - queued;
                        available = available - queued;
                        queue_consumed = queue_consumed + queued;
                        let fill_size = order.remaining.min(available);
                        if fill_size <= Decimal::ZERO {
                            continue;
                        }
                        order.remaining = order.remaining - fill_size;
                        order.filled = order.filled + fill_size;
                        available = available - fill_size;
                        let fill = ReplayFill {
                            fill_id: format!("{}:{}", order.order_id, fills.len() + 1),
                            order_id: order.order_id.clone(),
                            event_time_ms: e.event_time_ms,
                            side: order.side,
                            price: order.price,
                            size: Size::new(fill_size),
                        };
                        fills.push(fill);
                        let rebate = order.price.inner() * fill_size * self.maker_rebate_rate;
                        ledger.record_fill(AccountingFill {
                            side: order.side,
                            price: order.price,
                            size: Size::new(fill_size),
                            net_fee_rebate: Usd::new(rebate),
                        })?;
                        if order.remaining > Decimal::ZERO {
                            partial_fills += 1;
                        }
                    }
                }
            }
        }

        let accounting = ledger.close(ending_mark_price)?;
        let quality = ExecutionQuality {
            queue_ahead_consumed: Size::new(queue_consumed),
            fills: fills.len() as u64,
            partial_fills,
            ..ExecutionQuality::default()
        };
        let mut shadow_keys: Vec<&String> = orders.keys().collect();
        shadow_keys.sort();
        let shadow_orders: Vec<ShadowReplayOrder> = shadow_keys
            .iter()
            .filter_map(|k| orders.get(*k).cloned())
            .collect();

        Ok(MarketMakerReplayResult {
            scenario,
            fills,
            accounting_pnl: accounting,
            execution_quality: quality,
            shadow_orders,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::decimal::Decimal;

    fn px(s: &str) -> Price {
        Price::new(Decimal::from_str_strict(s).unwrap())
    }
    fn sz(s: &str) -> Size {
        Size::new(Decimal::from_str_strict(s).unwrap())
    }
    fn usd(s: &str) -> Usd {
        Usd::new(Decimal::from_str_strict(s).unwrap())
    }

    fn quote(id: &str, side: Side, price: &str, size: &str, ts: u64) -> ReplayEvent {
        ReplayEvent::Quote(QuoteEvent {
            event_time_ms: ts,
            order_id: id.into(),
            side,
            price: px(price),
            size: sz(size),
            queue_ahead: sz("0"),
        })
    }

    fn trade(side: Side, price: &str, size: &str, ts: u64) -> ReplayEvent {
        ReplayEvent::Trade(TradeEvent {
            event_time_ms: ts,
            aggressor_side: side,
            price: px(price),
            size: sz(size),
        })
    }

    #[test]
    fn optimistic_buy_quote_consumed_by_sell_trade() {
        let replay = MarketMakerReplay::new(Decimal::ZERO);
        let events = vec![
            quote("q1", Side::Buy, "100", "1", 0),
            trade(Side::Sell, "100", "1", 10),
        ];
        let result = replay
            .run(&events, ReplayScenario::Optimistic, px("100"), None)
            .unwrap();
        assert_eq!(result.fills.len(), 1);
        assert_eq!(result.fills[0].order_id, "q1");
        assert_eq!(result.fills[0].size, sz("1"));
        assert_eq!(result.accounting_pnl.net(), usd("0"));
        assert_eq!(result.execution_quality.fills, 1);
    }

    #[test]
    fn pessimistic_latency_skips_immediate_trade() {
        let replay = MarketMakerReplay::new(Decimal::ZERO);
        let events = vec![
            quote("q1", Side::Buy, "100", "1", 0),
            // Trade arrives before the quote is active (latency 100ms).
            trade(Side::Sell, "100", "1", 50),
        ];
        let result = replay
            .run(&events, ReplayScenario::Pessimistic, px("100"), None)
            .unwrap();
        assert!(result.fills.is_empty());
        assert_eq!(result.execution_quality.fills, 0);
    }

    #[test]
    fn neutral_queue_ahead_consumes_volume() {
        let replay = MarketMakerReplay::new(Decimal::ZERO);
        let events = vec![
            ReplayEvent::Quote(QuoteEvent {
                event_time_ms: 0,
                order_id: "q1".into(),
                side: Side::Buy,
                price: px("100"),
                size: sz("1"),
                queue_ahead: sz("0.5"),
            }),
            // Neutral latency = 25ms, so the quote is active at 25. Trade at
            // 30 arrives after activation.
            trade(Side::Sell, "100", "1", 30),
        ];
        // Neutral queue multiplier = 1: 0.5 queue ahead consumed first.
        let result = replay
            .run(&events, ReplayScenario::Neutral, px("100"), None)
            .unwrap();
        assert_eq!(result.fills.len(), 1);
        assert_eq!(result.fills[0].size, sz("0.5"));
        assert_eq!(result.execution_quality.queue_ahead_consumed, sz("0.5"));
    }

    #[test]
    fn funding_and_paid_action_flow() {
        let replay = MarketMakerReplay::new(Decimal::ZERO);
        let events = vec![
            quote("q1", Side::Buy, "100", "1", 0),
            trade(Side::Sell, "100", "1", 10),
            ReplayEvent::Funding(FundingEvent {
                event_time_ms: 20,
                amount: usd("0.5"),
            }),
            ReplayEvent::PaidAction(PaidActionEvent {
                event_time_ms: 30,
                cost: usd("0.1"),
            }),
        ];
        let result = replay
            .run(&events, ReplayScenario::Optimistic, px("100"), None)
            .unwrap();
        assert_eq!(result.accounting_pnl.funding, usd("0.5"));
        assert_eq!(result.accounting_pnl.paid_action, usd("0.1"));
        result.accounting_pnl.assert_ledger_identity(usd("0.4")).unwrap();
    }

    #[test]
    fn duplicate_live_order_is_rejected() {
        let replay = MarketMakerReplay::new(Decimal::ZERO);
        let events = vec![
            quote("q1", Side::Buy, "100", "1", 0),
            quote("q1", Side::Buy, "100", "1", 5),
        ];
        assert!(replay
            .run(&events, ReplayScenario::Optimistic, px("100"), None)
            .is_err());
    }

    #[test]
    fn equal_timestamps_retain_caller_order() {
        let replay = MarketMakerReplay::new(Decimal::ZERO);
        let events = vec![
            quote("q2", Side::Buy, "101", "1", 0),
            quote("q1", Side::Buy, "100", "1", 0),
            trade(Side::Sell, "100", "2", 10),
        ];
        // Both quotes at ts=0; stable order is q2 then q1. Sell @100 matches
        // both (buy price >= 100), q1 (price 100) and q2 (price 101). Fills
        // order by active_at then order_id.
        let result = replay
            .run(&events, ReplayScenario::Optimistic, px("100"), None)
            .unwrap();
        assert_eq!(result.fills.len(), 2);
    }

    #[test]
    fn maker_rebate_credits_ledger() {
        let replay = MarketMakerReplay::new(Decimal::from_str_lenient("0.0001").unwrap());
        let events = vec![
            quote("q1", Side::Buy, "100", "1", 0),
            trade(Side::Sell, "100", "1", 10),
        ];
        let result = replay
            .run(&events, ReplayScenario::Optimistic, px("100"), None)
            .unwrap();
        // Rebate = 100 * 1 * 0.0001 = 0.01.
        assert_eq!(result.accounting_pnl.net_fee_rebate, usd("0.01"));
    }

    #[test]
    fn cancel_before_active_is_ineffective() {
        let replay = MarketMakerReplay::new(Decimal::ZERO);
        let events = vec![
            quote("q1", Side::Buy, "100", "1", 0),
            ReplayEvent::Cancel(CancelEvent {
                event_time_ms: 5,
                order_id: "q1".into(),
            }),
            // Pessimistic latency 100ms: quote active at 100, cancel at 5 is
            // before active => ineffective.
            trade(Side::Sell, "100", "1", 150),
        ];
        let result = replay
            .run(&events, ReplayScenario::Pessimistic, px("100"), None)
            .unwrap();
        assert_eq!(result.fills.len(), 1);
    }
}
