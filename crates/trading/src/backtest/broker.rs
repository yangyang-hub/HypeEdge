//! Simulated broker for backtesting — fee, slippage, and fill modeling, port of
//! `src/hypeedge/backtest/broker.py`.

use hypeedge_domain::decimal::{Decimal, Price, Size, Usd};
use hypeedge_domain::enums::{OrderType, Side};
use hypeedge_domain::models::{Candle, Fill, OrderIntent, Position};

/// Fill price slippage assumption mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlippageMode {
    Optimistic,
    Pessimistic,
}

impl SlippageMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            SlippageMode::Optimistic => "optimistic",
            SlippageMode::Pessimistic => "pessimistic",
        }
    }
}

/// Fee structure: maker rebate (negative = you get paid), taker fee (positive).
#[derive(Debug, Clone, Copy)]
pub struct FeeConfig {
    pub maker_rebate_pct: f64,
    pub taker_fee_pct: f64,
}

impl Default for FeeConfig {
    fn default() -> Self {
        Self {
            maker_rebate_pct: -0.0002,
            taker_fee_pct: 0.0005,
        }
    }
}

/// Slippage assumptions in basis points.
#[derive(Debug, Clone, Copy)]
pub struct SlippageConfig {
    pub optimistic_bps: f64,
    pub pessimistic_bps: f64,
}

impl Default for SlippageConfig {
    fn default() -> Self {
        Self {
            optimistic_bps: 2.0,
            pessimistic_bps: 10.0,
        }
    }
}

/// Simulates order fills against candle data.
pub struct SimulatedBroker {
    fee: FeeConfig,
    slippage: SlippageConfig,
    mode: SlippageMode,
    next_oid: u64,
}

impl SimulatedBroker {
    pub fn new(fee: FeeConfig, slippage: SlippageConfig, mode: SlippageMode) -> Self {
        Self {
            fee,
            slippage,
            mode,
            next_oid: 0,
        }
    }

    pub fn mode(&self) -> SlippageMode {
        self.mode
    }

    /// Simulate a fill. Returns `Some(Fill)` if executed, `None` otherwise.
    pub fn simulate_fill(
        &mut self,
        intent: &OrderIntent,
        candle: &Candle,
        cloid: &str,
    ) -> Option<Fill> {
        match intent.order_type {
            OrderType::Market => Some(self.fill_market(intent, candle, cloid)),
            OrderType::Limit => self.fill_limit(intent, candle, cloid),
            _ => None,
        }
    }

    /// Apply slippage to a fill price: buy pays more, sell receives less.
    pub fn apply_slippage(&self, price: Price, side: Side) -> Price {
        let bps = self.slippage_bps();
        let factor = bps / 10_000.0;
        let factor = Decimal::from_f64(factor).unwrap_or(Decimal::ZERO);
        let one = Decimal::ONE;
        match side {
            Side::Buy => Price::new(price.inner() * (one + factor)),
            Side::Sell => Price::new(price.inner() * (one - factor)),
        }
    }

    /// Calculate the fee for a fill (positive = taker pays, negative = maker rebate).
    pub fn calculate_fee(&self, price: Price, size: Size, is_maker: bool) -> Usd {
        let notional = price.inner() * size.inner();
        let rate = if is_maker {
            self.fee.maker_rebate_pct
        } else {
            self.fee.taker_fee_pct
        };
        Usd::new(notional * Decimal::from_f64(rate).unwrap_or(Decimal::ZERO))
    }

    /// Hourly funding: `position_size * mark_price * funding_rate`.
    pub fn apply_hourly_funding(position: &Position, funding_rate: f64, mark_price: Price) -> Usd {
        if position.is_flat() {
            return Usd::ZERO;
        }
        Usd::new(
            position.size.inner()
                * mark_price.inner()
                * Decimal::from_f64(funding_rate).unwrap_or(Decimal::ZERO),
        )
    }

    fn fill_market(&mut self, intent: &OrderIntent, candle: &Candle, cloid: &str) -> Fill {
        let base_price = candle.close;
        let fill_price = self.apply_slippage(base_price, intent.side);
        let fee = self.calculate_fee(fill_price, intent.size, false);
        let oid = self.next_order_id();
        Fill {
            cloid: cloid.to_string(),
            exchange_oid: oid,
            symbol: intent.symbol.clone(),
            side: intent.side,
            price: fill_price,
            size: intent.size,
            fee,
            is_maker: false,
            timestamp: candle.timestamp,
            strategy_id: intent.strategy_id.clone(),
            sub_account: None,
            is_spot: false,
        }
    }

    fn fill_limit(&mut self, intent: &OrderIntent, candle: &Candle, cloid: &str) -> Option<Fill> {
        let limit_price = intent.price?;
        // B16: a limit that crossed the spread (the market traded through it) is
        // a taker, not a maker. For a buy, a limit at/above the candle high
        // would have hit the ask; for a sell, at/below the low hits the bid.
        let is_maker = match intent.side {
            Side::Buy => limit_price.inner() < candle.high.inner(),
            Side::Sell => limit_price.inner() > candle.low.inner(),
        };
        match intent.side {
            Side::Buy if candle.low.inner() > limit_price.inner() => return None,
            Side::Sell if candle.high.inner() < limit_price.inner() => return None,
            _ => {}
        }
        let fee = self.calculate_fee(limit_price, intent.size, is_maker);
        let oid = self.next_order_id();
        Some(Fill {
            cloid: cloid.to_string(),
            exchange_oid: oid,
            symbol: intent.symbol.clone(),
            side: intent.side,
            price: limit_price,
            size: intent.size,
            fee,
            is_maker,
            timestamp: candle.timestamp,
            strategy_id: intent.strategy_id.clone(),
            sub_account: None,
            is_spot: false,
        })
    }

    fn next_order_id(&mut self) -> String {
        self.next_oid += 1;
        format!("bt_{}", self.next_oid)
    }

    fn slippage_bps(&self) -> f64 {
        match self.mode {
            SlippageMode::Optimistic => self.slippage.optimistic_bps,
            SlippageMode::Pessimistic => self.slippage.pessimistic_bps,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candle(close: &str, low: &str, high: &str) -> Candle {
        Candle {
            symbol: "BTC".into(),
            interval: "1h".into(),
            open: Default::default(),
            high: Price::new(Decimal::from_str_lenient(high).unwrap()),
            low: Price::new(Decimal::from_str_lenient(low).unwrap()),
            close: Price::new(Decimal::from_str_lenient(close).unwrap()),
            volume: Size::new(Decimal::ONE),
            timestamp: 1_000,
        }
    }

    fn intent(side: Side, price: Option<&str>) -> OrderIntent {
        OrderIntent {
            symbol: "BTC".into(),
            side,
            size: Size::new(Decimal::from_str_lenient("1").unwrap()),
            price: price.map(|p| Price::new(Decimal::from_str_lenient(p).unwrap())),
            order_type: if price.is_some() {
                OrderType::Limit
            } else {
                OrderType::Market
            },
            time_in_force: hypeedge_domain::enums::TimeInForce::Gtc,
            strategy_id: None,
            sub_account: None,
            reduce_only: false,
            cloid: None,
            client_id: None,
            is_spot: false,
            risk_reducing: false,
            max_slippage_bps: 50,
        }
    }

    #[test]
    fn market_fill_applies_pessimistic_slippage() {
        let mut broker = SimulatedBroker::new(
            FeeConfig::default(),
            SlippageConfig::default(),
            SlippageMode::Pessimistic,
        );
        let c = candle("100", "99", "101");
        let fill = broker
            .simulate_fill(&intent(Side::Buy, None), &c, "c1")
            .unwrap();
        // Buy pessimistic: 100 * (1 + 10/10000) = 100.1.
        assert_eq!(fill.price.to_string(), "100.1");
        assert!(!fill.is_maker);
        // Taker fee: 100.1 * 0.0005 = 0.05005.
        assert!(fill.fee.inner() > Decimal::ZERO);
    }

    #[test]
    fn market_sell_slippage_reduces_price() {
        let mut broker = SimulatedBroker::new(
            FeeConfig::default(),
            SlippageConfig::default(),
            SlippageMode::Pessimistic,
        );
        let c = candle("100", "99", "101");
        let fill = broker
            .simulate_fill(&intent(Side::Sell, None), &c, "c1")
            .unwrap();
        assert_eq!(fill.price.to_string(), "99.9");
    }

    #[test]
    fn limit_buy_fills_when_low_crosses() {
        let mut broker = SimulatedBroker::new(
            FeeConfig::default(),
            SlippageConfig::default(),
            SlippageMode::Pessimistic,
        );
        let c = candle("101", "99.5", "102");
        let fill = broker
            .simulate_fill(&intent(Side::Buy, Some("100")), &c, "c1")
            .unwrap();
        assert!(fill.is_maker);
        assert_eq!(fill.price.to_string(), "100");
        // Maker rebate: 100 * -0.0002 = -0.02 (negative = paid to maker).
        assert!(fill.fee.inner() < Decimal::ZERO);
    }

    #[test]
    fn limit_buy_no_fill_when_low_above_limit() {
        let mut broker = SimulatedBroker::new(
            FeeConfig::default(),
            SlippageConfig::default(),
            SlippageMode::Pessimistic,
        );
        let c = candle("102", "101.5", "103");
        let fill = broker.simulate_fill(&intent(Side::Buy, Some("100")), &c, "c1");
        assert!(fill.is_none());
    }

    #[test]
    fn hourly_funding_matches_python() {
        let pos = Position {
            symbol: "BTC".into(),
            size: Size::new(Decimal::from_str_lenient("2").unwrap()),
            entry_price: None,
            mark_price: Some(Price::new(Decimal::from_str_lenient("100").unwrap())),
            unrealized_pnl: None,
            leverage: 1,
            liquidation_price: None,
            sub_account: None,
            strategy_id: None,
        };
        // 2 * 100 * 0.0001 = 0.02.
        let funding = SimulatedBroker::apply_hourly_funding(&pos, 0.0001, pos.mark_price.unwrap());
        assert_eq!(funding.to_string(), "0.02");
        // Flat position → 0.
        let flat = Position {
            size: Size::ZERO,
            ..pos
        };
        assert_eq!(
            SimulatedBroker::apply_hourly_funding(&flat, 0.0001, pos.mark_price.unwrap()),
            Usd::ZERO
        );
    }
}
