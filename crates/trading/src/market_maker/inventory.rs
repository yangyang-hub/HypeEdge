//! Inventory bands and reservation-price skew, port of
//! `src/hypeedge/strategy/market_maker/inventory.py`.

use hypeedge_domain::decimal::{Decimal, Price, Usd};

use super::models::{InventorySnapshot, MarketFeatures, MarketMakerConfig};

/// The inventory decision output.
#[derive(Debug, Clone, PartialEq)]
pub struct InventoryDecision {
    pub inventory_notional: Usd,
    pub normalized_inventory: Decimal,
    pub shift_bps: Decimal,
    pub reservation_price: Price,
    pub allow_bid: bool,
    pub allow_ask: bool,
    pub emergency: bool,
}

/// Move reservation price and disable inventory-increasing sides at limits.
pub struct InventoryController;

impl InventoryController {
    pub fn calculate(
        &self,
        fair_price: Price,
        inventory: &InventorySnapshot,
        features: &MarketFeatures,
        config: &MarketMakerConfig,
    ) -> Result<InventoryDecision, String> {
        if !inventory.healthy
            || inventory.equity.inner() <= Decimal::ZERO
            || fair_price.inner() <= Decimal::ZERO
        {
            return Err("inventory state, equity, and fair price must be healthy".into());
        }

        let notional = inventory.position_size.inner() * fair_price.inner();
        let mut z = notional.div(config.soft_inventory_notional.inner());
        z = z.clamp(
            Decimal::from_str_lenient("-2").unwrap(),
            Decimal::from_str_lenient("2").unwrap(),
        );
        let mut shift = config.inventory_skew_bps * z
            + config.inventory_gamma_bps
                * z
                * features.return_variance_per_second
                * config.horizon_seconds;
        shift = shift.clamp(
            -config.max_inventory_shift_bps,
            config.max_inventory_shift_bps,
        );
        let reservation = fair_price.inner()
            * (Decimal::ONE - shift.div(Decimal::from_str_lenient("10000").unwrap()));

        let absolute = notional.abs();
        let long_inventory = notional > Decimal::ZERO;
        let short_inventory = notional < Decimal::ZERO;
        let at_soft = absolute >= config.soft_inventory_notional.inner();
        let at_hard = absolute >= config.hard_inventory_notional.inner();
        let emergency = absolute >= config.emergency_inventory_notional.inner();

        let mut allow_bid = !(at_soft && long_inventory);
        let mut allow_ask = !(at_soft && short_inventory);
        if at_hard {
            allow_bid = short_inventory;
            allow_ask = long_inventory;
        }

        Ok(InventoryDecision {
            inventory_notional: Usd::new(notional),
            normalized_inventory: z,
            shift_bps: shift,
            reservation_price: Price::new(reservation),
            allow_bid: allow_bid && !emergency,
            allow_ask: allow_ask && !emergency,
            emergency,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use hypeedge_domain::decimal::Size;

    fn snap(position: &str, equity: &str) -> InventorySnapshot {
        InventorySnapshot {
            position_size: Size::new(Decimal::from_str_lenient(position).unwrap()),
            equity: Usd::new(Decimal::from_str_lenient(equity).unwrap()),
            available_balance: Usd::ZERO,
            margin_used: Usd::ZERO,
            observed_at: Utc::now(),
            healthy: true,
        }
    }

    fn features(variance: &str) -> MarketFeatures {
        MarketFeatures {
            symbol: "BTC".into(),
            market_version: 1,
            connection_generation: 0,
            exchange_ts: 0,
            received_at: Utc::now(),
            healthy: true,
            best_bid: Price::new(Decimal::from_str_lenient("99.5").unwrap()),
            best_ask: Price::new(Decimal::from_str_lenient("100.5").unwrap()),
            best_bid_size: Size::new(Decimal::ONE),
            best_ask_size: Size::new(Decimal::ONE),
            microprice: Price::new(Decimal::from_str_lenient("100").unwrap()),
            normalized_ofi: Decimal::ZERO,
            trade_flow: Decimal::ZERO,
            short_return: Decimal::ZERO,
            return_variance_per_second: Decimal::from_str_lenient(variance).unwrap(),
            expected_adverse_markout_bps: Decimal::ZERO,
            latency_buffer_bps: Decimal::ZERO,
            toxicity: Decimal::ZERO,
            funding_rate: Decimal::ZERO,
            external_source: None,
            external_symbol: None,
            external_raw_price: None,
            external_adjusted_price: None,
            external_basis_bps: Decimal::ZERO,
            external_effective_weight: Decimal::ZERO,
            external_confidence: Decimal::ZERO,
            external_age_ms: None,
            external_quality: "unavailable".into(),
            external_observed_at: None,
            latency_quality: "configured".into(),
            markout_quality: "configured".into(),
        }
    }

    fn config() -> MarketMakerConfig {
        let mut c = MarketMakerConfig::default_with(
            1,
            Decimal::from_str_lenient("0.1").unwrap(),
            Decimal::from_str_lenient("0.001").unwrap(),
            Decimal::from_str_lenient("0.001").unwrap(),
        );
        c.soft_inventory_notional = Usd::new(Decimal::from_str_lenient("1000").unwrap());
        c.hard_inventory_notional = Usd::new(Decimal::from_str_lenient("2000").unwrap());
        c.emergency_inventory_notional = Usd::new(Decimal::from_str_lenient("3000").unwrap());
        c.quote_size = Size::new(Decimal::ONE);
        c
    }

    #[test]
    fn flat_inventory_no_skew() {
        let d = InventoryController
            .calculate(
                Price::new(Decimal::from_str_lenient("100").unwrap()),
                &snap("0", "10000"),
                &features("0"),
                &config(),
            )
            .unwrap();
        assert_eq!(d.reservation_price.to_string(), "100");
        assert!(d.allow_bid && d.allow_ask);
        assert!(!d.emergency);
    }

    #[test]
    fn long_inventory_skews_down_and_blocks_bid() {
        // position 20 @ 100 = notional 2000 = 2x soft (1000) → z=2 clamped.
        let d = InventoryController
            .calculate(
                Price::new(Decimal::from_str_lenient("100").unwrap()),
                &snap("20", "10000"),
                &features("0"),
                &config(),
            )
            .unwrap();
        assert!(d.reservation_price.inner() < Decimal::from_str_lenient("100").unwrap());
        // shift = 5*2 + 1*2*0*5 = 10 bps → reservation = 100*(1-0.001) = 99.9.
        assert_eq!(d.reservation_price.to_string(), "99.9");
        // Long at soft → bid blocked (would add inventory), ask allowed.
        assert!(!d.allow_bid);
        assert!(d.allow_ask);
        assert!(!d.emergency);
    }

    #[test]
    fn at_hard_blocks_inventory_increasing_side() {
        // position 25 @ 100 = 2500 >= hard (2000), < emergency (3000).
        let d = InventoryController
            .calculate(
                Price::new(Decimal::from_str_lenient("100").unwrap()),
                &snap("25", "10000"),
                &features("0"),
                &config(),
            )
            .unwrap();
        // Long at hard → allow_bid = short_inventory = false; allow_ask = long = true.
        assert!(!d.allow_bid);
        assert!(d.allow_ask);
        assert!(!d.emergency);
    }

    #[test]
    fn emergency_disables_both() {
        let d = InventoryController
            .calculate(
                Price::new(Decimal::from_str_lenient("100").unwrap()),
                &snap("35", "10000"), // 3500 >= emergency 3000
                &features("0"),
                &config(),
            )
            .unwrap();
        assert!(d.emergency);
        assert!(!d.allow_bid);
        assert!(!d.allow_ask);
    }
}
