//! Bounded explainable fair-value model, port of
//! `src/hypeedge/strategy/market_maker/fair_value.py`.

use hypeedge_domain::decimal::{Decimal, Price};

use super::models::{MarketFeatures, MarketMakerConfig};

/// Combine microprice and short-horizon flow with a hard tick cap.
pub struct FairValueModel;

impl FairValueModel {
    pub fn calculate(&self, features: &MarketFeatures, config: &MarketMakerConfig) -> Price {
        let mid = features.mid_price().inner();
        let tick = config.tick_size;
        let local_raw_shift = config.beta_microprice * (features.microprice.inner() - mid)
            + config.beta_ofi_ticks * features.normalized_ofi * tick
            + config.beta_trade_flow_ticks * features.trade_flow * tick
            + config.beta_short_return_ticks * features.short_return * mid;
        let local_cap = config.max_fair_shift_ticks * tick;
        let local_shift = local_raw_shift.clamp(-local_cap, local_cap);

        let mut external_shift = Decimal::ZERO;
        if let Some(ext) = features.external_adjusted_price
            && features.external_effective_weight > Decimal::ZERO
        {
            let external_raw_shift = (ext.inner() - mid) * features.external_effective_weight;
            let external_cap = config.max_external_shift_ticks * tick;
            external_shift = external_raw_shift.clamp(-external_cap, external_cap);
        }

        let total_cap = config.max_total_fair_shift_ticks * tick;
        let total_shift = (local_shift + external_shift).clamp(-total_cap, total_cap);
        Price::new(mid + total_shift)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_maker::models::MarketFeatures;
    use hypeedge_domain::decimal::Size;

    fn features(
        microprice: &str,
        ofi: &str,
        trade_flow: &str,
        short_return: &str,
        mid: &str,
    ) -> MarketFeatures {
        let f = MarketFeatures {
            symbol: "BTC".into(),
            market_version: 1,
            connection_generation: 0,
            exchange_ts: 0,
            received_at: chrono::Utc::now(),
            healthy: true,
            best_bid: Price::new(
                Decimal::from_str_lenient(mid).unwrap() - Decimal::from_str_lenient("0.5").unwrap(),
            ),
            best_ask: Price::new(
                Decimal::from_str_lenient(mid).unwrap() + Decimal::from_str_lenient("0.5").unwrap(),
            ),
            best_bid_size: Size::new(Decimal::ONE),
            best_ask_size: Size::new(Decimal::ONE),
            microprice: Price::new(Decimal::from_str_lenient(microprice).unwrap()),
            normalized_ofi: Decimal::from_str_lenient(ofi).unwrap(),
            trade_flow: Decimal::from_str_lenient(trade_flow).unwrap(),
            short_return: Decimal::from_str_lenient(short_return).unwrap(),
            return_variance_per_second: Decimal::ZERO,
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
        };
        f.validate().unwrap();
        f
    }

    #[test]
    fn flat_market_fair_is_mid() {
        let mut config = crate::market_maker::models::MarketMakerConfig::default_with(
            1,
            Decimal::from_str_lenient("0.01").unwrap(),
            Decimal::from_str_lenient("0.001").unwrap(),
            Decimal::from_str_lenient("0.001").unwrap(),
        );
        config.soft_inventory_notional =
            hypeedge_domain::Usd::new(Decimal::from_str_lenient("1000").unwrap());
        config.hard_inventory_notional =
            hypeedge_domain::Usd::new(Decimal::from_str_lenient("2000").unwrap());
        config.emergency_inventory_notional =
            hypeedge_domain::Usd::new(Decimal::from_str_lenient("3000").unwrap());
        config.quote_size = Size::new(Decimal::ONE);
        let f = features("100", "0", "0", "0", "100");
        let fair = FairValueModel.calculate(&f, &config);
        assert_eq!(fair.to_string(), "100");
    }

    #[test]
    fn microprice_and_flow_shift_fair() {
        let mut config = crate::market_maker::models::MarketMakerConfig::default_with(
            1,
            Decimal::from_str_lenient("0.01").unwrap(),
            Decimal::from_str_lenient("0.001").unwrap(),
            Decimal::from_str_lenient("0.001").unwrap(),
        );
        config.soft_inventory_notional =
            hypeedge_domain::Usd::new(Decimal::from_str_lenient("1000").unwrap());
        config.hard_inventory_notional =
            hypeedge_domain::Usd::new(Decimal::from_str_lenient("2000").unwrap());
        config.emergency_inventory_notional =
            hypeedge_domain::Usd::new(Decimal::from_str_lenient("3000").unwrap());
        config.quote_size = Size::new(Decimal::ONE);
        // microprice 100.5 > mid 100 → +0.5*0.5 = +0.25; capped at 2 ticks=0.02.
        let f = features("100.5", "0", "0", "0", "100");
        let fair = FairValueModel.calculate(&f, &config);
        assert_eq!(fair.to_string(), "100.02"); // capped by max_fair_shift_ticks(2) * 0.01
    }

    #[test]
    fn total_shift_capped() {
        let mut config = crate::market_maker::models::MarketMakerConfig::default_with(
            1,
            Decimal::from_str_lenient("0.01").unwrap(),
            Decimal::from_str_lenient("0.001").unwrap(),
            Decimal::from_str_lenient("0.001").unwrap(),
        );
        config.soft_inventory_notional =
            hypeedge_domain::Usd::new(Decimal::from_str_lenient("1000").unwrap());
        config.hard_inventory_notional =
            hypeedge_domain::Usd::new(Decimal::from_str_lenient("2000").unwrap());
        config.emergency_inventory_notional =
            hypeedge_domain::Usd::new(Decimal::from_str_lenient("3000").unwrap());
        config.quote_size = Size::new(Decimal::ONE);
        // Local shift saturates at +2 ticks, external at +2 ticks → total would
        // be 4 ticks, but max_total_fair_shift_ticks = 3 → capped at 3 ticks.
        let mut f = features("100.5", "2", "2", "0.001", "100");
        f.external_adjusted_price = Some(Price::new(Decimal::from_str_lenient("101").unwrap()));
        f.external_effective_weight = Decimal::ONE;
        let fair = FairValueModel.calculate(&f, &config);
        assert_eq!(fair.to_string(), "100.03");
    }
}
