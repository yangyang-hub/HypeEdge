//! Pure market-making policy producing candidate desired quotes, port of
//! `src/hypeedge/strategy/market_maker/policy.py`.

use hypeedge_domain::decimal::{Decimal, Price, Size, Usd};
use hypeedge_domain::enums::{ActionBudgetMode, QuoteDecision, Side};

use super::fair_value::FairValueModel;
use super::inventory::InventoryController;
use super::models::{ActionBudgetSnapshot, InventorySnapshot, MarketFeatures, MarketMakerConfig};
use crate::trading::quotes::{DesiredQuote, DesiredQuoteSet, QuoteSlotKey};

/// Generate explainable one-level ALO quote candidates without I/O.
pub struct MarketMakerPolicy {
    fair_value: FairValueModel,
    inventory: InventoryController,
}

impl Default for MarketMakerPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl MarketMakerPolicy {
    pub fn new() -> Self {
        Self {
            fair_value: FairValueModel,
            inventory: InventoryController,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn quote(
        &self,
        strategy_id: &str,
        session_id: &str,
        revision: i64,
        current_slot_revision: i64,
        features: &MarketFeatures,
        inventory: &InventorySnapshot,
        budget: &ActionBudgetSnapshot,
        config: &MarketMakerConfig,
    ) -> Result<DesiredQuoteSet, String> {
        let fair = self.fair_value.calculate(features, config);
        let inventory_decision = self
            .inventory
            .calculate(fair, inventory, features, config)?;
        let now = features.received_at;

        let no_quote_reason: Option<String> = if !features.healthy {
            Some("market_unhealthy".into())
        } else if !budget.healthy {
            Some("action_budget_stale".into())
        } else if matches!(
            budget.mode,
            ActionBudgetMode::CancelOnly | ActionBudgetMode::Exhausted
        ) {
            Some(format!("budget_{}", budget.mode.as_str()))
        } else if inventory_decision.emergency {
            Some("inventory_emergency".into())
        } else {
            None
        };

        let half_spread_bps = config.min_half_spread_bps.max(
            features.expected_adverse_markout_bps
                + features.latency_buffer_bps
                + config.toxicity_spread_bps * features.toxicity,
        );
        let half_spread = inventory_decision.reservation_price.inner() * half_spread_bps
            / Decimal::from_str_lenient("10000").unwrap();
        let raw_bid = inventory_decision.reservation_price.inner() - half_spread;
        let raw_ask = inventory_decision.reservation_price.inner() + half_spread;
        let mut bid_price = Price::new(raw_bid.floor_to_step(config.tick_size));
        let mut ask_price = Price::new(raw_ask.ceil_to_step(config.tick_size));

        // ALO candidates must stay strictly outside the opposite best price.
        bid_price = Price::new(
            bid_price
                .inner()
                .min(features.best_ask.inner() - config.tick_size),
        );
        ask_price = Price::new(
            ask_price
                .inner()
                .max(features.best_bid.inner() + config.tick_size),
        );

        let quote_size = Self::quote_size(
            fair,
            inventory_decision.inventory_notional.inner(),
            features,
            config,
        );
        let mut allow_bid = inventory_decision.allow_bid;
        let mut allow_ask = inventory_decision.allow_ask;
        if budget.mode == ActionBudgetMode::Critical {
            allow_bid = allow_bid && inventory_decision.inventory_notional.inner() < Decimal::ZERO;
            allow_ask = allow_ask && inventory_decision.inventory_notional.inner() > Decimal::ZERO;
        }

        let bid_edge = Self::gross_edge(Side::Buy, fair, bid_price, quote_size, features, config);
        let ask_edge = Self::gross_edge(Side::Sell, fair, ask_price, quote_size, features, config);
        let bid = Self::desired(
            strategy_id,
            features,
            Side::Buy,
            bid_price,
            quote_size,
            bid_edge,
            allow_bid && no_quote_reason.is_none(),
            no_quote_reason.clone(),
            config,
        );
        let ask = Self::desired(
            strategy_id,
            features,
            Side::Sell,
            ask_price,
            quote_size,
            ask_edge,
            allow_ask && no_quote_reason.is_none(),
            no_quote_reason.clone(),
            config,
        );
        let expected_utility = Usd::new(bid.gross_edge_usdc.inner() + ask.gross_edge_usdc.inner());

        let set = DesiredQuoteSet {
            strategy_id: strategy_id.to_string(),
            symbol: features.symbol.clone(),
            session_id: session_id.to_string(),
            config_version: config.version,
            model_version: config.model_version.clone(),
            market_version: features.market_version,
            connection_generation: features.connection_generation,
            current_slot_revision,
            revision,
            fair_price: fair,
            reservation_price: inventory_decision.reservation_price,
            inventory_notional: inventory_decision.inventory_notional,
            expected_utility_usdc: expected_utility,
            budget_mode: budget.mode,
            bid,
            ask,
            created_at: now,
            valid_until: now
                + chrono::Duration::seconds(
                    config
                        .max_quote_lifetime_seconds
                        .to_string()
                        .parse::<i64>()
                        .unwrap_or(10),
                ),
            feature_values: vec![
                ("toxicity".into(), features.toxicity),
                ("half_spread_bps".into(), half_spread_bps),
                ("inventory_shift_bps".into(), inventory_decision.shift_bps),
            ],
        };
        set.validate()?;
        Ok(set)
    }

    fn quote_size(
        fair: Price,
        inventory_notional: Decimal,
        features: &MarketFeatures,
        config: &MarketMakerConfig,
    ) -> Size {
        let inventory_headroom =
            (config.hard_inventory_notional.inner() - inventory_notional.abs()).max(Decimal::ZERO);
        let inventory_size = inventory_headroom / fair.inner();
        let visible_depth = features
            .best_bid_size
            .inner()
            .min(features.best_ask_size.inner());
        let depth_size = visible_depth * config.max_depth_participation;
        let raw_size = config
            .quote_size
            .inner()
            .min(inventory_size)
            .min(depth_size);
        let stepped = raw_size.floor_to_step(config.lot_size);
        Size::new(stepped)
    }

    fn gross_edge(
        side: Side,
        fair: Price,
        quote_price: Price,
        size: Size,
        features: &MarketFeatures,
        config: &MarketMakerConfig,
    ) -> Usd {
        if size.inner() <= Decimal::ZERO {
            return Usd::ZERO;
        }
        let capture_rate = if side == Side::Buy {
            (fair.inner() - quote_price.inner()) / fair.inner()
        } else {
            (quote_price.inner() - fair.inner()) / fair.inner()
        };
        let adverse_rate =
            features.expected_adverse_markout_bps / Decimal::from_str_lenient("10000").unwrap();
        let funding_rate = features.funding_rate.abs() * config.horizon_seconds
            / Decimal::from_str_lenient("3600").unwrap();
        let edge_rate = capture_rate - config.signed_maker_fee_rate - adverse_rate - funding_rate;
        let expected =
            size.inner() * quote_price.inner() * edge_rate * config.expected_fill_probability;
        Usd::new(expected.max(Decimal::ZERO))
    }

    #[allow(clippy::too_many_arguments)]
    fn desired(
        strategy_id: &str,
        features: &MarketFeatures,
        side: Side,
        price: Price,
        size: Size,
        gross_edge: Usd,
        allowed: bool,
        global_reason: Option<String>,
        config: &MarketMakerConfig,
    ) -> DesiredQuote {
        let slot = QuoteSlotKey {
            strategy_id: strategy_id.to_string(),
            symbol: features.symbol.clone(),
            side,
            level: 0,
        };
        if !allowed {
            return DesiredQuote {
                slot,
                decision: QuoteDecision::NoQuote,
                price: None,
                size: None,
                gross_edge_usdc: Usd::ZERO,
                reason: global_reason.unwrap_or_else(|| "inventory_side_blocked".into()),
            };
        }
        if size.inner() < config.min_size {
            return DesiredQuote {
                slot,
                decision: QuoteDecision::NoQuote,
                price: None,
                size: None,
                gross_edge_usdc: Usd::ZERO,
                reason: "size_below_minimum".into(),
            };
        }
        if gross_edge.inner() <= config.min_expected_pnl_usdc.inner() {
            return DesiredQuote {
                slot,
                decision: QuoteDecision::NoQuote,
                price: None,
                size: None,
                gross_edge_usdc: Usd::ZERO,
                reason: "expected_edge_below_threshold".into(),
            };
        }
        DesiredQuote {
            slot,
            decision: QuoteDecision::Quote,
            price: Some(price),
            size: Some(size),
            gross_edge_usdc: gross_edge,
            reason: "positive_expected_edge".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_maker::models::MarketFeatures;
    use chrono::Utc;
    use hypeedge_domain::decimal::Size;

    fn features() -> MarketFeatures {
        MarketFeatures {
            symbol: "BTC".into(),
            market_version: 1,
            connection_generation: 0,
            exchange_ts: 0,
            received_at: Utc::now(),
            healthy: true,
            best_bid: Price::new(Decimal::from_str_lenient("99.5").unwrap()),
            best_ask: Price::new(Decimal::from_str_lenient("100.5").unwrap()),
            best_bid_size: Size::new(Decimal::from_str_lenient("5").unwrap()),
            best_ask_size: Size::new(Decimal::from_str_lenient("5").unwrap()),
            microprice: Price::new(Decimal::from_str_lenient("100").unwrap()),
            normalized_ofi: Decimal::ZERO,
            trade_flow: Decimal::ZERO,
            short_return: Decimal::ZERO,
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
        }
    }

    fn inventory() -> InventorySnapshot {
        InventorySnapshot {
            position_size: Size::ZERO,
            equity: Usd::new(Decimal::from_str_lenient("10000").unwrap()),
            available_balance: Usd::ZERO,
            margin_used: Usd::ZERO,
            observed_at: Utc::now(),
            healthy: true,
        }
    }

    fn budget(mode: ActionBudgetMode) -> ActionBudgetSnapshot {
        ActionBudgetSnapshot {
            mode,
            address_actions_remaining: 10000,
            cancel_headroom: 100,
            ip_weight_remaining: 1200,
            action_shadow_cost_usdc: Usd::ZERO,
            observed_at: Utc::now(),
            healthy: true,
        }
    }

    fn config() -> MarketMakerConfig {
        let mut c = MarketMakerConfig::default_with(
            1,
            Decimal::from_str_lenient("0.01").unwrap(),
            Decimal::from_str_lenient("0.001").unwrap(),
            Decimal::from_str_lenient("0.001").unwrap(),
        );
        c.soft_inventory_notional = Usd::new(Decimal::from_str_lenient("1000").unwrap());
        c.hard_inventory_notional = Usd::new(Decimal::from_str_lenient("2000").unwrap());
        c.emergency_inventory_notional = Usd::new(Decimal::from_str_lenient("3000").unwrap());
        c.quote_size = Size::new(Decimal::from_str_lenient("1").unwrap());
        c
    }

    #[test]
    fn flat_market_quotes_both_sides() {
        let policy = MarketMakerPolicy::new();
        let set = policy
            .quote(
                "mm_1",
                "s1",
                1,
                0,
                &features(),
                &inventory(),
                &budget(ActionBudgetMode::Normal),
                &config(),
            )
            .unwrap();
        assert_eq!(set.bid.decision, QuoteDecision::Quote);
        assert_eq!(set.ask.decision, QuoteDecision::Quote);
        assert!(set.bid.price.unwrap().inner() < set.ask.price.unwrap().inner());
        // reservation 100, min_half_spread 1bp → half spread = 100 * 1/10000 = 0.01.
        // bid floor(99.99) = 99.99; ask ceil(100.01) = 100.01.
        assert_eq!(set.bid.price.unwrap().to_string(), "99.99");
        assert_eq!(set.ask.price.unwrap().to_string(), "100.01");
        assert_eq!(set.feature_values[1].1.to_string(), "1");
    }

    /// Golden parity: the exact desired-quote values were produced by the pinned
    /// Python `MarketMakerPolicy.quote` for the identical inputs. This pins the
    /// fair-value, inventory, half-spread, step, size, and gross-edge math.
    #[test]
    fn policy_matches_python_golden() {
        let policy = MarketMakerPolicy::new();
        let set = policy
            .quote(
                "mm_1",
                "s1",
                1,
                0,
                &features(),
                &inventory(),
                &budget(ActionBudgetMode::Normal),
                &config(),
            )
            .unwrap();
        // Python: fair=100.0000, reservation=100, bid=99.99@0.25, ask=100.01@0.25,
        // half_spread_bps=1, inventory_shift_bps=0, expected_utility=0.0015.
        assert_eq!(set.fair_price.to_string(), "100");
        assert_eq!(set.reservation_price.to_string(), "100");
        assert_eq!(set.bid.price.unwrap().to_string(), "99.99");
        assert_eq!(set.bid.size.unwrap().to_string(), "0.25");
        assert_eq!(set.ask.price.unwrap().to_string(), "100.01");
        assert_eq!(set.ask.size.unwrap().to_string(), "0.25");
        assert_eq!(set.feature_values[1].1.to_string(), "1"); // half_spread_bps
        assert_eq!(set.feature_values[2].1.to_string(), "0"); // inventory_shift_bps
        // expected_utility ≈ 2 * (0.25 * 99.99 * 0.0003 * 0.1) = 0.0015.
        assert_eq!(set.expected_utility_usdc.to_string(), "0.0015");
    }

    #[test]
    fn cancel_only_blocks_quotes() {
        let policy = MarketMakerPolicy::new();
        let set = policy
            .quote(
                "mm_1",
                "s1",
                1,
                0,
                &features(),
                &inventory(),
                &budget(ActionBudgetMode::CancelOnly),
                &config(),
            )
            .unwrap();
        assert_eq!(set.bid.decision, QuoteDecision::NoQuote);
        assert_eq!(set.ask.decision, QuoteDecision::NoQuote);
        assert_eq!(set.bid.reason, "budget_cancel_only");
    }

    #[test]
    fn unhealthy_market_blocks() {
        let policy = MarketMakerPolicy::new();
        let mut f = features();
        f.healthy = false;
        let set = policy
            .quote(
                "mm_1",
                "s1",
                1,
                0,
                &f,
                &inventory(),
                &budget(ActionBudgetMode::Normal),
                &config(),
            )
            .unwrap();
        assert_eq!(set.bid.reason, "market_unhealthy");
    }

    #[test]
    fn long_inventory_skews_bid_down_and_blocks_bid() {
        let policy = MarketMakerPolicy::new();
        // Wider min spread so the ask (reservation + half_spread) stays above
        // fair despite the downward skew, letting the ask side quote.
        let mut config = config();
        config.min_half_spread_bps = Decimal::from_str_lenient("5").unwrap();
        let mut inv = inventory();
        inv.position_size = Size::new(Decimal::from_str_lenient("12").unwrap()); // 1200 notional (soft 1000, < hard)
        let set = policy
            .quote(
                "mm_1",
                "s1",
                1,
                0,
                &features(),
                &inv,
                &budget(ActionBudgetMode::Normal),
                &config,
            )
            .unwrap();
        // Long at soft → bid blocked by inventory_side_blocked.
        assert_eq!(set.bid.decision, QuoteDecision::NoQuote);
        assert_eq!(set.bid.reason, "inventory_side_blocked");
        // Ask still quotes (reservation 99.94, half_spread ~0.05 → ask ~99.99 above fair after step).
        assert_eq!(set.ask.decision, QuoteDecision::Quote);
    }
}
