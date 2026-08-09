//! Market microstructure feature engine, port of
//! `src/hypeedge/market_data/features.py` (the pure windows + build).
//!
//! Maintains small in-memory mid-price and trade windows and builds the
//! [`MarketFeatures`] snapshot consumed by the market-maker policy.

use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::{Decimal, Price};
use hypeedge_domain::enums::Side;
use hypeedge_domain::models::{L2BookSnapshot, Trade};

use crate::market_maker::models::{MarketFeatures, MarketMakerConfig};

/// External-reference feature subset used inside `build`.
#[derive(Debug, Clone, PartialEq)]
pub struct ExternalFeatures {
    pub source: Option<String>,
    pub symbol: Option<String>,
    pub raw_price: Option<Price>,
    pub adjusted_price: Option<Price>,
    pub basis_bps: Decimal,
    pub effective_weight: Decimal,
    pub confidence: Decimal,
    pub age_ms: Option<i64>,
    pub quality: String,
    pub observed_at: Option<DateTime<Utc>>,
}

impl Default for ExternalFeatures {
    fn default() -> Self {
        Self {
            source: None,
            symbol: None,
            raw_price: None,
            adjusted_price: None,
            basis_bps: Decimal::ZERO,
            effective_weight: Decimal::ZERO,
            confidence: Decimal::ZERO,
            age_ms: None,
            quality: "unavailable".into(),
            observed_at: None,
        }
    }
}

/// The market feature engine.
pub struct MarketFeatureEngine {
    depth_levels: usize,
    window: chrono::Duration,
    max_events: usize,
    mid_history: HashMap<String, VecDeque<(DateTime<Utc>, Decimal)>>,
    trades: HashMap<String, VecDeque<Trade>>,
}

impl MarketFeatureEngine {
    pub fn new(
        depth_levels: usize,
        window_seconds: f64,
        max_events: usize,
    ) -> Result<Self, String> {
        if depth_levels == 0 || window_seconds <= 0.0 || max_events <= 1 {
            return Err("feature-engine windows must be positive".into());
        }
        Ok(Self {
            depth_levels,
            window: chrono::Duration::milliseconds((window_seconds * 1000.0) as i64),
            max_events,
            mid_history: HashMap::new(),
            trades: HashMap::new(),
        })
    }

    pub fn observe_book(&mut self, snapshot: &L2BookSnapshot) {
        if snapshot.bids.is_empty() || snapshot.asks.is_empty() {
            return;
        }
        let mid = (snapshot.bids[0].price.inner() + snapshot.asks[0].price.inner())
            .div(Decimal::from_i128(2));
        let history = self
            .mid_history
            .entry(snapshot.symbol.clone())
            .or_insert_with(|| VecDeque::with_capacity(self.max_events));
        if history.len() >= self.max_events {
            history.pop_front();
        }
        history.push_back((snapshot.local_ts, mid));
        self.trim(snapshot.symbol.clone(), snapshot.local_ts);
    }

    pub fn observe_trade(&mut self, trade: &Trade) {
        let trades = self
            .trades
            .entry(trade.symbol.clone())
            .or_insert_with(|| VecDeque::with_capacity(self.max_events));
        if trades.len() >= self.max_events {
            trades.pop_front();
        }
        trades.push_back(trade.clone());
        self.trim(trade.symbol.clone(), trade.local_ts);
    }

    fn trim(&mut self, symbol: String, now: DateTime<Utc>) {
        if let Some(history) = self.mid_history.get_mut(&symbol) {
            while let Some(front) = history.front() {
                if now - front.0 > self.window {
                    history.pop_front();
                } else {
                    break;
                }
            }
        }
        if let Some(trades) = self.trades.get_mut(&symbol) {
            while let Some(front) = trades.front() {
                if now - front.local_ts > self.window {
                    trades.pop_front();
                } else {
                    break;
                }
            }
        }
    }

    /// Build a `MarketFeatures` snapshot from the current book.
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        &mut self,
        snapshot: &L2BookSnapshot,
        healthy: bool,
        funding_rate: Decimal,
        expected_adverse_markout_bps: Decimal,
        latency_seconds: Option<Decimal>,
        latency_quality: &str,
        markout_quality: &str,
        external: Option<&crate::market_maker::models::ExternalReferenceInput>,
        config: Option<&MarketMakerConfig>,
        decision_at: DateTime<Utc>,
    ) -> Result<MarketFeatures, String> {
        if snapshot.bids.is_empty() || snapshot.asks.is_empty() {
            return Err("cannot build market features from an empty book".into());
        }
        self.observe_book(snapshot);
        let bid = &snapshot.bids[0];
        let ask = &snapshot.asks[0];
        let top_total = bid.size.inner() + ask.size.inner();
        if top_total <= Decimal::ZERO {
            return Err("top-of-book liquidity must be positive".into());
        }
        let microprice = (ask.price.inner() * bid.size.inner()
            + bid.price.inner() * ask.size.inner())
        .div(top_total);

        let bid_depth: Decimal = snapshot
            .bids
            .iter()
            .take(self.depth_levels)
            .fold(Decimal::ZERO, |acc, l| acc + l.size.inner());
        let ask_depth: Decimal = snapshot
            .asks
            .iter()
            .take(self.depth_levels)
            .fold(Decimal::ZERO, |acc, l| acc + l.size.inner());
        let depth_total = bid_depth + ask_depth;
        let ofi = if depth_total > Decimal::ZERO {
            (bid_depth - ask_depth).div(depth_total)
        } else {
            Decimal::ZERO
        };
        let trade_flow = self.trade_flow(&snapshot.symbol);
        let (short_return, variance) = self.return_features(&snapshot.symbol);

        let mut latency_buffer_bps = Decimal::ZERO;
        if let (Some(seconds), Some(cfg)) = (latency_seconds, config) {
            let latency_variance = variance * seconds.max(Decimal::ZERO);
            latency_buffer_bps = sqrt_decimal(latency_variance)
                * Decimal::from_str_lenient("10000").unwrap()
                * cfg.latency_risk_multiplier;
        }
        let toxicity = Decimal::ONE.min(
            ofi.abs() * Decimal::from_str_lenient("0.35").unwrap()
                + trade_flow.abs() * Decimal::from_str_lenient("0.35").unwrap()
                + sqrt_decimal(variance).min(Decimal::ONE)
                    * Decimal::from_str_lenient("0.30").unwrap(),
        );
        let external = self.external_features(
            (bid.price.inner() + ask.price.inner()).div(Decimal::from_i128(2)),
            decision_at,
            external,
            config,
        );

        let features = MarketFeatures {
            symbol: snapshot.symbol.clone(),
            market_version: snapshot.version as i64,
            connection_generation: snapshot.connection_generation as i64,
            exchange_ts: snapshot.timestamp,
            received_at: snapshot.local_ts,
            healthy,
            best_bid: bid.price,
            best_ask: ask.price,
            best_bid_size: bid.size,
            best_ask_size: ask.size,
            microprice: Price::new(microprice),
            normalized_ofi: ofi,
            trade_flow,
            short_return,
            return_variance_per_second: variance,
            expected_adverse_markout_bps,
            latency_buffer_bps,
            toxicity,
            funding_rate,
            external_source: external.source,
            external_symbol: external.symbol,
            external_raw_price: external.raw_price,
            external_adjusted_price: external.adjusted_price,
            external_basis_bps: external.basis_bps,
            external_effective_weight: external.effective_weight,
            external_confidence: external.confidence,
            external_age_ms: external.age_ms,
            external_quality: external.quality,
            external_observed_at: external.observed_at,
            latency_quality: latency_quality.to_string(),
            markout_quality: markout_quality.to_string(),
        };
        features.validate()?;
        Ok(features)
    }

    fn external_features(
        &self,
        local_mid: Decimal,
        decision_at: DateTime<Utc>,
        reference: Option<&crate::market_maker::models::ExternalReferenceInput>,
        config: Option<&MarketMakerConfig>,
    ) -> ExternalFeatures {
        let (Some(reference), Some(config)) = (reference, config) else {
            return ExternalFeatures::default();
        };
        let age = decision_at - reference.observed_at;
        let age_ms = (age.num_milliseconds()).max(0);
        let mut common = ExternalFeatures {
            source: Some(reference.source.clone()),
            symbol: Some(reference.symbol.clone()),
            raw_price: reference.raw_price,
            confidence: reference.confidence,
            age_ms: Some(age_ms),
            observed_at: Some(reference.observed_at),
            ..Default::default()
        };
        if age.num_seconds() < 0 {
            common.quality = "clock_skew".into();
            return common;
        }
        if matches!(reference.quality.as_str(), "disabled" | "stale")
            || reference.confidence <= Decimal::ZERO
        {
            common.quality = reference.quality.clone();
            return common;
        }
        let max_age = config.external_max_age_seconds;
        let age_seconds =
            Decimal::from_str_lenient(&format!("{}", age.num_milliseconds() as f64 / 1000.0))
                .unwrap_or(Decimal::ZERO);
        if age_seconds > max_age {
            common.quality = "stale".into();
            return common;
        }
        let Some(adjusted) = reference.adjusted_price else {
            return common;
        };
        let adjusted = adjusted.inner();
        let deviation_bps = (adjusted / local_mid - Decimal::ONE).abs()
            * Decimal::from_str_lenient("10000").unwrap();
        let (quality, effective_weight) = if deviation_bps > config.external_outlier_bps {
            ("outlier".to_string(), Decimal::ZERO)
        } else {
            let quality = if reference.quality == "healthy" {
                "good"
            } else {
                "degraded"
            }
            .to_string();
            let freshness = (Decimal::ONE - age_seconds / max_age).max(Decimal::ZERO);
            let effective_weight = config
                .external_reference_weight
                .min(reference.effective_weight)
                .min(Decimal::ONE)
                * freshness;
            (quality, effective_weight)
        };
        common.adjusted_price = Some(Price::new(adjusted));
        common.basis_bps = reference.basis_bps;
        common.effective_weight = effective_weight;
        common.quality = quality;
        common
    }

    fn trade_flow(&self, symbol: &str) -> Decimal {
        let Some(trades) = self.trades.get(symbol) else {
            return Decimal::ZERO;
        };
        let mut buy = Decimal::ZERO;
        let mut sell = Decimal::ZERO;
        for trade in trades {
            let notional = trade.price.inner() * trade.size.inner();
            if trade.side == Side::Buy {
                buy += notional;
            } else {
                sell += notional;
            }
        }
        let total = buy + sell;
        if total > Decimal::ZERO {
            (buy - sell).div(total)
        } else {
            Decimal::ZERO
        }
    }

    fn return_features(&self, symbol: &str) -> (Decimal, Decimal) {
        let Some(history) = self.mid_history.get(symbol) else {
            return (Decimal::ZERO, Decimal::ZERO);
        };
        if history.len() < 2 {
            return (Decimal::ZERO, Decimal::ZERO);
        }
        let mids: Vec<Decimal> = history.iter().map(|(_, v)| *v).collect();
        let short_return = if mids[0] > Decimal::ZERO {
            (mids[mids.len() - 1] - mids[0]).div(mids[0])
        } else {
            Decimal::ZERO
        };
        let mut log_returns: Vec<f64> = Vec::new();
        for pair in mids.windows(2) {
            if pair[0] > Decimal::ZERO && pair[1] > Decimal::ZERO {
                log_returns.push(
                    (pair[1] / pair[0])
                        .to_string()
                        .parse::<f64>()
                        .unwrap_or(0.0)
                        .ln(),
                );
            }
        }
        if log_returns.is_empty() {
            return (short_return, Decimal::ZERO);
        }
        let mean: f64 = log_returns.iter().sum::<f64>() / log_returns.len() as f64;
        let variance: f64 = log_returns
            .iter()
            .map(|v| (v - mean) * (v - mean))
            .sum::<f64>()
            / log_returns.len() as f64;
        (
            short_return,
            Decimal::from_str_lenient(&format!("{variance}")).unwrap_or(Decimal::ZERO),
        )
    }
}

/// Square root of a non-negative decimal via f64.
fn sqrt_decimal(v: Decimal) -> Decimal {
    if v <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let f = v.to_string().parse::<f64>().unwrap_or(0.0);
    Decimal::from_f64(f.sqrt()).unwrap_or(Decimal::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::decimal::Size;

    fn book(bid: &str, ask: &str) -> L2BookSnapshot {
        L2BookSnapshot {
            symbol: "BTC".into(),
            bids: vec![hypeedge_domain::models::L2Level {
                price: Price::new(Decimal::from_str_lenient(bid).unwrap()),
                size: Size::new(Decimal::from_str_lenient("5").unwrap()),
            }],
            asks: vec![hypeedge_domain::models::L2Level {
                price: Price::new(Decimal::from_str_lenient(ask).unwrap()),
                size: Size::new(Decimal::from_str_lenient("5").unwrap()),
            }],
            timestamp: 1700000000000,
            local_ts: Utc::now(),
            version: 1,
            connection_generation: 0,
        }
    }

    #[test]
    fn build_flat_book_produces_healthy_features() {
        let mut engine = MarketFeatureEngine::new(5, 5.0, 2048).unwrap();
        let b = book("99.5", "100.5");
        let f = engine
            .build(
                &b,
                true,
                Decimal::ZERO,
                Decimal::ZERO,
                None,
                "configured",
                "configured",
                None,
                None,
                b.local_ts,
            )
            .unwrap();
        assert!(f.healthy);
        assert_eq!(f.microprice.to_string(), "100"); // equal sizes → mid
        assert_eq!(f.normalized_ofi.to_string(), "0");
        assert_eq!(f.toxicity.to_string(), "0");
        assert_eq!(f.mid_price().to_string(), "100");
    }

    #[test]
    fn trades_drive_trade_flow() {
        let mut engine = MarketFeatureEngine::new(5, 5.0, 2048).unwrap();
        let now = Utc::now();
        let buy = Trade {
            symbol: "BTC".into(),
            price: Price::new(Decimal::from_str_lenient("100").unwrap()),
            size: Size::new(Decimal::from_str_lenient("2").unwrap()),
            side: Side::Buy,
            tid: 1,
            timestamp: 1,
            local_ts: now,
        };
        let sell = Trade {
            symbol: "BTC".into(),
            price: Price::new(Decimal::from_str_lenient("100").unwrap()),
            size: Size::new(Decimal::from_str_lenient("1").unwrap()),
            side: Side::Sell,
            tid: 2,
            timestamp: 2,
            local_ts: now,
        };
        engine.observe_trade(&buy);
        engine.observe_trade(&sell);
        // flow = (200 - 100)/300 = 1/3.
        assert_eq!(engine.trade_flow("BTC").to_string(), "0.333333333333333333");
    }

    #[test]
    fn return_variance_is_computed() {
        let mut engine = MarketFeatureEngine::new(5, 100.0, 2048).unwrap();
        let now = Utc::now();
        let mut b = book("99", "101");
        for i in 0..4 {
            b.version = i as u64;
            b.local_ts = now + chrono::Duration::seconds(i as i64);
            b.bids[0].price = Price::new(
                Decimal::from_str_lenient(&format!("{}", 100.0 + i as f64 * 0.5)).unwrap(),
            );
            engine.observe_book(&b);
        }
        let (sr, var) = engine.return_features("BTC");
        // short_return = (last-first)/first over the window.
        assert!(sr > Decimal::ZERO);
        assert!(var >= Decimal::ZERO);
    }
}
