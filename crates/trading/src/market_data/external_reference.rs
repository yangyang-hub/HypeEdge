//! External reference-price normalization and latest-value access, port of
//! `src/hypeedge/market_data/external_reference.py`.
//!
//! External venues are advisory inputs only. A stale, crossed, or divergent
//! reference deterministically loses its weight and never blocks Hyperliquid's
//! native market-data path.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::Decimal;
use hypeedge_domain::models::{
    ExternalMarket, ExternalQuality, ExternalReferenceSnapshot, ExternalVenueQuote,
};

/// The bounded external-reference configuration (mirrors
/// `config::ExternalReferenceSettings` without the transport fields).
#[derive(Debug, Clone)]
pub struct ExternalReferenceConfig {
    pub enabled: bool,
    pub spot_weight: Decimal,
    pub perpetual_weight: Decimal,
    pub max_external_weight: Decimal,
    pub basis_ewma_alpha: Decimal,
    pub stale_after_ms: u64,
    pub max_perp_spot_divergence_bps: Decimal,
    pub max_mark_book_divergence_bps: Decimal,
}

impl ExternalReferenceConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        enabled: bool,
        spot_weight: Decimal,
        perpetual_weight: Decimal,
        max_external_weight: Decimal,
        basis_ewma_alpha: Decimal,
        stale_after_ms: u64,
        max_perp_spot_divergence_bps: Decimal,
        max_mark_book_divergence_bps: Decimal,
    ) -> Self {
        Self {
            enabled,
            spot_weight,
            perpetual_weight,
            max_external_weight,
            basis_ewma_alpha,
            stale_after_ms,
            max_perp_spot_divergence_bps,
            max_mark_book_divergence_bps,
        }
    }
}

/// Convert a domain Decimal to f64 for the slow EWMA math (the basis is a slow
/// calibration input; f64 precision is more than sufficient).
pub fn dec_to_f64(d: Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(0.0)
}

/// In-memory per-symbol latest-value provider with deterministic quality gates.
pub struct LatestExternalReferenceProvider {
    settings: ExternalReferenceConfig,
    quotes: HashMap<(String, ExternalMarket), ExternalVenueQuote>,
    basis_log_ewma: HashMap<String, f64>,
    version: HashMap<String, u64>,
}

impl LatestExternalReferenceProvider {
    pub fn new(settings: ExternalReferenceConfig) -> Self {
        Self {
            settings,
            quotes: HashMap::new(),
            basis_log_ewma: HashMap::new(),
            version: HashMap::new(),
        }
    }

    /// Apply a venue observation, ignoring regressions within a generation.
    pub fn update_quote(&mut self, quote: ExternalVenueQuote) -> ExternalReferenceSnapshot {
        let symbol = quote.symbol.clone();
        let key = (symbol.clone(), quote.market);
        if let Some(previous) = self.quotes.get(&key) {
            if quote.connection_generation < previous.connection_generation {
                return self.get_external_reference(&symbol, Utc::now());
            }
            if quote.connection_generation == previous.connection_generation
                && quote.sequence <= previous.sequence
            {
                return self.get_external_reference(&symbol, Utc::now());
            }
        }
        self.quotes.insert(key, quote);
        *self.version.entry(symbol.clone()).or_default() += 1;
        self.get_external_reference(&symbol, Utc::now())
    }

    /// Update the slow log-basis calibration from a native Hyperliquid midpoint.
    pub fn update_hyperliquid_mid(
        &mut self,
        symbol: &str,
        mid: Decimal,
    ) -> ExternalReferenceSnapshot {
        let snapshot = self.get_external_reference(symbol, Utc::now());
        if snapshot.quality == ExternalQuality::Healthy && mid > Decimal::ZERO {
            let Some(raw) = snapshot.raw_price else {
                return snapshot;
            };
            let observation = dec_to_f64(mid.div(raw.inner())).ln();
            let previous = self.basis_log_ewma.get(symbol).copied();
            let alpha = dec_to_f64(self.settings.basis_ewma_alpha);
            let next = match previous {
                Some(prev) => alpha * observation + (1.0 - alpha) * prev,
                None => observation,
            };
            self.basis_log_ewma.insert(symbol.to_string(), next);
            *self.version.entry(symbol.to_string()).or_default() += 1;
        }
        self.get_external_reference(symbol, Utc::now())
    }

    /// Build a freshness-aware snapshot from the latest observations.
    pub fn get_external_reference(
        &self,
        symbol: &str,
        now: DateTime<Utc>,
    ) -> ExternalReferenceSnapshot {
        if !self.settings.enabled {
            return empty_snapshot(
                symbol,
                now,
                ExternalQuality::Disabled,
                vec!["external_reference_disabled".into()],
            );
        }

        let spot = self.quotes.get(&(symbol.to_string(), ExternalMarket::Spot));
        let perpetual = self
            .quotes
            .get(&(symbol.to_string(), ExternalMarket::Perpetual));
        let mark = self
            .quotes
            .get(&(symbol.to_string(), ExternalMarket::PerpetualMark));
        let mut reasons: Vec<String> = Vec::new();

        let fresh_spot = spot.is_some_and(|q| self.is_fresh(q, now));
        let fresh_perpetual = perpetual.is_some_and(|q| self.is_fresh(q, now));
        let fresh_mark = mark.is_some_and(|q| self.is_fresh(q, now));

        if let Some(q) = spot
            && quote_crossed(q)
        {
            reasons.push("spot_crossed".into());
        }
        if let Some(q) = perpetual
            && quote_crossed(q)
        {
            reasons.push("perpetual_crossed".into());
        }

        let spot_mid = fresh_spot.then(|| spot.and_then(quote_mid)).flatten();
        let perpetual_mid = fresh_perpetual
            .then(|| perpetual.and_then(quote_mid))
            .flatten();
        let perpetual_mark = fresh_mark
            .then(|| mark.and_then(|q| q.mark_price))
            .flatten();

        if let (Some(sm), Some(pm)) = (spot_mid, perpetual_mid) {
            let divergence = divergence_bps(pm, sm);
            if divergence > self.settings.max_perp_spot_divergence_bps {
                reasons.push("perpetual_spot_outlier".into());
            }
        }
        if let (Some(pm), Some(mark_px)) = (perpetual_mid, perpetual_mark) {
            let mark_divergence = divergence_bps(pm, mark_px.inner());
            if mark_divergence > self.settings.max_mark_book_divergence_bps {
                reasons.push("perpetual_mark_outlier".into());
            }
        }

        let mut contributors: Vec<(Decimal, Decimal, &ExternalVenueQuote)> = Vec::new();
        if let (Some(sm), Some(q)) = (spot_mid, spot) {
            contributors.push((sm, self.settings.spot_weight, q));
        }
        if let (Some(pm), Some(q)) = (perpetual_mid, perpetual) {
            contributors.push((pm, self.settings.perpetual_weight, q));
        }
        if contributors.is_empty() {
            let reason = if spot.is_some() || perpetual.is_some() || mark.is_some() {
                "all_sources_stale_or_invalid"
            } else {
                "no_external_observation"
            };
            reasons.push(reason.into());
            return empty_snapshot(symbol, now, ExternalQuality::Stale, dedup(reasons));
        }

        let weight_sum: Decimal = contributors
            .iter()
            .map(|(_, w, _)| *w)
            .fold(Decimal::ZERO, |a, b| a + b);
        let mut weighted = Decimal::ZERO;
        for (price, weight, _) in &contributors {
            weighted += *price * *weight;
        }
        let raw = weighted.div(weight_sum);
        let observed_at = contributors
            .iter()
            .map(|(_, _, q)| q.received_at)
            .min()
            .unwrap_or(now);
        let age_ms = (now - observed_at).num_milliseconds().max(0) as u64;
        let sequence = contributors
            .iter()
            .map(|(_, _, q)| q.sequence)
            .max()
            .unwrap_or(0);
        let generation = contributors
            .iter()
            .map(|(_, _, q)| q.connection_generation)
            .max()
            .unwrap_or(0);

        let both_books = spot_mid.is_some() && perpetual_mid.is_some();
        if !both_books {
            reasons.push("single_source_only".into());
        }
        let anomaly = reasons
            .iter()
            .any(|r| r.ends_with("crossed") || r.ends_with("outlier"));
        let quality = if both_books && !anomaly {
            ExternalQuality::Healthy
        } else {
            ExternalQuality::Degraded
        };
        let age_frac = age_ms as f64 / self.settings.stale_after_ms.max(1) as f64;
        let freshness = (1.0 - age_frac).max(0.0);
        let confidence = if quality == ExternalQuality::Healthy {
            1.0
        } else {
            0.5
        } * freshness;
        let mut effective_weight = dec_to_f64(self.settings.max_external_weight) * confidence;
        let mut conf = confidence;
        if anomaly {
            conf = 0.0;
            effective_weight = 0.0;
        }

        let basis = self.basis_log_ewma.get(symbol).copied().unwrap_or(0.0);
        let adjusted = Decimal::from_f64(dec_to_f64(raw) * basis.exp()).unwrap_or_default();
        let basis_bps = Decimal::from_f64((basis.exp() - 1.0) * 10_000.0).unwrap_or_default();

        ExternalReferenceSnapshot {
            source: "binance_spot_perpetual".into(),
            symbol: symbol.to_string(),
            raw_price: Some(Price::new(raw)),
            adjusted_price: Some(Price::new(adjusted)),
            basis_bps,
            effective_weight: Decimal::from_f64(effective_weight).unwrap_or_default(),
            confidence: Decimal::from_f64(conf).unwrap_or_default(),
            age_ms,
            quality,
            observed_at,
            spot_mid: spot_mid.map(Price::new),
            perpetual_mid: perpetual_mid.map(Price::new),
            perpetual_mark,
            sequence,
            connection_generation: generation,
            quality_reasons: dedup(reasons),
        }
    }

    fn is_fresh(&self, quote: &ExternalVenueQuote, now: DateTime<Utc>) -> bool {
        let age_ms = (now - quote.received_at).num_milliseconds();
        age_ms >= 0 && (age_ms as u64) <= self.settings.stale_after_ms
    }

    /// Number of applied updates (for telemetry).
    pub fn version(&self, symbol: &str) -> u64 {
        self.version.get(symbol).copied().unwrap_or(0)
    }
}

/// Use the trading crate's re-exported Price type.
use hypeedge_domain::Price;

fn divergence_bps(left: Decimal, right: Decimal) -> Decimal {
    if right <= Decimal::ZERO {
        return Decimal::from_scaled(i64::MAX as u128 as i128, 0);
    }
    let ratio = left.div(right) - Decimal::ONE;
    let abs = if ratio < Decimal::ZERO { -ratio } else { ratio };
    abs * Decimal::from_scaled(10_000, 0)
}

fn empty_snapshot(
    symbol: &str,
    now: DateTime<Utc>,
    quality: ExternalQuality,
    reasons: Vec<String>,
) -> ExternalReferenceSnapshot {
    ExternalReferenceSnapshot {
        source: "binance_spot_perpetual".into(),
        symbol: symbol.to_string(),
        raw_price: None,
        adjusted_price: None,
        basis_bps: Decimal::ZERO,
        effective_weight: Decimal::ZERO,
        confidence: Decimal::ZERO,
        age_ms: 0,
        quality,
        observed_at: now,
        spot_mid: None,
        perpetual_mid: None,
        perpetual_mark: None,
        sequence: 0,
        connection_generation: 0,
        quality_reasons: reasons,
    }
}

fn dedup(reasons: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    reasons
        .into_iter()
        .filter(|r| seen.insert(r.clone()))
        .collect()
}

/// Whether a quote's bid/ask are crossed (bid >= ask).
fn quote_crossed(q: &ExternalVenueQuote) -> bool {
    matches!((q.bid, q.ask), (Some(b), Some(a)) if b >= a)
}

/// The quote's mid price, or `None` when one side is missing or crossed.
fn quote_mid(q: &ExternalVenueQuote) -> Option<Decimal> {
    match (q.bid, q.ask) {
        (Some(b), Some(a)) if !quote_crossed(q) => {
            Some((b.inner() + a.inner()).div(Decimal::from_scaled(2, 0)))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn settings() -> ExternalReferenceConfig {
        ExternalReferenceConfig {
            enabled: true,
            spot_weight: Decimal::from_scaled(40, 2),
            perpetual_weight: Decimal::from_scaled(60, 2),
            max_external_weight: Decimal::from_scaled(35, 2),
            basis_ewma_alpha: Decimal::from_scaled(2, 2),
            stale_after_ms: 1500,
            max_perp_spot_divergence_bps: Decimal::from_scaled(25, 0),
            max_mark_book_divergence_bps: Decimal::from_scaled(25, 0),
        }
    }

    fn at(ms: i64) -> DateTime<Utc> {
        Utc.timestamp_millis_opt(ms).unwrap()
    }

    /// A quote whose receipt time is fresh relative to now (default), or
    /// `ago_ms` milliseconds in the past for staleness tests.
    fn quote(
        symbol: &str,
        market: ExternalMarket,
        bid: &str,
        ask: &str,
        seq: u64,
    ) -> ExternalVenueQuote {
        quote_at(symbol, market, bid, ask, seq, Utc::now())
    }

    fn quote_at(
        symbol: &str,
        market: ExternalMarket,
        bid: &str,
        ask: &str,
        seq: u64,
        received_at: DateTime<Utc>,
    ) -> ExternalVenueQuote {
        ExternalVenueQuote {
            symbol: symbol.into(),
            venue_symbol: symbol.into(),
            market,
            exchange_ts: received_at.timestamp_millis(),
            received_at,
            sequence: seq,
            connection_generation: 0,
            bid: Some(Price::new(Decimal::from_str_lenient(bid).unwrap())),
            ask: Some(Price::new(Decimal::from_str_lenient(ask).unwrap())),
            mark_price: if market == ExternalMarket::PerpetualMark {
                Some(Price::new(Decimal::from_str_lenient(bid).unwrap()))
            } else {
                None
            },
        }
    }

    #[test]
    fn disabled_returns_empty_with_zero_weight() {
        let mut cfg = settings();
        cfg.enabled = false;
        let provider = LatestExternalReferenceProvider::new(cfg);
        let snapshot = provider.get_external_reference("BTC", at(1_000));
        assert_eq!(snapshot.quality, ExternalQuality::Disabled);
        assert_eq!(snapshot.effective_weight, Decimal::ZERO);
        assert!(snapshot.raw_price.is_none());
        assert_eq!(snapshot.quality_reasons[0], "external_reference_disabled");
    }

    #[test]
    fn healthy_with_both_books() {
        let mut provider = LatestExternalReferenceProvider::new(settings());
        provider.update_quote(quote("BTC", ExternalMarket::Spot, "49950", "50050", 1));
        provider.update_quote(quote("BTC", ExternalMarket::Perpetual, "50000", "50100", 1));
        let snapshot = provider.get_external_reference("BTC", Utc::now());
        assert_eq!(snapshot.quality, ExternalQuality::Healthy);
        assert!(snapshot.raw_price.is_some());
        assert!(snapshot.effective_weight > Decimal::ZERO);
        assert!(snapshot.spot_mid.is_some() && snapshot.perpetual_mid.is_some());
        // Both books => no single-source reason.
        assert!(
            !snapshot
                .quality_reasons
                .contains(&"single_source_only".to_string())
        );
    }

    #[test]
    fn single_source_is_degraded() {
        let mut provider = LatestExternalReferenceProvider::new(settings());
        provider.update_quote(quote("BTC", ExternalMarket::Spot, "49950", "50050", 1));
        let snapshot = provider.get_external_reference("BTC", Utc::now());
        assert_eq!(snapshot.quality, ExternalQuality::Degraded);
        assert!(
            snapshot
                .quality_reasons
                .contains(&"single_source_only".to_string())
        );
        // Degraded halves confidence.
        assert!(snapshot.confidence <= Decimal::from_scaled(50, 2));
    }

    #[test]
    fn stale_sources_yield_zero_weight() {
        let mut provider = LatestExternalReferenceProvider::new(settings());
        // 5000ms old > 1500ms stale threshold.
        let old = Utc::now() - chrono::Duration::milliseconds(5000);
        provider.update_quote(quote_at(
            "BTC",
            ExternalMarket::Spot,
            "49950",
            "50050",
            1,
            old,
        ));
        let snapshot = provider.get_external_reference("BTC", Utc::now());
        assert_eq!(snapshot.quality, ExternalQuality::Stale);
        assert_eq!(snapshot.effective_weight, Decimal::ZERO);
    }

    #[test]
    fn crossed_book_is_zero_weight() {
        let mut provider = LatestExternalReferenceProvider::new(settings());
        // bid >= ask => crossed.
        provider.update_quote(quote("BTC", ExternalMarket::Spot, "50100", "50050", 1));
        let snapshot = provider.get_external_reference("BTC", Utc::now());
        assert_eq!(snapshot.quality, ExternalQuality::Stale);
        assert_eq!(snapshot.effective_weight, Decimal::ZERO);
        assert!(
            snapshot
                .quality_reasons
                .contains(&"spot_crossed".to_string())
        );
    }

    #[test]
    fn update_quote_ignores_regressions() {
        let mut provider = LatestExternalReferenceProvider::new(settings());
        let q1 = quote("BTC", ExternalMarket::Spot, "49950", "50050", 5);
        provider.update_quote(q1);
        // Same generation, lower sequence — ignored.
        let q2 = quote("BTC", ExternalMarket::Spot, "49900", "50100", 3);
        let snapshot = provider.update_quote(q2);
        assert_eq!(snapshot.spot_mid.unwrap().to_string(), "50000");
        assert_eq!(provider.version("BTC"), 1);
    }

    #[test]
    fn update_hyperliquid_mid_calibrates_basis() {
        let mut provider = LatestExternalReferenceProvider::new(settings());
        provider.update_quote(quote("BTC", ExternalMarket::Spot, "49950", "50050", 1));
        provider.update_quote(quote("BTC", ExternalMarket::Perpetual, "50000", "50100", 1));
        // Native mid == 50250; raw ≈ 50050 (weighted). Basis becomes positive.
        provider.update_hyperliquid_mid("BTC", Decimal::from_scaled(50250, 0));
        let snapshot = provider.get_external_reference("BTC", Utc::now());
        assert!(snapshot.basis_bps != Decimal::ZERO);
        // Adjusted price reflects the basis.
        assert!(snapshot.adjusted_price.is_some());
    }

    #[test]
    fn no_observation_yields_stale() {
        let provider = LatestExternalReferenceProvider::new(settings());
        let snapshot = provider.get_external_reference("BTC", at(1_000));
        assert_eq!(snapshot.quality, ExternalQuality::Stale);
        assert!(
            snapshot
                .quality_reasons
                .contains(&"no_external_observation".to_string())
        );
    }

    #[test]
    fn divergence_flags_anomaly() {
        let mut provider = LatestExternalReferenceProvider::new(settings());
        // Spot mid ~50000, perpetual mid ~53000 => 600 bps divergence > 25.
        provider.update_quote(quote("BTC", ExternalMarket::Spot, "49950", "50050", 1));
        provider.update_quote(quote("BTC", ExternalMarket::Perpetual, "53000", "53100", 1));
        let snapshot = provider.get_external_reference("BTC", Utc::now());
        assert!(
            snapshot
                .quality_reasons
                .contains(&"perpetual_spot_outlier".to_string())
        );
        assert_eq!(snapshot.effective_weight, Decimal::ZERO);
    }
}
