//! Deterministic online estimators for market-maker execution costs, port of
//! `src/hypeedge/strategy/market_maker/estimators.py`.

use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::Decimal;

/// An adverse-markout estimate.
#[derive(Debug, Clone, PartialEq)]
pub struct MarkoutEstimate {
    pub adverse_bps: Decimal,
    pub quality: String,
    pub sample_count: usize,
}

/// Estimate adverse selection from completed, mature maker-fill markouts.
/// A minimal markout observation (mirrors the fields the estimator uses).
pub struct MarkoutSample {
    pub strategy_id: String,
    pub symbol: String,
    pub maker: bool,
    pub horizon_ms: i64,
    pub horizon_ts: DateTime<Utc>,
    pub ts: DateTime<Utc>,
    pub fill_id: String,
    pub calculation_version: String,
    pub signed_markout_bps: Decimal,
}

type MarkoutKey = (String, String);
type SampleIdentity = (String, i64, String);

pub struct AdverseMarkoutEstimator {
    min_samples: usize,
    max_samples: usize,
    default: Decimal,
    samples: HashMap<MarkoutKey, VecDeque<(SampleIdentity, Decimal)>>,
}

impl AdverseMarkoutEstimator {
    pub fn new(
        min_samples: usize,
        max_samples: usize,
        conservative_default_bps: Decimal,
    ) -> Result<Self, String> {
        if min_samples == 0 || max_samples < min_samples {
            return Err("markout sample windows must satisfy 0 < min <= max".into());
        }
        if conservative_default_bps < Decimal::ZERO {
            return Err("conservative markout default cannot be negative".into());
        }
        Ok(Self {
            min_samples,
            max_samples,
            default: conservative_default_bps,
            samples: HashMap::new(),
        })
    }

    pub fn observe(&mut self, sample: &MarkoutSample, now: DateTime<Utc>) -> bool {
        if !sample.maker
            || sample.horizon_ms <= 0
            || sample.horizon_ts > now
            || sample.ts < sample.horizon_ts
        {
            return false;
        }
        let identity = (
            sample.fill_id.to_string(),
            sample.horizon_ms,
            sample.calculation_version.to_string(),
        );
        let key = (sample.strategy_id.to_string(), sample.symbol.to_string());
        let values = self
            .samples
            .entry(key)
            .or_insert_with(|| VecDeque::with_capacity(self.max_samples));
        if values.iter().any(|(existing, _)| *existing == identity) {
            return false;
        }
        // Bound the deque at max_samples.
        if values.len() >= self.max_samples {
            values.pop_front();
        }
        values.push_back((identity, (-sample.signed_markout_bps).max(Decimal::ZERO)));
        true
    }

    pub fn estimate(
        &self,
        strategy_id: &str,
        symbol: &str,
        min_samples: Option<usize>,
        conservative_default_bps: Option<Decimal>,
    ) -> Result<MarkoutEstimate, String> {
        let values = self
            .samples
            .get(&(strategy_id.to_string(), symbol.to_string()));
        let required = min_samples.unwrap_or(self.min_samples);
        if required == 0 {
            return Err("min_samples must be positive".into());
        }
        let default = conservative_default_bps.unwrap_or(self.default);
        if default < Decimal::ZERO {
            return Err("conservative markout default cannot be negative".into());
        }
        let len = values.map(|v| v.len()).unwrap_or(0);
        if len < required {
            return Ok(MarkoutEstimate {
                adverse_bps: default,
                quality: "conservative_default".into(),
                sample_count: len,
            });
        }
        let mut ordered: Vec<Decimal> = values.unwrap().iter().map(|(_, v)| *v).collect();
        ordered.sort();
        let index = (len - 1).min((len * 3) / 4);
        Ok(MarkoutEstimate {
            adverse_bps: ordered[index],
            quality: "mature".into(),
            sample_count: len,
        })
    }
}

/// EWMA of receipt-to-decision latency.
pub struct DecisionLatencyEstimator {
    alpha: Decimal,
    default: Decimal,
    ewma: Option<Decimal>,
    samples: usize,
    min_samples: usize,
}

impl DecisionLatencyEstimator {
    pub fn new(
        alpha: Decimal,
        conservative_default_seconds: Decimal,
        min_samples: usize,
    ) -> Result<Self, String> {
        if !(Decimal::ZERO < alpha && alpha <= Decimal::ONE) {
            return Err("latency alpha must be in (0, 1]".into());
        }
        if conservative_default_seconds < Decimal::ZERO {
            return Err("latency default cannot be negative".into());
        }
        if min_samples == 0 {
            return Err("latency min_samples must be positive".into());
        }
        Ok(Self {
            alpha,
            default: conservative_default_seconds,
            ewma: None,
            samples: 0,
            min_samples,
        })
    }

    pub fn observe(&mut self, seconds: Decimal) {
        if seconds < Decimal::ZERO {
            return;
        }
        self.ewma = Some(match self.ewma {
            None => seconds,
            Some(prev) => self.alpha * seconds + (Decimal::ONE - self.alpha) * prev,
        });
        self.samples += 1;
    }

    pub fn seconds(&self) -> Decimal {
        match self.ewma {
            Some(ewma) if self.samples >= self.min_samples => ewma,
            _ => self.default,
        }
    }

    pub fn quality(&self) -> String {
        if self.samples < self.min_samples {
            "conservative_default".into()
        } else {
            "observed".into()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markout_uses_conservative_default_below_min() {
        let est =
            AdverseMarkoutEstimator::new(5, 100, Decimal::from_str_lenient("1").unwrap()).unwrap();
        let r = est.estimate("s", "BTC", None, None).unwrap();
        assert_eq!(r.quality, "conservative_default");
        assert_eq!(r.adverse_bps.to_string(), "1");
        assert_eq!(r.sample_count, 0);
    }

    #[test]
    fn markout_mature_uses_upper_median() {
        let mut est =
            AdverseMarkoutEstimator::new(2, 100, Decimal::from_str_lenient("1").unwrap()).unwrap();
        let now = Utc::now();
        let mk = |i: i64, bps: &str| MarkoutSample {
            strategy_id: "s".to_string(),
            symbol: "BTC".to_string(),
            maker: true,
            horizon_ms: 1000,
            horizon_ts: now - chrono::Duration::seconds(2),
            ts: now - chrono::Duration::seconds(1),
            fill_id: format!("f{i}"),
            calculation_version: "v1".to_string(),
            signed_markout_bps: Decimal::from_str_lenient(bps).unwrap(),
        };
        // 4 samples: adverse = max(0, -signed). Use signed negative to record adverse.
        est.observe(&mk(1, "-2"), now);
        est.observe(&mk(2, "-5"), now);
        est.observe(&mk(3, "-1"), now);
        est.observe(&mk(4, "-8"), now);
        // ordered adverse: 1, 2, 5, 8; index = min(3, (4*3)//4=3) = 3 → 8.
        let r = est.estimate("s", "BTC", None, None).unwrap();
        assert_eq!(r.quality, "mature");
        assert_eq!(r.adverse_bps.to_string(), "8");
        assert_eq!(r.sample_count, 4);
    }

    #[test]
    fn markout_dedups_by_fill_id() {
        let mut est =
            AdverseMarkoutEstimator::new(1, 100, Decimal::from_str_lenient("1").unwrap()).unwrap();
        let now = Utc::now();
        let mk = |bps: &str| MarkoutSample {
            strategy_id: "s".to_string(),
            symbol: "BTC".to_string(),
            maker: true,
            horizon_ms: 1000,
            horizon_ts: now - chrono::Duration::seconds(2),
            ts: now - chrono::Duration::seconds(1),
            fill_id: "f1".to_string(),
            calculation_version: "v1".to_string(),
            signed_markout_bps: Decimal::from_str_lenient(bps).unwrap(),
        };
        assert!(est.observe(&mk("-3"), now));
        assert!(!est.observe(&mk("-3"), now), "same fill_id deduped");
        assert_eq!(
            est.estimate("s", "BTC", None, None).unwrap().sample_count,
            1
        );
    }

    #[test]
    fn latency_ewma_and_quality() {
        let mut est = DecisionLatencyEstimator::new(
            Decimal::from_str_lenient("0.2").unwrap(),
            Decimal::from_str_lenient("0.1").unwrap(),
            3,
        )
        .unwrap();
        assert_eq!(est.quality(), "conservative_default");
        assert_eq!(est.seconds().to_string(), "0.1");
        est.observe(Decimal::from_str_lenient("0.2").unwrap());
        est.observe(Decimal::from_str_lenient("0.4").unwrap());
        est.observe(Decimal::from_str_lenient("0.6").unwrap());
        assert_eq!(est.quality(), "observed");
        // ewma: 0.2 → 0.2; then 0.2*0.4+0.8*0.2=0.24; then 0.2*0.6+0.8*0.24=0.312.
        assert_eq!(est.seconds().to_string(), "0.312");
    }
}
