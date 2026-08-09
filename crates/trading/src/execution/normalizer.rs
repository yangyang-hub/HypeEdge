//! Exact instrument-aware order normalization, port of
//! `src/hypeedge/execution/normalizer.py`.
//!
//! Every trading entry point funnels through [`OrderNormalizer::normalize`]:
//! size is floored to the lot, price is quantized by the instrument's
//! decimal-place / five-significant-figure rules, and minimum notional and
//! post-only-crossing constraints are enforced before risk admission.

use std::sync::Arc;

use hypeedge_domain::decimal::{Decimal, Price, Size};
use hypeedge_domain::enums::{Side, TimeInForce};
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::models::OrderIntent;

/// Exchange rules required to construct an exact order.
#[derive(Debug, Clone)]
pub struct InstrumentSpec {
    pub symbol: String,
    pub tick_size: Decimal,
    pub lot_size: Decimal,
    pub min_size: Decimal,
    pub min_notional: Option<Decimal>,
    pub max_price_decimals: Option<u32>,
    pub max_significant_figures: u32,
}

impl InstrumentSpec {
    pub fn new(
        symbol: impl Into<String>,
        tick_size: Decimal,
        lot_size: Decimal,
        min_size: Decimal,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            tick_size,
            lot_size,
            min_size,
            min_notional: None,
            max_price_decimals: None,
            max_significant_figures: 5,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        for name in ["tick_size", "lot_size", "min_size"] {
            let v = match name {
                "tick_size" => &self.tick_size,
                "lot_size" => &self.lot_size,
                _ => &self.min_size,
            };
            if *v <= Decimal::ZERO {
                return Err(format!("{name} must be positive"));
            }
        }
        if self.min_notional.is_some_and(|m| m <= Decimal::ZERO) {
            return Err("min_notional must be positive when configured".into());
        }
        if self.max_significant_figures == 0 {
            return Err("max_significant_figures must be positive".into());
        }
        Ok(())
    }
}

/// Synchronous instrument-rule lookup used on the trading hot path.
pub trait InstrumentSpecProvider: Send + Sync {
    fn get(&self, symbol: &str) -> Option<InstrumentSpec>;
}

/// Quantize and validate order intents before risk admission and signing.
pub struct OrderNormalizer {
    instruments: Arc<dyn InstrumentSpecProvider>,
}

impl OrderNormalizer {
    pub fn new(instruments: Arc<dyn InstrumentSpecProvider>) -> Self {
        Self { instruments }
    }

    pub fn normalize(
        &self,
        intent: &OrderIntent,
        best_bid: Option<Decimal>,
        best_ask: Option<Decimal>,
    ) -> Result<OrderIntent, HypeEdgeError> {
        let spec = self
            .instruments
            .get(&intent.symbol)
            .ok_or_else(|| self.reject(&intent.symbol, "instrument_meta_unavailable", "Instrument metadata is unavailable"))?;

        let size = Self::floor_to_step(intent.size.inner(), spec.lot_size);
        if size < spec.min_size {
            return Err(self.reject(
                &intent.symbol,
                "size_below_minimum",
                &format!("Normalized size {size} is below {}", spec.min_size),
            ));
        }

        let price: Option<Decimal> = match intent.price {
            Some(p) => {
                let price = Self::normalize_price(p.inner(), &spec);
                if price <= Decimal::ZERO {
                    return Err(self.reject(
                        &intent.symbol,
                        "price_not_positive",
                        "Normalized price must be positive",
                    ));
                }
                Some(price)
            }
            None => None,
        };

        if let Some(min_notional) = spec.min_notional {
            let Some(price) = price else {
                return Err(self.reject(
                    &intent.symbol,
                    "reference_price_required",
                    "A reference price is required to validate minimum notional",
                ));
            };
            if size.mul(price) < min_notional {
                return Err(self.reject(
                    &intent.symbol,
                    "notional_below_minimum",
                    &format!("Normalized notional {} is below {min_notional}", size.mul(price)),
                ));
            }
        }

        let post_only = matches!(intent.time_in_force, TimeInForce::Alo | TimeInForce::Gtx);
        if let Some(price) = price.filter(|_| post_only) {
            if intent.side == Side::Buy && best_ask.is_some_and(|a| price >= a) {
                return Err(self.reject(
                    &intent.symbol,
                    "post_only_would_cross",
                    "Post-only buy would cross the best ask",
                ));
            }
            if intent.side == Side::Sell && best_bid.is_some_and(|b| price <= b) {
                return Err(self.reject(
                    &intent.symbol,
                    "post_only_would_cross",
                    "Post-only sell would cross the best bid",
                ));
            }
        }

        let mut normalized = intent.clone();
        normalized.size = Size::new(size);
        normalized.price = price.map(Price::new);
        Ok(normalized)
    }

    /// `(value / step)` floored to the integer, times `step` (ROUND_DOWN).
    pub fn floor_to_step(value: Decimal, step: Decimal) -> Decimal {
        value.floor_to_step(step)
    }

    /// Apply Hyperliquid's decimal-place and five-significant-figure rules.
    fn normalize_price(value: Decimal, spec: &InstrumentSpec) -> Decimal {
        let max_decimals = spec.max_price_decimals;
        let integral = value.as_tuple().1 >= 0;
        if max_decimals.is_none() || integral {
            return Self::floor_to_step(value, spec.tick_size);
        }
        let significant_figures = spec.max_significant_figures.max(1) as i32;
        let significant_decimals = (significant_figures - adjusted(&value) - 1).max(0);
        let allowed_decimals = (max_decimals.unwrap() as i32).min(significant_decimals).clamp(0, 18);
        // "1e-N" parses under the lenient mode (strict rejects exponents); a
        // sub-1e-18 dynamic step cannot be represented at scale 18 — the tick
        // size dominates at that point anyway.
        let step_str = format!("1e-{allowed_decimals}");
        let dynamic_step = Decimal::from_str_lenient(&step_str).unwrap_or(spec.tick_size);
        Self::floor_to_step(value, spec.tick_size.max(dynamic_step))
    }

    fn reject(&self, symbol: &str, reason: &str, message: &str) -> HypeEdgeError {
        HypeEdgeError::OrderNormalization {
            message: message.to_string(),
            symbol: symbol.to_string(),
            reason: reason.to_string(),
        }
    }
}

/// Python `Decimal.adjusted()`: the exponent of the most significant digit
/// (e.g. `65000 → 4`, `0.12345 → -1`, `0.00123 → -3`).
fn adjusted(d: &Decimal) -> i32 {
    if d.is_zero() {
        return 0;
    }
    let s = d.to_exact_string();
    let s = s.trim_start_matches(['-', '+']);
    match s.split_once('.') {
        Some((int_part, frac_part)) => {
            let int_nonzero = !int_part.is_empty() && int_part != "0";
            if int_nonzero {
                (int_part.trim_start_matches('0').len() as i32) - 1
            } else {
                let leading_zeros = frac_part.bytes().take_while(|b| *b == b'0').count() as i32;
                if leading_zeros == 0 {
                    -1
                } else {
                    -leading_zeros - 1
                }
            }
        }
        None => (s.len() as i32) - 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::enums::OrderType;

    fn spec(tick: &str, lot: &str, min: &str) -> InstrumentSpec {
        InstrumentSpec::new(
            "BTC",
            Decimal::from_str_strict(tick).unwrap(),
            Decimal::from_str_strict(lot).unwrap(),
            Decimal::from_str_strict(min).unwrap(),
        )
    }

    struct FixedInstruments(InstrumentSpec);
    impl InstrumentSpecProvider for FixedInstruments {
        fn get(&self, _symbol: &str) -> Option<InstrumentSpec> {
            Some(self.0.clone())
        }
    }

    fn normalizer(s: InstrumentSpec) -> OrderNormalizer {
        OrderNormalizer::new(Arc::new(FixedInstruments(s)))
    }

    fn intent(price: &str, size: &str) -> OrderIntent {
        OrderIntent {
            symbol: "BTC".into(),
            side: Side::Buy,
            size: Size::new(Decimal::from_str_strict(size).unwrap()),
            price: Some(Price::new(Decimal::from_str_strict(price).unwrap())),
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
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
    fn floors_size_to_lot() {
        let n = normalizer(spec("0.1", "0.001", "0.001"));
        let out = n.normalize(&intent("100", "1.2345"), None, None).unwrap();
        assert_eq!(out.size.inner().to_exact_string(), "1.234");
    }

    #[test]
    fn rejects_size_below_minimum() {
        let n = normalizer(spec("0.1", "0.001", "0.01"));
        let err = n.normalize(&intent("100", "0.001"), None, None).unwrap_err();
        assert!(matches!(
            err,
            HypeEdgeError::OrderNormalization { ref reason, .. } if reason == "size_below_minimum"
        ));
    }

    #[test]
    fn quantizes_price_to_tick() {
        let n = normalizer(spec("0.1", "0.001", "0.001"));
        let out = n.normalize(&intent("100.55", "1.0"), None, None).unwrap();
        assert_eq!(out.price.unwrap().inner().to_exact_string(), "100.5");
    }

    #[test]
    fn price_with_max_decimals_respects_significant_figures() {
        // max_price_decimals = 4; 65000 with 5 sig figs allows 0 decimals.
        let mut s = spec("0.01", "0.001", "0.001");
        s.max_price_decimals = Some(4);
        let n = normalizer(s);
        let out = n.normalize(&intent("65000.55", "1.0"), None, None).unwrap();
        assert_eq!(out.price.unwrap().inner().to_exact_string(), "65000");
    }

    #[test]
    fn low_price_keeps_significant_decimals() {
        // 0.12345 with 5 sig figs and max 5 decimals -> keep 5 decimals.
        let mut s = spec("0.00001", "0.00001", "0.00001");
        s.max_price_decimals = Some(5);
        let n = normalizer(s);
        let out = n.normalize(&intent("0.12345999", "10.0"), None, None).unwrap();
        assert_eq!(out.price.unwrap().inner().to_exact_string(), "0.12345");
    }

    #[test]
    fn rejects_non_positive_normalized_price() {
        let n = normalizer(spec("0.5", "0.001", "0.001"));
        let err = n.normalize(&intent("0.4", "1.0"), None, None).unwrap_err();
        assert!(matches!(
            err,
            HypeEdgeError::OrderNormalization { ref reason, .. } if reason == "price_not_positive"
        ));
    }

    #[test]
    fn rejects_missing_instrument() {
        struct NoInstruments;
        impl InstrumentSpecProvider for NoInstruments {
            fn get(&self, _symbol: &str) -> Option<InstrumentSpec> {
                None
            }
        }
        let n = OrderNormalizer::new(Arc::new(NoInstruments));
        let err = n.normalize(&intent("100", "1.0"), None, None).unwrap_err();
        assert!(matches!(
            err,
            HypeEdgeError::OrderNormalization { ref reason, .. } if reason == "instrument_meta_unavailable"
        ));
    }

    #[test]
    fn post_only_buy_rejects_when_crossing_ask() {
        let n = normalizer(spec("0.1", "0.001", "0.001"));
        let mut i = intent("100", "1.0");
        i.time_in_force = TimeInForce::Alo;
        let err = n.normalize(&i, None, Some(Decimal::from_str_strict("99.9").unwrap())).unwrap_err();
        assert!(matches!(
            err,
            HypeEdgeError::OrderNormalization { ref reason, .. } if reason == "post_only_would_cross"
        ));
    }

    #[test]
    fn adjusted_matches_python() {
        assert_eq!(adjusted(&Decimal::from_str_strict("65000").unwrap()), 4);
        assert_eq!(adjusted(&Decimal::from_str_strict("0.12345").unwrap()), -1);
        assert_eq!(adjusted(&Decimal::from_str_strict("0.00123").unwrap()), -3);
        assert_eq!(adjusted(&Decimal::from_str_strict("1").unwrap()), 0);
        assert_eq!(adjusted(&Decimal::from_str_strict("123.45").unwrap()), 2);
        assert_eq!(adjusted(&Decimal::ZERO), 0);
        assert_eq!(adjusted(&Decimal::from_str_strict("-0.5").unwrap()), -1);
    }
}
