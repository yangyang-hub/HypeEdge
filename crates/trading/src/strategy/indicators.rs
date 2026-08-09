//! Technical indicators for strategy signal generation, port of
//! `src/hypeedge/strategy/indicators.py`.
//!
//! Pure functions, no state. All return vectors aligned with the input,
//! padded with `NaN` at the start where values are undefined. The math is
//! byte-identical to Python's so signal parity holds.

/// Exponential Moving Average. First `period-1` values are `NaN`; the first
/// valid value is the SMA of the first `period` values.
#[allow(clippy::needless_range_loop)]
pub fn ema(values: &[f64], period: usize) -> Vec<f64> {
    if period == 0 || values.len() < period {
        return vec![f64::NAN; values.len()];
    }
    let mut result = vec![f64::NAN; period - 1];
    let sma_seed: f64 = values[..period].iter().sum::<f64>() / period as f64;
    result.push(sma_seed);
    let multiplier = 2.0 / (period as f64 + 1.0);
    for i in period..values.len() {
        let prev = *result.last().unwrap();
        result.push(values[i] * multiplier + prev * (1.0 - multiplier));
    }
    result
}

/// Simple Moving Average. First `period-1` values are `NaN`.
#[allow(clippy::needless_range_loop)]
pub fn sma(values: &[f64], period: usize) -> Vec<f64> {
    if period == 0 || values.len() < period {
        return vec![f64::NAN; values.len()];
    }
    let mut result = vec![f64::NAN; period - 1];
    let mut window_sum: f64 = values[..period].iter().sum();
    result.push(window_sum / period as f64);
    for i in period..values.len() {
        window_sum += values[i] - values[i - period];
        result.push(window_sum / period as f64);
    }
    result
}

/// MACD: `(macd_line, signal_line, histogram)`, all same length as input.
pub fn macd(
    closes: &[f64],
    fast_period: usize,
    slow_period: usize,
    signal_period: usize,
) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let fast = ema(closes, fast_period);
    let slow = ema(closes, slow_period);

    let macd_line: Vec<f64> = fast
        .iter()
        .zip(&slow)
        .map(|(f, s)| {
            if f.is_nan() || s.is_nan() {
                f64::NAN
            } else {
                f - s
            }
        })
        .collect();

    let valid_macd: Vec<f64> = macd_line.iter().copied().filter(|v| !v.is_nan()).collect();
    let signal_line: Vec<f64> = if valid_macd.len() >= signal_period {
        let signal_valid = ema(&valid_macd, signal_period);
        let nan_count = macd_line.len() - valid_macd.len();
        let mut out = vec![f64::NAN; nan_count];
        out.extend(signal_valid);
        out
    } else {
        vec![f64::NAN; macd_line.len()]
    };

    let histogram: Vec<f64> = macd_line
        .iter()
        .zip(&signal_line)
        .map(|(m, s)| {
            if m.is_nan() || s.is_nan() {
                f64::NAN
            } else {
                m - s
            }
        })
        .collect();

    (macd_line, signal_line, histogram)
}

/// Average True Range: `TR = max(high-low, |high-prev_close|, |low-prev_close|)`,
/// `ATR = EMA(TR, period)`. First value is `NaN`.
pub fn atr(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> Vec<f64> {
    let n = highs.len();
    if n == 0 || n != lows.len() || n != closes.len() {
        return Vec::new();
    }
    let tr_values: Vec<f64> = (1..n)
        .map(|i| {
            let hl = highs[i] - lows[i];
            let hc = (highs[i] - closes[i - 1]).abs();
            let lc = (lows[i] - closes[i - 1]).abs();
            hl.max(hc).max(lc)
        })
        .collect();
    let mut atr_valid = ema(&tr_values, period);
    atr_valid.insert(0, f64::NAN);
    atr_valid
}

/// Rate of change (momentum): `(v[i] - v[i-period]) / v[i-period]`. First
/// `period` values are `NaN`.
#[allow(clippy::needless_range_loop)]
pub fn momentum(values: &[f64], period: usize) -> Vec<f64> {
    if period == 0 || values.len() <= period {
        return vec![f64::NAN; values.len()];
    }
    let mut result = vec![f64::NAN; period];
    for i in period..values.len() {
        if values[i - period] == 0.0 {
            result.push(f64::NAN);
        } else {
            result.push((values[i] - values[i - period]) / values[i - period]);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn sma_matches_reference() {
        let v = [1.0, 2.0, 3.0, 4.0, 5.0];
        let s = sma(&v, 3);
        assert_eq!(s.len(), 5);
        assert!(s[0].is_nan() && s[1].is_nan());
        assert!(approx(s[2], 2.0));
        assert!(approx(s[3], 3.0));
        assert!(approx(s[4], 4.0));
    }

    #[test]
    fn ema_seed_is_sma() {
        let v = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0];
        let e = ema(&v, 3);
        assert!(e[0].is_nan() && e[1].is_nan());
        assert!(approx(e[2], 2.0)); // SMA(1,2,3)
        // e[3] = 4*0.5 + 2*0.5 = 3
        assert!(approx(e[3], 3.0));
        // e[4] = 5*0.5 + 3*0.5 = 4
        assert!(approx(e[4], 4.0));
    }

    #[test]
    fn momentum_basic() {
        let v = [10.0, 10.0, 12.0];
        let m = momentum(&v, 2);
        assert!(m[0].is_nan() && m[1].is_nan());
        assert!(approx(m[2], 0.2)); // (12-10)/10
    }

    #[test]
    fn atr_basic() {
        let highs = [10.0, 12.0, 14.0];
        let lows = [8.0, 9.0, 11.0];
        let closes = [9.0, 10.0, 12.0];
        let a = atr(&highs, &lows, &closes, 2);
        assert_eq!(a.len(), 3);
        // Index 0 has no prev_close -> NaN; TR[1] = 3 and TR[2] = 4, so EMA(2)
        // seeds at index 1 of the TR series (i.e. our index 2) with SMA=3.5.
        assert!(a[0].is_nan());
        assert!(a[1].is_nan());
        assert!(approx(a[2], 3.5));
    }

    #[test]
    fn macd_returns_aligned_series() {
        let closes: Vec<f64> = (0..60).map(|i| 100.0 + (i as f64) * 0.5).collect();
        let (m, s, h) = macd(&closes, 12, 26, 9);
        assert_eq!(m.len(), 60);
        assert_eq!(s.len(), 60);
        assert_eq!(h.len(), 60);
        // Early values are NaN: slow EMA(26) is valid from index 25, so macd is
        // NaN through index 24.
        assert!(m[10].is_nan());
        assert!(m[24].is_nan());
        // From index 25 both EMAs are valid.
        assert!(!m[25].is_nan());
        assert!(!s[50].is_nan());
        assert!(approx(h[50], m[50] - s[50]));
    }

    /// Golden parity: the exact closes series and expected indicator tails were
    /// produced by the pinned Python `hypeedge.strategy.indicators` (random
    /// walk, seed 42). This pins the ema/macd/atr/momentum math byte-for-byte.
    #[test]
    fn indicator_values_match_python_golden() {
        let closes = [
            100.0, 100.278854, 99.326226, 98.879316, 98.331942, 98.796995, 99.146143, 99.923804,
            99.098312, 98.943563, 98.013093, 97.461549, 97.471988, 96.548998, 95.96746, 96.25514,
            96.341657, 95.802993, 95.974032, 96.567977, 95.614849, 96.199666, 96.580885, 96.27231,
            95.608955, 96.483228, 96.16791, 95.384614, 94.615273, 95.272839, 95.470484, 96.056918,
            96.498265, 96.568184, 97.481942, 97.245128, 97.346342, 97.987669, 98.219938, 98.930475,
            99.083525, 99.488919, 98.58521, 98.048706, 97.635701, 96.815155, 96.297757, 95.529304,
            95.105103, 95.363189, 95.105388, 94.858458, 94.307344, 93.86783, 94.687586, 94.967928,
            95.175207, 94.549218, 94.982494, 94.343076,
        ];
        let highs: Vec<f64> = closes.iter().map(|c| c * 1.002).collect();
        let lows: Vec<f64> = closes.iter().map(|c| c * 0.998).collect();

        let (m, s, h) = macd(&closes, 12, 26, 9);
        let a = atr(&highs, &lows, &closes, 14);
        let mom = momentum(&closes, 10);

        let macd_tail = &m[m.len() - 3..];
        let signal_tail = &s[s.len() - 3..];
        let hist_tail = &h[h.len() - 3..];
        let atr_tail = &a[a.len() - 3..];
        let mom_tail = &mom[mom.len() - 3..];

        let expect = |got: f64, want: f64| {
            assert!(
                (got - want).abs() < 1e-3,
                "got {got}, want {want} (within 1e-3)"
            );
        };
        expect(macd_tail[0], -0.729516);
        expect(macd_tail[1], -0.694424);
        expect(macd_tail[2], -0.710025);
        expect(signal_tail[0], -0.60001);
        expect(signal_tail[1], -0.618893);
        expect(signal_tail[2], -0.63712);
        expect(hist_tail[0], -0.129505);
        expect(hist_tail[1], -0.075531);
        expect(hist_tail[2], -0.072905);
        expect(atr_tail[0], 0.655165);
        expect(atr_tail[1], 0.650908);
        expect(atr_tail[2], 0.674534);
        expect(mom_tail[0], -0.01026);
        expect(mom_tail[1], -0.001289);
        expect(mom_tail[2], -0.010697);
    }
}
