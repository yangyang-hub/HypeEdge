//! Walk-forward analysis and anti-overfitting tools, port of
//! `src/hypeedge/backtest/walk_forward.py`.

/// Result of a single walk-forward window.
#[derive(Debug, Clone)]
pub struct WalkForwardWindow {
    pub window_index: usize,
    pub train_start_ms: i64,
    pub train_end_ms: i64,
    pub validate_start_ms: i64,
    pub validate_end_ms: i64,
    pub train_total_return_pct: f64,
    pub validate_total_return_pct: f64,
    pub validate_sharpe: f64,
    pub validate_max_drawdown_pct: f64,
    pub validate_trade_count: usize,
}

/// Aggregate result of walk-forward analysis.
#[derive(Debug, Clone)]
pub struct WalkForwardResult {
    pub windows: Vec<WalkForwardWindow>,
    pub aggregate_return_pct: f64,
    pub aggregate_sharpe: f64,
    pub aggregate_max_drawdown_pct: f64,
    pub total_validate_trades: usize,
    pub n_windows: usize,
}

impl WalkForwardResult {
    pub fn to_dict(&self) -> serde_json::Value {
        serde_json::json!({
            "n_windows": self.n_windows,
            "aggregate_return_pct": round4(self.aggregate_return_pct),
            "aggregate_sharpe": round4(self.aggregate_sharpe),
            "aggregate_max_drawdown_pct": round4(self.aggregate_max_drawdown_pct),
            "total_validate_trades": self.total_validate_trades,
            "windows": self.windows.iter().map(|w| serde_json::json!({
                "index": w.window_index,
                "train_return_pct": round4(w.train_total_return_pct),
                "validate_return_pct": round4(w.validate_total_return_pct),
                "validate_sharpe": round4(w.validate_sharpe),
                "validate_max_dd": round4(w.validate_max_drawdown_pct),
            })).collect::<Vec<_>>(),
        })
    }
}

/// Result of Monte Carlo bootstrap simulation.
#[derive(Debug, Clone)]
pub struct MonteCarloResult {
    pub n_simulations: usize,
    pub return_ci_lower: f64,
    pub return_ci_upper: f64,
    pub sharpe_ci_lower: f64,
    pub sharpe_ci_upper: f64,
    pub drawdown_ci_lower: f64,
    pub drawdown_ci_upper: f64,
    pub p_value_return: f64,
    pub observed_return: f64,
    pub observed_sharpe: f64,
}

impl MonteCarloResult {
    pub fn to_dict(&self) -> serde_json::Value {
        serde_json::json!({
            "n_simulations": self.n_simulations,
            "return_ci": [round4(self.return_ci_lower), round4(self.return_ci_upper)],
            "sharpe_ci": [round4(self.sharpe_ci_lower), round4(self.sharpe_ci_upper)],
            "drawdown_ci": [round4(self.drawdown_ci_lower), round4(self.drawdown_ci_upper)],
            "p_value_return": round4(self.p_value_return),
            "observed_return": round4(self.observed_return),
            "observed_sharpe": round4(self.observed_sharpe),
        })
    }
}

const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 24 * HOUR_MS;

/// The walk-forward engine. Pure window math — the backtest runs themselves
/// are delegated to a caller-supplied function so the Rust port stays
/// self-contained.
pub struct WalkForwardEngine;

impl WalkForwardEngine {
    /// Compute the rolling train/validate windows over the candle span.
    /// Returns per-window bounds; a caller runs each window's backtest.
    pub fn windows(
        candles: &[i64],
        train_days: i64,
        validate_days: i64,
        step_days: i64,
    ) -> Vec<(i64, i64, i64, i64)> {
        let (Some(first), Some(last)) = (candles.first(), candles.last()) else {
            return vec![];
        };
        let train_ms = train_days * DAY_MS;
        let validate_ms = validate_days * DAY_MS;
        let step_ms = step_days * DAY_MS;
        let last_ts = *last;
        let mut out = Vec::new();
        let mut window_start = *first;
        loop {
            let train_start = window_start;
            let train_end = train_start + train_ms;
            let validate_start = train_end;
            let validate_end = validate_start + validate_ms;
            if validate_end > last_ts {
                break;
            }
            out.push((train_start, train_end, validate_start, validate_end));
            window_start += step_ms;
        }
        out
    }
}

/// Monte Carlo bootstrap simulation.
pub fn run_monte_carlo(
    equity_curve: &[(i64, f64)],
    n_simulations: usize,
    confidence: f64,
    seed: u64,
) -> MonteCarloResult {
    if equity_curve.len() < 2 || n_simulations == 0 {
        // C1: zero simulations must return an empty result, not index an empty
        // vector.
        return MonteCarloResult {
            n_simulations: 0,
            return_ci_lower: 0.0,
            return_ci_upper: 0.0,
            sharpe_ci_lower: 0.0,
            sharpe_ci_upper: 0.0,
            drawdown_ci_lower: 0.0,
            drawdown_ci_upper: 0.0,
            p_value_return: 1.0,
            observed_return: 0.0,
            observed_sharpe: 0.0,
        };
    }

    let mut rng = rand::rng_from_seed(seed);
    let observed_returns = compute_returns(equity_curve);
    let observed_total_return = if equity_curve[0].1 > 0.0 {
        equity_curve[equity_curve.len() - 1].1 / equity_curve[0].1 - 1.0
    } else {
        0.0
    };
    let observed_sharpe = compute_sharpe(&observed_returns);

    let n = observed_returns.len();
    let mut sim_returns = Vec::with_capacity(n_simulations);
    let mut sim_sharpes = Vec::with_capacity(n_simulations);
    let mut sim_drawdowns = Vec::with_capacity(n_simulations);

    for _ in 0..n_simulations {
        let resampled: Vec<f64> = (0..n).map(|_| observed_returns[rng.next() % n]).collect();
        let mut sim_equity = equity_curve[0].1;
        let mut sim_curve = vec![(equity_curve[0].0, sim_equity)];
        for (i, ret) in resampled.iter().enumerate() {
            sim_equity *= 1.0 + ret;
            let ts = equity_curve[(i + 1).min(equity_curve.len() - 1)].0;
            sim_curve.push((ts, sim_equity));
        }
        let initial = sim_curve[0].1;
        let sim_total_return = if initial > 0.0 {
            sim_curve[sim_curve.len() - 1].1 / initial - 1.0
        } else {
            0.0
        };
        sim_returns.push(sim_total_return);
        sim_sharpes.push(compute_sharpe(&resampled));
        sim_drawdowns.push(compute_max_drawdown(&sim_curve));
    }

    let alpha = (1.0 - confidence) / 2.0;
    sim_returns.sort_by(|a, b| a.partial_cmp(b).unwrap());
    sim_sharpes.sort_by(|a, b| a.partial_cmp(b).unwrap());
    sim_drawdowns.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let lower_idx = ((alpha * n_simulations as f64) as usize).max(0);
    let upper_idx = (((1.0 - alpha) * n_simulations as f64) as usize).min(n_simulations.saturating_sub(1));
    let p_value = sim_returns.iter().filter(|r| **r >= observed_total_return).count() as f64 / n_simulations as f64;

    MonteCarloResult {
        n_simulations,
        return_ci_lower: sim_returns[lower_idx],
        return_ci_upper: sim_returns[upper_idx],
        sharpe_ci_lower: sim_sharpes[lower_idx],
        sharpe_ci_upper: sim_sharpes[upper_idx],
        drawdown_ci_lower: sim_drawdowns[lower_idx],
        drawdown_ci_upper: sim_drawdowns[upper_idx],
        p_value_return: p_value,
        observed_return: observed_total_return,
        observed_sharpe,
    }
}

/// Bonferroni multiple testing correction.
pub fn bonferroni_correction(p_value: f64, n_tests: usize) -> f64 {
    (p_value * n_tests as f64).min(1.0)
}

/// Simple returns from an equity curve.
pub fn compute_returns(equity_curve: &[(i64, f64)]) -> Vec<f64> {
    let mut returns = Vec::new();
    for i in 1..equity_curve.len() {
        let prev = equity_curve[i - 1].1;
        let curr = equity_curve[i].1;
        if prev > 0.0 {
            returns.push((curr - prev) / prev);
        }
    }
    returns
}

/// Annualized Sharpe from a return series (assumes hourly → ~8760/yr).
pub fn compute_sharpe(returns: &[f64]) -> f64 {
    if returns.is_empty() {
        return 0.0;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
    let std = if variance > 0.0 { variance.sqrt() } else { 0.0 };
    if std == 0.0 {
        return 0.0;
    }
    (mean / std) * 8760.0f64.sqrt()
}

/// Max drawdown from an equity curve.
pub fn compute_max_drawdown(equity_curve: &[(i64, f64)]) -> f64 {
    let mut peak = 0.0;
    let mut max_dd = 0.0;
    for (_, eq) in equity_curve {
        if *eq > peak {
            peak = *eq;
        }
        if peak > 0.0 {
            let dd = (peak - eq) / peak;
            if dd > max_dd {
                max_dd = dd;
            }
        }
    }
    max_dd
}

fn round4(v: f64) -> f64 {
    (v * 10_000.0).round() / 10_000.0
}

/// A tiny deterministic PRNG (xorshift64) so Monte Carlo is reproducible
/// without pulling in a rand dependency.
mod rand {
    pub struct Xorshift64(u64);
    impl Xorshift64 {
        pub fn next(&mut self) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 as usize
        }
    }
    pub fn rng_from_seed(seed: u64) -> Xorshift64 {
        Xorshift64(if seed == 0 { 0x9E3779B97F4A7C15 } else { seed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walk_forward_windows_slide() {
        // 100 hourly candles starting at 0 → span ~4.17 days.
        let candles: Vec<i64> = (0..100).map(|i| i * HOUR_MS).collect();
        let windows = WalkForwardEngine::windows(&candles, 1, 1, 1);
        assert!(!windows.is_empty());
        // Each window: train [start, start+1d), validate [start+1d, start+2d).
        let (ts, te, vs, ve) = windows[0];
        assert_eq!(te - ts, DAY_MS);
        assert_eq!(vs, te);
        assert_eq!(ve - vs, DAY_MS);
        // Windows step by 1 day.
        assert_eq!(windows[1].0 - ts, DAY_MS);
    }

    #[test]
    fn walk_forward_windows_respects_span() {
        // Too short: 1 day of candles can't fit train+validate of 1d each.
        let candles: Vec<i64> = (0..23).map(|i| i * HOUR_MS).collect();
        let windows = WalkForwardEngine::windows(&candles, 1, 1, 1);
        assert!(windows.is_empty());
    }

    #[test]
    fn bonferroni_correction_caps_at_one() {
        assert_eq!(bonferroni_correction(0.01, 5), 0.05);
        assert_eq!(bonferroni_correction(0.3, 10), 1.0);
    }

    #[test]
    fn sharpe_and_max_drawdown_basic() {
        let curve = vec![(0, 100.0), (1, 110.0), (2, 95.0), (3, 105.0)];
        let returns = compute_returns(&curve);
        assert_eq!(returns.len(), 3);
        assert!((returns[0] - 0.1).abs() < 1e-9);
        let sharpe = compute_sharpe(&returns);
        assert!(sharpe > 0.0);
        // Max drawdown: peak 110 → trough 95 → (110-95)/110 ≈ 0.136.
        let dd = compute_max_drawdown(&curve);
        assert!((dd - (110.0 - 95.0) / 110.0).abs() < 1e-9);
    }

    #[test]
    fn monte_carlo_runs_deterministically() {
        let curve: Vec<(i64, f64)> = (0..50).map(|i| (i, 100.0 * (1.0 + i as f64 * 0.002))).collect();
        let r1 = run_monte_carlo(&curve, 200, 0.95, 42);
        let r2 = run_monte_carlo(&curve, 200, 0.95, 42);
        assert_eq!(r1.return_ci_lower, r2.return_ci_lower);
        assert_eq!(r1.return_ci_upper, r2.return_ci_upper);
        assert_eq!(r1.p_value_return, r2.p_value_return);
        assert_eq!(r1.n_simulations, 200);
        assert!(r1.observed_return > 0.0);
    }

    #[test]
    fn monte_carlo_zero_simulations_does_not_panic() {
        // C1 regression: 0 simulations must return an empty result, not index
        // into an empty vector.
        let curve: Vec<(i64, f64)> = vec![(0, 100.0), (1, 101.0)];
        let r = run_monte_carlo(&curve, 0, 0.95, 1);
        assert_eq!(r.n_simulations, 0);
        assert_eq!(r.return_ci_lower, 0.0);
        assert_eq!(r.p_value_return, 1.0);
    }
}
