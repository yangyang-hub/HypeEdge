//! Performance metrics calculation for backtest results, port of
//! `src/hypeedge/backtest/metrics.py`.

use hypeedge_domain::decimal::{Decimal, Usd};

/// Aggregated performance statistics.
#[derive(Debug, Clone, PartialEq)]
pub struct PerformanceMetrics {
    pub total_return_pct: f64,
    pub annualized_return_pct: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown_pct: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub total_fees: Usd,
    pub total_funding: Usd,
    pub trade_count: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub avg_win: Usd,
    pub avg_loss: Usd,
    pub largest_win: Usd,
    pub largest_loss: Usd,
    pub final_equity: Usd,
    pub peak_equity: Usd,
    pub duration_days: f64,
}

impl PerformanceMetrics {
    /// Serialize all metrics (mirrors `to_dict` rounding).
    pub fn to_dict(&self) -> serde_json::Value {
        serde_json::json!({
            "total_return_pct": round4(self.total_return_pct),
            "annualized_return_pct": round4(self.annualized_return_pct),
            "sharpe_ratio": round4(self.sharpe_ratio),
            "max_drawdown_pct": round4(self.max_drawdown_pct),
            "win_rate": round4(self.win_rate),
            "profit_factor": round4(self.profit_factor),
            "total_fees": round2(self.total_fees),
            "total_funding": round2(self.total_funding),
            "trade_count": self.trade_count,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "avg_win": round2(self.avg_win),
            "avg_loss": round2(self.avg_loss),
            "largest_win": round2(self.largest_win),
            "largest_loss": round2(self.largest_loss),
            "final_equity": round2(self.final_equity),
            "peak_equity": round2(self.peak_equity),
            "duration_days": round2f(self.duration_days),
        })
    }
}

/// Calculates performance metrics from an equity curve and realized PnLs.
pub struct MetricsCalculator {
    equity_curve: Vec<(i64, Usd)>,
    initial_capital: Usd,
    funding_total: Usd,
    fees_total: Usd,
    trade_pnls: Vec<Usd>,
}

impl MetricsCalculator {
    pub fn new(
        equity_curve: Vec<(i64, Usd)>,
        initial_capital: Usd,
        funding_total: Usd,
        fees_total: Usd,
        trade_pnls: Vec<Usd>,
    ) -> Self {
        Self {
            equity_curve,
            initial_capital,
            funding_total,
            fees_total,
            trade_pnls,
        }
    }

    pub fn calculate(&self) -> PerformanceMetrics {
        let final_equity = self.final_equity();
        let peak_equity = self.peak_equity();
        let total_return = self.total_return_pct(final_equity);
        let duration_days = self.duration_days();
        let annualized = Self::annualized_return(total_return, duration_days);
        let sharpe = self.sharpe_ratio(0.0);
        let max_dd = self.max_drawdown_pct();
        let (wins, losses, win_rate, profit_factor, avg_win, avg_loss, largest_win, largest_loss) =
            self.trade_stats();

        PerformanceMetrics {
            total_return_pct: total_return,
            annualized_return_pct: annualized,
            sharpe_ratio: sharpe,
            max_drawdown_pct: max_dd,
            win_rate,
            profit_factor,
            total_fees: self.total_fees(),
            total_funding: self.funding_total,
            trade_count: self.trade_pnls.len(),
            winning_trades: wins,
            losing_trades: losses,
            avg_win,
            avg_loss,
            largest_win,
            largest_loss,
            final_equity,
            peak_equity,
            duration_days,
        }
    }

    fn final_equity(&self) -> Usd {
        self.equity_curve
            .last()
            .map(|(_, eq)| *eq)
            .unwrap_or(self.initial_capital)
    }

    fn peak_equity(&self) -> Usd {
        let max = self
            .equity_curve
            .iter()
            .map(|(_, eq)| eq.inner())
            .fold(Decimal::ZERO, |acc, v| if v > acc { v } else { acc });
        if self.equity_curve.is_empty() {
            self.initial_capital
        } else {
            Usd::new(max)
        }
    }

    fn total_return_pct(&self, final_equity: Usd) -> f64 {
        if self.initial_capital.inner() <= Decimal::ZERO {
            return 0.0;
        }
        to_f64((final_equity.inner() - self.initial_capital.inner()) / self.initial_capital.inner())
    }

    fn duration_days(&self) -> f64 {
        if self.equity_curve.len() < 2 {
            return 0.0;
        }
        let first_ts = self.equity_curve[0].0;
        let last_ts = self.equity_curve[self.equity_curve.len() - 1].0;
        (last_ts - first_ts) as f64 / (24.0 * 3600.0 * 1000.0)
    }

    fn annualized_return(total_return: f64, duration_days: f64) -> f64 {
        if duration_days <= 0.0 {
            return 0.0;
        }
        let years = duration_days / 365.25;
        if years <= 0.0 {
            return 0.0;
        }
        if total_return <= -1.0 {
            return -1.0;
        }
        if years < 1.0 / 8760.0 {
            return 0.0;
        }
        let result = (1.0 + total_return).powf(1.0 / years) - 1.0;
        if result.is_finite() { result } else { 0.0 }
    }

    fn sharpe_ratio(&self, risk_free_rate: f64) -> f64 {
        if self.equity_curve.len() < 2 {
            return 0.0;
        }
        let mut log_returns = Vec::new();
        for i in 1..self.equity_curve.len() {
            let prev_eq = to_f64(self.equity_curve[i - 1].1.inner());
            let curr_eq = to_f64(self.equity_curve[i].1.inner());
            if prev_eq <= 0.0 {
                continue;
            }
            log_returns.push((curr_eq / prev_eq).ln());
        }
        if log_returns.is_empty() {
            return 0.0;
        }
        let mean_ret = log_returns.iter().sum::<f64>() / log_returns.len() as f64;
        let variance = log_returns
            .iter()
            .map(|r| (r - mean_ret).powi(2))
            .sum::<f64>()
            / log_returns.len() as f64;
        let std_ret = if variance > 0.0 { variance.sqrt() } else { 0.0 };
        if std_ret == 0.0 {
            return 0.0;
        }
        let duration_days = self.duration_days();
        let snapshots_per_year = if duration_days > 0.0 {
            log_returns.len() as f64 / (duration_days / 365.25)
        } else {
            log_returns.len() as f64 * 365.25
        };
        let annualized_mean = mean_ret * snapshots_per_year;
        let annualized_std = std_ret * snapshots_per_year.sqrt();
        (annualized_mean - risk_free_rate) / annualized_std
    }

    fn max_drawdown_pct(&self) -> f64 {
        if self.equity_curve.is_empty() {
            return 0.0;
        }
        let mut peak = 0.0;
        let mut max_dd = 0.0;
        for (_, eq) in &self.equity_curve {
            let eq_f = to_f64(eq.inner());
            if eq_f > peak {
                peak = eq_f;
            }
            if peak > 0.0 {
                let dd = (peak - eq_f) / peak;
                if dd > max_dd {
                    max_dd = dd;
                }
            }
        }
        max_dd
    }

    /// Total fees paid over the run (accumulated by the engine from fill fees).
    fn total_fees(&self) -> Usd {
        self.fees_total
    }

    fn trade_stats(&self) -> (usize, usize, f64, f64, Usd, Usd, Usd, Usd) {
        if self.trade_pnls.is_empty() {
            return (0, 0, 0.0, 0.0, Usd::ZERO, Usd::ZERO, Usd::ZERO, Usd::ZERO);
        }
        let mut wins = 0usize;
        let mut losses = 0usize;
        let mut total_win = 0.0;
        let mut total_loss = 0.0;
        let mut largest_win: f64 = 0.0;
        let mut largest_loss: f64 = 0.0;
        for pnl in &self.trade_pnls {
            let value = to_f64(pnl.inner());
            if value > 0.0 {
                wins += 1;
                total_win += value;
                if value > largest_win {
                    largest_win = value;
                }
            } else if value < 0.0 {
                losses += 1;
                let loss = value.abs();
                total_loss += loss;
                if loss > largest_loss {
                    largest_loss = loss;
                }
            }
        }
        let total = wins + losses;
        let win_rate = if total > 0 {
            wins as f64 / total as f64
        } else {
            0.0
        };
        let profit_factor = if total_loss > 0.0 {
            total_win / total_loss
        } else {
            f64::INFINITY
        };
        let avg_win = if wins > 0 {
            Usd::new(Decimal::from_f64(total_win / wins as f64).unwrap())
        } else {
            Usd::ZERO
        };
        let avg_loss = if losses > 0 {
            Usd::new(Decimal::from_f64(total_loss / losses as f64).unwrap())
        } else {
            Usd::ZERO
        };
        (
            wins,
            losses,
            win_rate,
            profit_factor,
            avg_win,
            avg_loss,
            Usd::new(Decimal::from_f64(largest_win).unwrap()),
            Usd::new(Decimal::from_f64(largest_loss).unwrap()),
        )
    }
}

fn round4(v: f64) -> f64 {
    (v * 10_000.0).round() / 10_000.0
}

fn round2(v: Usd) -> f64 {
    (to_f64(v.inner()) * 100.0).round() / 100.0
}

fn round2f(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn to_f64(d: Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_from_flat_equity() {
        let capital = Usd::new(Decimal::from_str_lenient("10000").unwrap());
        let curve = vec![
            (0, capital),
            (
                3_600_000,
                Usd::new(Decimal::from_str_lenient("10100").unwrap()),
            ),
        ];
        let calc = MetricsCalculator::new(curve, capital, Usd::ZERO, Usd::ZERO, vec![]);
        let m = calc.calculate();
        assert_eq!(m.trade_count, 0);
        assert_eq!(m.win_rate, 0.0);
        // total_return = (10100-10000)/10000 = 0.01.
        assert!((m.total_return_pct - 0.01).abs() < 1e-9);
        assert!(m.max_drawdown_pct >= 0.0);
        assert_eq!(m.final_equity.to_string(), "10100");
    }

    #[test]
    fn trade_stats_win_loss() {
        let capital = Usd::new(Decimal::from_str_lenient("10000").unwrap());
        let curve = vec![(0, capital)];
        let calc = MetricsCalculator::new(
            curve,
            capital,
            Usd::ZERO,
            Usd::ZERO,
            vec![
                Usd::new(Decimal::from_str_lenient("50").unwrap()),
                Usd::new(Decimal::from_str_lenient("-20").unwrap()),
                Usd::new(Decimal::from_str_lenient("30").unwrap()),
            ],
        );
        let m = calc.calculate();
        assert_eq!(m.trade_count, 3);
        assert_eq!(m.winning_trades, 2);
        assert_eq!(m.losing_trades, 1);
        assert!((m.win_rate - 2.0 / 3.0).abs() < 1e-9);
        assert_eq!(m.avg_win.to_string(), "40");
        assert_eq!(m.avg_loss.to_string(), "20");
        assert_eq!(m.largest_win.to_string(), "50");
        assert_eq!(m.largest_loss.to_string(), "20");
        // profit_factor = 80/20 = 4.
        assert!((m.profit_factor - 4.0).abs() < 1e-9);
    }

    /// Golden parity: the exact metrics were produced by the pinned Python
    /// `hypeedge.backtest.metrics.MetricsCalculator` for identical inputs.
    #[test]
    fn metrics_match_python_golden() {
        let capital = Usd::new(Decimal::from_str_lenient("10000").unwrap());
        let curve = vec![
            (0, Usd::new(Decimal::from_str_lenient("10000").unwrap())),
            (
                3_600_000,
                Usd::new(Decimal::from_str_lenient("10100").unwrap()),
            ),
            (
                7_200_000,
                Usd::new(Decimal::from_str_lenient("9900").unwrap()),
            ),
            (
                10_800_000,
                Usd::new(Decimal::from_str_lenient("10200").unwrap()),
            ),
        ];
        let pnls = vec![
            Usd::new(Decimal::from_str_lenient("50").unwrap()),
            Usd::new(Decimal::from_str_lenient("-20").unwrap()),
            Usd::new(Decimal::from_str_lenient("30").unwrap()),
            Usd::new(Decimal::from_str_lenient("-100").unwrap()),
            Usd::new(Decimal::from_str_lenient("200").unwrap()),
        ];
        let calc = MetricsCalculator::new(
            curve,
            capital,
            Usd::new(Decimal::from_str_lenient("5").unwrap()),
            Usd::ZERO,
            pnls,
        );
        let m = calc.calculate();

        assert!((m.total_return_pct - 0.02).abs() < 1e-9);
        assert!((m.sharpe_ratio - 30.162012).abs() < 1e-4);
        assert!((m.max_drawdown_pct - 0.019802).abs() < 1e-5);
        assert!((m.win_rate - 0.6).abs() < 1e-9);
        assert!((m.profit_factor - 2.333333).abs() < 1e-5);
        assert_eq!(m.total_funding.to_string(), "5");
        assert_eq!(m.trade_count, 5);
        assert_eq!(m.winning_trades, 3);
        assert_eq!(m.losing_trades, 2);
        assert_eq!(m.avg_win.to_string(), "93.33333333333333");
        assert_eq!(m.avg_loss.to_string(), "60");
        assert_eq!(m.largest_win.to_string(), "200");
        assert_eq!(m.largest_loss.to_string(), "100");
        assert_eq!(m.final_equity.to_string(), "10200");
        assert_eq!(m.peak_equity.to_string(), "10200");
        assert!((m.duration_days - 0.125).abs() < 1e-9);
    }

    #[test]
    fn total_fees_surfaced_and_no_losses_profit_factor_is_null() {
        // B17 regression: the report must carry the real fee total (was always
        // 0), and a no-loss run must serialize profit_factor as an explicit
        // null rather than an opaque non-finite float.
        let capital = Usd::new(Decimal::from_str_lenient("10000").unwrap());
        let curve = vec![
            (0, capital),
            (1, Usd::new(Decimal::from_str_lenient("10200").unwrap())),
        ];
        let pnls = vec![
            Usd::new(Decimal::from_str_lenient("100").unwrap()),
            Usd::new(Decimal::from_str_lenient("100").unwrap()),
        ];
        let calc = MetricsCalculator::new(
            curve,
            capital,
            Usd::ZERO,
            Usd::new(Decimal::from_str_lenient("12.5").unwrap()),
            pnls,
        );
        let m = calc.calculate();
        assert_eq!(
            m.total_fees.to_string(),
            "12.5",
            "fees must be surfaced (B17)"
        );
        assert_eq!(m.profit_factor, f64::INFINITY);
        let dict = m.to_dict();
        assert_eq!(
            dict["profit_factor"],
            serde_json::Value::Null,
            "no-loss profit_factor must serialize as explicit null (B17)"
        );
    }
}
