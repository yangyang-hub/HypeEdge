"""Performance metrics calculation for backtest results."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import structlog

from hypeedge.core.models import Fill
from hypeedge.core.types import Timestamp, Usd

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class PerformanceMetrics:
    """Aggregated performance statistics from a backtest run."""

    total_return_pct: float
    annualized_return_pct: float
    sharpe_ratio: float
    max_drawdown_pct: float
    win_rate: float
    profit_factor: float
    total_fees: Usd
    total_funding: Usd
    trade_count: int
    winning_trades: int
    losing_trades: int
    avg_win: Usd
    avg_loss: Usd
    largest_win: Usd
    largest_loss: Usd
    final_equity: Usd
    peak_equity: Usd
    duration_days: float

    def to_dict(self) -> dict[str, Any]:
        """Serialize all metrics to a dictionary."""
        return {
            "total_return_pct": round(self.total_return_pct, 4),
            "annualized_return_pct": round(self.annualized_return_pct, 4),
            "sharpe_ratio": round(self.sharpe_ratio, 4),
            "max_drawdown_pct": round(self.max_drawdown_pct, 4),
            "win_rate": round(self.win_rate, 4),
            "profit_factor": round(self.profit_factor, 4),
            "total_fees": round(float(self.total_fees), 2),
            "total_funding": round(float(self.total_funding), 2),
            "trade_count": self.trade_count,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "avg_win": round(float(self.avg_win), 2),
            "avg_loss": round(float(self.avg_loss), 2),
            "largest_win": round(float(self.largest_win), 2),
            "largest_loss": round(float(self.largest_loss), 2),
            "final_equity": round(float(self.final_equity), 2),
            "peak_equity": round(float(self.peak_equity), 2),
            "duration_days": round(self.duration_days, 2),
        }


@dataclass
class EquitySnapshot:
    """A single point on the equity curve."""

    timestamp: Timestamp
    equity: Usd


class MetricsCalculator:
    """Calculates performance metrics from fills and an equity curve.

    The equity curve is built externally (by BacktestEngine) and passed in
    as a list of (timestamp, equity) tuples.
    """

    def __init__(
        self,
        fills: list[Fill],
        equity_curve: list[tuple[Timestamp, Usd]],
        initial_capital: Usd,
        funding_total: Usd | None = None,
        trade_pnls: list[Usd] | None = None,
    ) -> None:
        self._fills = fills
        self._equity_curve = equity_curve
        self._initial_capital = initial_capital
        self._funding_total = funding_total or Usd(0.0)
        self._trade_pnls = trade_pnls or []

    def calculate(self) -> PerformanceMetrics:
        """Compute all performance metrics."""
        final_equity = self._final_equity
        peak_equity = self._peak_equity
        total_return = self._total_return_pct(final_equity)
        duration_days = self._duration_days
        annualized = self._annualized_return(total_return, duration_days)
        sharpe = self._sharpe_ratio()
        max_dd = self._max_drawdown_pct()
        fees = self._total_fees()
        trade_count = len(self._trade_pnls)
        wins, losses, win_rate, profit_factor, avg_win, avg_loss, largest_win, largest_loss = self._trade_stats()

        return PerformanceMetrics(
            total_return_pct=total_return,
            annualized_return_pct=annualized,
            sharpe_ratio=sharpe,
            max_drawdown_pct=max_dd,
            win_rate=win_rate,
            profit_factor=profit_factor,
            total_fees=fees,
            total_funding=self._funding_total,
            trade_count=trade_count,
            winning_trades=wins,
            losing_trades=losses,
            avg_win=avg_win,
            avg_loss=avg_loss,
            largest_win=largest_win,
            largest_loss=largest_loss,
            final_equity=final_equity,
            peak_equity=peak_equity,
            duration_days=duration_days,
        )

    @property
    def _final_equity(self) -> Usd:
        if not self._equity_curve:
            return self._initial_capital
        return self._equity_curve[-1][1]

    @property
    def _peak_equity(self) -> Usd:
        if not self._equity_curve:
            return self._initial_capital
        return Usd(max(eq for _, eq in self._equity_curve))

    def _total_return_pct(self, final_equity: Usd) -> float:
        if self._initial_capital <= 0:
            return 0.0
        return float((final_equity - self._initial_capital) / self._initial_capital)

    @property
    def _duration_days(self) -> float:
        if len(self._equity_curve) < 2:
            return 0.0
        first_ts = self._equity_curve[0][0]
        last_ts = self._equity_curve[-1][0]
        return (last_ts - first_ts) / (24 * 3600 * 1000)

    @staticmethod
    def _annualized_return(total_return: float, duration_days: float) -> float:
        if duration_days <= 0:
            return 0.0
        years = duration_days / 365.25
        if years <= 0:
            return 0.0
        # Compound: (1 + r)^(1/years) - 1
        if total_return <= -1.0:
            return -1.0
        # Guard against overflow when duration is very small
        if years < 1.0 / 8760:  # less than 1 hour
            return 0.0
        try:
            return float((1.0 + total_return) ** (1.0 / years) - 1.0)
        except OverflowError:
            return 0.0

    def _sharpe_ratio(self, risk_free_rate: float = 0.0) -> float:
        """Compute annualized Sharpe ratio from equity curve returns.

        Uses log returns between consecutive equity snapshots.
        """
        if len(self._equity_curve) < 2:
            return 0.0

        log_returns: list[float] = []
        for i in range(1, len(self._equity_curve)):
            prev_eq = float(self._equity_curve[i - 1][1])
            curr_eq = float(self._equity_curve[i][1])
            if prev_eq <= 0:
                continue
            log_returns.append(math.log(curr_eq / prev_eq))

        if not log_returns:
            return 0.0

        mean_ret = sum(log_returns) / len(log_returns)
        variance = sum((r - mean_ret) ** 2 for r in log_returns) / len(log_returns)
        std_ret = math.sqrt(variance) if variance > 0 else 0.0

        if std_ret == 0:
            return 0.0

        # Annualize assuming hourly snapshots (funding-based cadence)
        # We estimate snapshots_per_year from the data duration
        duration_days = self._duration_days
        if duration_days > 0:
            snapshots_per_year = len(log_returns) / (duration_days / 365.25)
        else:
            snapshots_per_year = len(log_returns) * 365.25  # fallback

        annualized_mean = mean_ret * snapshots_per_year
        annualized_std = std_ret * math.sqrt(snapshots_per_year)

        return (annualized_mean - risk_free_rate) / annualized_std

    def _max_drawdown_pct(self) -> float:
        """Peak-to-trough maximum drawdown as a fraction."""
        if not self._equity_curve:
            return 0.0

        peak = 0.0
        max_dd = 0.0
        for _, eq in self._equity_curve:
            eq_f = float(eq)
            if eq_f > peak:
                peak = eq_f
            if peak > 0:
                dd = (peak - eq_f) / peak
                if dd > max_dd:
                    max_dd = dd
        return max_dd

    def _total_fees(self) -> Usd:
        return Usd(sum(abs(float(f.fee)) for f in self._fills))

    def _trade_stats(self) -> tuple[int, int, float, float, Usd, Usd, Usd, Usd]:
        """Compute win/loss statistics from fills.

        Statistics are computed from realized round-trip PnL supplied by the
        portfolio ledger. Open entries and maker/taker fee signs are not trades.
        """
        if not self._trade_pnls:
            return 0, 0, 0.0, 0.0, Usd(0.0), Usd(0.0), Usd(0.0), Usd(0.0)

        wins = 0
        losses = 0
        total_win_amount = 0.0
        total_loss_amount = 0.0
        largest_win = 0.0
        largest_loss = 0.0

        for pnl in self._trade_pnls:
            value = float(pnl)
            if value > 0:
                wins += 1
                total_win_amount += value
                largest_win = max(largest_win, value)
            elif value < 0:
                losses += 1
                loss = abs(value)
                total_loss_amount += loss
                largest_loss = max(largest_loss, loss)

        total = wins + losses
        win_rate = wins / total if total > 0 else 0.0
        profit_factor = total_win_amount / total_loss_amount if total_loss_amount > 0 else float("inf")
        avg_win = Usd(total_win_amount / wins) if wins > 0 else Usd(0.0)
        avg_loss = Usd(total_loss_amount / losses) if losses > 0 else Usd(0.0)

        return (
            wins,
            losses,
            win_rate,
            profit_factor,
            avg_win,
            avg_loss,
            Usd(largest_win),
            Usd(largest_loss),
        )
