"""Walk-forward analysis and anti-overfitting tools (design doc §6.1).

Implements:
- Walk-forward analysis with rolling train/validation windows
- Monte Carlo bootstrap simulation for statistical significance
- Bonferroni multiple testing correction
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Any

import structlog

from hypeedge.backtest.broker import FeeConfig, SlippageConfig, SlippageMode
from hypeedge.backtest.engine import BacktestEngine, BacktestResult, StrategyFactory
from hypeedge.core.models import Candle, FundingRate
from hypeedge.core.types import Usd

logger = structlog.get_logger(__name__)

_HOUR_MS = 3_600_000
_DAY_MS = 24 * _HOUR_MS


@dataclass(frozen=True)
class WalkForwardWindow:
    """Result of a single walk-forward window."""

    window_index: int
    train_start_ms: int
    train_end_ms: int
    validate_start_ms: int
    validate_end_ms: int
    train_result: BacktestResult
    validate_result: BacktestResult


@dataclass(frozen=True)
class WalkForwardResult:
    """Aggregate result of walk-forward analysis."""

    windows: list[WalkForwardWindow]
    aggregate_return_pct: float
    aggregate_sharpe: float
    aggregate_max_drawdown_pct: float
    total_validate_trades: int
    n_windows: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "n_windows": self.n_windows,
            "aggregate_return_pct": round(self.aggregate_return_pct, 4),
            "aggregate_sharpe": round(self.aggregate_sharpe, 4),
            "aggregate_max_drawdown_pct": round(self.aggregate_max_drawdown_pct, 4),
            "total_validate_trades": self.total_validate_trades,
            "windows": [
                {
                    "index": w.window_index,
                    "train_return_pct": round(w.train_result.metrics.total_return_pct, 4),
                    "validate_return_pct": round(w.validate_result.metrics.total_return_pct, 4),
                    "validate_sharpe": round(w.validate_result.metrics.sharpe_ratio, 4),
                    "validate_max_dd": round(w.validate_result.metrics.max_drawdown_pct, 4),
                }
                for w in self.windows
            ],
        }


@dataclass(frozen=True)
class MonteCarloResult:
    """Result of Monte Carlo bootstrap simulation."""

    n_simulations: int
    return_ci_lower: float
    return_ci_upper: float
    sharpe_ci_lower: float
    sharpe_ci_upper: float
    drawdown_ci_lower: float
    drawdown_ci_upper: float
    p_value_return: float  # Probability of achieving observed return by chance
    observed_return: float
    observed_sharpe: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "n_simulations": self.n_simulations,
            "return_ci": [round(self.return_ci_lower, 4), round(self.return_ci_upper, 4)],
            "sharpe_ci": [round(self.sharpe_ci_lower, 4), round(self.sharpe_ci_upper, 4)],
            "drawdown_ci": [round(self.drawdown_ci_lower, 4), round(self.drawdown_ci_upper, 4)],
            "p_value_return": round(self.p_value_return, 4),
            "observed_return": round(self.observed_return, 4),
            "observed_sharpe": round(self.observed_sharpe, 4),
        }


class WalkForwardEngine:
    """Walk-forward analysis engine (design doc §6.1).

    Splits data into rolling train/validation windows. Parameters are
    optimized on training segments and evaluated on validation segments.
    This better simulates real parameter decay than a single train/test split.
    """

    def __init__(
        self,
        engine: BacktestEngine | None = None,
        fee_config: FeeConfig | None = None,
        slippage_config: SlippageConfig | None = None,
    ) -> None:
        self._engine = engine or BacktestEngine(fee_config, slippage_config)

    async def run_walk_forward(
        self,
        candles: list[Candle],
        funding_rates: list[FundingRate] | None,
        strategy_factory: StrategyFactory,
        train_days: int = 60,
        validate_days: int = 30,
        step_days: int = 30,
        initial_capital: Usd | None = None,
        slippage_mode: SlippageMode = SlippageMode.PESSIMISTIC,
    ) -> WalkForwardResult:
        """Run walk-forward analysis with rolling windows.

        Args:
            candles: Historical candle data sorted by timestamp.
            funding_rates: Historical funding data (optional).
            strategy_factory: Creates strategy instances for each window run.
            train_days: Training window length in days.
            validate_days: Validation window length in days.
            step_days: Window slide step in days.
            initial_capital: Starting capital per window.
            slippage_mode: Fill simulation mode.

        Returns:
            WalkForwardResult with per-window and aggregate metrics.
        """
        if initial_capital is None:
            initial_capital = Usd(10_000.0)

        if not candles:
            logger.warning("walk_forward_empty_candles")
            return WalkForwardResult(
                windows=[],
                aggregate_return_pct=0.0,
                aggregate_sharpe=0.0,
                aggregate_max_drawdown_pct=0.0,
                total_validate_trades=0,
                n_windows=0,
            )

        train_ms = train_days * _DAY_MS
        validate_ms = validate_days * _DAY_MS
        step_ms = step_days * _DAY_MS

        first_ts: int = int(candles[0].timestamp)
        last_ts: int = int(candles[-1].timestamp)
        total_span = last_ts - first_ts

        logger.info(
            "walk_forward_start",
            train_days=train_days,
            validate_days=validate_days,
            step_days=step_days,
            total_candles=len(candles),
            total_span_days=total_span / _DAY_MS,
        )

        windows: list[WalkForwardWindow] = []
        window_start = first_ts
        window_idx = 0

        while True:
            train_start = window_start
            train_end = train_start + train_ms
            validate_start = train_end
            validate_end = validate_start + validate_ms

            # Check if we have enough data for this window
            if validate_end > last_ts:
                break

            # Slice candles for train and validate
            train_candles = [c for c in candles if train_start <= c.timestamp < train_end]
            validate_candles = [c for c in candles if validate_start <= c.timestamp < validate_end]

            if not train_candles or not validate_candles:
                window_start += step_ms
                window_idx += 1
                continue

            # Slice funding rates
            train_funding = self._slice_funding(funding_rates, train_start, train_end)
            validate_funding = self._slice_funding(funding_rates, validate_start, validate_end)

            logger.debug(
                "walk_forward_window",
                window=window_idx,
                train_candles=len(train_candles),
                validate_candles=len(validate_candles),
            )

            # Run on training segment
            train_result = await self._engine.run(
                candles=train_candles,
                funding_rates=train_funding,
                strategy_factory=strategy_factory,
                initial_capital=initial_capital,
                slippage_mode=slippage_mode,
            )

            # Run on validation segment
            validate_result = await self._engine.run(
                candles=validate_candles,
                funding_rates=validate_funding,
                strategy_factory=strategy_factory,
                initial_capital=initial_capital,
                slippage_mode=slippage_mode,
            )

            windows.append(
                WalkForwardWindow(
                    window_index=window_idx,
                    train_start_ms=train_start,
                    train_end_ms=train_end,
                    validate_start_ms=validate_start,
                    validate_end_ms=validate_end,
                    train_result=train_result,
                    validate_result=validate_result,
                )
            )

            window_start += step_ms
            window_idx += 1

        # Compute aggregate metrics
        if windows:
            avg_return = sum(w.validate_result.metrics.total_return_pct for w in windows) / len(windows)
            avg_sharpe = sum(w.validate_result.metrics.sharpe_ratio for w in windows) / len(windows)
            max_dd = max(w.validate_result.metrics.max_drawdown_pct for w in windows)
            total_trades = sum(w.validate_result.metrics.trade_count for w in windows)
        else:
            avg_return = 0.0
            avg_sharpe = 0.0
            max_dd = 0.0
            total_trades = 0

        result = WalkForwardResult(
            windows=windows,
            aggregate_return_pct=avg_return,
            aggregate_sharpe=avg_sharpe,
            aggregate_max_drawdown_pct=max_dd,
            total_validate_trades=total_trades,
            n_windows=len(windows),
        )

        logger.info(
            "walk_forward_complete",
            n_windows=result.n_windows,
            aggregate_return=f"{avg_return:.4%}",
            aggregate_sharpe=f"{avg_sharpe:.2f}",
        )
        return result

    @staticmethod
    def _slice_funding(
        rates: list[FundingRate] | None,
        start_ms: int,
        end_ms: int,
    ) -> list[FundingRate] | None:
        if not rates:
            return None
        return [r for r in rates if start_ms <= r.timestamp < end_ms]


def run_monte_carlo(
    equity_curve: list[tuple[int, float]],
    n_simulations: int = 1000,
    confidence: float = 0.95,
    seed: int | None = None,
) -> MonteCarloResult:
    """Monte Carlo bootstrap simulation (design doc §6.1).

    Randomly reshuffles (bootstrap) historical return series to test
    whether strategy returns are significantly better than random.

    Args:
        equity_curve: List of (timestamp, equity) tuples.
        n_simulations: Number of bootstrap iterations.
        confidence: Confidence level for intervals (e.g. 0.95 for 95%).
        seed: Random seed for reproducibility.

    Returns:
        MonteCarloResult with confidence intervals and p-value.
    """
    if len(equity_curve) < 2:
        return MonteCarloResult(
            n_simulations=0,
            return_ci_lower=0.0,
            return_ci_upper=0.0,
            sharpe_ci_lower=0.0,
            sharpe_ci_upper=0.0,
            drawdown_ci_lower=0.0,
            drawdown_ci_upper=0.0,
            p_value_return=1.0,
            observed_return=0.0,
            observed_sharpe=0.0,
        )

    rng = random.Random(seed)

    # Compute observed returns
    observed_returns = _compute_returns(equity_curve)
    observed_total_return = (equity_curve[-1][1] / equity_curve[0][1]) - 1.0 if equity_curve[0][1] > 0 else 0.0
    observed_sharpe = _compute_sharpe(observed_returns)

    # Bootstrap simulations
    sim_returns: list[float] = []
    sim_sharpes: list[float] = []
    sim_drawdowns: list[float] = []

    for _ in range(n_simulations):
        # Resample returns with replacement
        resampled = [rng.choice(observed_returns) for _ in range(len(observed_returns))]
        # Reconstruct equity from resampled returns
        sim_equity = equity_curve[0][1]
        sim_equity_curve = [(equity_curve[0][0], sim_equity)]
        for i, ret in enumerate(resampled):
            sim_equity = sim_equity * (1.0 + ret)
            ts = equity_curve[min(i + 1, len(equity_curve) - 1)][0]
            sim_equity_curve.append((ts, sim_equity))

        initial = sim_equity_curve[0][1]
        sim_total_return = (sim_equity_curve[-1][1] / initial) - 1.0 if initial > 0 else 0.0
        sim_returns.append(sim_total_return)
        sim_sharpes.append(_compute_sharpe(resampled))
        sim_drawdowns.append(_compute_max_drawdown(sim_equity_curve))

    # Compute confidence intervals
    alpha = (1.0 - confidence) / 2.0
    sim_returns.sort()
    sim_sharpes.sort()
    sim_drawdowns.sort()

    lower_idx = max(0, int(alpha * n_simulations))
    upper_idx = min(n_simulations - 1, int((1.0 - alpha) * n_simulations))

    # P-value: fraction of simulations that beat the observed return
    p_value = sum(1 for r in sim_returns if r >= observed_total_return) / n_simulations

    return MonteCarloResult(
        n_simulations=n_simulations,
        return_ci_lower=sim_returns[lower_idx],
        return_ci_upper=sim_returns[upper_idx],
        sharpe_ci_lower=sim_sharpes[lower_idx],
        sharpe_ci_upper=sim_sharpes[upper_idx],
        drawdown_ci_lower=sim_drawdowns[lower_idx],
        drawdown_ci_upper=sim_drawdowns[upper_idx],
        p_value_return=p_value,
        observed_return=observed_total_return,
        observed_sharpe=observed_sharpe,
    )


def bonferroni_correction(p_value: float, n_tests: int) -> float:
    """Apply Bonferroni multiple testing correction.

    Design doc §6.1: "100 个参数组合测试意味着显著性阈值从 0.05 上升到 0.0005"
    """
    return min(p_value * n_tests, 1.0)


def _compute_returns(equity_curve: list[tuple[int, float]]) -> list[float]:
    """Compute simple returns from an equity curve."""
    returns: list[float] = []
    for i in range(1, len(equity_curve)):
        prev = equity_curve[i - 1][1]
        curr = equity_curve[i][1]
        if prev > 0:
            returns.append((curr - prev) / prev)
    return returns


def _compute_sharpe(returns: list[float]) -> float:
    """Compute annualized Sharpe from a return series."""
    if not returns:
        return 0.0
    mean_ret = sum(returns) / len(returns)
    variance = sum((r - mean_ret) ** 2 for r in returns) / len(returns)
    std_ret = math.sqrt(variance) if variance > 0 else 0.0
    if std_ret == 0:
        return 0.0
    # Assume hourly returns → ~8760 per year
    return (mean_ret / std_ret) * math.sqrt(8760)


def _compute_max_drawdown(equity_curve: list[tuple[int, float]]) -> float:
    """Compute max drawdown from an equity curve."""
    peak = 0.0
    max_dd = 0.0
    for _, eq in equity_curve:
        if eq > peak:
            peak = eq
        if peak > 0:
            dd = (peak - eq) / peak
            if dd > max_dd:
                max_dd = dd
    return max_dd
