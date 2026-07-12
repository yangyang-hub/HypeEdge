"""Technical indicators for strategy signal generation.

Pure functions, no state, no side effects. All return lists aligned
with the input (padded with NaN at the start where values are undefined).
"""

from __future__ import annotations

import math


def ema(values: list[float], period: int) -> list[float]:
    """Exponential Moving Average.

    Returns a list of the same length as input. First (period-1) values
    are NaN. The first valid value is the SMA of the first `period` values.
    """
    if period <= 0 or len(values) < period:
        return [math.nan] * len(values)

    result: list[float] = [math.nan] * (period - 1)
    # Seed with SMA
    sma_seed = sum(values[:period]) / period
    result.append(sma_seed)

    multiplier = 2.0 / (period + 1)
    for i in range(period, len(values)):
        prev = result[-1]
        result.append(values[i] * multiplier + prev * (1 - multiplier))

    return result


def sma(values: list[float], period: int) -> list[float]:
    """Simple Moving Average.

    Returns a list of the same length as input. First (period-1) values
    are NaN.
    """
    if period <= 0 or len(values) < period:
        return [math.nan] * len(values)

    result: list[float] = [math.nan] * (period - 1)
    window_sum = sum(values[:period])
    result.append(window_sum / period)

    for i in range(period, len(values)):
        window_sum += values[i] - values[i - period]
        result.append(window_sum / period)

    return result


def macd(
    closes: list[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> tuple[list[float], list[float], list[float]]:
    """MACD (Moving Average Convergence Divergence).

    Returns (macd_line, signal_line, histogram) — all same length as input.
    - macd_line = fast_ema - slow_ema
    - signal_line = EMA(macd_line, signal_period)
    - histogram = macd_line - signal_line
    """
    fast = ema(closes, fast_period)
    slow = ema(closes, slow_period)

    # Compute MACD line (NaN where either fast or slow is NaN)
    macd_line: list[float] = []
    for f, s in zip(fast, slow, strict=True):
        if math.isnan(f) or math.isnan(s):
            macd_line.append(math.nan)
        else:
            macd_line.append(f - s)

    # Compute signal line from MACD line
    # Filter NaN values for EMA computation
    valid_macd = [v for v in macd_line if not math.isnan(v)]
    if len(valid_macd) >= signal_period:
        signal_valid = ema(valid_macd, signal_period)
        # Re-align: pad NaN at the start
        nan_count = len(macd_line) - len(valid_macd)
        signal_line = [math.nan] * nan_count + signal_valid
    else:
        signal_line = [math.nan] * len(macd_line)

    # Histogram
    histogram: list[float] = []
    for m, s in zip(macd_line, signal_line, strict=True):
        if math.isnan(m) or math.isnan(s):
            histogram.append(math.nan)
        else:
            histogram.append(m - s)

    return macd_line, signal_line, histogram


def atr(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    period: int = 14,
) -> list[float]:
    """Average True Range.

    TR = max(high-low, abs(high-prev_close), abs(low-prev_close))
    ATR = EMA(TR, period)

    Returns list of same length as input. First value is NaN.
    """
    n = len(highs)
    if n == 0 or n != len(lows) or n != len(closes):
        return []

    # True Range (start from index 1; index 0 has no prev_close)
    tr_values: list[float] = []
    for i in range(1, n):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i - 1])
        lc = abs(lows[i] - closes[i - 1])
        tr_values.append(max(hl, hc, lc))

    # ATR = EMA of TR, then prepend NaN for index 0
    atr_valid = ema(tr_values, period)
    return [math.nan] + atr_valid


def momentum(values: list[float], period: int = 10) -> list[float]:
    """Rate of change (momentum).

    momentum[i] = (values[i] - values[i-period]) / values[i-period]

    Returns list of same length. First `period` values are NaN.
    """
    if period <= 0 or len(values) <= period:
        return [math.nan] * len(values)

    result: list[float] = [math.nan] * period
    for i in range(period, len(values)):
        if values[i - period] == 0:
            result.append(math.nan)
        else:
            result.append((values[i] - values[i - period]) / values[i - period])

    return result
