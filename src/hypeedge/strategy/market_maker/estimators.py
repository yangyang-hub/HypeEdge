"""Deterministic online estimators for market-maker execution costs."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from hypeedge.core.types import StrategyId, Symbol
from hypeedge.storage.mm_analytics import MarketMakerFillMarkout


@dataclass(frozen=True, slots=True)
class MarkoutEstimate:
    adverse_bps: Decimal
    quality: str
    sample_count: int


class AdverseMarkoutEstimator:
    """Estimate adverse selection only from completed, mature maker-fill markouts."""

    def __init__(
        self,
        *,
        min_samples: int = 20,
        max_samples: int = 500,
        conservative_default_bps: Decimal = Decimal("1"),
    ) -> None:
        if min_samples <= 0 or max_samples < min_samples:
            raise ValueError("markout sample windows must satisfy 0 < min <= max")
        if conservative_default_bps < 0:
            raise ValueError("conservative markout default cannot be negative")
        self._min_samples = min_samples
        self._default = conservative_default_bps
        self._samples: dict[tuple[StrategyId, Symbol], deque[tuple[tuple[str, int, str], Decimal]]] = {}
        self._max_samples = max_samples

    def observe(self, sample: MarketMakerFillMarkout, *, now: datetime) -> bool:
        if not sample.maker or sample.horizon_ms <= 0 or sample.horizon_ts > now or sample.ts < sample.horizon_ts:
            return False
        identity = (sample.fill_id, sample.horizon_ms, sample.calculation_version)
        key = (sample.strategy_id, sample.symbol)
        values = self._samples.setdefault(key, deque(maxlen=self._max_samples))
        if any(existing == identity for existing, _ in values):
            return False
        values.append((identity, max(Decimal(0), -sample.signed_markout_bps)))
        return True

    def estimate(
        self,
        strategy_id: StrategyId,
        symbol: Symbol,
        *,
        min_samples: int | None = None,
        conservative_default_bps: Decimal | None = None,
    ) -> MarkoutEstimate:
        values = self._samples.get((strategy_id, symbol), ())
        required = self._min_samples if min_samples is None else min_samples
        if required <= 0:
            raise ValueError("min_samples must be positive")
        default = self._default if conservative_default_bps is None else conservative_default_bps
        if default < 0:
            raise ValueError("conservative markout default cannot be negative")
        if len(values) < required:
            return MarkoutEstimate(default, "conservative_default", len(values))
        ordered = sorted(value for _, value in values)
        # A conservative upper-median estimate reacts to adverse tails without
        # allowing a single bad print to dominate the quoting spread.
        index = min(len(ordered) - 1, (len(ordered) * 3) // 4)
        return MarkoutEstimate(ordered[index], "mature", len(ordered))


class DecisionLatencyEstimator:
    """EWMA of receipt-to-decision latency with an explicit warm-up quality."""

    def __init__(
        self,
        *,
        alpha: Decimal = Decimal("0.2"),
        conservative_default_seconds: Decimal = Decimal("0.1"),
        min_samples: int = 5,
    ) -> None:
        if not Decimal(0) < alpha <= Decimal(1):
            raise ValueError("latency alpha must be in (0, 1]")
        if conservative_default_seconds < 0:
            raise ValueError("latency default cannot be negative")
        if min_samples <= 0:
            raise ValueError("latency min_samples must be positive")
        self._alpha = alpha
        self._default = conservative_default_seconds
        self._ewma: Decimal | None = None
        self._samples = 0
        self._min_samples = min_samples

    def observe(self, seconds: Decimal) -> None:
        if seconds < 0:
            return
        if self._ewma is None:
            self._ewma = seconds
        else:
            self._ewma = self._alpha * seconds + (Decimal(1) - self._alpha) * self._ewma
        self._samples += 1

    @property
    def seconds(self) -> Decimal:
        return self._default if self._samples < self._min_samples or self._ewma is None else self._ewma

    @property
    def quality(self) -> str:
        return "conservative_default" if self._samples < self._min_samples else "observed"
