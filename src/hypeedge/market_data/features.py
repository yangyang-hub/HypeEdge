"""Latest-value market microstructure features for the market-maker policy."""

from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal

from hypeedge.core.enums import Side
from hypeedge.core.models import L2BookSnapshot, Trade
from hypeedge.core.types import Price, Symbol
from hypeedge.market_data.external_reference import ExternalReferenceSnapshot
from hypeedge.strategy.market_maker.models import MarketFeatures, MarketMakerConfig


@dataclass(frozen=True, slots=True)
class _ExternalFeatures:
    source: str | None = None
    symbol: str | None = None
    raw_price: Price | None = None
    adjusted_price: Price | None = None
    basis_bps: Decimal = Decimal(0)
    effective_weight: Decimal = Decimal(0)
    confidence: Decimal = Decimal(0)
    age_ms: int | None = None
    quality: str = "unavailable"
    observed_at: datetime | None = None


class MarketFeatureEngine:
    """Maintain small in-memory event windows and build deterministic features."""

    def __init__(self, *, depth_levels: int = 5, window_seconds: float = 5.0, max_events: int = 2048) -> None:
        if depth_levels <= 0 or window_seconds <= 0 or max_events <= 1:
            raise ValueError("feature-engine windows must be positive")
        self._depth_levels = depth_levels
        self._window = timedelta(seconds=window_seconds)
        self._mid_history: dict[Symbol, deque[tuple[datetime, Decimal]]] = defaultdict(lambda: deque(maxlen=max_events))
        self._trades: dict[Symbol, deque[Trade]] = defaultdict(lambda: deque(maxlen=max_events))

    def observe_book(self, snapshot: L2BookSnapshot) -> None:
        if not snapshot.bids or not snapshot.asks:
            return
        mid = (Decimal(snapshot.bids[0].price) + Decimal(snapshot.asks[0].price)) / Decimal(2)
        history = self._mid_history[snapshot.symbol]
        history.append((snapshot.received_at, mid))
        self._trim(snapshot.symbol, snapshot.received_at)

    def observe_trade(self, trade: Trade) -> None:
        trades = self._trades[trade.symbol]
        trades.append(trade)
        self._trim(trade.symbol, trade.local_ts)

    def build(
        self,
        snapshot: L2BookSnapshot,
        *,
        healthy: bool,
        funding_rate: Decimal = Decimal(0),
        expected_adverse_markout_bps: Decimal = Decimal(0),
        latency_buffer_bps: Decimal = Decimal(0),
        latency_seconds: Decimal | None = None,
        latency_quality: str = "configured",
        markout_quality: str = "configured",
        external_reference: ExternalReferenceSnapshot | None = None,
        config: MarketMakerConfig | None = None,
        decision_at: datetime | None = None,
    ) -> MarketFeatures:
        if not snapshot.bids or not snapshot.asks:
            raise ValueError("cannot build market features from an empty book")
        self.observe_book(snapshot)
        bid = snapshot.bids[0]
        ask = snapshot.asks[0]
        top_total = Decimal(bid.size) + Decimal(ask.size)
        if top_total <= 0:
            raise ValueError("top-of-book liquidity must be positive")
        microprice = (Decimal(ask.price) * Decimal(bid.size) + Decimal(bid.price) * Decimal(ask.size)) / top_total

        bid_depth = sum((Decimal(level.size) for level in snapshot.bids[: self._depth_levels]), start=Decimal(0))
        ask_depth = sum((Decimal(level.size) for level in snapshot.asks[: self._depth_levels]), start=Decimal(0))
        depth_total = bid_depth + ask_depth
        ofi = (bid_depth - ask_depth) / depth_total if depth_total > 0 else Decimal(0)
        trade_flow = self._trade_flow(snapshot.symbol)
        short_return, variance = self._return_features(snapshot.symbol)
        if latency_seconds is not None and config is not None:
            latency_variance = variance * max(Decimal(0), latency_seconds)
            latency_buffer_bps = latency_variance.sqrt() * Decimal("10000") * config.latency_risk_multiplier
        toxicity = min(
            Decimal(1),
            abs(ofi) * Decimal("0.35")
            + abs(trade_flow) * Decimal("0.35")
            + min(Decimal(1), Decimal(str(math.sqrt(float(variance)))) * Decimal("100")) * Decimal("0.30"),
        )
        external = self._external_features(
            (Decimal(bid.price) + Decimal(ask.price)) / Decimal(2),
            decision_at or snapshot.received_at,
            external_reference,
            config,
        )
        return MarketFeatures(
            symbol=snapshot.symbol,
            market_version=snapshot.version,
            connection_generation=snapshot.connection_generation,
            exchange_ts=int(snapshot.exchange_ts),
            received_at=snapshot.received_at,
            healthy=healthy,
            best_bid=bid.price,
            best_ask=ask.price,
            best_bid_size=bid.size,
            best_ask_size=ask.size,
            microprice=Price(microprice),
            normalized_ofi=ofi,
            trade_flow=trade_flow,
            short_return=short_return,
            return_variance_per_second=variance,
            expected_adverse_markout_bps=expected_adverse_markout_bps,
            latency_buffer_bps=latency_buffer_bps,
            toxicity=toxicity,
            funding_rate=funding_rate,
            latency_quality=latency_quality,
            markout_quality=markout_quality,
            external_source=external.source,
            external_symbol=external.symbol,
            external_raw_price=external.raw_price,
            external_adjusted_price=external.adjusted_price,
            external_basis_bps=external.basis_bps,
            external_effective_weight=external.effective_weight,
            external_confidence=external.confidence,
            external_age_ms=external.age_ms,
            external_quality=external.quality,
            external_observed_at=external.observed_at,
        )

    def _external_features(
        self,
        local_mid: Decimal,
        decision_at: datetime,
        reference: ExternalReferenceSnapshot | None,
        config: MarketMakerConfig | None,
    ) -> _ExternalFeatures:
        if reference is None or config is None:
            return _ExternalFeatures()
        age = decision_at - reference.observed_at
        age_ms = max(0, int(age.total_seconds() * 1000))
        common = _ExternalFeatures(
            source=reference.source,
            symbol=str(reference.symbol),
            raw_price=reference.raw_price,
            confidence=reference.confidence,
            age_ms=age_ms,
            observed_at=reference.observed_at,
        )
        if age.total_seconds() < 0:
            return replace(common, quality="clock_skew")
        if reference.quality in {"disabled", "stale"} or reference.confidence <= 0:
            return replace(common, quality=reference.quality)
        max_age = config.external_max_age_seconds
        age_seconds = Decimal(str(age.total_seconds()))
        if age_seconds > max_age:
            return replace(common, quality="stale")

        adjusted_price = reference.adjusted_price
        if reference.raw_price is None or adjusted_price is None:
            return common
        adjusted = Decimal(adjusted_price)
        deviation_bps = abs(adjusted / local_mid - Decimal(1)) * Decimal("10000")
        if deviation_bps > config.external_outlier_bps:
            quality = "outlier"
            effective_weight = Decimal(0)
        else:
            quality = "good" if reference.quality == "healthy" else "degraded"
            freshness = max(Decimal(0), Decimal(1) - age_seconds / max_age)
            effective_weight = (
                min(
                    Decimal(1),
                    config.external_reference_weight,
                    reference.effective_weight,
                )
                * freshness
            )
        return replace(
            common,
            adjusted_price=Price(adjusted),
            basis_bps=reference.basis_bps,
            effective_weight=effective_weight,
            quality=quality,
        )

    def _trade_flow(self, symbol: Symbol) -> Decimal:
        trades = self._trades[symbol]
        buy = Decimal(0)
        sell = Decimal(0)
        for trade in trades:
            notional = Decimal(trade.price) * Decimal(trade.size)
            if trade.side == Side.BUY:
                buy += notional
            else:
                sell += notional
        total = buy + sell
        return (buy - sell) / total if total > 0 else Decimal(0)

    def _return_features(self, symbol: Symbol) -> tuple[Decimal, Decimal]:
        history = self._mid_history[symbol]
        if len(history) < 2:
            return Decimal(0), Decimal(0)
        mids = [value for _, value in history]
        short_return = (mids[-1] - mids[0]) / mids[0] if mids[0] > 0 else Decimal(0)
        log_returns: list[float] = []
        for previous, current in zip(mids, mids[1:], strict=False):
            if previous > 0 and current > 0:
                log_returns.append(math.log(float(current / previous)))
        if not log_returns:
            return short_return, Decimal(0)
        mean = sum(log_returns) / len(log_returns)
        variance = sum((value - mean) ** 2 for value in log_returns) / len(log_returns)
        return short_return, Decimal(str(variance))

    def _trim(self, symbol: Symbol, now: datetime) -> None:
        history = self._mid_history[symbol]
        while history and now - history[0][0] > self._window:
            history.popleft()
        trades = self._trades[symbol]
        while trades and now - trades[0].local_ts > self._window:
            trades.popleft()
