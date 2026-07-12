"""External reference-price normalization and latest-value access.

External venues are advisory inputs only.  A stale, crossed, or divergent
reference deterministically loses its weight and never blocks Hyperliquid's
native market-data path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal, Protocol

from hypeedge.config.settings import ExternalReferenceSettings
from hypeedge.core.types import Price, Symbol, Timestamp

ExternalMarket = Literal["spot", "perpetual", "perpetual_mark"]
ExternalQuality = Literal["healthy", "degraded", "stale", "disabled"]


@dataclass(frozen=True)
class ExternalVenueQuote:
    """One immutable Binance observation, retaining exchange and receipt time."""

    symbol: Symbol
    venue_symbol: str
    market: ExternalMarket
    exchange_ts: Timestamp
    received_at: datetime
    sequence: int
    connection_generation: int
    bid: Price | None = None
    ask: Price | None = None
    mark_price: Price | None = None

    @property
    def crossed(self) -> bool:
        return self.bid is not None and self.ask is not None and self.bid >= self.ask

    @property
    def mid(self) -> Price | None:
        if self.bid is None or self.ask is None or self.crossed:
            return None
        return Price((self.bid + self.ask) / Decimal("2"))


@dataclass(frozen=True)
class ExternalReferenceSnapshot:
    """Stable strategy-facing external-price snapshot.

    ``adjusted_price`` is mapped into Hyperliquid's price domain using a slow
    EWMA log basis. ``effective_weight`` is always zero for unusable data.
    """

    source: str
    symbol: Symbol
    raw_price: Price | None
    adjusted_price: Price | None
    basis_bps: Decimal
    effective_weight: Decimal
    confidence: Decimal
    age_ms: int
    quality: ExternalQuality
    observed_at: datetime
    spot_mid: Price | None = None
    perpetual_mid: Price | None = None
    perpetual_mark: Price | None = None
    sequence: int = 0
    connection_generation: int = 0
    quality_reasons: tuple[str, ...] = field(default_factory=tuple)


class ExternalReferenceProvider(Protocol):
    """Read interface consumed by market-making strategy code."""

    def get_external_reference(self, symbol: Symbol) -> ExternalReferenceSnapshot:
        """Return the latest external reference; unavailable data has zero weight."""
        ...


class LatestExternalReferenceProvider:
    """In-memory per-symbol latest-value provider with deterministic quality gates."""

    def __init__(self, settings: ExternalReferenceSettings) -> None:
        self._settings = settings
        self._quotes: dict[tuple[Symbol, ExternalMarket], ExternalVenueQuote] = {}
        self._basis_log_ewma: dict[Symbol, Decimal] = {}
        self._version: dict[Symbol, int] = {}

    def update_quote(self, quote: ExternalVenueQuote) -> ExternalReferenceSnapshot:
        """Apply a venue observation, ignoring regressions within a connection generation."""
        key = (quote.symbol, quote.market)
        previous = self._quotes.get(key)
        if previous is not None and quote.connection_generation < previous.connection_generation:
            return self.get_external_reference(quote.symbol)
        if (
            previous is not None
            and quote.connection_generation == previous.connection_generation
            and quote.sequence <= previous.sequence
        ):
            return self.get_external_reference(quote.symbol)
        self._quotes[key] = quote
        self._version[quote.symbol] = self._version.get(quote.symbol, 0) + 1
        return self.get_external_reference(quote.symbol)

    def update_hyperliquid_mid(self, symbol: Symbol, mid: Price) -> ExternalReferenceSnapshot:
        """Update the slow log-basis calibration from a native Hyperliquid midpoint."""
        snapshot = self.get_external_reference(symbol)
        if snapshot.raw_price is not None and snapshot.quality == "healthy" and mid > 0:
            observation = (Decimal(mid) / Decimal(snapshot.raw_price)).ln()
            previous = self._basis_log_ewma.get(symbol)
            alpha = self._settings.basis_ewma_alpha
            self._basis_log_ewma[symbol] = (
                observation if previous is None else alpha * observation + (Decimal("1") - alpha) * previous
            )
            self._version[symbol] = self._version.get(symbol, 0) + 1
        return self.get_external_reference(symbol)

    def get_external_reference(self, symbol: Symbol) -> ExternalReferenceSnapshot:
        """Build a freshness-aware snapshot from the latest observations."""
        now = datetime.now(UTC)
        if not self._settings.external_reference_enabled:
            return self._empty_snapshot(symbol, now, "disabled", ("external_reference_disabled",))

        spot = self._quotes.get((symbol, "spot"))
        perpetual = self._quotes.get((symbol, "perpetual"))
        mark = self._quotes.get((symbol, "perpetual_mark"))
        reasons: list[str] = []
        fresh_spot = self._is_fresh(spot, now)
        fresh_perpetual = self._is_fresh(perpetual, now)
        fresh_mark = self._is_fresh(mark, now)

        if spot is not None and spot.crossed:
            reasons.append("spot_crossed")
        if perpetual is not None and perpetual.crossed:
            reasons.append("perpetual_crossed")

        spot_mid = spot.mid if fresh_spot and spot is not None else None
        perpetual_mid = perpetual.mid if fresh_perpetual and perpetual is not None else None
        perpetual_mark = mark.mark_price if fresh_mark and mark is not None else None

        if spot_mid is not None and perpetual_mid is not None:
            divergence = self._divergence_bps(perpetual_mid, spot_mid)
            if divergence > self._settings.max_perp_spot_divergence_bps:
                reasons.append("perpetual_spot_outlier")
        if perpetual_mid is not None and perpetual_mark is not None:
            mark_divergence = self._divergence_bps(perpetual_mid, perpetual_mark)
            if mark_divergence > self._settings.max_mark_book_divergence_bps:
                reasons.append("perpetual_mark_outlier")

        contributors: list[tuple[Price, Decimal, ExternalVenueQuote]] = []
        if spot_mid is not None and spot is not None:
            contributors.append((spot_mid, self._settings.spot_weight, spot))
        if perpetual_mid is not None and perpetual is not None:
            contributors.append((perpetual_mid, self._settings.perpetual_weight, perpetual))
        if not contributors:
            if spot is not None or perpetual is not None or mark is not None:
                reasons.append("all_sources_stale_or_invalid")
            else:
                reasons.append("no_external_observation")
            return self._empty_snapshot(symbol, now, "stale", tuple(dict.fromkeys(reasons)))

        weight_sum = sum((weight for _, weight, _ in contributors), start=Decimal("0"))
        weighted_price = sum(
            (Decimal(price) * weight for price, weight, _ in contributors),
            start=Decimal("0"),
        )
        raw = Price(weighted_price / weight_sum)
        observations = [quote for _, _, quote in contributors]
        observed_at = min(quote.received_at for quote in observations)
        age_ms = max(0, int((now - observed_at).total_seconds() * 1000))
        sequence = max(quote.sequence for quote in observations)
        generation = max(quote.connection_generation for quote in observations)

        both_books = spot_mid is not None and perpetual_mid is not None
        if not both_books:
            reasons.append("single_source_only")
        anomaly = any(reason.endswith("crossed") or reason.endswith("outlier") for reason in reasons)
        quality: ExternalQuality = "healthy" if both_books and not anomaly else "degraded"
        freshness = max(
            Decimal("0"),
            Decimal("1") - Decimal(age_ms) / Decimal(self._settings.stale_after_ms),
        )
        confidence = (Decimal("1") if quality == "healthy" else Decimal("0.5")) * freshness
        effective_weight = self._settings.max_external_weight * confidence
        if anomaly:
            confidence = Decimal("0")
            effective_weight = Decimal("0")

        basis = self._basis_log_ewma.get(symbol, Decimal("0"))
        adjusted = Price(Decimal(raw) * basis.exp())
        basis_bps = (basis.exp() - Decimal("1")) * Decimal("10000")
        return ExternalReferenceSnapshot(
            source="binance_spot_perpetual",
            symbol=symbol,
            raw_price=raw,
            adjusted_price=adjusted,
            basis_bps=basis_bps,
            effective_weight=effective_weight,
            confidence=confidence,
            age_ms=age_ms,
            quality=quality,
            observed_at=observed_at,
            spot_mid=spot_mid,
            perpetual_mid=perpetual_mid,
            perpetual_mark=perpetual_mark,
            sequence=sequence,
            connection_generation=generation,
            quality_reasons=tuple(dict.fromkeys(reasons)),
        )

    def _is_fresh(self, quote: ExternalVenueQuote | None, now: datetime) -> bool:
        if quote is None:
            return False
        age_ms = (now - quote.received_at).total_seconds() * 1000
        return 0 <= age_ms <= self._settings.stale_after_ms

    @staticmethod
    def _divergence_bps(left: Price, right: Price) -> Decimal:
        if right <= 0:
            return Decimal("Infinity")
        return abs(Decimal(left) / Decimal(right) - Decimal("1")) * Decimal("10000")

    @staticmethod
    def _empty_snapshot(
        symbol: Symbol,
        now: datetime,
        quality: ExternalQuality,
        reasons: tuple[str, ...],
    ) -> ExternalReferenceSnapshot:
        return ExternalReferenceSnapshot(
            source="binance_spot_perpetual",
            symbol=symbol,
            raw_price=None,
            adjusted_price=None,
            basis_bps=Decimal("0"),
            effective_weight=Decimal("0"),
            confidence=Decimal("0"),
            age_ms=0,
            quality=quality,
            observed_at=now,
            quality_reasons=reasons,
        )
