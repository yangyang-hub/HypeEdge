"""Pure market-maker inputs and versioned configuration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from hypeedge.core.enums import ActionBudgetMode
from hypeedge.core.types import Price, Size, Symbol, Usd


@dataclass(frozen=True, slots=True)
class MarketFeatures:
    symbol: Symbol
    market_version: int
    connection_generation: int
    exchange_ts: int
    received_at: datetime
    healthy: bool
    best_bid: Price
    best_ask: Price
    best_bid_size: Size
    best_ask_size: Size
    microprice: Price
    normalized_ofi: Decimal
    trade_flow: Decimal
    short_return: Decimal
    return_variance_per_second: Decimal
    expected_adverse_markout_bps: Decimal
    latency_buffer_bps: Decimal
    toxicity: Decimal
    funding_rate: Decimal
    external_source: str | None = None
    external_symbol: str | None = None
    external_raw_price: Price | None = None
    external_adjusted_price: Price | None = None
    external_basis_bps: Decimal = Decimal(0)
    external_effective_weight: Decimal = Decimal(0)
    external_confidence: Decimal = Decimal(0)
    external_age_ms: int | None = None
    external_quality: str = "unavailable"
    external_observed_at: datetime | None = None
    latency_quality: str = "configured"
    markout_quality: str = "configured"

    def __post_init__(self) -> None:
        if self.market_version < 0 or self.connection_generation < 0:
            raise ValueError("market versions cannot be negative")
        if self.best_bid <= 0 or self.best_ask <= self.best_bid:
            raise ValueError("market features require a positive non-crossed book")
        if self.best_bid_size <= 0 or self.best_ask_size <= 0:
            raise ValueError("top-of-book sizes must be positive")
        if not Decimal("0") <= self.toxicity <= Decimal("1"):
            raise ValueError("toxicity must be in [0, 1]")
        if self.return_variance_per_second < 0:
            raise ValueError("return variance cannot be negative")
        if not Decimal(0) <= self.external_effective_weight <= Decimal(1):
            raise ValueError("external effective weight must be in [0, 1]")
        if not Decimal(0) <= self.external_confidence <= Decimal(1):
            raise ValueError("external confidence must be in [0, 1]")
        if self.external_age_ms is not None and self.external_age_ms < 0:
            raise ValueError("external age cannot be negative")

    @property
    def mid_price(self) -> Price:
        return Price((self.best_bid + self.best_ask) / 2)


@dataclass(frozen=True, slots=True)
class InventorySnapshot:
    position_size: Size
    equity: Usd
    available_balance: Usd
    margin_used: Usd
    observed_at: datetime
    healthy: bool


@dataclass(frozen=True, slots=True)
class ActionBudgetSnapshot:
    mode: ActionBudgetMode
    address_actions_remaining: int
    cancel_headroom: int
    ip_weight_remaining: int
    action_shadow_cost_usdc: Usd
    observed_at: datetime
    healthy: bool


@dataclass(frozen=True, slots=True)
class MarketMakerConfig:
    version: int
    model_version: str
    tick_size: Decimal
    lot_size: Decimal
    min_size: Decimal
    soft_inventory_notional: Usd
    hard_inventory_notional: Usd
    emergency_inventory_notional: Usd
    quote_size: Size
    max_depth_participation: Decimal
    beta_microprice: Decimal = Decimal("0.5")
    beta_ofi_ticks: Decimal = Decimal("0.25")
    beta_trade_flow_ticks: Decimal = Decimal("0.25")
    beta_short_return_ticks: Decimal = Decimal("0.25")
    max_fair_shift_ticks: Decimal = Decimal("2")
    external_reference_weight: Decimal = Decimal("0.25")
    external_basis_alpha: Decimal = Decimal("0.02")
    external_max_age_seconds: Decimal = Decimal("0.5")
    external_outlier_bps: Decimal = Decimal("75")
    max_external_shift_ticks: Decimal = Decimal("2")
    max_total_fair_shift_ticks: Decimal = Decimal("3")
    latency_risk_multiplier: Decimal = Decimal("1")
    conservative_latency_seconds: Decimal = Decimal("0.1")
    conservative_markout_bps: Decimal = Decimal("1")
    min_markout_samples: int = 20
    inventory_skew_bps: Decimal = Decimal("5")
    inventory_gamma_bps: Decimal = Decimal("1")
    max_inventory_shift_bps: Decimal = Decimal("20")
    horizon_seconds: Decimal = Decimal("5")
    min_half_spread_bps: Decimal = Decimal("1")
    toxicity_spread_bps: Decimal = Decimal("10")
    signed_maker_fee_rate: Decimal = Decimal("-0.0002")
    expected_fill_probability: Decimal = Decimal("0.10")
    min_expected_pnl_usdc: Usd = Usd("0")
    max_quote_lifetime_seconds: Decimal = Decimal("10")

    def __post_init__(self) -> None:
        positive = {
            "version": Decimal(self.version),
            "tick_size": self.tick_size,
            "lot_size": self.lot_size,
            "min_size": self.min_size,
            "soft_inventory_notional": self.soft_inventory_notional,
            "hard_inventory_notional": self.hard_inventory_notional,
            "emergency_inventory_notional": self.emergency_inventory_notional,
            "quote_size": self.quote_size,
            "horizon_seconds": self.horizon_seconds,
            "max_quote_lifetime_seconds": self.max_quote_lifetime_seconds,
        }
        for name, value in positive.items():
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if not self.soft_inventory_notional < self.hard_inventory_notional < self.emergency_inventory_notional:
            raise ValueError("inventory limits must satisfy soft < hard < emergency")
        for name, value in {
            "max_depth_participation": self.max_depth_participation,
            "expected_fill_probability": self.expected_fill_probability,
        }.items():
            if not Decimal("0") < value <= Decimal("1"):
                raise ValueError(f"{name} must be in (0, 1]")
        if (
            self.max_fair_shift_ticks < 0
            or self.max_external_shift_ticks < 0
            or self.max_total_fair_shift_ticks < 0
            or self.max_inventory_shift_bps < 0
        ):
            raise ValueError("fair and inventory shift caps cannot be negative")
        for name, value in {
            "external_reference_weight": self.external_reference_weight,
            "external_basis_alpha": self.external_basis_alpha,
        }.items():
            if not Decimal(0) <= value <= Decimal(1):
                raise ValueError(f"{name} must be in [0, 1]")
        if self.external_max_age_seconds <= 0 or self.external_outlier_bps <= 0:
            raise ValueError("external age and outlier limits must be positive")
        if self.latency_risk_multiplier < 0 or self.conservative_latency_seconds < 0:
            raise ValueError("latency configuration cannot be negative")
        if self.conservative_markout_bps < 0 or self.min_markout_samples <= 0:
            raise ValueError("markout configuration must be non-negative with a positive sample count")
