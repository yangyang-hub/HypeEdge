"""Pydantic response/request schemas for the FastAPI API layer.

Unified response format: { "ok": true, "data": ... } or { "ok": false, "error": "..." }
All schemas are JSON-serializable and aligned with frontend lib/types.ts.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Annotated, Any, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, PlainSerializer, field_validator, model_validator

_DECIMAL_PATTERN = re.compile(r"^-?(?:0|[1-9]\d*)(?:\.\d+)?$")


def decimal_string(value: Decimal | float | int | str) -> str:
    """Return a canonical fixed-point JSON representation without precision loss."""
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    if not decimal_value.is_finite():
        raise ValueError("decimal value must be finite")
    if decimal_value == 0:
        return "0"
    return format(decimal_value.normalize(), "f")


def _parse_decimal_string(value: Any) -> Decimal:
    if not isinstance(value, str) or not _DECIMAL_PATTERN.fullmatch(value):
        raise ValueError("must be a base-10 decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("must be a valid decimal string") from exc
    digits = parsed.as_tuple().digits
    exponent = parsed.as_tuple().exponent
    fractional_digits = max(0, -exponent) if isinstance(exponent, int) else 0
    if len(digits) > 38 or fractional_digits > 18:
        raise ValueError("must fit NUMERIC(38,18)")
    return parsed


DecimalString = Annotated[
    Decimal,
    BeforeValidator(_parse_decimal_string),
    PlainSerializer(decimal_string, return_type=str),
]
DecimalJson = Annotated[Decimal, PlainSerializer(decimal_string, return_type=str)]


class StrictModel(BaseModel):
    """Reject unknown fields so clients cannot silently send ignored data."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


# --- Unified Response ---


class ApiResponse(BaseModel):
    """Standard API response wrapper."""

    ok: bool = True
    data: Any = None


class ErrorResponse(BaseModel):
    """Standard API error response."""

    ok: bool = False
    error: str


# --- Account ---


class AccountData(BaseModel):
    """Account overview data."""

    equity: DecimalJson
    available_balance: DecimalJson
    total_margin_used: DecimalJson
    total_unrealized_pnl: DecimalJson
    peak_equity: DecimalJson
    drawdown_pct: DecimalJson
    leverage: DecimalJson
    total_fees: DecimalJson
    total_funding: DecimalJson
    fill_count: int
    position_count: int
    last_update: str | None = None
    trading_enabled: bool = False


class PositionData(BaseModel):
    """Single position data."""

    symbol: str
    size: DecimalJson
    entry_price: DecimalJson | None = None
    mark_price: DecimalJson | None = None
    unrealized_pnl: DecimalJson = Decimal("0")
    leverage: int = 1
    side: str = "flat"  # "long", "short", "flat"


class EquityPoint(BaseModel):
    """Single point on the equity curve."""

    timestamp: int
    equity: DecimalJson


# --- Orders ---


class OrderData(BaseModel):
    """Order data."""

    cloid: str
    symbol: str
    side: str
    size: DecimalJson
    price: DecimalJson | None = None
    order_type: str
    status: str
    filled_size: DecimalJson = Decimal("0")
    avg_fill_price: DecimalJson | None = None
    strategy_id: str | None = None
    error_message: str | None = None
    created_at: str | None = None


class OrderSubmitRequest(StrictModel):
    """Request to submit a new order."""

    symbol: str = Field(min_length=1, max_length=20, pattern=r"^[A-Z0-9][A-Z0-9_.-]*$")
    side: Literal["buy", "sell"]
    size: DecimalString = Field(gt=0)
    price: DecimalString | None = Field(default=None, gt=0)
    order_type: Literal["limit", "market"] = "limit"
    reduce_only: bool = False
    strategy_id: str | None = Field(default=None, min_length=1, max_length=64)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def validate_price(self) -> OrderSubmitRequest:
        if self.order_type == "limit" and self.price is None:
            raise ValueError("price is required for limit orders")
        if self.order_type == "market" and self.price is not None:
            raise ValueError("price must be omitted for market orders")
        return self


class ClosePositionRequest(StrictModel):
    """Close a position; direction is always derived by the backend."""

    quantity: DecimalString | None = Field(default=None, gt=0)
    close_fraction: DecimalString | None = Field(default=None, gt=0, le=1)
    max_slippage_bps: int = Field(default=30, ge=1, le=500)

    @model_validator(mode="after")
    def validate_quantity_choice(self) -> ClosePositionRequest:
        if (self.quantity is None) == (self.close_fraction is None):
            raise ValueError("provide exactly one of quantity or close_fraction")
        return self


# --- Strategies ---


class StrategyData(BaseModel):
    """Strategy status data."""

    strategy_id: str
    status: str
    symbol: str
    position_size: DecimalJson = Decimal("0")
    entry_price: DecimalJson | None = None
    stop_price: DecimalJson | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class StrategyMetadataPatchRequest(StrictModel):
    metadata: dict[str, str]


class StrategyLifecycleRequest(StrictModel):
    target: Literal["shadow", "running"] | None = None
    target_state: Literal["shadow", "running"] | None = None
    confirmation: str | None = Field(default=None, max_length=128)

    @model_validator(mode="after")
    def validate_target_aliases(self) -> StrategyLifecycleRequest:
        if self.target is not None and self.target_state is not None and self.target != self.target_state:
            raise ValueError("target and target_state cannot conflict")
        return self


class MarketMakerConfigCreateRequest(StrictModel):
    soft_inventory_notional: DecimalString = Field(gt=0)
    hard_inventory_notional: DecimalString = Field(gt=0)
    emergency_inventory_notional: DecimalString = Field(gt=0)
    quote_size: DecimalString = Field(gt=0)
    max_depth_participation: DecimalString = Field(gt=0, le=1)
    inventory_skew_bps: DecimalString = Field(ge=0)
    max_inventory_shift_bps: DecimalString = Field(ge=0)
    min_half_spread_bps: DecimalString = Field(ge=0)
    toxicity_spread_bps: DecimalString = Field(ge=0)
    min_expected_pnl_usdc: DecimalString = Field(ge=0)
    external_reference_weight: DecimalString = Field(default=Decimal("0.25"), ge=0, le=1)
    external_max_age_seconds: DecimalString = Field(default=Decimal("0.5"), gt=0)
    external_outlier_bps: DecimalString = Field(default=Decimal("75"), gt=0)
    max_external_shift_ticks: DecimalString = Field(default=Decimal("2"), ge=0)
    max_total_fair_shift_ticks: DecimalString = Field(default=Decimal("3"), ge=0)
    latency_risk_multiplier: DecimalString = Field(default=Decimal("1"), ge=0)
    conservative_latency_seconds: DecimalString = Field(default=Decimal("0.1"), ge=0)
    conservative_markout_bps: DecimalString = Field(default=Decimal("1"), ge=0)
    min_markout_samples: int = Field(default=20, gt=0, le=1_000_000)
    min_quote_lifetime_ms: int = Field(ge=0, le=300_000)
    refresh_cooldown_ms: int = Field(ge=0, le=300_000)
    max_quote_age_ms: int = Field(gt=0, le=3_600_000)
    market_stale_after_ms: int = Field(gt=0, le=60_000)
    account_stale_after_ms: int = Field(gt=0, le=60_000)

    @model_validator(mode="after")
    def validate_inventory_bands(self) -> MarketMakerConfigCreateRequest:
        if not self.soft_inventory_notional < self.hard_inventory_notional < self.emergency_inventory_notional:
            raise ValueError("inventory bands must satisfy soft < hard < emergency")
        if self.min_quote_lifetime_ms > self.max_quote_age_ms:
            raise ValueError("minimum quote lifetime cannot exceed maximum quote age")
        return self


class MarketMakerConfigVersionCreateRequest(StrictModel):
    strategy_type: Literal["market_maker"] = "market_maker"
    config: MarketMakerConfigCreateRequest


class StrategyCreateRequest(StrictModel):
    strategy_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.:-]+$")
    strategy_type: Literal["market_maker"] = "market_maker"
    sub_account: str = Field(min_length=1, max_length=128)
    symbol: str = Field(min_length=1, max_length=20, pattern=r"^[A-Z0-9][A-Z0-9_.-]*$")
    initial_config: MarketMakerConfigCreateRequest
    metadata: dict[str, str] = Field(default_factory=dict)


class DangerousActionConfirmation(StrictModel):
    confirmation: str | None = Field(default=None, max_length=128)


# --- Risk ---


class RiskLimitData(BaseModel):
    """Single risk limit with current value."""

    name: str
    current: DecimalJson
    limit: DecimalJson
    unit: str = ""
    pct_used: DecimalJson = Decimal("0")


class RiskStatusData(BaseModel):
    """Full risk status."""

    kill_switch_active: bool
    kill_switch_reason: str | None = None
    safety_mode: str = "starting"
    safety_reason: str | None = None
    limits: list[RiskLimitData] = Field(default_factory=list)
    check_stats: dict[str, int] = Field(default_factory=dict)
    strategy_pnl: dict[str, DecimalJson] = Field(default_factory=dict)
    action_credits_remaining: int = 0


class KillSwitchRequest(StrictModel):
    """Kill switch trigger/reset request."""

    action: Literal["trigger", "reset"]
    reason: str = Field(default="", max_length=500)


class InstrumentMetaData(BaseModel):
    symbol: str
    price_decimals: int = Field(ge=0, le=18)
    size_decimals: int = Field(ge=0, le=18)
    tick_size: DecimalJson = Field(gt=0)
    lot_size: DecimalJson = Field(gt=0)
    min_order_size: DecimalJson = Field(gt=0)
    max_leverage: int = Field(ge=1)


class SystemStatusData(BaseModel):
    environment: str
    trading_enabled: bool
    kill_switch_active: bool
    kill_switch_reason: str | None = None
    safety_mode: str = "starting"
    safety_reason: str | None = None
    shutting_down: bool
    meta_loaded: bool


# --- SSE Events ---


class SSEEvent(BaseModel):
    """Server-Sent Event data."""

    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    timestamp: str = ""
    correlation_id: str | None = None


# --- Market ---


class FundingRateData(BaseModel):
    """Funding rate data."""

    symbol: str
    funding_rate: DecimalJson
    premium: DecimalJson
    mark_price: DecimalJson
    open_interest: DecimalJson
    timestamp: int
