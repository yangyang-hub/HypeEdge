"""Typed append-only ClickHouse payloads for market-making analytics.

These records are analytical projections only.  In particular, fill markout
measures execution quality and must never be treated as accounting PnL or a
control-plane source of truth.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from hypeedge.core.enums import ActionBudgetMode, Side
from hypeedge.core.types import Cloid, OrderId, Price, Size, StrategyId, Symbol, Usd


@dataclass(frozen=True, slots=True)
class MarketMakerFeatureSample:
    """One versioned feature/model evaluation sample."""

    ts: datetime
    strategy_id: StrategyId
    symbol: Symbol
    session_id: str
    config_version: int
    model_version: str
    market_version: int
    exchange_ts: datetime
    received_at: datetime
    mid_px: Price
    microprice: Price
    fair_px: Price
    best_bid_px: Price
    best_ask_px: Price
    normalized_ofi_l1: Decimal
    normalized_ofi_l5: Decimal
    trade_flow: Decimal
    short_return: Decimal
    volatility_1s: Decimal
    volatility_5s: Decimal
    volatility_30s: Decimal
    volatility_5m: Decimal
    toxicity: Decimal
    receipt_to_decision_us: int
    event_loop_lag_us: int


@dataclass(frozen=True, slots=True)
class MarketMakerQuoteDecision:
    """A quote-set decision, including KEEP and NO_QUOTE outcomes."""

    ts: datetime
    strategy_id: StrategyId
    symbol: Symbol
    session_id: str
    config_version: int
    model_version: str
    quote_revision: int
    market_version: int
    decision: str
    reason: str
    fair_px: Price
    reservation_px: Price
    desired_bid_px: Price | None
    desired_bid_size: Size | None
    desired_ask_px: Price | None
    desired_ask_size: Size | None
    live_bid_px: Price | None
    live_bid_size: Size | None
    live_ask_px: Price | None
    live_ask_size: Size | None
    position_size: Size
    inventory_notional_usdc: Usd
    budget_mode: ActionBudgetMode
    expected_gross_edge_usdc: Usd
    adverse_selection_cost_usdc: Usd
    inventory_cost_usdc: Usd
    funding_cost_usdc: Usd
    action_cost_usdc: Usd
    failure_cost_usdc: Usd
    expected_net_pnl_usdc: Usd


@dataclass(frozen=True, slots=True)
class MarketMakerInventorySample:
    """Low-frequency inventory and margin risk sample."""

    ts: datetime
    strategy_id: StrategyId
    symbol: Symbol
    session_id: str
    position_size: Size
    mark_px: Price
    inventory_notional_usdc: Usd
    soft_limit_utilization: Decimal
    hard_limit_utilization: Decimal
    emergency_limit_utilization: Decimal
    equity_usdc: Usd
    available_balance_usdc: Usd
    margin_used_usdc: Usd
    liquidation_distance_bps: Decimal | None
    funding_carry_usdc: Usd
    reduce_only: bool
    healthy: bool


@dataclass(frozen=True, slots=True)
class MarketMakerActionCreditSample:
    """Remote and shadow action-credit sustainability sample."""

    ts: datetime
    strategy_id: StrategyId
    symbol: Symbol
    quota_owner: str
    remote_remaining: int
    shadow_remaining: int
    cancel_headroom: int
    ip_weight_remaining: int
    actions_burned_1h: int
    actions_earned_1h: int
    actions_burned_24h: int
    actions_earned_24h: int
    fills_1h: int
    usdc_volume_1h: Usd
    usdc_per_action_1h: Decimal
    usdc_per_action_24h: Decimal
    runway_hours: Decimal | None
    soft_allocation: int
    hard_allocation: int
    emergency_reserve: int
    mode: ActionBudgetMode
    remote_observed_at: datetime
    window_end: datetime
    calculation_version: str


@dataclass(frozen=True, slots=True)
class MarketMakerFillMarkout:
    """Immutable execution-quality markout for one fill and one horizon.

    ``signed_markout_*`` follows the aggressor-independent order-side
    convention: positive means the market moved in favour of our fill
    (buy: reference rose; sell: reference fell).  It is diagnostic and is not
    an accounting ledger entry.
    """

    ts: datetime
    strategy_id: StrategyId
    symbol: Symbol
    session_id: str
    fill_id: str
    order_id: OrderId
    cloid: Cloid
    fill_ts: datetime
    side: Side
    fill_px: Price
    fill_size: Size
    reference: str
    reference_px: Price
    horizon_ms: int
    horizon_ts: datetime
    mark_px: Price
    signed_markout_bps: Decimal
    signed_markout_usdc: Usd
    spread_capture_usdc: Usd
    maker: bool
    queue_ahead_size: Size | None
    fill_probability: Decimal | None
    calculation_version: str
