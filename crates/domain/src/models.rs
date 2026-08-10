//! Domain models mirroring `src/hypeedge/core/models.py` plus the market-making
//! analytics payloads from `src/hypeedge/storage/mm_analytics.py`.
//!
//! These are the payload types carried by [`crate::events::DomainEvent`]
//! variants. Field-for-field parity with the Python dataclasses matters: they
//! serialize to the JSON API contract and to ClickHouse/Postgres rows.

use chrono::{DateTime, Utc};

use crate::decimal::{Decimal, Price, Size, Usd};
use crate::enums::{ActionBudgetMode, OrderStatus, OrderType, Side, TimeInForce};

// --- Market data ---

/// A single price level in the order book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2Level {
    pub price: Price,
    pub size: Size,
}

/// Full L2 order book snapshot for a symbol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2BookSnapshot {
    pub symbol: String,
    /// Sorted best -> worst.
    pub bids: Vec<L2Level>,
    /// Sorted best -> worst.
    pub asks: Vec<L2Level>,
    /// Exchange event time (Unix millis).
    pub timestamp: i64,
    /// Local receipt time.
    pub local_ts: DateTime<Utc>,
    pub version: u64,
    pub connection_generation: u32,
}

impl L2BookSnapshot {
    pub fn exchange_ts(&self) -> i64 {
        self.timestamp
    }
    pub fn received_at(&self) -> DateTime<Utc> {
        self.local_ts
    }
    pub fn best_bid(&self) -> Option<&L2Level> {
        self.bids.first()
    }
    pub fn best_ask(&self) -> Option<&L2Level> {
        self.asks.first()
    }
    /// Mid price of the top of book, or `None` when either side is empty.
    pub fn mid_price(&self) -> Option<Price> {
        let bid = self.bids.first()?.price;
        let ask = self.asks.first()?.price;
        Some(Price::new(
            (bid.inner() + ask.inner()).div(Decimal::from_i128(2)),
        ))
    }
    /// Bid/ask spread in basis points, or `None` when no mid.
    pub fn spread_bps(&self) -> Option<f64> {
        let mid = self.mid_price()?;
        if mid.is_zero() {
            return None;
        }
        let bid = self.bids.first()?.price;
        let ask = self.asks.first()?.price;
        let spread = (ask.inner() - bid.inner()).div(mid.inner());
        Some(spread.to_string().parse::<f64>().unwrap_or(0.0) * 10_000.0)
    }
}

/// A single trade from the exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trade {
    pub symbol: String,
    pub price: Price,
    pub size: Size,
    pub side: Side,
    /// Trade ID.
    pub tid: u64,
    /// Exchange event time (Unix millis).
    pub timestamp: i64,
    pub local_ts: DateTime<Utc>,
}

/// OHLCV candlestick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candle {
    pub symbol: String,
    /// e.g. "1m", "5m", "1h".
    pub interval: String,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: Size,
    /// Candle open time (Unix millis).
    pub timestamp: i64,
}

/// Funding rate snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct FundingRate {
    pub symbol: String,
    pub funding_rate: f64,
    pub premium: f64,
    pub mark_price: Price,
    pub open_interest: f64,
    /// Exchange event time (Unix millis).
    pub timestamp: i64,
}

/// Mid-price update for a symbol (from `allMids`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MidPrice {
    pub symbol: String,
    pub price: Decimal,
    pub timestamp: i64,
}

/// External market (Binance) observation retained with receipt time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalVenueQuote {
    pub symbol: String,
    pub venue_symbol: String,
    pub market: ExternalMarket,
    pub exchange_ts: i64,
    pub received_at: DateTime<Utc>,
    pub sequence: u64,
    pub connection_generation: u32,
    pub bid: Option<Price>,
    pub ask: Option<Price>,
    pub mark_price: Option<Price>,
}

/// Stable strategy-facing external-price snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct ExternalReferenceSnapshot {
    pub source: String,
    pub symbol: String,
    pub raw_price: Option<Price>,
    pub adjusted_price: Option<Price>,
    pub basis_bps: Decimal,
    pub effective_weight: Decimal,
    pub confidence: Decimal,
    pub age_ms: u64,
    pub quality: ExternalQuality,
    pub observed_at: DateTime<Utc>,
    pub spot_mid: Option<Price>,
    pub perpetual_mid: Option<Price>,
    pub perpetual_mark: Option<Price>,
    pub sequence: u64,
    pub connection_generation: u32,
    pub quality_reasons: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExternalMarket {
    Spot,
    Perpetual,
    PerpetualMark,
}

impl ExternalMarket {
    pub fn as_str(self) -> &'static str {
        match self {
            ExternalMarket::Spot => "spot",
            ExternalMarket::Perpetual => "perpetual",
            ExternalMarket::PerpetualMark => "perpetual_mark",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalQuality {
    Healthy,
    Degraded,
    Stale,
    Disabled,
}

impl ExternalQuality {
    pub fn as_str(self) -> &'static str {
        match self {
            ExternalQuality::Healthy => "healthy",
            ExternalQuality::Degraded => "degraded",
            ExternalQuality::Stale => "stale",
            ExternalQuality::Disabled => "disabled",
        }
    }
}

// --- Orders ---

/// Strategy's intent to place an order. Input to the risk + execution pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderIntent {
    pub symbol: String,
    pub side: Side,
    pub size: Size,
    /// `None` for market orders.
    pub price: Option<Price>,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub strategy_id: Option<String>,
    pub sub_account: Option<String>,
    pub reduce_only: bool,
    /// Pre-assigned, or auto-generated.
    pub cloid: Option<String>,
    /// Additional client tracking ID.
    pub client_id: Option<String>,
    pub is_spot: bool,
    pub risk_reducing: bool,
    /// 1..=500.
    pub max_slippage_bps: u16,
}

impl OrderIntent {
    /// Validate the `max_slippage_bps` bound (mirrors `__post_init__`).
    pub fn validate(&self) -> Result<(), crate::error::HypeEdgeError> {
        if !(1..=500).contains(&self.max_slippage_bps) {
            return Err(crate::error::HypeEdgeError::Config(
                "max_slippage_bps must be between 1 and 500".into(),
            ));
        }
        Ok(())
    }
}

/// Full order with lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Order {
    pub cloid: String,
    pub symbol: String,
    pub side: Side,
    pub size: Size,
    pub price: Option<Price>,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub status: OrderStatus,
    pub strategy_id: Option<String>,
    pub sub_account: Option<String>,
    pub reduce_only: bool,
    pub is_spot: bool,
    pub risk_reducing: bool,
    pub max_slippage_bps: u16,
    pub exchange_oid: Option<String>,
    pub filled_size: Size,
    pub avg_fill_price: Option<Price>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub filled_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl Order {
    pub fn new(
        cloid: String,
        symbol: String,
        side: Side,
        size: Size,
        price: Option<Price>,
        order_type: OrderType,
        time_in_force: TimeInForce,
    ) -> Self {
        Self {
            cloid,
            symbol,
            side,
            size,
            price,
            order_type,
            time_in_force,
            status: OrderStatus::Pending,
            strategy_id: None,
            sub_account: None,
            reduce_only: false,
            is_spot: false,
            risk_reducing: false,
            max_slippage_bps: 50,
            exchange_oid: None,
            filled_size: Size::ZERO,
            avg_fill_price: None,
            submitted_at: None,
            acknowledged_at: None,
            filled_at: None,
            error_message: None,
            created_at: Utc::now(),
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }

    pub fn remaining_size(&self) -> Size {
        Size::new(self.size.inner() - self.filled_size.inner())
    }
}

/// A single fill (execution) record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fill {
    pub cloid: String,
    pub exchange_oid: String,
    pub symbol: String,
    pub side: Side,
    pub price: Price,
    pub size: Size,
    pub fee: Usd,
    pub is_maker: bool,
    pub timestamp: i64,
    pub strategy_id: Option<String>,
    pub sub_account: Option<String>,
    pub is_spot: bool,
}

// --- Account ---

/// Current position for a symbol. `size > 0` = long, `< 0` = short.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    pub symbol: String,
    pub size: Size,
    pub entry_price: Option<Price>,
    pub mark_price: Option<Price>,
    pub unrealized_pnl: Option<Usd>,
    pub leverage: u32,
    pub liquidation_price: Option<Price>,
    pub sub_account: Option<String>,
    pub strategy_id: Option<String>,
}

impl Position {
    pub fn is_long(&self) -> bool {
        self.size.inner().is_positive()
    }
    pub fn is_short(&self) -> bool {
        self.size.inner().is_negative()
    }
    pub fn is_flat(&self) -> bool {
        self.size.is_zero()
    }
}

/// Authoritative Hyperliquid spot token balance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotBalance {
    pub token: String,
    pub total: Size,
    pub hold: Size,
    pub entry_ntl: Usd,
    pub sub_account: Option<String>,
    pub updated_at: DateTime<Utc>,
}

impl SpotBalance {
    pub fn available(&self) -> Size {
        let available = self.total.inner() - self.hold.inner();
        Size::new(if available.is_negative() {
            Decimal::ZERO
        } else {
            available
        })
    }
}

/// Account balance and state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountState {
    pub equity: Usd,
    pub available_balance: Usd,
    pub total_margin_used: Usd,
    pub total_unrealized_pnl: Usd,
    pub peak_equity: Usd,
    pub sub_account: Option<String>,
}

impl AccountState {
    /// Current drawdown from peak equity as a fraction (0.0 = at peak).
    pub fn drawdown_pct(&self) -> f64 {
        if self.peak_equity.inner() <= Decimal::ZERO {
            return 0.0;
        }
        let ratio = Decimal::ONE - self.equity.inner().div(self.peak_equity.inner());
        ratio.to_string().parse::<f64>().unwrap_or(0.0).max(0.0)
    }
}

// --- Risk ---

/// Result of a risk check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiskCheckResult {
    pub passed: bool,
    pub reason: Option<String>,
    pub checked_limits: Vec<String>,
}

/// Configurable risk limits (design doc §8.1).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RiskLimits {
    /// Max position as % of equity.
    pub max_position_pct: f64,
    /// Max loss per strategy as % of equity.
    pub max_strategy_loss_pct: f64,
    /// Max total drawdown from peak (triggers shutdown).
    pub max_drawdown_pct: f64,
    /// Max effective leverage.
    pub max_leverage: u32,
    /// Risk check timeout (fail-safe).
    pub timeout_ms: u64,
    /// Must exceed the current 5-minute reconciliation cadence.
    pub account_stale_seconds: f64,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_position_pct: 0.20,
            max_strategy_loss_pct: 0.05,
            max_drawdown_pct: 0.10,
            max_leverage: 5,
            timeout_ms: 500,
            account_stale_seconds: 360.0,
        }
    }
}

// --- Signal ---

/// Strategy signal output.
#[derive(Debug, Clone, PartialEq)]
pub struct Signal {
    pub strategy_id: String,
    pub symbol: String,
    /// "buy", "sell", "close", "cancel_all".
    pub action: String,
    pub size: Option<Size>,
    pub price: Option<Price>,
    /// 0.0-1.0.
    pub confidence: Option<f64>,
    pub metadata: serde_json::Value,
    pub timestamp: DateTime<Utc>,
}

// --- Market-making analytics (append-only ClickHouse projections) ---

/// One versioned feature/model evaluation sample.
#[derive(Debug, Clone, PartialEq)]
pub struct MmFeatureSample {
    pub ts: DateTime<Utc>,
    pub strategy_id: String,
    pub symbol: String,
    pub session_id: String,
    pub config_version: u64,
    pub model_version: String,
    pub market_version: u64,
    pub exchange_ts: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub mid_px: Price,
    pub microprice: Price,
    pub fair_px: Price,
    pub best_bid_px: Price,
    pub best_ask_px: Price,
    pub normalized_ofi_l1: Decimal,
    pub normalized_ofi_l5: Decimal,
    pub trade_flow: Decimal,
    pub short_return: Decimal,
    pub volatility_1s: Decimal,
    pub volatility_5s: Decimal,
    pub volatility_30s: Decimal,
    pub volatility_5m: Decimal,
    pub toxicity: Decimal,
    pub receipt_to_decision_us: u32,
    pub event_loop_lag_us: u32,
}

/// A quote-set decision, including KEEP and NO_QUOTE outcomes.
#[derive(Debug, Clone, PartialEq)]
pub struct MmQuoteDecision {
    pub ts: DateTime<Utc>,
    pub strategy_id: String,
    pub symbol: String,
    pub session_id: String,
    pub config_version: u64,
    pub model_version: String,
    pub quote_revision: u64,
    pub market_version: u64,
    pub decision: String,
    pub reason: String,
    pub fair_px: Price,
    pub reservation_px: Price,
    pub desired_bid_px: Option<Price>,
    pub desired_bid_size: Option<Size>,
    pub desired_ask_px: Option<Price>,
    pub desired_ask_size: Option<Size>,
    pub live_bid_px: Option<Price>,
    pub live_bid_size: Option<Size>,
    pub live_ask_px: Option<Price>,
    pub live_ask_size: Option<Size>,
    pub position_size: Size,
    pub inventory_notional_usdc: Usd,
    pub budget_mode: ActionBudgetMode,
    pub expected_gross_edge_usdc: Usd,
    pub adverse_selection_cost_usdc: Usd,
    pub inventory_cost_usdc: Usd,
    pub funding_cost_usdc: Usd,
    pub action_cost_usdc: Usd,
    pub failure_cost_usdc: Usd,
    pub expected_net_pnl_usdc: Usd,
}

/// Low-frequency inventory and margin risk sample.
#[derive(Debug, Clone, PartialEq)]
pub struct MmInventorySample {
    pub ts: DateTime<Utc>,
    pub strategy_id: String,
    pub symbol: String,
    pub session_id: String,
    pub position_size: Size,
    pub mark_px: Price,
    pub inventory_notional_usdc: Usd,
    pub soft_limit_utilization: Decimal,
    pub hard_limit_utilization: Decimal,
    pub emergency_limit_utilization: Decimal,
    pub equity_usdc: Usd,
    pub available_balance_usdc: Usd,
    pub margin_used_usdc: Usd,
    pub liquidation_distance_bps: Option<Decimal>,
    pub funding_carry_usdc: Usd,
    pub reduce_only: bool,
    pub healthy: bool,
}

/// Remote and shadow action-credit sustainability sample.
#[derive(Debug, Clone, PartialEq)]
pub struct MmActionCreditSample {
    pub ts: DateTime<Utc>,
    pub strategy_id: String,
    pub symbol: String,
    pub quota_owner: String,
    pub remote_remaining: i64,
    pub shadow_remaining: i64,
    pub cancel_headroom: i64,
    pub ip_weight_remaining: i64,
    pub actions_burned_1h: u64,
    pub actions_earned_1h: u64,
    pub actions_burned_24h: u64,
    pub actions_earned_24h: u64,
    pub fills_1h: u64,
    pub usdc_volume_1h: Usd,
    pub usdc_per_action_1h: Decimal,
    pub usdc_per_action_24h: Decimal,
    pub runway_hours: Option<Decimal>,
    pub soft_allocation: u64,
    pub hard_allocation: u64,
    pub emergency_reserve: u64,
    pub mode: ActionBudgetMode,
    pub remote_observed_at: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub calculation_version: String,
}

/// Immutable execution-quality markout for one fill and one horizon.
#[derive(Debug, Clone, PartialEq)]
pub struct MmFillMarkout {
    pub ts: DateTime<Utc>,
    pub strategy_id: String,
    pub symbol: String,
    pub session_id: String,
    pub fill_id: String,
    pub order_id: String,
    pub cloid: String,
    pub fill_ts: DateTime<Utc>,
    pub side: Side,
    pub fill_px: Price,
    pub fill_size: Size,
    pub reference: String,
    pub reference_px: Price,
    pub horizon_ms: u32,
    pub horizon_ts: DateTime<Utc>,
    pub mark_px: Price,
    pub signed_markout_bps: Decimal,
    pub signed_markout_usdc: Usd,
    pub spread_capture_usdc: Usd,
    pub maker: bool,
    pub queue_ahead_size: Option<Size>,
    pub fill_probability: Option<Decimal>,
    pub calculation_version: String,
}

// --- System / control payloads ---

/// Payload of `KillSwitchTriggered`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KillSwitchData {
    pub reason: Option<String>,
}

/// Payload of `WsConnected` / `WsDisconnected`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsStatus {
    pub url: Option<String>,
    pub connection_generation: Option<u32>,
    pub error: Option<String>,
}

/// Result of a completed reconciliation run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationResult {
    pub succeeded: bool,
    pub diff_count: u64,
    pub open_orders_checked: u64,
    pub positions_checked: u64,
    pub spot_balances_checked: u64,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

/// Payload of `ActionCreditsLow`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionCreditsData {
    pub remaining: i64,
    pub watermark: i64,
}
