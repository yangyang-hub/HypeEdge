//! Domain enumerations mirroring `src/hypeedge/core/enums.py`.
//!
//! The `.as_str()` values are byte-identical to the Python `StrEnum` values and
//! are load-bearing: they cross the Postgres `CheckConstraint` columns and the
//! JSON API boundary unchanged. Do not rename them.

use serde::{Deserialize, Serialize};

/// Implement `Display` as `as_str()` for the enums that carry one.
macro_rules! impl_display_as_str {
    ($($t:ident),+ $(,)?) => {
        $(impl std::fmt::Display for $t {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        })+
    };
}

/// Order side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }
}

impl std::str::FromStr for Side {
    type Err = ();
    fn from_str(s: &str) -> Result<Side, ()> {
        match s {
            "buy" => Ok(Side::Buy),
            "sell" => Ok(Side::Sell),
            _ => Err(()),
        }
    }
}

/// Order type on Hyperliquid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    /// Limit order.
    Limit,
    /// Market order (implemented as an aggressive limit).
    Market,
    StopMarket,
    StopLimit,
}

impl OrderType {
    pub fn as_str(self) -> &'static str {
        match self {
            OrderType::Limit => "limit",
            OrderType::Market => "market",
            OrderType::StopMarket => "stop_market",
            OrderType::StopLimit => "stop_limit",
        }
    }
}

impl std::str::FromStr for OrderType {
    type Err = ();
    fn from_str(s: &str) -> Result<OrderType, ()> {
        match s {
            "limit" => Ok(OrderType::Limit),
            "market" => Ok(OrderType::Market),
            "stop_market" => Ok(OrderType::StopMarket),
            "stop_limit" => Ok(OrderType::StopLimit),
            _ => Err(()),
        }
    }
}

/// Time-in-force for orders. Values are the Hyperliquid SDK spellings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TimeInForce {
    /// Good till cancelled.
    Gtc,
    /// Immediate or cancel.
    Ioc,
    /// Add liquidity only (post-only).
    Alo,
    /// Good till crossing (post-only variant).
    Gtx,
}

impl TimeInForce {
    pub fn as_str(self) -> &'static str {
        match self {
            TimeInForce::Gtc => "Gtc",
            TimeInForce::Ioc => "Ioc",
            TimeInForce::Alo => "Alo",
            TimeInForce::Gtx => "Gtx",
        }
    }
}

impl std::str::FromStr for TimeInForce {
    type Err = ();
    fn from_str(s: &str) -> Result<TimeInForce, ()> {
        match s {
            "Gtc" => Ok(TimeInForce::Gtc),
            "Ioc" => Ok(TimeInForce::Ioc),
            "Alo" => Ok(TimeInForce::Alo),
            "Gtx" => Ok(TimeInForce::Gtx),
            _ => Err(()),
        }
    }
}

/// Order lifecycle states (see design doc §9.2). The enum omits
/// `cancel_pending` (present in the Postgres column constraint but not a
/// first-class domain state in Python).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    /// Strategy intent, not yet submitted.
    Pending,
    /// Sent to exchange, awaiting ack.
    Submitted,
    /// Timed out; exchange truth not yet known.
    SubmitUnknown,
    /// Exchange confirmed, resting on book.
    Acknowledged,
    /// Partially filled.
    PartialFill,
    /// Cancel requested; exchange truth not yet known.
    CancelUnknown,
    /// Fully filled.
    Filled,
    /// Cancelled (strategy or engine).
    Cancelled,
    /// Exchange rejected.
    Rejected,
    /// expiresAfter triggered (5x penalty!).
    Expired,
}

impl OrderStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            OrderStatus::Pending => "pending",
            OrderStatus::Submitted => "submitted",
            OrderStatus::SubmitUnknown => "submit_unknown",
            OrderStatus::Acknowledged => "acknowledged",
            OrderStatus::PartialFill => "partial_fill",
            OrderStatus::CancelUnknown => "cancel_unknown",
            OrderStatus::Filled => "filled",
            OrderStatus::Cancelled => "cancelled",
            OrderStatus::Rejected => "rejected",
            OrderStatus::Expired => "expired",
        }
    }

    /// Whether this is a terminal state (no outgoing transitions).
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            OrderStatus::Filled
                | OrderStatus::Cancelled
                | OrderStatus::Rejected
                | OrderStatus::Expired
        )
    }

    /// Whether the transition `self -> to` is legal per `ORDER_TRANSITIONS`.
    pub fn can_transition(self, to: OrderStatus) -> bool {
        use OrderStatus::*;
        matches!(
            (self, to),
            (Pending, Submitted | Rejected | Cancelled)
                | (
                    Submitted,
                    Acknowledged | SubmitUnknown | Rejected | Filled | Cancelled | CancelUnknown
                )
                | (
                    SubmitUnknown,
                    Acknowledged | PartialFill | Filled | Cancelled | CancelUnknown | Rejected
                )
                | (
                    Acknowledged,
                    PartialFill | Filled | Cancelled | CancelUnknown | Expired
                )
                | (
                    PartialFill,
                    PartialFill | Filled | Cancelled | CancelUnknown | Expired
                )
                | (
                    CancelUnknown,
                    Acknowledged | PartialFill | Filled | Cancelled | Rejected | Expired
                )
        )
    }
}

impl std::str::FromStr for OrderStatus {
    type Err = ();
    fn from_str(s: &str) -> Result<OrderStatus, ()> {
        use OrderStatus::*;
        match s {
            "pending" => Ok(Pending),
            "submitted" => Ok(Submitted),
            "submit_unknown" => Ok(SubmitUnknown),
            "acknowledged" => Ok(Acknowledged),
            "partial_fill" => Ok(PartialFill),
            "cancel_unknown" => Ok(CancelUnknown),
            "filled" => Ok(Filled),
            "cancelled" => Ok(Cancelled),
            "rejected" => Ok(Rejected),
            "expired" => Ok(Expired),
            _ => Err(()),
        }
    }
}

/// Strategy lifecycle states (legacy `StrategyStatus`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyStatus {
    Stopped,
    Starting,
    Running,
    /// Degraded mode (e.g. risk data unavailable).
    Paused,
    Error,
    Stopping,
}

impl StrategyStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            StrategyStatus::Stopped => "stopped",
            StrategyStatus::Starting => "starting",
            StrategyStatus::Running => "running",
            StrategyStatus::Paused => "paused",
            StrategyStatus::Error => "error",
            StrategyStatus::Stopping => "stopping",
        }
    }
}

/// Persistent market-maker instance lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketMakerLifecycle {
    Stopped,
    Warming,
    Shadow,
    Running,
    Paused,
    Draining,
    Faulted,
}

impl MarketMakerLifecycle {
    pub fn as_str(self) -> &'static str {
        match self {
            MarketMakerLifecycle::Stopped => "stopped",
            MarketMakerLifecycle::Warming => "warming",
            MarketMakerLifecycle::Shadow => "shadow",
            MarketMakerLifecycle::Running => "running",
            MarketMakerLifecycle::Paused => "paused",
            MarketMakerLifecycle::Draining => "draining",
            MarketMakerLifecycle::Faulted => "faulted",
        }
    }
}

impl std::str::FromStr for MarketMakerLifecycle {
    type Err = ();
    fn from_str(s: &str) -> Result<MarketMakerLifecycle, ()> {
        use MarketMakerLifecycle::*;
        match s {
            "stopped" => Ok(Stopped),
            "warming" => Ok(Warming),
            "shadow" => Ok(Shadow),
            "running" => Ok(Running),
            "paused" => Ok(Paused),
            "draining" => Ok(Draining),
            "faulted" => Ok(Faulted),
            _ => Err(()),
        }
    }
}

/// Durable two-leg funding-arbitrage execution states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FundingArbCycleState {
    EnteringSpot,
    EnteringPerp,
    CompensatingEntry,
    Open,
    Rebalancing,
    ExitingPerp,
    ExitingSpot,
    Closed,
    Faulted,
}

impl FundingArbCycleState {
    pub fn as_str(self) -> &'static str {
        match self {
            FundingArbCycleState::EnteringSpot => "entering_spot",
            FundingArbCycleState::EnteringPerp => "entering_perp",
            FundingArbCycleState::CompensatingEntry => "compensating_entry",
            FundingArbCycleState::Open => "open",
            FundingArbCycleState::Rebalancing => "rebalancing",
            FundingArbCycleState::ExitingPerp => "exiting_perp",
            FundingArbCycleState::ExitingSpot => "exiting_spot",
            FundingArbCycleState::Closed => "closed",
            FundingArbCycleState::Faulted => "faulted",
        }
    }
}

impl std::str::FromStr for FundingArbCycleState {
    type Err = ();
    fn from_str(s: &str) -> Result<FundingArbCycleState, ()> {
        use FundingArbCycleState::*;
        match s {
            "entering_spot" => Ok(EnteringSpot),
            "entering_perp" => Ok(EnteringPerp),
            "compensating_entry" => Ok(CompensatingEntry),
            "open" => Ok(Open),
            "rebalancing" => Ok(Rebalancing),
            "exiting_perp" => Ok(ExitingPerp),
            "exiting_spot" => Ok(ExitingSpot),
            "closed" => Ok(Closed),
            "faulted" => Ok(Faulted),
            _ => Err(()),
        }
    }
}

/// Desired decision for a logical quote slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteDecision {
    Quote,
    Keep,
    NoQuote,
}

/// Minimal transition from authoritative live state to desired state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteAction {
    Keep,
    Place,
    Cancel,
    Modify,
    CancelThenPlace,
    NoAction,
    BlockedUnknown,
}

/// Address action-budget operating mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionBudgetMode {
    Normal,
    Conserve,
    Critical,
    CancelOnly,
    Exhausted,
}

impl ActionBudgetMode {
    pub fn as_str(self) -> &'static str {
        match self {
            ActionBudgetMode::Normal => "normal",
            ActionBudgetMode::Conserve => "conserve",
            ActionBudgetMode::Critical => "critical",
            ActionBudgetMode::CancelOnly => "cancel_only",
            ActionBudgetMode::Exhausted => "exhausted",
        }
    }
}

/// Global trading permission state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SafetyMode {
    Starting,
    Reconciling,
    Normal,
    ReduceOnly,
    CancelOnly,
    Halting,
    Halted,
    Recovering,
}

impl SafetyMode {
    pub fn as_str(self) -> &'static str {
        match self {
            SafetyMode::Starting => "starting",
            SafetyMode::Reconciling => "reconciling",
            SafetyMode::Normal => "normal",
            SafetyMode::ReduceOnly => "reduce_only",
            SafetyMode::CancelOnly => "cancel_only",
            SafetyMode::Halting => "halting",
            SafetyMode::Halted => "halted",
            SafetyMode::Recovering => "recovering",
        }
    }
}

impl std::str::FromStr for SafetyMode {
    type Err = ();
    fn from_str(s: &str) -> Result<SafetyMode, ()> {
        use SafetyMode::*;
        match s {
            "starting" => Ok(Starting),
            "reconciling" => Ok(Reconciling),
            "normal" => Ok(Normal),
            "reduce_only" => Ok(ReduceOnly),
            "cancel_only" => Ok(CancelOnly),
            "halting" => Ok(Halting),
            "halted" => Ok(Halted),
            "recovering" => Ok(Recovering),
            _ => Err(()),
        }
    }
}

/// Margin mode for sub-accounts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarginMode {
    Cross,
    Isolated,
}

/// Hyperliquid WebSocket subscription channels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WsChannel {
    L2Book,
    Trades,
    Candle,
    AllMids,
    ActiveAssetCtx,
    ActiveSpotAssetCtx,
    /// Phase-2 reserved (authenticated).
    UserFills,
    /// Phase-2 reserved (authenticated).
    OrderUpdates,
}

impl WsChannel {
    pub fn as_str(self) -> &'static str {
        match self {
            WsChannel::L2Book => "l2Book",
            WsChannel::Trades => "trades",
            WsChannel::Candle => "candle",
            WsChannel::AllMids => "allMids",
            WsChannel::ActiveAssetCtx => "activeAssetCtx",
            WsChannel::ActiveSpotAssetCtx => "activeSpotAssetCtx",
            WsChannel::UserFills => "userFills",
            WsChannel::OrderUpdates => "orderUpdates",
        }
    }
}

/// The strategy type discriminant for the multi-strategy control plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyType {
    MarketMaker,
    TrendFollow,
    FundingArb,
    Legacy,
}

impl StrategyType {
    pub fn as_str(self) -> &'static str {
        match self {
            StrategyType::MarketMaker => "market_maker",
            StrategyType::TrendFollow => "trend_follow",
            StrategyType::FundingArb => "funding_arb",
            StrategyType::Legacy => "legacy",
        }
    }
}

impl std::str::FromStr for StrategyType {
    type Err = ();
    fn from_str(s: &str) -> Result<StrategyType, ()> {
        match s {
            "market_maker" => Ok(StrategyType::MarketMaker),
            "trend_follow" => Ok(StrategyType::TrendFollow),
            "funding_arb" => Ok(StrategyType::FundingArb),
            "legacy" => Ok(StrategyType::Legacy),
            _ => Err(()),
        }
    }
}

/// System safety states (superset of `SafetyMode`, used by the durable
/// `system_state` table).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SystemState {
    Starting,
    Reconciling,
    Normal,
    ReduceOnly,
    CancelOnly,
    Halting,
    Halted,
    Recovering,
    Stopping,
}

impl SystemState {
    pub fn as_str(self) -> &'static str {
        match self {
            SystemState::Starting => "starting",
            SystemState::Reconciling => "reconciling",
            SystemState::Normal => "normal",
            SystemState::ReduceOnly => "reduce_only",
            SystemState::CancelOnly => "cancel_only",
            SystemState::Halting => "halting",
            SystemState::Halted => "halted",
            SystemState::Recovering => "recovering",
            SystemState::Stopping => "stopping",
        }
    }
}

impl_display_as_str!(
    Side,
    OrderType,
    TimeInForce,
    OrderStatus,
    StrategyStatus,
    MarketMakerLifecycle,
    FundingArbCycleState,
    ActionBudgetMode,
    SafetyMode,
    WsChannel,
    StrategyType,
    SystemState,
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_transition_table_matches_python() {
        // Spot-check the exact Python ORDER_TRANSITIONS.
        assert!(OrderStatus::Pending.can_transition(OrderStatus::Submitted));
        assert!(OrderStatus::Pending.can_transition(OrderStatus::Rejected));
        assert!(OrderStatus::Pending.can_transition(OrderStatus::Cancelled));
        assert!(!OrderStatus::Pending.can_transition(OrderStatus::Filled));
        assert!(!OrderStatus::Pending.can_transition(OrderStatus::Expired));

        assert!(OrderStatus::Acknowledged.can_transition(OrderStatus::PartialFill));
        assert!(OrderStatus::Acknowledged.can_transition(OrderStatus::Filled));
        assert!(OrderStatus::Acknowledged.can_transition(OrderStatus::Cancelled));
        assert!(OrderStatus::Acknowledged.can_transition(OrderStatus::CancelUnknown));
        assert!(OrderStatus::Acknowledged.can_transition(OrderStatus::Expired));
        assert!(!OrderStatus::Acknowledged.can_transition(OrderStatus::Rejected));

        assert!(OrderStatus::PartialFill.can_transition(OrderStatus::PartialFill));
        assert!(OrderStatus::PartialFill.can_transition(OrderStatus::Filled));
        assert!(!OrderStatus::PartialFill.can_transition(OrderStatus::Rejected));
    }

    #[test]
    fn terminal_states_have_no_outgoing() {
        for s in [
            OrderStatus::Filled,
            OrderStatus::Cancelled,
            OrderStatus::Rejected,
            OrderStatus::Expired,
        ] {
            assert!(s.is_terminal());
            for t in [
                OrderStatus::Pending,
                OrderStatus::Submitted,
                OrderStatus::SubmitUnknown,
                OrderStatus::Acknowledged,
                OrderStatus::PartialFill,
                OrderStatus::CancelUnknown,
            ] {
                assert!(!s.can_transition(t), "{s:?} -> {t:?} must be illegal");
            }
        }
    }

    #[test]
    fn every_non_terminal_state_can_reach_something() {
        for s in [
            OrderStatus::Pending,
            OrderStatus::Submitted,
            OrderStatus::SubmitUnknown,
            OrderStatus::Acknowledged,
            OrderStatus::PartialFill,
            OrderStatus::CancelUnknown,
        ] {
            let mut any = false;
            for t in [
                OrderStatus::Pending,
                OrderStatus::Submitted,
                OrderStatus::SubmitUnknown,
                OrderStatus::Acknowledged,
                OrderStatus::PartialFill,
                OrderStatus::CancelUnknown,
                OrderStatus::Filled,
                OrderStatus::Cancelled,
                OrderStatus::Rejected,
                OrderStatus::Expired,
            ] {
                any |= s.can_transition(t);
            }
            assert!(any, "{s:?} must have at least one outgoing transition");
        }
    }

    #[test]
    fn enum_str_roundtrips() {
        assert_eq!(Side::Buy.as_str(), "buy");
        assert_eq!(Side::Sell.as_str(), "sell");
        assert_eq!(TimeInForce::Gtc.as_str(), "Gtc");
        assert_eq!(OrderStatus::PartialFill.as_str(), "partial_fill");
        assert_eq!(MarketMakerLifecycle::Faulted.as_str(), "faulted");
        assert_eq!(
            FundingArbCycleState::CompensatingEntry.as_str(),
            "compensating_entry"
        );
        assert_eq!(SafetyMode::CancelOnly.as_str(), "cancel_only");
        assert_eq!(StrategyType::MarketMaker.as_str(), "market_maker");
        assert_eq!(SystemState::Recovering.as_str(), "recovering");
        assert_eq!(
            "cancelled".parse::<OrderStatus>().unwrap(),
            OrderStatus::Cancelled
        );
    }
}
