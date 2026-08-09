//! Typed domain events mirroring `src/hypeedge/core/events.py`.
//!
//! [`DomainEvent`] is a closed enum with one variant per Python event constant
//! (29 total). Consumers match exhaustively, so adding a variant is a
//! compiler-driven change at every `match`. The `.event_type()` string and the
//! lossy/reliable classification are byte-identical to Python's constants and
//! `LOSSY_EVENT_TYPES`.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::models::*;

/// An event payload published on the bus.
///
/// `clippy::large_enum_variant` is allowed: the closed typed-enum design is
/// intentional — payload sizes range from a small `MidPrice` to a full
/// `L2BookSnapshot`, and matching exhaustively is worth the enum size.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum DomainEvent {
    // Market data (lossy).
    L2BookUpdate(L2BookSnapshot),
    TradeUpdate(Trade),
    CandleUpdate(Candle),
    FundingUpdate(FundingRate),
    MidPriceUpdate(MidPrice),
    ExternalReferenceUpdate(ExternalReferenceSnapshot),

    // Market-making analytics (lossy, append-only ClickHouse projections).
    MmFeatureSample(MmFeatureSample),
    MmQuoteDecision(MmQuoteDecision),
    MmInventorySample(MmInventorySample),
    MmActionCreditSample(MmActionCreditSample),
    MmFillMarkout(MmFillMarkout),

    // Execution (reliable).
    OrderSubmitted(Order),
    OrderAcknowledged(Order),
    OrderFilled(Order),
    OrderPartialFill(Order),
    OrderCancelled(Order),
    OrderRejected(Order),
    OrderExpired(Order),

    // Account (reliable).
    PositionChanged(Position),
    BalanceChanged(SpotBalance),
    AccountStateUpdate(AccountState),

    // Strategy (reliable).
    SignalGenerated(Signal),

    // System (reliable).
    RiskCheckPassed(RiskCheckResult),
    RiskCheckFailed(RiskCheckResult),
    KillSwitchTriggered(KillSwitchData),
    ReconciliationComplete(ReconciliationResult),
    ActionCreditsLow(ActionCreditsData),
    WsConnected(WsStatus),
    WsDisconnected(WsStatus),
}

/// The 29 event-type discriminants, mirroring `ALL_EVENT_TYPES`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EventType {
    L2BookUpdate,
    TradeUpdate,
    CandleUpdate,
    FundingUpdate,
    MidPriceUpdate,
    ExternalReferenceUpdate,
    MmFeatureSample,
    MmQuoteDecision,
    MmInventorySample,
    MmActionCreditSample,
    MmFillMarkout,
    OrderSubmitted,
    OrderAcknowledged,
    OrderFilled,
    OrderPartialFill,
    OrderCancelled,
    OrderRejected,
    OrderExpired,
    PositionChanged,
    BalanceChanged,
    AccountStateUpdate,
    SignalGenerated,
    RiskCheckPassed,
    RiskCheckFailed,
    KillSwitchTriggered,
    ReconciliationComplete,
    ActionCreditsLow,
    WsConnected,
    WsDisconnected,
}

impl DomainEvent {
    /// The event-type discriminant.
    pub fn event_type(&self) -> EventType {
        use DomainEvent::*;
        match self {
            L2BookUpdate(_) => EventType::L2BookUpdate,
            TradeUpdate(_) => EventType::TradeUpdate,
            CandleUpdate(_) => EventType::CandleUpdate,
            FundingUpdate(_) => EventType::FundingUpdate,
            MidPriceUpdate(_) => EventType::MidPriceUpdate,
            ExternalReferenceUpdate(_) => EventType::ExternalReferenceUpdate,
            MmFeatureSample(_) => EventType::MmFeatureSample,
            MmQuoteDecision(_) => EventType::MmQuoteDecision,
            MmInventorySample(_) => EventType::MmInventorySample,
            MmActionCreditSample(_) => EventType::MmActionCreditSample,
            MmFillMarkout(_) => EventType::MmFillMarkout,
            OrderSubmitted(_) => EventType::OrderSubmitted,
            OrderAcknowledged(_) => EventType::OrderAcknowledged,
            OrderFilled(_) => EventType::OrderFilled,
            OrderPartialFill(_) => EventType::OrderPartialFill,
            OrderCancelled(_) => EventType::OrderCancelled,
            OrderRejected(_) => EventType::OrderRejected,
            OrderExpired(_) => EventType::OrderExpired,
            PositionChanged(_) => EventType::PositionChanged,
            BalanceChanged(_) => EventType::BalanceChanged,
            AccountStateUpdate(_) => EventType::AccountStateUpdate,
            SignalGenerated(_) => EventType::SignalGenerated,
            RiskCheckPassed(_) => EventType::RiskCheckPassed,
            RiskCheckFailed(_) => EventType::RiskCheckFailed,
            KillSwitchTriggered(_) => EventType::KillSwitchTriggered,
            ReconciliationComplete(_) => EventType::ReconciliationComplete,
            ActionCreditsLow(_) => EventType::ActionCreditsLow,
            WsConnected(_) => EventType::WsConnected,
            WsDisconnected(_) => EventType::WsDisconnected,
        }
    }

    /// Whether this event drops the oldest queued item on a full subscriber
    /// mailbox (lossy) or applies backpressure (reliable). Matches Python's
    /// `LOSSY_EVENT_TYPES` / `RELIABLE_EVENT_TYPES`.
    pub fn is_lossy(&self) -> bool {
        matches!(
            self,
            DomainEvent::L2BookUpdate(_)
                | DomainEvent::TradeUpdate(_)
                | DomainEvent::CandleUpdate(_)
                | DomainEvent::FundingUpdate(_)
                | DomainEvent::MidPriceUpdate(_)
                | DomainEvent::ExternalReferenceUpdate(_)
                | DomainEvent::MmFeatureSample(_)
                | DomainEvent::MmQuoteDecision(_)
                | DomainEvent::MmInventorySample(_)
                | DomainEvent::MmActionCreditSample(_)
                | DomainEvent::MmFillMarkout(_)
        )
    }
}

impl EventType {
    /// The string constant from `events.py` (e.g. `"OrderSubmitted"`). These
    /// exact spellings appear in SSE/WS frames and the durable outbox.
    pub fn as_str(self) -> &'static str {
        use EventType::*;
        match self {
            L2BookUpdate => "L2BookUpdate",
            TradeUpdate => "TradeUpdate",
            CandleUpdate => "CandleUpdate",
            FundingUpdate => "FundingUpdate",
            MidPriceUpdate => "MidPriceUpdate",
            ExternalReferenceUpdate => "ExternalReferenceUpdate",
            MmFeatureSample => "MarketMakerFeatureSample",
            MmQuoteDecision => "MarketMakerQuoteDecision",
            MmInventorySample => "MarketMakerInventorySample",
            MmActionCreditSample => "MarketMakerActionCreditSample",
            MmFillMarkout => "MarketMakerFillMarkout",
            OrderSubmitted => "OrderSubmitted",
            OrderAcknowledged => "OrderAcknowledged",
            OrderFilled => "OrderFilled",
            OrderPartialFill => "OrderPartialFill",
            OrderCancelled => "OrderCancelled",
            OrderRejected => "OrderRejected",
            OrderExpired => "OrderExpired",
            PositionChanged => "PositionChanged",
            BalanceChanged => "BalanceChanged",
            AccountStateUpdate => "AccountStateUpdate",
            SignalGenerated => "SignalGenerated",
            RiskCheckPassed => "RiskCheckPassed",
            RiskCheckFailed => "RiskCheckFailed",
            KillSwitchTriggered => "KillSwitchTriggered",
            ReconciliationComplete => "ReconciliationComplete",
            ActionCreditsLow => "ActionCreditsLow",
            WsConnected => "WsConnected",
            WsDisconnected => "WsDisconnected",
        }
    }
}

/// An event published to the event bus.
#[derive(Debug, Clone)]
pub struct Event {
    pub id: Uuid,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Option<String>,
    pub payload: DomainEvent,
}

impl Event {
    pub fn new(payload: DomainEvent) -> Self {
        Self {
            id: Uuid::new_v4(),
            occurred_at: Utc::now(),
            correlation_id: None,
            payload,
        }
    }

    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    pub fn event_type(&self) -> EventType {
        self.payload.event_type()
    }
}

/// All 29 event-type strings, mirroring `ALL_EVENT_TYPES`.
pub const ALL_EVENT_TYPES: &[&str] = &[
    "L2BookUpdate",
    "TradeUpdate",
    "CandleUpdate",
    "FundingUpdate",
    "MidPriceUpdate",
    "ExternalReferenceUpdate",
    "MarketMakerFeatureSample",
    "MarketMakerQuoteDecision",
    "MarketMakerInventorySample",
    "MarketMakerActionCreditSample",
    "MarketMakerFillMarkout",
    "OrderSubmitted",
    "OrderAcknowledged",
    "OrderFilled",
    "OrderPartialFill",
    "OrderCancelled",
    "OrderRejected",
    "OrderExpired",
    "PositionChanged",
    "BalanceChanged",
    "AccountStateUpdate",
    "SignalGenerated",
    "RiskCheckPassed",
    "RiskCheckFailed",
    "KillSwitchTriggered",
    "ReconciliationComplete",
    "ActionCreditsLow",
    "WsConnected",
    "WsDisconnected",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_29_types_are_covered() {
        assert_eq!(ALL_EVENT_TYPES.len(), 29);
        assert_eq!(EventType::L2BookUpdate.as_str(), "L2BookUpdate");
        assert_eq!(
            EventType::MmActionCreditSample.as_str(),
            "MarketMakerActionCreditSample"
        );
        assert_eq!(
            EventType::ReconciliationComplete.as_str(),
            "ReconciliationComplete"
        );
    }

    #[test]
    fn lossy_classification_matches_python() {
        // Lossy: all 6 market-data + all 5 MM-analytics.
        let lossy = [DomainEvent::L2BookUpdate(crate::models::L2BookSnapshot {
            symbol: "BTC".into(),
            bids: vec![],
            asks: vec![],
            timestamp: 0,
            local_ts: Utc::now(),
            version: 0,
            connection_generation: 0,
        })];
        for e in &lossy {
            assert!(e.is_lossy());
        }
        // Reliable: an execution event.
        let order = crate::models::Order::new(
            "cloid".into(),
            "BTC".into(),
            crate::enums::Side::Buy,
            crate::decimal::Size::ZERO,
            None,
            crate::enums::OrderType::Limit,
            crate::enums::TimeInForce::Gtc,
        );
        assert!(!DomainEvent::OrderSubmitted(order).is_lossy());
    }

    #[test]
    fn event_carries_id_and_timestamp() {
        let ev = Event::new(DomainEvent::ReconciliationComplete(ReconciliationResult {
            succeeded: true,
            diff_count: 0,
            open_orders_checked: 0,
            positions_checked: 0,
            spot_balances_checked: 0,
            error_code: None,
            error_message: None,
        }));
        assert_eq!(ev.event_type(), EventType::ReconciliationComplete);
        assert_eq!(ev.event_type().as_str(), "ReconciliationComplete");
        let _ = ev.id;
        let _ = ev.occurred_at;
    }
}
