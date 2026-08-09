//! Immutable desired-quote models shared by policy and coordinator, port of
//! `src/hypeedge/trading/quotes.py`.

use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::{Decimal, Price, Size, Usd};
use hypeedge_domain::enums::{ActionBudgetMode, OrderStatus, QuoteAction, QuoteDecision, Side};

/// A logical quote slot key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QuoteSlotKey {
    pub strategy_id: String,
    pub symbol: String,
    pub side: Side,
    pub level: u32,
}

/// A desired quote for one slot.
#[derive(Debug, Clone, PartialEq)]
pub struct DesiredQuote {
    pub slot: QuoteSlotKey,
    pub decision: QuoteDecision,
    pub price: Option<Price>,
    pub size: Option<Size>,
    pub gross_edge_usdc: Usd,
    pub reason: String,
}

impl DesiredQuote {
    pub fn validate(&self) -> Result<(), String> {
        let has_quote = self.decision == QuoteDecision::Quote;
        if has_quote != (self.price.is_some() && self.size.is_some()) {
            return Err("QUOTE requires price and size; non-QUOTE decisions must omit them".into());
        }
        if let Some(sz) = self.size
            && sz.inner() <= Decimal::ZERO
        {
            return Err("quote size must be positive".into());
        }
        if let Some(px) = self.price
            && px.inner() <= Decimal::ZERO
        {
            return Err("quote price must be positive".into());
        }
        Ok(())
    }
}

/// A set of desired quotes for a symbol.
#[derive(Debug, Clone, PartialEq)]
pub struct DesiredQuoteSet {
    pub strategy_id: String,
    pub symbol: String,
    pub session_id: String,
    pub config_version: u64,
    pub model_version: String,
    pub market_version: i64,
    pub connection_generation: i64,
    pub current_slot_revision: i64,
    pub revision: i64,
    pub fair_price: Price,
    pub reservation_price: Price,
    pub inventory_notional: Usd,
    pub expected_utility_usdc: Usd,
    pub budget_mode: ActionBudgetMode,
    pub bid: DesiredQuote,
    pub ask: DesiredQuote,
    pub created_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub feature_values: Vec<(String, Decimal)>,
}

impl DesiredQuoteSet {
    pub fn validate(&self) -> Result<(), String> {
        if self.revision < 0 || self.current_slot_revision < 0 || self.market_version < 0 {
            return Err("quote and market revisions cannot be negative".into());
        }
        if self.config_version == 0 {
            return Err("config version must be positive".into());
        }
        if self.valid_until <= self.created_at {
            return Err("quote set validity deadline must be after creation".into());
        }
        for (quote, side) in [(&self.bid, Side::Buy), (&self.ask, Side::Sell)] {
            if quote.slot.strategy_id != self.strategy_id || quote.slot.symbol != self.symbol {
                return Err("quote slot does not belong to quote set".into());
            }
            if quote.slot.side != side {
                return Err("quote slot side does not match quote-set side".into());
            }
        }
        Ok(())
    }
}

/// A risk owner: an order which can still fill.
#[derive(Debug, Clone, PartialEq)]
pub struct QuoteRiskOwner {
    pub order_id: Option<String>,
    pub cloid: String,
    pub price: Price,
    pub remaining_size: Size,
    pub status: OrderStatus,
    pub plan_revision: i64,
    pub live_since: DateTime<Utc>,
    pub exchange_order_id_known: bool,
}

impl QuoteRiskOwner {
    pub fn validate(&self) -> Result<(), String> {
        if self.price.inner() <= Decimal::ZERO || self.remaining_size.inner() <= Decimal::ZERO {
            return Err("risk-owner price and remaining size must be positive".into());
        }
        if self.plan_revision < 0 {
            return Err("risk-owner plan revision cannot be negative".into());
        }
        Ok(())
    }

    pub fn is_unknown(&self) -> bool {
        matches!(
            self.status,
            OrderStatus::SubmitUnknown | OrderStatus::CancelUnknown
        )
    }

    pub fn is_inflight(&self) -> bool {
        matches!(self.status, OrderStatus::Pending | OrderStatus::Submitted)
    }
}

/// The authoritative slot projection.
#[derive(Debug, Clone, PartialEq)]
pub struct QuoteSlotView {
    pub key: QuoteSlotKey,
    pub revision: i64,
    pub plan_revision: i64,
    pub owners: Vec<QuoteRiskOwner>,
    pub last_transition_at: Option<DateTime<Utc>>,
}

impl QuoteSlotView {
    pub fn validate(&self) -> Result<(), String> {
        if self.revision < 0 || self.plan_revision < 0 {
            return Err("slot revisions cannot be negative".into());
        }
        let mut cloids = std::collections::HashSet::new();
        for owner in &self.owners {
            if !cloids.insert(owner.cloid.clone()) {
                return Err("a risk owner may appear only once in a slot".into());
            }
        }
        Ok(())
    }

    pub fn has_unknown(&self) -> bool {
        self.owners.iter().any(|o| o.is_unknown())
    }

    pub fn has_inflight(&self) -> bool {
        self.owners.iter().any(|o| o.is_inflight())
    }

    pub fn has_orphaned_owner(&self) -> bool {
        self.owners
            .iter()
            .any(|o| o.plan_revision != self.plan_revision)
    }

    /// The single current desired owner; raises if more than one.
    pub fn current_owner(&self) -> Result<Option<&QuoteRiskOwner>, String> {
        let matching: Vec<&QuoteRiskOwner> = self
            .owners
            .iter()
            .filter(|o| o.plan_revision == self.plan_revision)
            .collect();
        if matching.len() > 1 {
            return Err("slot has more than one current desired owner".into());
        }
        Ok(matching.first().copied())
    }
}

/// One minimal slot transition.
#[derive(Debug, Clone, PartialEq)]
pub struct QuoteDiff {
    pub slot: QuoteSlotKey,
    pub action: QuoteAction,
    pub source: Option<QuoteRiskOwner>,
    pub desired: DesiredQuote,
    pub child_actions: Vec<String>,
    pub reason: String,
    pub gross_edge_usdc: Usd,
    pub transition_cost_usdc: Usd,
    pub net_incremental_utility_usdc: Usd,
}

impl QuoteDiff {
    pub fn estimated_incremental_actions(&self) -> usize {
        self.child_actions.len()
    }
}

/// A coordinated quote plan.
#[derive(Debug, Clone, PartialEq)]
pub struct QuotePlan {
    pub strategy_id: String,
    pub symbol: String,
    pub session_id: String,
    pub config_version: u64,
    pub revision: i64,
    pub market_version: i64,
    pub connection_generation: i64,
    pub valid_until: DateTime<Utc>,
    pub diffs: Vec<QuoteDiff>,
    pub fair_price: Option<Price>,
    pub reservation_price: Option<Price>,
    pub inventory_notional: Usd,
    pub budget_mode: ActionBudgetMode,
    pub fenced: bool,
    pub fence_reason: Option<String>,
}

impl QuotePlan {
    pub fn estimated_incremental_actions(&self) -> usize {
        self.diffs
            .iter()
            .map(|d| d.estimated_incremental_actions())
            .sum()
    }
}
