//! Pure desired-vs-authoritative quote reconciliation, port of
//! `src/hypeedge/trading/quote_coordinator.py`.

use chrono::{DateTime, Duration, Utc};
use hypeedge_domain::decimal::{Decimal, Size, Usd};
use hypeedge_domain::enums::{QuoteAction, QuoteDecision, Side};

use crate::trading::quotes::{
    DesiredQuote, DesiredQuoteSet, QuoteDiff, QuotePlan, QuoteRiskOwner, QuoteSlotView,
};

/// Coordinator tuning knobs.
#[derive(Debug, Clone)]
pub struct QuoteCoordinatorConfig {
    pub min_quote_lifetime: Duration,
    pub refresh_cooldown: Duration,
    pub max_quote_age: Duration,
    pub price_hysteresis_ticks: u64,
    pub size_hysteresis: Size,
    pub replace_hysteresis_usdc: Usd,
    pub action_shadow_cost_usdc: Usd,
    pub failure_tail_cost_per_action_usdc: Usd,
    pub modify_enabled: bool,
}

impl Default for QuoteCoordinatorConfig {
    fn default() -> Self {
        Self {
            min_quote_lifetime: Duration::milliseconds(500),
            refresh_cooldown: Duration::milliseconds(100),
            max_quote_age: Duration::seconds(15),
            price_hysteresis_ticks: 1,
            size_hysteresis: Size::ZERO,
            replace_hysteresis_usdc: Usd::ZERO,
            action_shadow_cost_usdc: Usd::ZERO,
            failure_tail_cost_per_action_usdc: Usd::ZERO,
            modify_enabled: false,
        }
    }
}

impl QuoteCoordinatorConfig {
    pub fn validate(&self) -> Result<(), String> {
        let zero = Duration::zero();
        if self.min_quote_lifetime < zero
            || self.refresh_cooldown < zero
            || self.max_quote_age < zero
        {
            return Err("quote timing controls cannot be negative".into());
        }
        if self.replace_hysteresis_usdc.inner() < Decimal::ZERO {
            return Err("replace hysteresis cannot be negative".into());
        }
        if self.action_shadow_cost_usdc.inner() < Decimal::ZERO
            || self.failure_tail_cost_per_action_usdc.inner() < Decimal::ZERO
        {
            return Err("transition costs cannot be negative".into());
        }
        if self.modify_enabled {
            return Err(
                "MODIFY is disabled until exchange recovery semantics are authoritative".into(),
            );
        }
        Ok(())
    }
}

/// Compute a minimum-action plan without mutating slot state.
pub struct QuoteCoordinator {
    config: QuoteCoordinatorConfig,
}

impl QuoteCoordinator {
    pub fn new(config: QuoteCoordinatorConfig) -> Result<Self, String> {
        config.validate()?;
        Ok(Self { config })
    }

    pub fn coordinate(
        &self,
        desired: &DesiredQuoteSet,
        bid_view: &QuoteSlotView,
        ask_view: &QuoteSlotView,
        tick_size: Decimal,
        now: DateTime<Utc>,
    ) -> Result<QuotePlan, String> {
        Self::validate_views(desired, bid_view, ask_view)?;
        // L-ST1: never place a crossing quote — downgrade crossed sides to
        // NoQuote (which cancels any live owner) instead of placing them.
        let working = Self::reject_crossed(desired);
        if let Some(fence_reason) = Self::fence_reason(&working, bid_view, ask_view, now) {
            // H-MM1: protective cancels outrank fences. A NoQuote intent must
            // still produce Cancel diffs so an expired/stale candidate cannot
            // pin live quotes on the book; only new placements are suppressed.
            let diffs = self.fenced_diffs(&working, bid_view, ask_view)?;
            return Ok(fenced_plan(&working, fence_reason, diffs));
        }
        if tick_size <= Decimal::ZERO {
            return Err("tick size must be positive".into());
        }
        let diffs = vec![
            self.diff_slot(&working.bid, bid_view, tick_size, now)?,
            self.diff_slot(&working.ask, ask_view, tick_size, now)?,
        ];
        Ok(QuotePlan {
            strategy_id: working.strategy_id.clone(),
            symbol: working.symbol.clone(),
            session_id: working.session_id.clone(),
            config_version: working.config_version,
            revision: working.revision,
            market_version: working.market_version,
            connection_generation: working.connection_generation,
            valid_until: working.valid_until,
            diffs,
            fair_price: Some(working.fair_price),
            reservation_price: Some(working.reservation_price),
            inventory_notional: working.inventory_notional,
            budget_mode: working.budget_mode,
            fenced: false,
            fence_reason: None,
        })
    }

    /// L-ST1: if both sides desire a quote and bid >= ask, downgrade both to
    /// NoQuote and warn. Returning a plan with cancels is safer than erroring:
    /// a coordinator error would leave live quotes on a crossed book.
    fn reject_crossed(desired: &DesiredQuoteSet) -> DesiredQuoteSet {
        let crossed = desired.bid.decision == QuoteDecision::Quote
            && desired.ask.decision == QuoteDecision::Quote
            && desired
                .bid
                .price
                .zip(desired.ask.price)
                .is_some_and(|(bid, ask)| bid.inner() >= ask.inner());
        if !crossed {
            return desired.clone();
        }
        tracing::warn!(
            strategy_id = %desired.strategy_id,
            symbol = %desired.symbol,
            bid_price = ?desired.bid.price.map(|p| p.to_string()),
            ask_price = ?desired.ask.price.map(|p| p.to_string()),
            "market_maker_crossed_quote_rejected"
        );
        let mut working = desired.clone();
        working.bid = no_quote_desired(working.bid, "crossed_quote_rejected");
        working.ask = no_quote_desired(working.ask, "crossed_quote_rejected");
        working
    }

    /// Diffs allowed while fenced: only protective cancels (plus blocked-unknown
    /// markers for reconciliation). New placements are suppressed by the fence.
    fn fenced_diffs(
        &self,
        desired: &DesiredQuoteSet,
        bid_view: &QuoteSlotView,
        ask_view: &QuoteSlotView,
    ) -> Result<Vec<QuoteDiff>, String> {
        let mut diffs = Vec::new();
        for (quote, view) in [(&desired.bid, bid_view), (&desired.ask, ask_view)] {
            if view.has_unknown() || view.has_orphaned_owner() {
                let reason = if view.has_unknown() {
                    "unknown_risk_owner_requires_reconciliation"
                } else {
                    "orphaned_live_owner_requires_recovery"
                };
                diffs.push(self.make_diff(
                    quote,
                    view.current_owner()?.cloned(),
                    QuoteAction::BlockedUnknown,
                    vec![],
                    reason,
                ));
                continue;
            }
            if quote.decision != QuoteDecision::NoQuote {
                continue; // fence suppresses new placements
            }
            match view.current_owner()? {
                Some(owner) => diffs.push(self.make_diff(
                    quote,
                    Some(owner.clone()),
                    QuoteAction::Cancel,
                    vec!["cancel".into()],
                    &quote.reason,
                )),
                None => diffs.push(self.make_diff(
                    quote,
                    None,
                    QuoteAction::NoAction,
                    vec![],
                    &quote.reason,
                )),
            }
        }
        Ok(diffs)
    }

    fn validate_views(
        desired: &DesiredQuoteSet,
        bid_view: &QuoteSlotView,
        ask_view: &QuoteSlotView,
    ) -> Result<(), String> {
        for (side, view) in [(Side::Buy, bid_view), (Side::Sell, ask_view)] {
            if view.key.strategy_id != desired.strategy_id
                || view.key.symbol != desired.symbol
                || view.key.side != side
            {
                return Err("authoritative slot view does not belong to desired quote set".into());
            }
            view.current_owner()?; // validate the at-most-one-current-owner invariant
        }
        Ok(())
    }

    fn fence_reason(
        desired: &DesiredQuoteSet,
        bid_view: &QuoteSlotView,
        ask_view: &QuoteSlotView,
        now: DateTime<Utc>,
    ) -> Option<String> {
        if now >= desired.valid_until {
            return Some("candidate_expired".into());
        }
        if desired.revision <= bid_view.plan_revision.max(ask_view.plan_revision) {
            return Some("stale_plan_revision".into());
        }
        if desired.current_slot_revision != bid_view.revision.max(ask_view.revision) {
            return Some("slot_revision_mismatch".into());
        }
        None
    }

    fn diff_slot(
        &self,
        desired: &DesiredQuote,
        view: &QuoteSlotView,
        tick_size: Decimal,
        now: DateTime<Utc>,
    ) -> Result<QuoteDiff, String> {
        let owner = view.current_owner()?;
        if view.has_unknown() || view.has_orphaned_owner() {
            let reason = if view.has_unknown() {
                "unknown_risk_owner_requires_reconciliation"
            } else {
                "orphaned_live_owner_requires_recovery"
            };
            return Ok(self.make_diff(
                desired,
                owner.cloned(),
                QuoteAction::BlockedUnknown,
                vec![],
                reason,
            ));
        }
        if view.has_inflight() {
            return Ok(self.make_diff(
                desired,
                owner.cloned(),
                QuoteAction::Keep,
                vec![],
                "child_action_inflight",
            ));
        }
        if desired.decision == QuoteDecision::Keep {
            let action = if owner.is_some() {
                QuoteAction::Keep
            } else {
                QuoteAction::NoAction
            };
            return Ok(self.make_diff(desired, owner.cloned(), action, vec![], "policy_keep"));
        }
        if desired.decision == QuoteDecision::NoQuote {
            if owner.is_none() {
                return Ok(self.make_diff(
                    desired,
                    None,
                    QuoteAction::NoAction,
                    vec![],
                    &desired.reason,
                ));
            }
            return Ok(self.make_diff(
                desired,
                owner.cloned(),
                QuoteAction::Cancel,
                vec!["cancel".into()],
                &desired.reason,
            ));
        }
        if owner.is_none() {
            return Ok(self.make_diff(
                desired,
                None,
                QuoteAction::Place,
                vec!["place".into()],
                &desired.reason,
            ));
        }
        // QUOTE with an existing owner.
        let Some(desired_price) = desired.price else {
            return Err("QUOTE requires price".into());
        };
        let owner = owner.unwrap();
        let age = now - owner.live_since;
        let price_ticks = (desired_price.inner() - owner.price.inner()).abs() / tick_size;
        // M-MM9: a partial fill must never re-quote the full original size.
        // Cap the replacement at `remaining * (1 + shrink hysteresis)` so the
        // order shrinks instead of snapping back to the pre-fill quantity.
        let effective = Self::clamp_to_partial_fill(desired, owner);
        let effective_size = effective
            .size
            .ok_or_else(|| "QUOTE requires size".to_string())?;
        let size_delta = (effective_size.inner() - owner.remaining_size.inner()).abs();
        let within_hysteresis = price_ticks
            <= Decimal::from_i128(self.config.price_hysteresis_ticks as i128)
            && size_delta <= self.config.size_hysteresis.inner();
        if within_hysteresis {
            return Ok(self.make_diff(
                &effective,
                Some(owner.clone()),
                QuoteAction::Keep,
                vec![],
                "within_price_size_hysteresis",
            ));
        }
        if age < self.config.min_quote_lifetime {
            return Ok(self.make_diff(
                &effective,
                Some(owner.clone()),
                QuoteAction::Keep,
                vec![],
                "minimum_quote_lifetime",
            ));
        }
        if let Some(last) = view.last_transition_at
            && now - last < self.config.refresh_cooldown
        {
            return Ok(self.make_diff(
                &effective,
                Some(owner.clone()),
                QuoteAction::Keep,
                vec![],
                "refresh_cooldown",
            ));
        }
        let forced_by_age = age >= self.config.max_quote_age;
        let replace_cost = self.transition_cost(2);
        let net = Usd::new(effective.gross_edge_usdc.inner() - replace_cost.inner());
        if !forced_by_age && net.inner() <= self.config.replace_hysteresis_usdc.inner() {
            return Ok(self.make_diff(
                &effective,
                Some(owner.clone()),
                QuoteAction::Keep,
                vec![],
                "replace_not_incrementally_better",
            ));
        }
        let reason = if forced_by_age {
            "maximum_quote_age"
        } else {
            "replace_hysteresis_passed"
        };
        Ok(self.make_diff(
            &effective,
            Some(owner.clone()),
            QuoteAction::CancelThenPlace,
            vec!["cancel".into(), "place".into()],
            reason,
        ))
    }

    /// M-MM9: clamp a desired quote size after a partial fill. When the owner
    /// has less remaining than desired, the replacement size is bounded by
    /// `remaining * (1 + shrink_hysteresis)` — enough to keep quoting without
    /// re-placing the full pre-fill quantity.
    fn clamp_to_partial_fill(desired: &DesiredQuote, owner: &QuoteRiskOwner) -> DesiredQuote {
        const SHRINK_HYSTERESIS: &str = "0.2"; // 20% above remaining
        let Some(size) = desired.size else {
            return desired.clone();
        };
        let remaining = owner.remaining_size.inner();
        if size.inner() <= remaining {
            return desired.clone(); // no partial fill (or desired already smaller)
        }
        let budget =
            remaining * (Decimal::ONE + Decimal::from_str_lenient(SHRINK_HYSTERESIS).unwrap());
        let mut clamped = desired.clone();
        clamped.size = Some(Size::new(size.inner().min(budget)));
        clamped
    }

    fn transition_cost(&self, child_count: usize) -> Usd {
        let per_child = self.config.action_shadow_cost_usdc.inner()
            + self.config.failure_tail_cost_per_action_usdc.inner();
        Usd::new(per_child * Decimal::from_i128(child_count as i128))
    }

    fn make_diff(
        &self,
        desired: &DesiredQuote,
        source: Option<QuoteRiskOwner>,
        action: QuoteAction,
        child_actions: Vec<String>,
        reason: &str,
    ) -> QuoteDiff {
        let cost = self.transition_cost(child_actions.len());
        let net = Usd::new(desired.gross_edge_usdc.inner() - cost.inner());
        QuoteDiff {
            slot: desired.slot.clone(),
            action,
            source,
            desired: desired.clone(),
            child_actions,
            reason: reason.to_string(),
            gross_edge_usdc: desired.gross_edge_usdc,
            transition_cost_usdc: cost,
            net_incremental_utility_usdc: net,
        }
    }
}

fn fenced_plan(
    desired: &DesiredQuoteSet,
    fence_reason: String,
    diffs: Vec<QuoteDiff>,
) -> QuotePlan {
    QuotePlan {
        strategy_id: desired.strategy_id.clone(),
        symbol: desired.symbol.clone(),
        session_id: desired.session_id.clone(),
        config_version: desired.config_version,
        revision: desired.revision,
        market_version: desired.market_version,
        connection_generation: desired.connection_generation,
        valid_until: desired.valid_until,
        diffs,
        fair_price: Some(desired.fair_price),
        reservation_price: Some(desired.reservation_price),
        inventory_notional: desired.inventory_notional,
        budget_mode: desired.budget_mode,
        fenced: true,
        fence_reason: Some(fence_reason),
    }
}

/// Downgrade a desired quote to NoQuote, dropping price/size (L-ST1).
fn no_quote_desired(mut quote: DesiredQuote, reason: &str) -> DesiredQuote {
    quote.decision = QuoteDecision::NoQuote;
    quote.price = None;
    quote.size = None;
    quote.reason = reason.to_string();
    quote
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trading::quotes::{DesiredQuote, QuoteRiskOwner, QuoteSlotKey};
    use hypeedge_domain::decimal::Price;
    use hypeedge_domain::enums::{ActionBudgetMode, OrderStatus};

    fn slot(side: Side) -> QuoteSlotKey {
        QuoteSlotKey {
            strategy_id: "mm_1".into(),
            symbol: "BTC".into(),
            side,
            level: 0,
        }
    }

    fn desired_set(bid: QuoteDecision, ask: QuoteDecision) -> DesiredQuoteSet {
        DesiredQuoteSet {
            strategy_id: "mm_1".into(),
            symbol: "BTC".into(),
            session_id: "s1".into(),
            config_version: 1,
            model_version: "v1".into(),
            market_version: 1,
            connection_generation: 0,
            current_slot_revision: 0,
            revision: 5,
            fair_price: Price::new(Decimal::from_str_lenient("100").unwrap()),
            reservation_price: Price::new(Decimal::from_str_lenient("100").unwrap()),
            inventory_notional: Usd::ZERO,
            expected_utility_usdc: Usd::ZERO,
            budget_mode: ActionBudgetMode::Normal,
            bid: DesiredQuote {
                slot: slot(Side::Buy),
                decision: bid,
                price: Some(Price::new(Decimal::from_str_lenient("99.99").unwrap())),
                size: Some(Size::new(Decimal::ONE)),
                gross_edge_usdc: Usd::new(Decimal::from_str_lenient("0.05").unwrap()),
                reason: "positive_expected_edge".into(),
            },
            ask: DesiredQuote {
                slot: slot(Side::Sell),
                decision: ask,
                price: Some(Price::new(Decimal::from_str_lenient("100.01").unwrap())),
                size: Some(Size::new(Decimal::ONE)),
                gross_edge_usdc: Usd::new(Decimal::from_str_lenient("0.05").unwrap()),
                reason: "positive_expected_edge".into(),
            },
            created_at: Utc::now() - Duration::seconds(1),
            valid_until: Utc::now() + Duration::seconds(5),
            feature_values: vec![],
        }
    }

    fn empty_view(side: Side) -> QuoteSlotView {
        QuoteSlotView {
            key: slot(side),
            revision: 0,
            plan_revision: 0,
            owners: vec![],
            last_transition_at: None,
        }
    }

    #[test]
    fn places_when_empty() {
        let coord = QuoteCoordinator::new(QuoteCoordinatorConfig::default()).unwrap();
        let set = desired_set(QuoteDecision::Quote, QuoteDecision::Quote);
        let plan = coord
            .coordinate(
                &set,
                &empty_view(Side::Buy),
                &empty_view(Side::Sell),
                Decimal::from_str_lenient("0.01").unwrap(),
                Utc::now(),
            )
            .unwrap();
        assert!(!plan.fenced);
        assert_eq!(plan.diffs.len(), 2);
        assert!(plan.diffs.iter().all(|d| d.action == QuoteAction::Place));
        assert_eq!(plan.estimated_incremental_actions(), 2);
    }

    #[test]
    fn keeps_when_within_hysteresis() {
        let coord = QuoteCoordinator::new(QuoteCoordinatorConfig::default()).unwrap();
        let set = desired_set(QuoteDecision::Quote, QuoteDecision::Quote);
        let mut view = empty_view(Side::Buy);
        view.owners = vec![QuoteRiskOwner {
            order_id: Some("o1".into()),
            cloid: "c1".into(),
            price: Price::new(Decimal::from_str_lenient("99.99").unwrap()), // == desired
            remaining_size: Size::new(Decimal::ONE),                        // == desired
            status: OrderStatus::Acknowledged,
            plan_revision: 0,
            live_since: Utc::now() - Duration::seconds(10),
            exchange_order_id_known: true,
        }];
        let plan = coord
            .coordinate(
                &set,
                &view,
                &empty_view(Side::Sell),
                Decimal::from_str_lenient("0.01").unwrap(),
                Utc::now(),
            )
            .unwrap();
        let bid_diff = &plan.diffs[0];
        assert_eq!(bid_diff.action, QuoteAction::Keep);
        assert_eq!(bid_diff.reason, "within_price_size_hysteresis");
        // Golden: Python net_incremental_utility == gross_edge when no child actions.
        assert_eq!(bid_diff.net_incremental_utility_usdc.to_string(), "0.05");
    }

    /// Golden parity: this REPLACE decision was produced by the pinned Python
    /// `QuoteCoordinator._diff_slot` for the identical owner + desired quote
    /// (price moved 5 ticks, owner 20s old ≥ max_quote_age 15s).
    #[test]
    fn replaces_when_past_max_age_golden() {
        let coord = QuoteCoordinator::new(QuoteCoordinatorConfig::default()).unwrap();
        let mut set = desired_set(QuoteDecision::Quote, QuoteDecision::Quote);
        // Move desired bid 5 ticks below the owner price.
        set.bid.price = Some(Price::new(Decimal::from_str_lenient("99.94").unwrap()));
        set.bid.gross_edge_usdc = Usd::new(Decimal::from_str_lenient("0.0015").unwrap());
        let mut view = empty_view(Side::Buy);
        view.owners = vec![QuoteRiskOwner {
            order_id: Some("o1".into()),
            cloid: "c1".into(),
            price: Price::new(Decimal::from_str_lenient("99.99").unwrap()),
            remaining_size: Size::new(Decimal::ONE),
            status: OrderStatus::Acknowledged,
            plan_revision: 0,
            live_since: Utc::now() - Duration::seconds(20),
            exchange_order_id_known: true,
        }];
        let plan = coord
            .coordinate(
                &set,
                &view,
                &empty_view(Side::Sell),
                Decimal::from_str_lenient("0.01").unwrap(),
                Utc::now(),
            )
            .unwrap();
        let bid_diff = &plan.diffs[0];
        assert_eq!(bid_diff.action, QuoteAction::CancelThenPlace);
        assert_eq!(bid_diff.reason, "maximum_quote_age");
        assert_eq!(bid_diff.child_actions, vec!["cancel", "place"]);
        // Golden: net = gross_edge (transition cost 0).
        assert_eq!(bid_diff.net_incremental_utility_usdc.to_string(), "0.0015");
    }

    #[test]
    fn cancels_when_no_quote_desired() {
        let coord = QuoteCoordinator::new(QuoteCoordinatorConfig::default()).unwrap();
        let set = desired_set(QuoteDecision::NoQuote, QuoteDecision::NoQuote);
        let mut view = empty_view(Side::Buy);
        view.owners = vec![QuoteRiskOwner {
            order_id: Some("o1".into()),
            cloid: "c1".into(),
            price: Price::new(Decimal::from_str_lenient("99.99").unwrap()),
            remaining_size: Size::new(Decimal::ONE),
            status: OrderStatus::Acknowledged,
            plan_revision: 0,
            live_since: Utc::now() - Duration::seconds(10),
            exchange_order_id_known: true,
        }];
        let plan = coord
            .coordinate(
                &set,
                &view,
                &empty_view(Side::Sell),
                Decimal::from_str_lenient("0.01").unwrap(),
                Utc::now(),
            )
            .unwrap();
        assert_eq!(plan.diffs[0].action, QuoteAction::Cancel);
        // Empty side with NO_QUOTE → NO_ACTION.
        assert_eq!(plan.diffs[1].action, QuoteAction::NoAction);
    }

    #[test]
    fn fences_on_stale_revision() {
        let coord = QuoteCoordinator::new(QuoteCoordinatorConfig::default()).unwrap();
        let mut set = desired_set(QuoteDecision::Quote, QuoteDecision::Quote);
        set.revision = 1; // <= plan_revision of a view
        let mut view = empty_view(Side::Buy);
        view.plan_revision = 2;
        let plan = coord
            .coordinate(
                &set,
                &view,
                &empty_view(Side::Sell),
                Decimal::from_str_lenient("0.01").unwrap(),
                Utc::now(),
            )
            .unwrap();
        assert!(plan.fenced);
        assert_eq!(plan.fence_reason.as_deref(), Some("stale_plan_revision"));
        assert!(plan.diffs.is_empty());
    }

    #[test]
    fn fences_on_expired_candidate() {
        let coord = QuoteCoordinator::new(QuoteCoordinatorConfig::default()).unwrap();
        let mut set = desired_set(QuoteDecision::Quote, QuoteDecision::Quote);
        set.valid_until = Utc::now() - Duration::seconds(1);
        let plan = coord
            .coordinate(
                &set,
                &empty_view(Side::Buy),
                &empty_view(Side::Sell),
                Decimal::from_str_lenient("0.01").unwrap(),
                Utc::now(),
            )
            .unwrap();
        assert!(plan.fenced);
        assert_eq!(plan.fence_reason.as_deref(), Some("candidate_expired"));
    }

    #[test]
    fn blocks_unknown_owner() {
        let coord = QuoteCoordinator::new(QuoteCoordinatorConfig::default()).unwrap();
        let set = desired_set(QuoteDecision::Quote, QuoteDecision::Quote);
        let mut view = empty_view(Side::Buy);
        view.owners = vec![QuoteRiskOwner {
            order_id: None,
            cloid: "c_unknown".into(),
            price: Price::new(Decimal::from_str_lenient("99").unwrap()),
            remaining_size: Size::new(Decimal::ONE),
            status: OrderStatus::SubmitUnknown,
            plan_revision: 0,
            live_since: Utc::now() - Duration::seconds(1),
            exchange_order_id_known: false,
        }];
        let plan = coord
            .coordinate(
                &set,
                &view,
                &empty_view(Side::Sell),
                Decimal::from_str_lenient("0.01").unwrap(),
                Utc::now(),
            )
            .unwrap();
        assert_eq!(plan.diffs[0].action, QuoteAction::BlockedUnknown);
        assert_eq!(
            plan.diffs[0].reason,
            "unknown_risk_owner_requires_reconciliation"
        );
    }

    // --- H-MM1: a fence must not suppress the NoQuote → Cancel intent ---

    #[test]
    fn expired_candidate_still_cancels_no_quote() {
        let coord = QuoteCoordinator::new(QuoteCoordinatorConfig::default()).unwrap();
        let mut set = desired_set(QuoteDecision::NoQuote, QuoteDecision::NoQuote);
        set.valid_until = Utc::now() - Duration::seconds(1); // expired candidate
        let mut view = empty_view(Side::Buy);
        view.owners = vec![QuoteRiskOwner {
            order_id: Some("o1".into()),
            cloid: "c1".into(),
            price: Price::new(Decimal::from_str_lenient("99.99").unwrap()),
            remaining_size: Size::new(Decimal::ONE),
            status: OrderStatus::Acknowledged,
            plan_revision: 0,
            live_since: Utc::now() - Duration::seconds(10),
            exchange_order_id_known: true,
        }];
        let plan = coord
            .coordinate(
                &set,
                &view,
                &empty_view(Side::Sell),
                Decimal::from_str_lenient("0.01").unwrap(),
                Utc::now(),
            )
            .unwrap();
        // Still fenced (candidate expired), but the protective cancel goes out.
        assert!(plan.fenced);
        assert_eq!(plan.fence_reason.as_deref(), Some("candidate_expired"));
        assert_eq!(plan.diffs[0].action, QuoteAction::Cancel);
        // Empty side with NO_QUOTE → NO_ACTION.
        assert_eq!(plan.diffs[1].action, QuoteAction::NoAction);
    }

    #[test]
    fn stale_revision_fence_suppresses_placements_not_cancels() {
        let coord = QuoteCoordinator::new(QuoteCoordinatorConfig::default()).unwrap();
        let mut set = desired_set(QuoteDecision::NoQuote, QuoteDecision::Quote);
        set.revision = 1; // <= plan_revision of a view → fenced
        let mut view = empty_view(Side::Buy);
        view.plan_revision = 2;
        let plan = coord
            .coordinate(
                &set,
                &view,
                &empty_view(Side::Sell),
                Decimal::from_str_lenient("0.01").unwrap(),
                Utc::now(),
            )
            .unwrap();
        assert!(plan.fenced);
        assert_eq!(plan.fence_reason.as_deref(), Some("stale_plan_revision"));
        // The NoQuote side stays silent (no owner); the Quote side must NOT be
        // placed while fenced — no Place action anywhere.
        assert!(plan.diffs.iter().all(|d| d.action != QuoteAction::Place));
    }

    // --- M-MM9: a partial fill must not re-quote the full original size ---

    #[test]
    fn partial_fill_requote_size_does_not_exceed_remaining() {
        let coord = QuoteCoordinator::new(QuoteCoordinatorConfig::default()).unwrap();
        let set = desired_set(QuoteDecision::Quote, QuoteDecision::Quote);
        let mut view = empty_view(Side::Buy);
        view.owners = vec![QuoteRiskOwner {
            order_id: Some("o1".into()),
            cloid: "c1".into(),
            price: Price::new(Decimal::from_str_lenient("99.99").unwrap()), // == desired
            remaining_size: Size::new(Decimal::from_str_lenient("0.5").unwrap()), // half filled
            status: OrderStatus::Acknowledged,
            plan_revision: 0,
            live_since: Utc::now() - Duration::seconds(20), // past max_quote_age
            exchange_order_id_known: true,
        }];
        let plan = coord
            .coordinate(
                &set,
                &view,
                &empty_view(Side::Sell),
                Decimal::from_str_lenient("0.01").unwrap(),
                Utc::now(),
            )
            .unwrap();
        assert_eq!(plan.diffs[0].action, QuoteAction::CancelThenPlace);
        let re_size = plan.diffs[0].desired.size.unwrap().inner();
        let remaining = Decimal::from_str_lenient("0.5").unwrap();
        let budget = remaining * Decimal::from_str_lenient("1.2").unwrap();
        assert!(
            re_size <= budget,
            "re-quote size {re_size} must not exceed remaining budget {budget}"
        );
        assert!(
            re_size < Decimal::ONE,
            "re-quote must shrink below the pre-fill size 1.0 (got {re_size})"
        );
    }

    // --- L-ST1: crossed bid/ask are rejected (downgraded to NoQuote) ---

    #[test]
    fn crossed_quotes_are_rejected() {
        let coord = QuoteCoordinator::new(QuoteCoordinatorConfig::default()).unwrap();
        let mut set = desired_set(QuoteDecision::Quote, QuoteDecision::Quote);
        set.bid.price = Some(Price::new(Decimal::from_str_lenient("100.5").unwrap()));
        set.ask.price = Some(Price::new(Decimal::from_str_lenient("100.4").unwrap())); // crossed
        let plan = coord
            .coordinate(
                &set,
                &empty_view(Side::Buy),
                &empty_view(Side::Sell),
                Decimal::from_str_lenient("0.01").unwrap(),
                Utc::now(),
            )
            .unwrap();
        assert!(
            plan.diffs.iter().all(|d| d.action != QuoteAction::Place),
            "crossed quotes must never be placed"
        );
        assert!(
            plan.diffs
                .iter()
                .all(|d| d.reason == "crossed_quote_rejected")
        );
    }

    #[test]
    fn non_crossed_quotes_are_not_downgraded() {
        let coord = QuoteCoordinator::new(QuoteCoordinatorConfig::default()).unwrap();
        let set = desired_set(QuoteDecision::Quote, QuoteDecision::Quote);
        let plan = coord
            .coordinate(
                &set,
                &empty_view(Side::Buy),
                &empty_view(Side::Sell),
                Decimal::from_str_lenient("0.01").unwrap(),
                Utc::now(),
            )
            .unwrap();
        assert_eq!(plan.diffs[0].action, QuoteAction::Place);
        assert_eq!(plan.diffs[1].action, QuoteAction::Place);
    }
}
