//! Research-only virtual quote lifecycle for shadow evaluation, port of
//! `src/hypeedge/strategy/market_maker/shadow.py`.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::{Decimal, Size};
use hypeedge_domain::enums::{OrderStatus, QuoteAction, Side};

use crate::trading::quotes::{QuotePlan, QuoteRiskOwner, QuoteSlotKey, QuoteSlotView};

/// The shadow action estimate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShadowActionEstimate {
    pub optimistic: usize,
    pub neutral: usize,
    pub pessimistic: usize,
}

/// Virtual resting orders; never writes authoritative trading facts.
pub struct ShadowOrderState {
    views: HashMap<QuoteSlotKey, QuoteSlotView>,
}

impl Default for ShadowOrderState {
    fn default() -> Self {
        Self::new()
    }
}

impl ShadowOrderState {
    pub fn new() -> Self {
        Self {
            views: HashMap::new(),
        }
    }

    pub fn views(&mut self, strategy_id: &str, symbol: &str) -> (QuoteSlotView, QuoteSlotView) {
        (
            self.view(QuoteSlotKey {
                strategy_id: strategy_id.to_string(),
                symbol: symbol.to_string(),
                side: Side::Buy,
                level: 0,
            }),
            self.view(QuoteSlotKey {
                strategy_id: strategy_id.to_string(),
                symbol: symbol.to_string(),
                side: Side::Sell,
                level: 0,
            }),
        )
    }

    pub fn apply(
        &mut self,
        plan: &QuotePlan,
        now: DateTime<Utc>,
    ) -> Result<ShadowActionEstimate, String> {
        if plan.fenced {
            return Ok(ShadowActionEstimate {
                optimistic: 0,
                neutral: 0,
                pessimistic: 0,
            });
        }
        let mut child_actions = 0usize;
        for diff in &plan.diffs {
            let view = self.view(diff.slot.clone());
            let owners: Vec<QuoteRiskOwner> = match diff.action {
                QuoteAction::Cancel => {
                    child_actions += 1;
                    vec![]
                }
                QuoteAction::Place => {
                    child_actions += 1;
                    vec![Self::owner(
                        plan,
                        &diff.slot,
                        diff.desired.price,
                        diff.desired.size,
                        now,
                    )]
                }
                QuoteAction::CancelThenPlace => {
                    child_actions += 2;
                    vec![Self::owner(
                        plan,
                        &diff.slot,
                        diff.desired.price,
                        diff.desired.size,
                        now,
                    )]
                }
                QuoteAction::Keep | QuoteAction::NoAction | QuoteAction::BlockedUnknown => continue,
                other => return Err(format!("unsupported shadow quote action: {other:?}")),
            };
            self.views.insert(
                diff.slot.clone(),
                QuoteSlotView {
                    key: diff.slot.clone(),
                    revision: view.revision + 1,
                    plan_revision: plan.revision,
                    owners,
                    last_transition_at: Some(now),
                },
            );
        }
        Ok(ShadowActionEstimate {
            optimistic: child_actions,
            neutral: child_actions + usize::from(child_actions > 0),
            pessimistic: child_actions * 2,
        })
    }

    pub fn simulate_fill(&mut self, slot: &QuoteSlotKey, size: Size) -> Result<(), String> {
        let view = self.view(slot.clone());
        let Some(owner) = view.current_owner()?.cloned() else {
            return Err(format!("no current owner for {slot:?}"));
        };
        let remaining = owner.remaining_size.inner() - size.inner();
        let owners: Vec<QuoteRiskOwner> = if remaining <= Decimal::ZERO {
            vec![]
        } else {
            vec![QuoteRiskOwner {
                remaining_size: Size::new(remaining),
                status: OrderStatus::PartialFill,
                ..owner
            }]
        };
        self.views.insert(
            slot.clone(),
            QuoteSlotView {
                key: slot.clone(),
                revision: view.revision + 1,
                plan_revision: view.plan_revision,
                owners,
                last_transition_at: view.last_transition_at,
            },
        );
        Ok(())
    }

    pub fn simulate_fill_by_cloid(&mut self, cloid: &str, size: Size) -> bool {
        let slots: Vec<QuoteSlotKey> = self.views.keys().cloned().collect();
        for slot in slots {
            if let Some(view) = self.views.get(&slot)
                && view.owners.iter().any(|o| o.cloid == cloid)
            {
                let _ = self.simulate_fill(&slot, size);
                return true;
            }
        }
        false
    }

    fn view(&mut self, key: QuoteSlotKey) -> QuoteSlotView {
        self.views
            .entry(key.clone())
            .or_insert_with(|| QuoteSlotView {
                key,
                revision: 0,
                plan_revision: 0,
                owners: vec![],
                last_transition_at: None,
            })
            .clone()
    }

    fn owner(
        plan: &QuotePlan,
        slot: &QuoteSlotKey,
        price: Option<hypeedge_domain::decimal::Price>,
        size: Option<Size>,
        now: DateTime<Utc>,
    ) -> QuoteRiskOwner {
        let (Some(price), Some(size)) = (price, size) else {
            panic!("shadow placement requires desired price and size");
        };
        let cloid = format!(
            "shadow:{}:{}:{}:{}",
            plan.session_id,
            plan.revision,
            slot.side.as_str(),
            slot.level
        );
        QuoteRiskOwner {
            order_id: None,
            cloid,
            price,
            remaining_size: size,
            status: OrderStatus::Acknowledged,
            plan_revision: plan.revision,
            live_since: now,
            exchange_order_id_known: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trading::quotes::{DesiredQuote, QuoteDiff};
    use hypeedge_domain::decimal::Price;
    use hypeedge_domain::enums::{ActionBudgetMode, QuoteDecision};

    #[test]
    fn apply_place_creates_owners() {
        let mut shadow = ShadowOrderState::new();
        let plan = test_plan(QuoteAction::Place, "place");
        let est = shadow.apply(&plan, Utc::now()).unwrap();
        assert_eq!(est.optimistic, 1);
        let (bid, ask) = shadow.views("mm_1", "BTC");
        assert_eq!(bid.owners.len(), 1);
        assert_eq!(ask.owners.len(), 0);
    }

    #[test]
    fn fenced_plan_is_noop() {
        let mut shadow = ShadowOrderState::new();
        let mut plan = test_plan(QuoteAction::Place, "place");
        plan.fenced = true;
        let est = shadow.apply(&plan, Utc::now()).unwrap();
        assert_eq!(est.optimistic, 0);
        let (bid, _) = shadow.views("mm_1", "BTC");
        assert_eq!(bid.owners.len(), 0);
    }

    #[test]
    fn simulate_fill_reduces_remaining() {
        let mut shadow = ShadowOrderState::new();
        let plan = test_plan(QuoteAction::Place, "place");
        let _ = shadow.apply(&plan, Utc::now());
        let (bid, _) = shadow.views("mm_1", "BTC");
        let owner = bid.owners[0].clone();
        assert!(
            shadow
                .simulate_fill(
                    &bid.key,
                    Size::new(Decimal::from_str_lenient("0.1").unwrap())
                )
                .is_ok()
        );
        let (bid2, _) = shadow.views("mm_1", "BTC");
        assert_eq!(bid2.owners.len(), 1);
        assert_eq!(bid2.owners[0].remaining_size.to_string(), "0.9");
        let _ = owner;
    }

    #[test]
    fn full_fill_clears_owner() {
        let mut shadow = ShadowOrderState::new();
        let plan = test_plan(QuoteAction::Place, "place");
        let _ = shadow.apply(&plan, Utc::now());
        let (bid, _) = shadow.views("mm_1", "BTC");
        let slot = bid.key.clone();
        shadow
            .simulate_fill(&slot, Size::new(Decimal::from_str_lenient("1").unwrap()))
            .unwrap();
        let (bid2, _) = shadow.views("mm_1", "BTC");
        assert_eq!(bid2.owners.len(), 0);
    }

    fn test_plan(action: QuoteAction, reason: &str) -> QuotePlan {
        let slot = QuoteSlotKey {
            strategy_id: "mm_1".into(),
            symbol: "BTC".into(),
            side: Side::Buy,
            level: 0,
        };
        let desired = DesiredQuote {
            slot: slot.clone(),
            decision: QuoteDecision::Quote,
            price: Some(Price::new(Decimal::from_str_lenient("99.99").unwrap())),
            size: Some(Size::new(Decimal::ONE)),
            gross_edge_usdc: hypeedge_domain::Usd::new(Decimal::ZERO),
            reason: reason.into(),
        };
        let diff = QuoteDiff {
            slot: slot.clone(),
            action,
            source: None,
            desired,
            child_actions: vec![],
            reason: reason.into(),
            gross_edge_usdc: hypeedge_domain::Usd::ZERO,
            transition_cost_usdc: hypeedge_domain::Usd::ZERO,
            net_incremental_utility_usdc: hypeedge_domain::Usd::ZERO,
        };
        QuotePlan {
            strategy_id: "mm_1".into(),
            symbol: "BTC".into(),
            session_id: "s1".into(),
            config_version: 1,
            revision: 1,
            market_version: 1,
            connection_generation: 0,
            valid_until: Utc::now() + chrono::Duration::seconds(10),
            diffs: vec![diff],
            fair_price: None,
            reservation_price: None,
            inventory_notional: hypeedge_domain::Usd::ZERO,
            budget_mode: ActionBudgetMode::Normal,
            fenced: false,
            fence_reason: None,
        }
    }
}
