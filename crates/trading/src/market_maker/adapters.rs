//! Market-maker runtime provider adapters (wiring follow-up): adapt the shared
//! account tracker, action-budget controller, and market-data provider to the
//! market-maker runtime's provider protocols.

use std::sync::Arc;

use chrono::Utc;
use hypeedge_domain::decimal::{Decimal, Size, Usd};
use hypeedge_domain::enums::{Side, TimeInForce};
use hypeedge_domain::traits::ExecutionClient;

use super::models::{ActionBudgetSnapshot, InventorySnapshot};
use super::runtime::{
    ActionBudgetSnapshotProvider, FundingSnapshotProvider, InventorySnapshotProvider,
    QuoteCancelRequest, QuotePlanCommandClient, QuoteSlotProvider,
};
use crate::account::AccountTracker;
use crate::execution::ExecutionEngine;
use crate::market_data::live_provider::LiveMarketDataProvider;
use crate::risk::ActionBudgetController;
use crate::trading::quotes::{QuotePlan, QuoteRiskOwner, QuoteSlotKey, QuoteSlotView};

/// Inventory from the live account tracker.
pub struct TrackerInventoryProvider {
    tracker: Arc<AccountTracker>,
}

impl TrackerInventoryProvider {
    pub fn new(tracker: Arc<AccountTracker>) -> Self {
        Self { tracker }
    }
}

impl InventorySnapshotProvider for TrackerInventoryProvider {
    fn get_inventory(&self, _sub_account: &str, symbol: &str) -> InventorySnapshot {
        let position = self.tracker.get_position(symbol);
        let equity = self.tracker.current_equity();
        let available = self
            .tracker
            .get_account_state()
            .map(|a| a.available_balance)
            .unwrap_or(Usd::ZERO);
        let margin_used = self
            .tracker
            .get_account_state()
            .map(|a| a.total_margin_used)
            .unwrap_or(Usd::ZERO);
        InventorySnapshot {
            position_size: Size::new(position.map(|p| p.size.inner()).unwrap_or_default()),
            equity,
            available_balance: available,
            margin_used,
            observed_at: self.tracker.last_update_ts().unwrap_or_else(Utc::now),
            healthy: self.tracker.last_update_ts().is_some(),
        }
    }
}

/// Action budget from the shared controller.
pub struct ControllerBudgetProvider {
    controller: Arc<tokio::sync::Mutex<ActionBudgetController>>,
}

impl ControllerBudgetProvider {
    pub fn new(controller: Arc<tokio::sync::Mutex<ActionBudgetController>>) -> Self {
        Self { controller }
    }
}

impl ActionBudgetSnapshotProvider for ControllerBudgetProvider {
    fn get_action_budget(&self, _strategy_id: &str, _symbol: &str) -> ActionBudgetSnapshot {
        let guard = match self.controller.try_lock() {
            Ok(g) => g,
            Err(_) => {
                // Budget is momentarily busy; report an unhealthy snapshot so the
                // runtime treats it as no-quote rather than a stale read.
                return ActionBudgetSnapshot {
                    mode: hypeedge_domain::enums::ActionBudgetMode::CancelOnly,
                    address_actions_remaining: 0,
                    cancel_headroom: 0,
                    ip_weight_remaining: 0,
                    action_shadow_cost_usdc: Usd::ZERO,
                    observed_at: Utc::now(),
                    healthy: false,
                };
            }
        };
        let view = guard.snapshot();
        ActionBudgetSnapshot {
            mode: guard.mode(),
            address_actions_remaining: view.address_remaining,
            cancel_headroom: view.cancel_headroom_remaining,
            ip_weight_remaining: view.ip_weight_remaining,
            action_shadow_cost_usdc: Usd::ZERO,
            observed_at: Utc::now(),
            healthy: true,
        }
    }
}

/// Account health from the tracker freshness.
pub struct TrackerHealthProvider {
    tracker: Arc<AccountTracker>,
}

impl TrackerHealthProvider {
    pub fn new(tracker: Arc<AccountTracker>) -> Self {
        Self { tracker }
    }
}

impl super::runtime::AccountHealthProvider for TrackerHealthProvider {
    fn allows_risk_increase(&self) -> bool {
        // Fresh account state ⇒ risk increases are allowed.
        self.tracker.last_update_ts().is_some()
    }
}

/// Funding from the live market-data provider.
pub struct ProviderFundingProvider {
    provider: Arc<LiveMarketDataProvider>,
}

impl ProviderFundingProvider {
    pub fn new(provider: Arc<LiveMarketDataProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait::async_trait]
impl FundingSnapshotProvider for ProviderFundingProvider {
    async fn get_funding(&self, symbol: &str) -> Option<(f64, i64)> {
        self.provider
            .get_funding(symbol)
            .await
            .map(|f| (f.funding_rate, f.timestamp))
    }
}

/// Quote slots from the engine's open orders.
pub struct EngineSlotProvider {
    engine: Arc<ExecutionEngine>,
}

impl EngineSlotProvider {
    pub fn new(engine: Arc<ExecutionEngine>) -> Self {
        Self { engine }
    }
}

#[async_trait::async_trait]
impl QuoteSlotProvider for EngineSlotProvider {
    async fn get_quote_slots(
        &self,
        strategy_id: &str,
        symbol: &str,
    ) -> Result<(QuoteSlotView, QuoteSlotView), String> {
        let orders = self
            .engine
            .get_open_orders(Some(symbol))
            .await
            .map_err(|e| format!("engine open orders: {e}"))?;
        let mut bid_owners = Vec::new();
        let mut ask_owners = Vec::new();
        for order in orders {
            if order.strategy_id.as_deref() != Some(strategy_id) {
                continue;
            }
            let Some(price) = order.price else {
                continue;
            };
            let remaining = order.size.inner() - order.filled_size.inner();
            if remaining <= Decimal::ZERO {
                continue;
            }
            let owner = QuoteRiskOwner {
                order_id: order.exchange_oid.clone(),
                cloid: order.cloid.clone(),
                price,
                remaining_size: Size::new(remaining),
                status: order.status,
                plan_revision: 0,
                live_since: order
                    .acknowledged_at
                    .or(order.submitted_at)
                    .unwrap_or_else(Utc::now),
                exchange_order_id_known: order.exchange_oid.is_some(),
            };
            match order.side {
                Side::Buy => bid_owners.push(owner),
                Side::Sell => ask_owners.push(owner),
            }
        }
        if bid_owners.len() > 1 || ask_owners.len() > 1 {
            return Err(format!(
                "strategy {strategy_id} has multiple live orders on one quote side (bid={}, ask={}); reconciliation required",
                bid_owners.len(),
                ask_owners.len()
            ));
        }
        let view = |side: Side, owners: Vec<QuoteRiskOwner>| QuoteSlotView {
            key: QuoteSlotKey {
                strategy_id: strategy_id.to_string(),
                symbol: symbol.to_string(),
                side,
                level: 0,
            },
            revision: 0,
            plan_revision: 0,
            owners,
            last_transition_at: None,
        };
        Ok((view(Side::Buy, bid_owners), view(Side::Sell, ask_owners)))
    }
}

/// Command client forwarding quote-plan diffs to the execution engine.
pub struct EngineQuotePlanClient {
    engine: Arc<ExecutionEngine>,
}

impl EngineQuotePlanClient {
    pub fn new(engine: Arc<ExecutionEngine>) -> Self {
        Self { engine }
    }
}

#[async_trait::async_trait]
impl QuotePlanCommandClient for EngineQuotePlanClient {
    async fn submit_quote_plan(&self, plan: &QuotePlan) -> Result<(), String> {
        for diff in &plan.diffs {
            match diff.action {
                hypeedge_domain::enums::QuoteAction::Place => {
                    self.place_quote(diff).await?;
                }
                hypeedge_domain::enums::QuoteAction::Cancel => {
                    if let Some(source) = &diff.source {
                        self.engine
                            .cancel_order(&source.cloid)
                            .await
                            .map_err(|e| format!("cancel quote: {e}"))?;
                    }
                }
                hypeedge_domain::enums::QuoteAction::CancelThenPlace => {
                    if let Some(source) = &diff.source {
                        self.engine
                            .cancel_order(&source.cloid)
                            .await
                            .map_err(|e| format!("cancel-then-place quote: {e}"))?;
                    }
                    self.place_quote(diff).await?;
                }
                hypeedge_domain::enums::QuoteAction::Keep
                | hypeedge_domain::enums::QuoteAction::NoAction
                | hypeedge_domain::enums::QuoteAction::BlockedUnknown
                | hypeedge_domain::enums::QuoteAction::Modify => {
                    // MODIFY is disabled; Keep/NoAction need no network work.
                    continue;
                }
            }
        }
        Ok(())
    }

    async fn cancel_strategy_quotes(&self, request: &QuoteCancelRequest) -> Result<(), String> {
        let open = self
            .engine
            .get_open_orders(Some(&request.symbol))
            .await
            .map_err(|e| format!("open orders: {e}"))?;
        for order in open {
            if order.strategy_id.as_deref() != Some(request.strategy_id.as_str()) {
                continue;
            }
            self.engine
                .cancel_order(&order.cloid)
                .await
                .map_err(|e| format!("cancel strategy quote: {e}"))?;
        }
        Ok(())
    }
}

impl EngineQuotePlanClient {
    async fn place_quote(&self, diff: &crate::trading::quotes::QuoteDiff) -> Result<(), String> {
        let Some(price) = diff.desired.price else {
            return Ok(());
        };
        let Some(size) = diff.desired.size else {
            return Ok(());
        };
        let intent = hypeedge_domain::models::OrderIntent {
            symbol: diff.desired.slot.symbol.clone(),
            side: diff.desired.slot.side,
            size,
            price: Some(price),
            order_type: hypeedge_domain::enums::OrderType::Limit,
            // ALO: maker quotes must never cross/take liquidity.
            time_in_force: TimeInForce::Alo,
            strategy_id: Some(diff.desired.slot.strategy_id.clone()),
            sub_account: None,
            reduce_only: false,
            cloid: None,
            client_id: None,
            is_spot: false,
            risk_reducing: false,
            max_slippage_bps: 50,
        };
        self.engine
            .submit_order(intent, None)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}
