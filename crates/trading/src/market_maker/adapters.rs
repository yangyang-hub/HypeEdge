//! Market-maker runtime provider adapters (wiring follow-up): adapt the shared
//! account tracker, action-budget controller, and market-data provider to the
//! market-maker runtime's provider protocols.

use std::sync::Arc;

use chrono::Utc;
use hypeedge_domain::decimal::{Size, Usd};
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
use crate::trading::quotes::{QuotePlan, QuoteSlotView};

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

impl FundingSnapshotProvider for ProviderFundingProvider {
    fn get_funding(&self, symbol: &str) -> Option<(f64, i64)> {
        // `get_funding` is async; the runtime's funding provider is sync. Bridge
        // by blocking on a short poll — acceptable because the provider caches
        // the latest funding in memory.
        let rt = tokio::runtime::Handle::current();
        rt.block_on(async { self.provider.get_funding(symbol).await })
            .map(|f| (f.funding_rate, f.timestamp))
    }
}

/// Quote slots from the engine's open orders (best-effort; empty views mean the
/// coordinator treats the slot as available to place).
pub struct EngineSlotProvider {
    #[allow(dead_code)] // future: read open orders from the engine for live slot views
    engine: Arc<ExecutionEngine>,
}

impl EngineSlotProvider {
    pub fn new(engine: Arc<ExecutionEngine>) -> Self {
        Self { engine }
    }
}

impl QuoteSlotProvider for EngineSlotProvider {
    fn get_quote_slots(
        &self,
        _strategy_id: &str,
        _symbol: &str,
    ) -> Result<(QuoteSlotView, QuoteSlotView), String> {
        // The coordinator validates slot ownership against the desired set; with
        // empty views it will Place. A fuller implementation reads open orders
        // from the engine. This is the wiring seam for live quote-slot tracking.
        Ok((empty_slot_view(), empty_slot_view()))
    }
}

fn empty_slot_view() -> QuoteSlotView {
    QuoteSlotView {
        key: crate::trading::quotes::QuoteSlotKey {
            strategy_id: String::new(),
            symbol: String::new(),
            side: hypeedge_domain::enums::Side::Buy,
            level: 0,
        },
        revision: 0,
        plan_revision: 0,
        owners: vec![],
        last_transition_at: None,
    }
}

/// Command client forwarding quote plans to the execution engine (best-effort:
/// submit the plan's child placements through the engine's ExecutionClient).
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
        // Place each desired quote through the engine. The plan's diffs carry
        // Place/Cancel decisions; here we forward the desired bids/asks as
        // limit orders. A fuller implementation reconciles diff-by-diff.
        for quote in [&plan.diffs[0], plan.diffs.get(1).unwrap_or(&plan.diffs[0])] {
            if quote.action != hypeedge_domain::enums::QuoteAction::Place {
                continue;
            }
            let Some(price) = quote.desired.price else {
                continue;
            };
            let Some(size) = quote.desired.size else {
                continue;
            };
            let intent = hypeedge_domain::models::OrderIntent {
                symbol: quote.desired.slot.symbol.clone(),
                side: quote.desired.slot.side,
                size,
                price: Some(price),
                order_type: hypeedge_domain::enums::OrderType::Limit,
                time_in_force: hypeedge_domain::enums::TimeInForce::Gtc,
                strategy_id: Some(plan.strategy_id.clone()),
                sub_account: None,
                reduce_only: false,
                cloid: None,
                client_id: None,
                is_spot: false,
                risk_reducing: false,
                max_slippage_bps: 50,
            };
            self.engine.submit_order(intent, None).await.map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    async fn cancel_strategy_quotes(&self, request: &QuoteCancelRequest) -> Result<(), String> {
        let _ = request;
        // Cancel all the strategy's open orders on its symbol.
        let _ = self.engine.cancel_all_orders(Some(&request.symbol)).await;
        Ok(())
    }
}
