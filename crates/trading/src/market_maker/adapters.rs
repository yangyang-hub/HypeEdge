//! Market-maker runtime provider adapters (wiring follow-up): adapt the shared
//! account tracker, action-budget controller, and market-data provider to the
//! market-maker runtime's provider protocols.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use hypeedge_domain::decimal::{Decimal, Size, Usd};
use hypeedge_domain::enums::{Side, TimeInForce};
use hypeedge_domain::traits::ExecutionClient;

use super::models::{ActionBudgetSnapshot, InventorySnapshot};
use super::runtime::{
    ActionBudgetSnapshotProvider, DEFAULT_ACTION_SHADOW_COST_USDC, FundingSnapshotProvider,
    InventorySnapshotProvider, QuoteCancelRequest, QuotePlanCommandClient, QuoteSlotProvider,
};
use crate::account::AccountTracker;
use crate::execution::ExecutionEngine;
use crate::market_data::live_provider::LiveMarketDataProvider;
use crate::risk::ActionBudgetController;
use crate::trading::quotes::{QuotePlan, QuoteRiskOwner, QuoteSlotKey, QuoteSlotView};

/// H-MM3: account data older than this is treated as stale (fail-closed).
/// The trading crate cannot read the deployment `account_poll_interval_seconds`
/// (default 3s), so the task's fallback constant is used; a configurable
/// threshold is available via [`TrackerHealthProvider::with_freshness_after`].
const ACCOUNT_FRESH_AFTER: Duration = Duration::seconds(30);

/// True when the tracker was updated recently enough to be trusted.
fn tracker_is_fresh(tracker: &AccountTracker, now: DateTime<Utc>) -> bool {
    match tracker.last_update_ts() {
        Some(ts) => now >= ts && now - ts <= ACCOUNT_FRESH_AFTER,
        None => false,
    }
}

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
            // H-MM3: "healthy" means *fresh*, not merely "was updated once".
            healthy: tracker_is_fresh(&self.tracker, Utc::now()),
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
            // M-MM11: expose a non-zero action shadow cost. The trailing
            // marginal USDC/action in the budget view is *earned* revenue, not
            // cost, so a conservative constant is the safe default until a
            // dynamic per-strategy cost estimate is wired in.
            action_shadow_cost_usdc: Usd::new(
                Decimal::from_str_lenient(DEFAULT_ACTION_SHADOW_COST_USDC).unwrap(),
            ),
            observed_at: Utc::now(),
            healthy: true,
        }
    }
}

/// Account health from the tracker freshness.
pub struct TrackerHealthProvider {
    tracker: Arc<AccountTracker>,
    fresh_after: Duration,
}

impl TrackerHealthProvider {
    pub fn new(tracker: Arc<AccountTracker>) -> Self {
        Self {
            tracker,
            fresh_after: ACCOUNT_FRESH_AFTER,
        }
    }

    /// H-MM3: allow overriding the freshness threshold (e.g. from deployment
    /// config `2 × account_poll_interval_seconds` at assembly time).
    pub fn with_freshness_after(mut self, fresh_after: Duration) -> Self {
        self.fresh_after = fresh_after;
        self
    }

    fn is_fresh(&self, now: DateTime<Utc>) -> bool {
        match self.tracker.last_update_ts() {
            Some(ts) => now >= ts && now - ts <= self.fresh_after,
            None => false,
        }
    }
}

impl super::runtime::AccountHealthProvider for TrackerHealthProvider {
    fn allows_risk_increase(&self) -> bool {
        // H-MM3: freshness gate, fail-closed — "was updated at least once"
        // is not enough; the account must have been updated recently.
        self.is_fresh(Utc::now())
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
                // L-MM2: the engine does not expose the quote-plan revision an
                // order belongs to, so this is hardcoded to 0. KNOWN
                // LIMITATION: with plan_revision always 0 the coordinator's
                // `stale_plan_revision` / `slot_revision_mismatch` fences and
                // the orphaned-owner detection are effectively disabled for
                // live slots (they still work in shadow mode). Fixing this
                // requires persisting the owning plan revision on the order —
                // tracked as a follow-up, not silently assumed correct.
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
            // L-MM2: see the plan_revision comment above — revision and
            // last_transition_at are also not derivable from the engine's open
            // orders, so revision-based fences and refresh_cooldown are inert
            // for live slots (a documented follow-up).
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_maker::runtime::AccountHealthProvider;
    use crate::risk::ActionBudgetSettings;
    use hypeedge_domain::decimal::Price;
    use hypeedge_domain::models::{Fill, Position};

    fn fill_at(timestamp_millis: i64) -> Fill {
        Fill {
            cloid: "c1".into(),
            exchange_oid: "o1".into(),
            symbol: "BTC".into(),
            side: Side::Buy,
            price: Price::new(Decimal::from_str_lenient("100").unwrap()),
            size: Size::new(Decimal::ONE),
            fee: Usd::ZERO,
            is_maker: true,
            timestamp: timestamp_millis,
            strategy_id: Some("mm_1".into()),
            sub_account: Some("sub1".into()),
            is_spot: false,
        }
    }

    fn position() -> Position {
        Position {
            symbol: "BTC".into(),
            size: Size::new(Decimal::ONE),
            entry_price: Some(Price::new(Decimal::from_str_lenient("100").unwrap())),
            mark_price: None,
            unrealized_pnl: None,
            leverage: 1,
            liquidation_price: None,
            sub_account: Some("sub1".into()),
            strategy_id: Some("mm_1".into()),
        }
    }

    // H-MM3: account health is a *freshness* gate, fail-closed — "updated at
    // least once" is not enough.

    #[test]
    fn tracker_health_requires_fresh_account_state() {
        let tracker = Arc::new(AccountTracker::new());
        let health = TrackerHealthProvider::new(tracker.clone());
        assert!(
            !health.allows_risk_increase(),
            "never-updated account must not allow risk increase"
        );
        // Stale update (older than the 30s threshold) → fail closed.
        let stale_ts = (Utc::now() - Duration::seconds(60)).timestamp_millis();
        tracker.apply_authoritative_fill("e_stale", &fill_at(stale_ts), Some(&position()));
        assert!(
            !health.allows_risk_increase(),
            "stale account must not allow risk increase"
        );
        // Fresh update → allowed.
        let fresh_ts = Utc::now().timestamp_millis();
        tracker.apply_authoritative_fill("e_fresh", &fill_at(fresh_ts), Some(&position()));
        assert!(
            health.allows_risk_increase(),
            "fresh account must allow risk increase"
        );
    }

    #[test]
    fn tracker_inventory_healthy_requires_fresh_state() {
        let tracker = Arc::new(AccountTracker::new());
        let provider = TrackerInventoryProvider::new(tracker.clone());
        assert!(!provider.get_inventory("sub1", "BTC").healthy);
        let stale_ts = (Utc::now() - Duration::seconds(60)).timestamp_millis();
        tracker.apply_authoritative_fill("e_stale", &fill_at(stale_ts), Some(&position()));
        assert!(!provider.get_inventory("sub1", "BTC").healthy);
        tracker.apply_authoritative_fill(
            "e_fresh",
            &fill_at(Utc::now().timestamp_millis()),
            Some(&position()),
        );
        assert!(provider.get_inventory("sub1", "BTC").healthy);
    }

    // M-MM11: the budget provider exposes a non-zero action shadow cost.

    #[test]
    fn budget_provider_reports_non_zero_shadow_cost() {
        let address = format!("0x{}", "a".repeat(40));
        let controller =
            ActionBudgetController::new(&address, ActionBudgetSettings::default()).unwrap();
        let provider = ControllerBudgetProvider::new(Arc::new(tokio::sync::Mutex::new(controller)));
        let snapshot = provider.get_action_budget("mm_1", "BTC");
        assert!(snapshot.healthy);
        assert_eq!(
            snapshot.action_shadow_cost_usdc.to_string(),
            DEFAULT_ACTION_SHADOW_COST_USDC
        );
        assert!(snapshot.action_shadow_cost_usdc.inner() > Decimal::ZERO);
    }
}
