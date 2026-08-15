//! Market-making snapshot routes (P4-1 / H-FE2).
//!
//! The frontend `useMarketMaking` hook polls six endpoints per strategy:
//! `state`, `quotes`, `inventory`, `performance`, `action-budget`, `events`.
//! They read the latest [`MarketMakerRuntimeSnapshot`] from the provider the
//! app wires (`AppState::mm_snapshot_provider`). Fields the runtime snapshot
//! does not carry yet — freshness dimensions, budget burn rates, performance
//! history, durable events — are reported as contract-shaped neutral values
//! (`null` / `"0"` / `[]`) so the dashboard renders without backend changes;
//! REST remains authoritative for the lifecycle/state contract.

use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};

use crate::errors::{ApiProblem, ok};
use crate::state::AppState;
use hypeedge_domain::enums::MarketMakerLifecycle;
use hypeedge_trading::market_maker::MarketMakerRuntimeSnapshot;

/// Load the live runtime snapshot, distinguishing a missing provider (503)
/// from a strategy with no live runtime (404).
async fn load_snapshot(
    state: &AppState,
    strategy_id: &str,
) -> Result<Option<MarketMakerRuntimeSnapshot>, Response> {
    let Some(provider) = &state.mm_snapshot_provider else {
        return Err(ApiProblem::new(
            503,
            "MARKET_MAKING_STORE_UNAVAILABLE",
            "Market-making snapshot provider is not wired",
        )
        .with_retryable(true)
        .into_response());
    };
    Ok(provider(strategy_id).await)
}

/// `GET /api/v1/market-making/{strategy_id}/state`
pub async fn mm_state(
    State(state): State<AppState>,
    Path(strategy_id): Path<String>,
) -> Response {
    let snapshot = match load_snapshot(&state, &strategy_id).await {
        Ok(Some(s)) => s,
        Ok(None) => return strategy_not_running(&strategy_id),
        Err(resp) => return resp,
    };
    let instance = state
        .strategies
        .state_store
        .get_instance(&strategy_id)
        .await
        .ok()
        .flatten();
    let sub_account = instance
        .as_ref()
        .map(|i| i.sub_account.clone())
        .unwrap_or_else(|| {
            if state.settings.exchange.account_address.is_empty() {
                "0x0000000000000000000000000000000000000000".to_string()
            } else {
                state.settings.exchange.account_address.to_lowercase()
            }
        });
    let desired_state = instance
        .map(|i| desired_state_str(i.desired_state).to_string())
        .unwrap_or_else(|| desired_state_str(snapshot.mode).to_string());
    let session_mode = match snapshot.mode {
        MarketMakerLifecycle::Shadow => Some("shadow"),
        MarketMakerLifecycle::Running => Some(if state.is_mainnet() {
            "mainnet"
        } else {
            "testnet"
        }),
        _ => None,
    };
    let kill_switch_active = state.kill_switch.is_active().await;
    let safety_mode = state.safety_mode.read().await.clone();
    let observed_at = snapshot
        .last_cycle_at
        .map(|t| t.to_rfc3339())
        .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
    let market_fresh = snapshot.features.is_some();
    ok(serde_json::json!({
        "strategy_id": snapshot.strategy_id,
        "strategy_type": "market_maker",
        "symbol": snapshot.symbol,
        "sub_account": sub_account,
        "environment": state.environment(),
        "desired_state": desired_state,
        "actual_state": snapshot.mode.as_str(),
        "runtime_reason": snapshot.last_reason,
        "runtime_revision": snapshot.quote_revision,
        "market_revision": snapshot.market_version.unwrap_or(0),
        "config_version": snapshot.config_version.unwrap_or(0),
        "session_id": if snapshot.session_id.is_empty() { None } else { Some(snapshot.session_id) },
        "session_mode": session_mode,
        "quote_uptime_pct": null,
        "kill_switch_active": kill_switch_active,
        "safety_mode": safety_mode,
        "freshness": {
            "market": freshness_dim(market_fresh, snapshot.last_cycle_at, 5_000, if market_fresh { None } else { Some("no live book features") }),
            "inventory": freshness_dim(false, None, 60_000, Some("no inventory feed wired")),
            "clearinghouse": freshness_dim(false, None, 60_000, Some("no clearinghouse feed wired")),
            "user_stream": freshness_dim(false, None, 60_000, Some("no user-stream feed wired")),
            "reconciliation": freshness_dim(false, None, 60_000, Some("no reconciliation feed wired")),
            "action_budget": freshness_dim(false, None, 60_000, Some("no action-budget feed wired")),
            "postgres": freshness_dim(false, None, 60_000, Some("no durable store wired")),
        },
        "alerts": [],
        "observed_at": observed_at,
        "stale": false,
    }))
}

/// `GET /api/v1/market-making/{strategy_id}/quotes`
pub async fn mm_quotes(
    State(state): State<AppState>,
    Path(strategy_id): Path<String>,
) -> Response {
    let snapshot = match load_snapshot(&state, &strategy_id).await {
        Ok(Some(s)) => s,
        Ok(None) => return strategy_not_running(&strategy_id),
        Err(resp) => return resp,
    };
    let desired = snapshot.desired.as_ref();
    let features = snapshot.features.as_ref();
    let plan = snapshot.plan.as_ref();
    let external_reference = features
        .filter(|f| f.external_source.is_some())
        .map(|f| {
            serde_json::json!({
                "source": f.external_source,
                "symbol": f.external_symbol,
                "raw_price": f.external_raw_price.map(|p| p.to_string()),
                "adjusted_price": f.external_adjusted_price.map(|p| p.to_string()),
                "basis_bps": f.external_basis_bps.to_string(),
                "effective_weight": f.external_effective_weight.to_string(),
                "confidence": f.external_confidence.to_string(),
                "age_ms": f.external_age_ms,
                "quality": f.external_quality,
                "observed_at": f.external_observed_at.map(|t| t.to_rfc3339()),
            })
        });
    let slots: Vec<serde_json::Value> = plan
        .map(|p| {
            p.diffs
                .iter()
                .map(|diff| {
                    let owner = diff.source.as_ref();
                    serde_json::json!({
                        "side": diff.slot.side.as_str(),
                        "level": diff.slot.level,
                        "state": slot_state(diff.action),
                        "desired_price": diff.desired.price.map(|px| px.to_string()),
                        "desired_size": diff.desired.size.map(|sz| sz.to_string()),
                        "live_price": owner.map(|o| o.price.to_string()),
                        "live_remaining_size": owner.map(|o| o.remaining_size.to_string()),
                        "cloid": owner.map(|o| o.cloid.clone()),
                        "quote_revision": p.revision,
                        "quote_age_ms": null,
                        "gross_edge_bps": null,
                        "no_quote_reason": if diff.desired.decision != hypeedge_domain::enums::QuoteDecision::Quote {
                            Some(diff.reason.clone())
                        } else {
                            None
                        },
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    ok(serde_json::json!({
        "strategy_id": snapshot.strategy_id,
        "symbol": snapshot.symbol,
        "runtime_revision": snapshot.quote_revision,
        "market_revision": snapshot.market_version.unwrap_or(0),
        "fair_price": desired.map(|d| d.fair_price.to_string()),
        "reservation_price": desired.map(|d| d.reservation_price.to_string()),
        "best_bid": features.map(|f| f.best_bid.to_string()),
        "best_ask": features.map(|f| f.best_ask.to_string()),
        "external_reference": external_reference,
        "slots": slots,
        "observed_at": snapshot
            .last_cycle_at
            .map(|t| t.to_rfc3339())
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
        "stale": false,
    }))
}

/// `GET /api/v1/market-making/{strategy_id}/inventory`
pub async fn mm_inventory(
    State(state): State<AppState>,
    Path(strategy_id): Path<String>,
) -> Response {
    let snapshot = match load_snapshot(&state, &strategy_id).await {
        Ok(Some(s)) => s,
        Ok(None) => return strategy_not_running(&strategy_id),
        Err(resp) => return resp,
    };
    let position = state.account_tracker.get_position(&snapshot.symbol);
    let position_size = position
        .map(|p| p.size.to_string())
        .unwrap_or_else(|| "0".to_string());
    let inventory_notional = snapshot
        .desired
        .as_ref()
        .map(|d| d.inventory_notional.to_string())
        .or_else(|| snapshot.plan.as_ref().map(|p| p.inventory_notional.to_string()))
        .unwrap_or_else(|| "0".to_string());
    // Soft/hard/emergency notional limits come from the persisted config.
    let mut limits: (String, String, String) = ("0".into(), "0".into(), "0".into());
    if let Some(version) = snapshot.config_version
        && let Ok(Some(config)) = state
            .strategies
            .state_store
            .get_config(&strategy_id, version)
            .await
    {
        let obj = config.values.as_object();
        let get = |key: &str| {
            obj.and_then(|o| o.get(key))
                .and_then(|v| v.as_str())
                .unwrap_or("0")
                .to_string()
        };
        limits = (
            get("soft_inventory_notional"),
            get("hard_inventory_notional"),
            get("emergency_inventory_notional"),
        );
    }
    let acct = state.account_tracker.get_account_state();
    ok(serde_json::json!({
        "strategy_id": snapshot.strategy_id,
        "symbol": snapshot.symbol,
        "runtime_revision": snapshot.quote_revision,
        "market_revision": snapshot.market_version.unwrap_or(0),
        "position_size": position_size,
        "inventory_notional": inventory_notional,
        "soft_limit_notional": limits.0,
        "hard_limit_notional": limits.1,
        "emergency_limit_notional": limits.2,
        "inventory_utilization": "0",
        "inventory_shift_bps": null,
        "margin_used": acct.as_ref().map(|a| a.total_margin_used.to_string()),
        "available_margin": acct.as_ref().map(|a| a.available_balance.to_string()),
        "liquidation_distance_pct": null,
        "funding_carry": "0",
        "reduction_mode": "none",
        "observed_at": snapshot
            .last_cycle_at
            .map(|t| t.to_rfc3339())
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
        "stale": false,
    }))
}

/// `GET /api/v1/market-making/{strategy_id}/performance`
pub async fn mm_performance(
    State(state): State<AppState>,
    Path(strategy_id): Path<String>,
) -> Response {
    let snapshot = match load_snapshot(&state, &strategy_id).await {
        Ok(Some(s)) => s,
        Ok(None) => return strategy_not_running(&strategy_id),
        Err(resp) => return resp,
    };
    // Accounting/execution quality are derived from the durable ledger, which
    // the API does not query yet (P4-2 wires the SSE stream; a performance
    // store is a follow-up). Report the contract shape with neutral values.
    ok(serde_json::json!({
        "strategy_id": snapshot.strategy_id,
        "accounting": null,
        "execution_quality": null,
        "inventory_episodes": [],
        "source": "postgres",
        "as_of": chrono::Utc::now().to_rfc3339(),
        "stale": false,
    }))
}

/// `GET /api/v1/market-making/{strategy_id}/action-budget`
pub async fn mm_action_budget(
    State(state): State<AppState>,
    Path(strategy_id): Path<String>,
) -> Response {
    let snapshot = match load_snapshot(&state, &strategy_id).await {
        Ok(Some(s)) => s,
        Ok(None) => return strategy_not_running(&strategy_id),
        Err(resp) => return resp,
    };
    // The live ActionBudgetController is per-account, not per-strategy; the
    // per-strategy mode/revision come from the runtime snapshot, the rest from
    // the controller's `ActionBudgetView` (windows carry burn/earn/runway).
    let budget = match &state.action_budget {
        Some(b) => Some(b.lock().await.snapshot()),
        None => None,
    };
    let (mode, revision) = match &snapshot.plan {
        Some(plan) => (plan.budget_mode.as_str().to_string(), plan.revision),
        None => (
            budget
                .as_ref()
                .map(|b| b.mode.as_str().to_string())
                .unwrap_or_else(|| "normal".to_string()),
            snapshot.quote_revision,
        ),
    };
    // Window stats by horizon (1h/6h/24h) when the controller has them.
    let window_stats = |hours: i64| {
        budget
            .as_ref()
            .and_then(|b| b.windows.iter().find(|w| w.window_hours == hours))
    };
    let rate = |hours: i64| {
        window_stats(hours)
            .map(|w| w.net_burn_per_hour.to_string())
            .unwrap_or_else(|| "0".to_string())
    };
    ok(serde_json::json!({
        "strategy_id": snapshot.strategy_id,
        "mode": mode,
        "remote_cap": budget
            .as_ref()
            .map(|b| b.remote_cap.max(0).to_string())
            .unwrap_or_else(|| "0".to_string()),
        "remote_used": budget
            .as_ref()
            .map(|b| b.remote_used.max(0).to_string())
            .unwrap_or_else(|| "0".to_string()),
        "remote_remaining": budget
            .as_ref()
            .map(|b| b.address_remaining.max(0).to_string())
            .unwrap_or_else(|| "0".to_string()),
        "shadow_remaining": "0",
        "emergency_reserve": "0",
        "cancel_headroom": budget
            .as_ref()
            .map(|b| b.cancel_headroom_remaining.max(0).to_string())
            .unwrap_or_else(|| "0".to_string()),
        "ip_weight_remaining": budget
            .as_ref()
            .map(|b| b.ip_weight_remaining.max(0).to_string())
            .unwrap_or_else(|| "0".to_string()),
        "burn_rate_1h": rate(1),
        "burn_rate_6h": rate(6),
        "burn_rate_24h": rate(24),
        "earned_rate_24h": window_stats(24)
            .map(|w| w.earned_actions.to_string())
            .unwrap_or_else(|| "0".to_string()),
        "usdc_per_action": window_stats(1)
            .and_then(|w| w.marginal_usdc_per_action)
            .map(|d| d.to_string()),
        "actions_per_fill": window_stats(24)
            .and_then(|w| w.actions_per_fill)
            .map(|d| d.to_string()),
        "runway_hours": window_stats(24).map(|w| w.runway_hours.to_string()),
        "revision": revision,
        "observed_at": snapshot
            .last_cycle_at
            .map(|t| t.to_rfc3339())
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
        "stale": false,
    }))
}

/// `GET /api/v1/market-making/{strategy_id}/events?limit=200`
///
/// Durable market-making events flow through the SSE stream (`/api/v1/events`)
/// rather than a REST history store, which the API does not query yet. Return
/// an empty, contract-shaped list so the frontend renders its empty state.
pub async fn mm_events(
    State(state): State<AppState>,
    Path(strategy_id): Path<String>,
    Query(_params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    match load_snapshot(&state, &strategy_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return strategy_not_running(&strategy_id),
        Err(resp) => return resp,
    }
    ok(serde_json::json!([]))
}

fn strategy_not_running(strategy_id: &str) -> Response {
    ApiProblem::new(
        404,
        "STRATEGY_NOT_FOUND",
        format!(
            "No live market-making runtime for strategy {strategy_id}; start it from the strategy page"
        ),
    )
    .into_response()
}

/// Map a lifecycle mode to the frontend `StrategyDesiredState` subset.
fn desired_state_str(mode: MarketMakerLifecycle) -> &'static str {
    match mode {
        MarketMakerLifecycle::Shadow => "shadow",
        MarketMakerLifecycle::Running => "running",
        MarketMakerLifecycle::Paused => "paused",
        _ => "stopped",
    }
}

/// Map a plan diff action to the frontend `QuoteSlotState`.
fn slot_state(action: hypeedge_domain::enums::QuoteAction) -> &'static str {
    use hypeedge_domain::enums::QuoteAction;
    match action {
        QuoteAction::Place | QuoteAction::Modify | QuoteAction::Keep => "live",
        QuoteAction::CancelThenPlace => "inflight",
        QuoteAction::BlockedUnknown => "unknown",
        QuoteAction::Cancel | QuoteAction::NoAction => "empty",
    }
}

fn freshness_dim(
    fresh: bool,
    observed_at: Option<chrono::DateTime<chrono::Utc>>,
    threshold_ms: i64,
    reason: Option<&str>,
) -> serde_json::Value {
    serde_json::json!({
        "status": if fresh { "fresh" } else { "unknown" },
        "observed_at": observed_at.map(|t| t.to_rfc3339()),
        "age_ms": null,
        "threshold_ms": threshold_ms,
        "reason": reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::sync::Arc;
    use tower::ServiceExt;

    use crate::state::MmSnapshotProvider;
    use hypeedge_domain::decimal::{Decimal, Price, Size, Usd};
    use hypeedge_domain::enums::{ActionBudgetMode, QuoteAction, QuoteDecision, Side};
    use hypeedge_trading::market_maker::MarketMakerRuntimeSnapshot;
    use hypeedge_trading::trading::quotes::{
        DesiredQuote, DesiredQuoteSet, QuoteDiff, QuotePlan, QuoteSlotKey,
    };

    fn test_state() -> AppState {
        let settings = Arc::new(hypeedge_config::settings::AppSettings::default());
        let bus = Arc::new(hypeedge_infra::event_bus::EventBus::new(64));
        let ks = Arc::new(hypeedge_trading::risk::KillSwitch::new(bus.clone(), false));
        AppState::new(
            settings,
            ks,
            bus,
            Arc::new(tokio::sync::Mutex::new(
                hypeedge_trading::market_data::BookManager::new(20),
            )),
        )
    }

    fn dec(s: &str) -> Decimal {
        Decimal::from_str_lenient(s).unwrap()
    }

    fn sample_snapshot() -> MarketMakerRuntimeSnapshot {
        let now = chrono::Utc::now();
        let slot = |side: Side, level: u32| QuoteSlotKey {
            strategy_id: "mm_1".into(),
            symbol: "BTC".into(),
            side,
            level,
        };
        let desired = DesiredQuoteSet {
            strategy_id: "mm_1".into(),
            symbol: "BTC".into(),
            session_id: "sess-1".into(),
            config_version: 3,
            model_version: "v1".into(),
            market_version: 12,
            connection_generation: 1,
            current_slot_revision: 5,
            revision: 7,
            fair_price: Price::new(dec("50000")),
            reservation_price: Price::new(dec("49990")),
            inventory_notional: Usd::new(dec("1000")),
            expected_utility_usdc: Usd::new(dec("1.5")),
            budget_mode: ActionBudgetMode::Normal,
            bid: DesiredQuote {
                slot: slot(Side::Buy, 0),
                decision: QuoteDecision::Quote,
                price: Some(Price::new(dec("49980"))),
                size: Some(Size::new(dec("0.5"))),
                gross_edge_usdc: Usd::new(dec("0.25")),
                reason: "edge".into(),
            },
            ask: DesiredQuote {
                slot: slot(Side::Sell, 0),
                decision: QuoteDecision::Quote,
                price: Some(Price::new(dec("50020"))),
                size: Some(Size::new(dec("0.5"))),
                gross_edge_usdc: Usd::new(dec("0.25")),
                reason: "edge".into(),
            },
            created_at: now,
            valid_until: now + chrono::Duration::seconds(5),
            feature_values: vec![],
        };
        let plan = QuotePlan {
            strategy_id: "mm_1".into(),
            symbol: "BTC".into(),
            session_id: "sess-1".into(),
            config_version: 3,
            revision: 7,
            market_version: 12,
            connection_generation: 1,
            valid_until: now + chrono::Duration::seconds(5),
            diffs: vec![QuoteDiff {
                slot: slot(Side::Buy, 0),
                action: QuoteAction::Place,
                source: None,
                desired: DesiredQuote {
                    slot: slot(Side::Buy, 0),
                    decision: QuoteDecision::Quote,
                    price: Some(Price::new(dec("49980"))),
                    size: Some(Size::new(dec("0.5"))),
                    gross_edge_usdc: Usd::new(dec("0.25")),
                    reason: "edge".into(),
                },
                child_actions: vec!["place".into()],
                reason: "new quote".into(),
                gross_edge_usdc: Usd::new(dec("0.25")),
                transition_cost_usdc: Usd::ZERO,
                net_incremental_utility_usdc: Usd::new(dec("0.25")),
            }],
            fair_price: Some(Price::new(dec("50000"))),
            reservation_price: Some(Price::new(dec("49990"))),
            inventory_notional: Usd::new(dec("1000")),
            budget_mode: ActionBudgetMode::Normal,
            fenced: false,
            fence_reason: None,
        };
        MarketMakerRuntimeSnapshot {
            strategy_id: "mm_1".into(),
            session_id: "sess-1".into(),
            symbol: "BTC".into(),
            mode: MarketMakerLifecycle::Shadow,
            config_version: Some(3),
            quote_revision: 7,
            market_version: Some(12),
            connection_generation: Some(1),
            last_cycle_at: Some(now),
            last_reason: Some("cycled".into()),
            desired: Some(desired),
            plan: Some(plan),
            features: None,
        }
    }

    fn state_with_provider() -> AppState {
        let mut state = test_state();
        let snapshot = sample_snapshot();
        let provider: MmSnapshotProvider = Arc::new(move |id: &str| {
            let id = id.to_string();
            let snapshot = snapshot.clone();
            Box::pin(async move {
                if id != "mm_1" {
                    return None;
                }
                Some(snapshot)
            })
        });
        state.mm_snapshot_provider = Some(provider);
        state
    }

    async fn get_json(router: axum::Router, uri: &str) -> serde_json::Value {
        let resp = router
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "GET {uri}");
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["ok"], true, "envelope for {uri}: {json}");
        json["data"].clone()
    }

    #[tokio::test]
    async fn state_endpoint_returns_contract_shape() {
        let router = crate::build_router(state_with_provider());
        let data = get_json(router, "/api/v1/market-making/mm_1/state").await;
        assert_eq!(data["strategy_id"], "mm_1");
        assert_eq!(data["strategy_type"], "market_maker");
        assert_eq!(data["actual_state"], "shadow");
        assert_eq!(data["session_mode"], "shadow");
        assert_eq!(data["runtime_revision"], 7);
        assert_eq!(data["config_version"], 3);
        assert_eq!(data["kill_switch_active"], false);
        assert!(data["freshness"]["market"].is_object());
        assert!(data["alerts"].is_array());
    }

    #[tokio::test]
    async fn quotes_endpoint_returns_contract_shape() {
        let router = crate::build_router(state_with_provider());
        let data = get_json(router, "/api/v1/market-making/mm_1/quotes").await;
        assert_eq!(data["strategy_id"], "mm_1");
        assert_eq!(data["fair_price"], "50000");
        assert_eq!(data["reservation_price"], "49990");
        let slots = data["slots"].as_array().unwrap();
        assert_eq!(slots.len(), 1);
        assert_eq!(slots[0]["side"], "buy");
        assert_eq!(slots[0]["state"], "live");
        assert_eq!(slots[0]["desired_price"], "49980");
        assert_eq!(slots[0]["quote_revision"], 7);
    }

    #[tokio::test]
    async fn inventory_endpoint_returns_contract_shape() {
        let router = crate::build_router(state_with_provider());
        let data = get_json(router, "/api/v1/market-making/mm_1/inventory").await;
        assert_eq!(data["strategy_id"], "mm_1");
        assert_eq!(data["inventory_notional"], "1000");
        assert_eq!(data["reduction_mode"], "none");
    }

    #[tokio::test]
    async fn performance_endpoint_returns_contract_shape() {
        let router = crate::build_router(state_with_provider());
        let data = get_json(router, "/api/v1/market-making/mm_1/performance").await;
        assert_eq!(data["strategy_id"], "mm_1");
        assert_eq!(data["accounting"], serde_json::Value::Null);
        assert!(data["inventory_episodes"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn action_budget_endpoint_returns_contract_shape() {
        let router = crate::build_router(state_with_provider());
        let data = get_json(router, "/api/v1/market-making/mm_1/action-budget").await;
        assert_eq!(data["strategy_id"], "mm_1");
        assert_eq!(data["mode"], "normal");
        assert_eq!(data["revision"], 7);
    }

    #[tokio::test]
    async fn events_endpoint_returns_empty_list() {
        let router = crate::build_router(state_with_provider());
        let data = get_json(router, "/api/v1/market-making/mm_1/events?limit=200").await;
        assert!(data.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unknown_strategy_returns_404() {
        let router = crate::build_router(state_with_provider());
        let resp = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/market-making/ghost/state")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn missing_provider_returns_503() {
        let router = crate::build_router(test_state());
        let resp = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/market-making/mm_1/state")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
