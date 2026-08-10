//! Reconciler — corrects local state against exchange truth, port of
//! `src/hypeedge/account/reconciler.py`.
//!
//! Runs on startup (before strategies resume), on WS reconnection, and
//! periodically. Mismatches are logged and corrected: **local → exchange wins**.
//! The pure comparison and parsing helpers are tested against scripted
//! clearinghouse snapshots; the exchange-fetch boundary is a trait so the full
//! cycle is unit-testable without network access.

use std::collections::BTreeMap;

use hypeedge_domain::decimal::{Decimal, Price, Size, Usd};
use hypeedge_domain::enums::OrderStatus;
use hypeedge_domain::models::{AccountState, Order, Position, SpotBalance};
use serde_json::Value;

use crate::account::tracker::AccountTracker;
use crate::execution::cloid::CloidGenerator;

/// One detected difference between local state and exchange truth.
#[derive(Debug, Clone, PartialEq)]
pub struct ReconDiff {
    pub entity_type: String,
    pub entity_key: String,
    pub difference_type: String,
    pub local_value: Option<Value>,
    pub exchange_value: Option<Value>,
    pub severity: Option<String>,
}

/// Result of a reconciliation cycle.
#[derive(Debug, Clone)]
pub struct ReconciliationResult {
    pub success: bool,
    pub orders_corrected: u64,
    pub positions_corrected: u64,
    pub spot_balances_corrected: u64,
    pub errors: Vec<String>,
}

/// Pure reconciliation logic, ported from the Python `Reconciler` static
/// helpers. The exchange-fetch boundary is left to the caller.
pub struct ReconcilerLogic;

impl ReconcilerLogic {
    /// Compare local open orders/positions/balances against exchange snapshots
    /// and list every difference (design doc: local → exchange wins).
    pub fn build_diffs(
        local_orders: &[Order],
        local_positions: &[Position],
        local_spot_balances: &[SpotBalance],
        exchange_orders: &[Value],
        exchange_positions: &BTreeMap<String, Value>,
        exchange_spot_balances: &BTreeMap<String, Value>,
    ) -> Vec<ReconDiff> {
        let mut diffs: Vec<ReconDiff> = Vec::new();

        let local_cloids: BTreeMap<String, &Order> =
            local_orders.iter().map(|o| (o.cloid.clone(), o)).collect();
        let exchange_cloids: BTreeMap<String, &Value> = exchange_orders
            .iter()
            .filter_map(|o| {
                o.get("cloid")
                    .and_then(|c| c.as_str())
                    .map(|c| (c.to_string(), o))
            })
            .collect();

        // Local open orders missing on the exchange.
        for (cloid, order) in &local_cloids {
            let canonical = canonical_cloid(cloid, order);
            if !exchange_cloids.contains_key(&canonical) {
                diffs.push(ReconDiff {
                    entity_type: "order".into(),
                    entity_key: cloid.clone(),
                    difference_type: "local_open_missing_on_exchange".into(),
                    local_value: Some(serde_json::json!({ "status": order.status.as_str() })),
                    exchange_value: None,
                    severity: None,
                });
            }
        }
        let canonical_local: BTreeMap<String, &Order> = local_cloids
            .iter()
            .map(|(cloid, order)| (canonical_cloid(cloid, order), *order))
            .collect();
        // Exchange open orders missing locally.
        for (cloid, exchange_order) in &exchange_cloids {
            if !canonical_local.contains_key(cloid) {
                diffs.push(ReconDiff {
                    entity_type: "order".into(),
                    entity_key: cloid.clone(),
                    difference_type: "exchange_open_missing_locally".into(),
                    local_value: None,
                    exchange_value: Some((*exchange_order).clone()),
                    severity: None,
                });
            }
        }

        // Position size mismatches.
        let local_by_symbol: BTreeMap<String, &Position> = local_positions
            .iter()
            .map(|p| (p.symbol.clone(), p))
            .collect();
        for (symbol, exchange) in exchange_positions {
            let exchange_size = exchange
                .get("szi")
                .and_then(|v| v.as_str())
                .and_then(|s| Decimal::from_str_lenient(s).ok())
                .unwrap_or(Decimal::ZERO);
            match local_by_symbol.get(symbol) {
                Some(local)
                    if (local.size.inner() - exchange_size).abs()
                        > Decimal::from_str_strict("0.00000001").unwrap() =>
                {
                    diffs.push(ReconDiff {
                        entity_type: "position".into(),
                        entity_key: symbol.clone(),
                        difference_type: "size_mismatch".into(),
                        local_value: Some(serde_json::json!({ "size": local.size.to_string() })),
                        exchange_value: Some(
                            serde_json::json!({ "size": exchange_size.to_string() }),
                        ),
                        severity: Some("critical".into()),
                    });
                }
                None => {
                    diffs.push(ReconDiff {
                        entity_type: "position".into(),
                        entity_key: symbol.clone(),
                        difference_type: "size_mismatch".into(),
                        local_value: None,
                        exchange_value: Some(
                            serde_json::json!({ "size": exchange_size.to_string() }),
                        ),
                        severity: Some("critical".into()),
                    });
                }
                _ => {}
            }
        }
        // Local positions closed on the exchange.
        for (symbol, local) in &local_by_symbol {
            if !exchange_positions.contains_key(symbol) && !local.is_flat() {
                diffs.push(ReconDiff {
                    entity_type: "position".into(),
                    entity_key: symbol.clone(),
                    difference_type: "closed_on_exchange".into(),
                    local_value: Some(serde_json::json!({ "size": local.size.to_string() })),
                    exchange_value: None,
                    severity: Some("critical".into()),
                });
            }
        }

        // Spot balance mismatches.
        let local_spot: BTreeMap<String, &SpotBalance> = local_spot_balances
            .iter()
            .map(|b| (b.token.clone(), b))
            .collect();
        let mut tokens: Vec<String> = local_spot.keys().cloned().collect();
        tokens.extend(exchange_spot_balances.keys().cloned());
        tokens.sort();
        tokens.dedup();
        for token in tokens {
            let spot_local = local_spot.get(&token);
            let spot_exchange = exchange_spot_balances.get(&token);
            let local_total = spot_local.map(|b| b.total.inner()).unwrap_or(Decimal::ZERO);
            let exchange_total = spot_exchange
                .and_then(|v| v.get("total"))
                .and_then(|v| v.as_str())
                .and_then(|s| Decimal::from_str_lenient(s).ok())
                .unwrap_or(Decimal::ZERO);
            let local_hold = spot_local.map(|b| b.hold.inner()).unwrap_or(Decimal::ZERO);
            let exchange_hold = spot_exchange
                .and_then(|v| v.get("hold"))
                .and_then(|v| v.as_str())
                .and_then(|s| Decimal::from_str_lenient(s).ok())
                .unwrap_or(Decimal::ZERO);
            if local_total != exchange_total || local_hold != exchange_hold {
                diffs.push(ReconDiff {
                    entity_type: "spot_balance".into(),
                    entity_key: token.clone(),
                    difference_type: "spot_balance_mismatch".into(),
                    local_value: Some(serde_json::json!({
                        "total": local_total.to_string(),
                        "hold": local_hold.to_string(),
                    })),
                    exchange_value: Some(serde_json::json!({
                        "total": exchange_total.to_string(),
                        "hold": exchange_hold.to_string(),
                    })),
                    severity: Some("critical".into()),
                });
            }
        }
        diffs
    }

    /// Parse `assetPositions` from a clearinghouse snapshot into coin → raw
    /// position. Mirrors `_positions_from_clearinghouse`.
    pub fn positions_from_clearinghouse(state: &Value) -> Result<BTreeMap<String, Value>, String> {
        let Some(asset_positions) = state.get("assetPositions").and_then(|v| v.as_array()) else {
            return Err("invalid_user_state_response".into());
        };
        let mut positions = BTreeMap::new();
        for pos_data in asset_positions {
            let pos_info = pos_data.get("position").cloned().unwrap_or(Value::Null);
            let coin = pos_info.get("coin").and_then(|c| c.as_str()).unwrap_or("");
            if !coin.is_empty() {
                positions.insert(coin.to_string(), pos_info);
            }
        }
        Ok(positions)
    }

    /// Build the authoritative `AccountState` from a clearinghouse snapshot.
    /// Mirrors `_account_from_clearinghouse`.
    pub fn account_from_clearinghouse(
        state: &Value,
        current_peak_equity: Usd,
    ) -> Result<AccountState, String> {
        let margin_summary = state
            .get("marginSummary")
            .filter(|v| v.is_object())
            .ok_or_else(|| "invalid_account_state_response".to_string())?;

        let mut unrealized_pnl = Decimal::ZERO;
        if let Some(asset_positions) = state.get("assetPositions").and_then(|v| v.as_array()) {
            for item in asset_positions {
                let u = item
                    .get("position")
                    .and_then(|p| p.get("unrealizedPnl"))
                    .and_then(|v| v.as_str())
                    .and_then(|s| Decimal::from_str_lenient(s).ok())
                    .unwrap_or(Decimal::ZERO);
                unrealized_pnl += u;
            }
        }
        let account_value = margin_summary
            .get("accountValue")
            .and_then(|v| v.as_str())
            .and_then(|s| Decimal::from_str_lenient(s).ok())
            .unwrap_or(Decimal::ZERO);
        let available = state
            .get("withdrawable")
            .and_then(|v| v.as_str())
            .and_then(|s| Decimal::from_str_lenient(s).ok())
            .unwrap_or_else(|| {
                margin_summary
                    .get("totalMarginAvailable")
                    .and_then(|v| v.as_str())
                    .and_then(|s| Decimal::from_str_lenient(s).ok())
                    .unwrap_or(Decimal::ZERO)
            });
        let total_margin_used = margin_summary
            .get("totalMarginUsed")
            .and_then(|v| v.as_str())
            .and_then(|s| Decimal::from_str_lenient(s).ok())
            .unwrap_or_else(|| (account_value - available).max(Decimal::ZERO));
        let peak = current_peak_equity.inner().max(account_value);
        Ok(AccountState {
            equity: Usd::new(account_value),
            available_balance: Usd::new(available),
            total_margin_used: Usd::new(total_margin_used),
            total_unrealized_pnl: Usd::new(unrealized_pnl),
            peak_equity: Usd::new(peak),
            sub_account: None,
        })
    }

    /// Convert an exchange open-order payload into a domain `Order` (used for
    /// importing exchange truth before a cancel-all).
    pub fn parse_exchange_order(
        exchange_order: &Value,
        canonical_cloid: &str,
    ) -> Result<Order, String> {
        let symbol = exchange_order
            .get("coin")
            .and_then(|c| c.as_str())
            .ok_or_else(|| "exchange_order_missing_coin".to_string())?;
        let side = match exchange_order.get("side").and_then(|s| s.as_str()) {
            Some("B") | Some("buy") | Some("Bid") => hypeedge_domain::enums::Side::Buy,
            Some("A") | Some("sell") | Some("Ask") => hypeedge_domain::enums::Side::Sell,
            _ => return Err("exchange_order_missing_side".into()),
        };
        let size = exchange_order
            .get("sz")
            .and_then(|v| v.as_str())
            .and_then(|s| Decimal::from_str_lenient(s).ok())
            .ok_or_else(|| "exchange_order_missing_size".to_string())?;
        let price = exchange_order
            .get("limitPx")
            .and_then(|v| v.as_str())
            .and_then(|s| Decimal::from_str_lenient(s).ok())
            .ok_or_else(|| "exchange_order_missing_price".to_string())?;
        let status = match exchange_order.get("status").and_then(|s| s.as_str()) {
            Some("open") | Some("resting") | Some("triggered") => OrderStatus::Acknowledged,
            _ => OrderStatus::Pending,
        };
        Ok(Order {
            cloid: canonical_cloid.to_string(),
            symbol: symbol.to_string(),
            side,
            size: Size::new(size),
            price: Some(Price::new(price)),
            order_type: hypeedge_domain::enums::OrderType::Limit,
            time_in_force: hypeedge_domain::enums::TimeInForce::Gtc,
            status,
            strategy_id: None,
            sub_account: None,
            reduce_only: exchange_order
                .get("reduceOnly")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            is_spot: false,
            risk_reducing: false,
            max_slippage_bps: 50,
            exchange_oid: exchange_order.get("oid").map(|o| o.to_string()),
            filled_size: Size::ZERO,
            avg_fill_price: None,
            submitted_at: None,
            acknowledged_at: None,
            filled_at: None,
            error_message: None,
            created_at: chrono::Utc::now(),
        })
    }
}

/// HL cloids are `0x` + 32 hex; internal cloids are hashed to that format for
/// comparison against exchange truth.
fn canonical_cloid(cloid: &str, order: &Order) -> String {
    if cloid.starts_with("0x") {
        cloid.to_string()
    } else {
        CloidGenerator::to_hl_cloid(&order.cloid)
    }
}

/// A reconciler cycle that applies exchange truth to the tracker. This is the
/// pure apply step — exchange fetching is injected by the caller.
pub struct Reconciler {
    tracker: std::sync::Arc<AccountTracker>,
}

impl Reconciler {
    pub fn new(tracker: std::sync::Arc<AccountTracker>) -> Self {
        Self { tracker }
    }

    /// Apply exchange truth: correct positions and spot balances in the
    /// tracker. Returns the number of corrections applied (local → exchange).
    pub fn apply_exchange_truth(
        &self,
        exchange_positions: &BTreeMap<String, Value>,
        exchange_spot_balances: &BTreeMap<String, Value>,
        exchange_account: &AccountState,
    ) -> (u64, u64) {
        // Track current local state to compute corrections.
        let local_positions = self.tracker.get_all_positions();
        let mut positions_corrected = 0u64;
        let mut seen: std::collections::HashSet<String> = Default::default();
        for (symbol, raw) in exchange_positions {
            seen.insert(symbol.clone());
            let size = raw
                .get("szi")
                .and_then(|v| v.as_str())
                .and_then(|s| Decimal::from_str_lenient(s).ok())
                .unwrap_or(Decimal::ZERO);
            let local = local_positions.iter().find(|p| p.symbol == *symbol);
            let local_size = local.map(|p| p.size.inner()).unwrap_or(Decimal::ZERO);
            if (local_size - size).abs() > Decimal::from_str_strict("0.00000001").unwrap() {
                positions_corrected += 1;
                let pos = Position {
                    symbol: symbol.clone(),
                    size: Size::new(size),
                    entry_price: raw
                        .get("entryPx")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Decimal::from_str_lenient(s).ok())
                        .map(Price::new),
                    mark_price: raw
                        .get("markPx")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Decimal::from_str_lenient(s).ok())
                        .map(Price::new),
                    unrealized_pnl: raw
                        .get("unrealizedPnl")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Decimal::from_str_lenient(s).ok())
                        .map(Usd::new),
                    leverage: raw
                        .get("leverage")
                        .and_then(|v| v.get("value"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0)
                        .max(0) as u32,
                    liquidation_price: raw
                        .get("liquidationPx")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Decimal::from_str_lenient(s).ok())
                        .map(Price::new),
                    sub_account: None,
                    strategy_id: None,
                };
                self.tracker.update_position_from_exchange(symbol, pos);
            }
        }
        // Local positions that the exchange no longer reports are closed.
        for local in &local_positions {
            if !seen.contains(&local.symbol) && !local.is_flat() {
                positions_corrected += 1;
                self.tracker.remove_position(&local.symbol);
            }
        }

        // Spot balances replace atomically.
        let observed_at = chrono::Utc::now();
        let balances: Vec<SpotBalance> = exchange_spot_balances
            .iter()
            .map(|(token, raw)| SpotBalance {
                token: token.clone(),
                total: Size::new(
                    raw.get("total")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Decimal::from_str_lenient(s).ok())
                        .unwrap_or(Decimal::ZERO),
                ),
                hold: Size::new(
                    raw.get("hold")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Decimal::from_str_lenient(s).ok())
                        .unwrap_or(Decimal::ZERO),
                ),
                entry_ntl: raw
                    .get("entryNtl")
                    .and_then(|v| v.as_str())
                    .and_then(|s| Decimal::from_str_lenient(s).ok())
                    .map(Usd::new)
                    .unwrap_or(Usd::ZERO),
                sub_account: None,
                updated_at: observed_at,
            })
            .collect();
        self.tracker.update_spot_balances(&balances, observed_at);

        self.tracker.update_account_state(exchange_account);
        (positions_corrected, balances.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::decimal::Decimal as D;

    fn order(cloid: &str, status: OrderStatus) -> Order {
        Order {
            cloid: cloid.to_string(),
            symbol: "BTC".into(),
            side: hypeedge_domain::enums::Side::Buy,
            size: Size::new(D::from_str_strict("1.0").unwrap()),
            price: Some(Price::new(D::from_str_strict("50000").unwrap())),
            order_type: hypeedge_domain::enums::OrderType::Limit,
            time_in_force: hypeedge_domain::enums::TimeInForce::Gtc,
            status,
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
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn diffs_detect_local_order_missing_on_exchange() {
        // Internal cloid "mm_1" hashes to a 0x cloid the exchange doesn't know.
        let local = order("mm_1_1700000000000_abc", OrderStatus::Acknowledged);
        let diffs = ReconcilerLogic::build_diffs(
            &[local],
            &[],
            &[],
            &[],
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].difference_type, "local_open_missing_on_exchange");
    }

    #[test]
    fn diffs_detect_exchange_order_missing_locally() {
        let exchange_orders = vec![serde_json::json!({
            "cloid": "0xabc", "coin": "BTC", "sz": "0.5", "limitPx": "50000", "side": "B"
        })];
        let diffs = ReconcilerLogic::build_diffs(
            &[],
            &[],
            &[],
            &exchange_orders,
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].difference_type, "exchange_open_missing_locally");
        assert_eq!(diffs[0].entity_key, "0xabc");
    }

    #[test]
    fn diffs_detect_position_size_mismatch() {
        let local_pos = Position {
            symbol: "BTC".into(),
            size: Size::new(D::from_str_strict("1.0").unwrap()),
            entry_price: None,
            mark_price: None,
            unrealized_pnl: None,
            leverage: 0,
            liquidation_price: None,
            sub_account: None,
            strategy_id: None,
        };
        let mut exchange_positions = BTreeMap::new();
        exchange_positions.insert("BTC".into(), serde_json::json!({"szi": "2.0"}));
        let diffs = ReconcilerLogic::build_diffs(
            &[],
            &[local_pos],
            &[],
            &[],
            &exchange_positions,
            &BTreeMap::new(),
        );
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].difference_type, "size_mismatch");
        assert_eq!(diffs[0].severity.as_deref(), Some("critical"));
    }

    #[test]
    fn diffs_detect_spot_balance_mismatch() {
        let local_spot = SpotBalance {
            token: "USDC".into(),
            total: Size::new(D::from_str_strict("1000").unwrap()),
            hold: Size::ZERO,
            entry_ntl: Usd::ZERO,
            sub_account: None,
            updated_at: chrono::Utc::now(),
        };
        let mut exchange_spot = BTreeMap::new();
        exchange_spot.insert(
            "USDC".into(),
            serde_json::json!({"total": "900", "hold": "0"}),
        );
        let diffs = ReconcilerLogic::build_diffs(
            &[],
            &[],
            &[local_spot],
            &[],
            &BTreeMap::new(),
            &exchange_spot,
        );
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].difference_type, "spot_balance_mismatch");
    }

    #[test]
    fn account_from_clearinghouse_matches_python_math() {
        let state = serde_json::json!({
            "marginSummary": {"accountValue": "10000", "totalMarginUsed": "500"},
            "withdrawable": "9500",
            "assetPositions": [
                {"position": {"coin": "BTC", "unrealizedPnl": "100"}}
            ]
        });
        let acct = ReconcilerLogic::account_from_clearinghouse(&state, Usd::ZERO).unwrap();
        assert_eq!(acct.equity.to_string(), "10000");
        assert_eq!(acct.available_balance.to_string(), "9500");
        assert_eq!(acct.total_margin_used.to_string(), "500");
        assert_eq!(acct.total_unrealized_pnl.to_string(), "100");
        assert_eq!(acct.peak_equity.to_string(), "10000");
    }

    #[test]
    fn account_from_clearinghouse_keeps_peak_equity() {
        let state = serde_json::json!({"marginSummary": {"accountValue": "8000"}});
        let acct = ReconcilerLogic::account_from_clearinghouse(
            &state,
            Usd::new(D::from_str_strict("12000").unwrap()),
        )
        .unwrap();
        assert_eq!(acct.peak_equity.to_string(), "12000");
    }

    #[test]
    fn positions_from_clearinghouse_extracts_by_coin() {
        let state = serde_json::json!({
            "assetPositions": [
                {"position": {"coin": "BTC", "szi": "1.0"}},
                {"position": {"coin": "ETH", "szi": "-2.0"}}
            ]
        });
        let pos = ReconcilerLogic::positions_from_clearinghouse(&state).unwrap();
        assert_eq!(pos.len(), 2);
        assert_eq!(pos["BTC"]["szi"], "1.0");
        assert_eq!(pos["ETH"]["szi"], "-2.0");
    }

    #[test]
    fn parse_exchange_order_roundtrips() {
        let raw = serde_json::json!({
            "coin": "BTC", "side": "B", "sz": "0.5", "limitPx": "50000",
            "oid": 12345, "reduceOnly": false, "status": "open"
        });
        let order = ReconcilerLogic::parse_exchange_order(&raw, "0xabc").unwrap();
        assert_eq!(order.symbol, "BTC");
        assert_eq!(order.size.to_string(), "0.5");
        assert_eq!(order.price.unwrap().to_string(), "50000");
        assert_eq!(order.status, OrderStatus::Acknowledged);
        assert_eq!(order.exchange_oid.as_deref(), Some("12345"));
    }

    #[tokio::test]
    async fn reconciler_applies_exchange_truth() {
        let tracker = std::sync::Arc::new(AccountTracker::new());
        // Local state has a stale BTC position.
        tracker.update_position_from_exchange(
            "BTC",
            Position {
                symbol: "BTC".into(),
                size: Size::new(D::from_str_strict("5.0").unwrap()),
                entry_price: None,
                mark_price: None,
                unrealized_pnl: None,
                leverage: 0,
                liquidation_price: None,
                sub_account: None,
                strategy_id: None,
            },
        );
        let reconciler = Reconciler::new(tracker.clone());
        let mut exchange_positions = BTreeMap::new();
        exchange_positions.insert("BTC".into(), serde_json::json!({"szi": "2.0"}));
        let acct = ReconcilerLogic::account_from_clearinghouse(
            &serde_json::json!({"marginSummary": {"accountValue": "10000"}}),
            Usd::ZERO,
        )
        .unwrap();
        let (corrected, _spot) =
            reconciler.apply_exchange_truth(&exchange_positions, &BTreeMap::new(), &acct);
        assert_eq!(corrected, 1);
        // Exchange wins.
        assert_eq!(tracker.get_position("BTC").unwrap().size.to_string(), "2");
        assert_eq!(
            tracker.get_account_state().unwrap().equity.to_string(),
            "10000"
        );
    }

    #[tokio::test]
    async fn reconciler_removes_local_position_closed_on_exchange() {
        let tracker = std::sync::Arc::new(AccountTracker::new());
        tracker.update_position_from_exchange(
            "ETH",
            Position {
                symbol: "ETH".into(),
                size: Size::new(D::from_str_strict("1.0").unwrap()),
                entry_price: None,
                mark_price: None,
                unrealized_pnl: None,
                leverage: 0,
                liquidation_price: None,
                sub_account: None,
                strategy_id: None,
            },
        );
        let reconciler = Reconciler::new(tracker.clone());
        let acct = ReconcilerLogic::account_from_clearinghouse(
            &serde_json::json!({"marginSummary": {"accountValue": "10000"}}),
            Usd::ZERO,
        )
        .unwrap();
        let (corrected, _) =
            reconciler.apply_exchange_truth(&BTreeMap::new(), &BTreeMap::new(), &acct);
        assert_eq!(corrected, 1);
        assert!(tracker.get_position("ETH").is_none());
    }
}
