//! Account state tracker — positions, equity, drawdown, PnL attribution, port
//! of `src/hypeedge/account/tracker.py`.
//!
//! Tracks account balance, positions, and PnL in real time from two sources:
//! exchange `clearinghouseState` polling (authoritative) and local fill
//! processing (for immediate position updates between polls). Risk limits
//! (design doc §8.1) depend on equity/peak_equity → max drawdown, per-coin
//! position → max position %, and per-strategy PnL → max strategy loss %.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::{Decimal, Price, Size, Usd};
use hypeedge_domain::enums::Side;
use hypeedge_domain::models::{AccountState, Fill, Position, SpotBalance};

use crate::risk::checker::AccountView;

/// Tracks account balance, positions, and PnL. Shareable behind an `Arc`.
pub struct AccountTracker {
    inner: Mutex<TrackerState>,
}

struct TrackerState {
    positions: HashMap<String, Position>,
    spot_balances: HashMap<String, SpotBalance>,
    account_state: Option<AccountState>,
    peak_equity: Usd,
    total_fees: Usd,
    total_funding: Usd,
    fill_count: u64,
    last_update_ts: Option<DateTime<Utc>>,
    last_spot_update_ts: Option<DateTime<Utc>>,
    /// Exchange fill ids applied exactly once (dedup set — L-RK2: a Vec grew
    /// without bound as the account traded).
    authoritative_fill_ids: HashSet<String>,
    /// Provisional fill fees keyed by cloid (replaced by authoritative fees).
    provisional_fill_fees: HashMap<String, Usd>,
}

impl Default for AccountTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl AccountTracker {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(TrackerState {
                positions: HashMap::new(),
                spot_balances: HashMap::new(),
                account_state: None,
                peak_equity: Usd::ZERO,
                total_fees: Usd::ZERO,
                total_funding: Usd::ZERO,
                fill_count: 0,
                last_update_ts: None,
                last_spot_update_ts: None,
                authoritative_fill_ids: HashSet::new(),
                provisional_fill_fees: HashMap::new(),
            }),
        }
    }

    // --- Position management from fills ---

    /// Update position tracking after a fill. Maintains per-symbol positions
    /// with a VWAP entry price. Fees are always keyed by cloid (B7) so a later
    /// authoritative fill net-corrects instead of double-counting.
    pub fn update_fill(&self, fill: &Fill, provisional: bool) {
        let mut st = self.inner.lock().unwrap();
        if fill.is_spot {
            record_fill_accounting(&mut st, fill, provisional);
            tracing::debug!(
                symbol = %fill.symbol,
                side = fill.side.as_str(),
                size = fill.size.to_string(),
                price = fill.price.to_string(),
                "tracker_spot_fill_processed"
            );
            return;
        }
        match st.positions.get(&fill.symbol) {
            None => {
                let signed_size = if fill.side == Side::Buy {
                    fill.size.inner()
                } else {
                    -fill.size.inner()
                };
                st.positions.insert(
                    fill.symbol.clone(),
                    Position {
                        symbol: fill.symbol.clone(),
                        size: Size::new(signed_size),
                        entry_price: Some(fill.price),
                        mark_price: Some(fill.price),
                        unrealized_pnl: None,
                        leverage: 0,
                        liquidation_price: None,
                        sub_account: None,
                        strategy_id: None,
                    },
                );
            }
            Some(existing) => {
                let is_buy = fill.side == Side::Buy;
                let old_size = existing.size.inner();
                let new_size = if is_buy {
                    old_size + fill.size.inner()
                } else {
                    old_size - fill.size.inner()
                };
                let pos = st.positions.get_mut(&fill.symbol).unwrap();
                if new_size == Decimal::ZERO {
                    // Position fully closed.
                    st.positions.remove(&fill.symbol);
                    tracing::info!(symbol = %fill.symbol, fill_cloid = %fill.cloid, "position_closed");
                } else if (old_size > Decimal::ZERO && new_size < Decimal::ZERO)
                    || (old_size < Decimal::ZERO && new_size > Decimal::ZERO)
                {
                    // Position flipped (e.g. long → short).
                    pos.size = Size::new(new_size);
                    pos.entry_price = Some(fill.price);
                    tracing::info!(
                        symbol = %fill.symbol,
                        old_size = %old_size,
                        new_size = %new_size,
                        "position_flipped"
                    );
                } else if (old_size > Decimal::ZERO && is_buy)
                    || (old_size < Decimal::ZERO && !is_buy)
                {
                    // Adding in the same direction — update VWAP entry price.
                    let entry = pos.entry_price.unwrap_or(fill.price).inner();
                    let old_notional = old_size.abs() * entry;
                    let new_notional = fill.size.inner() * fill.price.inner();
                    let total_size = new_size.abs();
                    let new_entry = if total_size > Decimal::ZERO {
                        (old_notional + new_notional) / total_size
                    } else {
                        fill.price.inner()
                    };
                    pos.size = Size::new(new_size);
                    pos.entry_price = Some(Price::new(new_entry));
                } else {
                    // Partial reduction keeps the original entry price. Realized
                    // PnL belongs in the ledger; re-weighting here corrupts the
                    // cost basis.
                    pos.size = Size::new(new_size);
                }
                if st.positions.contains_key(&fill.symbol) {
                    let pos = st.positions.get_mut(&fill.symbol).unwrap();
                    pos.mark_price = Some(fill.price);
                }
            }
        }
        record_fill_accounting(&mut st, fill, provisional);
    }

    /// Apply a committed exchange fill exactly once to the live projection.
    /// Returns `false` if the `external_event_id` was already applied or if
    /// the fill is malformed (spot fill with a perp projection, or a perp fill
    /// without a position projection).
    ///
    /// M-RK5: validation happens **before** the external id is recorded, so a
    /// rejected fill does not consume its id — a later corrected event for the
    /// same id can still be applied (previously the id was recorded first and a
    /// malformed fill permanently shadowed the good one via the dedup set).
    pub fn apply_authoritative_fill(
        &self,
        external_event_id: &str,
        fill: &Fill,
        position: Option<&Position>,
    ) -> bool {
        let mut st = self.inner.lock().unwrap();
        if st.authoritative_fill_ids.contains(external_event_id) {
            return false;
        }

        if fill.is_spot {
            if position.is_some() {
                // Mirrors Python's ValueError.
                tracing::error!(cloid = %fill.cloid, "spot fills must not carry a perpetual position projection");
                return false;
            }
        } else {
            let Some(position) = position else {
                // Fail-closed (M-RK5): a perp fill without an authoritative
                // position projection must never be guessed — treating it as a
                // zero-size position would delete the tracked position as if it
                // were a close. The caller passes `None` when the projector
                // could not determine `position_size`.
                tracing::error!(cloid = %fill.cloid, "perpetual fills require a position projection (fail-closed; refusing to apply)");
                return false;
            };
            if position.is_flat() {
                st.positions.remove(&position.symbol);
            } else {
                st.positions
                    .insert(position.symbol.clone(), position.clone());
            }
        }
        // Validation passed — only now consume the external id.
        st.authoritative_fill_ids.insert(external_event_id.to_string());

        // M-RK4: the provisional fee is keyed by cloid and accumulates across
        // multiple partial fills of the same order. Each authoritative fill
        // nets against the *remaining* provisional amount already counted, so
        // `total_fees` converges to the authoritative sum and `fill_count` is
        // not double-counted when several fills share one cloid.
        let authoritative_fee = fill.fee.inner().abs();
        let consumed = st
            .provisional_fill_fees
            .get(&fill.cloid)
            .map(|p| p.inner().min(authoritative_fee))
            .unwrap_or(Decimal::ZERO);
        st.total_fees = Usd::new(st.total_fees.inner() + authoritative_fee - consumed);
        if let Some(provisional) = st.provisional_fill_fees.get_mut(&fill.cloid) {
            let remaining = provisional.inner() - consumed;
            if remaining > Decimal::ZERO {
                *provisional = Usd::new(remaining);
            } else {
                st.provisional_fill_fees.remove(&fill.cloid);
            }
        } else {
            st.fill_count += 1;
        }
        st.last_update_ts = DateTime::from_timestamp_millis(fill.timestamp);
        tracing::debug!(
            external_event_id,
            cloid = %fill.cloid,
            symbol = %fill.symbol,
            is_spot = fill.is_spot,
            "tracker_authoritative_fill_applied"
        );
        true
    }

    // --- Account state from exchange polling ---

    /// Update from exchange clearinghouse state (authoritative). Updates peak
    /// equity for drawdown tracking. The stored state's `peak_equity` is
    /// normalized to the running peak (mirrors Python, where the caller passes
    /// `peak_equity=max(tracker.peak_equity, account_value)`).
    pub fn update_account_state(&self, state: &AccountState) {
        let mut st = self.inner.lock().unwrap();
        if state.equity.inner() > st.peak_equity.inner() {
            st.peak_equity = state.equity;
        }
        let mut stored = state.clone();
        stored.peak_equity = st.peak_equity;
        st.account_state = Some(stored);
        st.last_update_ts = Some(Utc::now());
        tracing::debug!(
            equity = state.equity.to_string(),
            peak_equity = st.peak_equity.to_string(),
            drawdown_pct = st
                .account_state
                .as_ref()
                .map(|s| s.drawdown_pct())
                .unwrap_or(0.0),
            "tracker_account_updated"
        );
    }

    /// Replace local position with exchange-truth (used by the reconciler).
    pub fn update_position_from_exchange(&self, symbol: &str, position: Position) {
        self.inner
            .lock()
            .unwrap()
            .positions
            .insert(symbol.to_string(), position);
    }

    /// Remove a position (used when the reconciler finds it closed on exchange).
    pub fn remove_position(&self, symbol: &str) {
        self.inner.lock().unwrap().positions.remove(symbol);
    }

    /// Atomically replace spot balances from `spotClearinghouseState`.
    pub fn update_spot_balances(&self, balances: &[SpotBalance], observed_at: DateTime<Utc>) {
        let mut st = self.inner.lock().unwrap();
        st.spot_balances = balances
            .iter()
            .filter(|b| !b.total.inner().is_zero() || !b.hold.inner().is_zero())
            .map(|b| (b.token.clone(), b.clone()))
            .collect();
        st.last_spot_update_ts = Some(observed_at);
        st.last_update_ts = Some(
            st.last_update_ts
                .map(|prev| prev.max(observed_at))
                .unwrap_or(observed_at),
        );
    }

    pub fn get_spot_balance(&self, token: &str) -> Option<SpotBalance> {
        self.inner.lock().unwrap().spot_balances.get(token).cloned()
    }

    pub fn get_all_spot_balances(&self) -> Vec<SpotBalance> {
        self.inner
            .lock()
            .unwrap()
            .spot_balances
            .values()
            .cloned()
            .collect()
    }

    // --- Funding tracking ---

    /// Record a funding payment (positive = received, negative = paid).
    pub fn apply_funding(&self, amount: &Usd) {
        let mut st = self.inner.lock().unwrap();
        st.total_funding = Usd::new(st.total_funding.inner() + amount.inner());
    }

    // --- Query methods ---

    pub fn get_position(&self, symbol: &str) -> Option<Position> {
        self.inner.lock().unwrap().positions.get(symbol).cloned()
    }

    pub fn get_all_positions(&self) -> Vec<Position> {
        self.inner
            .lock()
            .unwrap()
            .positions
            .values()
            .cloned()
            .collect()
    }

    pub fn get_account_state(&self) -> Option<AccountState> {
        self.inner.lock().unwrap().account_state.clone()
    }

    pub fn peak_equity(&self) -> Usd {
        self.inner.lock().unwrap().peak_equity
    }

    pub fn current_equity(&self) -> Usd {
        self.inner
            .lock()
            .unwrap()
            .account_state
            .as_ref()
            .map(|s| s.equity)
            .unwrap_or(Usd::ZERO)
    }

    /// Current drawdown from peak equity as a fraction (0.0 = at peak).
    pub fn drawdown_pct(&self) -> f64 {
        let st = self.inner.lock().unwrap();
        st.account_state
            .as_ref()
            .map(|s| s.drawdown_pct())
            .unwrap_or(0.0)
    }

    pub fn total_fees(&self) -> Usd {
        self.inner.lock().unwrap().total_fees
    }

    pub fn total_funding(&self) -> Usd {
        self.inner.lock().unwrap().total_funding
    }

    pub fn fill_count(&self) -> u64 {
        self.inner.lock().unwrap().fill_count
    }

    pub fn last_update_ts(&self) -> Option<DateTime<Utc>> {
        self.inner.lock().unwrap().last_update_ts
    }

    pub fn last_spot_update_ts(&self) -> Option<DateTime<Utc>> {
        self.inner.lock().unwrap().last_spot_update_ts
    }

    /// Notional value of one position.
    pub fn get_position_value(&self, symbol: &str) -> Usd {
        let st = self.inner.lock().unwrap();
        match st.positions.get(symbol) {
            Some(pos) => match pos.mark_price {
                Some(mark) => Usd::new(pos.size.inner().abs() * mark.inner()),
                None => Usd::ZERO,
            },
            None => Usd::ZERO,
        }
    }

    /// Total notional value of all positions.
    pub fn get_total_position_value(&self) -> Usd {
        let st = self.inner.lock().unwrap();
        let mut total = Decimal::ZERO;
        for pos in st.positions.values() {
            if let Some(mark) = pos.mark_price {
                total += pos.size.inner().abs() * mark.inner();
            }
        }
        Usd::new(total)
    }

    /// Current effective leverage = total position value / equity.
    pub fn get_leverage(&self) -> f64 {
        let equity = self.current_equity().inner();
        if equity <= Decimal::ZERO {
            return 0.0;
        }
        let total = self.get_total_position_value().inner();
        total.div(equity).to_string().parse::<f64>().unwrap_or(0.0)
    }

    /// Full tracker status for the `/api/v1/account` route.
    pub fn get_status(&self) -> serde_json::Value {
        // L-RK1: `get_leverage()` takes the inner mutex itself — computing it
        // *before* holding the guard here avoids a nested-lock self-deadlock
        // (the previous code called it while the guard was held).
        let leverage = format!("{:.2}", self.get_leverage());
        let st = self.inner.lock().unwrap();
        let positions: serde_json::Map<String, serde_json::Value> = st
            .positions
            .iter()
            .map(|(sym, pos)| {
                (
                    sym.clone(),
                    serde_json::json!({
                        "size": pos.size.to_string(),
                        "entry_price": pos.entry_price.map(|p| p.to_string()),
                        "mark_price": pos.mark_price.map(|p| p.to_string()),
                    }),
                )
            })
            .collect();
        let spot_balances: serde_json::Map<String, serde_json::Value> = st
            .spot_balances
            .iter()
            .map(|(token, b)| {
                (
                    token.clone(),
                    serde_json::json!({
                        "total": b.total.to_string(),
                        "hold": b.hold.to_string(),
                        "available": b.available().to_string(),
                        "entry_ntl": b.entry_ntl.to_string(),
                    }),
                )
            })
            .collect();
        serde_json::json!({
            "equity": st.account_state.as_ref().map(|s| s.equity.to_string()).unwrap_or_else(|| "0".into()),
            "peak_equity": st.peak_equity.to_string(),
            "drawdown_pct": format!("{:.4}", st.account_state.as_ref().map(|s| s.drawdown_pct()).unwrap_or(0.0)),
            "total_fees": st.total_fees.to_string(),
            "total_funding": st.total_funding.to_string(),
            "fill_count": st.fill_count,
            "position_count": st.positions.len(),
            "spot_balance_count": st.spot_balances.len(),
            "leverage": leverage,
            "positions": positions,
            "spot_balances": spot_balances,
            "last_update": st.last_update_ts.map(|t| t.to_rfc3339()),
        })
    }
}

fn record_fill_accounting(st: &mut TrackerState, fill: &Fill, _provisional: bool) {
    st.total_fees = Usd::new(st.total_fees.inner() + fill.fee.inner().abs());
    st.fill_count += 1;
    // B7: always key the provisional fee by cloid (regardless of the
    // `provisional` flag), so a later authoritative fill for the same cloid
    // does a net correction in `apply_authoritative_fill` instead of adding
    // the fee and fill_count a second time.
    //
    // M-RK4: multiple partial fills of the same order share a cloid, so the
    // provisional amount **accumulates** (entry exists → += fee) instead of
    // overwriting — otherwise the authoritative net-correction would only
    // offset the last partial fill and the earlier fees would be double
    // counted.
    let fee = Usd::new(fill.fee.inner().abs());
    match st.provisional_fill_fees.get_mut(&fill.cloid) {
        Some(accumulated) => *accumulated = Usd::new(accumulated.inner() + fee.inner()),
        None => {
            st.provisional_fill_fees.insert(fill.cloid.clone(), fee);
        }
    }
    st.last_update_ts = Some(Utc::now());
    tracing::debug!(
        symbol = %fill.symbol,
        side = fill.side.as_str(),
        size = fill.size.to_string(),
        price = fill.price.to_string(),
        positions = st.positions.len(),
        "tracker_fill_processed"
    );
}

/// The risk checker facade: account state, per-symbol position, freshness.
impl AccountView for AccountTracker {
    fn get_account_state(&self) -> Option<AccountState> {
        AccountTracker::get_account_state(self)
    }
    fn get_position(&self, symbol: &str) -> Option<Position> {
        AccountTracker::get_position(self, symbol)
    }
    fn last_update_ts(&self) -> Option<DateTime<Utc>> {
        AccountTracker::last_update_ts(self)
    }
}

/// The trend-follow strategy facade (6f wiring).
impl crate::strategy::trend_follow::StrategyAccountView for AccountTracker {
    fn get_position(&self, symbol: &str) -> Option<Position> {
        AccountTracker::get_position(self, symbol)
    }
    fn current_equity(&self) -> f64 {
        AccountTracker::current_equity(self)
            .inner()
            .to_string()
            .parse::<f64>()
            .unwrap_or(0.0)
    }
}

/// The funding-arb account facade (6f wiring): maps the tracker's spot balance
/// to the runtime's `SpotBalanceView` and exposes the account's available
/// balance.
impl crate::funding_arb::runtime::FundingArbAccountView for AccountTracker {
    fn get_position(&self, symbol: &str) -> Option<Position> {
        AccountTracker::get_position(self, symbol)
    }
    fn get_spot_balance(
        &self,
        token: &str,
    ) -> Option<crate::funding_arb::runtime::SpotBalanceView> {
        AccountTracker::get_spot_balance(self, token).map(|b| {
            crate::funding_arb::runtime::SpotBalanceView {
                total: b.total.inner(),
                hold: b.hold.inner(),
            }
        })
    }
    fn get_account_available_balance(&self) -> Option<Decimal> {
        AccountTracker::get_account_state(self).map(|s| s.available_balance.inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::decimal::Decimal as D;

    fn fill(symbol: &str, side: Side, size: &str, price: &str, cloid: &str) -> Fill {
        Fill {
            cloid: cloid.to_string(),
            exchange_oid: "oid".into(),
            symbol: symbol.to_string(),
            side,
            price: Price::new(D::from_str_strict(price).unwrap()),
            size: Size::new(D::from_str_strict(size).unwrap()),
            fee: Usd::new(D::from_str_strict("0.1").unwrap()),
            is_maker: false,
            timestamp: 1_700_000_000_000,
            strategy_id: None,
            sub_account: None,
            is_spot: false,
        }
    }

    #[test]
    fn new_position_and_vwap_averaging() {
        let t = AccountTracker::new();
        t.update_fill(&fill("BTC", Side::Buy, "1.0", "50000", "c1"), false);
        let pos = t.get_position("BTC").unwrap();
        assert_eq!(pos.size.to_string(), "1");
        assert_eq!(pos.entry_price.unwrap().to_string(), "50000");

        // Add 1 more @ 60000 → VWAP 55000.
        t.update_fill(&fill("BTC", Side::Buy, "1.0", "60000", "c2"), false);
        let pos = t.get_position("BTC").unwrap();
        assert_eq!(pos.size.to_string(), "2");
        assert_eq!(pos.entry_price.unwrap().to_string(), "55000");
    }

    #[test]
    fn partial_reduction_keeps_entry_price() {
        let t = AccountTracker::new();
        t.update_fill(&fill("BTC", Side::Buy, "2.0", "50000", "c1"), false);
        t.update_fill(&fill("BTC", Side::Sell, "1.0", "55000", "c2"), false);
        let pos = t.get_position("BTC").unwrap();
        assert_eq!(pos.size.to_string(), "1");
        assert_eq!(
            pos.entry_price.unwrap().to_string(),
            "50000",
            "reduction must not re-weight cost basis"
        );
    }

    #[test]
    fn full_close_removes_position() {
        let t = AccountTracker::new();
        t.update_fill(&fill("BTC", Side::Buy, "1.0", "50000", "c1"), false);
        t.update_fill(&fill("BTC", Side::Sell, "1.0", "51000", "c2"), false);
        assert!(t.get_position("BTC").is_none());
    }

    #[test]
    fn flip_updates_entry() {
        let t = AccountTracker::new();
        t.update_fill(&fill("BTC", Side::Buy, "1.0", "50000", "c1"), false);
        t.update_fill(&fill("BTC", Side::Sell, "2.0", "52000", "c2"), false);
        let pos = t.get_position("BTC").unwrap();
        assert_eq!(pos.size.to_string(), "-1");
        assert_eq!(pos.entry_price.unwrap().to_string(), "52000");
    }

    #[test]
    fn short_position_sizes_are_negative() {
        let t = AccountTracker::new();
        t.update_fill(&fill("BTC", Side::Sell, "1.0", "50000", "c1"), false);
        let pos = t.get_position("BTC").unwrap();
        assert!(pos.is_short());
        assert_eq!(pos.size.to_string(), "-1");
    }

    #[test]
    fn authoritative_fill_does_not_double_count_non_provisional_local() {
        // B7 regression: a local fill recorded with provisional=false must still
        // be keyed by cloid, so the authoritative fill net-corrects the fee and
        // does not add it (and fill_count) a second time.
        let t = AccountTracker::new();
        let mut local = fill("BTC", Side::Buy, "1.0", "50000", "c1");
        local.is_spot = true;
        t.update_fill(&local, false);

        let mut auth = fill("BTC", Side::Buy, "1.0", "50000", "c1");
        auth.is_spot = true;
        let applied = t.apply_authoritative_fill("evt-1", &auth, None);
        assert!(applied);

        assert_eq!(
            t.total_fees().to_string(),
            "0.1",
            "fee must be counted once (B7), not twice"
        );
        assert_eq!(t.fill_count(), 1, "fill_count must be counted once (B7)");
    }

    #[test]
    fn authoritative_fill_applied_exactly_once() {
        let t = AccountTracker::new();
        // Provisional fill records a fee; the authoritative one replaces it.
        let f = fill("BTC", Side::Buy, "1.0", "50000", "c1");
        let pos = Position {
            symbol: "BTC".into(),
            size: Size::new(D::from_str_strict("1.0").unwrap()),
            entry_price: Some(Price::new(D::from_str_strict("50000").unwrap())),
            mark_price: Some(Price::new(D::from_str_strict("50000").unwrap())),
            unrealized_pnl: None,
            leverage: 0,
            liquidation_price: None,
            sub_account: None,
            strategy_id: None,
        };
        assert!(t.apply_authoritative_fill("evt1", &f, Some(&pos)));
        // Idempotent: same external event id is a no-op.
        assert!(!t.apply_authoritative_fill("evt1", &f, Some(&pos)));
        assert_eq!(t.fill_count(), 1);
        assert_eq!(t.get_position("BTC").unwrap().size.to_string(), "1");
    }

    #[test]
    fn account_state_tracks_peak_equity_and_drawdown() {
        let t = AccountTracker::new();
        let state = |equity: &str| AccountState {
            equity: Usd::new(D::from_str_strict(equity).unwrap()),
            available_balance: Usd::ZERO,
            total_margin_used: Usd::ZERO,
            total_unrealized_pnl: Usd::ZERO,
            peak_equity: Usd::ZERO,
            sub_account: None,
        };
        t.update_account_state(&state("10000"));
        assert_eq!(t.peak_equity().to_string(), "10000");
        t.update_account_state(&state("9000"));
        assert_eq!(
            t.peak_equity().to_string(),
            "10000",
            "peak equity never decreases"
        );
        assert!((t.drawdown_pct() - 0.1).abs() < 1e-6);
    }

    #[test]
    fn funding_and_fees_accumulate() {
        let t = AccountTracker::new();
        t.update_fill(&fill("BTC", Side::Buy, "1.0", "50000", "c1"), false);
        assert_eq!(t.total_fees().to_string(), "0.1");
        assert_eq!(t.fill_count(), 1);
        t.apply_funding(&Usd::new(D::from_str_strict("0.05").unwrap()));
        t.apply_funding(&Usd::new(D::from_str_strict("-0.02").unwrap()));
        assert_eq!(t.total_funding().to_string(), "0.03");
    }

    #[test]
    fn spot_balances_replace_atomically() {
        let t = AccountTracker::new();
        let b = |token: &str, total: &str, hold: &str| SpotBalance {
            token: token.to_string(),
            total: Size::new(D::from_str_strict(total).unwrap()),
            hold: Size::new(D::from_str_strict(hold).unwrap()),
            entry_ntl: Usd::ZERO,
            sub_account: None,
            updated_at: Utc::now(),
        };
        let ts = Utc::now();
        t.update_spot_balances(&[b("USDC", "1000", "0"), b("BTC", "1", "0.5")], ts);
        assert_eq!(
            t.get_spot_balance("USDC").unwrap().available().to_string(),
            "1000"
        );
        assert_eq!(
            t.get_spot_balance("BTC").unwrap().available().to_string(),
            "0.5"
        );
        // Zero-balance tokens are filtered out.
        t.update_spot_balances(&[b("USDC", "0", "0")], ts);
        assert!(t.get_spot_balance("USDC").is_none());
    }

    fn pos(symbol: &str, size: &str) -> Position {
        Position {
            symbol: symbol.into(),
            size: Size::new(D::from_str_strict(size).unwrap()),
            entry_price: Some(Price::new(D::from_str_strict("50000").unwrap())),
            mark_price: Some(Price::new(D::from_str_strict("50000").unwrap())),
            unrealized_pnl: None,
            leverage: 0,
            liquidation_price: None,
            sub_account: None,
            strategy_id: None,
        }
    }

    #[test]
    fn same_cloid_partial_fills_accumulate_provisional_fees() {
        // M-RK4 regression: two partial fills of the same order (same cloid)
        // must accumulate the provisional fee, so the authoritative
        // net-correction converges to the true fee total instead of double
        // counting the earlier partial.
        let t = AccountTracker::new();
        let mut p1 = fill("BTC", Side::Buy, "0.5", "50000", "c1");
        p1.is_spot = true;
        let mut p2 = fill("BTC", Side::Buy, "0.5", "50000", "c1");
        p2.is_spot = true;
        t.update_fill(&p1, true);
        t.update_fill(&p2, true);
        assert_eq!(t.total_fees().to_string(), "0.2", "two partials accumulate");
        assert_eq!(t.fill_count(), 2);

        let mut a1 = fill("BTC", Side::Buy, "0.5", "50000", "c1");
        a1.is_spot = true;
        let mut a2 = fill("BTC", Side::Buy, "0.5", "50000", "c1");
        a2.is_spot = true;
        assert!(t.apply_authoritative_fill("evt-a1", &a1, None));
        assert!(t.apply_authoritative_fill("evt-a2", &a2, None));

        assert_eq!(
            t.total_fees().to_string(),
            "0.2",
            "authoritative reconciliation must net to the true fee total (M-RK4)"
        );
        assert_eq!(
            t.fill_count(),
            2,
            "fill_count must not double-count partial fills of one cloid (M-RK4)"
        );
    }

    #[test]
    fn rejected_fill_does_not_consume_external_id() {
        // M-RK5 regression: a malformed fill (spot fill carrying a perp
        // projection) must not consume its external id — the corrected event
        // with the same id must still be applied.
        let t = AccountTracker::new();
        let mut spot = fill("BTC", Side::Buy, "1.0", "50000", "c1");
        spot.is_spot = true;
        // Spot fill with a position projection is invalid.
        assert!(!t.apply_authoritative_fill("evt-1", &spot, Some(&pos("BTC", "1"))));
        // The same id applied correctly must succeed (id not consumed).
        assert!(t.apply_authoritative_fill("evt-1", &spot, None));
        assert_eq!(t.fill_count(), 1);
    }

    #[test]
    fn perp_fill_without_position_projection_is_fail_closed() {
        // M-RK5: a perp fill with no position projection must be rejected
        // (fail-closed) rather than deleting the tracked position as if the
        // position were closed. And the id must not be consumed.
        let t = AccountTracker::new();
        let entry = fill("BTC", Side::Buy, "1.0", "50000", "c1");
        t.update_fill(&entry, false);
        assert_eq!(t.get_position("BTC").unwrap().size.to_string(), "1");

        let mut fill_no_pos = fill("BTC", Side::Sell, "1.0", "51000", "c1");
        fill_no_pos.fee = Usd::new(D::from_str_strict("0.5").unwrap());
        assert!(
            !t.apply_authoritative_fill("evt-close", &fill_no_pos, None),
            "a perp fill without a position projection must be rejected"
        );
        // The position survives — it was NOT treated as a close.
        assert_eq!(t.get_position("BTC").unwrap().size.to_string(), "1");

        // The same id with a valid (flat) projection applies cleanly later.
        assert!(t.apply_authoritative_fill("evt-close", &fill_no_pos, Some(&pos("BTC", "0"))));
        assert!(t.get_position("BTC").is_none(), "real close removes the position");
    }

    #[test]
    fn get_status_does_not_deadlock() {
        // L-RK1 regression: `get_status` used to call `get_leverage()` while
        // holding the inner mutex — a self-deadlock. It must return promptly.
        let t = AccountTracker::new();
        t.update_fill(&fill("BTC", Side::Buy, "1.0", "50000", "c1"), false);
        let status = t.get_status();
        assert_eq!(status["fill_count"], 1);
        assert_eq!(status["position_count"], 1);
        assert!(status["leverage"].as_str().is_some());
    }
}
