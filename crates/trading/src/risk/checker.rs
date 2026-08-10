//! Risk checker, port of `src/hypeedge/risk/checker.py`.
//!
//! Sequential checks against an [`AccountView`] facade (the Rust analog of
//! `AccountTracker`): account presence/freshness, max drawdown, max position %
//! and max leverage. Fail-safe: any error or missing data rejects the order.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::Decimal;
use hypeedge_domain::enums::Side;
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::models::{AccountState, OrderIntent, Position, RiskCheckResult, RiskLimits};
use tokio::time::timeout;

/// The account data the risk checker reads (the Rust analog of `AccountTracker`).
pub trait AccountView: Send + Sync {
    fn get_account_state(&self) -> Option<AccountState>;
    fn get_position(&self, symbol: &str) -> Option<Position>;
    /// Local time of the last account-state update, for the freshness check.
    fn last_update_ts(&self) -> Option<DateTime<Utc>>;
}

/// The risk checker with the fail-safe timeout wrapper. Shareable behind an
/// `Arc` — counters are atomic so `check` takes `&self` (the execution engine
/// calls it on every placement).
pub struct RiskChecker {
    tracker: Arc<dyn AccountView>,
    limits: RiskLimits,
    check_count: AtomicU64,
    reject_count: AtomicU64,
}

impl RiskChecker {
    pub fn new(tracker: Arc<dyn AccountView>, limits: RiskLimits) -> Self {
        Self {
            tracker,
            limits,
            check_count: AtomicU64::new(0),
            reject_count: AtomicU64::new(0),
        }
    }

    pub fn check_count(&self) -> u64 {
        self.check_count.load(AtomicOrdering::Relaxed)
    }
    pub fn reject_count(&self) -> u64 {
        self.reject_count.load(AtomicOrdering::Relaxed)
    }

    /// Run the risk check with a fail-safe timeout: timeout or error = reject.
    pub async fn check(
        &self,
        intent: &OrderIntent,
        reference_price: Option<Decimal>,
    ) -> RiskCheckResult {
        self.check_count.fetch_add(1, AtomicOrdering::Relaxed);
        let timeout_ms = self.limits.timeout_ms.max(1);
        match timeout(
            Duration::from_millis(timeout_ms),
            self.run_checks(intent, reference_price),
        )
        .await
        {
            Ok(Ok(result)) => {
                if !result.passed {
                    self.reject_count.fetch_add(1, AtomicOrdering::Relaxed);
                }
                result
            }
            Ok(Err(e)) => {
                self.reject_count.fetch_add(1, AtomicOrdering::Relaxed);
                tracing::error!(error = %e, "risk_check_error");
                RiskCheckResult {
                    passed: false,
                    reason: Some(format!("risk_check_error: {e}")),
                    checked_limits: vec![],
                }
            }
            Err(_) => {
                self.reject_count.fetch_add(1, AtomicOrdering::Relaxed);
                tracing::error!("risk_check_timeout");
                RiskCheckResult {
                    passed: false,
                    reason: Some("risk_check_timeout".into()),
                    checked_limits: vec![],
                }
            }
        }
    }

    async fn run_checks(
        &self,
        intent: &OrderIntent,
        reference_price: Option<Decimal>,
    ) -> Result<RiskCheckResult, HypeEdgeError> {
        let mut checked: Vec<String> = Vec::new();

        // Check 0: account state must exist.
        let account = self.tracker.get_account_state();
        let Some(account) = account else {
            checked.push("account_state_missing".into());
            return Ok(fail("account_state_not_available", checked));
        };
        checked.push("account_state_available".into());

        // Freshness.
        checked.push("account_state_fresh".into());
        let last_update = self.tracker.last_update_ts();
        let stale = match last_update {
            None => true,
            Some(ts) => (Utc::now() - ts).num_seconds() as f64 > self.limits.account_stale_seconds,
        };
        if stale {
            return Ok(fail("account_state_stale", checked));
        }

        // Effective reference price. The market reference (passed by the engine
        // from the live provider) takes precedence over the order's own limit
        // price (A16): a marketable sell at a far-below-market limit must not
        // have its notional computed from that limit — the order controls the
        // limit, so the order would control the number risk checks.
        let existing_pos = self.tracker.get_position(&intent.symbol);
        let effective_reference_price = reference_price
            .or(intent.price.map(|p| p.inner()))
            .or_else(|| {
                existing_pos
                    .as_ref()
                    .and_then(|p| p.mark_price)
                    .map(|p| p.inner())
            })
            .unwrap_or(Decimal::ZERO);
        if effective_reference_price <= Decimal::ZERO {
            return Ok(fail("market_price_not_available", checked));
        }

        let existing_size = existing_pos
            .map(|p| p.size.inner())
            .unwrap_or(Decimal::ZERO);
        let signed_delta = if intent.side == Side::Buy {
            intent.size.inner()
        } else {
            -intent.size.inner()
        };
        let resulting_size = existing_size + signed_delta;

        // Spot path.
        if intent.is_spot {
            if intent.reduce_only {
                return Ok(fail("spot_reduce_only_invalid", checked));
            }
            checked.push("spot_balance_validated_upstream".into());
            if intent.risk_reducing {
                if intent.side != Side::Sell {
                    return Ok(fail("invalid_risk_reducing_order", checked));
                }
                checked.push("risk_reducing_exit".into());
                return Ok(pass(checked));
            }
            checked.push("max_drawdown".into());
            if account.drawdown_pct() >= self.limits.max_drawdown_pct {
                return Ok(fail(
                    &format!(
                        "drawdown_exceeded: {:.4} >= {}",
                        account.drawdown_pct(),
                        self.limits.max_drawdown_pct
                    ),
                    checked,
                ));
            }
            // A17: zero/negative equity is not "no constraint" — reject.
            if account.equity.inner() <= Decimal::ZERO {
                return Ok(fail("account_equity_non_positive", checked));
            }
            // Spot buys bounded by the per-strategy notional fraction.
            if intent.side == Side::Buy {
                let notional = intent.size.inner() * effective_reference_price;
                let max_notional = account.equity.inner()
                    * Decimal::from_f64(self.limits.max_position_pct).unwrap_or(Decimal::ZERO);
                checked.push("max_position_pct".into());
                if notional > max_notional {
                    return Ok(fail("position_limit_exceeded", checked));
                }
            }
            return Ok(pass(checked));
        }

        // Perp path.
        checked.push("max_drawdown".into());
        if account.drawdown_pct() >= self.limits.max_drawdown_pct {
            return Ok(fail(
                &format!(
                    "drawdown_exceeded: {:.4} >= {}",
                    account.drawdown_pct(),
                    self.limits.max_drawdown_pct
                ),
                checked,
            ));
        }

        checked.push("max_leverage".into());
        let resulting_notional = resulting_size.abs() * effective_reference_price;
        let equity = account.equity.inner();
        if equity <= Decimal::ZERO {
            return Ok(fail("account_equity_non_positive", checked));
        }
        if resulting_notional.div(equity) > Decimal::from_i128(self.limits.max_leverage as i128) {
            return Ok(fail("leverage_exceeded", checked));
        }

        checked.push("max_position_pct".into());
        let max_notional =
            equity * Decimal::from_f64(self.limits.max_position_pct).unwrap_or(Decimal::ZERO);
        if resulting_notional > max_notional {
            return Ok(fail("position_limit_exceeded", checked));
        }

        Ok(pass(checked))
    }
}

fn fail(reason: &str, checked: Vec<String>) -> RiskCheckResult {
    RiskCheckResult {
        passed: false,
        reason: Some(reason.to_string()),
        checked_limits: checked,
    }
}

fn pass(checked: Vec<String>) -> RiskCheckResult {
    RiskCheckResult {
        passed: true,
        reason: None,
        checked_limits: checked,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::decimal::Size;

    struct FakeAccount {
        state: Option<AccountState>,
        position: Option<Position>,
        updated_at: DateTime<Utc>,
    }

    impl AccountView for FakeAccount {
        fn get_account_state(&self) -> Option<AccountState> {
            self.state.clone()
        }
        fn get_position(&self, _symbol: &str) -> Option<Position> {
            self.position.clone()
        }
        fn last_update_ts(&self) -> Option<DateTime<Utc>> {
            Some(self.updated_at)
        }
    }

    fn eq(equity: &str) -> UsdProxy {
        UsdProxy(Decimal::from_str_strict(equity).unwrap())
    }
    struct UsdProxy(Decimal);
    use hypeedge_domain::Usd;

    fn make_tracker(equity: &str, pos_size: Option<&str>) -> Arc<dyn AccountView> {
        Arc::new(FakeAccount {
            state: Some(AccountState {
                equity: Usd::new(eq(equity).0),
                available_balance: Usd::new(eq(equity).0),
                total_margin_used: Usd::ZERO,
                total_unrealized_pnl: Usd::ZERO,
                peak_equity: Usd::new(eq(equity).0),
                sub_account: None,
            }),
            position: pos_size.map(|s| Position {
                symbol: "BTC".into(),
                size: Size::new(Decimal::from_str_strict(s).unwrap()),
                entry_price: None,
                mark_price: Some(hypeedge_domain::Price::new(
                    Decimal::from_str_strict("100").unwrap(),
                )),
                unrealized_pnl: None,
                leverage: 5,
                liquidation_price: None,
                sub_account: None,
                strategy_id: None,
            }),
            updated_at: Utc::now(),
        })
    }

    fn intent(size: &str, side: Side) -> OrderIntent {
        OrderIntent {
            symbol: "BTC".into(),
            side,
            size: Size::new(Decimal::from_str_strict(size).unwrap()),
            price: Some(hypeedge_domain::Price::new(
                Decimal::from_str_strict("100").unwrap(),
            )),
            order_type: hypeedge_domain::enums::OrderType::Limit,
            time_in_force: hypeedge_domain::enums::TimeInForce::Gtc,
            strategy_id: None,
            sub_account: None,
            reduce_only: false,
            cloid: None,
            client_id: None,
            is_spot: false,
            risk_reducing: false,
            max_slippage_bps: 50,
        }
    }

    #[tokio::test]
    async fn passes_within_limits() {
        let checker = RiskChecker::new(make_tracker("10000", None), RiskLimits::default());
        let r = checker.check(&intent("1", Side::Buy), None).await;
        assert!(
            r.passed,
            "1 BTC @ 100 = 100 notional, well within limits: {r:?}"
        );
    }

    #[tokio::test]
    async fn rejects_position_over_limit() {
        // equity 1000, max_position_pct 0.20 -> ceiling 200. 5 BTC @ 100 = 500.
        let checker = RiskChecker::new(make_tracker("1000", None), RiskLimits::default());
        let r = checker.check(&intent("5", Side::Buy), None).await;
        assert!(!r.passed);
        assert_eq!(r.reason.as_deref(), Some("position_limit_exceeded"));
    }

    #[tokio::test]
    async fn rejects_missing_account() {
        let tracker = Arc::new(FakeAccount {
            state: None,
            position: None,
            updated_at: Utc::now(),
        });
        let checker = RiskChecker::new(tracker, RiskLimits::default());
        let r = checker.check(&intent("1", Side::Buy), None).await;
        assert!(!r.passed);
        assert_eq!(r.reason.as_deref(), Some("account_state_not_available"));
    }

    #[tokio::test]
    async fn rejects_stale_account() {
        let tracker = Arc::new(FakeAccount {
            state: Some(AccountState {
                equity: Usd::new(Decimal::from_str_strict("10000").unwrap()),
                available_balance: Usd::ZERO,
                total_margin_used: Usd::ZERO,
                total_unrealized_pnl: Usd::ZERO,
                peak_equity: Usd::new(Decimal::from_str_strict("10000").unwrap()),
                sub_account: None,
            }),
            position: None,
            updated_at: Utc::now() - chrono::Duration::hours(1),
        });
        let checker = RiskChecker::new(tracker, RiskLimits::default());
        let r = checker.check(&intent("1", Side::Buy), None).await;
        assert!(!r.passed);
        assert_eq!(r.reason.as_deref(), Some("account_state_stale"));
    }

    #[tokio::test]
    async fn sell_limit_far_below_market_uses_market_notional() {
        // A16 regression: a marketable sell at a far-below-market limit must
        // have its notional computed from the market reference, not the limit
        // the order controls. equity 10000, max_position_pct 0.20 -> ceiling
        // 2000. 50 BTC @ market 100 = 5000 -> reject; @ limit 40 = 2000 would pass.
        let checker = RiskChecker::new(make_tracker("10000", None), RiskLimits::default());
        let mut it = intent("50", Side::Sell);
        it.price = Some(hypeedge_domain::Price::new(
            Decimal::from_str_strict("40").unwrap(),
        ));
        let r = checker
            .check(&it, Some(Decimal::from_str_strict("100").unwrap()))
            .await;
        assert!(!r.passed);
        assert_eq!(r.reason.as_deref(), Some("position_limit_exceeded"));
    }

    #[tokio::test]
    async fn zero_equity_is_fail_closed() {
        // A17 regression: equity == 0 must reject, not skip the leverage and
        // position gates (fail-open).
        let checker = RiskChecker::new(make_tracker("0", None), RiskLimits::default());
        let r = checker.check(&intent("1", Side::Buy), None).await;
        assert!(!r.passed);
        assert_eq!(r.reason.as_deref(), Some("account_equity_non_positive"));
    }

    #[tokio::test]
    async fn negative_equity_is_fail_closed() {
        let checker = RiskChecker::new(make_tracker("-100", None), RiskLimits::default());
        let r = checker.check(&intent("1", Side::Buy), None).await;
        assert!(!r.passed);
        assert_eq!(r.reason.as_deref(), Some("account_equity_non_positive"));
    }
}
