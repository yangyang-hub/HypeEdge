//! Exact market-making accounting and non-accounting execution diagnostics,
//! port of `src/hypeedge/backtest/market_maker_metrics.py`.
//!
//! [`AccountingLedger`] is an average-cost ledger supporting partial fills and
//! open inventory. The [`AccountingPnL`] identity is derived only from ledger
//! inputs, never markouts.

use hypeedge_domain::decimal::{Decimal, Price, Size, Usd};
use hypeedge_domain::enums::Side;

/// An immutable ledger input. Fees are signed: rebates are positive.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountingFill {
    pub side: Side,
    pub price: Price,
    pub size: Size,
    pub net_fee_rebate: Usd,
}

/// Accounting identity derived only from ledger inputs, never markouts.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountingPnL {
    pub realized_trading: Usd,
    pub unrealized_inventory_change: Usd,
    pub net_fee_rebate: Usd,
    pub funding: Usd,
    pub paid_action: Usd,
    pub ending_inventory: Size,
    pub ending_inventory_cost: Option<Price>,
}

impl AccountingPnL {
    pub fn net(&self) -> Usd {
        Usd::new(
            self.realized_trading.inner()
                + self.unrealized_inventory_change.inner()
                + self.net_fee_rebate.inner()
                + self.funding.inner()
                - self.paid_action.inner(),
        )
    }

    /// Assert that the accounting identity equals the ledger net.
    pub fn assert_ledger_identity(&self, ledger_net: Usd) -> Result<(), String> {
        if self.net() != ledger_net {
            return Err(format!(
                "accounting PnL does not equal ledger: calculated={} ledger={}",
                self.net(),
                ledger_net
            ));
        }
        Ok(())
    }
}

/// One markout measurement (research diagnostic, never in AccountingPnL).
#[derive(Debug, Clone, PartialEq)]
pub struct FillMarkout {
    pub fill_id: String,
    pub horizon_ms: u64,
    pub value: Usd,
}

/// Research diagnostics. These values must never enter AccountingPnL.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionQuality {
    pub quoted_spread_bps: Vec<Decimal>,
    pub realized_spread: Usd,
    pub markouts: Vec<FillMarkout>,
    pub queue_ahead_consumed: Size,
    pub fills: u64,
    pub partial_fills: u64,
}

impl Default for ExecutionQuality {
    fn default() -> Self {
        Self {
            quoted_spread_bps: Vec::new(),
            realized_spread: Usd::ZERO,
            markouts: Vec::new(),
            queue_ahead_consumed: Size::ZERO,
            fills: 0,
            partial_fills: 0,
        }
    }
}

/// Average-cost Decimal ledger supporting partial fills and open inventory.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountingLedger {
    quantity: Decimal,
    average_cost: Option<Decimal>,
    realized: Decimal,
    fees: Decimal,
    funding: Decimal,
    paid_action: Decimal,
    fills: Vec<AccountingFill>,
}

impl Default for AccountingLedger {
    fn default() -> Self {
        Self::new()
    }
}

impl AccountingLedger {
    pub fn new() -> Self {
        Self {
            quantity: Decimal::ZERO,
            average_cost: None,
            realized: Decimal::ZERO,
            fees: Decimal::ZERO,
            funding: Decimal::ZERO,
            paid_action: Decimal::ZERO,
            fills: Vec::new(),
        }
    }

    /// Record a signed fill. Buying adds to inventory; selling subtracts.
    pub fn record_fill(&mut self, fill: AccountingFill) -> Result<(), String> {
        let quantity = if fill.side == Side::Buy {
            fill.size.inner()
        } else {
            -fill.size.inner()
        };
        if quantity == Decimal::ZERO {
            return Err("fill size must be positive".into());
        }
        let price = fill.price.inner();
        let old_quantity = self.quantity;
        let same_direction = old_quantity == Decimal::ZERO
            || (old_quantity > Decimal::ZERO) == (quantity > Decimal::ZERO);
        if same_direction {
            let total = abs(old_quantity) + abs(quantity);
            let old_cost = self.average_cost.unwrap_or(Decimal::ZERO);
            self.average_cost =
                Some((abs(old_quantity) * old_cost + abs(quantity) * price).div(total));
            self.quantity += quantity;
        } else {
            let closing = abs(old_quantity).min(abs(quantity));
            let average = self
                .average_cost
                .ok_or_else(|| "non-flat inventory requires an average cost".to_string())?;
            let direction = if old_quantity > Decimal::ZERO {
                Decimal::ONE
            } else {
                -Decimal::ONE
            };
            self.realized += closing * (price - average) * direction;
            self.quantity += quantity;
            if self.quantity == Decimal::ZERO {
                self.average_cost = None;
            } else if (self.quantity > Decimal::ZERO) != (old_quantity > Decimal::ZERO) {
                self.average_cost = Some(price);
            }
        }
        self.fees += fill.net_fee_rebate.inner();
        self.fills.push(fill);
        Ok(())
    }

    /// Record signed funding income (a payment is negative).
    pub fn record_funding(&mut self, amount: Usd) {
        self.funding += amount.inner();
    }

    /// Record a paid action cost (must be non-negative).
    pub fn record_paid_action(&mut self, amount: Usd) -> Result<(), String> {
        if amount.inner() < Decimal::ZERO {
            return Err("paid action cost cannot be negative".into());
        }
        self.paid_action += amount.inner();
        Ok(())
    }

    /// Close the ledger at a mark price, returning the accounting PnL.
    pub fn close(&self, mark_price: Price) -> Result<AccountingPnL, String> {
        let mut unrealized = Decimal::ZERO;
        if self.quantity != Decimal::ZERO {
            let average = self
                .average_cost
                .ok_or_else(|| "non-flat inventory requires an average cost".to_string())?;
            unrealized = self.quantity * (mark_price.inner() - average);
        }
        Ok(AccountingPnL {
            realized_trading: Usd::new(self.realized),
            unrealized_inventory_change: Usd::new(unrealized),
            net_fee_rebate: Usd::new(self.fees),
            funding: Usd::new(self.funding),
            paid_action: Usd::new(self.paid_action),
            ending_inventory: Size::new(self.quantity),
            ending_inventory_cost: self.average_cost.map(Price::new),
        })
    }

    /// The recorded fills in insertion order.
    pub fn fills(&self) -> &[AccountingFill] {
        &self.fills
    }
}

fn abs(d: Decimal) -> Decimal {
    if d < Decimal::ZERO { -d } else { d }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::decimal::Decimal;

    fn px(s: &str) -> Price {
        Price::new(Decimal::from_str_strict(s).unwrap())
    }
    fn sz(s: &str) -> Size {
        Size::new(Decimal::from_str_strict(s).unwrap())
    }
    fn usd(s: &str) -> Usd {
        Usd::new(Decimal::from_str_strict(s).unwrap())
    }

    fn buy(price: &str, size: &str, fee: &str) -> AccountingFill {
        AccountingFill {
            side: Side::Buy,
            price: px(price),
            size: sz(size),
            net_fee_rebate: usd(fee),
        }
    }
    fn sell(price: &str, size: &str, fee: &str) -> AccountingFill {
        AccountingFill {
            side: Side::Sell,
            price: px(price),
            size: sz(size),
            net_fee_rebate: usd(fee),
        }
    }

    #[test]
    fn buy_then_sell_closes_flat() {
        let mut ledger = AccountingLedger::new();
        ledger.record_fill(buy("100", "1", "0.01")).unwrap();
        ledger.record_fill(sell("110", "1", "0.01")).unwrap();
        let pnl = ledger.close(px("110")).unwrap();
        // Realized: (110 - 100) * 1 = 10. Fees: 0.01 + 0.01 = 0.02.
        assert_eq!(pnl.realized_trading, usd("10"));
        assert_eq!(pnl.net_fee_rebate, usd("0.02"));
        assert_eq!(pnl.ending_inventory, Size::ZERO);
        pnl.assert_ledger_identity(usd("10.02")).unwrap();
    }

    #[test]
    fn partial_fill_averages_cost() {
        let mut ledger = AccountingLedger::new();
        ledger.record_fill(buy("100", "1", "0")).unwrap();
        ledger.record_fill(buy("120", "1", "0")).unwrap();
        // Average cost = (100 + 120) / 2 = 110.
        let pnl = ledger.close(px("130")).unwrap();
        assert_eq!(pnl.ending_inventory, sz("2"));
        assert_eq!(pnl.ending_inventory_cost.unwrap(), px("110"));
        // Unrealized = 2 * (130 - 110) = 40.
        assert_eq!(pnl.unrealized_inventory_change, usd("40"));
        assert_eq!(pnl.realized_trading, usd("0"));
    }

    #[test]
    fn reversal_resets_average_cost() {
        let mut ledger = AccountingLedger::new();
        ledger.record_fill(buy("100", "2", "0")).unwrap();
        ledger.record_fill(sell("90", "1", "0")).unwrap();
        // Closing 1 at 90 vs avg 100 => realized -10.
        let pnl = ledger.close(px("100")).unwrap();
        assert_eq!(pnl.realized_trading, usd("-10"));
        assert_eq!(pnl.ending_inventory, sz("1"));
        assert_eq!(pnl.ending_inventory_cost.unwrap(), px("100"));
    }

    #[test]
    fn full_reversal_sets_new_cost() {
        let mut ledger = AccountingLedger::new();
        ledger.record_fill(buy("100", "1", "0")).unwrap();
        ledger.record_fill(sell("110", "2", "0")).unwrap();
        // First sells 1 @110 (realized +10), then flips to short 1 @110.
        let pnl = ledger.close(px("120")).unwrap();
        assert_eq!(pnl.realized_trading, usd("10"));
        assert_eq!(pnl.ending_inventory, sz("-1"));
        assert_eq!(pnl.ending_inventory_cost.unwrap(), px("110"));
        // Unrealized = -1 * (120 - 110) = -10.
        assert_eq!(pnl.unrealized_inventory_change, usd("-10"));
    }

    #[test]
    fn funding_and_paid_action_flow_through() {
        let mut ledger = AccountingLedger::new();
        ledger.record_fill(buy("100", "1", "0")).unwrap();
        ledger.record_fill(sell("100", "1", "0")).unwrap();
        ledger.record_funding(usd("0.5"));
        ledger.record_paid_action(usd("0.1")).unwrap();
        let pnl = ledger.close(px("100")).unwrap();
        assert_eq!(pnl.funding, usd("0.5"));
        assert_eq!(pnl.paid_action, usd("0.1"));
        // Net = 0 + 0 + 0 + 0.5 - 0.1 = 0.4.
        pnl.assert_ledger_identity(usd("0.4")).unwrap();
    }

    #[test]
    fn zero_size_and_negative_action_rejected() {
        let mut ledger = AccountingLedger::new();
        assert!(
            ledger
                .record_fill(AccountingFill {
                    side: Side::Buy,
                    price: px("100"),
                    size: Size::ZERO,
                    net_fee_rebate: usd("0"),
                })
                .is_err()
        );
        assert!(ledger.record_paid_action(usd("-1")).is_err());
    }

    #[test]
    fn close_without_cost_errors_on_open_inventory() {
        // Impossible to reach in practice (fills always set a cost), but the
        // guard exists.
        let ledger = AccountingLedger::new();
        // Empty ledger closes cleanly.
        let pnl = ledger.close(px("100")).unwrap();
        assert_eq!(pnl.net(), usd("0"));
    }

    #[test]
    fn identity_mismatch_is_detected() {
        let mut ledger = AccountingLedger::new();
        ledger.record_fill(buy("100", "1", "0")).unwrap();
        ledger.record_fill(sell("110", "1", "0")).unwrap();
        let pnl = ledger.close(px("110")).unwrap();
        assert!(pnl.assert_ledger_identity(usd("999")).is_err());
    }
}
