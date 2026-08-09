//! In-memory L2 order book manager, port of `src/hypeedge/market_data/book.py`.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::{Decimal, Price, Size};
use hypeedge_domain::models::{L2BookSnapshot, L2Level};

/// Maintains an in-memory L2 order book for a single symbol. Updated from the
/// WebSocket feed; readable by strategies with zero latency.
pub struct OrderBook {
    symbol: String,
    depth: usize,
    bids: Vec<L2Level>,
    asks: Vec<L2Level>,
    last_update_ts: Option<i64>,
    version: u64,
    snapshot: Option<L2BookSnapshot>,
}

impl OrderBook {
    pub fn new(symbol: impl Into<String>, depth: usize) -> Self {
        Self {
            symbol: symbol.into(),
            depth,
            bids: Vec::new(),
            asks: Vec::new(),
            last_update_ts: None,
            version: 0,
            snapshot: None,
        }
    }

    /// Update the book with a full snapshot. `bids`/`asks` are `(price, size)`
    /// pairs best-first. Returns the created snapshot.
    pub fn update(
        &mut self,
        bids: &[(Price, Size)],
        asks: &[(Price, Size)],
        ts: i64,
        received_at: Option<DateTime<Utc>>,
        connection_generation: u32,
    ) -> L2BookSnapshot {
        self.bids = bids
            .iter()
            .take(self.depth)
            .map(|(p, s)| L2Level {
                price: *p,
                size: *s,
            })
            .collect();
        self.asks = asks
            .iter()
            .take(self.depth)
            .map(|(p, s)| L2Level {
                price: *p,
                size: *s,
            })
            .collect();
        self.last_update_ts = Some(ts);
        self.version += 1;
        self.snapshot = Some(L2BookSnapshot {
            symbol: self.symbol.clone(),
            bids: self.bids.clone(),
            asks: self.asks.clone(),
            timestamp: ts,
            local_ts: received_at.unwrap_or_else(Utc::now),
            version: self.version,
            connection_generation,
        });
        self.snapshot.clone().expect("just set")
    }

    pub fn get_snapshot(&self) -> Option<L2BookSnapshot> {
        self.snapshot.clone()
    }

    pub fn best_bid(&self) -> Option<Price> {
        self.bids.first().map(|l| l.price)
    }

    pub fn best_ask(&self) -> Option<Price> {
        self.asks.first().map(|l| l.price)
    }

    pub fn mid_price(&self) -> Option<f64> {
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        Some(
            (bid.inner() + ask.inner())
                .div(Decimal::from_i128(2))
                .to_string()
                .parse()
                .unwrap_or(0.0),
        )
    }

    pub fn spread_bps(&self) -> Option<f64> {
        let mid = self.mid_price()?;
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        if mid <= 0.0 {
            return None;
        }
        let spread =
            (ask.inner() - bid.inner()).div(Decimal::from_f64(mid).unwrap_or(Decimal::ONE));
        Some(spread.to_string().parse::<f64>().unwrap_or(0.0) * 10_000.0)
    }

    pub fn last_update_ts(&self) -> Option<i64> {
        self.last_update_ts
    }
}

/// Manages order books for multiple symbols.
pub struct BookManager {
    depth: usize,
    books: HashMap<String, OrderBook>,
}

impl Default for BookManager {
    fn default() -> Self {
        Self::new(20)
    }
}

impl BookManager {
    pub fn new(depth: usize) -> Self {
        Self {
            depth,
            books: HashMap::new(),
        }
    }

    pub fn get_book(&mut self, symbol: &str) -> &mut OrderBook {
        self.books
            .entry(symbol.to_string())
            .or_insert_with(|| OrderBook::new(symbol, self.depth))
    }

    pub fn get_snapshot(&self, symbol: &str) -> Option<L2BookSnapshot> {
        self.books.get(symbol).and_then(|b| b.get_snapshot())
    }

    /// Apply a normalized snapshot to the shared in-memory book.
    pub fn apply_snapshot(&mut self, snapshot: &L2BookSnapshot) -> L2BookSnapshot {
        let bids = snapshot
            .bids
            .iter()
            .map(|l| (l.price, l.size))
            .collect::<Vec<_>>();
        let asks = snapshot
            .asks
            .iter()
            .map(|l| (l.price, l.size))
            .collect::<Vec<_>>();
        self.get_book(&snapshot.symbol).update(
            &bids,
            &asks,
            snapshot.timestamp,
            Some(snapshot.local_ts),
            snapshot.connection_generation,
        )
    }

    pub fn get_mid_price(&self, symbol: &str) -> Option<f64> {
        self.books.get(symbol).and_then(|b| b.mid_price())
    }

    pub fn symbols(&self) -> Vec<String> {
        self.books.keys().cloned().collect()
    }
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

    #[test]
    fn update_replaces_book_and_versions() {
        let mut book = OrderBook::new("BTC", 20);
        let s1 = book.update(
            &[(px("100"), sz("2"))],
            &[(px("101"), sz("3"))],
            1000,
            None,
            0,
        );
        assert_eq!(s1.version, 1);
        assert_eq!(book.best_bid(), Some(px("100")));
        assert_eq!(book.best_ask(), Some(px("101")));
        assert_eq!(s1.bids.len(), 1);

        let s2 = book.update(
            &[(px("99"), sz("5"))],
            &[(px("102"), sz("4"))],
            1001,
            None,
            1,
        );
        assert_eq!(s2.version, 2);
        assert_eq!(s2.connection_generation, 1);
        assert_eq!(book.mid_price().unwrap(), 100.5);
    }

    #[test]
    fn depth_limits_levels() {
        let mut book = OrderBook::new("ETH", 2);
        let bids = vec![(px("10"), sz("1")), (px("9"), sz("1")), (px("8"), sz("1"))];
        let s = book.update(&bids, &[], 1, None, 0);
        assert_eq!(s.bids.len(), 2, "only the best 2 levels retained");
        assert_eq!(s.bids[0].price, px("10"));
        assert_eq!(s.bids[1].price, px("9"));
    }

    #[test]
    fn book_manager_creates_and_reuses() {
        let mut mgr = BookManager::new(20);
        let snapshot = mgr.apply_snapshot(&L2BookSnapshot {
            symbol: "SOL".into(),
            bids: vec![L2Level {
                price: px("150"),
                size: sz("1"),
            }],
            asks: vec![L2Level {
                price: px("151"),
                size: sz("1"),
            }],
            timestamp: 5,
            local_ts: Utc::now(),
            version: 0,
            connection_generation: 0,
        });
        assert_eq!(
            snapshot.version, 1,
            "manager re-stamps version via the book"
        );
        assert_eq!(mgr.get_mid_price("SOL").unwrap(), 150.5);
        assert_eq!(mgr.symbols(), vec!["SOL".to_string()]);
    }
}
