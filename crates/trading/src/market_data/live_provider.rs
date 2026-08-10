//! Live market data provider, port of
//! `src/hypeedge/market_data/live_provider.py`.
//!
//! Implements the domain [`MarketDataProvider`] facade by combining the WS
//! feed (via the event bus), the shared [`BookManager`], and a REST history
//! client for candle/funding backfill. Latest trade/mid/funding state is
//! maintained by consuming the bus in a background task.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::Decimal;
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::events::{DomainEvent, Event, EventType};
use hypeedge_domain::models::{Candle, FundingRate, L2BookSnapshot, MidPrice, Trade};
use hypeedge_domain::traits::MarketDataProvider;
use hypeedge_infra::event_bus::{BoundedMailbox, EventBus};

/// A subscriber mailbox (shared handle to a bounded event queue).
type Mailbox = Arc<BoundedMailbox<Arc<Event>>>;

use super::book::BookManager;

/// A normalized mid/mark price and its actual observation time.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketPriceSnapshot {
    pub price: f64,
    pub observed_at: DateTime<Utc>,
    pub exchange_ts: Option<i64>,
    pub version: u64,
    pub connection_generation: u32,
}

/// The REST history boundary used for backfill (implemented by the REST
/// client / app wiring).
#[async_trait]
pub trait CandleHistoryClient: Send + Sync {
    async fn backfill_candles(
        &self,
        coin: &str,
        interval: &str,
        start_time: i64,
        end_time: i64,
    ) -> Result<Vec<Candle>, HypeEdgeError>;

    async fn backfill_funding(
        &self,
        coin: &str,
        start_time: i64,
        end_time: i64,
    ) -> Result<Vec<FundingRate>, HypeEdgeError>;
}

/// Concrete [`MarketDataProvider`] backed by live WS events + REST backfill.
pub struct LiveMarketDataProvider {
    event_bus: Arc<EventBus>,
    rest_client: Arc<dyn CandleHistoryClient>,
    book_manager: Arc<tokio::sync::Mutex<BookManager>>,
    max_candles_per_series: usize,

    last_trades: tokio::sync::Mutex<HashMap<String, Trade>>,
    mid_prices: tokio::sync::Mutex<HashMap<String, MidPriceState>>,
    funding: tokio::sync::Mutex<HashMap<String, FundingRate>>,
    candles: tokio::sync::Mutex<HashMap<(String, String), Vec<Candle>>>,
}

/// Per-symbol mid-price bookkeeping (mirrors the Python dicts).
#[derive(Debug, Clone)]
struct MidPriceState {
    price: f64,
    observed_at: DateTime<Utc>,
    version: u64,
    connection_generation: u32,
    exchange_ts: Option<i64>,
}

impl LiveMarketDataProvider {
    pub fn new(
        event_bus: Arc<EventBus>,
        rest_client: Arc<dyn CandleHistoryClient>,
        book_manager: Arc<tokio::sync::Mutex<BookManager>>,
    ) -> Self {
        Self {
            event_bus,
            rest_client,
            book_manager,
            max_candles_per_series: 1_500,
            last_trades: tokio::sync::Mutex::new(HashMap::new()),
            mid_prices: tokio::sync::Mutex::new(HashMap::new()),
            funding: tokio::sync::Mutex::new(HashMap::new()),
            candles: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Subscribe to events and begin tracking market state in a background task.
    pub fn start(self: &Arc<Self>) -> Mailbox {
        let mailbox = self.event_bus.subscribe_many(&[
            EventType::TradeUpdate,
            EventType::MidPriceUpdate,
            EventType::FundingUpdate,
            EventType::CandleUpdate,
            EventType::L2BookUpdate,
        ]);
        let consumer = mailbox.clone();
        let provider = self.clone();
        tokio::spawn(async move {
            provider.consume_events(consumer).await;
        });
        tracing::info!("live_market_data_provider_started");
        mailbox
    }

    /// The consumer loop; run in the background task.
    async fn consume_events(&self, mailbox: Mailbox) {
        while let Some(event) = mailbox.recv().await {
            self.handle_event(&event).await;
        }
    }

    async fn handle_event(&self, event: &Event) {
        match &event.payload {
            // 6c: bridge WS l2Book snapshots into the shared book manager the
            // API and execution engine read.
            DomainEvent::L2BookUpdate(book) => {
                let mut books = self.book_manager.lock().await;
                books.apply_snapshot(book);
            }
            DomainEvent::TradeUpdate(trade) => {
                self.last_trades
                    .lock()
                    .await
                    .insert(trade.symbol.clone(), trade.clone());
            }
            DomainEvent::MidPriceUpdate(mid) => {
                let mut mid_prices = self.mid_prices.lock().await;
                let current = mid_prices.get(&mid.symbol).cloned();
                let version = current.as_ref().map(|c| c.version + 1).unwrap_or(1);
                mid_prices.insert(
                    mid.symbol.clone(),
                    MidPriceState {
                        price: mid.price.to_string().parse::<f64>().unwrap_or(0.0),
                        observed_at: Utc::now(),
                        version,
                        connection_generation: current
                            .as_ref()
                            .map(|c| c.connection_generation)
                            .unwrap_or(0),
                        exchange_ts: Some(mid.timestamp),
                    },
                );
            }
            DomainEvent::FundingUpdate(funding) => {
                let mut funding_map = self.funding.lock().await;
                let current = funding_map.get(&funding.symbol).cloned();
                // Ignore out-of-order or lower-quality snapshots.
                if let Some(current) = &current {
                    if funding.timestamp < current.timestamp {
                        return;
                    }
                    if funding.timestamp == current.timestamp
                        && current.mark_price.inner() > Decimal::ZERO
                        && funding.mark_price.inner() <= Decimal::ZERO
                    {
                        return;
                    }
                }
                funding_map.insert(funding.symbol.clone(), funding.clone());
            }
            DomainEvent::CandleUpdate(candle) => {
                let mut candles = self.candles.lock().await;
                let series = candles
                    .entry((candle.symbol.clone(), candle.interval.clone()))
                    .or_default();
                LiveMarketDataProvider::upsert_candle(series, candle.clone(), self.max_candles_per_series);
            }
            _ => {}
        }
    }

    /// Insert/replace a candle keeping the series time-ordered and bounded.
    fn upsert_candle(series: &mut Vec<Candle>, candle: Candle, max: usize) {
        if let Some(last) = series.last_mut()
            && last.timestamp == candle.timestamp
        {
            *last = candle;
            return;
        }
        if series.is_empty() || series.last().is_some_and(|c| candle.timestamp > c.timestamp) {
            series.push(candle);
        } else {
            let mut by_ts: HashMap<i64, Candle> = series.iter().map(|c| (c.timestamp, c.clone())).collect();
            by_ts.insert(candle.timestamp, candle);
            let mut merged: Vec<Candle> = by_ts.into_values().collect();
            merged.sort_by_key(|c| c.timestamp);
            *series = merged;
        }
        if series.len() > max {
            series.drain(..series.len() - max);
        }
    }

    /// Get the latest order book snapshot (perp or spot share the BookManager).
    pub async fn get_book(&self, symbol: &str) -> Option<L2BookSnapshot> {
        self.book_manager.lock().await.get_snapshot(symbol)
    }

    /// Get the latest mid price; prefers the allMids WS price, falls back to
    /// the book mid.
    pub async fn get_mid_price(&self, symbol: &str) -> Option<f64> {
        let mid_prices = self.mid_prices.lock().await;
        if let Some(state) = mid_prices.get(symbol) {
            return Some(state.price);
        }
        drop(mid_prices);
        self.book_manager.lock().await.get_mid_price(symbol)
    }

    /// Return a normalized mid/mark price and its actual observation time.
    pub async fn get_price_snapshot_full(&self, symbol: &str) -> Option<MarketPriceSnapshot> {
        let mid_prices = self.mid_prices.lock().await;
        if let Some(state) = mid_prices.get(symbol) {
            return Some(MarketPriceSnapshot {
                price: state.price,
                observed_at: state.observed_at,
                exchange_ts: state.exchange_ts,
                version: state.version,
                connection_generation: state.connection_generation,
            });
        }
        drop(mid_prices);
        let book = self.book_manager.lock().await;
        let snapshot = book.get_snapshot(symbol)?;
        let mid = book.get_mid_price(symbol)?;
        Some(MarketPriceSnapshot {
            price: mid,
            observed_at: snapshot.local_ts,
            exchange_ts: Some(snapshot.timestamp),
            version: snapshot.version,
            connection_generation: snapshot.connection_generation,
        })
    }

    pub async fn get_last_trade(&self, symbol: &str) -> Option<Trade> {
        self.last_trades.lock().await.get(symbol).cloned()
    }

    pub async fn get_funding(&self, symbol: &str) -> Option<FundingRate> {
        self.funding.lock().await.get(symbol).cloned()
    }

    pub async fn get_candles(&self, symbol: &str, interval: &str, limit: usize) -> Vec<Candle> {
        let candles = self.candles.lock().await;
        let series = candles.get(&(symbol.to_string(), interval.to_string()));
        let Some(series) = series else { return Vec::new() };
        let start = series.len().saturating_sub(limit);
        series[start..].to_vec()
    }

    /// Warm a candle series once, coalescing concurrent API requests.
    pub async fn ensure_candles(
        &self,
        symbol: &str,
        interval: &str,
        limit: usize,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Candle>, HypeEdgeError> {
        let cached = self.get_candles(symbol, interval, limit).await;
        if cached.len() >= limit {
            return Ok(cached);
        }
        let history = self
            .rest_client
            .backfill_candles(symbol, interval, start_ms, end_ms)
            .await?;
        let mut candles = self.candles.lock().await;
        let series = candles
            .entry((symbol.to_string(), interval.to_string()))
            .or_default();
        for candle in history {
            LiveMarketDataProvider::upsert_candle(series, candle, self.max_candles_per_series);
        }
        Ok(self
            .get_candles(symbol, interval, limit)
            .await)
    }

    pub async fn backfill_candles(
        &self,
        symbol: &str,
        interval: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Candle>, HypeEdgeError> {
        self.rest_client
            .backfill_candles(symbol, interval, start_ms, end_ms)
            .await
    }

    pub async fn backfill_funding(
        &self,
        symbol: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<FundingRate>, HypeEdgeError> {
        self.rest_client
            .backfill_funding(symbol, start_ms, end_ms)
            .await
    }
}

#[async_trait]
impl MarketDataProvider for LiveMarketDataProvider {
    async fn get_price_snapshot(
        &self,
        symbol: &str,
    ) -> Result<Option<MidPrice>, HypeEdgeError> {
        let snapshot = self.get_price_snapshot_full(symbol).await;
        Ok(snapshot.map(|s| MidPrice {
            symbol: symbol.to_string(),
            price: Decimal::from_f64(s.price).unwrap_or_default(),
            timestamp: s.exchange_ts.unwrap_or_else(|| Utc::now().timestamp_millis()),
        }))
    }

    async fn get_best_bid_ask(
        &self,
        symbol: &str,
    ) -> Result<Option<(Decimal, Decimal)>, HypeEdgeError> {
        let book = self.get_book(symbol).await;
        let Some(book) = book else { return Ok(None) };
        let Some(bid) = book.bids.first() else { return Ok(None) };
        let Some(ask) = book.asks.first() else { return Ok(None) };
        Ok(Some((bid.price.inner(), ask.price.inner())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::decimal::{Price, Size};
    use hypeedge_domain::enums::Side;
    use hypeedge_domain::events::EventType;
    use hypeedge_domain::models::L2Level;

    struct StubHistory;

    #[async_trait]
    impl CandleHistoryClient for StubHistory {
        async fn backfill_candles(
            &self,
            _coin: &str,
            _interval: &str,
            _start_time: i64,
            _end_time: i64,
        ) -> Result<Vec<Candle>, HypeEdgeError> {
            Ok(vec![])
        }
        async fn backfill_funding(
            &self,
            _coin: &str,
            _start_time: i64,
            _end_time: i64,
        ) -> Result<Vec<FundingRate>, HypeEdgeError> {
            Ok(vec![])
        }
    }

    fn candle(symbol: &str, interval: &str, ts: i64) -> Candle {
        Candle {
            symbol: symbol.into(),
            interval: interval.into(),
            open: Price::new(Decimal::ONE),
            high: Price::new(Decimal::ONE),
            low: Price::new(Decimal::ONE),
            close: Price::new(Decimal::ONE),
            volume: Size::new(Decimal::ONE),
            timestamp: ts,
        }
    }

    fn provider() -> LiveMarketDataProvider {
        let bus = Arc::new(EventBus::new(1024));
        let history: Arc<dyn CandleHistoryClient> = Arc::new(StubHistory);
        let books = Arc::new(tokio::sync::Mutex::new(BookManager::new(10)));
        LiveMarketDataProvider::new(bus, history, books)
    }

    #[tokio::test]
    async fn upsert_candle_replaces_same_timestamp() {
        let mut series = vec![candle("BTC", "1m", 100)];
        LiveMarketDataProvider::upsert_candle(&mut series, candle("BTC", "1m", 100), 10);
        assert_eq!(series.len(), 1);
    }

    #[tokio::test]
    async fn upsert_candle_appends_and_bounds() {
        let mut series = Vec::new();
        for ts in 0..5 {
            LiveMarketDataProvider::upsert_candle(&mut series, candle("BTC", "1m", ts), 3);
        }
        assert_eq!(series.len(), 3);
        assert_eq!(series[0].timestamp, 2);
    }

    #[tokio::test]
    async fn upsert_candle_sorts_out_of_order() {
        let mut series = Vec::new();
        LiveMarketDataProvider::upsert_candle(&mut series, candle("BTC", "1m", 100), 10);
        LiveMarketDataProvider::upsert_candle(&mut series, candle("BTC", "1m", 50), 10);
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].timestamp, 50);
        assert_eq!(series[1].timestamp, 100);
    }

    #[tokio::test]
    async fn handles_funding_out_of_order_and_lower_quality() {
        let p = provider();
        let ts = Utc::now().timestamp_millis();
        let f1 = FundingRate {
            symbol: "BTC".into(),
            funding_rate: 0.1,
            premium: 0.1,
            mark_price: Price::new(Decimal::from_scaled(50000, 0)),
            open_interest: 0.0,
            timestamp: ts,
        };
        let stale = FundingRate {
            symbol: "BTC".into(),
            funding_rate: 0.1,
            premium: 0.1,
            mark_price: Price::new(Decimal::from_scaled(50000, 0)),
            open_interest: 0.0,
            timestamp: ts - 1,
        };
        p.handle_event(&Event::new(DomainEvent::FundingUpdate(f1.clone())))
            .await;
        p.handle_event(&Event::new(DomainEvent::FundingUpdate(stale.clone())))
            .await;
        // Stale (older timestamp) must be ignored.
        let funding = p.get_funding("BTC").await.unwrap();
        assert_eq!(funding.timestamp, ts);
    }

    #[tokio::test]
    async fn tracks_last_trade_and_mid() {
        let p = provider();
        let trade = Trade {
            symbol: "BTC".into(),
            price: Price::new(Decimal::from_scaled(50000, 0)),
            size: Size::new(Decimal::ONE),
            side: Side::Buy,
            tid: 1,
            timestamp: 100,
            local_ts: Utc::now(),
        };
        p.handle_event(&Event::new(DomainEvent::TradeUpdate(trade.clone())))
            .await;
        assert_eq!(p.get_last_trade("BTC").await.unwrap().tid, 1);

        let mid = MidPrice {
            symbol: "BTC".into(),
            price: Decimal::from_scaled(50010, 0),
            timestamp: 200,
        };
        p.handle_event(&Event::new(DomainEvent::MidPriceUpdate(mid)))
            .await;
        assert_eq!(p.get_mid_price("BTC").await, Some(50010.0));
    }

    #[tokio::test]
    async fn candle_upsert_bounds_series() {
        let mut series = Vec::new();
        for ts in 0..2000 {
            LiveMarketDataProvider::upsert_candle(&mut series, candle("ETH", "5m", ts), 1500);
        }
        assert_eq!(series.len(), 1500);
        assert_eq!(series.first().unwrap().timestamp, 500);
    }

    #[test]
    fn event_types_supported() {
        // Compile-time: these event types exist and are consumed.
        let _ = EventType::TradeUpdate;
        let _ = EventType::MidPriceUpdate;
        let _ = EventType::FundingUpdate;
        let _ = EventType::CandleUpdate;
    }

    #[tokio::test]
    async fn l2_book_update_bridges_into_shared_book() {
        // 6c: an L2BookUpdate on the bus must write the shared BookManager the
        // API and execution engine read.
        let bus = Arc::new(EventBus::new(16));
        let history: Arc<dyn CandleHistoryClient> = Arc::new(StubHistory);
        let books = Arc::new(tokio::sync::Mutex::new(BookManager::new(10)));
        let p = LiveMarketDataProvider::new(bus, history, books.clone());

        let snapshot = L2BookSnapshot {
            symbol: "BTC".into(),
            bids: vec![L2Level {
                price: Price::new(Decimal::from_str_lenient("99").unwrap()),
                size: Size::new(Decimal::from_str_lenient("2").unwrap()),
            }],
            asks: vec![L2Level {
                price: Price::new(Decimal::from_str_lenient("101").unwrap()),
                size: Size::new(Decimal::from_str_lenient("3").unwrap()),
            }],
            timestamp: 1_700_000_000_000,
            local_ts: Utc::now(),
            version: 1,
            connection_generation: 1,
        };
        p.handle_event(&Event::new(DomainEvent::L2BookUpdate(snapshot)))
            .await;

        let shared = books.lock().await.get_snapshot("BTC").expect("book populated");
        assert_eq!(shared.bids.len(), 1);
        assert_eq!(shared.bids[0].price.to_string(), "99");
        assert_eq!(shared.asks[0].price.to_string(), "101");
    }
}
