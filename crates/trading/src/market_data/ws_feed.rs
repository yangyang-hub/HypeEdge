//! Hyperliquid WebSocket market-data feed, port of
//! `src/hypeedge/market_data/ws_feed.py`.
//!
//! Connects to the exchange WS, subscribes to the configured channels, and
//! publishes normalized [`DomainEvent`]s to the event bus. Reconnects with
//! exponential backoff, bumping `connection_generation` on each new socket.
//!
//! [`WebSocketFeed::build_subscriptions`] and [`WebSocketFeed::handle_message`]
//! are pure enough to unit-test without a live socket (canned frames → events).

use std::sync::Arc;

use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::{Decimal, Price, Size};
use hypeedge_domain::enums::Side;
use hypeedge_domain::events::{DomainEvent, Event};
use hypeedge_domain::models::{Candle, FundingRate, MidPrice, Trade};
use hypeedge_infra::event_bus::EventBus;

use super::book::BookManager;

/// Wrap a domain event with a correlation id in an `Arc<Event>`.
fn wrap_corr(payload: DomainEvent, correlation_id: impl Into<String>) -> Arc<Event> {
    Arc::new(Event::new(payload).with_correlation_id(correlation_id))
}

/// The WebSocket market-data feed.
#[allow(dead_code)] // ws_url + reconnect delays used by the live loop in app
pub struct WebSocketFeed {
    ws_url: String,
    coins: Vec<String>,
    spot_coins: Vec<String>,
    channels: Vec<String>,
    candle_intervals: Vec<String>,
    book_manager: BookManager,
    reconnect_delay_min: f64,
    reconnect_delay_max: f64,
    connection_generation: u32,
    message_count: u64,
}

impl WebSocketFeed {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ws_url: String,
        coins: Vec<String>,
        spot_coins: Vec<String>,
        channels: Vec<String>,
        candle_intervals: Vec<String>,
        depth: usize,
        reconnect_delay_min: f64,
        reconnect_delay_max: f64,
    ) -> Self {
        Self {
            ws_url,
            coins,
            spot_coins,
            channels,
            candle_intervals,
            book_manager: BookManager::new(depth),
            reconnect_delay_min,
            reconnect_delay_max,
            connection_generation: 0,
            message_count: 0,
        }
    }

    pub fn book_manager(&self) -> &BookManager {
        &self.book_manager
    }

    pub fn connection_generation(&self) -> u32 {
        self.connection_generation
    }

    /// Build the Hyperliquid subscription payloads (channel schemas differ:
    /// allMids is global, candle needs an interval, the rest are per coin).
    pub fn build_subscriptions(&self) -> Vec<serde_json::Value> {
        let mut subs = Vec::new();
        for channel in &self.channels {
            match channel.as_str() {
                "allMids" => subs.push(serde_json::json!({"type": "allMids"})),
                "candle" => {
                    for coin in &self.coins {
                        for interval in &self.candle_intervals {
                            subs.push(serde_json::json!({"type": "candle", "coin": coin, "interval": interval}));
                        }
                    }
                }
                "l2Book" | "trades" | "activeAssetCtx" => {
                    for coin in &self.coins {
                        subs.push(serde_json::json!({"type": channel, "coin": coin}));
                    }
                    if channel == "l2Book" {
                        for coin in &self.spot_coins {
                            subs.push(serde_json::json!({"type": "l2Book", "coin": coin}));
                        }
                    }
                }
                other => tracing::warn!(channel = other, "ws_subscription_unsupported"),
            }
        }
        subs
    }

    /// Parse and dispatch a WebSocket message. Publishes normalized events to
    /// the bus via `publish_sync`. Testable with canned frames.
    pub fn handle_message(&mut self, bus: &EventBus, raw: &str) -> Result<(), String> {
        let local_ts = Utc::now();
        let data: serde_json::Value =
            serde_json::from_str(raw).map_err(|e| format!("json: {e}"))?;
        self.message_count += 1;

        let channel = data.get("channel").and_then(|c| c.as_str()).unwrap_or("");
        if channel.is_empty() || channel == "subscriptionResponse" {
            return Ok(());
        }
        let payload = data.get("data").cloned().unwrap_or(serde_json::Value::Null);
        match channel {
            "l2Book" => self.handle_l2_book(bus, &payload, local_ts),
            "trades" => self.handle_trades(bus, &payload, local_ts),
            "candle" => self.handle_candle(bus, &payload),
            "allMids" => self.handle_all_mids(bus, &payload, local_ts),
            "activeAssetCtx" => self.handle_active_asset_ctx(bus, &payload, local_ts),
            other => {
                tracing::debug!(channel = other, "ws_unhandled_channel");
                Ok(())
            }
        }
    }

    fn handle_l2_book(
        &mut self,
        bus: &EventBus,
        data: &serde_json::Value,
        local_ts: DateTime<Utc>,
    ) -> Result<(), String> {
        let coin = data
            .get("coin")
            .and_then(|c| c.as_str())
            .unwrap_or("")
            .to_string();
        if coin.is_empty() {
            return Ok(());
        }
        let levels = data
            .get("levels")
            .and_then(|l| l.as_array())
            .cloned()
            .unwrap_or_default();
        let bids = parse_book_levels(levels.first());
        let asks = parse_book_levels(levels.get(1));
        let ts = data.get("time").and_then(|t| t.as_i64()).unwrap_or(0);

        let snapshot = self.book_manager.get_book(&coin).update(
            &bids,
            &asks,
            ts,
            Some(local_ts),
            self.connection_generation,
        );
        let _ = bus.publish_sync(wrap_corr(DomainEvent::L2BookUpdate(snapshot), coin.clone()));
        Ok(())
    }

    fn handle_trades(
        &mut self,
        bus: &EventBus,
        data: &serde_json::Value,
        local_ts: DateTime<Utc>,
    ) -> Result<(), String> {
        let trades = data.as_array().cloned().unwrap_or_else(|| {
            data.get("trades")
                .and_then(|t| t.as_array())
                .cloned()
                .unwrap_or_default()
        });
        for t in trades {
            let coin = t
                .get("coin")
                .and_then(|c| c.as_str())
                .unwrap_or("")
                .to_string();
            if coin.is_empty() {
                continue;
            }
            let side_str = t.get("side").and_then(|s| s.as_str()).unwrap_or("");
            let price = parse_price(t.get("px"));
            let size = parse_size(t.get("sz"));
            let trade = Trade {
                symbol: coin.clone(),
                price,
                size,
                side: if side_str == "B" {
                    Side::Buy
                } else {
                    Side::Sell
                },
                tid: t.get("tid").and_then(|x| x.as_u64()).unwrap_or(0),
                timestamp: t.get("time").and_then(|x| x.as_i64()).unwrap_or(0),
                local_ts,
            };
            let _ = bus.publish_sync(wrap_corr(DomainEvent::TradeUpdate(trade), coin));
        }
        Ok(())
    }

    fn handle_candle(&mut self, bus: &EventBus, data: &serde_json::Value) -> Result<(), String> {
        let coin = data
            .get("s")
            .and_then(|c| c.as_str())
            .unwrap_or("")
            .to_string();
        if coin.is_empty() {
            return Ok(());
        }
        let candle = Candle {
            symbol: coin.clone(),
            interval: data
                .get("i")
                .and_then(|c| c.as_str())
                .unwrap_or("")
                .to_string(),
            open: parse_price(data.get("o")),
            high: parse_price(data.get("h")),
            low: parse_price(data.get("l")),
            close: parse_price(data.get("c")),
            volume: parse_size(data.get("v")),
            timestamp: data.get("t").and_then(|x| x.as_i64()).unwrap_or(0),
        };
        let _ = bus.publish_sync(wrap_corr(DomainEvent::CandleUpdate(candle), coin));
        Ok(())
    }

    fn handle_all_mids(
        &mut self,
        bus: &EventBus,
        data: &serde_json::Value,
        local_ts: DateTime<Utc>,
    ) -> Result<(), String> {
        let mids = data.get("mids").cloned().unwrap_or_else(|| data.clone());
        let Some(map) = mids.as_object() else {
            return Ok(());
        };
        for (coin, price) in map {
            let Ok(price) = Decimal::from_str_lenient(price.as_str().unwrap_or("0")) else {
                tracing::warn!(coin, price = %price, "mid_price_parse_error");
                continue;
            };
            let mid = MidPrice {
                symbol: coin.clone(),
                price,
                timestamp: local_ts.timestamp_millis(),
            };
            let _ = bus.publish_sync(wrap_corr(DomainEvent::MidPriceUpdate(mid), coin.clone()));
        }
        Ok(())
    }

    fn handle_active_asset_ctx(
        &mut self,
        bus: &EventBus,
        data: &serde_json::Value,
        local_ts: DateTime<Utc>,
    ) -> Result<(), String> {
        let coin = data
            .get("coin")
            .and_then(|c| c.as_str())
            .unwrap_or("")
            .to_string();
        if coin.is_empty() {
            return Ok(());
        }
        let ctx = data.get("ctx").cloned().unwrap_or_else(|| data.clone());
        if !ctx.is_object() {
            return Ok(());
        }
        let mark_raw = ctx
            .get("markPx")
            .or_else(|| ctx.get("midPx"))
            .unwrap_or(&serde_json::Value::Null);
        let oi_raw = ctx
            .get("openInterest")
            .or_else(|| ctx.get("openInterestUsd"))
            .unwrap_or(&serde_json::Value::Null);
        let funding_raw = ctx
            .get("funding")
            .or_else(|| ctx.get("fundingRate"))
            .unwrap_or(&serde_json::Value::Null);
        let funding = FundingRate {
            symbol: coin.clone(),
            funding_rate: parse_f64(Some(funding_raw)),
            premium: parse_f64(ctx.get("premium")),
            mark_price: parse_price(Some(mark_raw)),
            open_interest: parse_f64(Some(oi_raw)),
            timestamp: ctx
                .get("time")
                .and_then(|x| x.as_i64())
                .unwrap_or_else(|| local_ts.timestamp_millis()),
        };
        let _ = bus.publish_sync(wrap_corr(DomainEvent::FundingUpdate(funding), coin));
        Ok(())
    }
}

/// Parse HL L2 levels: objects `{"px","sz"}` or two-item sequences.
fn parse_book_levels(levels: Option<&serde_json::Value>) -> Vec<(Price, Size)> {
    let Some(levels) = levels.and_then(|l| l.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for level in levels {
        let (px, sz) = if let Some(obj) = level.as_object() {
            (obj.get("px"), obj.get("sz"))
        } else if let Some(arr) = level.as_array() {
            if arr.len() >= 2 {
                (Some(&arr[0]), Some(&arr[1]))
            } else {
                continue;
            }
        } else {
            continue;
        };
        if let (Some(p), Some(s)) = (px, sz) {
            out.push((parse_price(Some(p)), parse_size(Some(s))))
        }
    }
    out
}

fn parse_price(v: Option<&serde_json::Value>) -> Price {
    Price::new(match v {
        Some(serde_json::Value::String(s)) => Decimal::from_str_lenient(s).unwrap_or(Decimal::ZERO),
        Some(serde_json::Value::Number(n)) => n
            .as_f64()
            .map(|f| Decimal::from_f64(f).unwrap_or(Decimal::ZERO))
            .unwrap_or(Decimal::ZERO),
        _ => Decimal::ZERO,
    })
}

fn parse_size(v: Option<&serde_json::Value>) -> Size {
    Size::new(match v {
        Some(serde_json::Value::String(s)) => Decimal::from_str_lenient(s).unwrap_or(Decimal::ZERO),
        Some(serde_json::Value::Number(n)) => n
            .as_f64()
            .map(|f| Decimal::from_f64(f).unwrap_or(Decimal::ZERO))
            .unwrap_or(Decimal::ZERO),
        _ => Decimal::ZERO,
    })
}

fn parse_f64(v: Option<&serde_json::Value>) -> f64 {
    match v {
        Some(serde_json::Value::String(s)) => s.parse().unwrap_or(0.0),
        Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
        _ => 0.0,
    }
}

/// Run loop placeholder — the live connect/subscribe/reconnect loop is wired
/// in the `app` crate; the pure parts above are what this module owns.
pub struct WsFeedConfig {
    pub url: String,
    pub coins: Vec<String>,
    pub spot_coins: Vec<String>,
    pub channels: Vec<String>,
    pub candle_intervals: Vec<String>,
    pub book_depth: usize,
    pub reconnect_delay_min: f64,
    pub reconnect_delay_max: f64,
}

impl WebSocketFeed {
    /// Build a feed from a config struct (used by `app` wiring).
    pub fn from_config(cfg: WsFeedConfig) -> Self {
        Self::new(
            cfg.url,
            cfg.coins,
            cfg.spot_coins,
            cfg.channels,
            cfg.candle_intervals,
            cfg.book_depth,
            cfg.reconnect_delay_min,
            cfg.reconnect_delay_max,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::events::EventType;

    fn feed() -> WebSocketFeed {
        WebSocketFeed::new(
            "wss://test".into(),
            vec!["BTC".into(), "ETH".into()],
            vec!["@1".into()],
            vec![
                "l2Book".into(),
                "trades".into(),
                "candle".into(),
                "allMids".into(),
                "activeAssetCtx".into(),
            ],
            vec!["1m".into()],
            20,
            1.0,
            30.0,
        )
    }

    #[test]
    fn subscriptions_follow_channel_schemas() {
        let subs = feed().build_subscriptions();
        // allMids global + 2 coins × (l2Book, trades, activeAssetCtx) + spot l2Book + 2 coins × 1m candle.
        assert!(subs.iter().any(|s| s["type"] == "allMids"));
        assert!(
            subs.iter()
                .any(|s| s["type"] == "candle" && s["interval"] == "1m")
        );
        assert!(
            subs.iter()
                .any(|s| s["type"] == "l2Book" && s["coin"] == "@1")
        );
        // 2 perp coins for l2Book + 1 spot = 3 l2Book subs.
        let l2 = subs.iter().filter(|s| s["type"] == "l2Book").count();
        assert_eq!(l2, 3);
    }

    #[test]
    fn l2_book_frame_publishes_snapshot() {
        let bus = EventBus::new(16);
        let mailbox = bus.subscribe(EventType::L2BookUpdate);
        let mut f = feed();
        let frame = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1700000000123,"levels":[[{"px":"100","sz":"2","n":1}],[{"px":"101","sz":"3","n":1}]]}}"#;
        f.handle_message(&bus, frame).unwrap();
        let ev = mailbox.try_recv().expect("book event published");
        match &ev.payload {
            DomainEvent::L2BookUpdate(s) => {
                assert_eq!(s.symbol, "BTC");
                assert_eq!(s.timestamp, 1700000000123);
                assert_eq!(s.bids.len(), 1);
                assert_eq!(s.bids[0].price.to_string(), "100");
                assert_eq!(s.asks[0].size.to_string(), "3");
            }
            _ => panic!("wrong event"),
        }
    }

    #[test]
    fn trade_frame_parses_side() {
        let bus = EventBus::new(16);
        let mailbox = bus.subscribe(EventType::TradeUpdate);
        let mut f = feed();
        let frame = r#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"65000","sz":"0.5","tid":42,"time":1700000000000}]}"#;
        f.handle_message(&bus, frame).unwrap();
        let ev = mailbox.try_recv().unwrap();
        match &ev.payload {
            DomainEvent::TradeUpdate(t) => {
                assert_eq!(t.symbol, "BTC");
                assert_eq!(t.side, Side::Buy);
                assert_eq!(t.tid, 42);
                assert_eq!(t.price.to_string(), "65000");
            }
            _ => panic!("wrong event"),
        }
    }

    #[test]
    fn funding_frame_parses_ctx() {
        let bus = EventBus::new(16);
        let mailbox = bus.subscribe(EventType::FundingUpdate);
        let mut f = feed();
        let frame = r#"{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{"markPx":"65000","premium":"0.0001","funding":"0.00005","openInterest":"1000","time":1700000000000}}}"#;
        f.handle_message(&bus, frame).unwrap();
        let ev = mailbox.try_recv().unwrap();
        match &ev.payload {
            DomainEvent::FundingUpdate(fr) => {
                assert_eq!(fr.symbol, "BTC");
                assert_eq!(fr.funding_rate, 0.00005);
                assert_eq!(fr.mark_price.to_string(), "65000");
            }
            _ => panic!("wrong event"),
        }
    }

    #[test]
    fn two_item_levels_compat() {
        let bus = EventBus::new(16);
        let mailbox = bus.subscribe(EventType::L2BookUpdate);
        let mut f = feed();
        let frame = r#"{"channel":"l2Book","data":{"coin":"ETH","time":1,"levels":[[["100","2"]],[["101","3"]]]}}"#;
        f.handle_message(&bus, frame).unwrap();
        let ev = mailbox.try_recv().unwrap();
        match &ev.payload {
            DomainEvent::L2BookUpdate(s) => {
                assert_eq!(s.symbol, "ETH");
                assert_eq!(s.bids[0].price.to_string(), "100");
            }
            _ => panic!("wrong event"),
        }
    }
}
