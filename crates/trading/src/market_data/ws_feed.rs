//! Hyperliquid WebSocket market-data feed, port of
//! `src/hypeedge/market_data/ws_feed.py`.
//!
//! Connects to the exchange WS, subscribes to the configured channels, and
//! publishes normalized [`DomainEvent`]s to the event bus. Reconnects with
//! exponential backoff, bumping `connection_generation` on each new socket.
//!
//! Robustness notes (fix plan P5-9):
//! - H-MD1: the reconnect backoff only resets to the minimum after a
//!   connection that survived [`STABLE_CONNECTION_SECONDS`]; "connect-then-
//!   drop" cycles keep accumulating the exponential backoff.
//! - M-MD3: `subscriptionResponse` failures are recorded and retried with a
//!   rate-limited resubscribe loop; `channel == "error"` frames log at warn.
//! - M-MD4: intra-connection trade `tid` / candle-timestamp gaps are detected
//!   and warned (never blocking the read loop); a reconnect warns that data
//!   may be incomplete.
//! - M-MD5: zero/negative price or size levels are dropped before they enter
//!   the book.
//! - M-MD6: `allMids` frames only publish [`MidPriceUpdate`] for configured
//!   coins (the subscription list), never the full exchange universe.
//!
//! [`WebSocketFeed::build_subscriptions`] and [`WebSocketFeed::handle_message`]
//! are pure enough to unit-test without a live socket (canned frames → events).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::{Decimal, Price, Size};
use hypeedge_domain::enums::Side;
use hypeedge_domain::events::{DomainEvent, Event};
use hypeedge_domain::models::{Candle, FundingRate, MidPrice, Trade};
use hypeedge_infra::event_bus::EventBus;

use super::book::BookManager;
use super::interval_to_ms;

/// A connection must survive this long before the reconnect backoff is reset
/// to its minimum (H-MD1). Shorter-lived connections keep doubling.
const STABLE_CONNECTION_SECONDS: f64 = 60.0;
/// Read timeout: if no frame arrives for this long the socket is considered
/// dead and a reconnect is forced (L-MD).
const READ_TIMEOUT_SECONDS: u64 = 30;
/// Minimum gap between subscription retries after a rejection (M-MD3).
const RESUBSCRIBE_INTERVAL: Duration = Duration::from_secs(5);
/// Max resubscribe attempts per subscription before giving up (M-MD3).
const RESUBSCRIBE_MAX_ATTEMPTS: u32 = 10;

/// Wrap a domain event with a correlation id in an `Arc<Event>`.
fn wrap_corr(payload: DomainEvent, correlation_id: impl Into<String>) -> Arc<Event> {
    Arc::new(Event::new(payload).with_correlation_id(correlation_id))
}

/// The WebSocket market-data feed.
pub struct WebSocketFeed {
    ws_url: String,
    coins: Vec<String>,
    spot_coins: Vec<String>,
    channels: Vec<String>,
    candle_intervals: Vec<String>,
    book_manager: BookManager,
    reconnect_delay_min: f64,
    reconnect_delay_max: f64,
    stable_alive_seconds: f64,
    connection_generation: u32,
    /// Coins eligible for `allMids` publishing (M-MD6): configured perps +
    /// spot coins, i.e. exactly the subscription list.
    subscribed_coins: HashSet<String>,
    /// Per-symbol last trade id for gap detection (M-MD4).
    last_tid: HashMap<String, u64>,
    /// Per-(symbol, interval) last candle timestamp for gap detection (M-MD4).
    last_candle_ts: HashMap<(String, String), i64>,
    /// Subscriptions rejected by the exchange, awaiting retry (M-MD3).
    failed_subscriptions: Vec<serde_json::Value>,
    resubscribe_attempts: HashMap<String, u32>,
    last_resubscribe_at: Option<Instant>,
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
        let subscribed_coins = coins.iter().chain(spot_coins.iter()).cloned().collect();
        Self {
            ws_url,
            coins,
            spot_coins,
            channels,
            candle_intervals,
            book_manager: BookManager::new(depth),
            reconnect_delay_min,
            reconnect_delay_max,
            stable_alive_seconds: STABLE_CONNECTION_SECONDS,
            connection_generation: 0,
            subscribed_coins,
            last_tid: HashMap::new(),
            last_candle_ts: HashMap::new(),
            failed_subscriptions: Vec::new(),
            resubscribe_attempts: HashMap::new(),
            last_resubscribe_at: None,
        }
    }

    /// Override the stable-alive threshold for backoff reset (H-MD1). Defaults
    /// to [`STABLE_CONNECTION_SECONDS`].
    pub fn with_stable_alive_seconds(mut self, seconds: f64) -> Self {
        self.stable_alive_seconds = seconds.max(0.0);
        self
    }

    pub fn book_manager(&self) -> &BookManager {
        &self.book_manager
    }

    pub fn connection_generation(&self) -> u32 {
        self.connection_generation
    }

    /// Live connect/subscribe/reconnect loop (6c): opens the WS, subscribes,
    /// forwards frames to [`WebSocketFeed::handle_message`], and reconnects
    /// with exponential backoff. Runs until the task is dropped. The feed is
    /// shared behind a mutex because `handle_message` is `&mut self`.
    pub async fn run(self: Arc<WebSocketFeed>, bus: Arc<EventBus>) {
        match Arc::try_unwrap(self) {
            Ok(feed) => run_feed_loop(std::sync::Mutex::new(feed), bus).await,
            Err(_) => {
                tracing::error!("ws_feed_run_called_with_shared_arc");
            }
        }
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

        let channel = data.get("channel").and_then(|c| c.as_str()).unwrap_or("");
        // Exchange-side error frames (M-MD3): surface at warn, not debug.
        if channel == "error" {
            let detail = data.get("data").map(|d| d.to_string()).unwrap_or_default();
            tracing::warn!(error = %detail, "ws_channel_error");
            return Ok(());
        }
        // Subscription acks carry a `type` field instead of a `channel`
        // (M-MD3): verify `success`, record failures for retry.
        if data.get("type").and_then(|t| t.as_str()) == Some("subscriptionResponse") {
            self.handle_subscription_response(&data);
            return Ok(());
        }
        if channel.is_empty() {
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

    /// Handle a `subscriptionResponse` frame (M-MD3). A successful ack clears
    /// any pending retry state; a failed ack (`success == false`) records the
    /// subscription so the read loop retries it with a rate limit.
    fn handle_subscription_response(&mut self, data: &serde_json::Value) {
        let subscription = data
            .get("response")
            .and_then(|r| r.get("subscription"))
            .cloned();
        let success = data
            .get("success")
            .and_then(|s| s.as_bool())
            .unwrap_or(true);
        if success {
            if let Some(sub) = &subscription {
                self.failed_subscriptions.retain(|s| s != sub);
                self.resubscribe_attempts.remove(&sub.to_string());
            }
            tracing::debug!(
                subscription = %subscription.map(|s| s.to_string()).unwrap_or_default(),
                "ws_subscription_confirmed"
            );
            return;
        }
        let error = data
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("unknown");
        tracing::error!(
            error,
            subscription = %subscription.as_ref().map(|s| s.to_string()).unwrap_or_default(),
            "ws_subscription_failed"
        );
        if let Some(sub) = subscription
            && !self.failed_subscriptions.contains(&sub)
        {
            self.failed_subscriptions.push(sub);
        }
    }

    /// Subscriptions rejected by the exchange that are eligible for retry now
    /// (M-MD3). Rate-limited by [`RESUBSCRIBE_INTERVAL`]; gives up per
    /// subscription after [`RESUBSCRIBE_MAX_ATTEMPTS`].
    pub fn drain_resubscribe_queue(&mut self) -> Vec<serde_json::Value> {
        if self.failed_subscriptions.is_empty() {
            return Vec::new();
        }
        let now = Instant::now();
        if self
            .last_resubscribe_at
            .is_some_and(|t| now.duration_since(t) < RESUBSCRIBE_INTERVAL)
        {
            return Vec::new();
        }
        self.last_resubscribe_at = Some(now);
        let mut to_send = Vec::new();
        let mut gave_up: Vec<serde_json::Value> = Vec::new();
        for sub in &self.failed_subscriptions {
            let key = sub.to_string();
            let attempts = self.resubscribe_attempts.entry(key).or_insert(0);
            *attempts += 1;
            if *attempts <= RESUBSCRIBE_MAX_ATTEMPTS {
                to_send.push(sub.clone());
            } else {
                gave_up.push(sub.clone());
            }
        }
        for sub in &gave_up {
            tracing::error!(subscription = %sub, "ws_subscription_gave_up_after_retries");
        }
        self.failed_subscriptions.retain(|s| !gave_up.contains(s));
        to_send
    }

    /// Called when a new socket is established (M-MD4): marks that data may be
    /// incomplete and resets the intra-connection gap baselines.
    fn on_reconnect(&mut self, generation: u32) {
        self.connection_generation = generation;
        let had_data = !self.last_tid.is_empty() || !self.last_candle_ts.is_empty();
        if had_data {
            tracing::warn!(generation, "ws_feed_reconnected_data_may_be_gapped");
        }
        self.last_tid.clear();
        self.last_candle_ts.clear();
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
            let tid = t.get("tid").and_then(|x| x.as_u64()).unwrap_or(0);
            // Intra-connection tid continuity check (M-MD4): a jump > 1 means
            // frames were dropped. Conservative: record + warn, never block.
            if tid > 0 {
                let prev = self.last_tid.get(&coin).copied().unwrap_or(0);
                if prev > 0 {
                    if tid > prev + 1 {
                        tracing::warn!(
                            coin = %coin,
                            prev_tid = prev,
                            tid,
                            missing = tid - prev - 1,
                            "ws_trades_gap_detected"
                        );
                    } else if tid < prev {
                        tracing::debug!(coin = %coin, prev_tid = prev, tid, "ws_trades_out_of_order");
                    }
                }
                self.last_tid.insert(coin.clone(), tid.max(prev));
            }
            let trade = Trade {
                symbol: coin.clone(),
                price,
                size,
                side: if side_str == "B" {
                    Side::Buy
                } else {
                    Side::Sell
                },
                tid,
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
        let interval = data
            .get("i")
            .and_then(|c| c.as_str())
            .unwrap_or("")
            .to_string();
        let timestamp = data.get("t").and_then(|x| x.as_i64()).unwrap_or(0);
        // Intra-connection candle continuity check (M-MD4): a timestamp jump
        // larger than the candle interval means bars were missed.
        if timestamp > 0 && !interval.is_empty() {
            let key = (coin.clone(), interval.clone());
            let prev = self.last_candle_ts.get(&key).copied();
            let missed_bar = prev.is_some_and(|prev| {
                timestamp > prev
                    && interval_to_ms(&interval)
                        .is_ok_and(|interval_ms| timestamp - prev > interval_ms)
            });
            if missed_bar {
                tracing::warn!(
                    coin = %coin,
                    interval = %interval,
                    prev_ts = prev.unwrap_or(0),
                    ts = timestamp,
                    "ws_candle_gap_detected"
                );
            }
            self.last_candle_ts.insert(key, timestamp);
        }
        let candle = Candle {
            symbol: coin.clone(),
            interval,
            open: parse_price(data.get("o")),
            high: parse_price(data.get("h")),
            low: parse_price(data.get("l")),
            close: parse_price(data.get("c")),
            volume: parse_size(data.get("v")),
            timestamp,
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
        // M-MD6: publish only the configured coins (the subscription list),
        // never the full exchange universe.
        let subscribed = &self.subscribed_coins;
        for (coin, price) in map {
            if !subscribed.contains(coin) {
                tracing::debug!(coin, "ws_all_mids_skip_unsubscribed");
                continue;
            }
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

/// Parse HL L2 levels: objects `{"px","sz"}` or two-item sequences. Levels
/// with zero/negative price or size are dropped (M-MD5) so they never pollute
/// the in-memory book.
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
            let price = parse_price(Some(p));
            let size = parse_size(Some(s));
            if price.inner() > Decimal::ZERO && size.inner() > Decimal::ZERO {
                out.push((price, size));
            }
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

/// Configuration for the live feed. The connect/subscribe/read/reconnect loop
/// runs in the app wiring (`run_feed_loop`); this module owns the pure
/// parse/publish parts that feed the shared [`BookManager`] and event bus.
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

/// Exponential reconnect backoff (H-MD1): doubles on connect failures and on
/// short-lived ("connect-then-drop") connections; resets to the minimum only
/// after a connection that survived at least `stable_alive_seconds`. The
/// doubling is capped at `max`, preserving the configured
/// `reconnect_delay_min` / `reconnect_delay_max` semantics.
#[derive(Debug, Clone)]
struct ReconnectBackoff {
    current: f64,
    min: f64,
    max: f64,
    stable_alive_seconds: f64,
}

impl ReconnectBackoff {
    fn new(min: f64, max: f64, stable_alive_seconds: f64) -> Self {
        let min = min.max(0.0);
        let max = max.max(min);
        Self {
            current: min,
            min,
            max,
            stable_alive_seconds: stable_alive_seconds.max(0.0),
        }
    }

    /// A connection ended; reset the backoff only if it was stable.
    fn on_disconnect(&mut self, alive: Duration) {
        if alive.as_secs_f64() >= self.stable_alive_seconds {
            self.current = self.min;
        } else {
            tracing::warn!(
                alive_ms = alive.as_millis(),
                stable_alive_seconds = self.stable_alive_seconds,
                "ws_feed_short_connection"
            );
        }
    }

    /// The delay to sleep before the next attempt; the attempt after that
    /// doubles (capped at `max`).
    fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = (self.current * 2.0).min(self.max);
        Duration::from_secs_f64(delay)
    }
}

/// The connect/subscribe/read/reconnect loop (6c). The feed is shared behind a
/// mutex so the read loop can take `&mut` (`handle_message` is `&mut self`).
async fn run_feed_loop(feed: std::sync::Mutex<WebSocketFeed>, bus: Arc<EventBus>) {
    use futures::SinkExt;
    use futures::StreamExt;
    use tokio_tungstenite::connect_async;

    let ws_url = feed.lock().unwrap().ws_url.clone();
    let mut backoff = {
        let inner = feed.lock().unwrap();
        ReconnectBackoff::new(
            inner.reconnect_delay_min,
            inner.reconnect_delay_max,
            inner.stable_alive_seconds,
        )
    };
    let mut generation = 0u32;
    loop {
        match connect_async(&ws_url).await {
            Ok((ws_stream, _resp)) => {
                let (mut write, mut read) = ws_stream.split();
                // Bump the generation for this socket; consumers use it to
                // detect that a reconnect happened (stale-frame rejection is
                // the consumer's responsibility).
                generation += 1;
                let connected_at = Instant::now();
                tracing::info!(generation, "ws_feed_connected");
                feed.lock().unwrap().on_reconnect(generation);

                // Subscribe.
                let subs = feed.lock().unwrap().build_subscriptions();
                for sub in &subs {
                    if let Err(e) = write
                        .send(tokio_tungstenite::tungstenite::Message::Text(
                            sub.to_string().into(),
                        ))
                        .await
                    {
                        tracing::warn!(error = %e, "ws_feed_subscribe_error");
                        break;
                    }
                }

                // Read frames until the socket closes or goes silent.
                loop {
                    let next = tokio::time::timeout(
                        Duration::from_secs(READ_TIMEOUT_SECONDS),
                        read.next(),
                    );
                    let msg = match next.await {
                        Err(_) => {
                            tracing::warn!(seconds = READ_TIMEOUT_SECONDS, "ws_feed_read_timeout");
                            break;
                        }
                        Ok(None) => break,
                        Ok(Some(Err(e))) => {
                            tracing::warn!(error = %e, "ws_feed_read_error");
                            break;
                        }
                        Ok(Some(Ok(m))) => m,
                    };
                    let text = match msg {
                        tokio_tungstenite::tungstenite::Message::Text(t) => t.to_string(),
                        tokio_tungstenite::tungstenite::Message::Ping(p) => {
                            let _ = write
                                .send(tokio_tungstenite::tungstenite::Message::Pong(p))
                                .await;
                            continue;
                        }
                        tokio_tungstenite::tungstenite::Message::Close(_) => break,
                        _ => continue,
                    };
                    // Re-send subscriptions the exchange rejected (M-MD3).
                    // The feed mutex is scoped so no lock is held across the
                    // `send().await` below.
                    let resubscribe = {
                        let mut inner = feed.lock().unwrap();
                        inner.connection_generation = generation;
                        if let Err(e) = inner.handle_message(&bus, &text) {
                            tracing::warn!(error = %e, "ws_feed_message_error");
                        }
                        inner.drain_resubscribe_queue()
                    };
                    for sub in resubscribe {
                        if let Err(e) = write
                            .send(tokio_tungstenite::tungstenite::Message::Text(
                                sub.to_string().into(),
                            ))
                            .await
                        {
                            tracing::warn!(error = %e, "ws_feed_resubscribe_error");
                            break;
                        }
                    }
                }
                // Socket closed or timed out. Reset the backoff only when the
                // connection was stable; short-lived connections keep the
                // accumulated exponential backoff (H-MD1).
                backoff.on_disconnect(connected_at.elapsed());
            }
            Err(e) => {
                tracing::error!(error = %e, "ws_feed_connection_error");
            }
        }
        tokio::time::sleep(backoff.next_delay()).await;
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

    #[test]
    fn backoff_accumulates_on_short_connections_and_resets_after_stable() {
        // H-MD1: connect-then-drop must keep the exponential backoff growing;
        // a connection that survives the stable threshold resets it to min.
        let mut b = ReconnectBackoff::new(1.0, 30.0, 60.0);
        assert_eq!(b.next_delay(), Duration::from_secs(1));
        // Connect succeeds but drops immediately (alive 1s < 60s).
        b.on_disconnect(Duration::from_secs(1));
        assert_eq!(b.next_delay(), Duration::from_secs(2));
        b.on_disconnect(Duration::from_secs(1));
        assert_eq!(b.next_delay(), Duration::from_secs(4));
        b.on_disconnect(Duration::from_secs(1));
        assert_eq!(
            b.next_delay(),
            Duration::from_secs(8),
            "backoff keeps doubling"
        );
        // Long-lived connection resets to the minimum.
        b.on_disconnect(Duration::from_secs(61));
        assert_eq!(
            b.next_delay(),
            Duration::from_secs(1),
            "stable connection resets backoff"
        );
        // Doubling is capped at max.
        let mut capped = ReconnectBackoff::new(16.0, 30.0, 60.0);
        assert_eq!(capped.next_delay(), Duration::from_secs(16));
        assert_eq!(capped.next_delay(), Duration::from_secs(30));
        assert_eq!(
            capped.next_delay(),
            Duration::from_secs(30),
            "capped at max"
        );
    }

    #[test]
    fn subscription_response_failure_queues_resubscribe() {
        // M-MD3: success=false records the subscription for retry; a later
        // success clears it. The retry queue is rate-limited.
        let mut f = feed();
        let failed = r#"{"type":"subscriptionResponse","response":{"subscription":{"type":"candle","coin":"BTC","interval":"1m"}},"success":false,"error":"invalid"}"#;
        f.handle_message(&EventBus::new(16), failed).unwrap();
        assert_eq!(f.failed_subscriptions.len(), 1);
        assert!(f.resubscribe_attempts.is_empty());
        // First drain sends the retry and counts the attempt.
        let queue = f.drain_resubscribe_queue();
        assert_eq!(queue.len(), 1);
        assert_eq!(f.resubscribe_attempts.get(&queue[0].to_string()), Some(&1));
        // Rate-limited: a second drain within the interval is empty.
        assert!(f.drain_resubscribe_queue().is_empty());
        // A successful ack clears the retry state.
        let ok = r#"{"type":"subscriptionResponse","response":{"subscription":{"type":"candle","coin":"BTC","interval":"1m"}},"success":true}"#;
        f.handle_message(&EventBus::new(16), ok).unwrap();
        assert!(f.failed_subscriptions.is_empty());
        assert!(f.resubscribe_attempts.is_empty());
    }

    #[test]
    fn error_channel_is_tolerated() {
        let mut f = feed();
        let frame = r#"{"channel":"error","data":"invalid subscription"}"#;
        assert!(f.handle_message(&EventBus::new(16), frame).is_ok());
    }

    #[test]
    fn zero_price_levels_are_dropped() {
        // M-MD5: levels with px<=0 or sz<=0 must never enter the book.
        let bus = EventBus::new(16);
        let mailbox = bus.subscribe(EventType::L2BookUpdate);
        let mut f = feed();
        let frame = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[{"px":"0","sz":"2"},{"px":"100","sz":"1"},{"px":"101","sz":"0"}],[{"px":"-5","sz":"3"},{"px":"102","sz":"4"}]]}}"#;
        f.handle_message(&bus, frame).unwrap();
        let ev = mailbox.try_recv().unwrap();
        match &ev.payload {
            DomainEvent::L2BookUpdate(s) => {
                assert_eq!(s.bids.len(), 1, "zero-px and zero-sz bids dropped");
                assert_eq!(s.bids[0].price.to_string(), "100");
                assert_eq!(s.asks.len(), 1, "negative-px ask dropped");
                assert_eq!(s.asks[0].price.to_string(), "102");
            }
            _ => panic!("wrong event"),
        }
    }

    #[test]
    fn all_mids_publishes_only_subscribed_coins() {
        // M-MD6: mids for coins outside the subscription list are not
        // published; configured coins still are.
        let bus = EventBus::new(16);
        let mailbox = bus.subscribe(EventType::MidPriceUpdate);
        let mut f = feed();
        let frame = r#"{"channel":"allMids","data":{"mids":{"BTC":"65000.5","ETH":"3200.25","DOGE":"0.123","@1":"0.5"}}}"#;
        f.handle_message(&bus, frame).unwrap();
        let mut symbols: Vec<String> = Vec::new();
        while let Some(ev) = mailbox.try_recv() {
            if let DomainEvent::MidPriceUpdate(mid) = &ev.payload {
                symbols.push(mid.symbol.clone());
            }
        }
        symbols.sort();
        // BTC + ETH subscribed; DOGE not; @1 is a configured spot coin.
        assert_eq!(
            symbols,
            vec!["@1".to_string(), "BTC".to_string(), "ETH".to_string()]
        );
    }

    #[test]
    fn trade_gap_tracking_keeps_max_tid() {
        // M-MD4: tid continuity state is maintained and out-of-order frames do
        // not regress the baseline.
        let bus = EventBus::new(16);
        let mut f = feed();
        let t = |tid: u64| {
            format!(
                r#"{{"channel":"trades","data":[{{"coin":"BTC","side":"B","px":"65000","sz":"0.5","tid":{tid},"time":1700000000000}}]}}"#
            )
        };
        f.handle_message(&bus, &t(100)).unwrap();
        assert_eq!(f.last_tid.get("BTC"), Some(&100));
        // Gap: tid jumps by 5 — warned, but the feed keeps working.
        f.handle_message(&bus, &t(105)).unwrap();
        assert_eq!(f.last_tid.get("BTC"), Some(&105));
        // Out-of-order regresses nothing.
        f.handle_message(&bus, &t(102)).unwrap();
        assert_eq!(f.last_tid.get("BTC"), Some(&105));
        // Reconnect resets the baseline and warns.
        f.on_reconnect(2);
        assert!(f.last_tid.is_empty());
        f.handle_message(&bus, &t(200)).unwrap();
        assert_eq!(f.last_tid.get("BTC"), Some(&200));
    }

    /// Compile-time assertion that `WebSocketFeed::run`'s future is
    /// `Send + 'static` — exactly the bounds `tokio::spawn` requires.
    ///
    /// The app layer drives the feed with `tokio::spawn`, which needs the
    /// future to be `Send`; a `std::sync::MutexGuard` held across an await
    /// point (e.g. inside `run_feed_loop`) would break that. This test fails
    /// to compile if any lock guard leaks across an await, so the reconnect
    /// loop stays spawnable.
    #[test]
    fn run_future_is_spawnable() {
        fn assert_spawnable<T: Send + 'static>(_future: T) {}
        let feed = Arc::new(feed());
        let bus = Arc::new(EventBus::new(16));
        // Build the future without driving it (async fn call is lazy).
        assert_spawnable(feed.run(bus));
    }
}
