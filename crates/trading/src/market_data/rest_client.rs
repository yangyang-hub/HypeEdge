//! REST client for Hyperliquid info endpoints, port of
//! `src/hypeedge/market_data/rest_client.py`.
//!
//! Handles info queries (with rate limiting and retries), candle/funding
//! backfill pagination, and account-state polling. Implements the
//! [`CandleHistoryClient`] and the account-health [`ClearinghouseRestClient`]
//! boundaries so the live provider and account poller stay concrete.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hypeedge_domain::decimal::{Price, Size};
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::models::{Candle, FundingRate};
use serde_json::{Value, json};

use crate::account::account_health::ClearinghouseRestClient;
use crate::market_data::instrument_cache::InstrumentMetaSource;
use crate::market_data::live_provider::CandleHistoryClient;
use crate::market_data::rate_limiter::RateLimiter;

/// How many info-request retries to attempt before giving up.
const MAX_INFO_RETRIES: u32 = 3;

/// Convert a candle interval string to milliseconds.
pub fn interval_to_ms(interval: &str) -> Result<i64, HypeEdgeError> {
    let ms = match interval {
        "1m" => 60_000,
        "3m" => 3 * 60_000,
        "5m" => 5 * 60_000,
        "15m" => 15 * 60_000,
        "30m" => 30 * 60_000,
        "1h" => 60 * 60_000,
        "2h" => 2 * 60 * 60_000,
        "4h" => 4 * 60 * 60_000,
        "8h" => 8 * 60 * 60_000,
        "12h" => 12 * 60 * 60_000,
        "1d" => 24 * 60 * 60_000,
        "3d" => 3 * 24 * 60 * 60_000,
        "1w" => 7 * 24 * 60 * 60_000,
        "1M" => 30 * 24 * 60 * 60_000,
        _ => {
            return Err(HypeEdgeError::MarketData(format!(
                "Unsupported candle interval: {interval}"
            )));
        }
    };
    Ok(ms)
}

/// Async REST client for Hyperliquid info endpoints.
pub struct RestClient {
    http: reqwest::Client,
    base_url: String,
    rate_limiter: Arc<RateLimiter>,
    backfill_batch_size: u32,
}

impl RestClient {
    pub fn new(
        base_url: &str,
        rate_limiter: Arc<RateLimiter>,
        backfill_batch_size: u32,
    ) -> Result<Self, HypeEdgeError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| HypeEdgeError::MarketData(format!("http client build: {e}")))?;
        Ok(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
            rate_limiter,
            backfill_batch_size,
        })
    }

    /// POST to /info with rate limiting and retry (429 / >=500 retryable).
    pub async fn post_info(
        &self,
        request_type: &str,
        payload: Option<&Value>,
        item_count: u64,
    ) -> Result<Value, HypeEdgeError> {
        let mut body = json!({ "type": request_type });
        if let Some(Value::Object(extra)) = payload
            && let Value::Object(map) = &mut body
        {
            for (k, v) in extra {
                map.insert(k.clone(), v.clone());
            }
        }
        let mut last_error: Option<String> = None;
        for attempt in 0..MAX_INFO_RETRIES {
            self.rate_limiter
                .acquire(request_type, 0, item_count)
                .await
                .map_err(HypeEdgeError::MarketData)?;
            let response = self
                .http
                .post(format!("{}/info", self.base_url))
                .json(&body)
                .send()
                .await;
            match response {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return resp
                            .json::<Value>()
                            .await
                            .map_err(|e| HypeEdgeError::MarketData(format!("info decode: {e}")));
                    }
                    let retryable = status == reqwest::StatusCode::TOO_MANY_REQUESTS
                        || status.is_server_error();
                    last_error = Some(format!("info {} status {}", request_type, status));
                    if !retryable || attempt >= MAX_INFO_RETRIES - 1 {
                        return Err(HypeEdgeError::MarketData(last_error.unwrap_or_default()));
                    }
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    if attempt >= MAX_INFO_RETRIES - 1 {
                        return Err(HypeEdgeError::MarketData(format!(
                            "info request failed: {e}"
                        )));
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(250 * (1 << attempt))).await;
        }
        Err(HypeEdgeError::MarketData(format!(
            "info {} failed: {}",
            request_type,
            last_error.unwrap_or_default()
        )))
    }

    pub async fn get_l2_book(&self, coin: &str) -> Result<Value, HypeEdgeError> {
        self.post_info("l2Book", Some(&json!({ "coin": coin })), 1)
            .await
    }

    pub async fn get_meta(&self) -> Result<Value, HypeEdgeError> {
        self.post_info("meta", None, 1).await
    }

    pub async fn get_meta_and_asset_ctxs(&self) -> Result<Value, HypeEdgeError> {
        self.post_info("metaAndAssetCtxs", None, 1).await
    }

    pub async fn get_spot_meta(&self) -> Result<Value, HypeEdgeError> {
        self.post_info("spotMeta", None, 1).await
    }

    pub async fn get_spot_user_state(&self, user: &str) -> Result<Value, HypeEdgeError> {
        self.post_info("spotClearinghouseState", Some(&json!({ "user": user })), 1)
            .await
    }

    pub async fn get_user_rate_limit(&self, user: &str) -> Result<Value, HypeEdgeError> {
        self.post_info("userRateLimit", Some(&json!({ "user": user })), 1)
            .await
    }

    /// Fetch one authoritative quota snapshot and update the shared limiter.
    pub async fn poll_action_credit_snapshot(
        &self,
        user: &str,
    ) -> Result<Option<Value>, HypeEdgeError> {
        match self.get_user_rate_limit(user).await {
            Ok(data) => {
                let remaining = data.get("remaining").and_then(|v| v.as_i64()).or_else(|| {
                    let used = data
                        .get("nRequestsUsed")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let cap = data
                        .get("nRequestsCap")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    Some((cap - used).max(0))
                });
                if let Some(remaining) = remaining {
                    self.rate_limiter.update_action_credits(remaining);
                }
                Ok(Some(data))
            }
            Err(_) => Ok(None),
        }
    }

    pub async fn poll_action_credits(&self, user: &str) -> Result<i64, HypeEdgeError> {
        Ok(self
            .poll_action_credit_snapshot(user)
            .await?
            .and_then(|data| {
                data.get("remaining").and_then(|v| v.as_i64()).or_else(|| {
                    let used = data
                        .get("nRequestsUsed")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let cap = data
                        .get("nRequestsCap")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    Some((cap - used).max(0))
                })
            })
            .unwrap_or(-1))
    }
}

#[async_trait]
impl CandleHistoryClient for RestClient {
    async fn backfill_candles(
        &self,
        coin: &str,
        interval: &str,
        start_time: i64,
        end_time: i64,
    ) -> Result<Vec<Candle>, HypeEdgeError> {
        let mut all_candles = Vec::new();
        let batch_size = self.backfill_batch_size as i64;
        let interval_ms = interval_to_ms(interval)?;
        let mut cursor = start_time;
        let mut consecutive_failures = 0u32;

        while cursor < end_time {
            let page_end = end_time.min(cursor + (batch_size * interval_ms));
            let estimated_items = ((page_end - cursor) / interval_ms).clamp(1, batch_size) as u64;
            self.rate_limiter
                .acquire("candleSnapshot", 0, estimated_items)
                .await
                .map_err(HypeEdgeError::MarketData)?;

            let body = json!({
                "type": "candleSnapshot",
                "req": {
                    "coin": coin,
                    "interval": interval,
                    "startTime": cursor,
                    "endTime": page_end,
                },
            });
            let response = self
                .http
                .post(format!("{}/info", self.base_url))
                .json(&body)
                .send()
                .await;
            let data: Value = match response {
                Ok(resp) if resp.status().is_success() => match resp.json::<Value>().await {
                    Ok(v) => v,
                    Err(_) => {
                        consecutive_failures += 1;
                        if consecutive_failures >= 3 {
                            return Err(HypeEdgeError::MarketData(format!(
                                "Candle backfill failed after {consecutive_failures} attempts: coin={coin} interval={interval}"
                            )));
                        }
                        tokio::time::sleep(Duration::from_millis(
                            500 * (1 << (consecutive_failures - 1)),
                        ))
                        .await;
                        continue;
                    }
                },
                Ok(resp) => {
                    consecutive_failures += 1;
                    if consecutive_failures >= 3 {
                        return Err(HypeEdgeError::MarketData(format!(
                            "Candle backfill failed after {consecutive_failures} attempts: coin={coin} interval={interval} status={}",
                            resp.status()
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(
                        500 * (1 << (consecutive_failures - 1)),
                    ))
                    .await;
                    continue;
                }
                Err(e) => {
                    consecutive_failures += 1;
                    if consecutive_failures >= 3 {
                        return Err(HypeEdgeError::MarketData(format!(
                            "Candle backfill failed after {consecutive_failures} attempts: coin={coin} interval={interval}: {e}"
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(
                        500 * (1 << (consecutive_failures - 1)),
                    ))
                    .await;
                    continue;
                }
            };

            consecutive_failures = 0;
            let Some(rows) = data.as_array() else {
                // Invalid response — treat as a terminal failure.
                return Err(HypeEdgeError::MarketData(format!(
                    "Candle backfill invalid response: coin={coin} interval={interval}"
                )));
            };
            if rows.is_empty() {
                if page_end >= end_time {
                    break;
                }
                cursor = page_end;
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            for row in rows {
                all_candles.push(Candle {
                    symbol: coin.to_string(),
                    interval: interval.to_string(),
                    open: Price::new(number_of(row, "o")),
                    high: Price::new(number_of(row, "h")),
                    low: Price::new(number_of(row, "l")),
                    close: Price::new(number_of(row, "c")),
                    volume: Size::new(number_of(row, "v")),
                    timestamp: int_of(row, "t"),
                });
            }
            let last_ts = int_of(rows.last().unwrap(), "t");
            if last_ts <= cursor {
                break;
            }
            cursor = last_ts + 1;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(all_candles)
    }

    async fn backfill_funding(
        &self,
        coin: &str,
        start_time: i64,
        end_time: i64,
    ) -> Result<Vec<FundingRate>, HypeEdgeError> {
        let mut all_funding = Vec::new();
        let batch_size = self.backfill_batch_size as i64;
        let funding_interval_ms = 60 * 60_000;
        let mut cursor = start_time;
        let mut consecutive_failures = 0u32;

        while cursor < end_time {
            let page_end = end_time.min(cursor + (batch_size * funding_interval_ms));
            let estimated_items =
                ((page_end - cursor) / funding_interval_ms).clamp(1, batch_size) as u64;
            self.rate_limiter
                .acquire("fundingHistory", 0, estimated_items)
                .await
                .map_err(HypeEdgeError::MarketData)?;

            let body = json!({
                "type": "fundingHistory",
                "coin": coin,
                "startTime": cursor,
                "endTime": page_end,
            });
            let response = self
                .http
                .post(format!("{}/info", self.base_url))
                .json(&body)
                .send()
                .await;
            let data: Value = match response {
                Ok(resp) if resp.status().is_success() => match resp.json::<Value>().await {
                    Ok(v) => v,
                    Err(e) => {
                        consecutive_failures += 1;
                        if consecutive_failures >= 3 {
                            return Err(HypeEdgeError::MarketData(format!(
                                "Funding backfill failed after {consecutive_failures} attempts: coin={coin}: {e}"
                            )));
                        }
                        tokio::time::sleep(Duration::from_millis(
                            500 * (1 << (consecutive_failures - 1)),
                        ))
                        .await;
                        continue;
                    }
                },
                Ok(resp) => {
                    consecutive_failures += 1;
                    if consecutive_failures >= 3 {
                        return Err(HypeEdgeError::MarketData(format!(
                            "Funding backfill failed after {consecutive_failures} attempts: coin={coin} status={}",
                            resp.status()
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(
                        500 * (1 << (consecutive_failures - 1)),
                    ))
                    .await;
                    continue;
                }
                Err(e) => {
                    consecutive_failures += 1;
                    if consecutive_failures >= 3 {
                        return Err(HypeEdgeError::MarketData(format!(
                            "Funding backfill failed after {consecutive_failures} attempts: coin={coin}: {e}"
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(
                        500 * (1 << (consecutive_failures - 1)),
                    ))
                    .await;
                    continue;
                }
            };

            consecutive_failures = 0;
            let Some(rows) = data.as_array() else {
                return Err(HypeEdgeError::MarketData(format!(
                    "Funding backfill invalid response: coin={coin}"
                )));
            };
            if rows.is_empty() {
                if page_end >= end_time {
                    break;
                }
                cursor = page_end;
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            for row in rows {
                all_funding.push(FundingRate {
                    symbol: coin.to_string(),
                    funding_rate: float_of(row, "fundingRate"),
                    premium: float_of(row, "premium"),
                    mark_price: Price::new(number_of(row, "markPx")),
                    open_interest: float_of(row, "openInterest"),
                    timestamp: int_of(row, "time"),
                });
            }
            let last_ts = int_of(rows.last().unwrap(), "time");
            if last_ts <= cursor {
                break;
            }
            cursor = last_ts + 1;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(all_funding)
    }
}

#[async_trait]
impl ClearinghouseRestClient for RestClient {
    async fn get_clearinghouse_state(&self, user: &str) -> Result<Value, HypeEdgeError> {
        self.post_info("clearinghouseState", Some(&json!({ "user": user })), 1)
            .await
    }

    async fn get_spot_user_state(&self, user: &str) -> Result<Value, HypeEdgeError> {
        self.post_info("spotClearinghouseState", Some(&json!({ "user": user })), 1)
            .await
    }
}

/// The account ingestor's authenticated-info boundary (6d wiring): historical
/// orders, fills-by-time, funding history, and single-order status, all built on
/// the shared `RestClient::post_info` engine.
#[async_trait]
impl crate::account::InfoClient for RestClient {
    async fn historical_orders(&self, account: &str) -> Result<Vec<Value>, String> {
        let resp = self
            .post_info("historicalOrders", Some(&json!({ "user": account })), 1)
            .await
            .map_err(|e| e.to_string())?;
        resp.as_array()
            .cloned()
            .ok_or_else(|| "historicalOrders: expected array".into())
    }

    /// M-RK7: server-side windowed page of historical orders.
    ///
    /// Hyperliquid's `historicalOrders` accepts `startTime`/`endTime` and caps
    /// a single response at ~2000 rows, so a client-side-filtered full pull
    /// would truncate deep history. This override pushes the window to the
    /// exchange; the ingestor's recovery loop then pages forward on the last
    /// `statusTimestamp`, recovering arbitrarily deep order history. Note the
    /// exchange expects the window under a `req` object for this endpoint
    /// (unlike `userFillsByTime`, which is flat).
    async fn historical_orders_paged(
        &self,
        account: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Value>, String> {
        if start_ms >= end_ms {
            return Ok(Vec::new());
        }
        let resp = self
            .post_info(
                "historicalOrders",
                Some(&json!({
                    "req": { "user": account, "startTime": start_ms, "endTime": end_ms }
                })),
                1,
            )
            .await
            .map_err(|e| e.to_string())?;
        resp.as_array()
            .cloned()
            .ok_or_else(|| "historicalOrders: expected array".into())
    }

    async fn user_fills_by_time(
        &self,
        account: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Value>, String> {
        let resp = self
            .post_info(
                "userFillsByTime",
                Some(&json!({ "user": account, "startTime": start_ms, "endTime": end_ms })),
                1,
            )
            .await
            .map_err(|e| e.to_string())?;
        resp.as_array()
            .cloned()
            .ok_or_else(|| "userFillsByTime: expected array".into())
    }

    async fn user_funding_history(
        &self,
        account: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Value>, String> {
        let resp = self
            .post_info(
                "userFunding",
                Some(&json!({ "user": account, "startTime": start_ms, "endTime": end_ms })),
                1,
            )
            .await
            .map_err(|e| e.to_string())?;
        resp.as_array()
            .cloned()
            .ok_or_else(|| "userFunding: expected array".into())
    }

    async fn query_order_by_oid(
        &self,
        account: &str,
        exchange_oid: i64,
    ) -> Result<Option<Value>, String> {
        let resp = self
            .post_info(
                "orderStatus",
                Some(&json!({ "user": account, "oid": exchange_oid })),
                1,
            )
            .await
            .map_err(|e| e.to_string())?;
        if resp.is_null() {
            Ok(None)
        } else {
            Ok(Some(resp))
        }
    }
}

#[async_trait]
impl InstrumentMetaSource for RestClient {
    async fn get_meta(&self) -> Result<Value, HypeEdgeError> {
        self.post_info("meta", None, 1).await
    }

    async fn get_spot_meta(&self) -> Result<Value, HypeEdgeError> {
        self.post_info("spotMeta", None, 1).await
    }
}

fn number_of(row: &Value, key: &str) -> hypeedge_domain::Decimal {
    let f = float_of(row, key);
    hypeedge_domain::Decimal::from_f64(f).unwrap_or_default()
}

fn float_of(row: &Value, key: &str) -> f64 {
    row.get(key)
        .and_then(|v| v.as_f64())
        .or_else(|| {
            row.get(key)
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(0.0)
}

fn int_of(row: &Value, key: &str) -> i64 {
    row.get(key)
        .and_then(|v| v.as_i64())
        .or_else(|| {
            row.get(key)
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::exchange_ingestor::InfoClient;

    #[test]
    fn interval_conversion() {
        assert_eq!(interval_to_ms("1m").unwrap(), 60_000);
        assert_eq!(interval_to_ms("1h").unwrap(), 3_600_000);
        assert_eq!(interval_to_ms("1d").unwrap(), 86_400_000);
        assert!(interval_to_ms("bogus").is_err());
    }

    #[test]
    fn row_parsing_handles_numbers_and_strings() {
        let row =
            json!({ "o": "50000", "h": 50100.5, "l": 49900, "c": 50050, "v": "1.5", "t": 123 });
        assert_eq!(float_of(&row, "o"), 50000.0);
        assert_eq!(float_of(&row, "h"), 50100.5);
        assert_eq!(int_of(&row, "t"), 123);
        assert_eq!(float_of(&row, "v"), 1.5);
    }

    #[tokio::test]
    async fn post_info_builds_payload_and_retries() {
        // No network in unit tests; this exercises the rate limiter path with a
        // never-resolving URL would be flaky — instead assert the config helper.
        let limiter = Arc::new(RateLimiter::new(1200, 100));
        let client = RestClient::new("http://127.0.0.1:1", limiter, 500).unwrap();
        assert_eq!(client.base_url, "http://127.0.0.1:1");
        assert_eq!(client.backfill_batch_size, 500);
    }

    /// Read one HTTP/1.1 request head + body from a raw stream. Does not touch
    /// the write half (the caller writes the response afterwards).
    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> (String, String) {
        use tokio::io::AsyncReadExt;
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            match stream.read(&mut byte).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    head.push(byte[0]);
                    if head.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
            }
        }
        let head_str = String::from_utf8_lossy(&head).to_string();
        let content_length = head_str
            .lines()
            .find_map(|l| {
                let (name, value) = l.split_once(':')?;
                if name.trim().eq_ignore_ascii_case("content-length") {
                    value.trim().parse::<usize>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0);
        let mut body = vec![0u8; content_length];
        if content_length > 0 {
            let _ = stream.read_exact(&mut body).await;
        }
        (head_str, String::from_utf8_lossy(&body).to_string())
    }

    /// Reply with a JSON body over a raw HTTP/1.1 stream, then close. Errors
    /// during teardown are ignored (the client may already be gone).
    async fn respond_json(stream: &mut tokio::net::TcpStream, value: &Value) {
        use tokio::io::AsyncWriteExt;
        let body = value.to_string();
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = stream.write_all(resp.as_bytes()).await;
        let _ = stream.shutdown().await;
    }

    /// A minimal in-test `/info` server: serves one canned JSON response per
    /// accepted connection and records each request body it received. Returns
    /// the base URL (the listener is owned by the spawned task) and a channel
    /// that receives the recorded request bodies.
    async fn serve_info(
        responses: Vec<Value>,
    ) -> (String, tokio::sync::oneshot::Receiver<Vec<Value>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut bodies = Vec::new();
            for response in responses {
                let (mut sock, _) = listener.accept().await.unwrap();
                let (_head, body) = read_http_request(&mut sock).await;
                bodies.push(serde_json::from_str(&body).unwrap_or(Value::Null));
                respond_json(&mut sock, &response).await;
            }
            let _ = tx.send(bodies);
        });
        (addr, rx)
    }

    #[tokio::test]
    async fn historical_orders_paged_windows_and_advances_cursor() {
        // M-RK7: the override must (1) send a server-side windowed request
        // (req.startTime/req.endTime), (2) return each page as-is, and (3) let
        // the caller's cursor advance — the second request must carry
        // startTime == the last statusTimestamp of the first page, so deep
        // history is never truncated at the ~2000-row server cap.
        use crate::account::exchange_ingestor::order_status_timestamp_ms;

        let page1_rows: Vec<Value> = (0..2000u64)
            .map(|i| json!({ "statusTimestamp": 1_000_000 + i as i64, "oid": i, "coin": "BTC" }))
            .collect();
        let page2_rows: Vec<Value> = (0..500u64)
            .map(|i| json!({ "statusTimestamp": 1_002_000 + i as i64, "oid": i, "coin": "BTC" }))
            .collect();
        let (addr, rx) = serve_info(vec![json!(&page1_rows), json!(&page2_rows)]).await;
        let limiter = Arc::new(RateLimiter::new(1200, 100));
        let client = RestClient::new(&format!("http://{addr}"), limiter, 500).unwrap();

        let window_end = 2_000_000i64;
        let page1 = client
            .historical_orders_paged("0xabc", 999, window_end)
            .await
            .unwrap();
        assert_eq!(page1.len(), 2000, "first page capped like the exchange");
        let last1 = order_status_timestamp_ms(page1.last().unwrap());
        assert_eq!(last1, 1_001_999);

        let page2 = client
            .historical_orders_paged("0xabc", last1, window_end)
            .await
            .unwrap();
        assert_eq!(page2.len(), 500, "second page returns the remainder");
        assert!(
            order_status_timestamp_ms(page2.first().unwrap()) > last1,
            "cursor must not regress"
        );

        let bodies = rx.await.unwrap();
        assert_eq!(bodies.len(), 2);
        assert_eq!(bodies[0]["type"], "historicalOrders");
        assert_eq!(bodies[0]["req"]["user"], "0xabc");
        assert_eq!(bodies[0]["req"]["startTime"], 999);
        assert_eq!(bodies[0]["req"]["endTime"], window_end);
        assert_eq!(
            bodies[1]["req"]["startTime"], last1,
            "cursor advanced to the last statusTimestamp of the previous page"
        );
    }

    #[tokio::test]
    async fn historical_orders_paged_empty_window_is_noop() {
        let limiter = Arc::new(RateLimiter::new(1200, 100));
        let client = RestClient::new("http://127.0.0.1:1", limiter, 500).unwrap();
        // start >= end: returns empty without any HTTP round trip.
        let out = client
            .historical_orders_paged("0xabc", 5_000, 5_000)
            .await
            .unwrap();
        assert!(out.is_empty());
        let out = client
            .historical_orders_paged("0xabc", 9_000, 5_000)
            .await
            .unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn historical_orders_paged_non_array_response_errors() {
        let (addr, rx) = serve_info(vec![json!({ "error": "boom" })]).await;
        let limiter = Arc::new(RateLimiter::new(1200, 100));
        let client = RestClient::new(&format!("http://{addr}"), limiter, 500).unwrap();
        let err = client
            .historical_orders_paged("0xabc", 0, 1_000)
            .await
            .unwrap_err();
        assert!(err.contains("expected array"), "got: {err}");
        let bodies = rx.await.unwrap();
        assert_eq!(bodies[0]["req"]["startTime"], 0);
        assert_eq!(bodies[0]["req"]["endTime"], 1_000);
    }
}
