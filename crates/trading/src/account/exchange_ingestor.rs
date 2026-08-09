//! Authenticated exchange event ingestion, port of
//! `src/hypeedge/account/exchange_ingestor.py`.
//!
//! WebSocket delivery (`userFills` / `orderUpdates`) is the low-latency path;
//! incremental REST history is the durability path after disconnects and
//! restarts. Both converge through the same inbox key (the
//! [`ExchangeFactProjector`] boundary) so duplicates and reordering are
//! harmless.
//!
//! This module owns the pure parsing/identity helpers, the projector + info
//! client traits, and the ingestion orchestration (queue loop + gap recovery).
//! The transactional Postgres projector lives in the `storage` crate behind
//! [`ExchangeFactProjector`]; the trading crate stays DB-free.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use hypeedge_domain::decimal::Decimal;
use hypeedge_domain::enums::OrderStatus;
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::models::Fill;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::account::tracker::AccountTracker;

/// The exchange source tag for cursor/inbox scoping.
pub const SOURCE: &str = "hyperliquid";

/// Non-terminal order statuses (the live projection keeps these).
pub const OPEN_STATUSES: &[&str] = &["pending", "submitted", "submit_unknown", "acknowledged", "partial_fill"];
/// Terminal order statuses.
pub const TERMINAL_STATUSES: &[&str] = &["filled", "cancelled", "rejected", "expired"];

// --- Pure identity / parsing helpers (mirror the Python module functions) ---

fn decimal_from(v: Option<&Value>) -> Decimal {
    match v {
        Some(Value::String(s)) => Decimal::from_str_lenient(s).unwrap_or(Decimal::ZERO),
        Some(Value::Number(n)) => Decimal::from_f64(n.as_f64().unwrap_or(0.0)).unwrap_or(Decimal::ZERO),
        _ => Decimal::ZERO,
    }
}

/// Canonical payload: keys sorted, compact separators, then sha256 hex.
/// Mirrors `_canonical_payload` (two-pass `json.dumps(sort_keys, separators)`).
pub fn canonical_payload(payload: &Value) -> (String, Value) {
    let normalized = sort_keys_recursive(payload);
    let encoded = serde_json::to_string(&normalized).unwrap_or_default();
    let digest = sha256_hex(encoded.as_bytes());
    (digest, normalized)
}

/// Recursively sort object keys (serde_json's `Map` is already a BTreeMap, but
/// nested arrays/values need the same treatment for full canonicalization).
fn sort_keys_recursive(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<String, Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), sort_keys_recursive(v)))
                .collect();
            Value::Object(sorted.into_iter().collect())
        }
        Value::Array(arr) => Value::Array(arr.iter().map(sort_keys_recursive).collect()),
        other => other.clone(),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Deterministic cloid for an exchange OID with no cloid in the payload
/// (mirrors `_synthetic_cloid`; only used for unknown-oid recovery orders).
pub fn synthetic_cloid(exchange_oid: &str) -> String {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(format!("exchange-order:{exchange_oid}"));
    format!("0x{}", hex::encode(hasher.finalize()))
}

/// Stable exchange identity used by inbox and fills.
pub fn fill_external_id(fill: &Value) -> String {
    if let Some(tid) = fill.get("tid").and_then(|t| t.as_i64()) {
        return format!("fill:{tid}");
    }
    let parts: Vec<String> = ["hash", "oid", "time", "coin", "side", "px", "sz"]
        .iter()
        .map(|k| str_of(fill.get(*k)))
        .collect();
    format!("fill:{}", parts.join(":"))
}

pub fn funding_external_id(update: &Value) -> String {
    let delta = update.get("delta");
    let coin = delta.and_then(|d| d.get("coin")).map(|v| str_of(Some(v))).unwrap_or_default();
    let usdc = delta.and_then(|d| d.get("usdc")).map(|v| str_of(Some(v))).unwrap_or_default();
    format!(
        "funding:{}:{}:{}:{}",
        str_of(update.get("hash")),
        str_of(update.get("time")),
        coin,
        usdc
    )
}

fn str_of(v: Option<&Value>) -> String {
    match v {
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

/// Projected position size after a fill: `startPosition + sz * side`.
pub fn fill_position_after(fill: &Value) -> Decimal {
    let start = decimal_from(fill.get("startPosition"));
    let sz = decimal_from(fill.get("sz"));
    let is_buy = str_of(fill.get("side")).eq_ignore_ascii_case("B");
    if is_buy {
        start + sz
    } else {
        start - sz
    }
}

/// Average-cost entry projection; exchange reconciliation remains authoritative.
pub fn projected_entry_price(
    old_size: Decimal,
    old_entry: Option<Decimal>,
    new_size: Decimal,
    fill_price: Decimal,
) -> Option<Decimal> {
    if new_size.is_zero() {
        return None;
    }
    if old_size.is_zero() || old_size * new_size < Decimal::ZERO {
        return Some(fill_price);
    }
    if old_size * new_size > Decimal::ZERO && new_size.abs() > old_size.abs() {
        let Some(old_entry) = old_entry else {
            return Some(fill_price);
        };
        let added = new_size.abs() - old_size.abs();
        let numerator = old_size.abs() * old_entry + added * fill_price;
        return Some(numerator / new_size.abs());
    }
    old_entry
}

/// Normalize an exchange order status to the domain vocabulary.
pub fn normalize_status(raw: Option<&Value>) -> String {
    let value = str_of(raw).trim().to_lowercase().replace('_', "");
    let normalized = match value.as_str() {
        "open" => Some("acknowledged"),
        "filled" => Some("filled"),
        "canceled" | "cancelled" => Some("cancelled"),
        "rejected" => Some("rejected"),
        "expired" => Some("expired"),
        "triggered" => Some("acknowledged"),
        _ => None,
    };
    if let Some(n) = normalized {
        return n.to_string();
    }
    if value.ends_with("canceled") || value.ends_with("cancelled") || matches!(value.as_str(), "ioccancel" | "scheduledcancel") {
        return "cancelled".into();
    }
    if value.ends_with("rejected") {
        return "rejected".into();
    }
    "acknowledged".into()
}

/// Extract the exchange order payload from an `orderStatus` response.
pub fn order_from_status_response(response: &Value) -> Option<Value> {
    if response.get("status").and_then(|s| s.as_str()) != Some("order") {
        return None;
    }
    let wrapper = response.get("order")?;
    let nested = wrapper.get("order");
    match nested {
        Some(v) if v.is_object() => Some(v.clone()),
        _ => Some(wrapper.clone()),
    }
}

/// Map a normalized status string onto the domain enum, or fall back to
/// `Acknowledged` for open states.
pub fn status_to_order_status(status: &str) -> OrderStatus {
    match status {
        "filled" => OrderStatus::Filled,
        "cancelled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        "partial_fill" => OrderStatus::PartialFill,
        "submit_unknown" => OrderStatus::SubmitUnknown,
        "cancel_unknown" => OrderStatus::CancelUnknown,
        "pending" => OrderStatus::Pending,
        _ => OrderStatus::Acknowledged,
    }
}

// --- Projector boundary ---

/// Values committed by the fact transaction and safe for live projection.
#[derive(Debug, Clone, PartialEq)]
pub struct CommittedFillProjection {
    pub external_event_id: String,
    pub cloid: String,
    pub exchange_oid: String,
    pub symbol: String,
    pub side: String,
    pub price: Decimal,
    pub size: Decimal,
    pub fee: Decimal,
    pub is_maker: bool,
    pub occurred_at: i64, // unix millis
    pub strategy_id: Option<String>,
    pub sub_account: Option<String>,
    pub position_size: Option<Decimal>,
    pub position_entry_price: Option<Decimal>,
    pub position_mark_price: Option<Decimal>,
    pub order_status: String,
    pub is_spot: bool,
}

/// Outcome of one ingest attempt.
#[derive(Debug, Clone, PartialEq)]
pub struct IngestResult {
    pub processed: bool,
    pub external_event_id: String,
    pub fill_projection: Option<CommittedFillProjection>,
    pub funding_amount: Option<Decimal>,
}

impl IngestResult {
    pub fn dedup(external_event_id: &str) -> Self {
        Self {
            processed: false,
            external_event_id: external_event_id.to_string(),
            fill_projection: None,
            funding_amount: None,
        }
    }
}

/// The transactional fact projector: claim-inbox + append facts in one
/// transaction. Implemented by the Postgres projector in `storage`.
#[async_trait]
pub trait ExchangeFactProjector: Send + Sync {
    /// Commit a fill (inbox claim + fill/order/position/ledger/outbox + cursor).
    async fn ingest_fill(&self, fill: &Value) -> Result<IngestResult, HypeEdgeError>;
    /// Commit an order status update.
    async fn ingest_order_update(&self, update: &Value) -> Result<IngestResult, HypeEdgeError>;
    /// Commit a funding payment.
    async fn ingest_funding(&self, update: &Value) -> Result<IngestResult, HypeEdgeError>;
    /// Whether an exchange OID is already bound in the durable projection.
    async fn has_order(&self, exchange_oid: &str) -> Result<bool, HypeEdgeError>;
    /// The last sync cursor for a stream (ms timestamp), 0 when unknown.
    async fn cursor(&self, stream: &str) -> Result<i64, HypeEdgeError>;
}

/// Read-only Hyperliquid info endpoints used for recovery. Implemented by the
/// app wiring (reqwest) and mocked in tests.
#[async_trait]
pub trait InfoClient: Send + Sync {
    async fn historical_orders(&self, account: &str) -> Result<Vec<Value>, String>;
    async fn user_fills_by_time(&self, account: &str, start_ms: i64, end_ms: i64) -> Result<Vec<Value>, String>;
    async fn user_funding_history(&self, account: &str, start_ms: i64, end_ms: i64) -> Result<Vec<Value>, String>;
    async fn query_order_by_oid(&self, account: &str, exchange_oid: i64) -> Result<Option<Value>, String>;
}

/// One enqueued authenticated WS message: `(kind, payload)`, kind ∈
/// `fill` | `order` | `funding`.
type IngestMessage = (String, Value);

/// Own the authenticated subscriptions and incremental REST gap recovery.
///
/// The WS callback enqueues into a bounded channel (mirrors `asyncio.Queue`);
/// a consumer loop ingests each message through the projector and, for fills,
/// applies the committed projection to the live tracker. Periodic recovery
/// backfills REST history after the cursor.
pub struct ExchangeEventIngestor {
    account: String,
    projector: Arc<dyn ExchangeFactProjector>,
    info: Arc<dyn InfoClient>,
    tracker: Option<Arc<AccountTracker>>,
    tx: mpsc::Sender<IngestMessage>,
    rx: mpsc::Receiver<IngestMessage>,
    poll_interval_seconds: f64,
    history_recovered: bool,
}

impl ExchangeEventIngestor {
    pub fn new(
        account: &str,
        projector: Arc<dyn ExchangeFactProjector>,
        info: Arc<dyn InfoClient>,
        tracker: Option<Arc<AccountTracker>>,
        poll_interval_seconds: f64,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<IngestMessage>(10_000);
        Self {
            account: account.to_string(),
            projector,
            info,
            tracker,
            tx,
            rx,
            poll_interval_seconds: if poll_interval_seconds > 0.0 { poll_interval_seconds } else { 30.0 },
            history_recovered: false,
        }
    }

    /// Enqueue a message from the WS callback (bounded; drops + logs on overflow).
    pub fn enqueue(&self, kind: impl Into<String>, payload: Value) {
        let kind = kind.into();
        match self.tx.try_send((kind.clone(), payload)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::error!(kind, "exchange_event_queue_full");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!(kind, "exchange_event_queue_closed");
            }
        }
    }

    /// Dequeue a message: `None` when the channel is closed.
    pub async fn recv(&mut self) -> Option<IngestMessage> {
        self.rx.recv().await
    }

    pub async fn run_until_closed(&mut self) {
        while let Some((kind, payload)) = self.rx.recv().await {
            let result = match kind.as_str() {
                "fill" => self.ingest_fill(&payload).await,
                "order" => self.projector.ingest_order_update(&payload).await,
                _ => self.projector.ingest_funding(&payload).await,
            };
            if let Err(e) = result {
                tracing::error!(kind, error = %e, "exchange_event_ingest_failed");
            }
        }
    }

    /// Process one fill: commit through the projector, then apply the
    /// committed projection to the live tracker.
    pub async fn ingest_fill(&self, fill_payload: &Value) -> Result<IngestResult, HypeEdgeError> {
        let mut payload = fill_payload.clone();
        let exchange_oid = str_of(payload.get("oid"));
        let raw_cloid = str_of(payload.get("cloid"));
        let has_canonical_cloid = raw_cloid.starts_with("0x") && raw_cloid.len() == 34;
        // Attach cloid/origSz/limitPx from a status lookup when the fill lacks a
        // canonical cloid and the order is not yet bound (mirrors `_ingest_fill`).
        if !exchange_oid.is_empty()
            && !has_canonical_cloid
            && !self.projector.has_order(&exchange_oid).await.unwrap_or(true)
            && let Ok(Some(response)) = self
                .info
                .query_order_by_oid(&self.account, exchange_oid.parse().unwrap_or(0))
                .await
            && let Some(exchange_order) = order_from_status_response(&response)
        {
            for key in ["cloid", "origSz", "limitPx"] {
                if let Some(v) = exchange_order.get(key).filter(|v| !v.is_null()) {
                    payload[key] = v.clone();
                }
            }
        }
        let result = self.projector.ingest_fill(&payload).await?;
        let Some(projection) = result.fill_projection.clone() else {
            return Ok(result);
        };
        let Some(tracker) = &self.tracker else {
            return Ok(result);
        };

        let fill = Fill {
            cloid: projection.cloid.clone(),
            exchange_oid: projection.exchange_oid.clone(),
            symbol: projection.symbol.clone(),
            side: if projection.side.eq_ignore_ascii_case("buy") {
                hypeedge_domain::enums::Side::Buy
            } else {
                hypeedge_domain::enums::Side::Sell
            },
            price: hypeedge_domain::decimal::Price::new(projection.price),
            size: hypeedge_domain::decimal::Size::new(projection.size),
            fee: hypeedge_domain::decimal::Usd::new(projection.fee),
            is_maker: projection.is_maker,
            timestamp: projection.occurred_at,
            strategy_id: projection.strategy_id.clone(),
            sub_account: projection.sub_account.clone(),
            is_spot: projection.is_spot,
        };
        let position = if projection.is_spot {
            None
        } else {
            Some(hypeedge_domain::models::Position {
                symbol: projection.symbol.clone(),
                size: hypeedge_domain::decimal::Size::new(projection.position_size.unwrap_or(Decimal::ZERO)),
                entry_price: projection.position_entry_price.map(hypeedge_domain::decimal::Price::new),
                mark_price: projection
                    .position_mark_price
                    .map(hypeedge_domain::decimal::Price::new)
                    .or(Some(fill.price)),
                unrealized_pnl: None,
                leverage: 0,
                liquidation_price: None,
                sub_account: projection.sub_account.clone(),
                strategy_id: projection.strategy_id.clone(),
            })
        };
        tracker.apply_authoritative_fill(&projection.external_event_id, &fill, position.as_ref());
        Ok(result)
    }

    /// Recover REST history after the last cursors. Mirrors `_recover_history_once`.
    pub async fn recover_history(&mut self) -> Result<(), String> {
        let end_ms = chrono::Utc::now().timestamp_millis();
        // 1. Historical orders (establish oid/cloid ownership before fills).
        let orders = self.info.historical_orders(&self.account).await?;
        let order_cursor = self.projector.cursor("orders").await.map_err(|e| e.to_string())?;
        let mut ordered = orders.clone();
        ordered.sort_by_key(|item| {
            item.get("statusTimestamp")
                .or_else(|| item.get("order").and_then(|o| o.get("timestamp")))
                .and_then(|v| v.as_i64())
                .unwrap_or(0)
        });
        for update in &ordered {
            let ts = update
                .get("statusTimestamp")
                .or_else(|| update.get("order").and_then(|o| o.get("timestamp")))
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            if ts >= order_cursor {
                self.projector.ingest_order_update(update).await.map_err(|e| e.to_string())?;
            }
        }

        // 2. Fills by time (paged until fewer than 2000 or cursor advances).
        let mut fill_cursor = self.projector.cursor("fills").await.map_err(|e| e.to_string())?;
        let mut start_ms = (fill_cursor - 1).max(0);
        loop {
            let fills = self.info.user_fills_by_time(&self.account, start_ms, end_ms).await?;
            let mut ordered_fills = fills;
            ordered_fills.sort_by(|a, b| {
                let (ta, ia) = (a.get("time").and_then(|v| v.as_i64()).unwrap_or(0), fill_external_id(a));
                let (tb, ib) = (b.get("time").and_then(|v| v.as_i64()).unwrap_or(0), fill_external_id(b));
                (ta, ia).cmp(&(tb, ib))
            });
            for fill in &ordered_fills {
                self.ingest_fill(fill).await.map_err(|e| e.to_string())?;
            }
            if ordered_fills.len() < 2000 {
                break;
            }
            if fill_cursor == 0 {
                tracing::warn!(retained = ordered_fills.len(), "initial_fill_history_bootstrap_truncated");
                break;
            }
            let latest_ms = ordered_fills.last().and_then(|f| f.get("time")).and_then(|v| v.as_i64()).unwrap_or(0);
            if latest_ms <= start_ms {
                return Err("user_fills_history_cursor_not_advancing".into());
            }
            start_ms = latest_ms;
            fill_cursor = start_ms;
        }

        // 3. Funding history.
        let funding_cursor = self.projector.cursor("funding").await.map_err(|e| e.to_string())?;
        let funding_updates = self.info.user_funding_history(&self.account, (funding_cursor - 1).max(0), end_ms).await?;
        let mut funding_sorted = funding_updates;
        funding_sorted.sort_by_key(|u| u.get("time").and_then(|v| v.as_i64()).unwrap_or(0));
        for update in &funding_sorted {
            let result = self.projector.ingest_funding(update).await.map_err(|e| e.to_string())?;
            if result.processed
                && let (Some(amount), Some(tracker)) = (result.funding_amount, &self.tracker)
            {
                tracker.apply_funding(&hypeedge_domain::decimal::Usd::new(amount));
            }
        }
        self.history_recovered = true;
        Ok(())
    }

    pub fn history_recovered(&self) -> bool {
        self.history_recovered
    }

    pub fn poll_interval_seconds(&self) -> f64 {
        self.poll_interval_seconds
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering as AtomicOrdering;

    #[test]
    fn fill_external_id_prefers_tid() {
        let fill = serde_json::json!({"tid": 42, "oid": "9", "hash": "abc"});
        assert_eq!(fill_external_id(&fill), "fill:42");
    }

    #[test]
    fn fill_external_id_falls_back_to_composite() {
        let fill = serde_json::json!({"oid": "9", "time": "1700000000000", "coin": "BTC", "side": "B", "px": "50000", "sz": "1.0"});
        let id = fill_external_id(&fill);
        assert!(id.starts_with("fill:"));
        assert!(id.contains("9"));
        assert!(id.contains("BTC"));
    }

    #[test]
    fn funding_external_id_composes_hash_time_coin_usdc() {
        let update = serde_json::json!({
            "hash": "0xabc", "time": "1700000000000",
            "delta": {"type": "funding", "coin": "BTC", "usdc": "1.5"}
        });
        assert_eq!(funding_external_id(&update), "funding:0xabc:1700000000000:BTC:1.5");
    }

    #[test]
    fn fill_position_after_signs_by_side() {
        let buy = serde_json::json!({"startPosition": "0.5", "sz": "0.25", "side": "B"});
        assert_eq!(fill_position_after(&buy).to_string(), "0.75");
        let sell = serde_json::json!({"startPosition": "0.5", "sz": "0.25", "side": "A"});
        assert_eq!(fill_position_after(&sell).to_string(), "0.25");
    }

    #[test]
    fn projected_entry_price_cases() {
        // Close → None.
        assert_eq!(projected_entry_price(Decimal::ONE, Some(Decimal::from_str_strict("100").unwrap()), Decimal::ZERO, Decimal::from_str_strict("101").unwrap()), None);
        // New position → fill price.
        let d1 = projected_entry_price(Decimal::ZERO, None, Decimal::from_str_strict("1").unwrap(), Decimal::from_str_strict("50000").unwrap());
        assert_eq!(d1.unwrap().to_string(), "50000");
        // Add same direction → VWAP.
        let d2 = projected_entry_price(
            Decimal::from_str_strict("1").unwrap(),
            Some(Decimal::from_str_strict("50000").unwrap()),
            Decimal::from_str_strict("2").unwrap(),
            Decimal::from_str_strict("60000").unwrap(),
        );
        assert_eq!(d2.unwrap().to_string(), "55000");
        // Reduce → keep old entry.
        let d3 = projected_entry_price(
            Decimal::from_str_strict("2").unwrap(),
            Some(Decimal::from_str_strict("50000").unwrap()),
            Decimal::from_str_strict("1").unwrap(),
            Decimal::from_str_strict("60000").unwrap(),
        );
        assert_eq!(d3.unwrap().to_string(), "50000");
    }

    #[test]
    fn normalize_status_variants() {
        assert_eq!(normalize_status(Some(&Value::from("open"))), "acknowledged");
        assert_eq!(normalize_status(Some(&Value::from("filled"))), "filled");
        assert_eq!(normalize_status(Some(&Value::from("canceled"))), "cancelled");
        assert_eq!(normalize_status(Some(&Value::from("cancelled"))), "cancelled");
        assert_eq!(normalize_status(Some(&Value::from("margincanceled"))), "cancelled");
        assert_eq!(normalize_status(Some(&Value::from("triggered"))), "acknowledged");
        assert_eq!(normalize_status(Some(&Value::from("scheduledcancel"))), "cancelled");
        assert_eq!(normalize_status(Some(&Value::from("expired"))), "expired");
    }

    #[test]
    fn order_from_status_response_unwraps_nested() {
        let resp = serde_json::json!({"status": "order", "order": {"order": {"oid": 1, "coin": "BTC"}}});
        let order = order_from_status_response(&resp).unwrap();
        assert_eq!(order["oid"], 1);
        // Non-order responses return None.
        assert!(order_from_status_response(&serde_json::json!({"status": "ok"})).is_none());
    }

    #[test]
    fn canonical_payload_is_stable_and_sorted() {
        let a = serde_json::json!({"b": 1, "a": {"d": 2, "c": 3}});
        let b = serde_json::json!({"a": {"c": 3, "d": 2}, "b": 1});
        let (ha, _) = canonical_payload(&a);
        let (hb, _) = canonical_payload(&b);
        assert_eq!(ha, hb, "key order must not change the canonical hash");
        assert_eq!(ha.len(), 64);
    }

    #[test]
    fn synthetic_cloid_is_deterministic() {
        let c1 = synthetic_cloid("12345");
        let c2 = synthetic_cloid("12345");
        assert_eq!(c1, c2);
        assert!(c1.starts_with("0x"));
        assert_eq!(c1.len(), 34);
    }

    #[test]
    fn status_maps_onto_domain_enum() {
        assert_eq!(status_to_order_status("filled"), OrderStatus::Filled);
        assert_eq!(status_to_order_status("cancelled"), OrderStatus::Cancelled);
        assert_eq!(status_to_order_status("acknowledged"), OrderStatus::Acknowledged);
        assert_eq!(status_to_order_status("partial_fill"), OrderStatus::PartialFill);
    }

    // --- Ingestor orchestration ---

    struct FakeProjector {
        fills_ingested: std::sync::atomic::AtomicU64,
    }
    impl FakeProjector {
        fn new() -> Self {
            Self { fills_ingested: std::sync::atomic::AtomicU64::new(0) }
        }
    }
    #[async_trait]
    impl ExchangeFactProjector for FakeProjector {
        async fn ingest_fill(&self, fill: &Value) -> Result<IngestResult, HypeEdgeError> {
            self.fills_ingested.fetch_add(1, AtomicOrdering::SeqCst);
            let id = fill_external_id(fill);
            Ok(IngestResult {
                processed: true,
                external_event_id: id.clone(),
                fill_projection: Some(CommittedFillProjection {
                    external_event_id: id,
                    cloid: "0xabc".into(),
                    exchange_oid: "9".into(),
                    symbol: "BTC".into(),
                    side: "buy".into(),
                    price: Decimal::from_str_strict("50000").unwrap(),
                    size: Decimal::from_str_strict("1.0").unwrap(),
                    fee: Decimal::from_str_strict("0.1").unwrap(),
                    is_maker: false,
                    occurred_at: 1_700_000_000_000,
                    strategy_id: None,
                    sub_account: Some("0xabc".into()),
                    position_size: Some(Decimal::from_str_strict("1.0").unwrap()),
                    position_entry_price: Some(Decimal::from_str_strict("50000").unwrap()),
                    position_mark_price: Some(Decimal::from_str_strict("50000").unwrap()),
                    order_status: "filled".into(),
                    is_spot: false,
                }),
                funding_amount: None,
            })
        }
        async fn ingest_order_update(&self, _update: &Value) -> Result<IngestResult, HypeEdgeError> {
            Ok(IngestResult::dedup("order:x"))
        }
        async fn ingest_funding(&self, _update: &Value) -> Result<IngestResult, HypeEdgeError> {
            Ok(IngestResult::dedup("funding:x"))
        }
        async fn has_order(&self, _oid: &str) -> Result<bool, HypeEdgeError> {
            Ok(true)
        }
        async fn cursor(&self, _stream: &str) -> Result<i64, HypeEdgeError> {
            Ok(0)
        }
    }

    struct FakeInfo {
        orders: Vec<Value>,
        fills: Vec<Value>,
    }
    #[async_trait]
    impl InfoClient for FakeInfo {
        async fn historical_orders(&self, _account: &str) -> Result<Vec<Value>, String> {
            Ok(self.orders.clone())
        }
        async fn user_fills_by_time(&self, _account: &str, _start: i64, _end: i64) -> Result<Vec<Value>, String> {
            Ok(self.fills.clone())
        }
        async fn user_funding_history(&self, _account: &str, _start: i64, _end: i64) -> Result<Vec<Value>, String> {
            Ok(vec![])
        }
        async fn query_order_by_oid(&self, _account: &str, _oid: i64) -> Result<Option<Value>, String> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn enqueue_and_consume_ingests_fills() {
        let projector = Arc::new(FakeProjector::new());
        let tracker = Arc::new(AccountTracker::new());
        let mut ingestor = ExchangeEventIngestor::new(
            "0xabc",
            projector.clone(),
            Arc::new(FakeInfo { orders: vec![], fills: vec![] }),
            Some(tracker.clone()),
            30.0,
        );
        ingestor.enqueue(
            "fill",
            serde_json::json!({"tid": 1, "oid": "9", "time": "1700000000000", "coin": "BTC", "side": "B", "px": "50000", "sz": "1.0", "fee": "0.1", "startPosition": "0"}),
        );
        let msg = ingestor.recv().await.unwrap();
        assert_eq!(msg.0, "fill");
        let result = ingestor.ingest_fill(&msg.1).await.unwrap();
        assert!(result.processed);
        // The committed projection applied to the tracker.
        let pos = tracker.get_position("BTC").unwrap();
        assert_eq!(pos.size.to_string(), "1");
        assert_eq!(tracker.fill_count(), 1);
    }

    #[tokio::test]
    async fn recover_history_replays_orders_and_fills() {
        let projector = Arc::new(FakeProjector::new());
        let mut ingestor = ExchangeEventIngestor::new(
            "0xabc",
            projector.clone(),
            Arc::new(FakeInfo {
                orders: vec![serde_json::json!({"oid": 1, "coin": "BTC", "status": "open", "sz": "1.0"})],
                fills: vec![serde_json::json!({"tid": 1, "oid": "1", "time": "1700000000000", "coin": "BTC", "side": "B", "px": "50000", "sz": "1.0"})],
            }),
            None,
            30.0,
        );
        ingestor.recover_history().await.unwrap();
        assert!(ingestor.history_recovered());
        assert!(projector.fills_ingested.load(AtomicOrdering::SeqCst) >= 1);
    }

    #[test]
    fn poll_interval_defaults_to_thirty_when_non_positive() {
        let projector = Arc::new(FakeProjector::new());
        let ingestor = ExchangeEventIngestor::new(
            "0xabc",
            projector,
            Arc::new(FakeInfo { orders: vec![], fills: vec![] }),
            None,
            0.0,
        );
        assert_eq!(ingestor.poll_interval_seconds(), 30.0);
    }
}
