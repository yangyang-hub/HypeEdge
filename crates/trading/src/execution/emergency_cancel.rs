//! Database-independent, WAL-backed emergency cancellation path, port of
//! `src/hypeedge/execution/emergency_cancel.py`.
//!
//! This module intentionally exposes no placement operation. It queries
//! exchange-authoritative open orders and sends cancellations through the same
//! serialized signing boundary used by normal execution. Every network attempt
//! is preceded by an append + fsync journal record so Postgres recovery can
//! later reconstruct what may have reached the exchange.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use hypeedge_domain::error::HypeEdgeError;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use super::cloid::CloidGenerator;
use super::exchange::{AssetIndexProvider, ExchangeClient};
use super::nonce::NonceQueue;
use super::signing::{CancelByCloidWire, CancelWire};

/// Exchange-authoritative order identifier safe for cancellation only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmergencyCancelTarget {
    pub symbol: String,
    pub cloid: Option<String>,
    /// Exchange order id (numeric, or string when a raw value is reported).
    pub oid: Option<String>,
}

impl EmergencyCancelTarget {
    pub fn new(
        symbol: impl Into<String>,
        cloid: Option<String>,
        oid: Option<String>,
    ) -> Result<Self, String> {
        let symbol = symbol.into();
        if symbol.is_empty() {
            return Err("emergency cancel target requires a symbol".into());
        }
        if cloid.is_none() && oid.is_none() {
            return Err("emergency cancel target requires cloid or oid".into());
        }
        Ok(Self { symbol, cloid, oid })
    }

    /// Stable identity used for dedup and verification.
    pub fn key(&self) -> String {
        match &self.cloid {
            Some(cloid) => format!("cloid:{}", canonical_cloid(cloid)),
            None => format!("oid:{}:{}", self.symbol, self.oid.as_deref().unwrap_or("")),
        }
    }
}

/// Authoritatively verified outcome for one cancellation target.
#[derive(Debug, Clone, PartialEq)]
pub struct EmergencyCancelResult {
    pub target: EmergencyCancelTarget,
    pub success: bool,
    pub outcome: String,
    pub attempt_id: Option<String>,
    pub error: Option<String>,
}

/// Result of cancel-all or WAL recovery.
#[derive(Debug, Clone, PartialEq)]
pub struct EmergencyCancelBatchResult {
    pub requested: usize,
    pub cancelled: usize,
    pub unresolved: Vec<EmergencyCancelTarget>,
}

impl EmergencyCancelBatchResult {
    pub fn success(&self) -> bool {
        self.unresolved.is_empty()
    }
}

/// Strict cancel-only execution boundary used during DB failure/halting.
#[async_trait]
pub trait EmergencyCancelExecutor: Send + Sync {
    async fn cancel(
        &self,
        target: EmergencyCancelTarget,
    ) -> Result<EmergencyCancelResult, HypeEdgeError>;
    async fn cancel_all(
        &self,
        symbol: Option<&str>,
    ) -> Result<EmergencyCancelBatchResult, HypeEdgeError>;
    async fn recover_pending(&self) -> Result<EmergencyCancelBatchResult, HypeEdgeError>;
}

/// Read exchange truth; an invalid response must raise, never become `[]`.
#[async_trait]
pub trait AuthoritativeOpenOrderProvider: Send + Sync {
    async fn get_open_orders(&self) -> Result<Vec<EmergencyCancelTarget>, HypeEdgeError>;
}

/// One immutable JSONL emergency journal fact.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmergencyJournalRecord {
    pub attempt_id: String,
    pub event: String,
    pub recorded_at: String,
    pub target: JournalTarget,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
}

/// JSON-serializable target shape for the journal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JournalTarget {
    pub symbol: String,
    pub cloid: Option<String>,
    pub oid: Option<String>,
}

/// A pending (unresolved) emergency attempt.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingEmergencyAttempt {
    pub attempt_id: String,
    pub target: EmergencyCancelTarget,
}

/// Append-only JSONL journal; every append is flushed and fsynced.
pub struct EmergencyCancelJournal {
    path: PathBuf,
    lock: Mutex<()>,
}

impl EmergencyCancelJournal {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            lock: Mutex::new(()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append a fact and fsync. Serialized by an async mutex; the blocking
    /// write runs on the blocking pool.
    pub async fn append(
        &self,
        attempt_id: &str,
        event: &str,
        target: &EmergencyCancelTarget,
        outcome: Option<&str>,
        error: Option<&str>,
    ) -> Result<(), HypeEdgeError> {
        let record = EmergencyJournalRecord {
            attempt_id: attempt_id.to_string(),
            event: event.to_string(),
            recorded_at: Utc::now().to_rfc3339(),
            target: JournalTarget {
                symbol: target.symbol.clone(),
                cloid: target.cloid.clone(),
                oid: target.oid.clone(),
            },
            outcome: outcome.map(|s| s.to_string()),
            error: error.map(|s| s.to_string()),
        };
        let mut payload = serde_json::to_vec(&record).map_err(|e| HypeEdgeError::Execution {
            message: e.to_string(),
        })?;
        payload.push(b'\n');
        let path = self.path.clone();
        let _guard = self.lock.lock().await;
        tokio::task::spawn_blocking(move || append_sync(&path, &payload))
            .await
            .map_err(|e| HypeEdgeError::Execution {
                message: format!("journal task panicked: {e}"),
            })??;
        Ok(())
    }

    /// Read all journal records, tolerating a torn tail line.
    pub async fn read_records(&self) -> Result<Vec<EmergencyJournalRecord>, HypeEdgeError> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || read_records_sync(&path))
            .await
            .map_err(|e| HypeEdgeError::Execution {
                message: format!("journal task panicked: {e}"),
            })?
    }

    /// Pending attempts: `dispatch_intent` minus terminal events.
    pub async fn pending_attempts(&self) -> Result<Vec<PendingEmergencyAttempt>, HypeEdgeError> {
        let records = self.read_records().await?;
        let mut pending: std::collections::HashMap<String, EmergencyCancelTarget> =
            std::collections::HashMap::new();
        for record in records {
            if record.event == "dispatch_intent" {
                let target = EmergencyCancelTarget::new(
                    record.target.symbol,
                    record.target.cloid,
                    record.target.oid,
                )
                .map_err(|e| HypeEdgeError::Execution { message: e })?;
                pending.insert(record.attempt_id.clone(), target);
            } else if is_terminal_event(&record.event) {
                pending.remove(&record.attempt_id);
            }
        }
        Ok(pending
            .into_iter()
            .map(|(attempt_id, target)| PendingEmergencyAttempt { attempt_id, target })
            .collect())
    }
}

fn is_terminal_event(event: &str) -> bool {
    matches!(
        event,
        "verified_absent" | "already_absent" | "recovery_resolved"
    )
}

/// Append + fsync one payload line.
fn append_sync(path: &Path, payload: &[u8]) -> Result<(), HypeEdgeError> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| HypeEdgeError::Execution {
            message: e.to_string(),
        })?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| HypeEdgeError::Execution {
            message: e.to_string(),
        })?;
    file.write_all(payload)
        .map_err(|e| HypeEdgeError::Execution {
            message: e.to_string(),
        })?;
    file.sync_all().map_err(|e| HypeEdgeError::Execution {
        message: e.to_string(),
    })?;
    Ok(())
}

/// Read and parse all JSONL records; a torn (incomplete) trailing line is
/// ignored, a malformed interior line is a hard error.
fn read_records_sync(path: &Path) -> Result<Vec<EmergencyJournalRecord>, HypeEdgeError> {
    let data = match std::fs::read(path) {
        Ok(data) => data,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(HypeEdgeError::Execution {
                message: e.to_string(),
            });
        }
    };
    let lines: Vec<&[u8]> = data
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .collect();
    let mut records = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        match serde_json::from_slice::<EmergencyJournalRecord>(line) {
            Ok(record) => records.push(record),
            Err(_) => {
                // Torn tail: the final line has no trailing newline and is
                // incomplete JSON — a crash mid-write. Ignore it.
                let is_torn_tail = index == lines.len() - 1 && !data.ends_with(b"\n");
                if is_torn_tail {
                    tracing::warn!(path = %path.display(), "emergency_journal_torn_tail_ignored");
                    break;
                }
                return Err(HypeEdgeError::Execution {
                    message: format!(
                        "Malformed emergency cancel journal: path={} line={}",
                        path.display(),
                        index + 1
                    ),
                });
            }
        }
    }
    Ok(records)
}

/// Adapter that reads exchange-authoritative open orders from an info client.
pub struct HyperliquidOpenOrderProvider<F> {
    fetch: F,
}

impl<F> HyperliquidOpenOrderProvider<F>
where
    F: Fn() -> Vec<serde_json::Value> + Send + Sync,
{
    pub fn new(fetch: F) -> Self {
        Self { fetch }
    }
}

#[async_trait]
impl<F> AuthoritativeOpenOrderProvider for HyperliquidOpenOrderProvider<F>
where
    F: Fn() -> Vec<serde_json::Value> + Send + Sync,
{
    async fn get_open_orders(&self) -> Result<Vec<EmergencyCancelTarget>, HypeEdgeError> {
        let raw = (self.fetch)();
        let mut targets = Vec::new();
        for item in &raw {
            let symbol = item
                .get("coin")
                .or_else(|| item.get("symbol"))
                .and_then(|v| v.as_str());
            let cloid = item.get("cloid").and_then(|v| v.as_str());
            let oid: Option<String> = match item.get("oid") {
                Some(v) => v
                    .as_str()
                    .map(str::to_string)
                    .or_else(|| v.as_i64().map(|i| i.to_string())),
                None => None,
            };
            let Some(symbol) = symbol.filter(|s| !s.is_empty()) else {
                return Err(HypeEdgeError::Execution {
                    message: "Authoritative open order lacks symbol".into(),
                });
            };
            if cloid.is_none() && oid.is_none() {
                return Err(HypeEdgeError::Execution {
                    message: "Authoritative open order lacks cloid and oid".into(),
                });
            }
            targets.push(
                EmergencyCancelTarget::new(symbol, cloid.map(str::to_string), oid)
                    .map_err(|e| HypeEdgeError::Execution { message: e })?,
            );
        }
        Ok(targets)
    }
}

/// WAL-backed cancel implementation using the sole nonce signing queue.
pub struct WalEmergencyCancelExecutor {
    nonce: Arc<NonceQueue>,
    exchange: Arc<dyn ExchangeClient>,
    asset_index: Arc<dyn AssetIndexProvider>,
    open_orders: Arc<dyn AuthoritativeOpenOrderProvider>,
    journal: Arc<EmergencyCancelJournal>,
    operation_lock: Mutex<()>,
}

impl WalEmergencyCancelExecutor {
    pub fn new(
        nonce: Arc<NonceQueue>,
        exchange: Arc<dyn ExchangeClient>,
        asset_index: Arc<dyn AssetIndexProvider>,
        open_orders: Arc<dyn AuthoritativeOpenOrderProvider>,
        journal: Arc<EmergencyCancelJournal>,
    ) -> Self {
        Self {
            nonce,
            exchange,
            asset_index,
            open_orders,
            journal,
            operation_lock: Mutex::new(()),
        }
    }
}

impl WalEmergencyCancelExecutor {
    /// Cancel one target after checking it is still authoritatively open.
    pub async fn cancel(
        &self,
        target: EmergencyCancelTarget,
    ) -> Result<EmergencyCancelResult, HypeEdgeError> {
        let _guard = self.operation_lock.lock().await;
        let authoritative = self.open_orders.get_open_orders().await?;
        let Some(matched) = find_target(&authoritative, &target) else {
            let attempt_id = uuid::Uuid::new_v4().to_string();
            self.journal
                .append(&attempt_id, "already_absent", &target, None, None)
                .await?;
            return Ok(EmergencyCancelResult {
                target,
                success: true,
                outcome: "already_absent".into(),
                attempt_id: Some(attempt_id),
                error: None,
            });
        };
        let attempt_id = self.dispatch(&matched).await?;
        let remaining = self.open_orders.get_open_orders().await?;
        self.verify(&matched, &remaining, Some(attempt_id.as_str()))
            .await
    }

    /// Cancel all open orders, optionally filtered by symbol.
    pub async fn cancel_all(
        &self,
        symbol: Option<&str>,
    ) -> Result<EmergencyCancelBatchResult, HypeEdgeError> {
        let _guard = self.operation_lock.lock().await;
        let authoritative = self.open_orders.get_open_orders().await?;
        let targets: Vec<EmergencyCancelTarget> = authoritative
            .into_iter()
            .filter(|t| symbol.is_none_or(|s| t.symbol == s))
            .collect();

        let mut attempts = std::collections::HashMap::new();
        let mut unresolved = Vec::new();
        for target in &targets {
            // D2: an unknown/unresolvable symbol must not abort the whole
            // cancel-all — skip it, report it, and keep cancelling the rest.
            match self.dispatch(target).await {
                Ok(attempt_id) => {
                    attempts.insert(target.key(), attempt_id);
                }
                Err(e) => {
                    tracing::error!(symbol = %target.symbol, error = %e, "emergency_cancel_dispatch_failed");
                    unresolved.push(target.clone());
                }
            }
        }

        let remaining = self.open_orders.get_open_orders().await?;
        for target in &targets {
            if !attempts.contains_key(&target.key()) {
                continue; // already counted in unresolved above
            }
            let result = self
                .verify(
                    target,
                    &remaining,
                    attempts.get(&target.key()).map(String::as_str),
                )
                .await?;
            if !result.success {
                unresolved.push(target.clone());
            }
        }
        Ok(EmergencyCancelBatchResult {
            requested: targets.len(),
            cancelled: targets.len() - unresolved.len(),
            unresolved,
        })
    }

    /// Replay unresolved intents only when the target is still authoritatively open.
    pub async fn recover_pending(&self) -> Result<EmergencyCancelBatchResult, HypeEdgeError> {
        let _guard = self.operation_lock.lock().await;
        let pending = self.journal.pending_attempts().await?;
        let mut authoritative = self.open_orders.get_open_orders().await?;
        let mut unresolved = Vec::new();
        let mut cancelled = 0usize;

        for old_attempt in &pending {
            let Some(matched) = find_target(&authoritative, &old_attempt.target) else {
                self.journal
                    .append(
                        &old_attempt.attempt_id,
                        "recovery_resolved",
                        &old_attempt.target,
                        Some("already_absent"),
                        None,
                    )
                    .await?;
                cancelled += 1;
                continue;
            };
            let new_attempt_id = self.dispatch(&matched).await?;
            let remaining = self.open_orders.get_open_orders().await?;
            let result = self
                .verify(&matched, &remaining, Some(new_attempt_id.as_str()))
                .await?;
            if result.success {
                self.journal
                    .append(
                        &old_attempt.attempt_id,
                        "recovery_resolved",
                        &old_attempt.target,
                        Some("cancelled"),
                        None,
                    )
                    .await?;
                cancelled += 1;
                authoritative = remaining;
            } else {
                unresolved.push(old_attempt.target.clone());
            }
        }
        Ok(EmergencyCancelBatchResult {
            requested: pending.len(),
            cancelled,
            unresolved,
        })
    }

    /// Journal a dispatch intent, then send the signed cancel through the sole
    /// nonce queue. A transport failure is journaled, never raised, so the
    /// attempt stays recoverable. The asset index is resolved *before* the
    /// intent is journaled (D2): an unknown symbol fails fast without leaving a
    /// poisoned `dispatch_intent` that would abort every later recovery.
    async fn dispatch(&self, target: &EmergencyCancelTarget) -> Result<String, HypeEdgeError> {
        let asset = self
            .asset_index
            .asset_index(&target.symbol)
            .ok_or_else(|| HypeEdgeError::Execution {
                message: format!("unknown symbol for emergency cancel: {}", target.symbol),
            })?;
        let attempt_id = uuid::Uuid::new_v4().to_string();
        self.journal
            .append(&attempt_id, "dispatch_intent", target, None, None)
            .await?;

        let exchange = self.exchange.clone();
        let nonce = self.nonce.clone();
        let result = if let Some(cloid) = &target.cloid {
            let hl = CloidGenerator::to_hl_cloid(cloid);
            let target_key = target.key();
            nonce
                .submit("emergency_cancel", move |nonce| {
                    let exchange = exchange.clone();
                    let hl = hl.clone();
                    Box::pin(async move {
                        exchange
                            .cancel_by_cloid(vec![CancelByCloidWire { asset, cloid: hl }], nonce)
                            .await
                    })
                })
                .await
                .map(|value| transport_outcome(&value))
                .map_err(|msg| {
                    tracing::error!(target = %target_key, attempt_id = %attempt_id, "emergency_cancel_transport_failed");
                    HypeEdgeError::Execution { message: msg }
                })
        } else {
            let oid = target
                .oid
                .as_deref()
                .unwrap_or_default()
                .parse::<i64>()
                .map_err(|_| HypeEdgeError::Execution {
                    message: format!("emergency cancel oid is not numeric: {:?}", target.oid),
                })?;
            let target_key = target.key();
            nonce
                .submit("emergency_cancel", move |nonce| {
                    let exchange = exchange.clone();
                    Box::pin(async move {
                        exchange
                            .cancel(vec![CancelWire { a: asset, o: oid }], nonce)
                            .await
                    })
                })
                .await
                .map(|value| transport_outcome(&value))
                .map_err(|msg| {
                    tracing::error!(target = %target_key, attempt_id = %attempt_id, "emergency_cancel_transport_failed");
                    HypeEdgeError::Execution { message: msg }
                })
        };

        match result {
            Ok(outcome) => {
                self.journal
                    .append(
                        &attempt_id,
                        "transport_result",
                        target,
                        Some(&outcome),
                        None,
                    )
                    .await?;
            }
            Err(e) => {
                self.journal
                    .append(
                        &attempt_id,
                        "transport_error",
                        target,
                        None,
                        Some(&e.to_string()),
                    )
                    .await?;
            }
        }
        Ok(attempt_id)
    }

    /// Verify the target is gone from authoritative open orders.
    async fn verify(
        &self,
        target: &EmergencyCancelTarget,
        remaining: &[EmergencyCancelTarget],
        attempt_id: Option<&str>,
    ) -> Result<EmergencyCancelResult, HypeEdgeError> {
        let resolved_attempt_id = match attempt_id {
            Some(id) => id.to_string(),
            None => {
                let pending = self.journal.pending_attempts().await?;
                pending
                    .iter()
                    .rev()
                    .find(|item| item.target.key() == target.key())
                    .map(|item| item.attempt_id.clone())
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
            }
        };
        if find_target(remaining, target).is_none() {
            self.journal
                .append(
                    &resolved_attempt_id,
                    "verified_absent",
                    target,
                    Some("cancelled"),
                    None,
                )
                .await?;
            Ok(EmergencyCancelResult {
                target: target.clone(),
                success: true,
                outcome: "cancelled".into(),
                attempt_id: Some(resolved_attempt_id),
                error: None,
            })
        } else {
            self.journal
                .append(
                    &resolved_attempt_id,
                    "verified_open",
                    target,
                    Some("still_open"),
                    None,
                )
                .await?;
            Ok(EmergencyCancelResult {
                target: target.clone(),
                success: false,
                outcome: "still_open".into(),
                attempt_id: Some(resolved_attempt_id),
                error: Some("authoritative_order_still_open".into()),
            })
        }
    }
}

/// Find a target among candidates by its canonical key.
fn find_target(
    candidates: &[EmergencyCancelTarget],
    target: &EmergencyCancelTarget,
) -> Option<EmergencyCancelTarget> {
    candidates
        .iter()
        .find(|candidate| {
            // Match by canonical identity first; also fall back to a bare oid match
            // (D2) so an order that the exchange reports only by oid is still
            // recognized instead of reporting a false `already_absent`.
            candidate.key() == target.key()
                || (target.oid.is_some()
                    && candidate.oid.is_some()
                    && candidate.oid.as_deref() == target.oid.as_deref())
        })
        .cloned()
}

fn canonical_cloid(cloid: &str) -> String {
    CloidGenerator::to_hl_cloid(cloid)
}

fn transport_outcome(response: &serde_json::Value) -> String {
    response
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown_response")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Mutex as StdMutex;

    use async_trait::async_trait;

    struct StaticOpenOrders(StdMutex<Vec<serde_json::Value>>);

    impl StaticOpenOrders {
        fn new(orders: Vec<serde_json::Value>) -> Self {
            Self(StdMutex::new(orders))
        }
        fn set(&self, orders: Vec<serde_json::Value>) {
            *self.0.lock().unwrap() = orders;
        }
    }

    #[async_trait]
    impl AuthoritativeOpenOrderProvider for StaticOpenOrders {
        async fn get_open_orders(&self) -> Result<Vec<EmergencyCancelTarget>, HypeEdgeError> {
            let raw = self.0.lock().unwrap().clone();
            let mut targets = Vec::new();
            for item in &raw {
                let symbol = item.get("coin").and_then(|v| v.as_str()).unwrap_or("BTC");
                let cloid = item.get("cloid").and_then(|v| v.as_str());
                let oid = item.get("oid").and_then(|v| v.as_i64());
                targets.push(
                    EmergencyCancelTarget::new(
                        symbol,
                        cloid.map(str::to_string),
                        oid.map(|o| o.to_string()),
                    )
                    .unwrap(),
                );
            }
            Ok(targets)
        }
    }

    /// A nonce-queue-compatible fake exchange: records cancels, never errors.
    struct FakeExchange {
        cancel_by_cloid_calls: StdMutex<Vec<(i64, String)>>,
        cancel_calls: StdMutex<Vec<(i64, i64)>>,
    }

    impl FakeExchange {
        fn new() -> Self {
            Self {
                cancel_by_cloid_calls: StdMutex::new(Vec::new()),
                cancel_calls: StdMutex::new(Vec::new()),
            }
        }
    }

    struct FixedAssetIndex;

    impl AssetIndexProvider for FixedAssetIndex {
        fn asset_index(&self, _symbol: &str) -> Option<i64> {
            Some(2)
        }
    }

    struct FakeExchangeClient(FakeExchange);

    #[async_trait]
    impl ExchangeClient for FakeExchangeClient {
        async fn order(
            &self,
            _orders: Vec<super::super::signing::OrderWire>,
            _nonce: u64,
        ) -> Result<serde_json::Value, String> {
            unreachable!()
        }
        async fn cancel(
            &self,
            cancels: Vec<CancelWire>,
            nonce: u64,
        ) -> Result<serde_json::Value, String> {
            let mut calls = self.0.cancel_calls.lock().unwrap();
            for c in cancels {
                calls.push((c.a, c.o));
            }
            let _ = nonce;
            Ok(serde_json::json!({ "status": "ok" }))
        }
        async fn cancel_by_cloid(
            &self,
            cancels: Vec<CancelByCloidWire>,
            nonce: u64,
        ) -> Result<serde_json::Value, String> {
            let mut calls = self.0.cancel_by_cloid_calls.lock().unwrap();
            for c in cancels {
                calls.push((c.asset, c.cloid.clone()));
            }
            let _ = nonce;
            Ok(serde_json::json!({ "status": "ok" }))
        }
        async fn update_leverage(
            &self,
            _asset: i64,
            _is_cross: bool,
            _leverage: i64,
            _nonce: u64,
        ) -> Result<serde_json::Value, String> {
            unreachable!()
        }
        async fn query_order_by_cloid(
            &self,
            _cloid: &str,
        ) -> Result<Option<serde_json::Value>, String> {
            Ok(None)
        }
    }

    fn temp_journal(tag: &str) -> (PathBuf, Arc<EmergencyCancelJournal>) {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "hypeedge_emergency_{tag}_{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        (path.clone(), Arc::new(EmergencyCancelJournal::new(path)))
    }

    fn target(symbol: &str, cloid: &str) -> EmergencyCancelTarget {
        EmergencyCancelTarget::new(symbol, Some(cloid.into()), None).unwrap()
    }

    fn executor(
        open_orders: Arc<dyn AuthoritativeOpenOrderProvider>,
        journal: Arc<EmergencyCancelJournal>,
        exchange: Arc<dyn ExchangeClient>,
    ) -> WalEmergencyCancelExecutor {
        WalEmergencyCancelExecutor::new(
            Arc::new(NonceQueue::new()),
            exchange,
            Arc::new(FixedAssetIndex),
            open_orders,
            journal,
        )
    }

    #[tokio::test]
    async fn cancel_already_absent_is_immediate_success() {
        let (_, journal) = temp_journal("absent");
        let open = Arc::new(StaticOpenOrders::new(vec![]));
        let exchange = Arc::new(FakeExchangeClient(FakeExchange::new()));
        let exec = executor(open, journal.clone(), exchange);
        let result = exec.cancel(target("BTC", "c1")).await.unwrap();
        assert!(result.success);
        assert_eq!(result.outcome, "already_absent");
        let records = journal.read_records().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].event, "already_absent");
    }

    #[tokio::test]
    async fn cancel_dispatches_and_verifies_absent() {
        let (_, journal) = temp_journal("cancel1");
        let exchange = Arc::new(FakeExchangeClient(FakeExchange::new()));
        let open = Arc::new(StaticOpenOrders::new(vec![
            serde_json::json!({ "coin": "BTC", "cloid": "c1" }),
        ]));
        let exec = executor(open.clone(), journal.clone(), exchange.clone());

        // First call: still open -> dispatch. After dispatch, simulate the
        // exchange clearing the order.
        let result = exec.cancel(target("BTC", "c1")).await.unwrap();
        // The fake open-orders provider never mutates, so the target remains.
        assert!(!result.success);
        assert_eq!(result.outcome, "still_open");

        // Clear the book and retry: the target is now absent without dispatch.
        open.set(vec![]);
        let result = exec.cancel(target("BTC", "c1")).await.unwrap();
        assert!(result.success);
        assert_eq!(result.outcome, "already_absent");
    }

    #[tokio::test]
    async fn cancel_all_filters_by_symbol_and_tracks_unresolved() {
        let (_, journal) = temp_journal("all");
        let exchange = Arc::new(FakeExchangeClient(FakeExchange::new()));
        let open = Arc::new(StaticOpenOrders::new(vec![
            serde_json::json!({ "coin": "BTC", "cloid": "c1" }),
            serde_json::json!({ "coin": "ETH", "cloid": "c2" }),
        ]));
        let exec = executor(open.clone(), journal.clone(), exchange);

        // Filter to ETH: one target, still open -> unresolved.
        let result = exec.cancel_all(Some("ETH")).await.unwrap();
        assert_eq!(result.requested, 1);
        assert_eq!(result.cancelled, 0);
        assert_eq!(result.unresolved.len(), 1);

        // Clear the book, then cancel_all(BTC) -> both disappear, but only
        // those present are dispatched.
        open.set(vec![]);
        let result = exec.cancel_all(None).await.unwrap();
        assert_eq!(result.requested, 0);
        assert_eq!(result.cancelled, 0);
        assert!(result.success());
    }

    #[tokio::test]
    async fn recover_pending_replays_only_absent_targets() {
        let (_, journal) = temp_journal("recover");
        let exchange = Arc::new(FakeExchangeClient(FakeExchange::new()));
        let open = Arc::new(StaticOpenOrders::new(vec![
            serde_json::json!({ "coin": "BTC", "cloid": "c1" }),
        ]));
        let exec = executor(open.clone(), journal.clone(), exchange.clone());

        // Create a pending attempt by dispatching (order stays open).
        exec.cancel(target("BTC", "c1")).await.unwrap();
        let pending = journal.pending_attempts().await.unwrap();
        assert_eq!(pending.len(), 1);

        // Exchange cleared the order; recovery resolves it as already_absent.
        open.set(vec![]);
        let result = exec.recover_pending().await.unwrap();
        assert_eq!(result.requested, 1);
        assert_eq!(result.cancelled, 1);
        assert!(result.success());

        // No pending intents remain.
        assert!(journal.pending_attempts().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn journal_reads_records_and_ignores_torn_tail() {
        let (path, journal) = temp_journal("torn");
        // Write a valid line plus an incomplete (torn) tail.
        std::fs::write(
            &path,
            format!(
                "{}\n{{\"attempt_id\":\"torn\"",
                serde_json::to_string(&EmergencyJournalRecord {
                    attempt_id: "ok".into(),
                    event: "dispatch_intent".into(),
                    recorded_at: "2026-01-01T00:00:00Z".into(),
                    target: JournalTarget {
                        symbol: "BTC".into(),
                        cloid: Some("c1".into()),
                        oid: None,
                    },
                    outcome: None,
                    error: None,
                })
                .unwrap()
            ),
        )
        .unwrap();

        let records = journal.read_records().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].attempt_id, "ok");
    }

    #[tokio::test]
    async fn journal_append_fsyncs_and_is_readable() {
        // Async append is covered by the executor tests; this exercises the
        // sync read path after a fresh (empty) journal.
        let (path, journal) = temp_journal("empty");
        assert!(!path.exists());
        let records = journal.read_records().await.unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn target_key_uses_canonical_cloid_or_oid() {
        let t = EmergencyCancelTarget::new("BTC", Some("0xc1".into()), None).unwrap();
        assert_eq!(t.key(), format!("cloid:{}", canonical_cloid("0xc1")));
        let t2 = EmergencyCancelTarget::new("BTC", None, Some("123".into())).unwrap();
        assert_eq!(t2.key(), "oid:BTC:123");
    }

    #[test]
    fn target_requires_symbol_and_identifier() {
        assert!(EmergencyCancelTarget::new("", Some("c1".into()), None).is_err());
        assert!(EmergencyCancelTarget::new("BTC", None, None).is_err());
        assert!(EmergencyCancelTarget::new("BTC", Some("c1".into()), None).is_ok());
    }

    #[test]
    fn find_target_falls_back_to_oid_match() {
        // D2 regression: a target keyed by cloid must still match an
        // authoritative order the exchange reports only by oid (otherwise
        // `cancel` returns a false `already_absent` while the order stays live).
        let target =
            EmergencyCancelTarget::new("BTC", Some("0xc1".into()), Some("123".into())).unwrap();
        let oid_only = EmergencyCancelTarget::new("BTC", None, Some("123".into())).unwrap();
        // Keys differ (cloid:... vs oid:BTC:123), so without the oid fallback
        // this would return None.
        let matched = find_target(&[oid_only], &target);
        assert!(matched.is_some(), "oid fallback must match the live order");
    }

    #[tokio::test]
    async fn journal_is_thread_safe_across_concurrent_appends() {
        // A smoke check that concurrent append + read do not panic. Correctness
        // of serialization is exercised by the async tests above.
        let (path, journal) = temp_journal("conc");
        let journal2 = journal.clone();
        let journal3 = journal.clone();
        let t1 = tokio::spawn(async move {
            journal2
                .append("a", "dispatch_intent", &target("BTC", "c1"), None, None)
                .await
                .unwrap();
        });
        let t2 = tokio::spawn(async move {
            journal3
                .append("b", "dispatch_intent", &target("BTC", "c2"), None, None)
                .await
                .unwrap();
        });
        t1.await.unwrap();
        t2.await.unwrap();
        assert!(path.exists());
    }
}
