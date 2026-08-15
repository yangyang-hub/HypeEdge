//! Layered account freshness and adaptive clearinghouse-state polling, port of
//! `src/hypeedge/account/health.py`.
//!
//! Market-making safety cannot collapse account health into one timestamp. The
//! authenticated inventory stream, clearinghouse REST snapshot, user-stream
//! connection, and full reconciliation each have different update rates and
//! failure semantics. This module keeps those facts separate and fails closed
//! when any required dimension is unknown, unhealthy, or stale.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use hypeedge_domain::decimal::{Decimal, Price, Size, Usd};
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::models::{AccountState, Position, SpotBalance};

use crate::account::tracker::AccountTracker;

/// Independent account facts required before increasing risk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AccountHealthDimension {
    Inventory,
    Clearinghouse,
    UserStream,
    Reconciliation,
}

impl AccountHealthDimension {
    pub fn as_str(self) -> &'static str {
        match self {
            AccountHealthDimension::Inventory => "inventory",
            AccountHealthDimension::Clearinghouse => "clearinghouse",
            AccountHealthDimension::UserStream => "user_stream",
            AccountHealthDimension::Reconciliation => "reconciliation",
        }
    }

    pub fn all() -> [AccountHealthDimension; 4] {
        [
            AccountHealthDimension::Inventory,
            AccountHealthDimension::Clearinghouse,
            AccountHealthDimension::UserStream,
            AccountHealthDimension::Reconciliation,
        ]
    }
}

/// Evaluated state of one account-health dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreshnessStatus {
    Unknown,
    Fresh,
    Stale,
    Unhealthy,
}

impl FreshnessStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            FreshnessStatus::Unknown => "unknown",
            FreshnessStatus::Fresh => "fresh",
            FreshnessStatus::Stale => "stale",
            FreshnessStatus::Unhealthy => "unhealthy",
        }
    }
}

/// Maximum ages for account facts with conservative production defaults.
#[derive(Debug, Clone, Copy)]
pub struct AccountFreshnessThresholds {
    pub inventory: Duration,
    pub clearinghouse: Duration,
    pub user_stream: Duration,
    pub reconciliation: Duration,
    pub max_future_skew: Duration,
}

impl Default for AccountFreshnessThresholds {
    fn default() -> Self {
        Self {
            inventory: Duration::from_secs(5),
            clearinghouse: Duration::from_secs(6),
            user_stream: Duration::from_secs(5),
            reconciliation: Duration::from_secs(600),
            max_future_skew: Duration::from_secs(1),
        }
    }
}

impl AccountFreshnessThresholds {
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("inventory", self.inventory),
            ("clearinghouse", self.clearinghouse),
            ("user_stream", self.user_stream),
            ("reconciliation", self.reconciliation),
        ] {
            if value.is_zero() {
                return Err(format!("{name} freshness threshold must be positive"));
            }
        }
        if self.max_future_skew.is_zero() {
            return Err("max_future_skew must be non-negative".into());
        }
        Ok(())
    }

    pub fn for_dimension(&self, dimension: AccountHealthDimension) -> Duration {
        match dimension {
            AccountHealthDimension::Inventory => self.inventory,
            AccountHealthDimension::Clearinghouse => self.clearinghouse,
            AccountHealthDimension::UserStream => self.user_stream,
            AccountHealthDimension::Reconciliation => self.reconciliation,
        }
    }
}

/// Last locally received observation for one health dimension.
#[derive(Debug, Clone, PartialEq)]
pub struct FreshnessObservation {
    pub dimension: AccountHealthDimension,
    pub observed_at: Option<DateTime<Utc>>,
    pub healthy: bool,
    pub reason: Option<String>,
}

impl FreshnessObservation {
    fn unobserved(dimension: AccountHealthDimension) -> Self {
        Self {
            dimension,
            observed_at: None,
            healthy: false,
            reason: Some("not_observed".into()),
        }
    }
}

/// Freshness evaluation at a specific point in time.
#[derive(Debug, Clone, PartialEq)]
pub struct FreshnessResult {
    pub dimension: AccountHealthDimension,
    pub status: FreshnessStatus,
    pub observed_at: Option<DateTime<Utc>>,
    pub age_seconds: Option<f64>,
    pub max_age_seconds: f64,
    pub reason: Option<String>,
}

impl FreshnessResult {
    pub fn is_fresh(&self) -> bool {
        self.status == FreshnessStatus::Fresh
    }
}

/// Immutable, point-in-time evaluation of all account safety facts.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountHealthSnapshot {
    pub evaluated_at: DateTime<Utc>,
    pub inventory: FreshnessResult,
    pub clearinghouse: FreshnessResult,
    pub user_stream: FreshnessResult,
    pub reconciliation: FreshnessResult,
}

impl AccountHealthSnapshot {
    pub fn dimensions(&self) -> [&FreshnessResult; 4] {
        [
            &self.inventory,
            &self.clearinghouse,
            &self.user_stream,
            &self.reconciliation,
        ]
    }

    /// M-RK1: the UserStream dimension does not participate in the risk gate
    /// when it has never been observed (no authenticated user-stream is wired,
    /// so "no stream" must not permanently block entry). An *observed* stale or
    /// unhealthy UserStream still blocks.
    fn dimension_participates(result: &FreshnessResult) -> bool {
        !(result.dimension == AccountHealthDimension::UserStream
            && result.status == FreshnessStatus::Unknown)
    }

    /// Every participating dimension must be fresh to allow a risk increase.
    pub fn allows_risk_increase(&self) -> bool {
        self.dimensions()
            .iter()
            .all(|r| !Self::dimension_participates(r) || r.is_fresh())
    }

    /// Any missing critical account fact requires maker quotes to be removed.
    pub fn requires_cancel(&self) -> bool {
        !self.allows_risk_increase()
    }

    /// Stable reasons for each non-fresh participating dimension, e.g.
    /// `inventory:stale`. Non-participating (never-observed UserStream)
    /// dimensions are excluded for consistency with [`Self::allows_risk_increase`].
    pub fn blocking_reasons(&self) -> Vec<String> {
        self.dimensions()
            .iter()
            .filter(|r| Self::dimension_participates(r) && !r.is_fresh())
            .map(|r| {
                format!(
                    "{}:{}",
                    r.dimension.as_str(),
                    r.reason.as_deref().unwrap_or(r.status.as_str())
                )
            })
            .collect()
    }
}

/// Read boundary used by risk and dispatch-time account freshness gates.
pub trait AccountHealthProvider: Send + Sync {
    fn get_account_health(&self, now: Option<DateTime<Utc>>) -> AccountHealthSnapshot;
}

/// Write boundary for the stream, poller, and reconciler owners.
pub trait MutableAccountHealthProvider: AccountHealthProvider {
    fn record_success(&self, dimension: AccountHealthDimension, observed_at: Option<DateTime<Utc>>);
    fn record_failure(
        &self,
        dimension: AccountHealthDimension,
        reason: &str,
        observed_at: Option<DateTime<Utc>>,
    ) -> Result<(), String>;
}

/// In-memory account health projection with no implicit timestamp refresh.
pub struct LayeredAccountHealthProvider {
    thresholds: AccountFreshnessThresholds,
    observations: std::sync::Mutex<HashMap<AccountHealthDimension, FreshnessObservation>>,
}

impl Default for LayeredAccountHealthProvider {
    fn default() -> Self {
        Self::new(AccountFreshnessThresholds::default())
    }
}

impl LayeredAccountHealthProvider {
    pub fn new(thresholds: AccountFreshnessThresholds) -> Self {
        let observations = AccountHealthDimension::all()
            .iter()
            .map(|d| (*d, FreshnessObservation::unobserved(*d)))
            .collect();
        Self {
            thresholds,
            observations: std::sync::Mutex::new(observations),
        }
    }

    pub fn thresholds(&self) -> AccountFreshnessThresholds {
        self.thresholds
    }
}

impl AccountHealthProvider for LayeredAccountHealthProvider {
    fn get_account_health(&self, now: Option<DateTime<Utc>>) -> AccountHealthSnapshot {
        let now = now.unwrap_or_else(Utc::now);
        let observations = self.observations.lock().unwrap();
        let mut by_dimension: HashMap<AccountHealthDimension, FreshnessResult> = HashMap::new();
        for dimension in AccountHealthDimension::all() {
            let observation = observations
                .get(&dimension)
                .cloned()
                .unwrap_or_else(|| FreshnessObservation::unobserved(dimension));
            by_dimension.insert(dimension, evaluate(&observation, now, &self.thresholds));
        }
        AccountHealthSnapshot {
            evaluated_at: now,
            inventory: by_dimension[&AccountHealthDimension::Inventory].clone(),
            clearinghouse: by_dimension[&AccountHealthDimension::Clearinghouse].clone(),
            user_stream: by_dimension[&AccountHealthDimension::UserStream].clone(),
            reconciliation: by_dimension[&AccountHealthDimension::Reconciliation].clone(),
        }
    }
}

impl MutableAccountHealthProvider for LayeredAccountHealthProvider {
    fn record_success(
        &self,
        dimension: AccountHealthDimension,
        observed_at: Option<DateTime<Utc>>,
    ) {
        let timestamp = observed_at.unwrap_or_else(Utc::now);
        let mut observations = self.observations.lock().unwrap();
        observations.insert(
            dimension,
            FreshnessObservation {
                dimension,
                observed_at: Some(timestamp),
                healthy: true,
                reason: None,
            },
        );
    }

    fn record_failure(
        &self,
        dimension: AccountHealthDimension,
        reason: &str,
        observed_at: Option<DateTime<Utc>>,
    ) -> Result<(), String> {
        if reason.is_empty() {
            return Err("account health failure reason must not be empty".into());
        }
        let timestamp = observed_at.unwrap_or_else(Utc::now);
        let mut observations = self.observations.lock().unwrap();
        observations.insert(
            dimension,
            FreshnessObservation {
                dimension,
                observed_at: Some(timestamp),
                healthy: false,
                reason: Some(reason.to_string()),
            },
        );
        Ok(())
    }
}

fn evaluate(
    observation: &FreshnessObservation,
    now: DateTime<Utc>,
    thresholds: &AccountFreshnessThresholds,
) -> FreshnessResult {
    let max_age_seconds = thresholds
        .for_dimension(observation.dimension)
        .as_secs_f64();
    let Some(observed_at) = observation.observed_at else {
        return FreshnessResult {
            dimension: observation.dimension,
            status: FreshnessStatus::Unknown,
            observed_at: None,
            age_seconds: None,
            max_age_seconds,
            reason: observation
                .reason
                .clone()
                .or_else(|| Some("not_observed".into())),
        };
    };

    let age = now - observed_at;
    let age_seconds = age.num_milliseconds() as f64 / 1000.0;
    let future_skew = thresholds.max_future_skew.as_secs_f64();
    if age_seconds < -future_skew {
        return FreshnessResult {
            dimension: observation.dimension,
            status: FreshnessStatus::Unhealthy,
            observed_at: Some(observed_at),
            age_seconds: Some(age_seconds),
            max_age_seconds,
            reason: Some("observed_at_in_future".into()),
        };
    }
    if !observation.healthy {
        return FreshnessResult {
            dimension: observation.dimension,
            status: FreshnessStatus::Unhealthy,
            observed_at: Some(observed_at),
            age_seconds: Some(age_seconds.max(0.0)),
            max_age_seconds,
            reason: observation
                .reason
                .clone()
                .or_else(|| Some("source_unhealthy".into())),
        };
    }
    if age_seconds > max_age_seconds {
        return FreshnessResult {
            dimension: observation.dimension,
            status: FreshnessStatus::Stale,
            observed_at: Some(observed_at),
            age_seconds: Some(age_seconds),
            max_age_seconds,
            reason: Some("observation_stale".into()),
        };
    }
    FreshnessResult {
        dimension: observation.dimension,
        status: FreshnessStatus::Fresh,
        observed_at: Some(observed_at),
        age_seconds: Some(age_seconds.max(0.0)),
        max_age_seconds,
        reason: None,
    }
}

/// One authoritative clearinghouse-state response.
#[derive(Debug, Clone, PartialEq)]
pub struct PolledAccountSnapshot {
    pub account_state: AccountState,
    pub positions: Vec<Position>,
    pub received_at: DateTime<Utc>,
    pub spot_balances: Vec<SpotBalance>,
}

/// Durable sink for authoritative clearinghouse snapshots (Postgres).
#[async_trait::async_trait]
pub trait AccountSnapshotSink: Send + Sync {
    async fn persist(&self, snapshot: &PolledAccountSnapshot) -> Result<(), String>;
}

/// Async clearinghouse-state source used by [`AccountStatePoller`].
#[async_trait::async_trait]
pub trait AccountStateSource: Send + Sync {
    async fn fetch_account_state(&self) -> Result<PolledAccountSnapshot, HypeEdgeError>;
}

/// Risk-proximity evaluator: `true` means poll at the near-risk cadence.
pub type RiskProximityEvaluator = Box<dyn Fn(&PolledAccountSnapshot) -> bool + Send + Sync>;

/// Async health-failure callback (e.g. reduce quotes).
pub type HealthFailureCallback =
    Box<dyn Fn(&str) -> futures::future::BoxFuture<'_, ()> + Send + Sync>;

/// Poll clearinghouse state at an adaptive, rate-budget-friendly cadence.
pub struct AccountStatePoller {
    source: Arc<dyn AccountStateSource>,
    tracker: Arc<AccountTracker>,
    health: Arc<dyn MutableAccountHealthProvider>,
    snapshot_sink: Option<Arc<dyn AccountSnapshotSink>>,
    normal_interval: Duration,
    near_risk_interval: Duration,
    risk_proximity_evaluator: RiskProximityEvaluator,
    on_health_failure: Option<HealthFailureCallback>,
    running: std::sync::atomic::AtomicBool,
    stop: tokio::sync::Notify,
}

impl AccountStatePoller {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: Arc<dyn AccountStateSource>,
        tracker: Arc<AccountTracker>,
        health: Arc<dyn MutableAccountHealthProvider>,
        snapshot_sink: Option<Arc<dyn AccountSnapshotSink>>,
        normal_interval_seconds: f64,
        near_risk_interval_seconds: f64,
        risk_proximity_evaluator: Option<RiskProximityEvaluator>,
        on_health_failure: Option<HealthFailureCallback>,
    ) -> Result<Self, String> {
        if !(2.0..=5.0).contains(&normal_interval_seconds) {
            return Err("normal account poll interval must be between 2 and 5 seconds".into());
        }
        if !(0.5..=2.0).contains(&near_risk_interval_seconds) {
            return Err("near-risk account poll interval must be between 0.5 and 2 seconds".into());
        }
        if near_risk_interval_seconds >= normal_interval_seconds {
            return Err(
                "near-risk account poll interval must be lower than normal interval".into(),
            );
        }
        Ok(Self {
            source,
            tracker,
            health,
            snapshot_sink,
            normal_interval: Duration::from_secs_f64(normal_interval_seconds),
            near_risk_interval: Duration::from_secs_f64(near_risk_interval_seconds),
            risk_proximity_evaluator: risk_proximity_evaluator
                .unwrap_or_else(|| Box::new(default_near_risk)),
            on_health_failure,
            running: std::sync::atomic::AtomicBool::new(false),
            stop: tokio::sync::Notify::new(),
        })
    }

    pub fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Fetch and apply one snapshot; return the next adaptive interval.
    pub async fn poll_once(&self) -> Duration {
        match self.source.fetch_account_state().await {
            Ok(snapshot) => {
                let received_at = snapshot.received_at;
                self.apply_snapshot(&snapshot);
                if let Some(sink) = &self.snapshot_sink
                    && let Err(e) = sink.persist(&snapshot).await
                {
                    tracing::warn!(error = %e, "account_snapshot_persist_failed");
                }
                self.health
                    .record_success(AccountHealthDimension::Clearinghouse, Some(received_at));
                self.health
                    .record_success(AccountHealthDimension::Inventory, Some(received_at));
                let near_risk = (self.risk_proximity_evaluator)(&snapshot);
                let interval = if near_risk {
                    self.near_risk_interval
                } else {
                    self.normal_interval
                };
                tracing::debug!(
                    equity = ?snapshot.account_state.equity,
                    positions = snapshot.positions.len(),
                    near_risk,
                    interval_seconds = interval.as_secs_f64(),
                    "account_state_poll_succeeded"
                );
                interval
            }
            Err(e) => {
                let reason = format!("clearinghouse_poll_failed:{e}");
                let _ = self.health.record_failure(
                    AccountHealthDimension::Clearinghouse,
                    &reason,
                    None,
                );
                tracing::warn!(reason = %reason, "account_state_poll_failed");
                if let Some(callback) = &self.on_health_failure {
                    callback(&reason).await;
                }
                self.near_risk_interval
            }
        }
    }

    /// Poll immediately, then sleep interruptibly until stopped.
    pub async fn run(&self) -> Result<(), String> {
        if self.is_running() {
            return Err("account state poller is already running".into());
        }
        self.running
            .store(true, std::sync::atomic::Ordering::Relaxed);
        tracing::info!("account_state_poller_started");
        loop {
            let interval = self.poll_once().await;
            // Interruptible sleep: stop() wakes the notifier.
            tokio::select! {
                _ = self.stop.notified() => break,
                _ = tokio::time::sleep(interval) => {}
            }
        }
        self.running
            .store(false, std::sync::atomic::Ordering::Relaxed);
        tracing::info!("account_state_poller_stopped");
        Ok(())
    }

    pub fn stop(&self) {
        self.stop.notify_one();
    }

    fn apply_snapshot(&self, snapshot: &PolledAccountSnapshot) {
        self.tracker.update_account_state(&snapshot.account_state);
        let current_symbols: Vec<String> = self
            .tracker
            .get_all_positions()
            .iter()
            .map(|p| p.symbol.clone())
            .collect();
        let mut exchange_symbols = std::collections::HashSet::new();
        for position in &snapshot.positions {
            exchange_symbols.insert(position.symbol.clone());
            if position.is_flat() {
                self.tracker.remove_position(&position.symbol);
            } else {
                self.tracker
                    .update_position_from_exchange(&position.symbol, position.clone());
            }
        }
        for symbol in current_symbols {
            if !exchange_symbols.contains(&symbol) {
                self.tracker.remove_position(&symbol);
            }
        }
        self.tracker
            .update_spot_balances(&snapshot.spot_balances, snapshot.received_at);
    }
}

/// Default near-risk heuristic: thin available equity ratio or mark price
/// within 10% of the liquidation price.
fn default_near_risk(snapshot: &PolledAccountSnapshot) -> bool {
    let equity = snapshot.account_state.equity.inner();
    if equity <= Decimal::ZERO {
        return true;
    }
    let available_ratio = snapshot.account_state.available_balance.inner().div(equity);
    if available_ratio.to_string().parse::<f64>().unwrap_or(0.0) <= 0.25 {
        return true;
    }
    for position in &snapshot.positions {
        let (Some(mark), Some(liquidation)) = (position.mark_price, position.liquidation_price)
        else {
            continue;
        };
        let mark = mark.inner();
        if mark <= Decimal::ZERO {
            continue;
        }
        let distance = (mark - liquidation.inner()).abs().div(mark);
        if distance.to_string().parse::<f64>().unwrap_or(0.0) <= 0.10 {
            return true;
        }
    }
    false
}

/// The narrow REST clearinghouse-state boundary used by the account-state
/// adapter (implemented by the exchange client / app wiring).
#[async_trait::async_trait]
pub trait ClearinghouseRestClient: Send + Sync {
    async fn get_clearinghouse_state(&self, user: &str)
    -> Result<serde_json::Value, HypeEdgeError>;
    async fn get_spot_user_state(&self, user: &str) -> Result<serde_json::Value, HypeEdgeError>;
}

/// Parse Hyperliquid clearinghouse state without using the signing SDK.
pub struct RestAccountStateSource {
    client: Arc<dyn ClearinghouseRestClient>,
    account_address: String,
    tracker: Arc<AccountTracker>,
}

impl RestAccountStateSource {
    pub fn new(
        client: Arc<dyn ClearinghouseRestClient>,
        account_address: &str,
        tracker: Arc<AccountTracker>,
    ) -> Result<Self, String> {
        if account_address.is_empty() {
            return Err("account_address must not be empty".into());
        }
        Ok(Self {
            client,
            account_address: account_address.to_string(),
            tracker,
        })
    }

    async fn fetch_raw(&self) -> Result<(serde_json::Value, serde_json::Value), HypeEdgeError> {
        let raw_fut = self.client.get_clearinghouse_state(&self.account_address);
        let spot_fut = self.client.get_spot_user_state(&self.account_address);
        let (raw, spot_raw) = tokio::join!(raw_fut, spot_fut);
        Ok((raw?, spot_raw?))
    }
}

#[async_trait::async_trait]
impl AccountStateSource for RestAccountStateSource {
    async fn fetch_account_state(&self) -> Result<PolledAccountSnapshot, HypeEdgeError> {
        let (raw, spot_raw) = self.fetch_raw().await?;
        let margin_summary = raw.get("marginSummary").and_then(|v| v.as_object());
        let asset_positions = raw.get("assetPositions").and_then(|v| v.as_array());
        let (Some(margin_summary), Some(asset_positions)) = (margin_summary, asset_positions)
        else {
            return Err(HypeEdgeError::MarketData(
                "invalid_clearinghouse_state_response".into(),
            ));
        };

        let account_value = as_float(margin_summary.get("accountValue"), "accountValue")?;
        let available = as_float(
            raw.get("withdrawable")
                .or_else(|| margin_summary.get("totalMarginAvailable")),
            "withdrawable",
        )?;
        let margin_used_value = serde_json::json!((account_value - available).max(0.0));
        let margin_used = as_float(
            margin_summary
                .get("totalMarginUsed")
                .or(Some(&margin_used_value)),
            "totalMarginUsed",
        )?;

        let mut positions = Vec::new();
        for item in asset_positions {
            positions.push(parse_position(item)?);
        }
        let spot_balances = parse_spot_balances(&spot_raw, &self.account_address)?;
        let unrealized: f64 = positions
            .iter()
            .map(|p| {
                p.unrealized_pnl
                    .map(|u| u.to_string().parse::<f64>().unwrap_or(0.0))
                    .unwrap_or(0.0)
            })
            .sum();
        let sub_account = self.account_address.to_lowercase();
        let peak = self.tracker.peak_equity().inner();
        let state = AccountState {
            equity: Usd::new(Decimal::from_f64(account_value).unwrap_or_default()),
            available_balance: Usd::new(Decimal::from_f64(available).unwrap_or_default()),
            total_margin_used: Usd::new(Decimal::from_f64(margin_used).unwrap_or_default()),
            total_unrealized_pnl: Usd::new(Decimal::from_f64(unrealized).unwrap_or_default()),
            peak_equity: Usd::new(peak.max(Decimal::from_f64(account_value).unwrap_or_default())),
            sub_account: Some(sub_account),
        };
        let received_at = Utc::now();
        let normalized_spot = spot_balances
            .into_iter()
            .map(|mut b| {
                b.updated_at = received_at;
                b
            })
            .collect();
        Ok(PolledAccountSnapshot {
            account_state: state,
            positions,
            received_at,
            spot_balances: normalized_spot,
        })
    }
}

fn parse_spot_balances(
    raw: &serde_json::Value,
    account_address: &str,
) -> Result<Vec<SpotBalance>, HypeEdgeError> {
    let Some(balances) = raw.get("balances").and_then(|v| v.as_array()) else {
        return Err(HypeEdgeError::MarketData(
            "invalid_spot_clearinghouse_state_response".into(),
        ));
    };
    let mut out = Vec::new();
    for item in balances {
        let token = item
            .get("coin")
            .or_else(|| item.get("token"))
            .and_then(|v| v.as_str());
        let Some(token) = token.filter(|t| !t.is_empty()) else {
            return Err(HypeEdgeError::MarketData(
                "spot_balance_missing_token".into(),
            ));
        };
        let total = as_float(item.get("total"), "spot.total")?;
        let hold = as_float(item.get("hold"), "spot.hold")?;
        let entry_ntl = as_float(item.get("entryNtl"), "spot.entryNtl")?;
        out.push(SpotBalance {
            token: token.to_string(),
            total: Size::new(Decimal::from_f64(total).unwrap_or_default()),
            hold: Size::new(Decimal::from_f64(hold).unwrap_or_default()),
            entry_ntl: Usd::new(Decimal::from_f64(entry_ntl).unwrap_or_default()),
            sub_account: Some(account_address.to_lowercase()),
            updated_at: Utc::now(),
        });
    }
    Ok(out)
}

fn parse_position(item: &serde_json::Value) -> Result<Position, HypeEdgeError> {
    let raw = item.get("position").and_then(|v| v.as_object());
    let Some(raw) = raw else {
        return Err(HypeEdgeError::MarketData("invalid_asset_position".into()));
    };
    let coin = raw.get("coin").and_then(|v| v.as_str());
    let Some(coin) = coin.filter(|c| !c.is_empty()) else {
        return Err(HypeEdgeError::MarketData(
            "asset_position_missing_coin".into(),
        ));
    };
    let size = as_float(raw.get("szi"), "szi")?;
    let position_value = as_float(raw.get("positionValue"), "positionValue")?.abs();
    let mark_price = if size != 0.0 {
        Some(Price::new(
            Decimal::from_f64(position_value / size.abs()).unwrap_or_default(),
        ))
    } else {
        None
    };
    let leverage_raw = raw.get("leverage").and_then(|v| v.as_object());
    let leverage_value = leverage_raw
        .and_then(|l| l.get("value"))
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0);
    Ok(Position {
        symbol: coin.to_string(),
        size: Size::new(Decimal::from_f64(size).unwrap_or_default()),
        entry_price: optional_price(raw.get("entryPx"))?,
        mark_price,
        unrealized_pnl: Some(Usd::new(
            Decimal::from_f64(as_float(raw.get("unrealizedPnl"), "unrealizedPnl")?)
                .unwrap_or_default(),
        )),
        leverage: (leverage_value.max(1.0)) as u32,
        liquidation_price: optional_price(raw.get("liquidationPx"))?,
        sub_account: None,
        strategy_id: None,
    })
}

fn optional_price(value: Option<&serde_json::Value>) -> Result<Option<Price>, HypeEdgeError> {
    match value {
        Some(v) if !v.is_null() && v.as_str() != Some("") => {
            let f = as_float(Some(v), "price")?;
            Ok(Some(Price::new(Decimal::from_f64(f).unwrap_or_default())))
        }
        _ => Ok(None),
    }
}

fn as_float(value: Option<&serde_json::Value>, field: &str) -> Result<f64, HypeEdgeError> {
    value
        .and_then(|v| v.as_f64())
        .or_else(|| {
            value
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
        })
        .ok_or_else(|| HypeEdgeError::MarketData(format!("invalid numeric field: {field}")))
}

/// A convenience alias so `AccountTracker`-owning callers can share the
/// poller's snapshot application without duplicating the position logic.
pub type SnapshotApplicator = fn(&AccountTracker, &PolledAccountSnapshot);

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    #[test]
    fn thresholds_validate_positive() {
        assert!(AccountFreshnessThresholds::default().validate().is_ok());
        let bad = AccountFreshnessThresholds {
            inventory: Duration::ZERO,
            ..AccountFreshnessThresholds::default()
        };
        assert!(bad.validate().is_err());
    }

    #[test]
    fn unobserved_dimension_is_unknown() {
        let provider = LayeredAccountHealthProvider::default();
        let snapshot = provider.get_account_health(Some(at(100)));
        assert!(!snapshot.allows_risk_increase());
        assert!(snapshot.requires_cancel());
        assert_eq!(snapshot.inventory.status, FreshnessStatus::Unknown);
        assert_eq!(snapshot.inventory.reason.as_deref(), Some("not_observed"));
        let reasons = snapshot.blocking_reasons();
        assert!(reasons.iter().any(|r| r.starts_with("inventory:")));
    }

    #[test]
    fn fresh_success_allows_risk_increase() {
        let provider = LayeredAccountHealthProvider::default();
        for dimension in AccountHealthDimension::all() {
            provider.record_success(dimension, Some(at(96)));
        }
        let snapshot = provider.get_account_health(Some(at(100)));
        assert!(snapshot.allows_risk_increase());
        assert!(!snapshot.requires_cancel());
        assert!(snapshot.blocking_reasons().is_empty());
        assert!(snapshot.inventory.is_fresh());
    }

    #[test]
    fn stale_observation_blocks_risk() {
        let provider = LayeredAccountHealthProvider::default();
        provider.record_success(AccountHealthDimension::Clearinghouse, Some(at(50)));
        let snapshot = provider.get_account_health(Some(at(100)));
        // clearinghouse threshold is 6s; age 50s => stale.
        assert_eq!(snapshot.clearinghouse.status, FreshnessStatus::Stale);
        assert!(!snapshot.allows_risk_increase());
    }

    #[test]
    fn failure_records_unhealthy() {
        let provider = LayeredAccountHealthProvider::default();
        provider
            .record_failure(
                AccountHealthDimension::Inventory,
                "source_boom",
                Some(at(90)),
            )
            .unwrap();
        let snapshot = provider.get_account_health(Some(at(100)));
        assert_eq!(snapshot.inventory.status, FreshnessStatus::Unhealthy);
        assert_eq!(snapshot.inventory.reason.as_deref(), Some("source_boom"));
    }

    #[test]
    fn empty_failure_reason_rejected() {
        let provider = LayeredAccountHealthProvider::default();
        assert!(
            provider
                .record_failure(AccountHealthDimension::Inventory, "", None)
                .is_err()
        );
    }

    #[test]
    fn future_observation_is_unhealthy() {
        let provider = LayeredAccountHealthProvider::default();
        provider.record_success(AccountHealthDimension::Inventory, Some(at(1000)));
        let snapshot = provider.get_account_health(Some(at(100)));
        assert_eq!(snapshot.inventory.status, FreshnessStatus::Unhealthy);
        assert_eq!(
            snapshot.inventory.reason.as_deref(),
            Some("observed_at_in_future")
        );
    }

    #[test]
    fn poller_validates_intervals() {
        let source: Arc<dyn AccountStateSource> = Arc::new(FailSource);
        let tracker = Arc::new(AccountTracker::new());
        let health: Arc<dyn MutableAccountHealthProvider> =
            Arc::new(LayeredAccountHealthProvider::default());
        assert!(
            AccountStatePoller::new(
                source.clone(),
                tracker.clone(),
                health.clone(),
                None,
                6.0,
                1.0,
                None,
                None
            )
            .is_err()
        );
        assert!(
            AccountStatePoller::new(
                source.clone(),
                tracker.clone(),
                health.clone(),
                None,
                3.0,
                3.0,
                None,
                None
            )
            .is_err()
        );
        assert!(
            AccountStatePoller::new(source, tracker, health, None, 3.0, 1.0, None, None).is_ok()
        );
    }

    struct FailSource;

    #[async_trait::async_trait]
    impl AccountStateSource for FailSource {
        async fn fetch_account_state(&self) -> Result<PolledAccountSnapshot, HypeEdgeError> {
            Err(HypeEdgeError::MarketData("boom".into()))
        }
    }

    #[tokio::test]
    async fn poll_failure_records_health_failure() {
        let source: Arc<dyn AccountStateSource> = Arc::new(FailSource);
        let tracker = Arc::new(AccountTracker::new());
        let health: Arc<dyn MutableAccountHealthProvider> =
            Arc::new(LayeredAccountHealthProvider::default());
        let poller =
            AccountStatePoller::new(source, tracker, health.clone(), None, 3.0, 1.0, None, None)
                .unwrap();
        let interval = poller.poll_once().await;
        assert_eq!(interval, Duration::from_secs(1));
        let snapshot = health.get_account_health(None);
        assert_eq!(snapshot.clearinghouse.status, FreshnessStatus::Unhealthy);
        assert!(
            snapshot
                .clearinghouse
                .reason
                .as_deref()
                .unwrap()
                .starts_with("clearinghouse_poll_failed:")
        );
    }

    #[test]
    fn default_near_risk_detects_thin_equity() {
        let snapshot = PolledAccountSnapshot {
            account_state: AccountState {
                equity: Usd::new(Decimal::from_scaled(1000, 0)),
                available_balance: Usd::new(Decimal::from_scaled(100, 0)),
                total_margin_used: Usd::new(Decimal::from_scaled(0, 0)),
                total_unrealized_pnl: Usd::new(Decimal::from_scaled(0, 0)),
                peak_equity: Usd::new(Decimal::from_scaled(1000, 0)),
                sub_account: Some("0xabc".into()),
            },
            positions: vec![],
            received_at: at(100),
            spot_balances: vec![],
        };
        assert!(default_near_risk(&snapshot));
    }

    #[test]
    fn default_near_risk_false_for_healthy() {
        let snapshot = PolledAccountSnapshot {
            account_state: AccountState {
                equity: Usd::new(Decimal::from_scaled(1000, 0)),
                available_balance: Usd::new(Decimal::from_scaled(800, 0)),
                total_margin_used: Usd::new(Decimal::from_scaled(0, 0)),
                total_unrealized_pnl: Usd::new(Decimal::from_scaled(0, 0)),
                peak_equity: Usd::new(Decimal::from_scaled(1000, 0)),
                sub_account: Some("0xabc".into()),
            },
            positions: vec![],
            received_at: at(100),
            spot_balances: vec![],
        };
        assert!(!default_near_risk(&snapshot));
    }

    #[test]
    fn parse_position_from_clearinghouse() {
        let item = serde_json::json!({
            "position": {
                "coin": "BTC",
                "szi": "1.5",
                "positionValue": "75000",
                "entryPx": "49000",
                "unrealizedPnl": "1500",
                "leverage": {"value": 3},
                "liquidationPx": "30000"
            }
        });
        let position = parse_position(&item).unwrap();
        assert_eq!(position.symbol, "BTC");
        assert_eq!(position.leverage, 3);
        assert!(position.size.inner().to_string().starts_with("1.5"));
        assert!(position.entry_price.is_some());
    }

    #[test]
    fn parse_spot_balances_missing_token_is_error() {
        let raw = serde_json::json!({
            "balances": [
                {"coin": "USDC", "total": "100", "hold": "10", "entryNtl": "100"},
                {"coin": "", "total": "1", "hold": "0", "entryNtl": "1"}
            ]
        });
        // Empty token must raise (never silently drop) — mirrors the Python.
        assert!(parse_spot_balances(&raw, "0xABC").is_err());
    }

    #[test]
    fn parse_spot_balances_ok() {
        let raw = serde_json::json!({
            "balances": [
                {"coin": "USDC", "total": "100", "hold": "10", "entryNtl": "100"}
            ]
        });
        let balances = parse_spot_balances(&raw, "0xABC").unwrap();
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].token, "USDC");
        assert_eq!(balances[0].sub_account.as_deref(), Some("0xabc"));
    }

    #[test]
    fn blocking_reasons_format() {
        let provider = LayeredAccountHealthProvider::default();
        let snapshot = provider.get_account_health(Some(at(100)));
        let reasons = snapshot.blocking_reasons();
        // M-RK1: the never-observed UserStream dimension does not participate,
        // so only inventory/clearinghouse/reconciliation block.
        assert_eq!(reasons.len(), 3);
        assert!(reasons.iter().all(|r| r.ends_with("not_observed")));
        assert!(reasons.iter().all(|r| !r.starts_with("user_stream:")));
    }

    #[test]
    fn unobserved_user_stream_does_not_block_risk() {
        // M-RK1: with no user stream wired (never observed), the UserStream
        // dimension must not make `allows_risk_increase` permanently false —
        // the other three dimensions drive the gate.
        let provider = LayeredAccountHealthProvider::default();
        for dimension in [
            AccountHealthDimension::Inventory,
            AccountHealthDimension::Clearinghouse,
            AccountHealthDimension::Reconciliation,
        ] {
            provider.record_success(dimension, Some(at(96)));
        }
        let snapshot = provider.get_account_health(Some(at(100)));
        assert!(
            snapshot.allows_risk_increase(),
            "unobserved user stream must not block risk increase (M-RK1)"
        );
        assert!(!snapshot.requires_cancel());

        // An observed-but-stale UserStream still blocks (the stream exists and
        // is broken).
        provider.record_success(AccountHealthDimension::UserStream, Some(at(50)));
        let snapshot = provider.get_account_health(Some(at(100)));
        assert!(!snapshot.allows_risk_increase());
        assert!(snapshot
            .blocking_reasons()
            .iter()
            .any(|r| r.starts_with("user_stream:")));
    }
}
