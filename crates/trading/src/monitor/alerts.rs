//! Structured alerts, port of `src/hypeedge/monitor/alerts.py`.
//!
//! [`AlertPayload`] is the structured alert contract shared by logs and
//! external dispatchers. The dispatcher trait keeps the transport pluggable
//! (log/Telegram/etc.); this crate ships the validating payload and a log
//! dispatcher, and leaves Telegram/webhook wiring to the app layer.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

/// Alert severity, mirroring `AlertSeverity`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AlertSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

impl AlertSeverity {
    pub fn as_str(self) -> &'static str {
        match self {
            AlertSeverity::Info => "info",
            AlertSeverity::Warning => "warning",
            AlertSeverity::Error => "error",
            AlertSeverity::Critical => "critical",
        }
    }
}

/// Structured alert contract shared by logs and external dispatchers.
#[derive(Debug, Clone, PartialEq)]
pub struct AlertPayload {
    pub rule_id: String,
    pub title: String,
    pub message: String,
    pub severity: AlertSeverity,
    pub observed_at: DateTime<Utc>,
    pub labels: BTreeMap<String, String>,
    pub runbook_url: Option<String>,
}

impl AlertPayload {
    pub fn new(
        rule_id: impl Into<String>,
        title: impl Into<String>,
        message: impl Into<String>,
        severity: AlertSeverity,
        labels: BTreeMap<String, String>,
        runbook_url: Option<String>,
    ) -> Result<Self, String> {
        let rule_id = rule_id.into();
        let title = title.into();
        let message = message.into();
        if rule_id.is_empty() || title.is_empty() || message.is_empty() {
            return Err("alert rule_id, title, and message cannot be empty".into());
        }
        if labels.iter().any(|(k, v)| k.is_empty() || v.is_empty()) {
            return Err("alert labels cannot contain empty keys or values".into());
        }
        Ok(Self {
            rule_id,
            title,
            message,
            severity,
            observed_at: Utc::now(),
            labels,
            runbook_url,
        })
    }

    pub fn to_dict(&self) -> serde_json::Value {
        serde_json::json!({
            "rule_id": self.rule_id,
            "title": self.title,
            "message": self.message,
            "severity": self.severity.as_str(),
            "observed_at": self.observed_at.to_rfc3339(),
            "labels": serde_json::to_value(&self.labels).unwrap_or_default(),
            "runbook_url": self.runbook_url,
        })
    }

    /// Multi-line render for log dispatchers.
    pub fn render_text(&self) -> String {
        let mut lines = vec![self.message.clone()];
        if !self.labels.is_empty() {
            let labels = self
                .labels
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!("labels={labels}"));
        }
        if let Some(runbook) = &self.runbook_url {
            lines.push(format!("runbook={runbook}"));
        }
        lines.push(format!("observed_at={}", self.observed_at.to_rfc3339()));
        lines.join("\n")
    }
}

/// The alert dispatch boundary (transport-agnostic).
#[async_trait]
pub trait AlertDispatcher: Send + Sync {
    async fn dispatch(&self, alert: &AlertPayload) -> Result<(), String>;
}

/// Default per-rule suppression window (seconds): a rule is not re-dispatched
/// within this window of its last dispatch (P5-2).
const DEFAULT_ALERT_SUPPRESS_SECONDS: u64 = 60;

/// Log-only dispatcher: emits a tracing warning/error per severity, with a
/// per-rule suppression window so a flapping condition does not spam the log
/// (P5-2). Repeats within the window are counted, not re-emitted.
pub struct LogAlertDispatcher {
    suppress_window: std::time::Duration,
    last_sent_at: Mutex<BTreeMap<String, DateTime<Utc>>>,
    dispatched_total: AtomicU64,
    suppressed_total: AtomicU64,
}

impl Default for LogAlertDispatcher {
    fn default() -> Self {
        Self::new(DEFAULT_ALERT_SUPPRESS_SECONDS)
    }
}

impl LogAlertDispatcher {
    pub fn new(suppress_window_seconds: u64) -> Self {
        Self {
            suppress_window: std::time::Duration::from_secs(suppress_window_seconds),
            last_sent_at: Mutex::new(BTreeMap::new()),
            dispatched_total: AtomicU64::new(0),
            suppressed_total: AtomicU64::new(0),
        }
    }

    /// Builder: override the per-rule suppression window in seconds.
    pub fn with_suppress_window(mut self, seconds: u64) -> Self {
        self.suppress_window = std::time::Duration::from_secs(seconds);
        self
    }

    /// Total alerts actually emitted.
    pub fn dispatched_total(&self) -> u64 {
        self.dispatched_total.load(Ordering::Relaxed)
    }

    /// Total alerts suppressed by the per-rule window.
    pub fn suppressed_total(&self) -> u64 {
        self.suppressed_total.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl AlertDispatcher for LogAlertDispatcher {
    async fn dispatch(&self, alert: &AlertPayload) -> Result<(), String> {
        let now = Utc::now();
        let window_ms = self.suppress_window.as_millis() as u64;
        let mut last_sent = self.last_sent_at.lock().unwrap();
        let suppressed = match last_sent.get(&alert.rule_id) {
            Some(t) => ((now - *t).num_milliseconds().max(0) as u64) < window_ms,
            None => false,
        };
        if suppressed {
            self.suppressed_total.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        last_sent.insert(alert.rule_id.clone(), now);
        drop(last_sent);
        self.dispatched_total.fetch_add(1, Ordering::Relaxed);
        match alert.severity {
            AlertSeverity::Error | AlertSeverity::Critical => {
                tracing::error!(
                    rule_id = %alert.rule_id,
                    severity = alert.severity.as_str(),
                    title = %alert.title,
                    message = %alert.message,
                    "alert"
                );
            }
            _ => {
                tracing::warn!(
                    rule_id = %alert.rule_id,
                    severity = alert.severity.as_str(),
                    title = %alert.title,
                    message = %alert.message,
                    "alert"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alert() -> AlertPayload {
        AlertPayload::new(
            "mm_quote_stale",
            "Stale quote",
            "Quote exceeded max age",
            AlertSeverity::Warning,
            BTreeMap::from([("strategy_id".into(), "mm-btc".into())]),
            Some("https://runbook.example/stale-quote".into()),
        )
        .unwrap()
    }

    #[test]
    fn severity_strings() {
        assert_eq!(AlertSeverity::Info.as_str(), "info");
        assert_eq!(AlertSeverity::Critical.as_str(), "critical");
    }

    #[test]
    fn empty_required_fields_rejected() {
        assert!(
            AlertPayload::new("", "t", "m", AlertSeverity::Info, BTreeMap::new(), None).is_err()
        );
        assert!(
            AlertPayload::new("r", "", "m", AlertSeverity::Info, BTreeMap::new(), None).is_err()
        );
        assert!(
            AlertPayload::new("r", "t", "", AlertSeverity::Info, BTreeMap::new(), None).is_err()
        );
    }

    #[test]
    fn empty_label_key_or_value_rejected() {
        let mut labels = BTreeMap::new();
        labels.insert("".into(), "v".into());
        assert!(AlertPayload::new("r", "t", "m", AlertSeverity::Info, labels, None).is_err());
        let mut labels = BTreeMap::new();
        labels.insert("k".into(), "".into());
        assert!(AlertPayload::new("r", "t", "m", AlertSeverity::Info, labels, None).is_err());
    }

    #[test]
    fn to_dict_is_complete() {
        let alert = alert();
        let dict = alert.to_dict();
        assert_eq!(dict["rule_id"], "mm_quote_stale");
        assert_eq!(dict["severity"], "warning");
        assert_eq!(dict["labels"]["strategy_id"], "mm-btc");
        assert_eq!(dict["runbook_url"], "https://runbook.example/stale-quote");
        assert!(dict["observed_at"].is_string());
    }

    #[test]
    fn render_text_includes_labels_and_runbook() {
        let text = alert().render_text();
        assert!(text.contains("Quote exceeded max age"));
        assert!(text.contains("labels=strategy_id=mm-btc"));
        assert!(text.contains("runbook=https://runbook.example/stale-quote"));
        assert!(text.contains("observed_at="));
    }

    #[tokio::test]
    async fn log_dispatcher_accepts_all_severities() {
        let dispatcher = LogAlertDispatcher::default();
        assert!(dispatcher.dispatch(&alert()).await.is_ok());
        let critical = AlertPayload::new(
            "kill_switch",
            "KS",
            "triggered",
            AlertSeverity::Critical,
            BTreeMap::new(),
            None,
        )
        .unwrap();
        assert!(dispatcher.dispatch(&critical).await.is_ok());
    }

    #[tokio::test]
    async fn dispatcher_suppresses_repeats_within_window() {
        // P5-2: the same rule is not re-emitted within the suppression window;
        // a different rule or a later dispatch passes through.
        let d = LogAlertDispatcher::new(60);
        let a = alert();
        assert!(d.dispatch(&a).await.is_ok());
        assert_eq!(d.dispatched_total(), 1);
        assert!(d.dispatch(&a).await.is_ok(), "repeat returns Ok");
        assert_eq!(d.dispatched_total(), 1, "repeat suppressed");
        assert_eq!(d.suppressed_total(), 1);
        // Different rule is not suppressed.
        let other = AlertPayload::new(
            "other_rule",
            "Other",
            "msg",
            AlertSeverity::Info,
            BTreeMap::new(),
            None,
        )
        .unwrap();
        assert!(d.dispatch(&other).await.is_ok());
        assert_eq!(d.dispatched_total(), 2);
    }

    #[tokio::test]
    async fn dispatcher_allows_after_window_elapses() {
        // P5-2: once the window passes, the same rule dispatches again.
        let d = LogAlertDispatcher::with_suppress_window(LogAlertDispatcher::new(60), 0);
        let a = alert();
        assert!(d.dispatch(&a).await.is_ok());
        assert!(d.dispatch(&a).await.is_ok());
        assert_eq!(d.dispatched_total(), 2);
    }
}
