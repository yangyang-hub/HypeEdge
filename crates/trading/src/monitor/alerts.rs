//! Structured alerts, port of `src/hypeedge/monitor/alerts.py`.
//!
//! [`AlertPayload`] is the structured alert contract shared by logs and
//! external dispatchers. The dispatcher trait keeps the transport pluggable
//! (log/Telegram/etc.); this crate ships the validating payload and a log
//! dispatcher, and leaves Telegram/webhook wiring to the app layer.

use std::collections::BTreeMap;

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

/// Log-only dispatcher: emits a tracing warning/error per severity.
pub struct LogAlertDispatcher;

#[async_trait]
impl AlertDispatcher for LogAlertDispatcher {
    async fn dispatch(&self, alert: &AlertPayload) -> Result<(), String> {
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
        let dispatcher = LogAlertDispatcher;
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
}
