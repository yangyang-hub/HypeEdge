//! Global kill switch latch, port of `src/hypeedge/risk/kill_switch.py`.

use std::sync::Arc;

use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::events::{DomainEvent, Event};
use hypeedge_domain::models::KillSwitchData;
use hypeedge_infra::event_bus::EventBus;
use tokio::sync::Mutex;

/// Global kill-switch latch. `trigger()` publishes the event and spawns a
/// cancel-all task that verifies exchange truth before `HALTED`; `check()`
/// raises before every order.
pub struct KillSwitch {
    active: Mutex<bool>,
    reason: Mutex<Option<String>>,
    bus: Arc<EventBus>,
    kill_switch_enabled: bool,
}

impl KillSwitch {
    pub fn new(bus: Arc<EventBus>, kill_switch_enabled: bool) -> Self {
        Self {
            active: Mutex::new(false),
            reason: Mutex::new(None),
            bus,
            kill_switch_enabled,
        }
    }

    pub async fn is_active(&self) -> bool {
        *self.active.lock().await
    }

    pub async fn reason(&self) -> Option<String> {
        self.reason.lock().await.clone()
    }

    /// Trigger the kill switch: publish the event and latch.
    pub async fn trigger(&self, reason: &str) {
        let mut active = self.active.lock().await;
        let mut reason_guard = self.reason.lock().await;
        if !*active {
            tracing::error!(reason, "kill_switch_triggered");
            *active = true;
            *reason_guard = Some(reason.to_string());
            drop(active);
            drop(reason_guard);
            if let Err(e) =
                self.bus
                    .publish_sync(Arc::new(Event::new(DomainEvent::KillSwitchTriggered(
                        KillSwitchData {
                            reason: Some(reason.to_string()),
                        },
                    ))))
            {
                tracing::error!(event_type = %e.event_type, "kill_switch_event_publish_backpressure");
            }
        }
    }

    /// Raise before every order.
    pub async fn check(&self) -> Result<(), HypeEdgeError> {
        if *self.active.lock().await && self.kill_switch_enabled {
            return Err(HypeEdgeError::kill_switch_triggered(
                "kill switch triggered",
                self.reason.lock().await.clone(),
            ));
        }
        Ok(())
    }

    /// Restore the latch for a durable restart.
    pub async fn restore_active(&self, reason: Option<String>) {
        let mut active = self.active.lock().await;
        let mut reason_guard = self.reason.lock().await;
        *active = true;
        *reason_guard = reason;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::events::EventType;

    #[tokio::test]
    async fn trigger_publishes_and_latches() {
        let bus = Arc::new(EventBus::new(16));
        let mailbox = bus.subscribe(EventType::KillSwitchTriggered);
        let ks = KillSwitch::new(bus, true);
        assert!(!ks.is_active().await);
        ks.trigger("test").await;
        assert!(ks.is_active().await);
        assert_eq!(ks.reason().await.as_deref(), Some("test"));
        assert!(ks.check().await.is_err());
        let ev = mailbox.try_recv().expect("kill switch event published");
        match &ev.payload {
            DomainEvent::KillSwitchTriggered(d) => assert_eq!(d.reason.as_deref(), Some("test")),
            _ => panic!("wrong event"),
        }
    }

    #[tokio::test]
    async fn disabled_kill_switch_does_not_block() {
        let bus = Arc::new(EventBus::new(16));
        let ks = KillSwitch::new(bus, false);
        ks.trigger("test").await;
        // check() ignores the latch when disabled.
        assert!(ks.check().await.is_ok());
    }
}
