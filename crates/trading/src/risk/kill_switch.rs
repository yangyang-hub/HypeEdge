//! Global kill switch latch, port of `src/hypeedge/risk/kill_switch.py`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::events::{DomainEvent, Event};
use hypeedge_domain::models::KillSwitchData;
use hypeedge_domain::traits::SystemStateStore;
use hypeedge_infra::event_bus::EventBus;
use tokio::sync::Mutex;

/// A cancel-all hook spawned on trigger, so a kill actually flattens working
/// orders rather than only blocking new placements (A14).
type CancelAllHook = dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + Sync;

/// Global kill-switch latch. `trigger()` latches, durably records the halt
/// (when a [`SystemStateStore`] is attached), spawns the cancel-all hook, and
/// publishes the event; `check()` raises before every order.
pub struct KillSwitch {
    active: Mutex<bool>,
    reason: Mutex<Option<String>>,
    bus: Arc<EventBus>,
    kill_switch_enabled: bool,
    cancel_all: Option<Arc<CancelAllHook>>,
    state_store: Option<Arc<dyn SystemStateStore>>,
}

impl KillSwitch {
    pub fn new(bus: Arc<EventBus>, kill_switch_enabled: bool) -> Self {
        // L-RK5: a disabled kill switch is a risk red line being OFF — `check()`
        // will pass on every order even after `trigger()`. The config layer
        // (mainnet forcing `kill_switch_enabled == true`) is enforced elsewhere;
        // here we make the disabled state loud at construction. `trigger()`
        // itself is intentionally unaffected: it still latches, persists,
        // spawns the cancel-all hook and publishes the event.
        if !kill_switch_enabled {
            tracing::warn!(
                "kill_switch_disabled: kill switch is DISABLED — the halt-all red line is off; \
                 check() will not block placements, though trigger() still cancels working orders"
            );
        }
        Self {
            active: Mutex::new(false),
            reason: Mutex::new(None),
            bus,
            kill_switch_enabled,
            cancel_all: None,
            state_store: None,
        }
    }

    /// Attach a cancel-all hook: spawned on trigger so a kill flattens working
    /// orders on the exchange, not just blocks new placements (A14).
    pub fn with_cancel_all(
        mut self,
        cancel_all: impl Fn() -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.cancel_all = Some(Arc::new(cancel_all));
        self
    }

    /// Attach a durable system-state store so a kill survives a restart (A15).
    pub fn with_state_store(mut self, state_store: Arc<dyn SystemStateStore>) -> Self {
        self.state_store = Some(state_store);
        self
    }

    pub async fn is_active(&self) -> bool {
        *self.active.lock().await
    }

    pub async fn reason(&self) -> Option<String> {
        self.reason.lock().await.clone()
    }

    /// Trigger the kill switch: latch, persist, cancel all working orders, and
    /// publish the event.
    pub async fn trigger(&self, reason: &str) {
        let mut active = self.active.lock().await;
        let mut reason_guard = self.reason.lock().await;
        if !*active {
            tracing::error!(reason, "kill_switch_triggered");
            *active = true;
            *reason_guard = Some(reason.to_string());
            drop(active);
            drop(reason_guard);
            if let Some(store) = &self.state_store
                && let Err(e) = store
                    .transition("halted", Some(reason), true, "kill_switch")
                    .await
            {
                tracing::error!(error = %e, "kill_switch_durable_persist_failed");
            }
            if let Some(hook) = &self.cancel_all {
                let hook = hook.clone();
                tokio::spawn(async move { hook().await });
            }
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

    /// Clear the latch (operator-initiated reset after reconciliation).
    pub async fn reset(&self) {
        let mut active = self.active.lock().await;
        let mut reason_guard = self.reason.lock().await;
        if *active {
            *active = false;
            *reason_guard = None;
            drop(active);
            drop(reason_guard);
            if let Some(store) = &self.state_store
                && let Err(e) = store.transition("normal", None, false, "api_reset").await
            {
                tracing::error!(error = %e, "kill_switch_reset_persist_failed");
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
    use hypeedge_domain::traits::DurableSystemState;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    /// A minimal in-memory [`SystemStateStore`] fake for kill-switch persistence.
    struct FakeStateStore(Arc<std::sync::Mutex<Option<DurableSystemState>>>);

    #[async_trait::async_trait]
    impl SystemStateStore for FakeStateStore {
        async fn load(&self) -> Result<Option<DurableSystemState>, HypeEdgeError> {
            Ok(self.0.lock().unwrap().clone())
        }
        async fn transition(
            &self,
            state: &str,
            reason: Option<&str>,
            kill_switch_active: bool,
            _triggered_by: &str,
        ) -> Result<(), HypeEdgeError> {
            *self.0.lock().unwrap() = Some(DurableSystemState {
                state: state.to_string(),
                reason: reason.map(String::from),
                kill_switch_active,
            });
            Ok(())
        }
    }

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

    #[test]
    fn disabled_construction_warns() {
        // L-RK5 regression: constructing a kill switch with `kill_switch_enabled
        // == false` must raise a loud warning — the halt-all red line is off.
        let events = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let subscriber = CaptureSubscriber(events.clone());
        tracing::subscriber::with_default(subscriber, || {
            let bus = Arc::new(EventBus::new(16));
            let _ks = KillSwitch::new(bus, false);
        });
        let events = events.lock().unwrap();
        assert!(
            events.iter().any(|e| e.contains("kill_switch_disabled")),
            "expected a kill_switch_disabled warning, got {events:?}"
        );
    }

    /// Minimal [`tracing::Subscriber`] that collects event payloads as strings.
    struct CaptureSubscriber(Arc<std::sync::Mutex<Vec<String>>>);

    impl tracing::Subscriber for CaptureSubscriber {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::Id {
            tracing::Id::from_u64(0)
        }
        fn record(&self, _span: &tracing::Id, _values: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _span: &tracing::Id, _follows: &tracing::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            let mut visitor = MessageVisitor(String::new());
            event.record(&mut visitor);
            self.0.lock().unwrap().push(visitor.0);
        }
        fn enter(&self, _span: &tracing::Id) {}
        fn exit(&self, _span: &tracing::Id) {}
    }

    /// Collects the `message` field of an event (the `tracing::warn!` text).
    struct MessageVisitor(String);

    impl tracing::field::Visit for MessageVisitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0.push_str(&format!("{value:?}"));
            }
        }
    }

    #[tokio::test]
    async fn trigger_spawns_cancel_all_hook() {
        // A14 regression: a kill must flatten working orders, not just block
        // new placements.
        let bus = Arc::new(EventBus::new(16));
        let called = Arc::new(AtomicBool::new(false));
        let ks = KillSwitch::new(bus, true).with_cancel_all({
            let called = called.clone();
            move || {
                let called = called.clone();
                Box::pin(async move {
                    called.store(true, AtomicOrdering::SeqCst);
                })
            }
        });
        ks.trigger("test").await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(
            called.load(AtomicOrdering::SeqCst),
            "cancel-all hook must be spawned on trigger (A14)"
        );
    }

    #[tokio::test]
    async fn trigger_persists_halt_to_state_store() {
        // A15 regression: a kill must be durably recorded so a restart restores it.
        let bus = Arc::new(EventBus::new(16));
        let store = Arc::new(FakeStateStore(Arc::new(std::sync::Mutex::new(None))));
        let ks = KillSwitch::new(bus, true).with_state_store(store.clone());
        ks.trigger("drawdown").await;
        let state = store.0.lock().unwrap().clone().expect("halt persisted");
        assert_eq!(state.state, "halted");
        assert!(state.kill_switch_active);
        assert_eq!(state.reason.as_deref(), Some("drawdown"));
    }

    #[tokio::test]
    async fn reset_clears_latch_and_persists_normal() {
        let bus = Arc::new(EventBus::new(16));
        let store = Arc::new(FakeStateStore(Arc::new(std::sync::Mutex::new(None))));
        let ks = KillSwitch::new(bus, true).with_state_store(store.clone());
        ks.trigger("test").await;
        assert!(ks.is_active().await);
        ks.reset().await;
        assert!(!ks.is_active().await);
        let state = store.0.lock().unwrap().clone().expect("reset persisted");
        assert_eq!(state.state, "normal");
        assert!(!state.kill_switch_active);
    }
}
