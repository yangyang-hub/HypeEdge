//! In-process event bus mirroring `src/hypeedge/core/events.py`.
//!
//! A bounded mailbox per subscriber. Market-data / market-making-analytics
//! events are **lossy** (a full queue drops its oldest item); trading, risk,
//! kill-switch, reconciliation, and account events are **reliable** (async
//! publishers apply backpressure, sync publishers fail loudly). The
//! classification comes from [`DomainEvent::is_lossy`] and is byte-identical
//! to Python's `LOSSY_EVENT_TYPES`.
//!
//! ## Zombie-subscriber hygiene
//!
//! The bus stores only [`Weak`] references to subscriber mailboxes. A consumer
//! that drops its mailbox handle is pruned lazily on the next matching
//! publish, so a dead subscriber can never pin a full mailbox forever (which
//! would otherwise block reliable publishers indefinitely). Mutex poisoning
//! (a panicked consumer/callback) is tolerated: locks are recovered with
//! `into_inner()` and the bus keeps operating.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use hypeedge_domain::events::{DomainEvent, Event, EventType};
use tokio::sync::Notify;

/// One subscriber's bounded queue.
///
/// `put` awaits space (reliable semantics); `put_lossy` drops the oldest item
/// when full (lossy semantics); `recv` removes the oldest item and wakes a
/// blocked putter.
pub struct BoundedMailbox<T> {
    state: Mutex<MailboxState<T>>,
    /// Wakes a blocked `put` when `recv` frees a slot.
    space_available: Notify,
    /// Wakes a blocked `recv` when `put` adds an item.
    item_ready: Notify,
    /// Classifies an item as lossy (evictable on overflow). Only lossy-typed
    /// items may be dropped oldest-first when the queue is full; reliable items
    /// are never evicted (B8: a wildcard mailbox must not lose Order*/Risk
    /// events to market-data overflow).
    is_lossy: Box<dyn Fn(&T) -> bool + Send + Sync>,
}

struct MailboxState<T> {
    items: VecDeque<T>,
    capacity: usize,
    dropped: u64,
    closed: bool,
}

impl<T> BoundedMailbox<T> {
    pub fn new(capacity: usize) -> Self {
        Self::with_classifier(capacity, |_| false)
    }

    pub fn with_classifier(
        capacity: usize,
        is_lossy: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            state: Mutex::new(MailboxState {
                items: VecDeque::with_capacity(capacity),
                capacity,
                dropped: 0,
                closed: false,
            }),
            space_available: Notify::new(),
            item_ready: Notify::new(),
            is_lossy: Box::new(is_lossy),
        }
    }

    /// Lock the mailbox state, tolerating a poisoned mutex: a panicked
    /// consumer must not take the whole bus down (M-IN3).
    fn lock_state(&self) -> MutexGuard<'_, MailboxState<T>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Whether the mailbox was closed (or the owning consumer dropped its
    /// handle entirely, in which case the bus prunes it lazily).
    pub fn is_closed(&self) -> bool {
        self.lock_state().closed
    }

    /// Reliable put: await space if the queue is full. Returns `false` if the
    /// mailbox was closed.
    pub async fn put(&self, item: T) -> bool {
        loop {
            {
                let mut st = self.lock_state();
                if st.closed {
                    return false;
                }
                if st.items.len() < st.capacity {
                    st.items.push_back(item);
                    drop(st);
                    self.item_ready.notify_one();
                    return true;
                }
            }
            self.space_available.notified().await;
        }
    }

    /// Lossy put: drop the oldest lossy item if full, then push. Reliable
    /// items are never evicted; if the queue is full of reliable items the
    /// incoming lossy item is dropped instead. Returns how many items were
    /// dropped (0 or 1). Returns `None` if closed.
    pub fn put_lossy(&self, item: T) -> Option<u64> {
        let mut st = self.lock_state();
        if st.closed {
            return None;
        }
        let mut dropped = 0;
        if st.items.len() >= st.capacity {
            match st.items.iter().position(|it| (self.is_lossy)(it)) {
                Some(idx) => {
                    st.items.remove(idx);
                    dropped = 1;
                    st.dropped += 1;
                }
                None => {
                    // Queue is full of reliable items; drop the incoming lossy
                    // item rather than evicting a reliable event.
                    st.dropped += 1;
                    return Some(1);
                }
            }
        }
        st.items.push_back(item);
        drop(st);
        self.item_ready.notify_one();
        Some(dropped)
    }

    /// Wait for the next item, or `None` once the mailbox is closed and drained.
    pub async fn recv(&self) -> Option<T> {
        loop {
            {
                let mut st = self.lock_state();
                if let Some(item) = st.items.pop_front() {
                    drop(st);
                    self.space_available.notify_one();
                    return Some(item);
                }
                if st.closed {
                    return None;
                }
            }
            self.item_ready.notified().await;
        }
    }

    /// Non-blocking peek of the oldest item, if any.
    pub fn try_recv(&self) -> Option<T> {
        let mut st = self.lock_state();
        let item = st.items.pop_front();
        if item.is_some() {
            drop(st);
            self.space_available.notify_one();
        }
        item
    }

    pub fn len(&self) -> usize {
        self.lock_state().items.len()
    }

    /// The queue capacity (used by the sync publish path's backpressure check).
    pub fn capacity(&self) -> usize {
        self.lock_state().capacity
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of items dropped by the lossy path.
    pub fn dropped(&self) -> u64 {
        self.lock_state().dropped
    }

    /// Close the mailbox: `put`/`put_lossy` return closed, `recv` drains then
    /// returns `None`.
    pub fn close(&self) {
        self.lock_state().closed = true;
        self.space_available.notify_waiters();
        self.item_ready.notify_waiters();
    }
}

type Mailbox = Arc<BoundedMailbox<Arc<Event>>>;
/// The bus retains subscribers weakly so a dropped consumer handle is pruned
/// lazily instead of pinning a full mailbox forever (M-IN3).
type WeakMailbox = Weak<BoundedMailbox<Arc<Event>>>;

struct BusState {
    by_type: HashMap<EventType, Vec<WeakMailbox>>,
    wildcard: Vec<WeakMailbox>,
    queue_maxsize: usize,
}

/// The in-process event bus.
pub struct EventBus {
    state: Mutex<BusState>,
    publish_count: AtomicU64,
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(10_000)
    }
}

impl EventBus {
    pub fn new(queue_maxsize: usize) -> Self {
        Self {
            state: Mutex::new(BusState {
                by_type: HashMap::new(),
                wildcard: Vec::new(),
                queue_maxsize,
            }),
            publish_count: AtomicU64::new(0),
        }
    }

    /// Lock the bus state, tolerating a poisoned mutex: a panicked subscriber
    /// or callback must not take the whole bus down (M-IN3).
    fn lock_bus_state(&self) -> MutexGuard<'_, BusState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Subscribe to events of a specific type. Returns a mailbox to read from.
    pub fn subscribe(&self, event_type: EventType) -> Mailbox {
        let mb = self.new_mailbox();
        self.lock_bus_state()
            .by_type
            .entry(event_type)
            .or_default()
            .push(Arc::downgrade(&mb));
        mb
    }

    /// Subscribe one mailbox to a declared set of event types, preserving
    /// publish order across them.
    pub fn subscribe_many(&self, event_types: &[EventType]) -> Mailbox {
        let mb = self.new_mailbox();
        let mut st = self.lock_bus_state();
        for et in event_types {
            st.by_type.entry(*et).or_default().push(Arc::downgrade(&mb));
        }
        mb
    }

    /// Subscribe to one event type with a custom mailbox capacity (e.g. the
    /// strategy runner's latest-value `maxsize=1` lossy mailboxes).
    pub fn subscribe_maxsize(&self, event_type: EventType, maxsize: usize) -> Mailbox {
        let mb = Arc::new(BoundedMailbox::with_classifier(maxsize, event_is_lossy));
        self.lock_bus_state()
            .by_type
            .entry(event_type)
            .or_default()
            .push(Arc::downgrade(&mb));
        mb
    }

    /// Subscribe to all events (for audit/logging/metrics).
    pub fn subscribe_all(&self) -> Mailbox {
        let mb = self.new_mailbox();
        self.lock_bus_state().wildcard.push(Arc::downgrade(&mb));
        mb
    }

    /// Remove a mailbox from a specific event type's subscription list.
    pub fn unsubscribe(&self, event_type: EventType, mailbox: &Mailbox) {
        let mut st = self.lock_bus_state();
        if let Some(queues) = st.by_type.get_mut(&event_type) {
            queues.retain(|q| !same_mailbox(q, mailbox));
        }
    }

    /// Remove a mailbox from a set of event type subscriptions.
    pub fn unsubscribe_many(&self, event_types: &[EventType], mailbox: &Mailbox) {
        let mut st = self.lock_bus_state();
        for et in event_types {
            if let Some(queues) = st.by_type.get_mut(et) {
                queues.retain(|q| !same_mailbox(q, mailbox));
            }
        }
    }

    /// Remove a mailbox from the wildcard subscription list.
    pub fn unsubscribe_wildcard(&self, mailbox: &Mailbox) {
        self.lock_bus_state()
            .wildcard
            .retain(|q| !same_mailbox(q, mailbox));
    }

    /// Publish an event to all matching subscribers. Reliable event types
    /// await space (backpressure); lossy types drop the oldest item on a full
    /// mailbox.
    pub async fn publish(&self, event: Arc<Event>) {
        let lossy = event.payload.is_lossy();
        let targets = self.matching_mailboxes(event.payload.event_type());
        for mb in targets {
            if lossy {
                mb.put_lossy(event.clone());
            } else {
                mb.put(event.clone()).await;
            }
        }
        self.publish_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Synchronous publish for use from sync contexts (e.g. callbacks).
    /// Reliable events are pushed atomically (retrying briefly for a concurrent
    /// consumer to drain) and fail loudly with `Err` rather than being dropped;
    /// lossy events drop under overflow (only lossy-typed items are evicted).
    pub fn publish_sync(&self, event: Arc<Event>) -> Result<(), EventBusBackpressureError> {
        let lossy = event.payload.is_lossy();
        let targets = self.matching_mailboxes(event.payload.event_type());
        for mb in targets {
            if lossy {
                mb.put_lossy(event.clone());
            } else {
                let mut pushed = false;
                for _ in 0..100 {
                    if mb.try_recv_forced(event.clone()) {
                        pushed = true;
                        break;
                    }
                    std::thread::yield_now();
                }
                if !pushed {
                    return Err(EventBusBackpressureError {
                        event_type: event.payload.event_type().as_str().to_string(),
                    });
                }
            }
        }
        self.publish_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Total number of events published (both paths).
    pub fn publish_count(&self) -> u64 {
        self.publish_count.load(Ordering::Relaxed)
    }

    /// Number of live subscribers across all event types plus wildcard.
    /// Subscribers that dropped their mailbox handle are not counted.
    pub fn subscriber_count(&self) -> usize {
        let st = self.lock_bus_state();
        st.by_type
            .values()
            .map(|v| v.iter().filter(|q| is_live(q)).count())
            .sum::<usize>()
            + st.wildcard.iter().filter(|q| is_live(q)).count()
    }

    /// Number of distinct subscribed event types with at least one live
    /// subscriber.
    pub fn event_type_count(&self) -> usize {
        self.lock_bus_state()
            .by_type
            .iter()
            .filter(|(_, v)| v.iter().any(is_live))
            .count()
    }

    fn capacity(&self) -> usize {
        self.lock_bus_state().queue_maxsize
    }

    fn new_mailbox(&self) -> Mailbox {
        let cap = self.capacity();
        Arc::new(BoundedMailbox::with_classifier(cap, event_is_lossy))
    }

    /// Collect strong references to the live mailboxes matching an event type,
    /// pruning dead (dropped-handle) subscribers from the maps in the same
    /// critical section. A dropped subscriber can never pin a full mailbox and
    /// block a reliable publisher (M-IN3).
    fn matching_mailboxes(&self, event_type: EventType) -> Vec<Mailbox> {
        let mut st = self.lock_bus_state();
        let mut out = Vec::new();
        if let Some(queues) = st.by_type.get_mut(&event_type) {
            queues.retain(|q| {
                match q.upgrade() {
                    Some(mb) => {
                        out.push(mb);
                        true
                    }
                    None => false, // zombie subscriber: prune
                }
            });
        }
        st.wildcard.retain(|q| {
            match q.upgrade() {
                Some(mb) => {
                    out.push(mb);
                    true
                }
                None => false, // zombie subscriber: prune
            }
        });
        out
    }
}

/// Whether a weak mailbox reference still points at a live mailbox.
fn is_live(q: &WeakMailbox) -> bool {
    q.strong_count() > 0
}

/// Whether a weak reference and a strong mailbox refer to the same mailbox.
fn same_mailbox(q: &WeakMailbox, mb: &Mailbox) -> bool {
    q.ptr_eq(&Arc::downgrade(mb))
}

/// A reliable event could not be delivered without dropping data (mirrors
/// `EventBusBackpressureError`).
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("reliable event queue is full: event_type={event_type}")]
pub struct EventBusBackpressureError {
    pub event_type: String,
}

// The sync putter needs a true non-blocking push that respects capacity; it is
// reached only after the space check above.
impl<T> BoundedMailbox<T> {
    /// Push without blocking, returning `false` if the mailbox is closed or
    /// full. Used by the sync publish path after its capacity check.
    pub(crate) fn try_recv_forced(&self, item: T) -> bool {
        let mut st = self.lock_state();
        if st.closed || st.items.len() >= st.capacity {
            return false;
        }
        st.items.push_back(item);
        drop(st);
        self.item_ready.notify_one();
        true
    }
}

/// Convenience: turn a domain event into a shared, envelope-wrapped event.
pub fn wrap(event: DomainEvent) -> Arc<Event> {
    Arc::new(Event::new(event))
}

/// Classifier for `Arc<Event>` mailboxes: market-data / market-making-analytics
/// events are lossy (evictable on overflow); trading, risk, kill-switch,
/// reconciliation, and account events are reliable (never evicted).
fn event_is_lossy(event: &Arc<Event>) -> bool {
    event.payload.is_lossy()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::models::{L2BookSnapshot, Order};

    fn order_event() -> Arc<Event> {
        wrap(DomainEvent::OrderSubmitted(Order::new(
            "c".into(),
            "BTC".into(),
            hypeedge_domain::Side::Buy,
            hypeedge_domain::Size::ZERO,
            None,
            hypeedge_domain::OrderType::Limit,
            hypeedge_domain::TimeInForce::Gtc,
        )))
    }

    fn book_event() -> Arc<Event> {
        wrap(DomainEvent::L2BookUpdate(L2BookSnapshot {
            symbol: "BTC".into(),
            bids: vec![],
            asks: vec![],
            timestamp: 0,
            local_ts: chrono::Utc::now(),
            version: 0,
            connection_generation: 0,
        }))
    }

    #[tokio::test]
    async fn lossy_drops_oldest_when_full() {
        let bus = EventBus::new(2);
        let sub = bus.subscribe(EventType::L2BookUpdate);
        bus.publish(book_event()).await;
        bus.publish(book_event()).await;
        assert_eq!(sub.len(), 2);
        bus.publish(book_event()).await; // full -> drops oldest
        assert_eq!(sub.len(), 2);
        assert_eq!(sub.dropped(), 1);
        // The oldest was dropped: remaining are items 2 and 3.
        let _ = sub.recv().await;
        let _ = sub.recv().await;
    }

    #[tokio::test]
    async fn reliable_backpressures_until_space() {
        let bus = Arc::new(EventBus::new(2));
        let sub = bus.subscribe(EventType::OrderSubmitted);
        bus.publish(order_event()).await;
        bus.publish(order_event()).await;
        assert_eq!(sub.len(), 2);

        // Reliable put on a full mailbox must wait, not drop.
        let bus2 = bus.clone();
        let publisher = tokio::spawn(async move {
            bus2.publish(order_event()).await;
        });
        // Give the publisher a moment to block.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        let _ = sub.recv().await; // free one slot
        publisher.await.unwrap();
        assert_eq!(sub.len(), 2);
        assert_eq!(sub.dropped(), 0);
    }

    #[tokio::test]
    async fn wildcard_receives_all() {
        let bus = EventBus::new(16);
        let all = bus.subscribe_all();
        bus.publish(book_event()).await;
        bus.publish(order_event()).await;
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn wildcard_overflow_never_drops_reliable() {
        // B8 regression: a full wildcard mailbox must evict only lossy-typed
        // items; a reliable Order* event queued first must survive overflow.
        let bus = EventBus::new(2);
        let all = bus.subscribe_all();
        bus.publish(order_event()).await; // reliable
        bus.publish(book_event()).await; // lossy -> full
        bus.publish(book_event()).await; // lossy overflow -> evicts lossy only
        assert_eq!(all.len(), 2);
        assert_eq!(all.dropped(), 1);
        let first = all.recv().await.unwrap();
        assert!(
            matches!(first.payload, DomainEvent::OrderSubmitted(_)),
            "reliable order event must not be evicted by lossy overflow"
        );
    }

    #[test]
    fn sync_publish_reliable_full_returns_err_not_drop() {
        // B9 regression: publish_sync on a full reliable mailbox must surface
        // backpressure (Err) and must not drop the queued reliable event.
        let bus = EventBus::new(1);
        let sub = bus.subscribe(EventType::OrderSubmitted);
        bus.publish_sync(order_event()).unwrap();
        assert_eq!(sub.len(), 1);
        let err = bus.publish_sync(order_event()).unwrap_err();
        assert!(err.event_type.contains("Order"));
        assert_eq!(sub.len(), 1, "reliable event must not be dropped");
    }

    #[tokio::test]
    async fn unsubscribe_stops_delivery() {
        let bus = EventBus::new(16);
        let sub = bus.subscribe(EventType::OrderSubmitted);
        bus.unsubscribe(EventType::OrderSubmitted, &sub);
        bus.publish(order_event()).await;
        assert_eq!(sub.len(), 0);
    }

    // --- M-IN3: zombie subscribers and poisoned locks ---

    #[tokio::test]
    async fn dropped_subscriber_is_pruned_and_publish_does_not_block() {
        let bus = Arc::new(EventBus::new(2));
        {
            let sub = bus.subscribe(EventType::OrderSubmitted);
            assert_eq!(bus.subscriber_count(), 1);
            bus.publish(order_event()).await;
            assert_eq!(sub.len(), 1);
        } // consumer drops its handle — only the bus's Weak ref remains

        // A dropped consumer must not pin a mailbox forever: publish skips it
        // (no infinite await) and the registration is pruned.
        bus.publish(order_event()).await;
        assert_eq!(bus.subscriber_count(), 0);
        assert_eq!(bus.event_type_count(), 0);
    }

    #[tokio::test]
    async fn closed_mailbox_publish_returns_immediately() {
        let bus = EventBus::new(1);
        let sub = bus.subscribe(EventType::OrderSubmitted);
        sub.close();
        // put on a closed mailbox returns false on the first check; publish
        // must not block.
        bus.publish(order_event()).await;
        assert_eq!(sub.len(), 0);
        assert_eq!(bus.publish_count(), 1);
    }

    #[tokio::test]
    async fn wildcard_zombie_pruned_alongside_typed() {
        let bus = EventBus::new(4);
        {
            let _typed = bus.subscribe(EventType::OrderSubmitted);
            let _wild = bus.subscribe_all();
            assert_eq!(bus.subscriber_count(), 2);
        }
        bus.publish(order_event()).await;
        assert_eq!(bus.subscriber_count(), 0);
    }

    #[test]
    fn poisoned_bus_state_keeps_working() {
        let bus = Arc::new(EventBus::new(4));
        // Poison the internal mutex: panic while holding its guard.
        let bus2 = bus.clone();
        let handle = std::thread::spawn(move || {
            let _guard = bus2.state.lock().unwrap();
            panic!("intentional panic to poison the bus state");
        });
        assert!(handle.join().is_err(), "the panic must poison the mutex");
        // The bus keeps operating on the poisoned state.
        let sub = bus.subscribe(EventType::OrderSubmitted);
        bus.publish_sync(order_event()).unwrap();
        assert_eq!(sub.len(), 1);
        assert_eq!(bus.subscriber_count(), 1);
    }

    #[test]
    fn poisoned_mailbox_state_keeps_working() {
        let bus = EventBus::new(4);
        let sub = bus.subscribe(EventType::OrderSubmitted);
        // Poison the mailbox state mutex.
        let sub2 = sub.clone();
        let handle = std::thread::spawn(move || {
            let _guard = sub2.state.lock().unwrap();
            panic!("intentional panic to poison the mailbox");
        });
        assert!(handle.join().is_err());
        bus.publish_sync(order_event()).unwrap();
        assert_eq!(sub.len(), 1);
        assert_eq!(sub.dropped(), 0);
    }
}
