//! Sequential EventBus consumer for a single strategy, port of
//! `src/hypeedge/strategy/runner.py`.
//!
//! Reliable events share one mailbox (their delivery is backpressured); each
//! lossy event type gets a dedicated `maxsize=1` mailbox so the strategy always
//! sees the *latest* market-data snapshot. The runner reads all mailboxes
//! concurrently and processes reliable facts before lossy notifications,
//! matching Python's ordering guarantee.

use std::sync::Arc;

use hypeedge_domain::events::{DomainEvent, Event, EventType};
use hypeedge_infra::event_bus::{BoundedMailbox, EventBus};
use tokio::sync::mpsc;

use super::base::Strategy;

type Mailbox = Arc<BoundedMailbox<Arc<Event>>>;
type RecvFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Option<Arc<Event>>> + Send>>;

/// Own subscriptions and deliver events sequentially to a strategy.
pub struct StrategyRunner {
    strategy: Box<dyn Strategy>,
    bus: Arc<EventBus>,
    running: bool,
}

impl StrategyRunner {
    pub fn new(strategy: Box<dyn Strategy>, bus: Arc<EventBus>) -> Self {
        Self {
            strategy,
            bus,
            running: false,
        }
    }

    /// Run the strategy: subscribe, call `on_start`, then deliver events until
    /// `stop` is signalled. Returns when stopped.
    pub async fn run(&mut self, mut stop_rx: mpsc::Receiver<()>) -> Result<(), String> {
        let declared = self.strategy.subscriptions();
        let reliable_types: Vec<EventType> = declared
            .iter()
            .copied()
            .filter(|et| !is_lossy(*et))
            .collect();
        let lossy_types: Vec<EventType> = declared
            .iter()
            .copied()
            .filter(|et| is_lossy(*et))
            .collect();

        // Reliable events share one mailbox (backpressure preserved).
        let reliable_mailbox: Option<Mailbox> = if reliable_types.is_empty() {
            None
        } else {
            Some(self.bus.subscribe_many(&reliable_types))
        };
        // Each lossy event type gets a dedicated latest-value mailbox.
        let lossy_mailboxes: Vec<(EventType, Mailbox)> = lossy_types
            .iter()
            .map(|et| (*et, self.bus.subscribe_maxsize(*et, 1)))
            .collect();

        self.strategy.on_start().await?;
        self.running = true;
        tracing::info!(
            reliable = ?reliable_types,
            lossy = ?lossy_types,
            "strategy_runner_started"
        );

        let mut result = Ok(());
        loop {
            tokio::select! {
                _ = stop_rx.recv() => break,
                maybe = recv_any(&reliable_mailbox, &lossy_mailboxes) => {
                    match maybe {
                        Some(event) => {
                            if let Err(e) = self.strategy.on_event(&event).await {
                                result = Err(e);
                                break;
                            }
                        }
                        None => break, // all mailboxes closed
                    }
                }
            }
        }

        self.running = false;
        if let Some(mb) = &reliable_mailbox {
            self.bus.unsubscribe_many(&reliable_types, mb);
        }
        for (et, mb) in &lossy_mailboxes {
            self.bus.unsubscribe(*et, mb);
        }
        self.strategy.on_stop().await?;
        result
    }

    pub fn is_running(&self) -> bool {
        self.running
    }
}

/// Whether an event type is lossy per the bus classification.
fn is_lossy(event_type: EventType) -> bool {
    use EventType::*;
    matches!(
        event_type,
        L2BookUpdate
            | TradeUpdate
            | CandleUpdate
            | FundingUpdate
            | MidPriceUpdate
            | ExternalReferenceUpdate
            | MmFeatureSample
            | MmQuoteDecision
            | MmInventorySample
            | MmActionCreditSample
            | MmFillMarkout
    )
}

/// Receive the next event from the reliable mailbox if present, else any lossy
/// mailbox. Reliable facts are preferred (matching Python's reliable-first sort).
async fn recv_any(
    reliable: &Option<Mailbox>,
    lossy: &[(EventType, Mailbox)],
) -> Option<Arc<Event>> {
    if let Some(rb) = reliable
        && let Some(ev) = rb.try_recv()
    {
        return Some(ev);
    }
    match (reliable, lossy.len()) {
        (None, 0) => None,
        (Some(rb), 0) => rb.recv().await,
        (None, 1) => lossy[0].1.recv().await,
        _ => {
            // Await the first ready mailbox via select_all over pinned futures.
            let mut futs: Vec<RecvFuture> = Vec::new();
            if let Some(rb) = reliable {
                let mb = rb.clone();
                futs.push(Box::pin(async move { mb.recv().await }));
            }
            for (_, mb) in lossy {
                let mb = mb.clone();
                futs.push(Box::pin(async move { mb.recv().await }));
            }
            let (res, _, _) = futures::future::select_all(futs).await;
            res
        }
    }
}

/// Convenience: domain-level is_lossy (used to keep the runner classification
/// honest). Exposed for tests.
pub fn domain_is_lossy(payload: &DomainEvent) -> bool {
    payload.is_lossy()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypeedge_domain::enums::StrategyStatus;
    use hypeedge_domain::models::{Candle, L2BookSnapshot};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingStrategy {
        started: Arc<AtomicUsize>,
        events: Arc<Mutex<Vec<String>>>,
        status: StrategyStatus,
    }

    #[async_trait::async_trait]
    impl Strategy for CountingStrategy {
        async fn on_start(&mut self) -> Result<(), String> {
            self.started.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn on_event(&mut self, event: &Event) -> Result<(), String> {
            self.events
                .lock()
                .unwrap()
                .push(event.event_type().as_str().to_string());
            Ok(())
        }
        async fn on_stop(&mut self) -> Result<(), String> {
            Ok(())
        }
        fn subscriptions(&self) -> Vec<EventType> {
            vec![EventType::CandleUpdate, EventType::L2BookUpdate]
        }
        fn status(&self) -> StrategyStatus {
            self.status
        }
        fn set_status(&mut self, status: StrategyStatus) {
            self.status = status;
        }
    }

    #[tokio::test]
    async fn runner_delivers_events_then_stops() {
        let bus = Arc::new(EventBus::new(16));
        let started = Arc::new(AtomicUsize::new(0));
        let events = Arc::new(Mutex::new(Vec::new()));
        let strat = CountingStrategy {
            started: started.clone(),
            events: events.clone(),
            status: StrategyStatus::Stopped,
        };
        let mut runner = StrategyRunner::new(Box::new(strat), bus.clone());
        let (stop_tx, stop_rx) = mpsc::channel(1);

        let handle = tokio::spawn(async move { runner.run(stop_rx).await });

        // Give the runner time to subscribe before publishing (the bus has no
        // subscribers yet until the runner's `on_start`-preceding subscribe).
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Publish a candle + book event.
        let candle = Candle {
            symbol: "BTC".into(),
            interval: "1m".into(),
            open: Default::default(),
            high: Default::default(),
            low: Default::default(),
            close: Default::default(),
            volume: Default::default(),
            timestamp: 1,
        };
        bus.publish(Arc::new(Event::new(DomainEvent::CandleUpdate(candle))))
            .await;
        let book = L2BookSnapshot {
            symbol: "BTC".into(),
            bids: vec![],
            asks: vec![],
            timestamp: 2,
            local_ts: chrono::Utc::now(),
            version: 0,
            connection_generation: 0,
        };
        bus.publish(Arc::new(Event::new(DomainEvent::L2BookUpdate(book))))
            .await;

        // Let the runner process, then stop.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let _ = stop_tx.send(()).await;
        handle.await.unwrap().unwrap();

        assert_eq!(started.load(Ordering::SeqCst), 1);
        let seen = events.lock().unwrap();
        assert!(seen.contains(&"CandleUpdate".to_string()));
        assert!(seen.contains(&"L2BookUpdate".to_string()));
    }
}
