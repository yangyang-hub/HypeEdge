//! Recoverable worker for durable signed exchange commands, port of
//! `src/hypeedge/execution/worker.py`.
//!
//! Claims durable commands from the lease queue and dispatches them through
//! the engine — the sole outlet to the serial nonce queue. A recovered/UNKNOWN
//! command is never resent: it is resolved exclusively by querying Hyperliquid
//! with its cloid, and absence keeps the reservation active (the engine defers
//! to `defer_unknown`).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::traits::{DurableCommandQueue, DurableExecutionCommand};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use super::engine::AfterSendHook;

/// Initial backoff after a transient failure (P2-4/H-EX1).
const BACKOFF_BASE: Duration = Duration::from_millis(100);
/// Backoff ceiling so a permanently failing worker retries at most every 30s.
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// The engine surface the worker dispatches through (implemented by
/// [`super::engine::ExecutionEngine`] and faked in tests).
#[async_trait]
pub trait DurableCommandDispatcher: Send + Sync {
    /// Execute a durable cancel command; `true` when a terminal outcome is
    /// proven.
    async fn execute_durable_cancel_command(
        &self,
        command: &DurableExecutionCommand,
    ) -> Result<bool, HypeEdgeError>;

    /// Execute a durable command; `true` when a terminal outcome is proven.
    async fn execute_durable_command(
        &self,
        command: &DurableExecutionCommand,
        after_send_hook: Option<Box<AfterSendHook>>,
    ) -> Result<bool, HypeEdgeError>;
}

/// Test seam: invoked at `before_send` / `after_send` points to inject faults.
pub trait FaultInjector: Send + Sync {
    fn inject(&self, phase: &str, command: &DurableExecutionCommand);
}

/// Claim durable commands and send them through the one NonceManager queue.
#[derive(Clone)]
pub struct SignedActionExecutor {
    queue: Arc<dyn DurableCommandQueue>,
    dispatcher: Arc<dyn DurableCommandDispatcher>,
    poll_interval: Duration,
    worker_id: String,
    fault_injector: Option<Arc<dyn FaultInjector>>,
    running: Arc<AtomicBool>,
}

impl SignedActionExecutor {
    pub fn new(
        queue: Arc<dyn DurableCommandQueue>,
        dispatcher: Arc<dyn DurableCommandDispatcher>,
        poll_interval_ms: u64,
        worker_id: Option<String>,
    ) -> Self {
        Self {
            queue,
            dispatcher,
            poll_interval: Duration::from_millis(poll_interval_ms),
            worker_id: worker_id
                .unwrap_or_else(|| format!("signed-action-{}", uuid::Uuid::new_v4())),
            fault_injector: None,
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn with_fault_injector(mut self, injector: Arc<dyn FaultInjector>) -> Self {
        self.fault_injector = Some(injector);
        self
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Run the claim→dispatch loop until stopped. P2-4/H-EX1: a single
    /// transient failure (DB jitter, network blip) must not kill the worker —
    /// errors back off exponentially and the loop continues. Only `stop()` or
    /// the channel/queue being closed exits the loop.
    pub async fn run(&self) -> Result<(), HypeEdgeError> {
        tracing::info!(worker_id = %self.worker_id, "signed_action_executor_started");
        let mut backoff = BACKOFF_BASE;
        loop {
            if !self.running.load(AtomicOrdering::Relaxed) {
                break;
            }
            match self.run_once().await {
                Ok(true) => {
                    backoff = BACKOFF_BASE;
                }
                Ok(false) => {
                    backoff = BACKOFF_BASE;
                    tokio::time::sleep(self.poll_interval).await;
                }
                Err(e) => {
                    tracing::error!(
                        worker_id = %self.worker_id,
                        error = %e,
                        backoff_ms = backoff.as_millis() as u64,
                        "signed_action_executor_transient_failure_retry"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                }
            }
        }
        tracing::info!(worker_id = %self.worker_id, "signed_action_executor_stopped");
        Ok(())
    }

    /// Claim one command and dispatch it. Returns `true` if a command ran.
    pub async fn run_once(&self) -> Result<bool, HypeEdgeError> {
        let command = self.queue.claim(&self.worker_id).await?;
        let Some(command) = command else {
            return Ok(false);
        };

        if let Some(injector) = &self.fault_injector
            && !command.requires_resolution
        {
            injector.inject("before_send", &command);
        }
        let after_send = self.fault_injector.clone();
        let after_send_hook: Option<Box<AfterSendHook>> = after_send.map(|inj| {
            let command = command.clone();
            Box::new(move |_claimed: &DurableExecutionCommand| inj.inject("after_send", &command))
                as Box<AfterSendHook>
        });

        let resolved = if command.command_type == "cancel_order" {
            self.dispatcher
                .execute_durable_cancel_command(&command)
                .await?
        } else {
            self.dispatcher
                .execute_durable_command(&command, after_send_hook)
                .await?
        };
        if !resolved {
            self.queue
                .defer_unknown(
                    command.command_id,
                    "cloid lookup did not prove a terminal outcome",
                )
                .await?;
        }
        Ok(true)
    }

    /// Signal the worker to stop after the current command.
    pub fn stop(&self) {
        self.running.store(false, AtomicOrdering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::AtomicU32;

    /// In-memory queue with a programmable claim sequence.
    struct FakeQueue {
        commands: StdMutex<VecDeque<Option<DurableExecutionCommand>>>,
        deferred: StdMutex<Vec<(uuid::Uuid, String)>>,
        /// Number of consecutive `claim` failures to inject first.
        claim_errors: AtomicU32,
    }

    impl FakeQueue {
        fn new(commands: Vec<Option<DurableExecutionCommand>>) -> Self {
            Self {
                commands: StdMutex::new(commands.into()),
                deferred: StdMutex::new(Vec::new()),
                claim_errors: AtomicU32::new(0),
            }
        }
        fn deferred(&self) -> Vec<(uuid::Uuid, String)> {
            self.deferred.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DurableCommandQueue for FakeQueue {
        async fn claim(
            &self,
            _worker_id: &str,
        ) -> Result<Option<DurableExecutionCommand>, HypeEdgeError> {
            let injected = self
                .claim_errors
                .fetch_update(AtomicOrdering::SeqCst, AtomicOrdering::SeqCst, |n| {
                    n.checked_sub(1)
                })
                .is_ok();
            if injected {
                return Err(HypeEdgeError::Postgres {
                    message: "transient claim failure".into(),
                });
            }
            let mut queue = self.commands.lock().unwrap();
            Ok(queue.pop_front().unwrap_or(None))
        }

        async fn defer_unknown(
            &self,
            command_id: uuid::Uuid,
            reason: &str,
        ) -> Result<(), HypeEdgeError> {
            self.deferred
                .lock()
                .unwrap()
                .push((command_id, reason.to_string()));
            Ok(())
        }
    }

    /// Records dispatches; each command resolves per a lookup table.
    struct FakeDispatcher {
        results: StdMutex<HashMap<uuid::Uuid, bool>>,
        dispatched: StdMutex<Vec<(String, bool)>>,
    }

    use std::collections::HashMap;

    impl FakeDispatcher {
        fn new() -> Self {
            Self {
                results: StdMutex::new(HashMap::new()),
                dispatched: StdMutex::new(Vec::new()),
            }
        }
        fn with_result(self, command_id: uuid::Uuid, resolved: bool) -> Self {
            self.results.lock().unwrap().insert(command_id, resolved);
            self
        }
        fn dispatched(&self) -> Vec<(String, bool)> {
            self.dispatched.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DurableCommandDispatcher for FakeDispatcher {
        async fn execute_durable_cancel_command(
            &self,
            command: &DurableExecutionCommand,
        ) -> Result<bool, HypeEdgeError> {
            let resolved = self
                .results
                .lock()
                .unwrap()
                .get(&command.command_id)
                .copied()
                .unwrap_or(true);
            self.dispatched
                .lock()
                .unwrap()
                .push((command.command_type.clone(), resolved));
            Ok(resolved)
        }

        async fn execute_durable_command(
            &self,
            command: &DurableExecutionCommand,
            after_send_hook: Option<Box<AfterSendHook>>,
        ) -> Result<bool, HypeEdgeError> {
            let resolved = self
                .results
                .lock()
                .unwrap()
                .get(&command.command_id)
                .copied()
                .unwrap_or(true);
            self.dispatched
                .lock()
                .unwrap()
                .push((command.command_type.clone(), resolved));
            if resolved && let Some(hook) = after_send_hook {
                hook(command);
            }
            Ok(resolved)
        }
    }

    #[derive(Default)]
    struct RecorderInjector {
        phases: StdMutex<Vec<String>>,
    }

    impl FaultInjector for RecorderInjector {
        fn inject(&self, phase: &str, _command: &DurableExecutionCommand) {
            self.phases.lock().unwrap().push(phase.to_string());
        }
    }

    fn command(command_type: &str, requires_resolution: bool) -> DurableExecutionCommand {
        DurableExecutionCommand {
            command_id: uuid::Uuid::new_v4(),
            command_type: command_type.into(),
            payload: serde_json::json!({ "cloid": "mm_1" }),
            attempt_count: 1,
            requires_resolution,
        }
    }

    fn worker_with(
        queue: Arc<dyn DurableCommandQueue>,
        dispatcher: Arc<dyn DurableCommandDispatcher>,
    ) -> SignedActionExecutor {
        SignedActionExecutor::new(queue, dispatcher, 10, Some("w".into()))
    }

    #[tokio::test]
    async fn run_once_returns_false_when_queue_empty() {
        let queue = Arc::new(FakeQueue::new(vec![None]));
        let dispatcher = Arc::new(FakeDispatcher::new());
        let worker = worker_with(queue, dispatcher.clone());
        let processed = worker.run_once().await.unwrap();
        assert!(!processed);
        assert!(dispatcher.dispatched().is_empty());
    }

    #[tokio::test]
    async fn run_once_dispatches_place_order_and_resolves() {
        let cmd = command("place_order", false);
        let queue = Arc::new(FakeQueue::new(vec![Some(cmd.clone())]));
        let dispatcher = Arc::new(FakeDispatcher::new().with_result(cmd.command_id, true));
        let worker = worker_with(queue, dispatcher.clone());
        let processed = worker.run_once().await.unwrap();
        assert!(processed);
        assert_eq!(dispatcher.dispatched(), vec![("place_order".into(), true)]);
    }

    #[tokio::test]
    async fn run_once_dispatches_cancel_order() {
        let cmd = command("cancel_order", false);
        let queue = Arc::new(FakeQueue::new(vec![Some(cmd.clone())]));
        let dispatcher = Arc::new(FakeDispatcher::new().with_result(cmd.command_id, true));
        let worker = worker_with(queue, dispatcher.clone());
        let processed = worker.run_once().await.unwrap();
        assert!(processed);
        assert_eq!(dispatcher.dispatched(), vec![("cancel_order".into(), true)]);
    }

    #[tokio::test]
    async fn unresolved_command_is_deferred() {
        let cmd = command("place_order", true);
        let queue = Arc::new(FakeQueue::new(vec![Some(cmd.clone())]));
        let dispatcher = Arc::new(FakeDispatcher::new().with_result(cmd.command_id, false));
        let worker = worker_with(queue.clone(), dispatcher);
        let processed = worker.run_once().await.unwrap();
        assert!(processed);
        let deferred = queue.deferred();
        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred[0].0, cmd.command_id);
        assert!(deferred[0].1.contains("cloid lookup"));
    }

    #[tokio::test]
    async fn fault_injector_fires_before_and_after_send() {
        let cmd = command("place_order", false);
        let queue = Arc::new(FakeQueue::new(vec![Some(cmd.clone())]));
        let dispatcher = Arc::new(FakeDispatcher::new().with_result(cmd.command_id, true));
        let injector = Arc::new(RecorderInjector::default());
        let worker = worker_with(queue, dispatcher).with_fault_injector(injector.clone());
        worker.run_once().await.unwrap();
        let phases = injector.phases.lock().unwrap().clone();
        assert_eq!(
            phases,
            vec!["before_send".to_string(), "after_send".to_string()]
        );
    }

    #[tokio::test]
    async fn unresolved_command_skips_fault_injector() {
        let cmd = command("place_order", true);
        let queue = Arc::new(FakeQueue::new(vec![Some(cmd.clone())]));
        let dispatcher = Arc::new(FakeDispatcher::new().with_result(cmd.command_id, false));
        let injector = Arc::new(RecorderInjector::default());
        let worker = worker_with(queue, dispatcher).with_fault_injector(injector.clone());
        worker.run_once().await.unwrap();
        assert!(injector.phases.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn run_loop_stops_when_stopped() {
        let queue = Arc::new(FakeQueue::new(vec![None; 100]));
        let dispatcher = Arc::new(FakeDispatcher::new());
        let worker = worker_with(queue, dispatcher);
        worker.stop();
        // With `running` false the loop exits without processing.
        let run = tokio::time::timeout(Duration::from_millis(100), worker.run()).await;
        assert!(run.is_ok(), "run() should return promptly after stop()");
    }

    #[tokio::test]
    async fn run_loop_processes_queue_then_returns_after_stop() {
        let cmd = command("place_order", false);
        let queue = Arc::new(FakeQueue::new(vec![Some(cmd.clone()), None]));
        let dispatcher = Arc::new(FakeDispatcher::new().with_result(cmd.command_id, true));
        let worker = worker_with(queue, dispatcher.clone());
        let run_worker = worker.clone();
        let run = tokio::spawn(async move { run_worker.run().await });
        tokio::time::sleep(Duration::from_millis(5)).await;
        worker.stop();
        run.await.unwrap().unwrap();
        assert_eq!(dispatcher.dispatched(), vec![("place_order".into(), true)]);
    }

    #[tokio::test]
    async fn transient_claim_error_backs_off_and_worker_continues() {
        // H-EX1 regression: one transient DB error must not kill the worker —
        // run() backs off and keeps processing subsequent commands.
        let cmd = command("place_order", false);
        let queue = Arc::new(FakeQueue::new(vec![Some(cmd.clone()), None]));
        queue.claim_errors.store(1, AtomicOrdering::SeqCst);
        let dispatcher = Arc::new(FakeDispatcher::new().with_result(cmd.command_id, true));
        let worker = worker_with(queue.clone(), dispatcher.clone());
        let run_worker = worker.clone();
        let run = tokio::spawn(async move { run_worker.run().await });

        // Give the injected claim failure + 100ms base backoff + retry time.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !dispatcher.dispatched().is_empty(),
            "worker must continue after the transient failure"
        );
        assert_eq!(dispatcher.dispatched(), vec![("place_order".into(), true)]);

        worker.stop();
        run.await.unwrap().unwrap();
    }
}
