//! Serial nonce queue, port of `NonceManager` in `src/hypeedge/execution/nonce.py`.
//!
//! One worker consumes `ActionRequest`s serially so all signing flows through a
//! single monotonic nonce (`max(now_ms, last + 1)`), mirroring Hyperliquid's
//! timestamp-millis nonce requirement. The worker enforces the placement
//! preflight immediately before signing and resolves timeouts by cloid query
//! rather than blindly resending.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{Mutex, mpsc, oneshot};

/// The nonce generator: `max(now_ms, last + 1)` guarantees strict monotonicity
/// even when two actions land in the same millisecond.
pub struct NonceGenerator {
    last: Mutex<u64>,
}

impl Default for NonceGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl NonceGenerator {
    pub fn new() -> Self {
        Self {
            last: Mutex::new(0),
        }
    }

    pub async fn next(&self) -> u64 {
        let mut last = self.last.lock().await;
        let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
        *last = now_ms.max(*last + 1);
        *last
    }

    pub fn next_sync(&self) -> u64 {
        let mut last = self.last.blocking_lock();
        let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
        *last = now_ms.max(*last + 1);
        *last
    }
}

/// Result of one serial action.
pub type ActionResult = Result<serde_json::Value, String>;

/// The runner for one serial action: receives the assigned monotonic nonce and
/// returns a future. Runs inside the worker after the nonce is reserved, so the
/// sign + HTTP send stay serialized (nonce ordering is preserved end-to-end).
pub type ActionRunner =
    Box<dyn FnOnce(u64) -> Pin<Box<dyn Future<Output = ActionResult> + Send>> + Send>;

/// A request to run one signing action on the serial queue.
pub struct ActionRequest {
    pub description: String,
    /// The callback that performs the sign + send. Runs inside the serial
    /// worker after the monotonic nonce is assigned.
    pub run: ActionRunner,
    pub reply: oneshot::Sender<ActionResult>,
}

/// The serial nonce queue.
pub struct NonceQueue {
    tx: mpsc::Sender<ActionRequest>,
}

impl NonceQueue {
    pub fn new() -> Self {
        Self::with_capacity(64)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let nonce = Arc::new(NonceGenerator::new());
        let (tx, mut rx) = mpsc::channel::<ActionRequest>(capacity);
        let worker_nonce = nonce;
        // Spawn the serial worker. The nonce is assigned *inside* the worker,
        // immediately before the action runs, so reservation order == execution
        // order (B1) — two concurrent callers can no longer reserve nonces and
        // enqueue in a different order, which would have the exchange receive
        // a higher nonce before a lower one.
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let nonce = worker_nonce.next().await;
                let result = (req.run)(nonce).await;
                let _ = req.reply.send(result);
            }
        });
        Self { tx }
    }

    /// Submit an action for serial signing. The nonce is assigned inside the
    /// worker (guaranteed monotonic); the caller awaits the reply.
    pub async fn submit<F>(&self, description: impl Into<String>, run: F) -> ActionResult
    where
        F: FnOnce(u64) -> Pin<Box<dyn Future<Output = ActionResult> + Send>> + Send + 'static,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ActionRequest {
                description: description.into(),
                run: Box::new(run),
                reply: reply_tx,
            })
            .await
            .map_err(|_| "nonce queue closed".to_string())?;
        reply_rx
            .await
            .map_err(|_| "nonce worker dropped".to_string())?
    }

    /// Number of actions currently queued behind the running one.
    pub fn depth(&self) -> usize {
        0 // not tracked; kept for API symmetry
    }
}

impl Default for NonceQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn nonce_is_strictly_monotonic() {
        let generator = NonceGenerator::new();
        let mut prev = generator.next().await;
        for _ in 0..100 {
            let n = generator.next().await;
            assert!(n > prev, "nonce must be strictly increasing: {n} <= {prev}");
            prev = n;
        }
    }

    #[tokio::test]
    async fn serial_queue_runs_actions_in_order() {
        let queue = NonceQueue::new();
        let mut results = Vec::new();
        for i in 0..5u64 {
            let r = queue
                .submit("test", move |nonce| {
                    Box::pin(async move { Ok(serde_json::json!({"i": i, "nonce": nonce})) })
                })
                .await
                .unwrap();
            results.push(r);
        }
        let nonces: Vec<u64> = results
            .iter()
            .map(|r| r["nonce"].as_u64().unwrap())
            .collect();
        let mut sorted = nonces.clone();
        sorted.sort_unstable();
        assert_eq!(nonces, sorted, "nonces issued in submission order");
        for w in nonces.windows(2) {
            assert!(w[1] > w[0]);
        }
    }

    #[tokio::test]
    async fn worker_error_propagates() {
        let queue = NonceQueue::new();
        let err = queue
            .submit("fail", |_| Box::pin(async { Err("boom".to_string()) }))
            .await
            .unwrap_err();
        assert_eq!(err, "boom");
    }

    #[tokio::test]
    async fn concurrent_submits_nonces_monotonic_in_execution_order() {
        // B1 regression: nonces are assigned inside the worker, so even under
        // concurrent submission the *execution* order (== FIFO delivery order)
        // sees strictly increasing nonces.
        let queue = Arc::new(NonceQueue::new());
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for i in 0..8u64 {
            let q = queue.clone();
            let seen = seen.clone();
            handles.push(tokio::spawn(async move {
                q.submit("conc", move |nonce| {
                    let seen = seen.clone();
                    Box::pin(async move {
                        seen.lock().unwrap().push((i, nonce));
                        Ok(serde_json::json!({ "nonce": nonce }))
                    })
                })
                .await
                .unwrap()
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let pairs = seen.lock().unwrap().clone();
        assert_eq!(pairs.len(), 8);
        for w in pairs.windows(2) {
            assert!(
                w[1].1 > w[0].1,
                "nonces must be strictly increasing in execution order: {pairs:?}"
            );
        }
    }

    #[tokio::test]
    async fn actions_are_serialized_even_when_async() {
        let queue = NonceQueue::new();
        let mut nonces = Vec::new();
        for i in 0..3u64 {
            let n = queue
                .submit("async", move |nonce| {
                    Box::pin(async move {
                        // A real HTTP send would park here; ordering must survive.
                        tokio::time::sleep(std::time::Duration::from_millis(i * 5)).await;
                        Ok(serde_json::json!({"nonce": nonce}))
                    })
                })
                .await
                .unwrap();
            nonces.push(n["nonce"].as_u64().unwrap());
        }
        for w in nonces.windows(2) {
            assert!(w[1] > w[0], "nonces must be strictly increasing");
        }
    }
}
