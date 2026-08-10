//! HypeEdge application: boots the config, event bus, market data, kill
//! switch, and the HTTP API server.

use std::sync::Arc;

use hypeedge_api::AppState;
use hypeedge_config::loader::load_settings;
use hypeedge_config::settings::AppSettings;
use hypeedge_infra::event_bus::EventBus;
use hypeedge_trading::market_data::BookManager;
use hypeedge_trading::risk::KillSwitch;

pub mod runtime;

/// The running application components.
pub struct HypeEdgeApp {
    pub settings: Arc<AppSettings>,
    pub event_bus: Arc<EventBus>,
    pub kill_switch: Arc<KillSwitch>,
    pub books: Arc<tokio::sync::Mutex<BookManager>>,
    /// The wired trading runtime (execution, account, market data, strategies).
    pub runtime: runtime::RuntimeWiring,
}

impl HypeEdgeApp {
    /// Build the full trading runtime (6d/6e/6f) asynchronously. When the V2
    /// chain is disabled or the runtime build fails, falls back to the
    /// control-plane-only wiring.
    pub async fn build(settings: AppSettings) -> Self {
        let settings = Arc::new(settings);
        let event_bus = Arc::new(EventBus::new(10_000));
        let runtime = match runtime::build_runtime(&settings, event_bus.clone()).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "runtime_build_failed_falling_back_to_control_plane");
                runtime::build_control_plane(&settings, event_bus.clone())
            }
        };
        let kill_switch = runtime.kill_switch.clone();
        let books = runtime.books.clone();
        Self {
            settings,
            event_bus,
            kill_switch,
            books,
            runtime,
        }
    }

    /// Synchronous constructor: always the control-plane wiring (used by tests
    /// and the pre-connect path). Use [`HypeEdgeApp::build`] for the full runtime.
    pub fn new(settings: AppSettings) -> Self {
        let settings = Arc::new(settings);
        let event_bus = Arc::new(EventBus::new(10_000));
        let runtime = runtime::build_control_plane(&settings, event_bus.clone());
        let kill_switch = runtime.kill_switch.clone();
        let books = runtime.books.clone();
        Self {
            settings,
            event_bus,
            kill_switch,
            books,
            runtime,
        }
    }

    /// Build the API state and axum router.
    pub fn router(&self) -> axum::Router {
        let base_state = AppState::new(
            self.settings.clone(),
            self.kill_switch.clone(),
            self.event_bus.clone(),
            self.books.clone(),
        );
        let api_state = AppState::from_wiring(
            base_state,
            self.runtime.execution.clone(),
            self.runtime.market_data.clone(),
            self.runtime.config_versions.clone(),
            self.runtime.trading_enabled.clone(),
            self.runtime.safety_mode.clone(),
            self.runtime.sse_outbox.clone(),
            self.runtime.sse_pool.clone(),
        );
        hypeedge_api::build_router(api_state)
    }

    /// Run the HTTP server until shutdown (C9: graceful shutdown on
    /// SIGINT/SIGTERM so in-flight requests drain instead of an abrupt kill).
    pub async fn serve(&self) -> Result<(), String> {
        let api = &self.settings.api;
        let addr = format!("{}:{}", api.host, api.port);
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .map_err(|e| format!("bind {addr}: {e}"))?;
        let router = self.router();
        tracing::info!(addr = %addr, "api_server_started");
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|e| format!("serve: {e}"))
    }
}

/// Wait for SIGINT (Ctrl-C) or SIGTERM, then signal graceful shutdown.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = %e, "ctrl_c handler failed");
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => tracing::warn!(error = %e, "SIGTERM handler failed"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown_signal_received");
}

/// Build settings from the environment (mirrors `hypeedge.__main__`). When
/// `HYPE_ENV` is unset the loader falls back to `.env` / "dev".
pub fn load_app_settings() -> Result<AppSettings, String> {
    load_settings(std::env::var("HYPE_ENV").ok().as_deref()).map_err(|e| e.to_string())
}

/// Convenience: settings defaults for tests (dev).
pub fn dev_settings() -> AppSettings {
    load_settings(Some("dev")).expect("dev settings load")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn build_runtime_returns_control_plane_when_v2_disabled() {
        // 6d/6e/6f: with the V2 chain disabled, build_runtime must return a
        // control-plane-only wiring (no execution engine, trading disabled)
        // without panicking.
        let settings = dev_settings();
        let bus = Arc::new(EventBus::new(10_000));
        let wiring = runtime::build_runtime(&settings, bus.clone()).await.unwrap();
        assert!(wiring.execution.is_none(), "v2 disabled → no engine");
        assert!(!*wiring.trading_enabled.read().await, "trading disabled");
        assert!(wiring.market_data.is_none());
        let app = HypeEdgeApp::new(settings);
        let router = app.router();
        let resp = router
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn build_runtime_errors_without_exchange_credentials() {
        // The runtime refuses to trade when exchange credentials are unset even
        // if the V2 chain is enabled.
        let mut settings = dev_settings();
        settings.features = hypeedge_config::settings::FeatureFlagsSettings {
            execution_v2: true,
            durable_ledger_v2: true,
            user_stream_v2: true,
            reconciliation_v2: true,
            api_v1: true,
            strategy_runner_v2: true,
            market_making_enabled: true,
            funding_arb_execution_enabled: true,
            legacy_execution: false,
        };
        settings.exchange.account_address = String::new();
        settings.exchange.agent_private_key = String::new();
        let bus = Arc::new(EventBus::new(10_000));
        let result = runtime::build_runtime(&settings, bus).await;
        assert!(
            result.is_err(),
            "trading enabled without exchange credentials must error"
        );
    }

    #[tokio::test]
    async fn router_serves_health() {
        let settings = dev_settings();
        let app = HypeEdgeApp::new(settings);
        let router = app.router();

        let resp = router
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("\"status\":\"ok\""), "health body: {text}");
    }

    #[tokio::test]
    async fn router_serves_system_status() {
        let settings = dev_settings();
        let app = HypeEdgeApp::new(settings);
        let router = app.router();

        let resp = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/system/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("\"ok\":true"), "system status body: {text}");
        assert!(
            text.contains("\"environment\":\"dev\""),
            "env in body: {text}"
        );
    }

    #[tokio::test]
    async fn mutation_without_idempotency_key_rejected() {
        let settings = dev_settings();
        let app = HypeEdgeApp::new(settings);
        let router = app.router();

        let resp = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/kill-switch")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"action":"trigger"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("IDEMPOTENCY_KEY_REQUIRED"), "body: {text}");
    }

    #[tokio::test]
    async fn kill_switch_trigger_with_idempotency() {
        let settings = dev_settings();
        let app = HypeEdgeApp::new(settings);
        let router = app.router();

        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/kill-switch")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "test-key-1")
                    .body(Body::from(r#"{"action":"trigger"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("\"action\":\"triggered\""), "body: {text}");
        assert!(
            app.kill_switch.is_active().await,
            "kill switch should be latched"
        );
    }

    #[tokio::test]
    async fn kill_switch_reset_actually_clears_latch() {
        // A22 regression: `POST /api/v1/kill-switch {action:"reset"}` must
        // clear the latch (it was a no-op that reported success).
        let settings = dev_settings();
        let app = HypeEdgeApp::new(settings);
        let router = app.router();
        let trigger = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/kill-switch")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "ks-trigger")
                    .body(Body::from(r#"{"action":"trigger"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(trigger.status(), StatusCode::OK);
        assert!(app.kill_switch.is_active().await);

        let reset = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/kill-switch")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "ks-reset")
                    .body(Body::from(r#"{"action":"reset"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(reset.status(), StatusCode::OK);
        assert!(
            !app.kill_switch.is_active().await,
            "reset must actually clear the latch (A22)"
        );
    }

    #[tokio::test]
    async fn viewer_token_cannot_trigger_kill_switch() {
        // A23 regression: a viewer credential must not be able to halt trading.
        let mut settings = dev_settings();
        settings.api.viewer_token = "viewer-token-12345678901234567890123456789012".into();
        settings.api.operator_token = "".into();
        settings.api.admin_token = "".into();
        settings.api.auth_token = "".into();
        let app = HypeEdgeApp::new(settings);
        let router = app.router();

        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/kill-switch")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "viewer-ks")
                    .header(
                        "authorization",
                        "Bearer viewer-token-12345678901234567890123456789012",
                    )
                    .body(Body::from(r#"{"action":"trigger"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "viewer must be forbidden from the kill switch (A23)"
        );
        assert!(
            !app.kill_switch.is_active().await,
            "kill switch must remain inactive after a viewer attempt"
        );
    }

    /// Helper: send a JSON request body through the router.
    async fn send_json(
        router: &axum::Router,
        method: &str,
        uri: &str,
        body: &str,
    ) -> (StatusCode, String) {
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .header("idempotency-key", "strat-key")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&body).to_string())
    }

    #[tokio::test]
    async fn strategy_lifecycle_create_list_start_stop() {
        let settings = dev_settings();
        let app = HypeEdgeApp::new(settings);
        let router = app.router();

        // Empty list initially.
        let (status, body) = send_json(&router, "GET", "/api/v1/strategies", "").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"data\":[]"), "empty strategies: {body}");

        // Create a trend_follow strategy.
        let (status, body) = send_json(
            &router,
            "POST",
            "/api/v1/strategies",
            r#"{"strategy_id":"tf_1","strategy_type":"trend_follow","symbol":"BTC","initial_config":{"fast_ema_period":12}}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "create body: {body}");
        assert!(
            body.contains("\"strategy_id\":\"tf_1\""),
            "create body: {body}"
        );
        assert!(body.contains("\"strategy_type\":\"trend_follow\""));

        // List now has one.
        let (_, body) = send_json(&router, "GET", "/api/v1/strategies", "").await;
        assert!(body.contains("tf_1"), "list body: {body}");

        // Get by id.
        let (status, body) = send_json(&router, "GET", "/api/v1/strategies/tf_1", "").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("tf_1"));

        // Start → running.
        let (status, body) = send_json(
            &router,
            "POST",
            "/api/v1/strategies/tf_1/actions/start",
            r#"{"target":"running"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "start body: {body}");
        assert!(
            body.contains("\"actual_state\":\"running\""),
            "start body: {body}"
        );

        // Pause → paused.
        let (status, body) = send_json(
            &router,
            "POST",
            "/api/v1/strategies/tf_1/actions/pause",
            "{}",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "pause body: {body}");
        assert!(
            body.contains("\"actual_state\":\"paused\""),
            "pause body: {body}"
        );

        // Stop → stopped.
        let (status, body) = send_json(
            &router,
            "POST",
            "/api/v1/strategies/tf_1/actions/stop",
            "{}",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "stop body: {body}");

        // Unknown action → 404.
        let (status, body) = send_json(
            &router,
            "POST",
            "/api/v1/strategies/tf_1/actions/frobnicate",
            "{}",
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body.contains("STRATEGY_ACTION_NOT_FOUND"));
    }

    #[tokio::test]
    async fn unknown_strategy_lifecycle_conflict() {
        let settings = dev_settings();
        let app = HypeEdgeApp::new(settings);
        let router = app.router();

        // Starting a nonexistent strategy → conflict (unknown strategy).
        let (status, body) = send_json(
            &router,
            "POST",
            "/api/v1/strategies/nope/actions/start",
            r#"{"target":"running"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "body: {body}");
        assert!(body.contains("STRATEGY_LIFECYCLE_CONFLICT"));
    }
}
