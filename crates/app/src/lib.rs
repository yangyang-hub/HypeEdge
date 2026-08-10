//! HypeEdge application: boots the config, event bus, market data, kill
//! switch, and the HTTP API server.

use std::sync::Arc;

use hypeedge_api::AppState;
use hypeedge_config::loader::load_settings;
use hypeedge_config::settings::AppSettings;
use hypeedge_infra::event_bus::EventBus;
use hypeedge_trading::market_data::BookManager;
use hypeedge_trading::risk::KillSwitch;

/// The running application components.
pub struct HypeEdgeApp {
    pub settings: Arc<AppSettings>,
    pub event_bus: Arc<EventBus>,
    pub kill_switch: Arc<KillSwitch>,
    pub books: Arc<tokio::sync::Mutex<BookManager>>,
}

impl HypeEdgeApp {
    pub fn new(settings: AppSettings) -> Self {
        let settings = Arc::new(settings);
        let event_bus = Arc::new(EventBus::new(10_000));
        let kill_switch = Arc::new(KillSwitch::new(
            event_bus.clone(),
            settings.risk.kill_switch_enabled,
        ));
        let books = Arc::new(tokio::sync::Mutex::new(BookManager::new(
            settings.market_data.l2_book_depth as usize,
        )));
        Self {
            settings,
            event_bus,
            kill_switch,
            books,
        }
    }

    /// Build the API state and axum router.
    pub fn router(&self) -> axum::Router {
        let api_state = AppState::new(
            self.settings.clone(),
            self.kill_switch.clone(),
            self.event_bus.clone(),
            self.books.clone(),
        );
        hypeedge_api::build_router(api_state)
    }

    /// Run the HTTP server until shutdown.
    pub async fn serve(&self) -> Result<(), String> {
        let api = &self.settings.api;
        let addr = format!("{}:{}", api.host, api.port);
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .map_err(|e| format!("bind {addr}: {e}"))?;
        let router = self.router();
        tracing::info!(addr = %addr, "api_server_started");
        axum::serve(listener, router)
            .await
            .map_err(|e| format!("serve: {e}"))
    }
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
