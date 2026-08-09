//! Error hierarchy mirroring `src/hypeedge/core/exceptions.py`.
//!
//! All domain errors are members of [`HypeEdgeError`]. The `reason` strings
//! carried by specific variants are load-bearing: the execution engine, the
//! API layer, and the SSE stream all surface them to callers unchanged.

use crate::enums::OrderStatus;

/// Root error for all HypeEdge domain errors.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum HypeEdgeError {
    // --- Event Bus ---
    #[error("reliable event queue is full: event_type={event_type}")]
    EventBusBackpressure { event_type: String },

    // --- Configuration ---
    #[error("configuration error: {0}")]
    Config(String),

    // --- Market data ---
    #[error("market data error: {0}")]
    MarketData(String),
    #[error("websocket connection lost")]
    WebSocketDisconnected,
    #[error("rate limit exceeded ({limit_type}): {message}")]
    RateLimitExceeded {
        message: String,
        limit_type: String,
        retry_after: Option<f64>,
    },

    // --- Execution ---
    #[error("execution error: {message}")]
    Execution { message: String },
    #[error("order rejected by exchange: {message} (cloid={cloid:?} reason={reason:?})")]
    OrderRejected {
        message: String,
        cloid: Option<String>,
        reason: Option<String>,
    },
    #[error("order submission timed out: {message} (cloid={cloid:?})")]
    OrderTimeout {
        message: String,
        cloid: Option<String>,
    },
    #[error(
        "order cannot be represented by the instrument's trading rules: {message} (symbol={symbol} reason={reason})"
    )]
    OrderNormalization {
        message: String,
        symbol: String,
        reason: String,
    },
    #[error("order signing failed: {message}")]
    Signing { message: String },
    #[error("nonce management error: {message}")]
    Nonce { message: String },

    // --- Risk ---
    #[error("risk check failed: {message}")]
    RiskCheck { message: String },
    #[error("risk check timed out (fail-safe: treated as rejection)")]
    RiskCheckTimeout,
    #[error("kill switch triggered: {message}")]
    KillSwitchTriggered {
        message: String,
        reason: Option<String>,
    },

    // --- Storage ---
    #[error("storage error: {message}")]
    Storage { message: String },
    #[error("clickhouse error: {message}")]
    ClickHouse { message: String },
    #[error("postgres error: {message}")]
    Postgres { message: String },

    // --- Account ---
    #[error("reconciliation error: {message}")]
    Reconciliation { message: String },

    // --- State machine ---
    #[error("invalid transition: {from} -> {to} (cloid={cloid:?})")]
    InvalidStateTransition {
        from: OrderStatus,
        to: OrderStatus,
        cloid: Option<String>,
    },

    // --- Trading command / strategy lifecycle ---
    #[error("trading command error: {message}")]
    TradingCommand { message: String },
    #[error("command id reused with a different normalized payload: {message}")]
    TradingCommandConflict { message: String },
    #[error("durable command boundary unavailable (fail closed): {message}")]
    TradingCommandPersistence { message: String },
    #[error("strategy registration error: {message}")]
    StrategyRegistration { message: String },
    #[error("strategy lifecycle error: {message}")]
    StrategyLifecycle { message: String },
}

impl HypeEdgeError {
    /// A stable machine-readable reason code, used by the API `ApiProblem`
    /// `code` field and the SSE `event_type`. Snake_case, no spaces.
    pub fn code(&self) -> &'static str {
        match self {
            HypeEdgeError::EventBusBackpressure { .. } => "event_bus_backpressure",
            HypeEdgeError::Config(_) => "config_error",
            HypeEdgeError::MarketData(_) => "market_data_error",
            HypeEdgeError::WebSocketDisconnected => "websocket_disconnected",
            HypeEdgeError::RateLimitExceeded { .. } => "rate_limit_exceeded",
            HypeEdgeError::Execution { .. } => "execution_error",
            HypeEdgeError::OrderRejected { .. } => "order_rejected",
            HypeEdgeError::OrderTimeout { .. } => "order_timeout",
            HypeEdgeError::OrderNormalization { .. } => "order_normalization",
            HypeEdgeError::Signing { .. } => "signing_error",
            HypeEdgeError::Nonce { .. } => "nonce_error",
            HypeEdgeError::RiskCheck { .. } => "risk_check_error",
            HypeEdgeError::RiskCheckTimeout => "risk_check_timeout",
            HypeEdgeError::KillSwitchTriggered { .. } => "kill_switch_triggered",
            HypeEdgeError::Storage { .. } => "storage_error",
            HypeEdgeError::ClickHouse { .. } => "clickhouse_error",
            HypeEdgeError::Postgres { .. } => "postgres_error",
            HypeEdgeError::Reconciliation { .. } => "reconciliation_error",
            HypeEdgeError::InvalidStateTransition { .. } => "invalid_state_transition",
            HypeEdgeError::TradingCommand { .. } => "trading_command_error",
            HypeEdgeError::TradingCommandConflict { .. } => "trading_command_conflict",
            HypeEdgeError::TradingCommandPersistence { .. } => "trading_command_persistence",
            HypeEdgeError::StrategyRegistration { .. } => "strategy_registration_error",
            HypeEdgeError::StrategyLifecycle { .. } => "strategy_lifecycle_error",
        }
    }
}

// Convenience constructors used throughout the codebase.
impl HypeEdgeError {
    pub fn order_rejected(
        message: impl Into<String>,
        cloid: Option<String>,
        reason: Option<String>,
    ) -> Self {
        HypeEdgeError::OrderRejected {
            message: message.into(),
            cloid,
            reason,
        }
    }

    pub fn order_timeout(message: impl Into<String>, cloid: Option<String>) -> Self {
        HypeEdgeError::OrderTimeout {
            message: message.into(),
            cloid,
        }
    }

    pub fn order_normalization(message: impl Into<String>, symbol: String, reason: String) -> Self {
        HypeEdgeError::OrderNormalization {
            message: message.into(),
            symbol,
            reason,
        }
    }

    pub fn kill_switch_triggered(message: impl Into<String>, reason: Option<String>) -> Self {
        HypeEdgeError::KillSwitchTriggered {
            message: message.into(),
            reason,
        }
    }

    pub fn postgres(message: impl Into<String>) -> Self {
        HypeEdgeError::Postgres {
            message: message.into(),
        }
    }

    pub fn clickhouse(message: impl Into<String>) -> Self {
        HypeEdgeError::ClickHouse {
            message: message.into(),
        }
    }
}
