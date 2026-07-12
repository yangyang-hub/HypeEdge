"""Custom exceptions for HypeEdge."""


class HypeEdgeError(Exception):
    """Base exception for all HypeEdge errors."""


# --- Event Bus ---


class EventBusBackpressureError(HypeEdgeError):
    """A reliable event could not be delivered without dropping data."""

    def __init__(self, *, event_type: str, queue_id: int) -> None:
        self.event_type = event_type
        self.queue_id = queue_id
        super().__init__(f"Reliable event queue is full: event_type={event_type} queue_id={queue_id}")


# --- Configuration ---


class ConfigError(HypeEdgeError):
    """Configuration loading or validation error."""


# --- Market Data ---


class MarketDataError(HypeEdgeError):
    """Market data connection or processing error."""


class WebSocketDisconnectedError(MarketDataError):
    """WebSocket connection lost."""


class RateLimitExceededError(MarketDataError):
    """Rate limit exceeded (IP weight or address action quota)."""

    def __init__(self, message: str, *, limit_type: str = "unknown", retry_after: float | None = None) -> None:
        super().__init__(message)
        self.limit_type = limit_type
        self.retry_after = retry_after


# --- Execution ---


class ExecutionError(HypeEdgeError):
    """Order execution error."""


class OrderRejectedError(ExecutionError):
    """Order rejected by exchange."""

    def __init__(self, message: str, *, cloid: str | None = None, reason: str | None = None) -> None:
        super().__init__(message)
        self.cloid = cloid
        self.reason = reason


class OrderTimeoutError(ExecutionError):
    """Order submission timed out."""

    def __init__(self, message: str, *, cloid: str | None = None) -> None:
        super().__init__(message)
        self.cloid = cloid


class OrderNormalizationError(ExecutionError):
    """An order cannot be represented by the instrument's exact trading rules."""

    def __init__(self, message: str, *, symbol: str, reason: str) -> None:
        super().__init__(message)
        self.symbol = symbol
        self.reason = reason


class SigningError(ExecutionError):
    """Order signing failed (nonce, wallet, etc.)."""


class NonceError(ExecutionError):
    """Nonce management error (conflict, stale, etc.)."""


# --- Risk ---


class RiskCheckError(HypeEdgeError):
    """Risk check failed (not rejection — actual error in the check)."""


class RiskCheckTimeoutError(RiskCheckError):
    """Risk check timed out (fail-safe: treat as rejection)."""


class KillSwitchTriggeredError(HypeEdgeError):
    """Kill switch has been triggered. All trading must stop."""

    def __init__(self, message: str = "Kill switch triggered", *, reason: str | None = None) -> None:
        super().__init__(message)
        self.reason = reason


# --- Storage ---


class StorageError(HypeEdgeError):
    """Database storage error."""


class ClickHouseError(StorageError):
    """ClickHouse write or query error."""


class PostgresError(StorageError):
    """Postgres write or query error."""


# --- Account ---


class ReconciliationError(HypeEdgeError):
    """State reconciliation mismatch."""


# --- State Machine ---


class InvalidStateTransition(HypeEdgeError):
    """Attempted an illegal order state transition."""

    def __init__(self, *, from_status: str, to_status: str, cloid: str | None = None) -> None:
        self.from_status = from_status
        self.to_status = to_status
        self.cloid = cloid
        super().__init__(f"Invalid transition: {from_status} -> {to_status} (cloid={cloid})")


# --- Trading command / strategy lifecycle ---


class TradingCommandError(HypeEdgeError):
    """A unified trading command could not be admitted or persisted."""


class TradingCommandConflictError(TradingCommandError):
    """A command id was reused with a different normalized payload."""


class TradingCommandPersistenceError(TradingCommandError):
    """The durable command boundary was unavailable (fail closed)."""


class StrategyRegistrationError(HypeEdgeError):
    """A strategy type or instance registration is invalid."""


class StrategyLifecycleError(HypeEdgeError):
    """A strategy lifecycle operation failed or was not permitted."""
