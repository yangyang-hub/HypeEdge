"""Enumerations used across HypeEdge modules."""

from enum import StrEnum


class Side(StrEnum):
    """Order side."""

    BUY = "buy"
    SELL = "sell"


class OrderType(StrEnum):
    """Order type on Hyperliquid."""

    LIMIT = "limit"
    MARKET = "market"  # Implemented as aggressive limit
    STOP_MARKET = "stop_market"
    STOP_LIMIT = "stop_limit"


class TimeInForce(StrEnum):
    """Time-in-force for orders."""

    GTC = "Gtc"  # Good till cancelled
    IOC = "Ioc"  # Immediate or cancel
    ALO = "Alo"  # Add liquidity only (post-only)
    GTX = "Gtx"  # Good till crossing (post-only variant)


class OrderStatus(StrEnum):
    """Order lifecycle states (see design doc §9.2)."""

    PENDING = "pending"  # Strategy intent, not yet submitted
    SUBMITTED = "submitted"  # Sent to exchange, awaiting ack
    SUBMIT_UNKNOWN = "submit_unknown"  # Timed out; exchange truth not yet known
    ACKNOWLEDGED = "acknowledged"  # Exchange confirmed, resting on book
    PARTIAL_FILL = "partial_fill"  # Partially filled
    CANCEL_UNKNOWN = "cancel_unknown"  # Cancel requested; exchange truth not yet known
    FILLED = "filled"  # Fully filled
    CANCELLED = "cancelled"  # Cancelled (strategy or engine)
    REJECTED = "rejected"  # Exchange rejected
    EXPIRED = "expired"  # expiresAfter triggered (5x penalty!)


# Legal state transitions: OrderStatus -> set of allowed next states
ORDER_TRANSITIONS: dict[OrderStatus, set[OrderStatus]] = {
    OrderStatus.PENDING: {OrderStatus.SUBMITTED, OrderStatus.REJECTED, OrderStatus.CANCELLED},
    OrderStatus.SUBMITTED: {
        OrderStatus.ACKNOWLEDGED,
        OrderStatus.SUBMIT_UNKNOWN,
        OrderStatus.REJECTED,
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.CANCEL_UNKNOWN,
    },
    OrderStatus.SUBMIT_UNKNOWN: {
        OrderStatus.ACKNOWLEDGED,
        OrderStatus.PARTIAL_FILL,
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.CANCEL_UNKNOWN,
        OrderStatus.REJECTED,
    },
    OrderStatus.ACKNOWLEDGED: {
        OrderStatus.PARTIAL_FILL,
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.CANCEL_UNKNOWN,
        OrderStatus.EXPIRED,
    },
    OrderStatus.PARTIAL_FILL: {
        OrderStatus.PARTIAL_FILL,
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.CANCEL_UNKNOWN,
        OrderStatus.EXPIRED,
    },
    OrderStatus.CANCEL_UNKNOWN: {
        OrderStatus.ACKNOWLEDGED,
        OrderStatus.PARTIAL_FILL,
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
    },
    OrderStatus.FILLED: set(),  # Terminal
    OrderStatus.CANCELLED: set(),  # Terminal
    OrderStatus.REJECTED: set(),  # Terminal
    OrderStatus.EXPIRED: set(),  # Terminal
}

TERMINAL_STATES = {
    OrderStatus.FILLED,
    OrderStatus.CANCELLED,
    OrderStatus.REJECTED,
    OrderStatus.EXPIRED,
}


class StrategyStatus(StrEnum):
    """Strategy lifecycle states."""

    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    PAUSED = "paused"  # Degraded mode (e.g. risk data unavailable)
    ERROR = "error"
    STOPPING = "stopping"


class MarketMakerLifecycle(StrEnum):
    """Persistent market-maker instance lifecycle."""

    STOPPED = "stopped"
    WARMING = "warming"
    SHADOW = "shadow"
    RUNNING = "running"
    PAUSED = "paused"
    DRAINING = "draining"
    FAULTED = "faulted"


class QuoteDecision(StrEnum):
    """Desired decision for a logical quote slot."""

    QUOTE = "quote"
    KEEP = "keep"
    NO_QUOTE = "no_quote"


class QuoteAction(StrEnum):
    """Minimal transition from authoritative live state to desired state."""

    KEEP = "keep"
    PLACE = "place"
    CANCEL = "cancel"
    MODIFY = "modify"
    CANCEL_THEN_PLACE = "cancel_then_place"
    NO_ACTION = "no_action"
    BLOCKED_UNKNOWN = "blocked_unknown"


class ActionBudgetMode(StrEnum):
    """Address action-budget operating mode."""

    NORMAL = "normal"
    CONSERVE = "conserve"
    CRITICAL = "critical"
    CANCEL_ONLY = "cancel_only"
    EXHAUSTED = "exhausted"


class SafetyMode(StrEnum):
    """Global trading permission state."""

    STARTING = "starting"
    RECONCILING = "reconciling"
    NORMAL = "normal"
    REDUCE_ONLY = "reduce_only"
    CANCEL_ONLY = "cancel_only"
    HALTING = "halting"
    HALTED = "halted"
    RECOVERING = "recovering"


class MarginMode(StrEnum):
    """Margin mode for sub-accounts."""

    CROSS = "cross"
    ISOLATED = "isolated"


class WsChannel(StrEnum):
    """Hyperliquid WebSocket subscription channels."""

    L2_BOOK = "l2Book"
    TRADES = "trades"
    CANDLE = "candle"
    ALL_MIDS = "allMids"
    ACTIVE_ASSET_CTX = "activeAssetCtx"
    USER_FILLS = "userFills"
    ORDER_UPDATES = "orderUpdates"
