"""Prometheus metrics for HypeEdge (Phase 1 + Phase 2 metrics)."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import structlog
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from hypeedge.config.settings import AppSettings
from hypeedge.core.events import (
    EVENT_KILL_SWITCH_TRIGGERED,
    EVENT_ORDER_CANCELLED,
    EVENT_ORDER_FILLED,
    EVENT_ORDER_REJECTED,
    EVENT_ORDER_SUBMITTED,
    EVENT_RECONCILIATION_COMPLETE,
    EVENT_RISK_CHECK_FAILED,
    EVENT_RISK_CHECK_PASSED,
    EVENT_SIGNAL_GENERATED,
    EVENT_WS_CONNECTED,
    EVENT_WS_DISCONNECTED,
    Event,
    EventBus,
)

if TYPE_CHECKING:
    pass

logger = structlog.get_logger(__name__)

# --- Metric Definitions ---

# WebSocket
ws_connected = Gauge(
    "hype_ws_connected",
    "WebSocket connection status (1=connected, 0=disconnected)",
)
ws_messages_total = Counter(
    "hype_ws_messages_total",
    "Total WebSocket messages received",
    ["channel"],
)
ws_message_latency_seconds = Histogram(
    "hype_ws_message_latency_seconds",
    "WebSocket message processing latency",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

# Market data
events_published_total = Counter(
    "hype_events_published_total",
    "Total events published to EventBus",
    ["event_type"],
)
events_dropped_total = Counter(
    "hype_events_dropped_total",
    "Total events dropped due to full queues",
)

# Rate limits
action_credits_remaining = Gauge(
    "hype_action_credits_remaining",
    "Remaining Hyperliquid action credits",
)
ip_weight_remaining = Gauge(
    "hype_ip_weight_remaining",
    "Remaining IP weight budget",
)

# ClickHouse
ch_rows_written_total = Counter(
    "hype_ch_rows_written_total",
    "Total rows written to ClickHouse",
    ["table"],
)
ch_flush_errors_total = Counter(
    "hype_ch_flush_errors_total",
    "Total ClickHouse flush errors",
    ["table"],
)

# Data quality (Phase 1B)
data_quality_gaps_total = Counter(
    "hype_data_quality_gaps_total",
    "Total data gaps detected",
    ["table", "coin"],
)
data_quality_duplicates_total = Counter(
    "hype_data_quality_duplicates_total",
    "Total duplicate rows detected",
    ["table", "coin"],
)
data_quality_anomalies_total = Counter(
    "hype_data_quality_anomalies_total",
    "Total data anomalies detected (bid>=ask, high<low, etc.)",
    ["table", "coin"],
)

# Application
app_info = Gauge(
    "hype_app_info",
    "Application info",
    ["version", "environment"],
)

# --- Phase 2: Trading Metrics ---

# Orders
orders_submitted_total = Counter(
    "hype_orders_submitted_total",
    "Total orders submitted",
    ["symbol", "side"],
)
orders_filled_total = Counter(
    "hype_orders_filled_total",
    "Total orders filled",
    ["symbol", "side"],
)
orders_cancelled_total = Counter(
    "hype_orders_cancelled_total",
    "Total orders cancelled",
    ["symbol"],
)
orders_rejected_total = Counter(
    "hype_orders_rejected_total",
    "Total orders rejected",
    ["symbol", "reason"],
)

# Risk checks
risk_checks_total = Counter(
    "hype_risk_checks_total",
    "Total risk checks performed",
    ["result"],  # "pass" or "fail"
)

# Strategy signals
signals_generated_total = Counter(
    "hype_signals_generated_total",
    "Total strategy signals generated",
    ["strategy_id", "action"],
)

# Kill switch
kill_switch_triggered_total = Counter(
    "hype_kill_switch_triggered_total",
    "Total kill switch triggers",
)

# Reconciliation
reconciliation_total = Counter(
    "hype_reconciliation_total",
    "Total reconciliation cycles",
    ["result"],  # "success" or "failure"
)
reconciliation_corrections_total = Counter(
    "hype_reconciliation_corrections_total",
    "Total state corrections made by reconciler",
    ["type"],  # "orders" or "positions"
)

# Trading status
trading_enabled = Gauge(
    "hype_trading_enabled",
    "Whether trading is currently enabled (1=yes, 0=no)",
)


class MetricsCollector:
    """Collects metrics from the EventBus and serves Prometheus endpoints."""

    def __init__(self, settings: AppSettings, event_bus: EventBus) -> None:
        self._settings = settings
        self._event_bus = event_bus
        self._port = settings.monitor.prometheus_port

    async def serve(self) -> None:
        """Start the Prometheus HTTP metrics server and collect events."""
        # Start Prometheus HTTP server in background
        start_http_server(self._port)
        logger.info("metrics_server_started", port=self._port)

        # Set static info
        app_info.labels(version="0.2.0", environment=self._settings.environment).set(1)

        # Subscribe to all events for counting
        audit_queue = self._event_bus.subscribe_all()

        try:
            while True:
                event: Event = await audit_queue.get()
                events_published_total.labels(event_type=event.event_type).inc()

                # Update specific gauges and counters
                if event.event_type == EVENT_WS_CONNECTED:
                    ws_connected.set(1)
                elif event.event_type == EVENT_WS_DISCONNECTED:
                    ws_connected.set(0)

                # Phase 2: Trading events
                elif event.event_type == EVENT_ORDER_SUBMITTED:
                    order = event.payload
                    orders_submitted_total.labels(
                        symbol=str(getattr(order, "symbol", "unknown")),
                        side=str(getattr(order, "side", "unknown")),
                    ).inc()
                elif event.event_type == EVENT_ORDER_FILLED:
                    order = event.payload
                    orders_filled_total.labels(
                        symbol=str(getattr(order, "symbol", "unknown")),
                        side=str(getattr(order, "side", "unknown")),
                    ).inc()
                elif event.event_type == EVENT_ORDER_CANCELLED:
                    order = event.payload
                    orders_cancelled_total.labels(
                        symbol=str(getattr(order, "symbol", "unknown")),
                    ).inc()
                elif event.event_type == EVENT_ORDER_REJECTED:
                    order = event.payload
                    orders_rejected_total.labels(
                        symbol=str(getattr(order, "symbol", "unknown")),
                        reason=str(getattr(order, "error_message", "unknown") or "unknown")[:20],
                    ).inc()
                elif event.event_type == EVENT_RISK_CHECK_PASSED:
                    risk_checks_total.labels(result="pass").inc()
                elif event.event_type == EVENT_RISK_CHECK_FAILED:
                    risk_checks_total.labels(result="fail").inc()
                elif event.event_type == EVENT_SIGNAL_GENERATED:
                    signal = event.payload
                    signals_generated_total.labels(
                        strategy_id=str(getattr(signal, "strategy_id", "unknown")),
                        action=str(getattr(signal, "action", "unknown")),
                    ).inc()
                elif event.event_type == EVENT_KILL_SWITCH_TRIGGERED:
                    kill_switch_triggered_total.inc()
                elif event.event_type == EVENT_RECONCILIATION_COMPLETE:
                    result = event.payload
                    success = getattr(result, "success", False)
                    reconciliation_total.labels(
                        result="success" if success else "failure",
                    ).inc()
                    if success:
                        orders_corr = getattr(result, "orders_corrected", 0)
                        pos_corr = getattr(result, "positions_corrected", 0)
                        if orders_corr > 0:
                            reconciliation_corrections_total.labels(type="orders").inc(orders_corr)
                        if pos_corr > 0:
                            reconciliation_corrections_total.labels(type="positions").inc(pos_corr)

        except asyncio.CancelledError:
            pass

    def set_trading_enabled(self, enabled: bool) -> None:
        """Update the trading_enabled gauge."""
        trading_enabled.set(1 if enabled else 0)
