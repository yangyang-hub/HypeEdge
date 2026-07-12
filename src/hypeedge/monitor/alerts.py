"""Alert dispatchers (design doc §12).

Protocol + implementations for sending alerts to external channels.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol

import structlog

logger = structlog.get_logger(__name__)


class AlertSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass(frozen=True, slots=True)
class AlertPayload:
    """Structured alert contract shared by logs and external dispatchers."""

    rule_id: str
    title: str
    message: str
    severity: AlertSeverity = AlertSeverity.WARNING
    observed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    labels: Mapping[str, str] = field(default_factory=dict)
    runbook_url: str | None = None

    def __post_init__(self) -> None:
        if not self.rule_id or not self.title or not self.message:
            raise ValueError("alert rule_id, title, and message cannot be empty")
        if self.observed_at.tzinfo is None:
            raise ValueError("alert observed_at must be timezone-aware")
        if any(not key or not value for key, value in self.labels.items()):
            raise ValueError("alert labels cannot contain empty keys or values")

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "title": self.title,
            "message": self.message,
            "severity": self.severity.value,
            "observed_at": self.observed_at.astimezone(UTC).isoformat(),
            "labels": dict(sorted(self.labels.items())),
            "runbook_url": self.runbook_url,
        }

    def render_text(self) -> str:
        details = [self.message]
        if self.labels:
            details.append("labels=" + ", ".join(f"{key}={value}" for key, value in sorted(self.labels.items())))
        if self.runbook_url:
            details.append(f"runbook={self.runbook_url}")
        details.append(f"observed_at={self.observed_at.astimezone(UTC).isoformat()}")
        return "\n".join(details)


class AlertDispatcher(Protocol):
    """Protocol for sending alerts (design doc §12).

    Implementations: TelegramAlertDispatcher, DingTalkAlertDispatcher, etc.
    """

    async def dispatch(self, alert: AlertPayload) -> None:
        """Send a structured alert notification."""
        ...

    async def send(self, title: str, message: str, severity: str = "warning") -> None:
        """Send an alert notification."""
        ...


class LogAlertDispatcher:
    """Fallback alert dispatcher that logs alerts (always available)."""

    async def dispatch(self, alert: AlertPayload) -> None:
        log_method = {
            AlertSeverity.INFO: logger.info,
            AlertSeverity.WARNING: logger.warning,
            AlertSeverity.ERROR: logger.error,
            AlertSeverity.CRITICAL: logger.critical,
        }[alert.severity]
        log_method("alert", **alert.to_dict())

    async def send(self, title: str, message: str, severity: str = "warning") -> None:
        await self.dispatch(_legacy_payload(title, message, severity))


class TelegramAlertDispatcher:
    """Telegram Bot alert dispatcher.

    Sends alerts to a Telegram chat via the Bot API.
    Requires: bot_token and chat_id in config.
    """

    def __init__(self, bot_token: str, chat_id: str) -> None:
        self._bot_token = bot_token
        self._chat_id = chat_id
        self._base_url = f"https://api.telegram.org/bot{bot_token}"
        self._enabled = bool(bot_token and chat_id)

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    async def dispatch(self, alert: AlertPayload) -> None:
        if not self._enabled:
            logger.debug("telegram_alert_disabled", rule_id=alert.rule_id, title=alert.title)
            return

        severity_emoji = {
            AlertSeverity.INFO: "ℹ️",
            AlertSeverity.WARNING: "⚠️",
            AlertSeverity.ERROR: "🔴",
            AlertSeverity.CRITICAL: "🚨",
        }
        text = f"{severity_emoji[alert.severity]} {alert.title}\n\n{alert.render_text()}"

        try:
            import httpx

            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    f"{self._base_url}/sendMessage",
                    json={"chat_id": self._chat_id, "text": text},
                )
                if response.status_code == 200:
                    logger.debug("telegram_alert_sent", rule_id=alert.rule_id, title=alert.title)
                else:
                    logger.error(
                        "telegram_alert_failed",
                        rule_id=alert.rule_id,
                        title=alert.title,
                        status=response.status_code,
                        body=response.text[:200],
                    )
        except Exception:
            logger.exception("telegram_alert_error", rule_id=alert.rule_id, title=alert.title)

    async def send(self, title: str, message: str, severity: str = "warning") -> None:
        """Send alert to Telegram chat."""
        await self.dispatch(_legacy_payload(title, message, severity))


class DingTalkAlertDispatcher:
    """DingTalk (钉钉) webhook alert dispatcher.

    Sends alerts via DingTalk custom robot webhook.
    """

    def __init__(self, webhook_url: str, secret: str = "") -> None:
        self._webhook_url = webhook_url
        self._secret = secret
        self._enabled = bool(webhook_url)

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    async def dispatch(self, alert: AlertPayload) -> None:
        if not self._enabled:
            logger.debug("dingtalk_alert_disabled", rule_id=alert.rule_id, title=alert.title)
            return

        try:
            import httpx

            url = self._webhook_url
            if self._secret:
                import base64
                import hashlib
                import hmac
                import time
                from urllib.parse import quote

                timestamp = str(int(time.time() * 1000))
                string_to_sign = f"{timestamp}\n{self._secret}"
                hmac_code = hmac.new(
                    self._secret.encode(),
                    string_to_sign.encode(),
                    digestmod=hashlib.sha256,
                ).digest()
                sign = quote(base64.b64encode(hmac_code))
                url = f"{url}&timestamp={timestamp}&sign={sign}"

            text = f"[{alert.severity.value.upper()}] {alert.title}\n\n{alert.render_text()}"
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    url,
                    json={"msgtype": "text", "text": {"content": text}},
                )
                if response.status_code == 200:
                    logger.debug("dingtalk_alert_sent", rule_id=alert.rule_id, title=alert.title)
                else:
                    logger.error(
                        "dingtalk_alert_failed",
                        rule_id=alert.rule_id,
                        title=alert.title,
                        status=response.status_code,
                    )
        except Exception:
            logger.exception("dingtalk_alert_error", rule_id=alert.rule_id, title=alert.title)

    async def send(self, title: str, message: str, severity: str = "warning") -> None:
        """Send alert to DingTalk webhook."""
        await self.dispatch(_legacy_payload(title, message, severity))


class CompositeAlertDispatcher:
    """Dispatches alerts to multiple channels simultaneously."""

    def __init__(self, dispatchers: list[AlertDispatcher]) -> None:
        self._dispatchers = dispatchers

    async def dispatch(self, alert: AlertPayload) -> None:
        for dispatcher in self._dispatchers:
            try:
                await dispatcher.dispatch(alert)
            except Exception:
                logger.exception(
                    "composite_alert_error",
                    dispatcher=type(dispatcher).__name__,
                    rule_id=alert.rule_id,
                )

    async def send(self, title: str, message: str, severity: str = "warning") -> None:
        """Send alert to all registered dispatchers."""
        for dispatcher in self._dispatchers:
            try:
                await dispatcher.send(title, message, severity)
            except Exception:
                logger.exception("composite_alert_error", dispatcher=type(dispatcher).__name__)


def _legacy_payload(title: str, message: str, severity: str) -> AlertPayload:
    try:
        normalized = AlertSeverity(severity)
    except ValueError:
        normalized = AlertSeverity.WARNING
    return AlertPayload(
        rule_id="legacy.manual",
        title=title,
        message=message,
        severity=normalized,
    )
