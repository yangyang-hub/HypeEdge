"""Tests for alert dispatchers."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hypeedge.monitor.alerts import (
    AlertPayload,
    AlertSeverity,
    CompositeAlertDispatcher,
    DingTalkAlertDispatcher,
    LogAlertDispatcher,
    TelegramAlertDispatcher,
)


class TestLogAlertDispatcher:
    @pytest.mark.asyncio
    async def test_sends_alert(self):
        dispatcher = LogAlertDispatcher()
        # Should not raise
        await dispatcher.send("Test Alert", "Something happened", "warning")

    @pytest.mark.asyncio
    async def test_critical_severity(self):
        dispatcher = LogAlertDispatcher()
        await dispatcher.send("Critical", "System down", "critical")


class TestTelegramAlertDispatcher:
    def test_disabled_when_no_token(self):
        dispatcher = TelegramAlertDispatcher("", "")
        assert dispatcher.is_enabled is False

    def test_enabled_when_configured(self):
        dispatcher = TelegramAlertDispatcher("token123", "chat456")
        assert dispatcher.is_enabled is True

    @pytest.mark.asyncio
    async def test_disabled_skips_send(self):
        dispatcher = TelegramAlertDispatcher("", "")
        # Should not raise or make HTTP calls
        await dispatcher.send("Test", "Message")

    @pytest.mark.asyncio
    async def test_sends_to_telegram(self):
        dispatcher = TelegramAlertDispatcher("token123", "chat456")

        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client_cls.return_value = mock_client

            await dispatcher.send("Test", "Hello", "warning")

            mock_client.post.assert_called_once()
            call_args = mock_client.post.call_args
            assert "sendMessage" in call_args[0][0]
            assert call_args[1]["json"]["chat_id"] == "chat456"


class TestDingTalkAlertDispatcher:
    def test_disabled_when_no_webhook(self):
        dispatcher = DingTalkAlertDispatcher("")
        assert dispatcher.is_enabled is False

    def test_enabled_when_configured(self):
        dispatcher = DingTalkAlertDispatcher("https://oapi.dingtalk.com/robot/send?access_token=xxx")
        assert dispatcher.is_enabled is True


class TestCompositeAlertDispatcher:
    @pytest.mark.asyncio
    async def test_dispatches_structured_payload_to_all(self):
        mock1 = AsyncMock()
        mock2 = AsyncMock()
        dispatcher = CompositeAlertDispatcher([mock1, mock2])
        alert = AlertPayload(
            rule_id="mm.postgres_unavailable",
            title="Postgres unavailable",
            message="Placement disabled.",
            severity=AlertSeverity.CRITICAL,
            labels={"component": "storage"},
        )

        await dispatcher.dispatch(alert)

        mock1.dispatch.assert_called_once_with(alert)
        mock2.dispatch.assert_called_once_with(alert)

    @pytest.mark.asyncio
    async def test_dispatches_to_all(self):
        mock1 = AsyncMock()
        mock2 = AsyncMock()
        dispatcher = CompositeAlertDispatcher([mock1, mock2])

        await dispatcher.send("Test", "Message", "warning")

        mock1.send.assert_called_once_with("Test", "Message", "warning")
        mock2.send.assert_called_once_with("Test", "Message", "warning")

    @pytest.mark.asyncio
    async def test_continues_on_failure(self):
        mock1 = AsyncMock()
        mock1.send.side_effect = Exception("channel 1 failed")
        mock2 = AsyncMock()

        dispatcher = CompositeAlertDispatcher([mock1, mock2])
        # Should not raise — second dispatcher still called
        await dispatcher.send("Test", "Message")

        mock2.send.assert_called_once()
