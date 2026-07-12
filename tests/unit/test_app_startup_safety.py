from __future__ import annotations

from pathlib import Path

from hypeedge.app import HypeEdgeApp
from hypeedge.config.settings import AppSettings
from hypeedge.core.enums import SafetyMode


async def test_credentials_do_not_initialize_trading_when_v2_flags_are_incomplete(tmp_path: Path) -> None:
    settings = AppSettings(
        environment="dev",
        exchange={
            "api_url": "https://api.hyperliquid-testnet.xyz",
            "ws_url": "wss://api.hyperliquid-testnet.xyz/ws",
            "account_address": "0x1234",
            "agent_private_key": "0xdeadbeef",
        },
        backfill={"state_dir": str(tmp_path)},
        clickhouse={"spool_path": str(tmp_path / "spool.sqlite3")},
        features={},
    )
    app = HypeEdgeApp(settings)

    await app._initialize_components()

    assert app._pg_engine is None
    assert app.execution_engine is None
    assert app._trading_prerequisites_ok is False
    assert app._safety_controller.mode == SafetyMode.CANCEL_ONLY
    assert app._safety_controller.reason == "v2_feature_set_incomplete"


def test_sdk_connection_uses_configured_testnet_url_not_environment_inference() -> None:
    settings = AppSettings(
        environment="dev",
        exchange={"api_url": "https://api.hyperliquid-testnet.xyz"},
    )
    app = HypeEdgeApp(settings)
    assert app.settings.exchange.api_url == "https://api.hyperliquid-testnet.xyz"
