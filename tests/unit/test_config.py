"""Tests for configuration loading."""

import pydantic
import pytest

from hypeedge.config import loader
from hypeedge.config.loader import load_settings
from hypeedge.config.settings import (
    ActionBudgetSettings,
    AppSettings,
    ExchangeSettings,
    FeatureFlagsSettings,
    MarketMakingSettings,
    RiskSettings,
)


class TestSettings:
    def test_default_settings(self):
        settings = AppSettings()
        assert settings.environment == "dev"
        assert settings.log_level == "INFO"
        assert settings.risk.max_leverage == 5
        assert settings.risk.max_drawdown_pct == 0.10

    def test_environment_properties(self):
        settings = AppSettings(environment="dev")
        assert settings.is_dev is True
        assert settings.is_testnet is False
        assert settings.is_mainnet is False

        settings = AppSettings(environment="testnet")
        assert settings.is_testnet is True

        settings = AppSettings(environment="mainnet")
        assert settings.is_mainnet is True

    def test_risk_settings_validation(self):
        with pytest.raises(pydantic.ValidationError):
            RiskSettings(max_leverage=0)  # Must be >= 1

        with pytest.raises(pydantic.ValidationError):
            RiskSettings(max_drawdown_pct=0.0)  # Must be >= 0.01

    def test_exchange_settings_not_configured_by_default(self):
        settings = ExchangeSettings()
        assert settings.is_configured is False

    def test_exchange_settings_configured(self):
        settings = ExchangeSettings(
            account_address="0x1234",
            agent_private_key="0xdeadbeef",
        )
        assert settings.is_configured is True

    def test_clickhouse_settings(self):
        settings = AppSettings()
        assert settings.clickhouse.host == "localhost"
        assert settings.clickhouse.port == 8123
        assert settings.clickhouse.batch_size == 10_000

    def test_market_data_settings(self):
        settings = AppSettings()
        assert "BTC" in settings.market_data.coins
        assert settings.market_data.l2_book_depth == 20

    def test_environment_variables_override_yaml(self, monkeypatch):
        monkeypatch.setenv("HYPE_LOG_LEVEL", "ERROR")
        monkeypatch.setenv("HYPE_MARKET_DATA__L2_BOOK_DEPTH", "50")
        monkeypatch.setenv("HYPE_EXCHANGE__API_URL", "https://override.example")

        settings = load_settings("dev")

        assert settings.log_level == "ERROR"
        assert settings.market_data.l2_book_depth == 50
        assert settings.exchange.api_url == "https://override.example"

    def test_all_yaml_sections_are_loaded(self):
        settings = load_settings("testnet")

        assert settings.backfill.backfill_window_days == 7
        assert settings.backtest.monte_carlo_simulations == 1000
        assert settings.api.host == "127.0.0.1"
        assert settings.api.port == 8080
        assert settings.features.v2_trading_enabled is True
        assert settings.features.legacy_execution is False
        assert settings.action_budget.ip_weight_limit_per_minute == 1200
        assert settings.action_budget.paid_reserve_enabled is False
        assert settings.market_making.max_quote_levels_per_side == 1
        assert settings.features.market_making_enabled is False

    def test_v2_execution_requires_durable_ledger(self):
        with pytest.raises(pydantic.ValidationError, match="durable_ledger_v2"):
            FeatureFlagsSettings(execution_v2=True)

    def test_legacy_and_v2_execution_are_mutually_exclusive(self):
        with pytest.raises(pydantic.ValidationError, match="mutually exclusive"):
            FeatureFlagsSettings(legacy_execution=True, durable_ledger_v2=True, execution_v2=True)

    def test_requested_environment_is_authoritative(self, monkeypatch):
        monkeypatch.setattr(loader, "load_yaml_config", lambda _environment: {"environment": "dev"})

        settings = load_settings("testnet")

        assert settings.environment == "testnet"

    def test_mainnet_requires_all_secrets_from_environment(self, monkeypatch):
        for name in (
            "HYPE_EXCHANGE__ACCOUNT_ADDRESS",
            "HYPE_EXCHANGE__AGENT_PRIVATE_KEY",
            "HYPE_POSTGRES__URL",
            "HYPE_API__AUTH_TOKEN",
        ):
            monkeypatch.delenv(name, raising=False)

        with pytest.raises(RuntimeError, match="mainnet requires secret environment variables"):
            load_settings("mainnet")

    def test_mainnet_rejects_development_postgres_password(self, monkeypatch):
        monkeypatch.setenv("HYPE_EXCHANGE__ACCOUNT_ADDRESS", "0x1234")
        monkeypatch.setenv("HYPE_EXCHANGE__AGENT_PRIVATE_KEY", "0xdeadbeef")
        monkeypatch.setenv(
            "HYPE_POSTGRES__URL",
            "postgresql+asyncpg://hypeedge:hypeedge@localhost:5432/hypeedge_mainnet",
        )
        monkeypatch.setenv("HYPE_API__AUTH_TOKEN", "a" * 32)

        with pytest.raises(RuntimeError, match="non-default password"):
            load_settings("mainnet")

    def test_mainnet_loads_with_explicit_strong_environment_secrets(self, monkeypatch):
        postgres_url = (
            "postgresql+asyncpg://hypeedge:strong-random-db-secret@db.internal:5432/hypeedge_mainnet?ssl=require"
        )
        api_token = "a" * 32
        monkeypatch.setenv("HYPE_EXCHANGE__ACCOUNT_ADDRESS", "0x1234")
        monkeypatch.setenv("HYPE_EXCHANGE__AGENT_PRIVATE_KEY", "0xdeadbeef")
        monkeypatch.setenv("HYPE_POSTGRES__URL", postgres_url)
        monkeypatch.setenv("HYPE_API__AUTH_TOKEN", api_token)

        settings = load_settings("mainnet")

        assert settings.postgres.url == postgres_url
        assert settings.api.auth_token == api_token

    def test_mainnet_rejects_postgres_without_tls(self, monkeypatch):
        monkeypatch.setenv("HYPE_EXCHANGE__ACCOUNT_ADDRESS", "0x1234")
        monkeypatch.setenv("HYPE_EXCHANGE__AGENT_PRIVATE_KEY", "0xdeadbeef")
        monkeypatch.setenv(
            "HYPE_POSTGRES__URL",
            "postgresql+asyncpg://hypeedge:strong-random-db-secret@db.internal:5432/hypeedge_mainnet",
        )
        monkeypatch.setenv("HYPE_API__AUTH_TOKEN", "a" * 32)

        with pytest.raises(RuntimeError, match="must require TLS"):
            load_settings("mainnet")

    def test_api_tokens_must_be_strong_and_unique(self):
        from hypeedge.config.settings import APISettings

        with pytest.raises(pydantic.ValidationError, match="at least 32"):
            APISettings(viewer_token="short")
        with pytest.raises(pydantic.ValidationError, match="must be unique"):
            APISettings(viewer_token="v" * 32, operator_token="v" * 32)

    def test_action_budget_settings_validate_emergency_reserves(self):
        with pytest.raises(pydantic.ValidationError, match="ip_emergency_reserve"):
            ActionBudgetSettings(ip_weight_limit_per_minute=100, ip_emergency_reserve=100)
        with pytest.raises(pydantic.ValidationError, match="positive single, daily, and monthly"):
            ActionBudgetSettings(paid_reserve_enabled=True)

    def test_market_making_settings_are_safety_ceilings(self):
        with pytest.raises(pydantic.ValidationError, match="strictly faster"):
            MarketMakingSettings(
                account_poll_interval_seconds=1.0,
                near_risk_account_poll_interval_seconds=1.0,
            )
        with pytest.raises(pydantic.ValidationError, match="quote notional ceiling"):
            MarketMakingSettings(
                max_hard_inventory_equity_pct=0.10,
                max_quote_notional_equity_pct=0.11,
            )

    def test_market_making_feature_requires_full_v2_chain(self):
        with pytest.raises(pydantic.ValidationError, match="complete V2 trading chain"):
            FeatureFlagsSettings(market_making_enabled=True)

    def test_dev_credentials_can_never_target_mainnet(self, monkeypatch):
        monkeypatch.setenv("HYPE_EXCHANGE__ACCOUNT_ADDRESS", "0x1234")
        monkeypatch.setenv("HYPE_EXCHANGE__AGENT_PRIVATE_KEY", "0xdeadbeef")
        monkeypatch.setenv("HYPE_EXCHANGE__API_URL", "https://api.hyperliquid.xyz")
        monkeypatch.setenv("HYPE_EXCHANGE__WS_URL", "wss://api.hyperliquid.xyz/ws")

        with pytest.raises(RuntimeError, match="dev trading credentials require the official testnet"):
            load_settings("dev")
