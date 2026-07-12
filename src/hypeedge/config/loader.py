"""YAML configuration file loader."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

import structlog
import yaml

from hypeedge.config.settings import AppSettings

logger = structlog.get_logger(__name__)

CONFIGS_DIR = Path(__file__).resolve().parents[3] / "configs"

MAINNET_REQUIRED_ENV_VARS = (
    "HYPE_EXCHANGE__ACCOUNT_ADDRESS",
    "HYPE_EXCHANGE__AGENT_PRIVATE_KEY",
    "HYPE_POSTGRES__URL",
)
MAINNET_API_TOKEN_ENV_VARS = (
    "HYPE_API__AUTH_TOKEN",
    "HYPE_API__VIEWER_TOKEN",
    "HYPE_API__OPERATOR_TOKEN",
    "HYPE_API__ADMIN_TOKEN",
)
_WEAK_POSTGRES_PASSWORDS = frozenset({"", "changeme", "change-me", "hypeedge", "password", "postgres"})
_MAINNET_API_URL = "https://api.hyperliquid.xyz"
_MAINNET_WS_URL = "wss://api.hyperliquid.xyz/ws"
_TESTNET_API_URL = "https://api.hyperliquid-testnet.xyz"
_TESTNET_WS_URL = "wss://api.hyperliquid-testnet.xyz/ws"
_SUPPORTED_ENVIRONMENTS = frozenset({"dev", "testnet", "mainnet"})


def exchange_urls_for_environment(environment: str) -> tuple[str, str]:
    """Return the official Hyperliquid REST/WS URLs for ``HYPE_ENV``.

    ``dev`` and ``testnet`` both connect to Hyperliquid testnet.
    ``mainnet`` connects to Hyperliquid mainnet.
    """
    if environment == "mainnet":
        return _MAINNET_API_URL, _MAINNET_WS_URL
    if environment in {"dev", "testnet"}:
        return _TESTNET_API_URL, _TESTNET_WS_URL
    raise RuntimeError(f"unsupported HYPE_ENV={environment!r}; expected one of {sorted(_SUPPORTED_ENVIRONMENTS)}")


def load_yaml_config(environment: str | None = None) -> dict[str, Any]:
    """Load YAML config file for the given environment.

    Falls back to 'dev' if environment is not specified.
    """
    env = environment or os.getenv("HYPE_ENV", "dev")
    config_path = CONFIGS_DIR / f"{env}.yaml"

    if not config_path.exists():
        logger.warning("config_file_not_found", path=str(config_path), fallback="defaults")
        return {}

    with open(config_path) as f:
        config = yaml.safe_load(f) or {}

    logger.info("config_loaded", environment=env, path=str(config_path))
    return config if isinstance(config, dict) else {}


def load_settings(environment: str | None = None) -> AppSettings:
    """Load application settings from YAML + environment variables.

    Priority (highest wins), except exchange API/WS URLs:
    1. Environment variables (HYPE_* prefix)
    2. .env file
    3. YAML config file
    4. Defaults in settings classes

    Exchange ``api_url`` / ``ws_url`` are always derived from ``HYPE_ENV``
    (``dev``/``testnet`` → testnet, ``mainnet`` → mainnet) and ignore manual overrides.
    """
    selected_environment: str = environment if environment is not None else os.environ.get("HYPE_ENV", "dev")
    if selected_environment not in _SUPPORTED_ENVIRONMENTS:
        raise RuntimeError(
            f"unsupported HYPE_ENV={selected_environment!r}; expected one of {sorted(_SUPPORTED_ENVIRONMENTS)}"
        )
    yaml_config = load_yaml_config(selected_environment)

    # pydantic-settings handles env vars and .env automatically.
    # We pass YAML values as init kwargs — env vars override them.
    settings = AppSettings(
        environment=selected_environment,
        log_level=yaml_config.get("log_level", "INFO"),
        exchange=yaml_config.get("exchange", {}),
        market_data=yaml_config.get("market_data", {}),
        clickhouse=yaml_config.get("clickhouse", {}),
        postgres=yaml_config.get("postgres", {}),
        risk=yaml_config.get("risk", {}),
        monitor=yaml_config.get("monitor", {}),
        backfill=yaml_config.get("backfill", {}),
        backtest=yaml_config.get("backtest", {}),
        api=yaml_config.get("api", {}),
        features=yaml_config.get("features", {}),
    )
    if settings.environment != selected_environment:
        raise RuntimeError("HYPE_ENV and HYPE_ENVIRONMENT must not select different environments")
    settings = _apply_exchange_urls(settings)
    if settings.is_mainnet:
        _validate_mainnet_environment(settings)
    _validate_exchange_environment(settings)
    return settings


def _apply_exchange_urls(settings: AppSettings) -> AppSettings:
    """Force official Hyperliquid endpoints from ``HYPE_ENV`` (network source of truth)."""
    api_url, ws_url = exchange_urls_for_environment(settings.environment)
    previous_api = settings.exchange.api_url.rstrip("/")
    previous_ws = settings.exchange.ws_url.rstrip("/")
    if previous_api != api_url or previous_ws != ws_url:
        logger.warning(
            "exchange_urls_overridden_by_environment",
            environment=settings.environment,
            previous_api_url=settings.exchange.api_url,
            previous_ws_url=settings.exchange.ws_url,
            api_url=api_url,
            ws_url=ws_url,
        )
    exchange = settings.exchange.model_copy(update={"api_url": api_url, "ws_url": ws_url})
    return settings.model_copy(update={"exchange": exchange})


def _validate_exchange_environment(settings: AppSettings) -> None:
    """Exchange URLs must match the official endpoints for the selected environment."""
    expected_api, expected_ws = exchange_urls_for_environment(settings.environment)
    if settings.exchange.api_url.rstrip("/") != expected_api or settings.exchange.ws_url.rstrip("/") != expected_ws:
        raise RuntimeError(
            f"{settings.environment} requires the official "
            f"{'mainnet' if settings.is_mainnet else 'testnet'} API and WebSocket URLs"
        )


def _validate_mainnet_environment(settings: AppSettings) -> None:
    """Fail closed unless mainnet secrets came from explicit environment variables."""
    missing = [name for name in MAINNET_REQUIRED_ENV_VARS if not os.getenv(name, "").strip()]
    if not any(os.getenv(name, "").strip() for name in MAINNET_API_TOKEN_ENV_VARS):
        missing.append("one of " + "/".join(MAINNET_API_TOKEN_ENV_VARS))
    if missing:
        names = ", ".join(missing)
        raise RuntimeError(f"mainnet requires secret environment variables: {names}")

    admin_tokens = (settings.api.auth_token, settings.api.admin_token)
    if not any(len(token) >= 32 for token in admin_tokens):
        raise RuntimeError("an admin HYPE_API token must contain at least 32 characters on mainnet")

    parsed_url = urlsplit(settings.postgres.url)
    password = unquote(parsed_url.password or "").lower()
    invalid_url = not parsed_url.scheme.startswith("postgresql") or parsed_url.hostname is None
    if invalid_url or password in _WEAK_POSTGRES_PASSWORDS:
        raise RuntimeError("HYPE_POSTGRES__URL must be a valid mainnet URL with a non-default password")
    ssl_mode = parse_qs(parsed_url.query).get("ssl", parse_qs(parsed_url.query).get("sslmode", [""]))[-1]
    if ssl_mode not in {"require", "verify-ca", "verify-full"}:
        raise RuntimeError("mainnet HYPE_POSTGRES__URL must require TLS with ssl=require, verify-ca, or verify-full")
