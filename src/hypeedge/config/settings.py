"""Pydantic-settings configuration models for HypeEdge."""

from __future__ import annotations

from decimal import Decimal

from pydantic import Field, ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict


class HypeSettings(BaseSettings):
    """Base settings with env/.env overriding YAML init values."""

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return env_settings, dotenv_settings, init_settings, file_secret_settings


class ExchangeSettings(HypeSettings):
    """Hyperliquid exchange connection settings."""

    model_config = SettingsConfigDict(env_prefix="HYPE_EXCHANGE__", env_file=".env", extra="ignore")

    api_url: str = "https://api.hyperliquid-testnet.xyz"
    ws_url: str = "wss://api.hyperliquid-testnet.xyz/ws"
    account_address: str = ""
    agent_private_key: str = ""
    sub_account: str | None = None

    @field_validator("account_address", "agent_private_key")
    @classmethod
    def validate_not_empty_on_mainnet(cls, v: str, info: ValidationInfo) -> str:
        """Allow empty in dev, but warn."""
        _ = info
        return v

    @property
    def is_configured(self) -> bool:
        """Check if exchange credentials are set."""
        return bool(self.account_address and self.agent_private_key)


class MarketDataSettings(HypeSettings):
    """Market data collection settings."""

    model_config = SettingsConfigDict(env_prefix="HYPE_MARKET_DATA__", env_file=".env", extra="ignore")

    coins: list[str] = Field(default=["BTC", "ETH", "SOL"])
    ws_subscriptions: list[str] = Field(default=["l2Book", "trades", "candle", "allMids", "activeAssetCtx"])
    candle_intervals: list[str] = Field(default=["1m"])
    l2_book_depth: int = Field(default=20, ge=1, le=100)
    ws_reconnect_delay_min: float = Field(default=1.0, ge=0.1)
    ws_reconnect_delay_max: float = Field(default=30.0, ge=1.0)
    rest_poll_interval: float = Field(default=10.0, ge=1.0)
    backfill_batch_size: int = Field(default=500, ge=10, le=5000)


class ExternalReferenceSettings(HypeSettings):
    """Deployment-wide safety limits for optional external reference prices."""

    model_config = SettingsConfigDict(env_prefix="HYPE_EXTERNAL_REFERENCE__", env_file=".env", extra="ignore")

    external_reference_enabled: bool = False
    spot_ws_url: str = "wss://stream.binance.com:9443/stream"
    perpetual_ws_url: str = "wss://fstream.binance.com/stream"
    symbol_map: dict[str, str] = Field(default={"BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT"})
    spot_weight: Decimal = Field(default=Decimal("0.40"), ge=Decimal("0"), le=Decimal("1"))
    perpetual_weight: Decimal = Field(default=Decimal("0.60"), ge=Decimal("0"), le=Decimal("1"))
    max_external_weight: Decimal = Field(default=Decimal("0.35"), ge=Decimal("0"), le=Decimal("0.50"))
    basis_ewma_alpha: Decimal = Field(default=Decimal("0.02"), gt=Decimal("0"), le=Decimal("0.20"))
    stale_after_ms: int = Field(default=1500, ge=100, le=10_000)
    max_perp_spot_divergence_bps: Decimal = Field(default=Decimal("25"), ge=Decimal("1"), le=Decimal("500"))
    max_mark_book_divergence_bps: Decimal = Field(default=Decimal("25"), ge=Decimal("1"), le=Decimal("500"))
    reconnect_delay_min_seconds: float = Field(default=1.0, ge=0.1, le=10.0)
    reconnect_delay_max_seconds: float = Field(default=30.0, ge=1.0, le=60.0)
    max_symbols: int = Field(default=20, ge=1, le=100)

    @model_validator(mode="after")
    def validate_external_reference_limits(self) -> ExternalReferenceSettings:
        if self.spot_weight + self.perpetual_weight != Decimal("1"):
            raise ValueError("external spot and perpetual weights must sum to 1")
        if self.reconnect_delay_min_seconds > self.reconnect_delay_max_seconds:
            raise ValueError("external reconnect minimum cannot exceed maximum")
        if len(self.symbol_map) > self.max_symbols:
            raise ValueError("external symbol map exceeds max_symbols safety limit")
        normalized = [symbol.upper() for symbol in self.symbol_map.values()]
        if len(normalized) != len(set(normalized)):
            raise ValueError("external venue symbols must be unique")
        return self


class ClickHouseSettings(HypeSettings):
    """ClickHouse connection settings."""

    model_config = SettingsConfigDict(env_prefix="HYPE_CLICKHOUSE__", env_file=".env", extra="ignore")

    host: str = "localhost"
    port: int = Field(default=8123, ge=1, le=65535)
    username: str = "default"
    password: str = ""
    database: str = "hypeedge"
    batch_size: int = Field(default=10_000, ge=100, le=1_000_000)
    flush_interval: float = Field(default=5.0, ge=0.1, le=60.0)
    spool_path: str = "data/clickhouse_spool.sqlite3"


class PostgresSettings(HypeSettings):
    """Postgres connection settings."""

    model_config = SettingsConfigDict(env_prefix="HYPE_POSTGRES__", env_file=".env", extra="ignore")

    url: str = "postgresql+asyncpg://hypeedge:hypeedge@localhost:5432/hypeedge"
    pool_size: int = Field(default=5, ge=1, le=50)
    command_poll_interval_ms: int = Field(default=100, ge=10, le=5000)
    command_lease_seconds: int = Field(default=15, ge=5, le=300)
    unknown_recheck_seconds: int = Field(default=5, ge=1, le=300)
    risk_reservation_ttl_seconds: int = Field(default=86400, ge=60, le=604800)


class RiskSettings(HypeSettings):
    """Risk management settings (design doc §8)."""

    model_config = SettingsConfigDict(env_prefix="HYPE_RISK__", env_file=".env", extra="ignore")

    max_position_pct: float = Field(default=0.20, ge=0.01, le=0.50)
    max_strategy_loss_pct: float = Field(default=0.05, ge=0.01, le=0.20)
    max_drawdown_pct: float = Field(default=0.10, ge=0.01, le=0.30)
    max_leverage: int = Field(default=5, ge=1, le=20)
    risk_check_timeout_ms: int = Field(default=500, ge=100, le=5000)
    market_price_stale_seconds: float = Field(default=5.0, ge=0.1, le=60.0)
    action_credits_low_watermark: int = Field(default=1000, ge=100)
    reserve_weight_cost_limit: float = Field(default=10.0, ge=0.0)  # USDC/day for reserveRequestWeight
    kill_switch_enabled: bool = True


class ActionBudgetSettings(HypeSettings):
    """Address-action, cancel-headroom, and IP-weight safety budgets."""

    model_config = SettingsConfigDict(env_prefix="HYPE_ACTION_BUDGET__", env_file=".env", extra="ignore")

    remote_snapshot_max_age_seconds: float = Field(default=60.0, ge=5.0, le=600.0)
    remote_poll_interval_normal_seconds: float = Field(default=30.0, ge=5.0, le=120.0)
    remote_poll_interval_conserve_seconds: float = Field(default=15.0, ge=2.0, le=60.0)
    remote_poll_interval_critical_seconds: float = Field(default=5.0, ge=1.0, le=30.0)

    address_conserve_threshold: int = Field(default=3000, ge=0)
    address_critical_threshold: int = Field(default=1500, ge=0)
    address_cancel_only_threshold: int = Field(default=500, ge=0)
    cancel_retry_buffer: int = Field(default=10, ge=0, le=100_000)
    close_action_reserve: int = Field(default=5, ge=0, le=100_000)
    cancel_headroom_initial: int = Field(default=10_000, ge=0)

    ip_weight_limit_per_minute: int = Field(default=1200, ge=1)
    ip_emergency_reserve: int = Field(default=100, ge=0)
    runway_conserve_hours: float = Field(default=24.0, ge=0.0)
    runway_critical_hours: float = Field(default=6.0, ge=0.0)
    runway_cancel_only_hours: float = Field(default=1.0, ge=0.0)
    minimum_marginal_usdc_per_action: float = Field(default=1.25, ge=0.0)
    minimum_actions_for_economic_gate: int = Field(default=20, ge=1)

    paid_reserve_enabled: bool = False
    paid_reserve_cost_per_request_usdc: float = Field(default=0.0005, ge=0.0)
    paid_reserve_max_single_usdc: float = Field(default=0.0, ge=0.0)
    paid_reserve_max_daily_usdc: float = Field(default=0.0, ge=0.0)
    paid_reserve_max_monthly_usdc: float = Field(default=0.0, ge=0.0)

    @model_validator(mode="after")
    def validate_budget_thresholds(self) -> ActionBudgetSettings:
        if not (
            self.address_cancel_only_threshold <= self.address_critical_threshold <= self.address_conserve_threshold
        ):
            raise ValueError("address thresholds must satisfy cancel_only <= critical <= conserve")
        if not (
            self.remote_poll_interval_critical_seconds
            <= self.remote_poll_interval_conserve_seconds
            <= self.remote_poll_interval_normal_seconds
        ):
            raise ValueError("budget polling intervals must get shorter as pressure increases")
        if not self.runway_cancel_only_hours <= self.runway_critical_hours <= self.runway_conserve_hours:
            raise ValueError("runway thresholds must satisfy cancel_only <= critical <= conserve")
        if self.ip_emergency_reserve >= self.ip_weight_limit_per_minute:
            raise ValueError("ip_emergency_reserve must be below the per-minute IP limit")
        if (
            self.paid_reserve_enabled
            and min(
                self.paid_reserve_max_single_usdc,
                self.paid_reserve_max_daily_usdc,
                self.paid_reserve_max_monthly_usdc,
            )
            <= 0
        ):
            raise ValueError("enabled paid reserve requires positive single, daily, and monthly limits")
        if self.paid_reserve_max_single_usdc > self.paid_reserve_max_daily_usdc:
            raise ValueError("paid reserve single limit cannot exceed daily limit")
        if self.paid_reserve_max_daily_usdc > self.paid_reserve_max_monthly_usdc:
            raise ValueError("paid reserve daily limit cannot exceed monthly limit")
        return self


class MarketMakingSettings(HypeSettings):
    """Global market-making safety ceilings and control-plane defaults.

    Per-strategy quoting parameters intentionally live in immutable Postgres
    config versions.  These values are deployment-wide hard ceilings only.
    """

    model_config = SettingsConfigDict(env_prefix="HYPE_MARKET_MAKING__", env_file=".env", extra="ignore")

    max_active_strategies: int = Field(default=1, ge=1, le=100)
    max_quote_levels_per_side: int = Field(default=1, ge=1, le=10)
    max_hard_inventory_equity_pct: float = Field(default=0.15, gt=0.0, le=0.50)
    max_quote_notional_equity_pct: float = Field(default=0.05, gt=0.0, le=0.25)
    account_poll_interval_seconds: float = Field(default=3.0, ge=0.5, le=5.0)
    near_risk_account_poll_interval_seconds: float = Field(default=1.0, ge=0.5, le=2.0)
    full_reconciliation_interval_seconds: float = Field(default=300.0, ge=30.0, le=3600.0)
    unknown_order_sla_seconds: float = Field(default=15.0, ge=1.0, le=300.0)
    emergency_cancel_wal_path: str = "data/emergency_cancel.jsonl"
    shadow_min_utc_days: int = Field(default=14, ge=1, le=365)
    testnet_soak_min_days: int = Field(default=14, ge=1, le=365)
    canary_observation_min_days: int = Field(default=30, ge=1, le=365)

    @model_validator(mode="after")
    def validate_market_making_safety(self) -> MarketMakingSettings:
        if self.near_risk_account_poll_interval_seconds >= self.account_poll_interval_seconds:
            raise ValueError("near-risk account polling must be strictly faster than normal polling")
        if self.max_quote_notional_equity_pct > self.max_hard_inventory_equity_pct:
            raise ValueError("quote notional ceiling cannot exceed the hard inventory ceiling")
        return self


class MonitorSettings(HypeSettings):
    """Monitoring settings."""

    model_config = SettingsConfigDict(env_prefix="HYPE_MONITOR__", env_file=".env", extra="ignore")

    prometheus_port: int = Field(default=9090, ge=1024, le=65535)


class BackfillSettings(HypeSettings):
    """Backfill state and data integrity settings (Phase 1B)."""

    model_config = SettingsConfigDict(env_prefix="HYPE_BACKFILL__", env_file=".env", extra="ignore")

    state_dir: str = "data"
    backfill_window_days: int = Field(default=7, ge=1, le=90)
    refresh_interval_hours: float = Field(default=6.0, ge=0.5, le=48.0)
    quality_check_interval_hours: float = Field(default=1.0, ge=0.1, le=24.0)
    dedup_max_keys: int = Field(default=1_000_000, ge=10_000)


class BacktestSettings(HypeSettings):
    """Backtest framework settings (design doc §6)."""

    model_config = SettingsConfigDict(env_prefix="HYPE_BACKTEST__", env_file=".env", extra="ignore")

    initial_capital: float = Field(default=10_000.0, ge=100.0)
    default_maker_rebate_pct: float = Field(default=-0.0002)
    default_taker_fee_pct: float = Field(default=0.0005, ge=0.0)
    slippage_optimistic_bps: float = Field(default=2.0, ge=0.0)
    slippage_pessimistic_bps: float = Field(default=10.0, ge=0.0)
    walk_forward_train_days: int = Field(default=60, ge=7)
    walk_forward_validate_days: int = Field(default=30, ge=7)
    walk_forward_step_days: int = Field(default=30, ge=1)
    monte_carlo_simulations: int = Field(default=1000, ge=100)


class APISettings(HypeSettings):
    """FastAPI HTTP API settings."""

    model_config = SettingsConfigDict(env_prefix="HYPE_API__", env_file=".env", extra="ignore")

    host: str = "127.0.0.1"
    port: int = Field(default=37001, ge=1024, le=65535)
    cors_origins: list[str] = Field(
        default_factory=lambda: [
            "http://localhost:34001",
            "http://127.0.0.1:34001",
        ]
    )
    # ``auth_token`` is retained as a backwards-compatible admin token. New
    # deployments should use the role-specific tokens below.
    auth_token: str = ""
    viewer_token: str = ""
    operator_token: str = ""
    admin_token: str = ""
    request_rate_limit_per_minute: int = Field(default=600, ge=10, le=100_000)
    mutation_rate_limit_per_minute: int = Field(default=60, ge=1, le=10_000)
    auth_failure_limit_per_minute: int = Field(default=10, ge=1, le=1_000)
    market_ws_max_connections: int = Field(default=100, ge=1, le=10_000)
    market_ws_max_connections_per_ip: int = Field(default=5, ge=1, le=1_000)
    market_ws_queue_size: int = Field(default=64, ge=1, le=1_000)
    market_ws_messages_per_second: int = Field(default=50, ge=1, le=1_000)

    @model_validator(mode="after")
    def validate_role_tokens(self) -> APISettings:
        configured = [
            token for token in (self.viewer_token, self.operator_token, self.admin_token, self.auth_token) if token
        ]
        if any(len(token) < 32 for token in configured):
            raise ValueError("every configured API token must contain at least 32 characters")
        if len(configured) != len(set(configured)):
            raise ValueError("configured API role tokens must be unique")
        return self


class FeatureFlagsSettings(HypeSettings):
    """V2 cut-over flags. Trading stays disabled unless the full V2 chain is enabled."""

    model_config = SettingsConfigDict(env_prefix="HYPE_FEATURES__", env_file=".env", extra="ignore")

    legacy_execution: bool = False
    durable_ledger_v2: bool = False
    execution_v2: bool = False
    user_stream_v2: bool = False
    reconciliation_v2: bool = False
    api_v1: bool = False
    strategy_runner_v2: bool = False
    market_making_enabled: bool = False

    @model_validator(mode="after")
    def validate_cutover(self) -> FeatureFlagsSettings:
        if self.legacy_execution and self.execution_v2:
            raise ValueError("legacy_execution and execution_v2 are mutually exclusive")
        if self.execution_v2 and not self.durable_ledger_v2:
            raise ValueError("execution_v2 requires durable_ledger_v2")
        if self.user_stream_v2 and not self.durable_ledger_v2:
            raise ValueError("user_stream_v2 requires durable_ledger_v2")
        if self.reconciliation_v2 and not self.durable_ledger_v2:
            raise ValueError("reconciliation_v2 requires durable_ledger_v2")
        if self.strategy_runner_v2 and not self.execution_v2:
            raise ValueError("strategy_runner_v2 requires execution_v2")
        if self.market_making_enabled and not self.v2_trading_enabled:
            raise ValueError("market_making_enabled requires the complete V2 trading chain")
        return self

    @property
    def v2_trading_enabled(self) -> bool:
        """Whether every safety-critical V2 trading component is selected."""
        return all(
            (
                self.durable_ledger_v2,
                self.execution_v2,
                self.user_stream_v2,
                self.reconciliation_v2,
                self.strategy_runner_v2,
            )
        )


class AppSettings(HypeSettings):
    """Top-level application settings. Composes all sub-settings."""

    model_config = SettingsConfigDict(env_prefix="HYPE_", env_file=".env", env_nested_delimiter="__", extra="ignore")

    environment: str = Field(default="dev", pattern=r"^(dev|testnet|mainnet)$")
    log_level: str = Field(default="INFO", pattern=r"^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")

    exchange: ExchangeSettings = Field(default_factory=ExchangeSettings)
    market_data: MarketDataSettings = Field(default_factory=MarketDataSettings)
    external_reference: ExternalReferenceSettings = Field(default_factory=ExternalReferenceSettings)
    clickhouse: ClickHouseSettings = Field(default_factory=ClickHouseSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    risk: RiskSettings = Field(default_factory=RiskSettings)
    action_budget: ActionBudgetSettings = Field(default_factory=ActionBudgetSettings)
    market_making: MarketMakingSettings = Field(default_factory=MarketMakingSettings)
    monitor: MonitorSettings = Field(default_factory=MonitorSettings)
    backfill: BackfillSettings = Field(default_factory=BackfillSettings)
    backtest: BacktestSettings = Field(default_factory=BacktestSettings)
    api: APISettings = Field(default_factory=APISettings)
    features: FeatureFlagsSettings = Field(default_factory=FeatureFlagsSettings)

    @property
    def is_dev(self) -> bool:
        return self.environment == "dev"

    @property
    def is_testnet(self) -> bool:
        return self.environment == "testnet"

    @property
    def is_mainnet(self) -> bool:
        return self.environment == "mainnet"
