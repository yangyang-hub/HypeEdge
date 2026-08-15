//! Configuration settings mirroring `src/hypeedge/config/settings.py`.
//!
//! Every field name, default, and cross-field validator is ported 1:1 so the
//! three environment YAMLs (`configs/{dev,testnet,mainnet}.yaml`) deserialize
//! unchanged. Validation that Python runs in `model_validator(mode="after")`
//! lives in [`Settings::validate`].

use std::collections::HashMap;
use std::fmt;

use hypeedge_domain::Decimal;
use serde::{Deserialize, Serialize};
use url::Url;

/// A base-10 number that appears in config. Accepts a YAML number (int/float)
/// or a decimal string, mirroring how pydantic's `Decimal` coerces.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct ConfigDecimal(pub Decimal);

impl<'de> Deserialize<'de> for ConfigDecimal {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<ConfigDecimal, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            I(i64),
            F(f64),
            S(String),
            U(u64),
        }
        let raw = Raw::deserialize(d)?;
        let dec = match raw {
            Raw::I(i) => Decimal::from_i128(i as i128),
            Raw::U(u) => Decimal::from_i128(u as i128),
            Raw::F(f) => Decimal::from_f64(f).map_err(serde::de::Error::custom)?,
            Raw::S(s) => Decimal::from_str_lenient(&s).map_err(serde::de::Error::custom)?,
        };
        Ok(ConfigDecimal(dec))
    }
}

impl std::ops::Deref for ConfigDecimal {
    type Target = Decimal;
    fn deref(&self) -> &Decimal {
        &self.0
    }
}

impl Default for ConfigDecimal {
    fn default() -> Self {
        ConfigDecimal(Decimal::ZERO)
    }
}

/// Render a secret for `Debug` output: `<unset>` when empty, otherwise a
/// `<redacted:abcd>` marker carrying only the first 4 chars (mirrors the
/// `ExchangeSettings` pattern). Structured logging of settings must never
/// leak signing keys, DB passwords, or API tokens into the log stream.
fn redact_secret(secret: &str) -> String {
    if secret.is_empty() {
        "<unset>".to_string()
    } else {
        format!("<redacted:{}>", secret.chars().take(4).collect::<String>())
    }
}

/// Mask the password component of a `postgresql://user:pass@host:port/db`
/// URL so `Debug` output of [`PostgresSettings`] never leaks the DB credential.
fn redact_url_password(url: &str) -> String {
    let Ok(parsed) = Url::parse(url) else {
        return "<invalid url>".to_string();
    };
    let has_password = parsed.password().is_some();
    let mut out = format!("{}://", parsed.scheme());
    if !parsed.username().is_empty() || has_password {
        out.push_str(parsed.username());
        out.push(':');
        out.push_str(if has_password { "***" } else { "" });
        out.push('@');
    }
    if let Some(host) = parsed.host_str() {
        out.push_str(host);
        if let Some(port) = parsed.port() {
            out.push(':');
            out.push_str(&port.to_string());
        }
    }
    out.push_str(parsed.path());
    if let Some(query) = parsed.query() {
        out.push('?');
        out.push_str(query);
    }
    out
}

/// Hyperliquid exchange connection settings.
#[derive(Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExchangeSettings {
    pub api_url: String,
    pub ws_url: String,
    pub account_address: String,
    pub agent_private_key: String,
}

/// `Debug` redacts the agent private key so structured logging of settings
/// never leaks the signing key into the log stream.
impl fmt::Debug for ExchangeSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExchangeSettings")
            .field("api_url", &self.api_url)
            .field("ws_url", &self.ws_url)
            .field("account_address", &self.account_address)
            .field("agent_private_key", &redact_secret(&self.agent_private_key))
            .finish()
    }
}

impl Default for ExchangeSettings {
    fn default() -> Self {
        Self {
            api_url: "https://api.hyperliquid-testnet.xyz".into(),
            ws_url: "wss://api.hyperliquid-testnet.xyz/ws".into(),
            account_address: String::new(),
            agent_private_key: String::new(),
        }
    }
}

impl ExchangeSettings {
    /// Whether exchange credentials are set.
    pub fn is_configured(&self) -> bool {
        !self.account_address.is_empty() && !self.agent_private_key.is_empty()
    }
}

/// Market data collection settings.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MarketDataSettings {
    pub coins: Vec<String>,
    pub spot_coins: Vec<String>,
    pub ws_subscriptions: Vec<String>,
    pub candle_intervals: Vec<String>,
    pub l2_book_depth: u32,
    pub ws_reconnect_delay_min: f64,
    pub ws_reconnect_delay_max: f64,
    pub rest_poll_interval: f64,
    pub backfill_batch_size: u32,
}

impl Default for MarketDataSettings {
    fn default() -> Self {
        Self {
            coins: vec!["BTC".into(), "ETH".into(), "SOL".into()],
            spot_coins: vec![],
            ws_subscriptions: vec![
                "l2Book".into(),
                "trades".into(),
                "candle".into(),
                "allMids".into(),
                "activeAssetCtx".into(),
            ],
            candle_intervals: vec!["1m".into()],
            l2_book_depth: 20,
            ws_reconnect_delay_min: 1.0,
            ws_reconnect_delay_max: 30.0,
            rest_poll_interval: 10.0,
            backfill_batch_size: 500,
        }
    }
}

/// Deployment-wide safety limits for optional external reference prices.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExternalReferenceSettings {
    pub external_reference_enabled: bool,
    pub spot_ws_url: String,
    pub perpetual_ws_url: String,
    pub symbol_map: HashMap<String, String>,
    pub spot_weight: ConfigDecimal,
    pub perpetual_weight: ConfigDecimal,
    pub max_external_weight: ConfigDecimal,
    pub basis_ewma_alpha: ConfigDecimal,
    pub stale_after_ms: u32,
    pub max_perp_spot_divergence_bps: ConfigDecimal,
    pub max_mark_book_divergence_bps: ConfigDecimal,
    pub reconnect_delay_min_seconds: f64,
    pub reconnect_delay_max_seconds: f64,
    pub max_symbols: u32,
}

impl Default for ExternalReferenceSettings {
    fn default() -> Self {
        Self {
            external_reference_enabled: false,
            spot_ws_url: "wss://stream.binance.com:9443/stream".into(),
            perpetual_ws_url: "wss://fstream.binance.com/stream".into(),
            symbol_map: HashMap::from([
                ("BTC".into(), "BTCUSDT".into()),
                ("ETH".into(), "ETHUSDT".into()),
                ("SOL".into(), "SOLUSDT".into()),
            ]),
            spot_weight: ConfigDecimal(Decimal::from_str_lenient("0.40").unwrap()),
            perpetual_weight: ConfigDecimal(Decimal::from_str_lenient("0.60").unwrap()),
            max_external_weight: ConfigDecimal(Decimal::from_str_lenient("0.35").unwrap()),
            basis_ewma_alpha: ConfigDecimal(Decimal::from_str_lenient("0.02").unwrap()),
            stale_after_ms: 1500,
            max_perp_spot_divergence_bps: ConfigDecimal(Decimal::from_str_lenient("25").unwrap()),
            max_mark_book_divergence_bps: ConfigDecimal(Decimal::from_str_lenient("25").unwrap()),
            reconnect_delay_min_seconds: 1.0,
            reconnect_delay_max_seconds: 30.0,
            max_symbols: 20,
        }
    }
}

/// ClickHouse connection settings.
#[derive(Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClickHouseSettings {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
    pub batch_size: u32,
    pub flush_interval: f64,
    pub spool_path: String,
}

/// `Debug` redacts the password (M-CF2).
impl fmt::Debug for ClickHouseSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseSettings")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &redact_secret(&self.password))
            .field("database", &self.database)
            .field("batch_size", &self.batch_size)
            .field("flush_interval", &self.flush_interval)
            .field("spool_path", &self.spool_path)
            .finish()
    }
}

impl Default for ClickHouseSettings {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 8123,
            username: "default".into(),
            password: String::new(),
            database: "hypeedge".into(),
            batch_size: 10_000,
            flush_interval: 5.0,
            spool_path: "data/clickhouse_spool.sqlite3".into(),
        }
    }
}

/// Postgres connection settings.
#[derive(Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PostgresSettings {
    pub url: String,
    pub pool_size: u32,
    pub command_poll_interval_ms: u32,
    pub command_lease_seconds: u32,
    pub unknown_recheck_seconds: u32,
    pub risk_reservation_ttl_seconds: u32,
}

/// `Debug` redacts the password embedded in the connection URL (M-CF2).
impl fmt::Debug for PostgresSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresSettings")
            .field("url", &redact_url_password(&self.url))
            .field("pool_size", &self.pool_size)
            .field("command_poll_interval_ms", &self.command_poll_interval_ms)
            .field("command_lease_seconds", &self.command_lease_seconds)
            .field("unknown_recheck_seconds", &self.unknown_recheck_seconds)
            .field(
                "risk_reservation_ttl_seconds",
                &self.risk_reservation_ttl_seconds,
            )
            .finish()
    }
}

impl Default for PostgresSettings {
    fn default() -> Self {
        Self {
            // Plain `postgresql://` scheme: the `postgresql+asyncpg://`
            // dialect prefix is a Python/SQLAlchemy-ism and is not a scheme
            // sqlx understands (it currently parses anyway only because
            // `PgConnectOptions::from_str` does not validate the scheme).
            url: "postgresql://hypeedge:hypeedge@localhost:5432/hypeedge".into(),
            pool_size: 5,
            command_poll_interval_ms: 100,
            command_lease_seconds: 15,
            unknown_recheck_seconds: 5,
            risk_reservation_ttl_seconds: 86_400,
        }
    }
}

/// Risk management settings (design doc §8).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RiskSettings {
    pub max_position_pct: f64,
    pub max_strategy_loss_pct: f64,
    pub max_drawdown_pct: f64,
    pub max_leverage: u32,
    pub risk_check_timeout_ms: u32,
    pub market_price_stale_seconds: f64,
    pub action_credits_low_watermark: u64,
    /// USDC/day for `reserveRequestWeight`.
    pub reserve_weight_cost_limit: f64,
    pub kill_switch_enabled: bool,
}

impl Default for RiskSettings {
    fn default() -> Self {
        Self {
            max_position_pct: 0.20,
            max_strategy_loss_pct: 0.05,
            max_drawdown_pct: 0.10,
            max_leverage: 5,
            risk_check_timeout_ms: 500,
            market_price_stale_seconds: 5.0,
            action_credits_low_watermark: 1000,
            reserve_weight_cost_limit: 10.0,
            kill_switch_enabled: true,
        }
    }
}

/// Address-action, cancel-headroom, and IP-weight safety budgets.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ActionBudgetSettings {
    pub remote_snapshot_max_age_seconds: f64,
    pub remote_poll_interval_normal_seconds: f64,
    pub remote_poll_interval_conserve_seconds: f64,
    pub remote_poll_interval_critical_seconds: f64,
    pub address_conserve_threshold: u64,
    pub address_critical_threshold: u64,
    pub address_cancel_only_threshold: u64,
    pub cancel_retry_buffer: u64,
    pub close_action_reserve: u64,
    pub cancel_headroom_initial: u64,
    pub ip_weight_limit_per_minute: u64,
    pub ip_emergency_reserve: u64,
    pub runway_conserve_hours: f64,
    pub runway_critical_hours: f64,
    pub runway_cancel_only_hours: f64,
    pub minimum_marginal_usdc_per_action: f64,
    pub minimum_actions_for_economic_gate: u32,
    pub paid_reserve_enabled: bool,
    pub paid_reserve_cost_per_request_usdc: f64,
    pub paid_reserve_max_single_usdc: f64,
    pub paid_reserve_max_daily_usdc: f64,
    pub paid_reserve_max_monthly_usdc: f64,
}

impl Default for ActionBudgetSettings {
    fn default() -> Self {
        Self {
            remote_snapshot_max_age_seconds: 60.0,
            remote_poll_interval_normal_seconds: 30.0,
            remote_poll_interval_conserve_seconds: 15.0,
            remote_poll_interval_critical_seconds: 5.0,
            address_conserve_threshold: 3000,
            address_critical_threshold: 1500,
            address_cancel_only_threshold: 500,
            cancel_retry_buffer: 10,
            close_action_reserve: 5,
            cancel_headroom_initial: 10_000,
            ip_weight_limit_per_minute: 1200,
            ip_emergency_reserve: 100,
            runway_conserve_hours: 24.0,
            runway_critical_hours: 6.0,
            runway_cancel_only_hours: 1.0,
            minimum_marginal_usdc_per_action: 1.25,
            minimum_actions_for_economic_gate: 20,
            paid_reserve_enabled: false,
            paid_reserve_cost_per_request_usdc: 0.0005,
            paid_reserve_max_single_usdc: 0.0,
            paid_reserve_max_daily_usdc: 0.0,
            paid_reserve_max_monthly_usdc: 0.0,
        }
    }
}

/// Global market-making safety ceilings and control-plane defaults.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MarketMakingSettings {
    pub max_active_strategies: u32,
    pub max_quote_levels_per_side: u32,
    pub max_hard_inventory_equity_pct: f64,
    pub max_quote_notional_equity_pct: f64,
    pub account_poll_interval_seconds: f64,
    pub near_risk_account_poll_interval_seconds: f64,
    pub full_reconciliation_interval_seconds: f64,
    pub unknown_order_sla_seconds: f64,
    pub emergency_cancel_wal_path: String,
    pub shadow_min_utc_days: u32,
    pub testnet_soak_min_days: u32,
    pub canary_observation_min_days: u32,
}

impl Default for MarketMakingSettings {
    fn default() -> Self {
        Self {
            max_active_strategies: 1,
            max_quote_levels_per_side: 1,
            max_hard_inventory_equity_pct: 0.15,
            max_quote_notional_equity_pct: 0.05,
            account_poll_interval_seconds: 3.0,
            near_risk_account_poll_interval_seconds: 1.0,
            full_reconciliation_interval_seconds: 300.0,
            unknown_order_sla_seconds: 15.0,
            emergency_cancel_wal_path: "data/emergency_cancel.jsonl".into(),
            shadow_min_utc_days: 14,
            testnet_soak_min_days: 14,
            canary_observation_min_days: 30,
        }
    }
}

/// Deployment-wide funding-arbitrage execution ceilings.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct FundingArbSettings {
    pub max_notional_usd: ConfigDecimal,
    pub poll_interval_seconds: f64,
    pub order_status_poll_interval_seconds: f64,
    pub max_leg_attempts: u32,
    pub market_stale_seconds: f64,
    pub universe_refresh_seconds: f64,
    pub book_refresh_seconds: f64,
    pub max_candidate_markets: u32,
    pub min_spot_24h_volume_usd: ConfigDecimal,
    pub min_perp_24h_volume_usd: ConfigDecimal,
    pub min_top_book_depth_usd: ConfigDecimal,
    pub max_combined_spread_bps: ConfigDecimal,
}

impl Default for FundingArbSettings {
    fn default() -> Self {
        Self {
            max_notional_usd: ConfigDecimal(Decimal::from_str_lenient("500").unwrap()),
            poll_interval_seconds: 5.0,
            order_status_poll_interval_seconds: 0.25,
            max_leg_attempts: 3,
            market_stale_seconds: 5.0,
            universe_refresh_seconds: 30.0,
            book_refresh_seconds: 5.0,
            max_candidate_markets: 8,
            min_spot_24h_volume_usd: ConfigDecimal(Decimal::from_str_lenient("1000").unwrap()),
            min_perp_24h_volume_usd: ConfigDecimal(Decimal::from_str_lenient("10000").unwrap()),
            min_top_book_depth_usd: ConfigDecimal(Decimal::from_str_lenient("100").unwrap()),
            max_combined_spread_bps: ConfigDecimal(Decimal::from_str_lenient("100").unwrap()),
        }
    }
}

/// Monitoring settings.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MonitorSettings {
    pub prometheus_port: u16,
}

impl Default for MonitorSettings {
    fn default() -> Self {
        Self {
            prometheus_port: 9090,
        }
    }
}

/// Backfill state and data integrity settings.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackfillSettings {
    pub state_dir: String,
    pub backfill_window_days: u32,
    pub refresh_interval_hours: f64,
    pub quality_check_interval_hours: f64,
    pub dedup_max_keys: u32,
}

impl Default for BackfillSettings {
    fn default() -> Self {
        Self {
            state_dir: "data".into(),
            backfill_window_days: 7,
            refresh_interval_hours: 6.0,
            quality_check_interval_hours: 1.0,
            dedup_max_keys: 1_000_000,
        }
    }
}

/// Backtest framework settings (design doc §6).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BacktestSettings {
    pub initial_capital: f64,
    pub default_maker_rebate_pct: f64,
    pub default_taker_fee_pct: f64,
    pub slippage_optimistic_bps: f64,
    pub slippage_pessimistic_bps: f64,
    pub walk_forward_train_days: u32,
    pub walk_forward_validate_days: u32,
    pub walk_forward_step_days: u32,
    pub monte_carlo_simulations: u32,
}

impl Default for BacktestSettings {
    fn default() -> Self {
        Self {
            initial_capital: 10_000.0,
            default_maker_rebate_pct: -0.0002,
            default_taker_fee_pct: 0.0005,
            slippage_optimistic_bps: 2.0,
            slippage_pessimistic_bps: 10.0,
            walk_forward_train_days: 60,
            walk_forward_validate_days: 30,
            walk_forward_step_days: 30,
            monte_carlo_simulations: 1000,
        }
    }
}

/// HTTP API settings.
#[derive(Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ApiSettings {
    pub host: String,
    pub port: u16,
    pub cors_origins: Vec<String>,
    /// Retained as a backwards-compatible admin token.
    pub auth_token: String,
    pub viewer_token: String,
    pub operator_token: String,
    pub admin_token: String,
    pub request_rate_limit_per_minute: u32,
    pub mutation_rate_limit_per_minute: u32,
    pub auth_failure_limit_per_minute: u32,
    pub market_ws_max_connections: u32,
    pub market_ws_max_connections_per_ip: u32,
    pub market_ws_queue_size: u32,
    pub market_ws_messages_per_second: u32,
}

/// `Debug` redacts the four API role tokens (M-CF2).
impl fmt::Debug for ApiSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiSettings")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("cors_origins", &self.cors_origins)
            .field("auth_token", &redact_secret(&self.auth_token))
            .field("viewer_token", &redact_secret(&self.viewer_token))
            .field("operator_token", &redact_secret(&self.operator_token))
            .field("admin_token", &redact_secret(&self.admin_token))
            .field(
                "request_rate_limit_per_minute",
                &self.request_rate_limit_per_minute,
            )
            .field(
                "mutation_rate_limit_per_minute",
                &self.mutation_rate_limit_per_minute,
            )
            .field(
                "auth_failure_limit_per_minute",
                &self.auth_failure_limit_per_minute,
            )
            .field("market_ws_max_connections", &self.market_ws_max_connections)
            .field(
                "market_ws_max_connections_per_ip",
                &self.market_ws_max_connections_per_ip,
            )
            .field("market_ws_queue_size", &self.market_ws_queue_size)
            .field(
                "market_ws_messages_per_second",
                &self.market_ws_messages_per_second,
            )
            .finish()
    }
}

impl Default for ApiSettings {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 37001,
            cors_origins: vec![
                "http://localhost:34001".into(),
                "http://127.0.0.1:34001".into(),
            ],
            auth_token: String::new(),
            viewer_token: String::new(),
            operator_token: String::new(),
            admin_token: String::new(),
            request_rate_limit_per_minute: 600,
            mutation_rate_limit_per_minute: 60,
            auth_failure_limit_per_minute: 10,
            market_ws_max_connections: 100,
            market_ws_max_connections_per_ip: 5,
            market_ws_queue_size: 64,
            market_ws_messages_per_second: 50,
        }
    }
}

/// V2 cut-over flags. Trading stays disabled unless the full V2 chain is on.
#[derive(Debug, Clone, PartialEq, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct FeatureFlagsSettings {
    pub legacy_execution: bool,
    pub durable_ledger_v2: bool,
    pub execution_v2: bool,
    pub user_stream_v2: bool,
    pub reconciliation_v2: bool,
    /// The Rust port always serves the v1 API; retained for config parity.
    pub api_v1: bool,
    pub strategy_runner_v2: bool,
    pub market_making_enabled: bool,
    pub funding_arb_execution_enabled: bool,
}

impl FeatureFlagsSettings {
    /// Whether every safety-critical V2 trading component is selected.
    pub fn v2_trading_enabled(&self) -> bool {
        self.durable_ledger_v2
            && self.execution_v2
            && self.user_stream_v2
            && self.reconciliation_v2
            && self.strategy_runner_v2
    }
}

/// Top-level application settings. Composes all sub-settings.
///
/// `Debug` is derived: every secret-bearing sub-settings struct implements a
/// redacting `Debug` (`ExchangeSettings`, `ClickHouseSettings`,
/// `PostgresSettings`, `ApiSettings`), so formatting the whole tree never
/// leaks keys/passwords/tokens (M-CF2).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AppSettings {
    pub environment: String,
    pub log_level: String,
    pub exchange: ExchangeSettings,
    pub market_data: MarketDataSettings,
    pub external_reference: ExternalReferenceSettings,
    pub clickhouse: ClickHouseSettings,
    pub postgres: PostgresSettings,
    pub risk: RiskSettings,
    pub action_budget: ActionBudgetSettings,
    pub market_making: MarketMakingSettings,
    pub funding_arb: FundingArbSettings,
    pub monitor: MonitorSettings,
    pub backfill: BackfillSettings,
    pub backtest: BacktestSettings,
    pub api: ApiSettings,
    pub features: FeatureFlagsSettings,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            environment: "dev".into(),
            log_level: "INFO".into(),
            exchange: ExchangeSettings::default(),
            market_data: MarketDataSettings::default(),
            external_reference: ExternalReferenceSettings::default(),
            clickhouse: ClickHouseSettings::default(),
            postgres: PostgresSettings::default(),
            risk: RiskSettings::default(),
            action_budget: ActionBudgetSettings::default(),
            market_making: MarketMakingSettings::default(),
            funding_arb: FundingArbSettings::default(),
            monitor: MonitorSettings::default(),
            backfill: BackfillSettings::default(),
            backtest: BacktestSettings::default(),
            api: ApiSettings::default(),
            features: FeatureFlagsSettings::default(),
        }
    }
}

impl AppSettings {
    pub fn is_dev(&self) -> bool {
        self.environment == "dev"
    }
    pub fn is_testnet(&self) -> bool {
        self.environment == "testnet"
    }
    pub fn is_mainnet(&self) -> bool {
        self.environment == "mainnet"
    }

    /// Run every cross-field validator that Python's `model_validator` blocks
    /// run after construction. Returns a descriptive [`ConfigError`].
    pub fn validate(&self) -> Result<(), ConfigError> {
        // External reference limits.
        if self.external_reference.spot_weight.0 + self.external_reference.perpetual_weight.0
            != Decimal::ONE
        {
            return Err(ConfigError::validation(
                "external spot and perpetual weights must sum to 1",
            ));
        }
        if self.external_reference.reconnect_delay_min_seconds
            > self.external_reference.reconnect_delay_max_seconds
        {
            return Err(ConfigError::validation(
                "external reconnect minimum cannot exceed maximum",
            ));
        }
        if self.external_reference.symbol_map.len() > self.external_reference.max_symbols as usize {
            return Err(ConfigError::validation(
                "external symbol map exceeds max_symbols safety limit",
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for v in self.external_reference.symbol_map.values() {
            if !seen.insert(v.to_uppercase()) {
                return Err(ConfigError::validation(
                    "external venue symbols must be unique",
                ));
            }
        }

        // Risk settings ranges (H-CF1): these are safety-critical ceilings, so
        // nonsense values fail closed at startup instead of silently
        // disabling protection.
        let r = &self.risk;
        if r.max_leverage == 0 || r.max_leverage > 20 {
            return Err(ConfigError::validation("max_leverage must be in (0, 20]"));
        }
        for (name, v) in [
            ("max_position_pct", r.max_position_pct),
            ("max_strategy_loss_pct", r.max_strategy_loss_pct),
            ("max_drawdown_pct", r.max_drawdown_pct),
        ] {
            if !(v > 0.0 && v <= 1.0) {
                return Err(ConfigError::validation(format!("{name} must be in (0, 1]")));
            }
        }
        if r.risk_check_timeout_ms < 100 {
            return Err(ConfigError::validation(
                "risk_check_timeout_ms must be at least 100ms",
            ));
        }
        if !r.market_price_stale_seconds.is_finite() || r.market_price_stale_seconds <= 0.0 {
            return Err(ConfigError::validation(
                "market_price_stale_seconds must be positive and finite",
            ));
        }
        // `action_credits_low_watermark` is a `u64`, so `>= 0` is guaranteed
        // by the type; no runtime check needed.

        // Action budget thresholds.
        let ab = &self.action_budget;
        if !(ab.address_cancel_only_threshold <= ab.address_critical_threshold
            && ab.address_critical_threshold <= ab.address_conserve_threshold)
        {
            return Err(ConfigError::validation(
                "address thresholds must satisfy cancel_only <= critical <= conserve",
            ));
        }
        if !(ab.remote_poll_interval_critical_seconds <= ab.remote_poll_interval_conserve_seconds
            && ab.remote_poll_interval_conserve_seconds <= ab.remote_poll_interval_normal_seconds)
        {
            return Err(ConfigError::validation(
                "budget polling intervals must get shorter as pressure increases",
            ));
        }
        if !(ab.runway_cancel_only_hours <= ab.runway_critical_hours
            && ab.runway_critical_hours <= ab.runway_conserve_hours)
        {
            return Err(ConfigError::validation(
                "runway thresholds must satisfy cancel_only <= critical <= conserve",
            ));
        }
        if ab.ip_emergency_reserve >= ab.ip_weight_limit_per_minute {
            return Err(ConfigError::validation(
                "ip_emergency_reserve must be below the per-minute IP limit",
            ));
        }
        if ab.paid_reserve_enabled
            && (ab.paid_reserve_max_single_usdc <= 0.0
                || ab.paid_reserve_max_daily_usdc <= 0.0
                || ab.paid_reserve_max_monthly_usdc <= 0.0)
        {
            return Err(ConfigError::validation(
                "enabled paid reserve requires positive single, daily, and monthly limits",
            ));
        }
        if ab.paid_reserve_max_single_usdc > ab.paid_reserve_max_daily_usdc {
            return Err(ConfigError::validation(
                "paid reserve single limit cannot exceed daily limit",
            ));
        }
        if ab.paid_reserve_max_daily_usdc > ab.paid_reserve_max_monthly_usdc {
            return Err(ConfigError::validation(
                "paid reserve daily limit cannot exceed monthly limit",
            ));
        }

        // Market-making safety ceilings.
        let mm = &self.market_making;
        if mm.near_risk_account_poll_interval_seconds >= mm.account_poll_interval_seconds {
            return Err(ConfigError::validation(
                "near-risk account polling must be strictly faster than normal polling",
            ));
        }
        if mm.max_quote_notional_equity_pct > mm.max_hard_inventory_equity_pct {
            return Err(ConfigError::validation(
                "quote notional ceiling cannot exceed the hard inventory ceiling",
            ));
        }

        // Funding-arb scan cadence.
        let fa = &self.funding_arb;
        if fa.book_refresh_seconds > fa.market_stale_seconds {
            return Err(ConfigError::validation(
                "funding-arb book refresh must not exceed the market stale threshold",
            ));
        }
        if fa.universe_refresh_seconds < fa.book_refresh_seconds {
            return Err(ConfigError::validation(
                "funding-arb universe refresh cannot be faster than book refresh",
            ));
        }

        // API role tokens.
        let configured_tokens = [
            self.api.viewer_token.as_str(),
            self.api.operator_token.as_str(),
            self.api.admin_token.as_str(),
            self.api.auth_token.as_str(),
        ]
        .into_iter()
        .filter(|t| !t.is_empty())
        .collect::<Vec<_>>();
        if configured_tokens.iter().any(|t| t.len() < 32) {
            return Err(ConfigError::validation(
                "every configured API token must contain at least 32 characters",
            ));
        }
        let mut token_set = std::collections::HashSet::new();
        for t in &configured_tokens {
            if !token_set.insert(*t) {
                return Err(ConfigError::validation(
                    "configured API role tokens must be unique",
                ));
            }
        }
        // A23: on mainnet (real money), binding a non-loopback host with no API
        // tokens would expose the full admin control plane (kill switch,
        // strategy lifecycle, trading) to the LAN with no credential — fail
        // closed. Dev/testnet are operator-controlled lab deployments that
        // deliberately bind a LAN host, so they are left to the operator.
        if self.environment == "mainnet"
            && configured_tokens.is_empty()
            && !is_loopback_host(&self.api.host)
        {
            return Err(ConfigError::validation(
                "mainnet binding a non-loopback API host requires at least one API role token",
            ));
        }

        // Feature cut-over chain.
        let f = &self.features;
        if f.legacy_execution && f.execution_v2 {
            return Err(ConfigError::validation(
                "legacy_execution and execution_v2 are mutually exclusive",
            ));
        }
        if f.execution_v2 && !f.durable_ledger_v2 {
            return Err(ConfigError::validation(
                "execution_v2 requires durable_ledger_v2",
            ));
        }
        if f.user_stream_v2 && !f.durable_ledger_v2 {
            return Err(ConfigError::validation(
                "user_stream_v2 requires durable_ledger_v2",
            ));
        }
        if f.reconciliation_v2 && !f.durable_ledger_v2 {
            return Err(ConfigError::validation(
                "reconciliation_v2 requires durable_ledger_v2",
            ));
        }
        if f.strategy_runner_v2 && !f.execution_v2 {
            return Err(ConfigError::validation(
                "strategy_runner_v2 requires execution_v2",
            ));
        }
        if f.market_making_enabled && !f.v2_trading_enabled() {
            return Err(ConfigError::validation(
                "market_making_enabled requires the complete V2 trading chain",
            ));
        }
        if f.funding_arb_execution_enabled && !f.v2_trading_enabled() {
            return Err(ConfigError::validation(
                "funding_arb_execution_enabled requires the complete V2 trading chain",
            ));
        }

        // Live-strategy environment restriction (M-FA6): funding-arb live
        // execution is **testnet-only**. Mainnet is hard-disabled by design
        // (`docs/funding_arb_design.md` §1: "mainnet 继续硬禁用"; §5.3: "本版本
        // 没有 mainnet 解锁路径"), and `dev` runs the observation/control
        // plane only.
        if f.funding_arb_execution_enabled && self.environment.as_str() != "testnet" {
            return Err(ConfigError::validation(
                "funding_arb_execution_enabled is restricted to HYPE_ENV=testnet (mainnet is hard-disabled)",
            ));
        }

        // Environment must be one of the supported set.
        if !matches!(self.environment.as_str(), "dev" | "testnet" | "mainnet") {
            return Err(ConfigError::validation(format!(
                "unsupported HYPE_ENV={:?}; expected one of dev, testnet, mainnet",
                self.environment
            )));
        }
        Ok(())
    }
}

/// Configuration loading or validation error.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ConfigError {
    #[error("configuration error: {0}")]
    Validation(String),
    #[error("unsupported environment: {0}")]
    UnsupportedEnvironment(String),
    #[error("mainnet requires secret environment variables: {0}")]
    MainnetSecretsMissing(String),
    #[error("mainnet requires the kill switch to be enabled (risk.kill_switch_enabled=true)")]
    MainnetKillSwitchDisabled,
    #[error("invalid mainnet Postgres URL: {0}")]
    MainnetPostgresInvalid(String),
    #[error("invalid mainnet API token: {0}")]
    MainnetApiTokenInvalid(String),
    #[error("configuration file error: {0}")]
    Io(String),
    #[error("environment mismatch: {0}")]
    EnvironmentMismatch(String),
}

impl ConfigError {
    fn validation(msg: impl Into<String>) -> Self {
        ConfigError::Validation(msg.into())
    }
}

/// Whether an API bind host is loopback (no LAN exposure when tokens are empty).
fn is_loopback_host(host: &str) -> bool {
    let host = host.trim();
    matches!(host, "localhost" | "127.0.0.1" | "::1")
        || host.starts_with("127.")
        || host.starts_with("[::1")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exchange_debug_redacts_agent_private_key() {
        let s = ExchangeSettings {
            agent_private_key: "0xdeadbeefcafe1234".into(),
            ..ExchangeSettings::default()
        };
        let dbg = format!("{s:?}");
        assert!(
            !dbg.contains("0xdeadbeefcafe1234"),
            "Debug must not leak the agent private key: {dbg}"
        );
        assert!(
            dbg.contains("<redacted:0xde>"),
            "redaction marker missing: {dbg}"
        );
    }

    #[test]
    fn mainnet_non_loopback_host_requires_api_token() {
        // A23: mainnet binding a non-loopback host with no tokens fails closed.
        let mut s = AppSettings {
            environment: "mainnet".into(),
            ..AppSettings::default()
        };
        s.api.host = "0.0.0.0".into();
        let err = s.validate().unwrap_err();
        assert!(
            err.to_string().contains("API role token"),
            "mainnet non-loopback must require a token: {err}"
        );
        s.api.host = "127.0.0.1".into();
        assert!(
            s.validate().is_ok(),
            "loopback host is allowed on mainnet without tokens"
        );
    }

    // --- H-CF1: RiskSettings range validation ---

    #[test]
    fn risk_settings_range_validation() {
        let mut s = AppSettings::default();
        assert!(s.validate().is_ok(), "defaults are valid");

        // max_leverage in (0, 20].
        s.risk.max_leverage = 0;
        assert!(s.validate().is_err());
        s.risk.max_leverage = 21;
        assert!(s.validate().is_err());
        s.risk.max_leverage = 20;
        assert!(s.validate().is_ok());
        s.risk.max_leverage = 1;
        assert!(s.validate().is_ok());
        s.risk.max_leverage = 5;

        // pct fields in (0, 1].
        s.risk.max_position_pct = 0.0;
        assert!(s.validate().is_err());
        s.risk.max_position_pct = 1.1;
        assert!(s.validate().is_err());
        s.risk.max_position_pct = 1.0;
        assert!(s.validate().is_ok());
        s.risk.max_position_pct = 0.20;
        s.risk.max_strategy_loss_pct = -0.01;
        assert!(s.validate().is_err());
        s.risk.max_strategy_loss_pct = 0.05;
        s.risk.max_drawdown_pct = 0.0;
        assert!(s.validate().is_err());
        s.risk.max_drawdown_pct = 0.10;

        // risk_check_timeout_ms >= 100.
        s.risk.risk_check_timeout_ms = 99;
        assert!(s.validate().is_err());
        s.risk.risk_check_timeout_ms = 100;
        assert!(s.validate().is_ok());
        s.risk.risk_check_timeout_ms = 500;

        // market_price_stale_seconds > 0.
        s.risk.market_price_stale_seconds = 0.0;
        assert!(s.validate().is_err());
        s.risk.market_price_stale_seconds = -1.0;
        assert!(s.validate().is_err());
        s.risk.market_price_stale_seconds = 5.0;

        assert!(s.validate().is_ok(), "all boundary-valid values pass");
    }

    // --- M-CF2: Debug redaction ---

    #[test]
    fn postgres_url_debug_redacts_password() {
        let s = PostgresSettings {
            url: "postgresql://hypeedge:supersecretpw@db.internal:5432/hypeedge".into(),
            ..PostgresSettings::default()
        };
        let dbg = format!("{s:?}");
        assert!(
            !dbg.contains("supersecretpw"),
            "Debug must not leak the DB password: {dbg}"
        );
        assert!(
            dbg.contains(":***@db.internal"),
            "redacted URL should keep user + host shape: {dbg}"
        );
    }

    #[test]
    fn clickhouse_debug_redacts_password() {
        let s = ClickHouseSettings {
            password: "ch-secret-123".into(),
            ..ClickHouseSettings::default()
        };
        let dbg = format!("{s:?}");
        assert!(
            !dbg.contains("ch-secret-123"),
            "Debug must not leak the ClickHouse password: {dbg}"
        );
        assert!(dbg.contains("<redacted:ch-s>"), "marker expected: {dbg}");
    }

    #[test]
    fn api_debug_redacts_tokens() {
        let s = ApiSettings {
            auth_token: "0123456789abcdef0123456789abcdef".into(),
            viewer_token: "fedcba9876543210fedcba9876543210".into(),
            operator_token: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            admin_token: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
            ..ApiSettings::default()
        };
        let dbg = format!("{s:?}");
        for secret in [
            "0123456789abcdef0123456789abcdef",
            "fedcba9876543210fedcba9876543210",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ] {
            assert!(
                !dbg.contains(secret),
                "Debug must not leak {secret:?}: {dbg}"
            );
        }
    }

    #[test]
    fn app_settings_debug_redacts_all_secrets() {
        let mut s = AppSettings::default();
        s.exchange.agent_private_key = "0xdeadbeefcafe1234".into();
        s.exchange.account_address = "0x1111111111111111111111111111111111111111".into();
        s.postgres.url = "postgresql://hypeedge:supersecretpw@db.internal:5432/hypeedge".into();
        s.clickhouse.password = "ch-secret-123".into();
        s.api.auth_token = "0123456789abcdef0123456789abcdef".into();
        s.api.viewer_token = "fedcba9876543210fedcba9876543210".into();
        s.api.operator_token = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into();
        s.api.admin_token = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into();
        let dbg = format!("{s:?}");
        for secret in [
            "0xdeadbeefcafe1234",
            "supersecretpw",
            "ch-secret-123",
            "0123456789abcdef0123456789abcdef",
            "fedcba9876543210fedcba9876543210",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ] {
            assert!(
                !dbg.contains(secret),
                "AppSettings Debug must not leak {secret:?}: {dbg}"
            );
        }
        // Redaction markers are present so the output is still actionable.
        assert!(dbg.contains("<redacted:0xde>"), "exchange marker: {dbg}");
        assert!(
            dbg.contains("hypeedge:***@db.internal"),
            "postgres URL password masked: {dbg}"
        );
    }

    // --- M-CF1: unknown settings fields must be rejected on deserialization ---

    #[test]
    fn unknown_risk_field_is_rejected() {
        let yaml = "risk:\n  max_levarage: 5\n"; // typo of max_leverage
        let err = serde_yaml::from_str::<AppSettings>(yaml).unwrap_err();
        assert!(
            err.to_string().contains("max_levarage"),
            "typo'd field must be reported: {err}"
        );
    }
}
