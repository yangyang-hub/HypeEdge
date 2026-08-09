//! Configuration layer mirroring `src/hypeedge/config/`.
//!
//! [`settings`] holds the settings structs (port of `settings.py`),
//! [`loader`] implements the layered loader and mainnet fail-closed rules
//! (port of `loader.py`).

pub mod loader;
pub mod settings;

pub use loader::{load_settings, load_yaml_config, select_environment};
pub use settings::{
    ActionBudgetSettings, ApiSettings, AppSettings, BackfillSettings, BacktestSettings,
    ClickHouseSettings, ConfigError, ExchangeSettings, ExternalReferenceSettings,
    FeatureFlagsSettings, FundingArbSettings, MarketDataSettings, MarketMakingSettings,
    MonitorSettings, PostgresSettings, RiskSettings,
};
