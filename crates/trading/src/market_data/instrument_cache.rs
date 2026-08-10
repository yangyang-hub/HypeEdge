//! Authoritative Hyperliquid perpetual and spot instrument metadata cache,
//! port of `src/hypeedge/market_data/instrument_cache.py`.
//!
//! Parses the `meta` / `spotMeta` responses into an in-memory cache keyed by
//! exchange symbol, with display-name aliases for spot pairs. The REST fetch
//! is behind a trait so parsing is unit-testable; the concrete adapter wraps
//! the [`RestClient`](crate::market_data::rest_client::RestClient).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use hypeedge_domain::decimal::Decimal;
use hypeedge_domain::error::HypeEdgeError;

/// Refresh interval for metadata (contracts rarely change).
pub const META_REFRESH_INTERVAL_HOURS: f64 = 6.0;

/// Exchange rules and asset identity for one perpetual or spot market.
#[derive(Debug, Clone, PartialEq)]
pub struct InstrumentInfo {
    pub symbol: String,
    /// Number of decimal places for size.
    pub sz_decimals: u32,
    pub max_leverage: u32,
    /// Smallest price increment before the 5-significant-figure rule.
    pub tick_size: Decimal,
    /// Minimum size increment (`10^-sz_decimals`).
    pub lot_size: Decimal,
    pub min_size: Decimal,
    pub display_name: String,
    pub min_notional: Option<Decimal>,
    pub max_price_decimals: u32,
    pub max_significant_figures: u32,
    pub is_spot: bool,
    pub base_token: Option<String>,
    pub quote_token: Option<String>,
    pub only_isolated: bool,
    pub margin_mode: Option<String>,
    /// The numeric asset index from the HL meta universe, used by the execution
    /// engine's order wire (`a` field). `None` for spot pairs.
    pub asset_index: Option<i64>,
}

impl InstrumentInfo {
    fn new_perp(
        symbol: String,
        sz_decimals: u32,
        max_leverage: u32,
        only_isolated: bool,
        margin_mode: Option<String>,
    ) -> Self {
        let max_price_decimals = 6u32.saturating_sub(sz_decimals);
        let lot_size = power_of_ten_neg(sz_decimals);
        Self {
            symbol: symbol.clone(),
            sz_decimals,
            max_leverage,
            tick_size: power_of_ten_neg(max_price_decimals),
            lot_size,
            min_size: lot_size,
            display_name: symbol,
            min_notional: None,
            max_price_decimals,
            max_significant_figures: 5,
            is_spot: false,
            base_token: None,
            quote_token: None,
            only_isolated,
            margin_mode,
            asset_index: None,
        }
    }

    fn new_spot(
        symbol: String,
        display_name: String,
        sz_decimals: u32,
        base_token: String,
        quote_token: String,
    ) -> Self {
        let max_price_decimals = 8u32.saturating_sub(sz_decimals);
        let lot_size = power_of_ten_neg(sz_decimals);
        Self {
            symbol,
            sz_decimals,
            max_leverage: 1,
            tick_size: power_of_ten_neg(max_price_decimals),
            lot_size,
            min_size: lot_size,
            display_name,
            min_notional: None,
            max_price_decimals,
            max_significant_figures: 5,
            is_spot: true,
            base_token: Some(base_token),
            quote_token: Some(quote_token),
            only_isolated: false,
            margin_mode: None,
            asset_index: None,
        }
    }

    /// An [`InstrumentSpec`](crate::execution::normalizer::InstrumentSpec) for
    /// the order normalizer's hot-path lookups.
    pub fn to_spec(&self) -> crate::execution::normalizer::InstrumentSpec {
        crate::execution::normalizer::InstrumentSpec {
            symbol: self.symbol.clone(),
            tick_size: self.tick_size,
            lot_size: self.lot_size,
            min_size: self.min_size,
            min_notional: self.min_notional,
            max_price_decimals: Some(self.max_price_decimals),
            max_significant_figures: self.max_significant_figures,
        }
    }
}

fn power_of_ten_neg(places: u32) -> Decimal {
    // `from_scaled(1, places)` = 1 × 10^-places (raw value at scale 18).
    Decimal::from_scaled(1, places)
}

/// The REST metadata fetch boundary (implemented by `RestClient`).
#[async_trait]
pub trait InstrumentMetaSource: Send + Sync {
    async fn get_meta(&self) -> Result<serde_json::Value, HypeEdgeError>;
    async fn get_spot_meta(&self) -> Result<serde_json::Value, HypeEdgeError>;
}

/// The parsed metadata: instruments by exchange symbol, plus spot display
/// aliases (`PURR/USDC` → `@1`).
pub type ParsedMeta = (HashMap<String, InstrumentInfo>, HashMap<String, String>);

/// Parse perpetual universe + spot universe + tokens into instrument info and
/// spot display aliases. Pure and unit-testable.
pub fn parse_meta(
    perp_data: &serde_json::Value,
    spot_data: &serde_json::Value,
) -> Result<ParsedMeta, HypeEdgeError> {
    let perp_universe = perp_data.get("universe").and_then(|v| v.as_array());
    let Some(perp_universe) = perp_universe else {
        return Err(HypeEdgeError::MarketData("meta_empty_universe".into()));
    };
    let spot_universe = spot_data.get("universe").and_then(|v| v.as_array());
    let tokens = spot_data.get("tokens").and_then(|v| v.as_array());
    let (Some(spot_universe), Some(tokens)) = (spot_universe, tokens) else {
        return Err(HypeEdgeError::MarketData(
            "invalid_spot_meta_response".into(),
        ));
    };

    let mut instruments: HashMap<String, InstrumentInfo> = HashMap::new();
    for asset in perp_universe {
        let Some(name) = asset.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        let sz_decimals = asset
            .get("szDecimals")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;
        let max_leverage = asset
            .get("maxLeverage")
            .and_then(|v| v.as_u64())
            .unwrap_or(50) as u32;
        let only_isolated = asset
            .get("onlyIsolated")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let margin_mode = asset
            .get("marginMode")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let mut info = InstrumentInfo::new_perp(
            name.to_string(),
            sz_decimals,
            max_leverage,
            only_isolated,
            margin_mode,
        );
        info.asset_index = asset.get("index").and_then(|v| v.as_i64());
        instruments.insert(name.to_string(), info);
    }

    let token_by_index: HashMap<i64, &serde_json::Value> = tokens
        .iter()
        .filter_map(|t| {
            let index = t.get("index").and_then(|v| v.as_i64())?;
            let name = t.get("name").and_then(|v| v.as_str())?;
            if name.is_empty() {
                None
            } else {
                Some((index, t))
            }
        })
        .collect();

    let mut aliases: HashMap<String, String> = HashMap::new();
    for asset in spot_universe {
        let exchange_name = asset.get("name").and_then(|v| v.as_str()).map(str::trim);
        let raw_tokens = asset.get("tokens").and_then(|v| v.as_array());
        let (Some(exchange_name), Some(raw_tokens)) = (exchange_name, raw_tokens) else {
            continue;
        };
        if exchange_name.is_empty() || raw_tokens.len() != 2 {
            continue;
        }
        let base_idx = raw_tokens[0].as_i64();
        let quote_idx = raw_tokens[1].as_i64();
        let (Some(base_idx), Some(quote_idx)) = (base_idx, quote_idx) else {
            continue;
        };
        let (Some(base), Some(quote)) = (token_by_index.get(&base_idx), token_by_index.get(&quote_idx))
        else {
            continue;
        };
        let Some(base_name) = base.get("name").and_then(|v| v.as_str()) else { continue };
        let Some(quote_name) = quote.get("name").and_then(|v| v.as_str()) else { continue };
        let sz_decimals = base
            .get("szDecimals")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;
        let display_name = format!("{base_name}/{quote_name}");
        instruments.insert(
            exchange_name.to_string(),
            InstrumentInfo::new_spot(
                exchange_name.to_string(),
                display_name.clone(),
                sz_decimals,
                base_name.to_string(),
                quote_name.to_string(),
            ),
        );
        aliases.insert(exchange_name.to_string(), exchange_name.to_string());
        match aliases.get(&display_name) {
            Some(existing) if existing != exchange_name => {
                // Ambiguous display alias: drop it rather than guess.
                aliases.remove(&display_name);
            }
            None => {
                aliases.insert(display_name, exchange_name.to_string());
            }
            _ => {}
        }
    }

    Ok((instruments, aliases))
}

/// In-memory cache of Hyperliquid contract metadata.
pub struct InstrumentMetaCache {
    source: Arc<dyn InstrumentMetaSource>,
    refresh_interval: std::time::Duration,
    instruments: std::sync::RwLock<HashMap<String, InstrumentInfo>>,
    spot_aliases: std::sync::RwLock<HashMap<String, String>>,
    loaded: std::sync::atomic::AtomicBool,
}

impl InstrumentMetaCache {
    pub fn new(
        source: Arc<dyn InstrumentMetaSource>,
        refresh_interval_hours: Option<f64>,
    ) -> Self {
        Self {
            source,
            refresh_interval: std::time::Duration::from_secs_f64(
                refresh_interval_hours.unwrap_or(META_REFRESH_INTERVAL_HOURS) * 3600.0,
            ),
            instruments: std::sync::RwLock::new(HashMap::new()),
            spot_aliases: std::sync::RwLock::new(HashMap::new()),
            loaded: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn is_loaded(&self) -> bool {
        self.loaded.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn get(&self, symbol: &str) -> Option<InstrumentInfo> {
        let instruments = self.instruments.read().unwrap();
        if let Some(info) = instruments.get(symbol) {
            return Some(info.clone());
        }
        let aliases = self.spot_aliases.read().unwrap();
        let resolved = aliases.get(symbol)?;
        instruments.get(resolved).cloned()
    }

    /// Resolve a display pair or exchange coin (`@N`) to spot metadata.
    pub fn resolve_spot(&self, market: &str) -> Option<InstrumentInfo> {
        let info = self.get(market.trim())?;
        if info.is_spot {
            Some(info)
        } else {
            None
        }
    }

    pub fn get_spot(&self, market: &str) -> Option<InstrumentInfo> {
        self.resolve_spot(market)
    }

    pub fn get_sz_decimals(&self, symbol: &str) -> Option<u32> {
        self.instruments
            .read()
            .unwrap()
            .get(symbol)
            .map(|i| i.sz_decimals)
    }

    pub fn get_tick_size(&self, symbol: &str) -> Option<Decimal> {
        self.instruments
            .read()
            .unwrap()
            .get(symbol)
            .map(|i| i.tick_size)
    }

    /// Fetch and atomically replace perpetual + spot metadata.
    pub async fn refresh(&self) -> Result<(), HypeEdgeError> {
        let perp = self.source.get_meta().await?;
        let spot = self.source.get_spot_meta().await?;
        let (instruments, aliases) = parse_meta(&perp, &spot)?;
        *self.instruments.write().unwrap() = instruments;
        *self.spot_aliases.write().unwrap() = aliases;
        self.loaded.store(true, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(instruments = self.instruments.read().unwrap().len(), "meta_loaded");
        Ok(())
    }

    /// Ensure metadata is loaded at least once, propagating failures to the
    /// startup gate.
    pub async fn ensure_loaded(&self) -> Result<(), HypeEdgeError> {
        if self.is_loaded() {
            return Ok(());
        }
        self.refresh().await
    }

    /// Main loop: fetch on startup, then refresh periodically.
    pub async fn run(&self) {
        if let Err(e) = self.refresh().await {
            tracing::warn!(error = %e, "meta_fetch_failed");
        }
        loop {
            tokio::time::sleep(self.refresh_interval).await;
            if let Err(e) = self.refresh().await {
                tracing::warn!(error = %e, "meta_fetch_failed");
            }
        }
    }
}

impl crate::execution::normalizer::InstrumentSpecProvider for InstrumentMetaCache {
    fn get(&self, symbol: &str) -> Option<crate::execution::normalizer::InstrumentSpec> {
        self.get(symbol).map(|info| info.to_spec())
    }
}

impl crate::execution::exchange::AssetIndexProvider for InstrumentMetaCache {
    fn asset_index(&self, symbol: &str) -> Option<i64> {
        self.get(symbol).and_then(|info| info.asset_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn perp_meta() -> serde_json::Value {
        serde_json::json!({
            "universe": [
                { "name": "BTC", "index": 1, "szDecimals": 5, "maxLeverage": 50, "marginMode": "cross" },
                { "name": "ETH", "index": 2, "szDecimals": 4, "maxLeverage": 25 }
            ]
        })
    }

    fn spot_meta() -> serde_json::Value {
        serde_json::json!({
            "tokens": [
                { "index": 0, "name": "USDC", "szDecimals": 8 },
                { "index": 1, "name": "PURR", "szDecimals": 6 },
                { "index": 2, "name": "WBTC", "szDecimals": 8 }
            ],
            "universe": [
                { "name": "@1", "tokens": [1, 0] },
                { "name": "@2", "tokens": [2, 0] }
            ]
        })
    }

    #[test]
    fn parses_perp_and_spot_metadata() {
        let (instruments, aliases) = parse_meta(&perp_meta(), &spot_meta()).unwrap();
        let btc = instruments.get("BTC").unwrap();
        assert_eq!(btc.sz_decimals, 5);
        assert_eq!(btc.max_leverage, 50);
        assert_eq!(btc.max_price_decimals, 1);
        assert!(!btc.is_spot);
        assert_eq!(btc.lot_size, power_of_ten_neg(5));
        assert_eq!(btc.margin_mode.as_deref(), Some("cross"));

        let purr = instruments.get("@1").unwrap();
        assert!(purr.is_spot);
        assert_eq!(purr.base_token.as_deref(), Some("PURR"));
        assert_eq!(purr.quote_token.as_deref(), Some("USDC"));
        assert_eq!(purr.display_name, "PURR/USDC");

        // Display-name alias resolves to the exchange coin.
        assert_eq!(aliases.get("PURR/USDC").map(String::as_str), Some("@1"));
    }

    #[test]
    fn asset_index_provider_reads_meta_index() {
        // 6b: the cache must expose the perp asset index the execution engine
        // needs for the order wire.
        let (instruments, _) = parse_meta(&perp_meta(), &spot_meta()).unwrap();
        let cache = InstrumentMetaCache {
            source: Arc::new(UnreachableSource),
            refresh_interval: std::time::Duration::from_secs(1),
            instruments: std::sync::RwLock::new(instruments),
            spot_aliases: std::sync::RwLock::new(Default::default()),
            loaded: std::sync::atomic::AtomicBool::new(true),
        };
        use crate::execution::exchange::AssetIndexProvider;
        assert_eq!(cache.asset_index("BTC"), Some(1));
        assert_eq!(cache.asset_index("ETH"), Some(2));
        assert_eq!(cache.asset_index("UNKNOWN"), None);
    }

    #[test]
    fn get_resolves_alias() {
        let (instruments, aliases) = parse_meta(&perp_meta(), &spot_meta()).unwrap();
        let cache = InstrumentMetaCache {
            source: Arc::new(UnreachableSource),
            refresh_interval: std::time::Duration::from_secs(1),
            instruments: std::sync::RwLock::new(instruments),
            spot_aliases: std::sync::RwLock::new(aliases),
            loaded: std::sync::atomic::AtomicBool::new(true),
        };
        let info = cache.get("PURR/USDC").unwrap();
        assert!(info.is_spot);
        assert_eq!(info.symbol, "@1");
        // Direct exchange coin also resolves.
        assert_eq!(cache.get("@1").unwrap().display_name, "PURR/USDC");
        // Perp resolves by direct symbol.
        assert_eq!(cache.get("BTC").unwrap().sz_decimals, 5);
    }

    struct UnreachableSource;

    #[async_trait]
    impl InstrumentMetaSource for UnreachableSource {
        async fn get_meta(&self) -> Result<serde_json::Value, HypeEdgeError> {
            unreachable!()
        }
        async fn get_spot_meta(&self) -> Result<serde_json::Value, HypeEdgeError> {
            unreachable!()
        }
    }

    #[test]
    fn resolve_spot_returns_only_spot() {
        let (instruments, aliases) = parse_meta(&perp_meta(), &spot_meta()).unwrap();
        let cache = InstrumentMetaCache {
            source: Arc::new(UnreachableSource),
            refresh_interval: std::time::Duration::from_secs(1),
            instruments: std::sync::RwLock::new(instruments),
            spot_aliases: std::sync::RwLock::new(aliases),
            loaded: std::sync::atomic::AtomicBool::new(true),
        };
        assert!(cache.resolve_spot("@1").is_some());
        assert!(cache.resolve_spot("BTC").is_none());
    }

    #[test]
    fn empty_universe_is_error() {
        assert!(parse_meta(&serde_json::json!({}), &spot_meta()).is_err());
    }

    #[test]
    fn convenience_getters() {
        let (instruments, _) = parse_meta(&perp_meta(), &spot_meta()).unwrap();
        let cache = InstrumentMetaCache {
            source: Arc::new(UnreachableSource),
            refresh_interval: std::time::Duration::from_secs(1),
            instruments: std::sync::RwLock::new(instruments),
            spot_aliases: std::sync::RwLock::new(HashMap::new()),
            loaded: std::sync::atomic::AtomicBool::new(true),
        };
        assert_eq!(cache.get_sz_decimals("BTC"), Some(5));
        assert!(cache.get_tick_size("BTC").is_some());
        assert_eq!(cache.get_sz_decimals("NOPE"), None);
    }

    #[test]
    fn to_spec_maps_fields() {
        let (instruments, _) = parse_meta(&perp_meta(), &spot_meta()).unwrap();
        let spec = instruments.get("BTC").unwrap().to_spec();
        assert_eq!(spec.symbol, "BTC");
        assert_eq!(spec.lot_size, power_of_ten_neg(5));
        assert_eq!(spec.max_significant_figures, 5);
        spec.validate().unwrap();
    }

    #[test]
    fn power_of_ten_neg_scales() {
        assert_eq!(power_of_ten_neg(0), Decimal::ONE);
        assert_eq!(power_of_ten_neg(2), Decimal::from_scaled(1, 2));
        assert_eq!(power_of_ten_neg(5), Decimal::from_scaled(1, 5));
    }
}
