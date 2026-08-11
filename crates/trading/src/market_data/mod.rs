//! Market data: the Hyperliquid WebSocket feed, REST client, instrument cache,
//! rate limiter, in-memory order books, and the latest-state provider facade
//! (port of `src/hypeedge/market_data/`).

pub mod book;
pub mod external_reference;
pub mod features;
pub mod instrument_cache;
pub mod live_provider;
pub mod rate_limiter;
pub mod rest_client;
pub mod ws_feed;

pub use book::{BookManager, OrderBook};
pub use external_reference::{
    ExternalReferenceConfig, LatestExternalReferenceProvider, dec_to_f64,
};
pub use features::MarketFeatureEngine;
pub use instrument_cache::{
    InstrumentInfo, InstrumentMetaCache, InstrumentMetaSource, META_REFRESH_INTERVAL_HOURS,
    ParsedMeta, parse_meta,
};
pub use live_provider::{CandleHistoryClient, LiveMarketDataProvider, MarketPriceSnapshot};
pub use rate_limiter::{IP_WEIGHT_LIMIT_PER_MIN, RateLimiter};
pub use rest_client::{RestClient, interval_to_ms};
pub use ws_feed::{WebSocketFeed, WsFeedConfig};
