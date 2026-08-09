//! Market data: in-memory order books, the Hyperliquid rate limiter, and the
//! latest-state provider facade (port of `src/hypeedge/market_data/`).
//!
//! The WebSocket feed, REST client, and instrument cache land in later Phase-2
//! increments; the pure, testable pieces (book, rate limiter, provider) are
//! here now.

pub mod book;
pub mod features;
pub mod rate_limiter;
pub mod ws_feed;

pub use book::{BookManager, OrderBook};
pub use features::MarketFeatureEngine;
pub use rate_limiter::{IP_WEIGHT_LIMIT_PER_MIN, RateLimiter};
pub use ws_feed::{WebSocketFeed, WsFeedConfig};
