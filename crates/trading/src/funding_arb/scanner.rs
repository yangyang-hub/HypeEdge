//! Funding-arb market scanner protocol + snapshot, port of
//! `src/hypeedge/market_data/funding_arb_scanner.py` boundary.

use async_trait::async_trait;
use hypeedge_domain::decimal::Decimal;
use hypeedge_domain::models::L2BookSnapshot;

/// A funding-arb candidate market snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct FundingArbMarketSnapshot {
    pub perp_symbol: String,
    pub spot_symbol: String,
    pub spot_display: String,
    pub funding_rate: Decimal,
    pub perp_24h_volume_usd: Decimal,
    pub spot_24h_volume_usd: Decimal,
    pub perp_book: L2BookSnapshot,
    pub spot_book: L2BookSnapshot,
}

/// The scanner boundary consumed by the funding-arb runtime.
#[async_trait]
pub trait FundingArbMarketScanner: Send + Sync {
    /// Scan the perp+spot universe for candidate markets.
    async fn scan(&self) -> Result<Vec<FundingArbMarketSnapshot>, String>;
    /// Fetch one specific market pair.
    async fn get_market(
        &self,
        perp_symbol: &str,
        spot_symbol: &str,
    ) -> Result<Option<FundingArbMarketSnapshot>, String>;
}
