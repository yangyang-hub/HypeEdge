//! Live funding-arb market scanner (wiring follow-up): builds a perp/spot
//! candidate universe from live books + funding (via the market-data provider)
//! and 24h volumes (via the REST meta/asset-ctxs), mapping spot display names to
//! exchange coins through the instrument cache.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use hypeedge_domain::decimal::Decimal;
use hypeedge_domain::error::HypeEdgeError;
use hypeedge_domain::models::L2BookSnapshot;

use super::runtime::FundingArbInstrumentMeta;
use super::scanner::{FundingArbMarketScanner, FundingArbMarketSnapshot};
use crate::market_data::instrument_cache::InstrumentMetaCache;
use crate::market_data::live_provider::LiveMarketDataProvider;
use crate::market_data::rest_client::RestClient;

/// Adapter exposing the shared instrument cache as the funding-arb runtime's
/// instrument-metadata boundary (spot/perp specs: tick/lot/min size, leverage).
pub struct InstrumentCacheFundingArbMeta {
    cache: Arc<InstrumentMetaCache>,
}

impl InstrumentCacheFundingArbMeta {
    pub fn new(cache: Arc<InstrumentMetaCache>) -> Self {
        Self { cache }
    }
}

impl FundingArbInstrumentMeta for InstrumentCacheFundingArbMeta {
    fn get(&self, symbol: &str) -> Option<super::runtime::InstrumentInfo> {
        let info = self.cache.get(symbol)?;
        let base = info
            .base_token
            .clone()
            .unwrap_or_else(|| info.symbol.clone());
        Some(super::runtime::InstrumentInfo {
            symbol: info.symbol.clone(),
            display_name: info.display_name.clone(),
            base_token: base,
            quote_token: info
                .quote_token
                .clone()
                .unwrap_or_else(|| "USDC".to_string()),
            is_spot: info.is_spot,
            tick_size: info.tick_size,
            lot_size: info.lot_size,
            min_size: info.min_size,
            max_leverage: info.max_leverage,
        })
    }
}

/// The subset of [`RestClient`] the live scanner needs. Kept as a trait so the
/// scanner is testable with a counting fake (M-FA4) while the public
/// constructor still accepts the concrete `Arc<RestClient>`.
#[async_trait]
trait ScannerRestSource: Send + Sync {
    async fn get_meta(&self) -> Result<serde_json::Value, HypeEdgeError>;
    async fn get_spot_meta(&self) -> Result<serde_json::Value, HypeEdgeError>;
    async fn get_meta_and_asset_ctxs(&self) -> Result<serde_json::Value, HypeEdgeError>;
}

#[async_trait]
impl ScannerRestSource for RestClient {
    async fn get_meta(&self) -> Result<serde_json::Value, HypeEdgeError> {
        RestClient::get_meta(self).await
    }
    async fn get_spot_meta(&self) -> Result<serde_json::Value, HypeEdgeError> {
        RestClient::get_spot_meta(self).await
    }
    async fn get_meta_and_asset_ctxs(&self) -> Result<serde_json::Value, HypeEdgeError> {
        RestClient::get_meta_and_asset_ctxs(self).await
    }
}

/// How long one `metaAndAssetCtxs` fetch is reused (M-FA4). Volumes move on
/// the minute scale; a 30s cache is safely fresh while cutting N-pair scans to
/// one meta request per scan window.
const ASSET_CTXS_CACHE_SECONDS: u64 = 30;

/// Live scanner over the shared market-data provider + REST client.
pub struct LiveFundingArbScanner {
    provider: Arc<LiveMarketDataProvider>,
    rest: Arc<dyn ScannerRestSource>,
    /// Perp symbol → (spot exchange symbol, spot display name).
    spot_map: tokio::sync::Mutex<Vec<(String, String, String)>>,
    /// M-FA4: one cached `metaAndAssetCtxs` response per scan window — the
    /// per-pair `volumes()` lookups share it instead of re-fetching.
    asset_ctxs_cache: tokio::sync::Mutex<Option<(Instant, serde_json::Value)>>,
}

impl LiveFundingArbScanner {
    pub fn new(provider: Arc<LiveMarketDataProvider>, rest: Arc<RestClient>) -> Self {
        Self::new_with_source(provider, rest)
    }

    /// Test seam: inject any `ScannerRestSource` (counting fake).
    fn new_with_source(
        provider: Arc<LiveMarketDataProvider>,
        rest: Arc<dyn ScannerRestSource>,
    ) -> Self {
        Self {
            provider,
            rest,
            spot_map: tokio::sync::Mutex::new(Vec::new()),
            asset_ctxs_cache: tokio::sync::Mutex::new(None),
        }
    }

    /// Refresh the perp→spot mapping from the spot meta (display name → exchange
    /// coin) and the perp universe (base token → perp symbol).
    async fn refresh_spot_map(&self) -> Result<(), String> {
        let meta = self
            .rest
            .get_meta()
            .await
            .map_err(|e| format!("get_meta: {e}"))?;
        let spot_meta = self
            .rest
            .get_spot_meta()
            .await
            .map_err(|e| format!("get_spot_meta: {e}"))?;

        // spot universe: exchange name (@N) + tokens [base, quote] → display.
        let tokens = spot_meta
            .get("tokens")
            .and_then(|t| t.as_array())
            .cloned()
            .unwrap_or_default();
        let token_name = |idx: i64| -> Option<String> {
            tokens
                .iter()
                .find(|t| t.get("index").and_then(|v| v.as_i64()) == Some(idx))
                .and_then(|t| t.get("name").and_then(|v| v.as_str()))
                .map(String::from)
        };
        let spot_universe = spot_meta
            .get("universe")
            .and_then(|u| u.as_array())
            .cloned()
            .unwrap_or_default();
        // Map base token → spot pair display.
        let mut base_to_spot: Vec<(String, String, String)> = Vec::new();
        for spot in &spot_universe {
            let Some(exchange_name) = spot.get("name").and_then(|v| v.as_str()) else {
                continue;
            };
            let idxs = spot
                .get("tokens")
                .and_then(|t| t.as_array())
                .and_then(|a| a.iter().map(|v| v.as_i64()).collect::<Option<Vec<_>>>())
                .unwrap_or_default();
            if idxs.len() != 2 {
                continue;
            }
            let (Some(base), Some(quote)) = (token_name(idxs[0]), token_name(idxs[1])) else {
                continue;
            };
            if quote != "USDC" {
                continue;
            }
            let display = format!("{base}/{quote}");
            base_to_spot.push((base, exchange_name.to_string(), display));
        }

        // Perp universe: base token → perp symbol.
        let perp_universe = meta
            .get("universe")
            .and_then(|u| u.as_array())
            .cloned()
            .unwrap_or_default();
        let mut pairs = Vec::new();
        for perp in &perp_universe {
            let Some(perp_symbol) = perp.get("name").and_then(|v| v.as_str()) else {
                continue;
            };
            // On Hyperliquid the perp symbol IS the base token (e.g. "BTC").
            for (base_token, spot_symbol, display) in &base_to_spot {
                if base_token == perp_symbol {
                    pairs.push((
                        perp_symbol.to_string(),
                        spot_symbol.clone(),
                        display.clone(),
                    ));
                }
            }
        }
        *self.spot_map.lock().await = pairs;
        Ok(())
    }
}

#[async_trait]
impl FundingArbMarketScanner for LiveFundingArbScanner {
    async fn scan(&self) -> Result<Vec<FundingArbMarketSnapshot>, String> {
        self.refresh_spot_map().await?;
        let funding = self.provider.all_funding().await;
        // M-FA4: fetch the shared asset-ctxs once per scan (cached 30s) and
        // feed every pair from it — previously each pair re-fetched it.
        let ctxs = match self.asset_ctxs().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "funding_arb_asset_ctxs_unavailable");
                serde_json::json!({}) // all volumes read as 0 → no candidate passes
            }
        };
        let mut out = Vec::new();
        let pairs = self.spot_map.lock().await.clone();
        for (perp, spot, display) in pairs {
            let Some(funding_rate) = funding
                .iter()
                .find(|f| f.symbol == perp)
                .map(|f| f.funding_rate)
            else {
                continue;
            };
            let (Some(perp_book), Some(spot_book)) = (
                self.provider.get_book(&perp).await,
                self.provider.get_book(&spot).await,
            ) else {
                continue;
            };
            let volumes = self.volumes(&ctxs, &perp, &spot, &display);
            out.push(FundingArbMarketSnapshot {
                perp_symbol: perp,
                spot_symbol: spot,
                spot_display: display,
                funding_rate: Decimal::from_f64(funding_rate).unwrap_or_default(),
                perp_24h_volume_usd: volumes.0,
                spot_24h_volume_usd: volumes.1,
                perp_book,
                spot_book,
            });
        }
        Ok(out)
    }

    async fn get_market(
        &self,
        perp_symbol: &str,
        spot_symbol: &str,
    ) -> Result<Option<FundingArbMarketSnapshot>, String> {
        let funding = self.provider.get_funding(perp_symbol).await;
        let (Some(funding_rate), Some(perp_book), Some(spot_book)) = (
            funding.map(|f| f.funding_rate),
            self.provider.get_book(perp_symbol).await,
            self.provider.get_book(spot_symbol).await,
        ) else {
            return Ok(None);
        };
        let ctxs = match self.asset_ctxs().await {
            Ok(v) => v,
            Err(_) => serde_json::json!({}),
        };
        let volumes = self.volumes(&ctxs, perp_symbol, spot_symbol, spot_symbol);
        Ok(Some(FundingArbMarketSnapshot {
            perp_symbol: perp_symbol.to_string(),
            spot_symbol: spot_symbol.to_string(),
            spot_display: format!("{spot_symbol}/USDC"),
            funding_rate: Decimal::from_f64(funding_rate).unwrap_or_default(),
            perp_24h_volume_usd: volumes.0,
            spot_24h_volume_usd: volumes.1,
            perp_book,
            spot_book,
        }))
    }
}

impl LiveFundingArbScanner {
    /// The shared `metaAndAssetCtxs` response, cached for
    /// [`ASSET_CTXS_CACHE_SECONDS`] (M-FA4: one request per scan window, not
    /// one per pair).
    async fn asset_ctxs(&self) -> Result<serde_json::Value, String> {
        let mut cache = self.asset_ctxs_cache.lock().await;
        if let Some((fetched_at, value)) = cache.as_ref()
            && fetched_at.elapsed() < Duration::from_secs(ASSET_CTXS_CACHE_SECONDS)
        {
            return Ok(value.clone());
        }
        let value = self
            .rest
            .get_meta_and_asset_ctxs()
            .await
            .map_err(|e| e.to_string())?;
        *cache = Some((Instant::now(), value.clone()));
        Ok(value)
    }

    /// 24h notional volumes from a pre-fetched asset-ctxs payload (perp
    /// `dayNtlVlm` and spot `dayNtlVlm` on spot pairs). Falls back to 0.
    fn volumes(
        &self,
        ctxs: &serde_json::Value,
        perp: &str,
        spot_exchange: &str,
        spot_display: &str,
    ) -> (Decimal, Decimal) {
        let mut perp_v = Decimal::ZERO;
        let mut spot_v = Decimal::ZERO;
        if let Some(assets) = ctxs.get("assetCtxs").and_then(|a| a.as_array()) {
            for a in assets {
                let coin = a.get("coin").and_then(|c| c.as_str()).unwrap_or("");
                let vol = a
                    .get("dayNtlVlm")
                    .and_then(|v| v.as_str())
                    .and_then(|s| Decimal::from_str_lenient(s).ok())
                    .unwrap_or(Decimal::ZERO);
                if coin == perp {
                    perp_v = vol;
                }
            }
        }
        if let Some(assets) = ctxs.get("spotAssetCtxs").and_then(|a| a.as_array()) {
            for a in assets {
                let coin = a.get("coin").and_then(|c| c.as_str()).unwrap_or("");
                let vol = a
                    .get("dayNtlVlm")
                    .and_then(|v| v.as_str())
                    .and_then(|s| Decimal::from_str_lenient(s).ok())
                    .unwrap_or(Decimal::ZERO);
                if coin == spot_exchange || coin == spot_display {
                    spot_v = vol;
                }
            }
        }
        (perp_v, spot_v)
    }
}

/// A helper to suppress the unused L2BookSnapshot import if it's unused.
#[allow(unused)]
fn _book(_: &L2BookSnapshot) {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    /// Counting REST source: answers the meta/spot-meta/asset-ctxs calls with
    /// fixture payloads and counts `metaAndAssetCtxs` fetches (M-FA4).
    struct CountingSource {
        ctxs_calls: AtomicUsize,
    }
    #[async_trait]
    impl ScannerRestSource for CountingSource {
        async fn get_meta(&self) -> Result<serde_json::Value, HypeEdgeError> {
            Ok(serde_json::json!({
                "universe": [{"name": "BTC"}, {"name": "ETH"}]
            }))
        }
        async fn get_spot_meta(&self) -> Result<serde_json::Value, HypeEdgeError> {
            Ok(serde_json::json!({
                "tokens": [
                    {"index": 0, "name": "BTC"},
                    {"index": 1, "name": "USDC"},
                    {"index": 2, "name": "ETH"}
                ],
                "universe": [
                    {"name": "@1", "tokens": [0, 1]},
                    {"name": "@2", "tokens": [2, 1]}
                ]
            }))
        }
        async fn get_meta_and_asset_ctxs(&self) -> Result<serde_json::Value, HypeEdgeError> {
            self.ctxs_calls.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(serde_json::json!({
                "assetCtxs": [{"coin": "BTC", "dayNtlVlm": "100000"}],
                "spotAssetCtxs": [{"coin": "@1", "dayNtlVlm": "50000"}]
            }))
        }
    }

    struct NoopCandleClient;
    #[async_trait]
    impl crate::market_data::live_provider::CandleHistoryClient for NoopCandleClient {
        async fn backfill_candles(
            &self,
            _: &str,
            _: &str,
            _: i64,
            _: i64,
        ) -> Result<Vec<hypeedge_domain::models::Candle>, HypeEdgeError> {
            Ok(vec![])
        }
        async fn backfill_funding(
            &self,
            _: &str,
            _: i64,
            _: i64,
        ) -> Result<Vec<hypeedge_domain::models::FundingRate>, HypeEdgeError> {
            Ok(vec![])
        }
    }

    fn provider() -> Arc<LiveMarketDataProvider> {
        Arc::new(LiveMarketDataProvider::new(
            Arc::new(hypeedge_infra::event_bus::EventBus::new(16)),
            Arc::new(NoopCandleClient),
            Arc::new(tokio::sync::Mutex::new(crate::market_data::BookManager::new(20))),
        ))
    }

    #[tokio::test]
    async fn scan_fetches_asset_ctxs_once_across_pairs_and_scans() {
        // M-FA4 regression: the shared metaAndAssetCtxs is fetched once per
        // scan window (cached 30s), not once per pair — two scans over two
        // pairs must hit the endpoint exactly once.
        let source = Arc::new(CountingSource {
            ctxs_calls: AtomicUsize::new(0),
        });
        let scanner = LiveFundingArbScanner::new_with_source(provider(), source.clone());
        scanner.scan().await.unwrap();
        scanner.scan().await.unwrap();
        assert_eq!(
            source.ctxs_calls.load(AtomicOrdering::SeqCst),
            1,
            "asset-ctxs must be fetched once per cache window (M-FA4)"
        );
    }

    #[test]
    fn volumes_parse_from_shared_ctxs() {
        let scanner = LiveFundingArbScanner::new_with_source(provider(), Arc::new(CountingSource {
            ctxs_calls: AtomicUsize::new(0),
        }));
        let ctxs = serde_json::json!({
            "assetCtxs": [
                {"coin": "BTC", "dayNtlVlm": "100000"},
                {"coin": "ETH", "dayNtlVlm": "50000"}
            ],
            "spotAssetCtxs": [
                {"coin": "@1", "dayNtlVlm": "40000"},
                {"coin": "@2", "dayNtlVlm": "30000"}
            ]
        });
        let (perp, spot) = scanner.volumes(&ctxs, "BTC", "@1", "BTC/USDC");
        assert_eq!(perp.to_string(), "100000");
        assert_eq!(spot.to_string(), "40000");
        let (perp, spot) = scanner.volumes(&ctxs, "ETH", "@2", "ETH/USDC");
        assert_eq!(perp.to_string(), "50000");
        assert_eq!(spot.to_string(), "30000");
        // Unknown pair → 0, never a failure.
        let (perp, spot) = scanner.volumes(&ctxs, "DOGE", "@9", "DOGE/USDC");
        assert_eq!(perp.to_string(), "0");
        assert_eq!(spot.to_string(), "0");
    }
}
