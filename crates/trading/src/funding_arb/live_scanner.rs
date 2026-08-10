//! Live funding-arb market scanner (wiring follow-up): builds a perp/spot
//! candidate universe from live books + funding (via the market-data provider)
//! and 24h volumes (via the REST meta/asset-ctxs), mapping spot display names to
//! exchange coins through the instrument cache.

use std::sync::Arc;

use async_trait::async_trait;
use hypeedge_domain::decimal::Decimal;
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

/// Live scanner over the shared market-data provider + REST client.
pub struct LiveFundingArbScanner {
    provider: Arc<LiveMarketDataProvider>,
    rest: Arc<RestClient>,
    /// Perp symbol → (spot exchange symbol, spot display name).
    spot_map: tokio::sync::Mutex<Vec<(String, String, String)>>,
}

impl LiveFundingArbScanner {
    pub fn new(provider: Arc<LiveMarketDataProvider>, rest: Arc<RestClient>) -> Self {
        Self {
            provider,
            rest,
            spot_map: tokio::sync::Mutex::new(Vec::new()),
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
            let volumes = self.volumes(&perp, &spot, &display).await;
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
        let volumes = self.volumes(perp_symbol, spot_symbol, spot_symbol).await;
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
    /// 24h notional volumes from the asset-ctxs (perp `dayNtlVlm`) and spot
    /// (`dayNtlVlm` on spot pairs). Falls back to 0.
    async fn volumes(
        &self,
        perp: &str,
        spot_exchange: &str,
        spot_display: &str,
    ) -> (Decimal, Decimal) {
        let ctxs = match self.rest.get_meta_and_asset_ctxs().await {
            Ok(v) => v,
            Err(_) => return (Decimal::ZERO, Decimal::ZERO),
        };
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
