//! The exchange mutation boundary (mirrors the HL SDK `Exchange`/`Info`
//! methods the execution engine calls).
//!
//! The engine never talks to Hyperliquid directly: it builds the wire orders
//! and hands them to an [`ExchangeClient`], which signs and POSTs inside the
//! serial nonce worker. Implementations are mockable, so the full gate
//! sequence (submit → ack → fill/cancel, timeout → SUBMIT_UNKNOWN → cloid
//! resolution) is unit-tested against scripted responses.

use async_trait::async_trait;
use serde_json::Value;

use crate::execution::signing::{
    CancelActionWire, CancelByCloidActionWire, CancelByCloidWire, CancelWire, LeverageActionWire,
    OrderActionWire, OrderWire, pack_action, sign_l1_action,
};

/// Symbol → asset-index resolution (`info.name_to_asset`). Populated from the
/// exchange `meta` universe; a `None` means the symbol is unknown.
pub trait AssetIndexProvider: Send + Sync {
    fn asset_index(&self, symbol: &str) -> Option<i64>;
}

/// Sign + send exchange mutations, and query order truth without signing.
#[async_trait]
pub trait ExchangeClient: Send + Sync {
    /// Place a batch of orders (limit, or market-as-IoC via `Ioc` tif).
    async fn order(&self, orders: Vec<OrderWire>, nonce: u64) -> Result<Value, String>;

    /// Cancel orders by exchange oid.
    async fn cancel(&self, cancels: Vec<CancelWire>, nonce: u64) -> Result<Value, String>;

    /// Cancel orders by cloid (`cancelByCloid` — what the engine uses).
    async fn cancel_by_cloid(
        &self,
        cancels: Vec<CancelByCloidWire>,
        nonce: u64,
    ) -> Result<Value, String>;

    /// Update per-symbol leverage.
    async fn update_leverage(
        &self,
        asset: i64,
        is_cross: bool,
        leverage: i64,
        nonce: u64,
    ) -> Result<Value, String>;

    /// Query an order's authoritative status by cloid. Returns the raw
    /// `{"status": "order", "order": {...}}` response, or `None` when the
    /// exchange reports it does not know the cloid.
    async fn query_order_by_cloid(&self, cloid: &str) -> Result<Option<Value>, String>;
}

/// Real Hyperliquid client: signs the L1 phantom-agent action and POSTs to
/// `/exchange`; reads `/info` for order-status queries. URL/header shape
/// matches the pinned SDK 0.23.0 so a golden `/exchange` body check can later
/// assert byte-for-byte equality.
pub struct HyperliquidExchangeClient {
    private_key: [u8; 32],
    is_mainnet: bool,
    http: reqwest::Client,
    exchange_url: String,
    info_url: String,
    account_address: String,
}

impl HyperliquidExchangeClient {
    pub fn new(
        private_key: [u8; 32],
        is_mainnet: bool,
        exchange_url: impl Into<String>,
        info_url: impl Into<String>,
        account_address: impl Into<String>,
    ) -> Self {
        Self {
            private_key,
            is_mainnet,
            http: reqwest::Client::new(),
            exchange_url: exchange_url.into(),
            info_url: info_url.into(),
            account_address: account_address.into().to_lowercase(),
        }
    }

    /// The `/exchange` request body: action + nonce + signature + vault +
    /// expiry markers. Matches the SDK `_post_action` payload field-for-field.
    pub fn exchange_body(
        action: &impl serde::Serialize,
        signature: &crate::execution::signing::SignatureParts,
        nonce: u64,
        vault_address: Option<String>,
        expires_after: Option<u64>,
    ) -> Result<Value, String> {
        let action = serde_json::to_value(action).map_err(|e| format!("action json: {e}"))?;
        Ok(serde_json::json!({
            "action": action,
            "nonce": nonce,
            "signature": {
                "r": signature.r_hex(),
                "s": signature.s_hex(),
                "v": signature.v,
            },
            "vaultAddress": vault_address,
            "expiresAfter": expires_after,
        }))
    }

    async fn post_exchange(&self, body: Value) -> Result<Value, String> {
        let resp = self
            .http
            .post(&self.exchange_url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("exchange POST failed: {e}"))?;
        resp.json::<Value>()
            .await
            .map_err(|e| format!("exchange POST response: {e}"))
    }

    async fn post_info(&self, body: Value) -> Result<Value, String> {
        let resp = self
            .http
            .post(&self.info_url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("info POST failed: {e}"))?;
        resp.json::<Value>()
            .await
            .map_err(|e| format!("info POST response: {e}"))
    }
}

#[async_trait]
impl ExchangeClient for HyperliquidExchangeClient {
    async fn order(&self, orders: Vec<OrderWire>, nonce: u64) -> Result<Value, String> {
        let action = OrderActionWire {
            type_: "order",
            orders,
            grouping: "na",
        };
        let bytes = pack_action(&action)?;
        let sig = sign_l1_action(
            &self.private_key,
            &bytes,
            None,
            nonce,
            None,
            self.is_mainnet,
        )?;
        let body = Self::exchange_body(&action, &sig, nonce, None, None)?;
        self.post_exchange(body).await
    }

    async fn cancel(&self, cancels: Vec<CancelWire>, nonce: u64) -> Result<Value, String> {
        let action = CancelActionWire {
            type_: "cancel",
            cancels,
        };
        let bytes = pack_action(&action)?;
        let sig = sign_l1_action(
            &self.private_key,
            &bytes,
            None,
            nonce,
            None,
            self.is_mainnet,
        )?;
        let body = Self::exchange_body(&action, &sig, nonce, None, None)?;
        self.post_exchange(body).await
    }

    async fn cancel_by_cloid(
        &self,
        cancels: Vec<CancelByCloidWire>,
        nonce: u64,
    ) -> Result<Value, String> {
        let action = CancelByCloidActionWire {
            type_: "cancelByCloid",
            cancels,
        };
        let bytes = pack_action(&action)?;
        let sig = sign_l1_action(
            &self.private_key,
            &bytes,
            None,
            nonce,
            None,
            self.is_mainnet,
        )?;
        let body = Self::exchange_body(&action, &sig, nonce, None, None)?;
        self.post_exchange(body).await
    }

    async fn update_leverage(
        &self,
        asset: i64,
        is_cross: bool,
        leverage: i64,
        nonce: u64,
    ) -> Result<Value, String> {
        let action = LeverageActionWire {
            type_: "updateLeverage",
            asset,
            is_cross,
            leverage,
        };
        let bytes = pack_action(&action)?;
        let sig = sign_l1_action(
            &self.private_key,
            &bytes,
            None,
            nonce,
            None,
            self.is_mainnet,
        )?;
        let body = Self::exchange_body(&action, &sig, nonce, None, None)?;
        self.post_exchange(body).await
    }

    async fn query_order_by_cloid(&self, cloid: &str) -> Result<Option<Value>, String> {
        let body = serde_json::json!({
            "type": "queryOrderByCloid",
            "user": self.account_address(),
            "oid": cloid,
        });
        let resp = self.post_info(body).await?;
        match resp.get("status").and_then(|s| s.as_str()) {
            Some("order") => Ok(Some(resp)),
            _ => Ok(None),
        }
    }
}

impl HyperliquidExchangeClient {
    fn account_address(&self) -> &str {
        &self.account_address
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_address_returns_configured_value() {
        // A2 regression: queryOrderByCloid must use the real account address,
        // not the zero-address placeholder.
        let client = HyperliquidExchangeClient::new(
            [0u8; 32],
            false,
            "https://exchange.test",
            "https://info.test",
            "0xAbCdEf1234567890AbCdEf1234567890",
        );
        assert_eq!(
            client.account_address(),
            "0xabcdef1234567890abcdef1234567890"
        );
    }

    #[test]
    fn exchange_body_matches_sdk_shape() {
        let wire = OrderWire {
            a: 0,
            b: true,
            p: "65000.0".to_string(),
            s: "0.1".to_string(),
            r: false,
            t: crate::execution::signing::OrderTypeWire {
                limit: crate::execution::signing::TifWire { tif: "Gtc" },
            },
            c: Some(format!("0x{}", "0".repeat(62) + "01")),
        };
        let action = OrderActionWire {
            type_: "order",
            orders: vec![wire],
            grouping: "na",
        };
        let sig = crate::execution::signing::SignatureParts {
            r: [1u8; 32],
            s: [2u8; 32],
            v: 27,
        };
        let body = HyperliquidExchangeClient::exchange_body(&action, &sig, 99, None, None).unwrap();
        assert_eq!(body["action"]["type"], "order");
        assert_eq!(body["nonce"], 99);
        assert_eq!(body["signature"]["v"], 27);
        assert!(body["signature"]["r"].as_str().unwrap().starts_with("0x"));
        // vault/expiry present as null (SDK always includes them).
        assert!(body["vaultAddress"].is_null());
        assert!(body["expiresAfter"].is_null());
        // Action carries the full wire order.
        assert_eq!(body["action"]["orders"][0]["a"], 0);
        assert_eq!(body["action"]["orders"][0]["p"], "65000.0");
    }
}
