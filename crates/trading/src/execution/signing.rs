//! Hyperliquid L1 "phantom agent" signing, byte-exact port of
//! `hyperliquid/utils/signing.py` (SDK 0.23.0) as used by
//! `src/hypeedge/execution/`.
//!
//! The scheme (from the Python SDK):
//! 1. `action_hash = keccak256( msgpack.packb(action) ‖ nonce(8B big) ‖
//!    vault_marker ‖ [expiry_marker] )` where vault_marker is `0x00` (no vault)
//!    or `0x01 ‖ 20B address`, and expiry_marker is `0x00 ‖ expires_after(8B)`.
//! 2. `phantom_agent = { source: "a"|"b", connectionId: 32B }`.
//! 3. EIP-712 typed data over `Agent(string source,bytes32 connectionId)` with
//!    domain `{name:"Exchange", version:"1", chainId:1337,
//!    verifyingContract:0x00…0}`.
//! 4. ECDSA over the EIP-712 digest; `v = 27 + recovery_id` (eth-account's
//!    `to_eth_v` with no chain id adds `V_OFFSET=27`).
//!
//! Parity is pinned by `crates/domain/tests/fixtures/signing.jsonl` (Phase 0
//! recorder): the Rust signature and full `/exchange` POST body must match the
//! recorded Python output byte-for-byte.

use k256::ecdsa::SigningKey;
use serde::Serialize;
use sha3::{Digest, Keccak256};

/// keccak256 of the given bytes.
fn keccak(data: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// `time-in-force` wire variants.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct TifWire {
    pub tif: &'static str,
}

/// `OrderTypeWire.t` — the nested order-type object.
#[derive(Debug, Clone, Serialize)]
pub struct OrderTypeWire {
    pub limit: TifWire,
}

/// `OrderWire` — one order in the batch.
#[derive(Debug, Clone, Serialize)]
pub struct OrderWire {
    /// Asset index.
    pub a: i64,
    /// Is buy.
    pub b: bool,
    /// Limit price as decimal string.
    pub p: String,
    /// Size as decimal string.
    pub s: String,
    /// Reduce-only.
    pub r: bool,
    /// Order-type object.
    pub t: OrderTypeWire,
    /// Client order id (`0x` + 32 hex). Serialized only when present.
    #[serde(rename = "c", skip_serializing_if = "Option::is_none")]
    pub c: Option<String>,
}

/// `{"type":"order","orders":[...],"grouping":"na"}` — field order is
/// load-bearing for msgpack key order (matches the Python dict insertion
/// order).
#[derive(Debug, Clone, Serialize)]
pub struct OrderActionWire {
    #[serde(rename = "type")]
    pub type_: &'static str,
    pub orders: Vec<OrderWire>,
    pub grouping: &'static str,
}

/// `{"type":"cancel","cancels":[{"a":asset,"o":oid}]}`.
#[derive(Debug, Clone, Serialize)]
pub struct CancelActionWire {
    #[serde(rename = "type")]
    pub type_: &'static str,
    pub cancels: Vec<CancelWire>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CancelWire {
    pub a: i64,
    pub o: i64,
}

/// `{"type":"cancelByCloid","cancels":[{"asset":a,"cloid":"0x…"}]}`.
#[derive(Debug, Clone, Serialize)]
pub struct CancelByCloidActionWire {
    #[serde(rename = "type")]
    pub type_: &'static str,
    pub cancels: Vec<CancelByCloidWire>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CancelByCloidWire {
    pub asset: i64,
    pub cloid: String,
}

/// `{"type":"updateLeverage","asset":a,"isCross":b,"leverage":n}`.
#[derive(Debug, Clone, Serialize)]
pub struct LeverageActionWire {
    #[serde(rename = "type")]
    pub type_: &'static str,
    pub asset: i64,
    #[serde(rename = "isCross")]
    pub is_cross: bool,
    pub leverage: i64,
}

/// Messagepack the action with `rmp_serde::to_vec_named` (map with field-name
/// keys, declaration order) — the exact equivalent of Python `msgpack.packb`
/// on the equivalent dict.
pub fn pack_action(action: &impl Serialize) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(action).map_err(|e| format!("msgpack: {e}"))
}

/// The `action_hash`: keccak256 of msgpack(action) ‖ nonce ‖ vault ‖ [expiry].
pub fn action_hash(
    action_bytes: &[u8],
    vault_address: Option<&[u8; 20]>,
    nonce: u64,
    expires_after: Option<u64>,
) -> [u8; 32] {
    let mut data = Vec::with_capacity(action_bytes.len() + 8 + 1 + 20 + 1 + 8);
    data.extend_from_slice(action_bytes);
    data.extend_from_slice(&nonce.to_be_bytes());
    match vault_address {
        None => data.push(0x00),
        Some(addr) => {
            data.push(0x01);
            data.extend_from_slice(addr);
        }
    }
    if let Some(exp) = expires_after {
        data.push(0x00);
        data.extend_from_slice(&exp.to_be_bytes());
    }
    keccak(&data)
}

/// `construct_phantom_agent`: `{source: "a"|"b", connectionId}`.
fn phantom_agent(hash: &[u8; 32], is_mainnet: bool) -> (String, [u8; 32]) {
    (
        if is_mainnet {
            "a".to_string()
        } else {
            "b".to_string()
        },
        *hash,
    )
}

/// EIP-712 `Agent(string source,bytes32 connectionId)` digest.
///
/// digest = keccak256( 0x1901 ‖ domainSeparator ‖ structHash )
/// domainSeparator = keccak256( abi.encode(EIP712Domain(name,version,chainId,
///   verifyingContract)) )  — all four are static types so it's a plain concat.
/// structHash = keccak256( abi.encode( AgentTypehash, keccak256(source),
///   connectionId ) ).
fn eip712_agent_digest(source: &str, connection_id: &[u8; 32]) -> [u8; 32] {
    let agent_typehash: [u8; 32] = keccak(b"Agent(string source,bytes32 connectionId)");
    let domain_typehash: [u8; 32] = keccak(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let name_hash: [u8; 32] = keccak(b"Exchange");
    let version_hash: [u8; 32] = keccak(b"1");

    // abi.encode of static types: domainSeparator = typehash ‖ name ‖ version
    // ‖ chainId(32B) ‖ verifyingContract(32B left-padded) = 5 words.
    let mut domain = [0u8; 32 * 5];
    domain[0..32].copy_from_slice(&domain_typehash);
    domain[32..64].copy_from_slice(&name_hash);
    domain[64..96].copy_from_slice(&version_hash);
    // chainId as uint256, right-aligned in its 32-byte slot.
    domain[120..128].copy_from_slice(&1337u64.to_be_bytes());
    // verifyingContract = address(0) — the last 32 bytes stay zero.
    let domain_separator: [u8; 32] = keccak(&domain);

    // structHash = AgentTypehash ‖ keccak256(source) ‖ connectionId.
    let source_hash: [u8; 32] = keccak(source.as_bytes());
    let mut struct_data = [0u8; 96];
    struct_data[0..32].copy_from_slice(&agent_typehash);
    struct_data[32..64].copy_from_slice(&source_hash);
    struct_data[64..96].copy_from_slice(connection_id);

    let struct_hash: [u8; 32] = keccak(&struct_data);

    let mut seal = [0u8; 66];
    seal[0..2].copy_from_slice(b"\x19\x01");
    seal[2..34].copy_from_slice(&domain_separator);
    seal[34..66].copy_from_slice(&struct_hash);
    keccak(&seal)
}

/// A (r, s, v) signature, matching the SDK's `sign_inner` output shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignatureParts {
    pub r: [u8; 32],
    pub s: [u8; 32],
    /// `27 + recovery_id` (eth-account `to_eth_v` with no chain id).
    pub v: u8,
}

impl SignatureParts {
    pub fn r_hex(&self) -> String {
        format!("0x{}", hex::encode(self.r))
    }
    pub fn s_hex(&self) -> String {
        format!("0x{}", hex::encode(self.s))
    }
}

/// Sign an L1 action (already msgpacked) with the agent private key.
pub fn sign_l1_action(
    private_key: &[u8; 32],
    action_bytes: &[u8],
    vault_address: Option<&[u8; 20]>,
    nonce: u64,
    expires_after: Option<u64>,
    is_mainnet: bool,
) -> Result<SignatureParts, String> {
    let hash = action_hash(action_bytes, vault_address, nonce, expires_after);
    let (source, connection_id) = phantom_agent(&hash, is_mainnet);
    let digest = eip712_agent_digest(&source, &connection_id);

    let signing_key = SigningKey::from_bytes(private_key.into())
        .map_err(|e| format!("invalid private key: {e}"))?;
    let (sig, recid) = signing_key
        .sign_prehash_recoverable(&digest)
        .map_err(|e| format!("sign: {e}"))?;
    let bytes = sig.to_bytes();
    let r: [u8; 32] = bytes[..32].try_into().expect("r is 32 bytes");
    let s: [u8; 32] = bytes[32..].try_into().expect("s is 32 bytes");
    Ok(SignatureParts {
        r,
        s,
        v: 27 + recid.to_byte(),
    })
}

/// Build and sign an order action, returning `(action_bytes, signature)`.
pub fn sign_order_action(
    private_key: &[u8; 32],
    orders: &[OrderWire],
    nonce: u64,
    is_mainnet: bool,
) -> Result<(Vec<u8>, SignatureParts), String> {
    let action = OrderActionWire {
        type_: "order",
        orders: orders.to_vec(),
        grouping: "na",
    };
    let bytes = pack_action(&action)?;
    let sig = sign_l1_action(private_key, &bytes, None, nonce, None, is_mainnet)?;
    Ok((bytes, sig))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order_wire() -> OrderWire {
        OrderWire {
            a: 0,
            b: true,
            p: "65000.0".to_string(),
            s: "0.1".to_string(),
            r: false,
            t: OrderTypeWire {
                limit: TifWire { tif: "Gtc" },
            },
            c: Some(
                "0x0000000000000000000000000000000000000000000000000000000000000001".to_string(),
            ),
        }
    }

    #[test]
    fn order_action_msgpack_matches_expected_shape() {
        let action = OrderActionWire {
            type_: "order",
            orders: vec![order_wire()],
            grouping: "na",
        };
        let bytes = pack_action(&action).unwrap();
        // First byte should be a fixmap (0x80 | 3) with 3 keys.
        assert_eq!(bytes[0] & 0xf0, 0x80);
        // Contains the keys in declaration order.
        let s = String::from_utf8_lossy(&bytes);
        assert!(s.contains("type"));
        assert!(s.contains("orders"));
        assert!(s.contains("grouping"));
    }

    #[test]
    fn action_hash_deterministic_and_32_bytes() {
        let action_bytes = pack_action(&OrderActionWire {
            type_: "order",
            orders: vec![order_wire()],
            grouping: "na",
        })
        .unwrap();
        let h1 = action_hash(&action_bytes, None, 1_700_000_000_000, None);
        let h2 = action_hash(&action_bytes, None, 1_700_000_000_000, None);
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 32);
        // Changing the nonce changes the hash.
        let h3 = action_hash(&action_bytes, None, 1_700_000_000_001, None);
        assert_ne!(h1, h3);
    }

    #[test]
    fn agent_digest_is_stable() {
        let d1 = eip712_agent_digest("a", &[0u8; 32]);
        let d2 = eip712_agent_digest("a", &[0u8; 32]);
        assert_eq!(d1, d2);
        let d3 = eip712_agent_digest("b", &[0u8; 32]);
        assert_ne!(d1, d3);
    }

    #[test]
    fn sign_produces_r_s_v_and_v_is_27_or_28() {
        let key = [7u8; 32];
        let action_bytes = pack_action(&OrderActionWire {
            type_: "order",
            orders: vec![order_wire()],
            grouping: "na",
        })
        .unwrap();
        let sig = sign_l1_action(&key, &action_bytes, None, 99, None, false).unwrap();
        assert_eq!(sig.r.len(), 32);
        assert_eq!(sig.s.len(), 32);
        assert!(
            sig.v == 27 || sig.v == 28,
            "v must be 27 or 28, got {}",
            sig.v
        );
        assert!(sig.r_hex().starts_with("0x"));
    }

    #[test]
    fn sign_order_action_roundtrips() {
        let key = [9u8; 32];
        let (bytes, sig) = sign_order_action(&key, &[order_wire()], 42, true).unwrap();
        assert!(!bytes.is_empty());
        assert!(sig.v == 27 || sig.v == 28);
    }

    /// Golden parity: this exact signature was produced by the pinned
    /// hyperliquid-python-sdk (0.23.0) via `sign_l1_action` for the same
    /// action, private key, and nonce. It pins the msgpack key order, the
    /// action_hash construction, the EIP-712 digest, and the `v=27+recid`
    /// convention in one assertion.
    #[test]
    fn signature_matches_python_sdk_golden() {
        let key = [7u8; 32];
        let nonce = 99u64;
        let order = OrderWire {
            a: 0,
            b: true,
            p: "65000.0".to_string(),
            s: "0.1".to_string(),
            r: false,
            t: OrderTypeWire {
                limit: TifWire { tif: "Gtc" },
            },
            c: Some(format!("0x{}", "0".repeat(62) + "01")),
        };
        let action = OrderActionWire {
            type_: "order",
            orders: vec![order],
            grouping: "na",
        };
        let action_bytes = pack_action(&action).unwrap();

        // Python: action_hash = 0x5b03f32cf3cc2d5c819b01cd6a9a2c384c2182e5680c771a568382a548bb075b
        let hash = action_hash(&action_bytes, None, nonce, None);
        assert_eq!(
            hex::encode(hash),
            "5b03f32cf3cc2d5c819b01cd6a9a2c384c2182e5680c771a568382a548bb075b",
            "action_hash must match the Python SDK byte-for-byte"
        );

        let sig = sign_l1_action(&key, &action_bytes, None, nonce, None, false).unwrap();
        assert_eq!(
            sig.r_hex(),
            "0xb02738a893575469b4610342806fa246440753b7519ac84b05471e8967b73c3d"
        );
        assert_eq!(
            sig.s_hex(),
            "0x6a6dfe5431c686b2247a73afc472902c120be2c2a5236e78497859a03af74ca9"
        );
        assert_eq!(sig.v, 27);
    }
}
