//! Execution: cloid generation, order normalization, the order state machine,
//! EIP-712/L1 signing, the serial nonce queue, the exchange boundary, and the
//! execution engine.
//!
//! Ports `src/hypeedge/execution/`. The engine is the sole order submission
//! outlet: it funnels every mutation through the serial nonce queue, checks the
//! kill switch before each placement, and applies exchange outcomes only from
//! authoritative responses (timeouts degrade to `SUBMIT_UNKNOWN`/`CANCEL_UNKNOWN`
//! for reconciliation — never a blind resend).

pub mod cloid;
pub mod engine;
pub mod exchange;
pub mod nonce;
pub mod normalizer;
pub mod order_state;
pub mod signing;

pub use cloid::CloidGenerator;
pub use engine::{ExecutionEngine, ExecutionEngineConfig};
pub use exchange::{
    AssetIndexProvider, ExchangeClient, HyperliquidExchangeClient,
};
pub use nonce::{ActionRequest, ActionResult, NonceGenerator, NonceQueue};
pub use normalizer::{InstrumentSpec, InstrumentSpecProvider, OrderNormalizer};
pub use order_state::OrderStateMachine;
pub use signing::{
    CancelActionWire, CancelByCloidActionWire, CancelByCloidWire, CancelWire,
    LeverageActionWire, OrderActionWire, OrderTypeWire, OrderWire, SignatureParts, TifWire,
    action_hash, pack_action, sign_l1_action, sign_order_action,
};
