//! Account tracking, reconciliation, and authenticated exchange ingestion,
//! port of `src/hypeedge/account/`.
//!
//! [`AccountTracker`] maintains live balances/positions/PnL from fills and
//! exchange polling; [`Reconciler`] corrects local state against exchange truth
//! (local → exchange wins); [`ExchangeEventIngestor`] owns the authenticated
//! WS subscriptions and REST gap recovery, converging through the
//! [`ExchangeFactProjector`] transactional boundary. The Postgres projector
//! lives in the `storage` crate; the trading crate stays DB-free.

pub mod account_health;
pub mod exchange_ingestor;
pub mod reconciler;
pub mod tracker;

pub use account_health::{
    AccountFreshnessThresholds, AccountHealthDimension, AccountHealthProvider,
    AccountHealthSnapshot, AccountSnapshotSink, AccountStatePoller, AccountStateSource,
    ClearinghouseRestClient, FreshnessObservation, FreshnessResult, FreshnessStatus,
    LayeredAccountHealthProvider, MutableAccountHealthProvider, PolledAccountSnapshot,
    RestAccountStateSource, RiskProximityEvaluator,
};
pub use exchange_ingestor::{
    CommittedFillProjection, ExchangeEventIngestor, ExchangeFactProjector, InfoClient,
    IngestResult, SOURCE, canonical_payload, fill_external_id, fill_position_after,
    funding_external_id, normalize_status, order_from_status_response, projected_entry_price,
    status_to_order_status, synthetic_cloid,
};
pub use reconciler::{ReconDiff, Reconciler, ReconcilerLogic, ReconciliationResult};
pub use tracker::AccountTracker;
