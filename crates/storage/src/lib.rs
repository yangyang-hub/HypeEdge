//! Storage layer: Postgres transactional stores + ClickHouse writer.
//!
//! [`pg`] manages the pool and migrations; [`durable_order_store`] is the
//! transactional placement boundary with the DB-enforced risk admission;
//! [`command_queue`] is the lease queue for the signed-action executor;
//! [`outbox`] is the transactional outbox feeding SSE; [`system_state_store`]
//! is the durable safety latch. [`decimal_sqlx`] integrates the domain Decimal
//! with `NUMERIC(38,18)`.
//!
//! The ClickHouse writer and spool are added in a later Phase-1 increment.

pub mod checkpoint;
pub mod clickhouse_writer;
pub mod command_queue;
pub mod config_version_pg;
pub mod config_version_store;
pub mod decimal_sqlx;
pub mod dedup;
pub mod duckdb_export;
pub mod durable_order_store;
pub mod exchange_ingestor_store;
pub mod outbox;
pub mod pg;
pub mod quote_plan_store;
pub mod rows;
pub mod system_state_store;

pub use checkpoint::BackfillCheckpointStore;
pub use config_version_pg::PostgresConfigVersionStore;
pub use config_version_store::{ConfigVersionRecord, ConfigVersionStore, config_hash};
pub use dedup::DedupFilter;
pub use duckdb_export::{export_all, export_table};
pub use exchange_ingestor_store::PostgresExchangeFactProjector;
pub use pg::{Postgres, Postgres as PostgresPool};
pub use quote_plan_store::{PostgresQuotePlanStore, QuotePlanChildRow};
