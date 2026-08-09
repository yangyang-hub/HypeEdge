//! HypeEdge domain layer.
//!
//! Pure types with no async/IO: the fixed-point [`decimal`] core, [`enums`]
//! and their state machines, the typed [`events`], the domain [`models`], the
//! [`error`] hierarchy, and the durable-boundary [`traits`]. This crate has no
//! knowledge of Postgres, ClickHouse, the exchange SDK, or tokio — only
//! `serde`, `chrono`, `uuid`, and `primitive-types`.
//!
//! Mirrors `src/hypeedge/core/` and the analytics payloads in
//! `src/hypeedge/storage/mm_analytics.py`.

pub mod decimal;
pub mod enums;
pub mod error;
pub mod events;
pub mod models;
pub mod traits;

pub use decimal::{Decimal, Price, Size, Usd};
pub use enums::*;
pub use error::HypeEdgeError;
pub use events::{DomainEvent, Event, EventType};
pub use models::*;
