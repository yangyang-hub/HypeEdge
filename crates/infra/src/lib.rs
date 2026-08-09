//! Infrastructure layer: in-process event bus, shared runtime utilities.
//!
//! [`event_bus`] is the port of `src/hypeedge/core/events.py`'s `EventBus`
//! with the lossy/reliable delivery split. Future modules here: tracing setup,
//! the Prometheus registry, and the Hyperliquid rate limiter.

pub mod event_bus;

pub use event_bus::{BoundedMailbox, EventBus, EventBusBackpressureError, wrap};
