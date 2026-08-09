//! Trading domain: market data, execution, risk, account, and strategy.
//!
//! This crate depends only on the `domain` traits and the `infra` event bus —
//! never on a concrete storage implementation — so the hot paths are
//! unit-testable against in-memory fakes. The `app` crate wires concrete
//! Postgres/ClickHouse stores behind the domain traits.

pub mod account;
pub mod backtest;
pub mod execution;
pub mod funding_arb;
pub mod market_data;
pub mod market_maker;
pub mod risk;
pub mod strategy;
pub mod trading;
