//! Market data routes, port of `src/hypeedge/api/routes/market.py`.
//!
//! The live snapshot (book/mid) comes from the in-memory [`BookManager`];
//! funding/candles require the live market-data provider + ClickHouse backfill,
//! which land with the app wiring (Phase 7), so they currently report
//! `MARKET_DATA_NOT_READY` when absent.

use axum::extract::{Path, State};
use axum::response::Response;

use crate::errors::{ApiProblem, ok};
use crate::state::AppState;
use axum::response::IntoResponse;

/// Validate a symbol string (mirrors `_validated_symbol`).
fn validated_symbol(symbol: &str) -> Result<String, ApiProblem> {
    let normalized = symbol.trim().to_uppercase();
    if normalized.is_empty()
        || normalized.len() > 20
        || !normalized
            .chars()
            .all(|c| c.is_alphanumeric() || "_.-".contains(c))
    {
        return Err(ApiProblem::new(
            422,
            "INVALID_SYMBOL",
            "Symbol format is invalid",
        ));
    }
    Ok(normalized)
}

/// `GET /api/v1/market/{symbol}/book` — current L2 book from the in-memory snapshot.
pub async fn book(State(state): State<AppState>, Path(symbol): Path<String>) -> Response {
    let Ok(symbol) = validated_symbol(&symbol) else {
        return ApiProblem::new(422, "INVALID_SYMBOL", "Symbol format is invalid").into_response();
    };
    let snapshot = state.books.lock().await.get_snapshot(&symbol);
    match snapshot {
        Some(s) => ok(serde_json::json!({
            "symbol": s.symbol,
            "bids": s.bids.iter().map(|l| serde_json::json!([l.price.to_string(), l.size.to_string()])).collect::<Vec<_>>(),
            "asks": s.asks.iter().map(|l| serde_json::json!([l.price.to_string(), l.size.to_string()])).collect::<Vec<_>>(),
            "timestamp": s.timestamp,
            "source": "websocket",
        })),
        None => ApiProblem::new(
            503,
            "MARKET_DATA_NOT_READY",
            "Order book snapshot has not been received",
        )
        .with_retryable(true)
        .into_response(),
    }
}

/// `GET /api/v1/market/{symbol}/meta` — instrument metadata from the cache.
pub async fn meta(State(_state): State<AppState>, Path(symbol): Path<String>) -> Response {
    let Ok(symbol) = validated_symbol(&symbol) else {
        return ApiProblem::new(422, "INVALID_SYMBOL", "Symbol format is invalid").into_response();
    };
    // Instrument cache lands with the app wiring; use a sane default contract
    // so the frontend can render. Once `InstrumentMetaCache` exists this reads
    // real tick/lot/precision.
    ok(serde_json::json!({
        "symbol": symbol,
        "price_decimals": 2,
        "size_decimals": 4,
        "tick_size": "0.1",
        "lot_size": "0.001",
        "min_order_size": "0.001",
        "max_leverage": 5,
    }))
}

/// `GET /api/v1/market/{symbol}/funding`.
pub async fn funding(State(_state): State<AppState>, Path(symbol): Path<String>) -> Response {
    let Ok(_symbol) = validated_symbol(&symbol) else {
        return ApiProblem::new(422, "INVALID_SYMBOL", "Symbol format is invalid").into_response();
    };
    ApiProblem::new(
        503,
        "MARKET_DATA_NOT_READY",
        "Funding snapshot has not been received",
    )
    .with_retryable(true)
    .into_response()
}

/// `GET /api/v1/market/{symbol}/candles?interval=&limit=`.
pub async fn candles(
    State(_state): State<AppState>,
    Path(symbol): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Ok(_symbol) = validated_symbol(&symbol) else {
        return ApiProblem::new(422, "INVALID_SYMBOL", "Symbol format is invalid").into_response();
    };
    let _interval = params.get("interval").map(String::as_str).unwrap_or("1m");
    let _limit: usize = params
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(300)
        .min(1000);
    // Candles need the ClickHouse backfill layer; until it lands, return an
    // empty list so the frontend degrades gracefully.
    ok(serde_json::json!([]))
}
