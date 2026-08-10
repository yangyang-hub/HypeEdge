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
pub async fn meta(State(state): State<AppState>, Path(symbol): Path<String>) -> Response {
    let Ok(symbol) = validated_symbol(&symbol) else {
        return ApiProblem::new(422, "INVALID_SYMBOL", "Symbol format is invalid").into_response();
    };
    if let Some(cache) = &state.instrument_meta
        && let Some(info) = cache.get(&symbol)
    {
        return ok(serde_json::json!({
            "symbol": symbol,
            "price_decimals": info.max_price_decimals,
            "size_decimals": info.sz_decimals,
            "tick_size": info.tick_size.to_string(),
            "lot_size": info.lot_size.to_string(),
            "min_order_size": info.min_size.to_string(),
            "max_leverage": info.max_leverage,
        }));
    }
    // Control-plane fallback so the dashboard can still render.
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
pub async fn funding(State(state): State<AppState>, Path(symbol): Path<String>) -> Response {
    let Ok(symbol) = validated_symbol(&symbol) else {
        return ApiProblem::new(422, "INVALID_SYMBOL", "Symbol format is invalid").into_response();
    };
    let Some(provider) = &state.market_data else {
        return ApiProblem::new(
            503,
            "MARKET_DATA_NOT_READY",
            "Funding snapshot has not been received",
        )
        .with_retryable(true)
        .into_response();
    };
    match provider.get_funding(&symbol).await {
        Some(f) => ok(serde_json::json!({
            "symbol": symbol,
            "funding_rate": f.funding_rate,
            "premium": f.premium,
            "open_interest": f.open_interest,
            "mark_price": f.mark_price.to_string(),
            "timestamp": f.timestamp,
        })),
        None => ApiProblem::new(
            503,
            "MARKET_DATA_NOT_READY",
            "Funding snapshot has not been received",
        )
        .with_retryable(true)
        .into_response(),
    }
}

/// `GET /api/v1/market/{symbol}/candles?interval=&limit=`.
pub async fn candles(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Ok(symbol) = validated_symbol(&symbol) else {
        return ApiProblem::new(422, "INVALID_SYMBOL", "Symbol format is invalid").into_response();
    };
    let interval = params.get("interval").map(String::as_str).unwrap_or("1m");
    let limit: usize = params
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(300)
        .min(1000);
    let Some(provider) = &state.market_data else {
        return ok(serde_json::json!([]));
    };
    let interval_ms = match interval_to_ms(interval) {
        Ok(ms) => ms,
        Err(_) => {
            return ApiProblem::new(
                422,
                "INVALID_INTERVAL",
                format!("Unsupported candle interval: {interval}"),
            )
            .into_response();
        }
    };
    let now_ms = chrono::Utc::now().timestamp_millis();
    let start_ms = now_ms - (limit as i64) * interval_ms;
    match provider
        .ensure_candles(&symbol, interval, limit, start_ms, now_ms)
        .await
    {
        Ok(candles) => ok(serde_json::json!(
            candles
                .into_iter()
                .map(|c| serde_json::json!({
                    "timestamp": c.timestamp,
                    "open": c.open.to_string(),
                    "high": c.high.to_string(),
                    "low": c.low.to_string(),
                    "close": c.close.to_string(),
                    "volume": c.volume.to_string(),
                }))
                .collect::<Vec<_>>()
        )),
        Err(e) => ApiProblem::new(
            502,
            "CANDLE_BACKFILL_FAILED",
            format!("candle backfill failed: {e}"),
        )
        .into_response(),
    }
}

fn interval_to_ms(interval: &str) -> Result<i64, ()> {
    let ms = match interval {
        "1m" => 60_000,
        "3m" => 3 * 60_000,
        "5m" => 5 * 60_000,
        "15m" => 15 * 60_000,
        "30m" => 30 * 60_000,
        "1h" => 60 * 60_000,
        "2h" => 2 * 60 * 60_000,
        "4h" => 4 * 60 * 60_000,
        "8h" => 8 * 60 * 60_000,
        "12h" => 12 * 60 * 60_000,
        "1d" => 24 * 60 * 60_000,
        "3d" => 3 * 24 * 60 * 60_000,
        "1w" => 7 * 24 * 60 * 60_000,
        "1M" => 30 * 24 * 60 * 60_000,
        _ => return Err(()),
    };
    Ok(ms)
}
