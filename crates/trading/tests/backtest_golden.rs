//! Golden parity test: the same 10-candle sequence driven through the Rust
//! backtest broker/client must produce the identical fills the pinned Python
//! `BacktestEngine` produces (optimistic slippage, market orders).

use std::sync::Arc;

use chrono::Utc;
use hypeedge_domain::decimal::{Decimal, Price, Size};
use hypeedge_domain::enums::{OrderType, Side, TimeInForce};
use hypeedge_domain::models::{Candle, OrderIntent};
use hypeedge_domain::traits::ExecutionClient;
use hypeedge_infra::event_bus::EventBus;
use hypeedge_trading::backtest::broker::{
    FeeConfig, SimulatedBroker, SlippageConfig, SlippageMode,
};
use hypeedge_trading::backtest::engine::SimulatedExecutionClient;

fn candles() -> Vec<Candle> {
    (0..10)
        .map(|i| {
            let px = 100 + i;
            Candle {
                symbol: "BTC".into(),
                interval: "1h".into(),
                open: Price::new(Decimal::from_f64(px as f64 - 0.5).unwrap()),
                high: Price::new(Decimal::from_f64(px as f64 + 1.0).unwrap()),
                low: Price::new(Decimal::from_f64(px as f64 - 1.0).unwrap()),
                close: Price::new(Decimal::from_f64(px as f64 + 0.5).unwrap()),
                volume: Size::new(Decimal::from_str_lenient("10").unwrap()),
                timestamp: 1_000_000 + i * 3_600_000,
            }
        })
        .collect()
}

fn intent(side: Side, size: &str, cloid: &str) -> OrderIntent {
    OrderIntent {
        symbol: "BTC".into(),
        side,
        size: Size::new(Decimal::from_str_lenient(size).unwrap()),
        price: None,
        order_type: OrderType::Market,
        time_in_force: TimeInForce::Gtc,
        strategy_id: Some("bt".into()),
        sub_account: None,
        reduce_only: false,
        cloid: Some(cloid.into()),
        client_id: None,
        is_spot: false,
        risk_reducing: false,
        max_slippage_bps: 50,
    }
}

#[tokio::test]
async fn backtest_fills_match_python_golden() {
    let bus = Arc::new(EventBus::new(10_000));
    let broker = Arc::new(std::sync::Mutex::new(SimulatedBroker::new(
        FeeConfig::default(),
        SlippageConfig::default(),
        SlippageMode::Optimistic,
    )));
    let client = SimulatedExecutionClient::new(broker, bus);
    let candles = candles();
    let mut fills = Vec::new();

    // Buy at candle 2 (close 102.5, optimistic buy slippage +2bps → 102.5205).
    // Process candles 0,1 first, then submit the buy so it fills on candle 2.
    for c in &candles[0..2] {
        client.try_fill_orders(c, &mut fills).await;
    }
    client
        .submit_order(intent(Side::Buy, "1", "bt_1"), None)
        .await
        .unwrap();
    client.try_fill_orders(&candles[2], &mut fills).await;
    // Sell at candle 7 (close 107.5, optimistic sell slippage −2bps → 107.4785).
    for c in &candles[3..7] {
        client.try_fill_orders(c, &mut fills).await;
    }
    client
        .submit_order(intent(Side::Sell, "1", "bt_2"), None)
        .await
        .unwrap();
    client.try_fill_orders(&candles[7], &mut fills).await;

    assert_eq!(fills.len(), 2, "both market orders fill");
    let buy = &fills[0];
    let sell = &fills[1];

    // Python: buy price 102.52050, sell 107.47850, taker fees 0.05126/0.05374.
    assert_eq!(buy.side, Side::Buy);
    assert_eq!(buy.price.to_string(), "102.5205", "buy fill price");
    assert!(!buy.is_maker);
    assert_eq!(buy.fee.to_string(), "0.05126025", "buy taker fee");
    assert_eq!(buy.timestamp, 8_200_000, "buy candle timestamp");

    assert_eq!(sell.side, Side::Sell);
    assert_eq!(sell.price.to_string(), "107.4785", "sell fill price");
    assert_eq!(sell.fee.to_string(), "0.05373925", "sell taker fee");
    assert_eq!(sell.timestamp, 26_200_000, "sell candle timestamp");

    let _ = Utc::now();
}
