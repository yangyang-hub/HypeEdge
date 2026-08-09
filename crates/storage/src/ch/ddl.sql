-- ClickHouse schema, transcribed verbatim from `DDL_STATEMENTS` in
-- `src/hypeedge/storage/clickhouse.py`. Applied at startup by the writer
-- (idempotent `CREATE TABLE IF NOT EXISTS`).
--
-- The five core market-data tables (Phase 1 mandatory) are written by the
-- Rust writer now. The five `mm_*` analytics tables are created here too so
-- the schema matches Python, but their rows are produced once the
-- market-making runtime lands (Phase 5).

CREATE TABLE IF NOT EXISTS l2_book (
    ts          DateTime64(3),
    coin        LowCardinality(String),
    side        Enum8('bid' = 1, 'ask' = 2),
    level       UInt16,
    px          Float64,
    sz          Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ts)
ORDER BY (coin, ts, side, level)
TTL ts + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS trades (
    ts          DateTime64(3),
    coin        LowCardinality(String),
    px          Float64,
    sz          Float64,
    side        Enum8('buy' = 1, 'sell' = 2),
    tid         UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ts)
ORDER BY (coin, ts)
TTL ts + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS candles (
    ts          DateTime64(3),
    coin        LowCardinality(String),
    interval    LowCardinality(String),
    open        Float64,
    high        Float64,
    low         Float64,
    close       Float64,
    volume      Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (coin, interval, ts)
TTL ts + INTERVAL 730 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS funding (
    ts              DateTime64(3),
    coin            LowCardinality(String),
    funding_rate    Float64,
    premium         Float64,
    oi              Float64,
    mark_px         Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (coin, ts)
TTL ts + INTERVAL 730 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS mid_prices (
    ts          DateTime64(3),
    coin        LowCardinality(String),
    px          Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ts)
ORDER BY (coin, ts)
TTL ts + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS mm_feature_samples (
    ts                          DateTime64(6, 'UTC'),
    strategy_id                 LowCardinality(String),
    symbol                      LowCardinality(String),
    session_id                  String,
    config_version              UInt64,
    model_version               LowCardinality(String),
    market_version              UInt64,
    exchange_ts                 DateTime64(3, 'UTC'),
    received_at                 DateTime64(6, 'UTC'),
    mid_px                      Decimal(38, 18),
    microprice                  Decimal(38, 18),
    fair_px                     Decimal(38, 18),
    best_bid_px                 Decimal(38, 18),
    best_ask_px                 Decimal(38, 18),
    normalized_ofi_l1           Float64,
    normalized_ofi_l5           Float64,
    trade_flow                  Float64,
    short_return                Float64,
    volatility_1s               Float64,
    volatility_5s               Float64,
    volatility_30s              Float64,
    volatility_5m               Float64,
    toxicity                    Float64,
    receipt_to_decision_us      UInt32,
    event_loop_lag_us           UInt32
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ts)
ORDER BY (strategy_id, symbol, ts)
TTL ts + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS mm_quote_decisions (
    ts                              DateTime64(6, 'UTC'),
    strategy_id                     LowCardinality(String),
    symbol                          LowCardinality(String),
    session_id                      String,
    config_version                  UInt64,
    model_version                   LowCardinality(String),
    quote_revision                  UInt64,
    market_version                  UInt64,
    decision                        LowCardinality(String),
    reason                          LowCardinality(String),
    fair_px                         Decimal(38, 18),
    reservation_px                  Decimal(38, 18),
    desired_bid_px                  Nullable(Decimal(38, 18)),
    desired_bid_size                Nullable(Decimal(38, 18)),
    desired_ask_px                  Nullable(Decimal(38, 18)),
    desired_ask_size                Nullable(Decimal(38, 18)),
    live_bid_px                     Nullable(Decimal(38, 18)),
    live_bid_size                   Nullable(Decimal(38, 18)),
    live_ask_px                     Nullable(Decimal(38, 18)),
    live_ask_size                   Nullable(Decimal(38, 18)),
    position_size                   Decimal(38, 18),
    inventory_notional_usdc         Decimal(38, 18),
    budget_mode                     LowCardinality(String),
    expected_gross_edge_usdc        Decimal(38, 18),
    adverse_selection_cost_usdc     Decimal(38, 18),
    inventory_cost_usdc             Decimal(38, 18),
    funding_cost_usdc               Decimal(38, 18),
    action_cost_usdc                Decimal(38, 18),
    failure_cost_usdc               Decimal(38, 18),
    expected_net_pnl_usdc           Decimal(38, 18)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ts)
ORDER BY (strategy_id, symbol, ts)
TTL ts + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS mm_inventory_samples (
    ts                              DateTime64(6, 'UTC'),
    strategy_id                     LowCardinality(String),
    symbol                          LowCardinality(String),
    session_id                      String,
    position_size                   Decimal(38, 18),
    mark_px                         Decimal(38, 18),
    inventory_notional_usdc         Decimal(38, 18),
    soft_limit_utilization          Float64,
    hard_limit_utilization          Float64,
    emergency_limit_utilization     Float64,
    equity_usdc                     Decimal(38, 18),
    available_balance_usdc          Decimal(38, 18),
    margin_used_usdc                Decimal(38, 18),
    liquidation_distance_bps        Nullable(Float64),
    funding_carry_usdc              Decimal(38, 18),
    reduce_only                     Bool,
    healthy                         Bool
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ts)
ORDER BY (strategy_id, symbol, ts)
TTL ts + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS mm_action_credit_samples (
    ts                          DateTime64(6, 'UTC'),
    strategy_id                 LowCardinality(String),
    symbol                      LowCardinality(String),
    quota_owner                 LowCardinality(String),
    remote_remaining            Int64,
    shadow_remaining            Int64,
    cancel_headroom             Int64,
    ip_weight_remaining         Int64,
    actions_burned_1h           UInt64,
    actions_earned_1h           UInt64,
    actions_burned_24h          UInt64,
    actions_earned_24h          UInt64,
    fills_1h                    UInt64,
    usdc_volume_1h              Decimal(38, 18),
    usdc_per_action_1h          Float64,
    usdc_per_action_24h         Float64,
    runway_hours                Nullable(Float64),
    soft_allocation             UInt64,
    hard_allocation             UInt64,
    emergency_reserve           UInt64,
    mode                        LowCardinality(String),
    remote_observed_at          DateTime64(6, 'UTC'),
    window_end                  DateTime64(6, 'UTC'),
    calculation_version         LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ts)
ORDER BY (strategy_id, symbol, ts)
TTL ts + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS mm_fill_markouts (
    ts                          DateTime64(6, 'UTC'),
    strategy_id                 LowCardinality(String),
    symbol                      LowCardinality(String),
    session_id                  String,
    fill_id                     String,
    order_id                    String,
    cloid                       String,
    fill_ts                     DateTime64(6, 'UTC'),
    side                        Enum8('buy' = 1, 'sell' = 2),
    fill_px                     Decimal(38, 18),
    fill_size                   Decimal(38, 18),
    reference                   LowCardinality(String),
    reference_px                Decimal(38, 18),
    horizon_ms                  UInt32,
    horizon_ts                  DateTime64(6, 'UTC'),
    mark_px                     Decimal(38, 18),
    signed_markout_bps          Float64,
    signed_markout_usdc         Decimal(38, 18),
    spread_capture_usdc         Decimal(38, 18),
    maker                       Bool,
    queue_ahead_size            Nullable(Decimal(38, 18)),
    fill_probability            Nullable(Float64),
    calculation_version         LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_id, symbol, ts)
TTL ts + INTERVAL 730 DAY
SETTINGS index_granularity = 8192;
