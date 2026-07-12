"""ClickHouse async writer for market data (Phase 1 implementation).

Consumes events from the EventBus and batches writes to ClickHouse.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from typing import Any

import structlog

from hypeedge.config.settings import ClickHouseSettings
from hypeedge.core.events import (
    EVENT_CANDLE_UPDATE,
    EVENT_FUNDING_UPDATE,
    EVENT_L2_BOOK_UPDATE,
    EVENT_MID_PRICE_UPDATE,
    EVENT_MM_ACTION_CREDIT_SAMPLE,
    EVENT_MM_FEATURE_SAMPLE,
    EVENT_MM_FILL_MARKOUT,
    EVENT_MM_INVENTORY_SAMPLE,
    EVENT_MM_QUOTE_DECISION,
    EVENT_TRADE_UPDATE,
    Event,
    EventBus,
)
from hypeedge.core.models import Candle, FundingRate, L2BookSnapshot, Trade
from hypeedge.storage.clickhouse_spool import ClickHouseSpool
from hypeedge.storage.dedup import DedupFilter
from hypeedge.storage.mm_analytics import (
    MarketMakerActionCreditSample,
    MarketMakerFeatureSample,
    MarketMakerFillMarkout,
    MarketMakerInventorySample,
    MarketMakerQuoteDecision,
)

logger = structlog.get_logger(__name__)

# ClickHouse DDL for all tables
DDL_STATEMENTS = [
    """
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
    SETTINGS index_granularity = 8192
    """,
    """
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
    SETTINGS index_granularity = 8192
    """,
    """
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
    SETTINGS index_granularity = 8192
    """,
    """
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
    SETTINGS index_granularity = 8192
    """,
    """
    CREATE TABLE IF NOT EXISTS mid_prices (
        ts          DateTime64(3),
        coin        LowCardinality(String),
        px          Float64
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMMDD(ts)
    ORDER BY (coin, ts)
    TTL ts + INTERVAL 90 DAY
    SETTINGS index_granularity = 8192
    """,
    """
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
    SETTINGS index_granularity = 8192
    """,
    """
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
    SETTINGS index_granularity = 8192
    """,
    """
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
    SETTINGS index_granularity = 8192
    """,
    """
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
    SETTINGS index_granularity = 8192
    """,
    """
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
    SETTINGS index_granularity = 8192
    """,
]


class ClickHouseWriter:
    """Async ClickHouse writer that batches market data events.

    Subscribes to EventBus and accumulates rows in memory.
    Flushes to ClickHouse when batch_size is reached or flush_interval elapses.
    """

    def __init__(
        self,
        settings: ClickHouseSettings,
        event_bus: EventBus,
        dedup_filter: DedupFilter | None = None,
    ) -> None:
        self._settings = settings
        self._event_bus = event_bus
        self._dedup = dedup_filter
        self._client: Any = None  # clickhouse-connect client
        self._running = False

        # Buffers per table
        self._book_rows: list[dict[str, Any]] = []
        self._trade_rows: list[dict[str, Any]] = []
        self._candle_rows: list[dict[str, Any]] = []
        self._funding_rows: list[dict[str, Any]] = []
        self._mid_price_rows: list[dict[str, Any]] = []
        self._mm_feature_rows: list[dict[str, Any]] = []
        self._mm_quote_decision_rows: list[dict[str, Any]] = []
        self._mm_inventory_rows: list[dict[str, Any]] = []
        self._mm_action_credit_rows: list[dict[str, Any]] = []
        self._mm_fill_markout_rows: list[dict[str, Any]] = []
        self._flush_lock = asyncio.Lock()
        self._spool = ClickHouseSpool(settings.spool_path)

        self._total_written = 0

    async def run(self) -> None:
        """Main loop: subscribe to events, batch and flush."""
        self._running = True

        # Connect and create tables
        await self._connect()
        await self._create_tables()
        await self._spool.initialize()
        await self._replay_spool()

        # Subscribe to market data events
        book_queue = self._event_bus.subscribe(EVENT_L2_BOOK_UPDATE)
        trade_queue = self._event_bus.subscribe(EVENT_TRADE_UPDATE)
        candle_queue = self._event_bus.subscribe(EVENT_CANDLE_UPDATE)
        funding_queue = self._event_bus.subscribe(EVENT_FUNDING_UPDATE)
        mid_price_queue = self._event_bus.subscribe(EVENT_MID_PRICE_UPDATE)
        mm_feature_queue = self._event_bus.subscribe(EVENT_MM_FEATURE_SAMPLE)
        mm_quote_decision_queue = self._event_bus.subscribe(EVENT_MM_QUOTE_DECISION)
        mm_inventory_queue = self._event_bus.subscribe(EVENT_MM_INVENTORY_SAMPLE)
        mm_action_credit_queue = self._event_bus.subscribe(EVENT_MM_ACTION_CREDIT_SAMPLE)
        mm_fill_markout_queue = self._event_bus.subscribe(EVENT_MM_FILL_MARKOUT)

        logger.info("ch_writer_started", database=self._settings.database)

        flush_task = asyncio.create_task(self._flush_loop())
        queue_tasks: dict[asyncio.Task[Event], asyncio.Queue[Event]] = {}
        for queue in (
            book_queue,
            trade_queue,
            candle_queue,
            funding_queue,
            mid_price_queue,
            mm_feature_queue,
            mm_quote_decision_queue,
            mm_inventory_queue,
            mm_action_credit_queue,
            mm_fill_markout_queue,
        ):
            queue_tasks[asyncio.create_task(queue.get())] = queue

        try:
            while self._running:
                done, _ = await asyncio.wait(
                    list(queue_tasks),
                    timeout=1.0,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in done:
                    try:
                        queue = queue_tasks.pop(task)
                        event: Event = task.result()
                        self._buffer_event(event)
                        queue_tasks[asyncio.create_task(queue.get())] = queue
                    except Exception:
                        logger.exception("ch_writer_event_error")

                # Check if any buffer is full
                if len(self._book_rows) >= self._settings.batch_size:
                    await self._flush_buffer("_book_rows", "l2_book")
                if len(self._trade_rows) >= self._settings.batch_size:
                    await self._flush_buffer("_trade_rows", "trades")
                if len(self._candle_rows) >= self._settings.batch_size:
                    await self._flush_buffer("_candle_rows", "candles")
                if len(self._funding_rows) >= self._settings.batch_size:
                    await self._flush_buffer("_funding_rows", "funding")
                if len(self._mid_price_rows) >= self._settings.batch_size:
                    await self._flush_buffer("_mid_price_rows", "mid_prices")
                if len(self._mm_feature_rows) >= self._settings.batch_size:
                    await self._flush_buffer("_mm_feature_rows", "mm_feature_samples")
                if len(self._mm_quote_decision_rows) >= self._settings.batch_size:
                    await self._flush_buffer("_mm_quote_decision_rows", "mm_quote_decisions")
                if len(self._mm_inventory_rows) >= self._settings.batch_size:
                    await self._flush_buffer("_mm_inventory_rows", "mm_inventory_samples")
                if len(self._mm_action_credit_rows) >= self._settings.batch_size:
                    await self._flush_buffer("_mm_action_credit_rows", "mm_action_credit_samples")
                if len(self._mm_fill_markout_rows) >= self._settings.batch_size:
                    await self._flush_buffer("_mm_fill_markout_rows", "mm_fill_markouts")

        except asyncio.CancelledError:
            logger.debug("ch_writer_cancelled")
        finally:
            flush_task.cancel()
            for task in queue_tasks:
                task.cancel()
            await asyncio.gather(*queue_tasks, return_exceptions=True)
            logger.info("ch_writer_stopped", total_written=self._total_written)

    async def _connect(self) -> None:
        """Establish ClickHouse connection."""
        try:
            import clickhouse_connect

            self._client = clickhouse_connect.get_client(
                host=self._settings.host,
                port=self._settings.port,
                username=self._settings.username,
                password=self._settings.password,
                database=self._settings.database,
            )
            # Verify connection
            self._client.command("SELECT 1")
            logger.info("ch_connected", host=self._settings.host, database=self._settings.database)
        except Exception as e:
            logger.error("ch_connection_failed", error=str(e))
            raise

    async def _create_tables(self) -> None:
        """Create tables if they don't exist."""
        if not self._client:
            return
        for ddl in DDL_STATEMENTS:
            try:
                self._client.command(ddl)
            except Exception as e:
                logger.error("ch_create_table_error", error=str(e))
        logger.info("ch_tables_ensured")

    async def _flush_loop(self) -> None:
        """Periodic flush of all buffers."""
        try:
            while self._running:
                await asyncio.sleep(self._settings.flush_interval)
                await self.flush()
        except asyncio.CancelledError:
            logger.debug("ch_flush_loop_cancelled")

    async def flush(self) -> None:
        """Flush all buffers to ClickHouse."""
        async with self._flush_lock:
            await self._replay_spool_locked()
            await self._flush_buffer_locked("_book_rows", "l2_book")
            await self._flush_buffer_locked("_trade_rows", "trades")
            await self._flush_buffer_locked("_candle_rows", "candles")
            await self._flush_buffer_locked("_funding_rows", "funding")
            await self._flush_buffer_locked("_mid_price_rows", "mid_prices")
            await self._flush_buffer_locked("_mm_feature_rows", "mm_feature_samples")
            await self._flush_buffer_locked("_mm_quote_decision_rows", "mm_quote_decisions")
            await self._flush_buffer_locked("_mm_inventory_rows", "mm_inventory_samples")
            await self._flush_buffer_locked("_mm_action_credit_rows", "mm_action_credit_samples")
            await self._flush_buffer_locked("_mm_fill_markout_rows", "mm_fill_markouts")

    async def _flush_buffer(self, attribute: str, table: str) -> None:
        """Serialize flushes and detach a batch before awaiting blocking I/O."""
        async with self._flush_lock:
            await self._flush_buffer_locked(attribute, table)

    async def _flush_buffer_locked(self, attribute: str, table: str) -> None:
        rows = getattr(self, attribute)
        if not rows:
            return

        # Detach first. Events arriving while the insert is in progress are appended
        # to the new list and therefore cannot be cleared by the completed flush.
        setattr(self, attribute, [])
        succeeded = await self._flush_table(table, rows)
        if not succeeded:
            try:
                batch_id = await self._spool.put(table, rows)
                logger.warning("ch_batch_spooled", table=table, rows=len(rows), batch_id=batch_id)
            except Exception:
                logger.exception("ch_spool_write_failed", table=table, rows=len(rows))
                current_rows = getattr(self, attribute)
                setattr(self, attribute, rows + current_rows)

    async def _replay_spool(self) -> None:
        async with self._flush_lock:
            await self._replay_spool_locked()

    async def _replay_spool_locked(self) -> None:
        for batch_id, table, rows in await self._spool.pending():
            if not await self._flush_table(table, rows):
                return
            await self._spool.acknowledge(batch_id)
            logger.info("ch_spool_batch_replayed", table=table, rows=len(rows), batch_id=batch_id)

    async def _flush_table(self, table: str, rows: list[dict[str, Any]]) -> bool:
        """Flush a batch of rows to a ClickHouse table."""
        if not self._client or not rows:
            return False

        try:
            # Run in executor to avoid blocking the event loop
            loop = asyncio.get_running_loop()
            column_names, data = self._rows_to_column_data(rows)
            await loop.run_in_executor(
                None,
                lambda: self._client.insert(
                    table,
                    data,
                    column_names=column_names,
                ),
            )
            self._total_written += len(rows)
            logger.debug("ch_flush", table=table, rows=len(rows), total=self._total_written)
            return True
        except Exception as e:
            logger.error("ch_flush_error", table=table, error=str(e), rows=len(rows))
            return False

    def _rows_to_column_data(self, rows: list[dict[str, Any]]) -> tuple[list[str], Sequence[Sequence[Any]]]:
        """Convert row dictionaries to ClickHouse column names and row sequences."""
        column_names = list(rows[0].keys())
        data = [[row[column] for column in column_names] for row in rows]
        return column_names, data

    def _buffer_event(self, event: Event) -> None:
        """Buffer an event for batch writing."""
        payload = event.payload

        if event.event_type == EVENT_L2_BOOK_UPDATE:
            self._buffer_book(payload)
        elif event.event_type == EVENT_TRADE_UPDATE:
            self._buffer_trade(payload)
        elif event.event_type == EVENT_CANDLE_UPDATE:
            self._buffer_candle(payload)
        elif event.event_type == EVENT_FUNDING_UPDATE:
            self._buffer_funding(payload)
        elif event.event_type == EVENT_MID_PRICE_UPDATE:
            self._buffer_mid_price(payload)
        elif event.event_type == EVENT_MM_FEATURE_SAMPLE:
            self._buffer_mm_feature(payload)
        elif event.event_type == EVENT_MM_QUOTE_DECISION:
            self._buffer_mm_quote_decision(payload)
        elif event.event_type == EVENT_MM_INVENTORY_SAMPLE:
            self._buffer_mm_inventory(payload)
        elif event.event_type == EVENT_MM_ACTION_CREDIT_SAMPLE:
            self._buffer_mm_action_credit(payload)
        elif event.event_type == EVENT_MM_FILL_MARKOUT:
            self._buffer_mm_fill_markout(payload)

    def _buffer_book(self, snapshot: L2BookSnapshot) -> None:
        ts_ms = snapshot.timestamp
        ts_sec = ts_ms / 1000.0  # ClickHouse DateTime64(3) expects seconds with ms precision

        for level_idx, bid in enumerate(snapshot.bids):
            if self._dedup and self._dedup.check_and_mark("l2_book", f"{snapshot.symbol}:{ts_ms}:bid:{level_idx}"):
                continue
            self._book_rows.append(
                {
                    "ts": ts_sec,
                    "coin": str(snapshot.symbol),
                    "side": "bid",
                    "level": level_idx,
                    "px": bid.price,
                    "sz": bid.size,
                }
            )
        for level_idx, ask in enumerate(snapshot.asks):
            if self._dedup and self._dedup.check_and_mark("l2_book", f"{snapshot.symbol}:{ts_ms}:ask:{level_idx}"):
                continue
            self._book_rows.append(
                {
                    "ts": ts_sec,
                    "coin": str(snapshot.symbol),
                    "side": "ask",
                    "level": level_idx,
                    "px": ask.price,
                    "sz": ask.size,
                }
            )

    def _buffer_trade(self, trade: Trade) -> None:
        if self._dedup and self._dedup.check_and_mark("trades", f"{trade.symbol}:{trade.tid}"):
            return
        self._trade_rows.append(
            {
                "ts": trade.timestamp / 1000.0,
                "coin": str(trade.symbol),
                "px": trade.price,
                "sz": trade.size,
                "side": str(trade.side.value),
                "tid": trade.tid,
            }
        )

    def _buffer_candle(self, candle: Candle) -> None:
        dedup_key = f"{candle.symbol}:{candle.interval}:{candle.timestamp}"
        if self._dedup and self._dedup.check_and_mark("candles", dedup_key):
            return
        self._candle_rows.append(
            {
                "ts": candle.timestamp / 1000.0,
                "coin": str(candle.symbol),
                "interval": candle.interval,
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
            }
        )

    def _buffer_funding(self, funding: FundingRate) -> None:
        if self._dedup and self._dedup.check_and_mark("funding", f"{funding.symbol}:{funding.timestamp}"):
            return
        self._funding_rows.append(
            {
                "ts": funding.timestamp / 1000.0,
                "coin": str(funding.symbol),
                "funding_rate": funding.funding_rate,
                "premium": funding.premium,
                "oi": funding.open_interest,
                "mark_px": funding.mark_price,
            }
        )

    def _buffer_mid_price(self, payload: dict[str, Any]) -> None:
        """Buffer a mid-price update from allMids channel."""
        now_ms = int(time.time() * 1000)
        symbol = payload.get("symbol")
        price = payload.get("price")
        if symbol is None or price is None:
            return
        if self._dedup and self._dedup.check_and_mark("mid_prices", f"{symbol}:{now_ms}"):
            return
        self._mid_price_rows.append(
            {
                "ts": now_ms / 1000.0,
                "coin": str(symbol),
                "px": float(price),
            }
        )

    def _buffer_mm_feature(self, sample: MarketMakerFeatureSample) -> None:
        self._mm_feature_rows.append(
            {
                "ts": sample.ts,
                "strategy_id": str(sample.strategy_id),
                "symbol": str(sample.symbol),
                "session_id": sample.session_id,
                "config_version": sample.config_version,
                "model_version": sample.model_version,
                "market_version": sample.market_version,
                "exchange_ts": sample.exchange_ts,
                "received_at": sample.received_at,
                "mid_px": sample.mid_px,
                "microprice": sample.microprice,
                "fair_px": sample.fair_px,
                "best_bid_px": sample.best_bid_px,
                "best_ask_px": sample.best_ask_px,
                "normalized_ofi_l1": float(sample.normalized_ofi_l1),
                "normalized_ofi_l5": float(sample.normalized_ofi_l5),
                "trade_flow": float(sample.trade_flow),
                "short_return": float(sample.short_return),
                "volatility_1s": float(sample.volatility_1s),
                "volatility_5s": float(sample.volatility_5s),
                "volatility_30s": float(sample.volatility_30s),
                "volatility_5m": float(sample.volatility_5m),
                "toxicity": float(sample.toxicity),
                "receipt_to_decision_us": sample.receipt_to_decision_us,
                "event_loop_lag_us": sample.event_loop_lag_us,
            }
        )

    def _buffer_mm_quote_decision(self, decision: MarketMakerQuoteDecision) -> None:
        self._mm_quote_decision_rows.append(
            {
                "ts": decision.ts,
                "strategy_id": str(decision.strategy_id),
                "symbol": str(decision.symbol),
                "session_id": decision.session_id,
                "config_version": decision.config_version,
                "model_version": decision.model_version,
                "quote_revision": decision.quote_revision,
                "market_version": decision.market_version,
                "decision": decision.decision,
                "reason": decision.reason,
                "fair_px": decision.fair_px,
                "reservation_px": decision.reservation_px,
                "desired_bid_px": decision.desired_bid_px,
                "desired_bid_size": decision.desired_bid_size,
                "desired_ask_px": decision.desired_ask_px,
                "desired_ask_size": decision.desired_ask_size,
                "live_bid_px": decision.live_bid_px,
                "live_bid_size": decision.live_bid_size,
                "live_ask_px": decision.live_ask_px,
                "live_ask_size": decision.live_ask_size,
                "position_size": decision.position_size,
                "inventory_notional_usdc": decision.inventory_notional_usdc,
                "budget_mode": decision.budget_mode.value,
                "expected_gross_edge_usdc": decision.expected_gross_edge_usdc,
                "adverse_selection_cost_usdc": decision.adverse_selection_cost_usdc,
                "inventory_cost_usdc": decision.inventory_cost_usdc,
                "funding_cost_usdc": decision.funding_cost_usdc,
                "action_cost_usdc": decision.action_cost_usdc,
                "failure_cost_usdc": decision.failure_cost_usdc,
                "expected_net_pnl_usdc": decision.expected_net_pnl_usdc,
            }
        )

    def _buffer_mm_inventory(self, sample: MarketMakerInventorySample) -> None:
        self._mm_inventory_rows.append(
            {
                "ts": sample.ts,
                "strategy_id": str(sample.strategy_id),
                "symbol": str(sample.symbol),
                "session_id": sample.session_id,
                "position_size": sample.position_size,
                "mark_px": sample.mark_px,
                "inventory_notional_usdc": sample.inventory_notional_usdc,
                "soft_limit_utilization": float(sample.soft_limit_utilization),
                "hard_limit_utilization": float(sample.hard_limit_utilization),
                "emergency_limit_utilization": float(sample.emergency_limit_utilization),
                "equity_usdc": sample.equity_usdc,
                "available_balance_usdc": sample.available_balance_usdc,
                "margin_used_usdc": sample.margin_used_usdc,
                "liquidation_distance_bps": (
                    None if sample.liquidation_distance_bps is None else float(sample.liquidation_distance_bps)
                ),
                "funding_carry_usdc": sample.funding_carry_usdc,
                "reduce_only": sample.reduce_only,
                "healthy": sample.healthy,
            }
        )

    def _buffer_mm_action_credit(self, sample: MarketMakerActionCreditSample) -> None:
        self._mm_action_credit_rows.append(
            {
                "ts": sample.ts,
                "strategy_id": str(sample.strategy_id),
                "symbol": str(sample.symbol),
                "quota_owner": sample.quota_owner,
                "remote_remaining": sample.remote_remaining,
                "shadow_remaining": sample.shadow_remaining,
                "cancel_headroom": sample.cancel_headroom,
                "ip_weight_remaining": sample.ip_weight_remaining,
                "actions_burned_1h": sample.actions_burned_1h,
                "actions_earned_1h": sample.actions_earned_1h,
                "actions_burned_24h": sample.actions_burned_24h,
                "actions_earned_24h": sample.actions_earned_24h,
                "fills_1h": sample.fills_1h,
                "usdc_volume_1h": sample.usdc_volume_1h,
                "usdc_per_action_1h": float(sample.usdc_per_action_1h),
                "usdc_per_action_24h": float(sample.usdc_per_action_24h),
                "runway_hours": None if sample.runway_hours is None else float(sample.runway_hours),
                "soft_allocation": sample.soft_allocation,
                "hard_allocation": sample.hard_allocation,
                "emergency_reserve": sample.emergency_reserve,
                "mode": sample.mode.value,
                "remote_observed_at": sample.remote_observed_at,
                "window_end": sample.window_end,
                "calculation_version": sample.calculation_version,
            }
        )

    def _buffer_mm_fill_markout(self, markout: MarketMakerFillMarkout) -> None:
        self._mm_fill_markout_rows.append(
            {
                "ts": markout.ts,
                "strategy_id": str(markout.strategy_id),
                "symbol": str(markout.symbol),
                "session_id": markout.session_id,
                "fill_id": markout.fill_id,
                "order_id": str(markout.order_id),
                "cloid": str(markout.cloid),
                "fill_ts": markout.fill_ts,
                "side": markout.side.value,
                "fill_px": markout.fill_px,
                "fill_size": markout.fill_size,
                "reference": markout.reference,
                "reference_px": markout.reference_px,
                "horizon_ms": markout.horizon_ms,
                "horizon_ts": markout.horizon_ts,
                "mark_px": markout.mark_px,
                "signed_markout_bps": float(markout.signed_markout_bps),
                "signed_markout_usdc": markout.signed_markout_usdc,
                "spread_capture_usdc": markout.spread_capture_usdc,
                "maker": markout.maker,
                "queue_ahead_size": markout.queue_ahead_size,
                "fill_probability": None if markout.fill_probability is None else float(markout.fill_probability),
                "calculation_version": markout.calculation_version,
            }
        )

    async def close(self) -> None:
        """Flush remaining data and close connection."""
        await self.flush()
        if self._client:
            self._client.close()
            self._client = None
        logger.info("ch_writer_closed")
