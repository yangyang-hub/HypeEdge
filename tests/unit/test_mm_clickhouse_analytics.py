"""Tests for market-making ClickHouse analytical projections."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

from hypeedge.config.settings import ClickHouseSettings
from hypeedge.core.enums import ActionBudgetMode, Side
from hypeedge.core.events import (
    ALL_EVENT_TYPES,
    EVENT_MM_ACTION_CREDIT_SAMPLE,
    EVENT_MM_FEATURE_SAMPLE,
    EVENT_MM_FILL_MARKOUT,
    EVENT_MM_INVENTORY_SAMPLE,
    EVENT_MM_QUOTE_DECISION,
    Event,
    EventBus,
)
from hypeedge.core.types import Cloid, OrderId, Price, Size, StrategyId, Symbol, Usd
from hypeedge.storage.clickhouse import DDL_STATEMENTS, ClickHouseWriter
from hypeedge.storage.mm_analytics import (
    MarketMakerActionCreditSample,
    MarketMakerFeatureSample,
    MarketMakerFillMarkout,
    MarketMakerInventorySample,
    MarketMakerQuoteDecision,
)

NOW = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)
STRATEGY_ID = StrategyId("mm_btc")
SYMBOL = Symbol("BTC")


def _writer(tmp_path: object) -> ClickHouseWriter:
    return ClickHouseWriter(
        ClickHouseSettings(batch_size=100, spool_path=str(tmp_path) + "/spool.sqlite3"),
        EventBus(),
    )


def test_market_making_ddl_uses_required_sorting_and_retention() -> None:
    ddl = "\n".join(DDL_STATEMENTS)

    for table in (
        "mm_feature_samples",
        "mm_quote_decisions",
        "mm_inventory_samples",
        "mm_action_credit_samples",
        "mm_fill_markouts",
    ):
        table_ddl = ddl.split(f"CREATE TABLE IF NOT EXISTS {table} (", maxsplit=1)[1].split(
            "CREATE TABLE IF NOT EXISTS", maxsplit=1
        )[0]
        assert "ORDER BY (strategy_id, symbol, ts)" in table_ddl
        assert "TTL ts + INTERVAL" in table_ddl

    markout_ddl = ddl.split("CREATE TABLE IF NOT EXISTS mm_fill_markouts (", maxsplit=1)[1]
    assert "reference" in markout_ddl
    assert "horizon_ms" in markout_ddl
    assert "side" in markout_ddl
    assert "calculation_version" in markout_ddl


def test_market_making_telemetry_events_are_registered_and_lossy() -> None:
    bus = EventBus()
    event_types = {
        EVENT_MM_FEATURE_SAMPLE,
        EVENT_MM_QUOTE_DECISION,
        EVENT_MM_INVENTORY_SAMPLE,
        EVENT_MM_ACTION_CREDIT_SAMPLE,
        EVENT_MM_FILL_MARKOUT,
    }

    assert event_types <= ALL_EVENT_TYPES
    assert all(bus.is_lossy_event(event_type) for event_type in event_types)


def test_writer_buffers_all_market_making_analytics(tmp_path) -> None:
    writer = _writer(tmp_path)
    feature = MarketMakerFeatureSample(
        ts=NOW,
        strategy_id=STRATEGY_ID,
        symbol=SYMBOL,
        session_id="session-1",
        config_version=7,
        model_version="fair-v2",
        market_version=101,
        exchange_ts=NOW - timedelta(milliseconds=2),
        received_at=NOW - timedelta(milliseconds=1),
        mid_px=Price("60000"),
        microprice=Price("60000.1"),
        fair_px=Price("60000.2"),
        best_bid_px=Price("59999.5"),
        best_ask_px=Price("60000.5"),
        normalized_ofi_l1=Decimal("0.1"),
        normalized_ofi_l5=Decimal("0.2"),
        trade_flow=Decimal("-0.1"),
        short_return=Decimal("0.00001"),
        volatility_1s=Decimal("0.001"),
        volatility_5s=Decimal("0.002"),
        volatility_30s=Decimal("0.003"),
        volatility_5m=Decimal("0.004"),
        toxicity=Decimal("0.25"),
        receipt_to_decision_us=500,
        event_loop_lag_us=50,
    )
    decision = MarketMakerQuoteDecision(
        ts=NOW,
        strategy_id=STRATEGY_ID,
        symbol=SYMBOL,
        session_id="session-1",
        config_version=7,
        model_version="fair-v2",
        quote_revision=12,
        market_version=101,
        decision="no_quote",
        reason="edge_below_threshold",
        fair_px=Price("60000.2"),
        reservation_px=Price("60000.1"),
        desired_bid_px=None,
        desired_bid_size=None,
        desired_ask_px=None,
        desired_ask_size=None,
        live_bid_px=Price("59999"),
        live_bid_size=Size("0.001"),
        live_ask_px=None,
        live_ask_size=None,
        position_size=Size("0.001"),
        inventory_notional_usdc=Usd("60"),
        budget_mode=ActionBudgetMode.CONSERVE,
        expected_gross_edge_usdc=Usd("0.02"),
        adverse_selection_cost_usdc=Usd("0.01"),
        inventory_cost_usdc=Usd("0.004"),
        funding_cost_usdc=Usd("0.001"),
        action_cost_usdc=Usd("0.003"),
        failure_cost_usdc=Usd("0.003"),
        expected_net_pnl_usdc=Usd("-0.001"),
    )
    inventory = MarketMakerInventorySample(
        ts=NOW,
        strategy_id=STRATEGY_ID,
        symbol=SYMBOL,
        session_id="session-1",
        position_size=Size("0.001"),
        mark_px=Price("60000"),
        inventory_notional_usdc=Usd("60"),
        soft_limit_utilization=Decimal("0.1"),
        hard_limit_utilization=Decimal("0.05"),
        emergency_limit_utilization=Decimal("0.02"),
        equity_usdc=Usd("1000"),
        available_balance_usdc=Usd("900"),
        margin_used_usdc=Usd("100"),
        liquidation_distance_bps=None,
        funding_carry_usdc=Usd("-0.01"),
        reduce_only=False,
        healthy=True,
    )
    credits = MarketMakerActionCreditSample(
        ts=NOW,
        strategy_id=STRATEGY_ID,
        symbol=SYMBOL,
        quota_owner="0xabc",
        remote_remaining=9000,
        shadow_remaining=8998,
        cancel_headroom=1000,
        ip_weight_remaining=1100,
        actions_burned_1h=10,
        actions_earned_1h=20,
        actions_burned_24h=100,
        actions_earned_24h=200,
        fills_1h=2,
        usdc_volume_1h=Usd("25"),
        usdc_per_action_1h=Decimal("2.5"),
        usdc_per_action_24h=Decimal("2"),
        runway_hours=Decimal("12.5"),
        soft_allocation=5000,
        hard_allocation=8000,
        emergency_reserve=1000,
        mode=ActionBudgetMode.NORMAL,
        remote_observed_at=NOW,
        window_end=NOW,
        calculation_version="credits-v1",
    )
    markout = MarketMakerFillMarkout(
        ts=NOW + timedelta(seconds=5),
        strategy_id=STRATEGY_ID,
        symbol=SYMBOL,
        session_id="session-1",
        fill_id="fill-1",
        order_id=OrderId("order-1"),
        cloid=Cloid("0x0123456789abcdef0123456789abcdef"),
        fill_ts=NOW,
        side=Side.BUY,
        fill_px=Price("60000"),
        fill_size=Size("0.001"),
        reference="mid",
        reference_px=Price("60000.1"),
        horizon_ms=5000,
        horizon_ts=NOW + timedelta(seconds=5),
        mark_px=Price("60001"),
        signed_markout_bps=Decimal("0.166666"),
        signed_markout_usdc=Usd("0.001"),
        spread_capture_usdc=Usd("0.0001"),
        maker=True,
        queue_ahead_size=None,
        fill_probability=Decimal("0.25"),
        calculation_version="markout-v1",
    )

    events = (
        (EVENT_MM_FEATURE_SAMPLE, feature),
        (EVENT_MM_QUOTE_DECISION, decision),
        (EVENT_MM_INVENTORY_SAMPLE, inventory),
        (EVENT_MM_ACTION_CREDIT_SAMPLE, credits),
        (EVENT_MM_FILL_MARKOUT, markout),
    )
    for event_type, payload in events:
        writer._buffer_event(Event(event_type=event_type, payload=payload))

    assert writer._mm_feature_rows[0]["fair_px"] == Decimal("60000.2")
    assert writer._mm_quote_decision_rows[0]["decision"] == "no_quote"
    assert writer._mm_inventory_rows[0]["healthy"] is True
    assert writer._mm_action_credit_rows[0]["mode"] == "normal"
    assert writer._mm_fill_markout_rows[0]["reference"] == "mid"
    assert writer._mm_fill_markout_rows[0]["horizon_ms"] == 5000
    assert writer._mm_fill_markout_rows[0]["side"] == "buy"
    assert writer._mm_fill_markout_rows[0]["calculation_version"] == "markout-v1"


async def test_failed_decimal_analytics_flush_uses_spool(tmp_path) -> None:
    writer = _writer(tmp_path)
    writer._client = MagicMock()
    writer._client.insert.side_effect = RuntimeError("clickhouse unavailable")
    writer._mm_fill_markout_rows = [{"ts": NOW, "fill_px": Decimal("60000.01")}]
    await writer._spool.initialize()

    await writer._flush_buffer("_mm_fill_markout_rows", "mm_fill_markouts")

    assert writer._mm_fill_markout_rows == []
    pending = await writer._spool.pending()
    assert pending[0][1] == "mm_fill_markouts"
    assert pending[0][2][0]["fill_px"] == Decimal("60000.01")
