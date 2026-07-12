"""Async safety tests for the market-maker runtime boundary."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from hypeedge.account.health import AccountHealthDimension, LayeredAccountHealthProvider
from hypeedge.core.enums import ActionBudgetMode, MarketMakerLifecycle, OrderStatus, QuoteDecision, Side
from hypeedge.core.events import EVENT_L2_BOOK_UPDATE, EVENT_ORDER_PARTIAL_FILL, Event, EventBus
from hypeedge.core.models import Fill, FundingRate, L2BookSnapshot, L2Level
from hypeedge.core.types import Cloid, OrderId, Price, Size, StrategyId, SubAccount, Symbol, Timestamp, Usd
from hypeedge.market_data.external_reference import ExternalReferenceSnapshot
from hypeedge.market_data.features import MarketFeatureEngine
from hypeedge.strategy.market_maker.models import ActionBudgetSnapshot, InventorySnapshot, MarketMakerConfig
from hypeedge.strategy.market_maker.policy import MarketMakerPolicy
from hypeedge.strategy.market_maker.runtime import MarketMakerRuntime, QuoteCancelRequest
from hypeedge.strategy.registry import StrategyConfigSnapshot
from hypeedge.trading.quote_coordinator import QuoteCoordinator, QuoteCoordinatorConfig
from hypeedge.trading.quotes import QuotePlan, QuoteRiskOwner, QuoteSlotKey, QuoteSlotView

NOW = datetime(2026, 7, 11, tzinfo=UTC)
SID = StrategyId("mm-btc")
SYMBOL = Symbol("BTC")
SUB = SubAccount("maker")


class _Inventory:
    def get_inventory(self, sub_account: SubAccount, symbol: Symbol) -> InventorySnapshot:
        assert (sub_account, symbol) == (SUB, SYMBOL)
        return InventorySnapshot(Size("0"), Usd("1000"), Usd("900"), Usd("100"), NOW, True)


class _Budget:
    def get_action_budget(self, strategy_id: StrategyId, symbol: Symbol) -> ActionBudgetSnapshot:
        assert (strategy_id, symbol) == (SID, SYMBOL)
        return ActionBudgetSnapshot(ActionBudgetMode.NORMAL, 9000, 9000, 1000, Usd("0"), NOW, True)


class _Slots:
    def __init__(self, views: tuple[QuoteSlotView, QuoteSlotView] | None = None) -> None:
        self.views = views or _empty_views()

    async def get_quote_slots(self, strategy_id: StrategyId, symbol: Symbol) -> tuple[QuoteSlotView, QuoteSlotView]:
        assert (strategy_id, symbol) == (SID, SYMBOL)
        return self.views


class _Funding:
    def get_funding(self, symbol: Symbol) -> FundingRate | None:
        assert symbol == SYMBOL
        return FundingRate(symbol, 0.0003, 0.0, Price("100"), 10.0, Timestamp(1))


class _External:
    def get_external_reference(self, symbol: Symbol) -> ExternalReferenceSnapshot:
        assert symbol == SYMBOL
        return ExternalReferenceSnapshot(
            source="binance_spot_perpetual",
            symbol=SYMBOL,
            raw_price=Price("100.1"),
            adjusted_price=Price("100.2"),
            basis_bps=Decimal("10"),
            effective_weight=Decimal("0.5"),
            confidence=Decimal("1"),
            age_ms=0,
            quality="healthy",
            observed_at=NOW,
        )


class _Commands:
    def __init__(self) -> None:
        self.plans: list[QuotePlan] = []
        self.cancels: list[QuoteCancelRequest] = []

    async def submit_quote_plan(self, plan: QuotePlan) -> None:
        self.plans.append(plan)

    async def cancel_strategy_quotes(self, request: QuoteCancelRequest) -> None:
        self.cancels.append(request)


@dataclass
class _Telemetry:
    cycles: list[tuple[MarketMakerLifecycle, QuotePlan]]

    async def record_cycle(self, **values: object) -> None:
        self.cycles.append((values["mode"], values["plan"]))  # type: ignore[arg-type]


def _config(version: int = 1, *, quote_size: str = "0.1") -> MarketMakerConfig:
    return MarketMakerConfig(
        version=version,
        model_version=f"v{version}",
        tick_size=Decimal("0.1"),
        lot_size=Decimal("0.001"),
        min_size=Decimal("0.001"),
        soft_inventory_notional=Usd("100"),
        hard_inventory_notional=Usd("150"),
        emergency_inventory_notional=Usd("200"),
        quote_size=Size(quote_size),
        max_depth_participation=Decimal("0.1"),
        signed_maker_fee_rate=Decimal("-0.001"),
        expected_fill_probability=Decimal("1"),
    )


def _book(version: int = 1, generation: int = 1) -> L2BookSnapshot:
    return L2BookSnapshot(
        symbol=SYMBOL,
        bids=(L2Level(Price("99.9"), Size("5")),),
        asks=(L2Level(Price("100.1"), Size("5")),),
        timestamp=Timestamp(1),
        local_ts=NOW,
        version=version,
        connection_generation=generation,
    )


def _empty_views() -> tuple[QuoteSlotView, QuoteSlotView]:
    return (
        QuoteSlotView(QuoteSlotKey(SID, SYMBOL, Side.BUY), 0, 0, ()),
        QuoteSlotView(QuoteSlotKey(SID, SYMBOL, Side.SELL), 0, 0, ()),
    )


def _health() -> LayeredAccountHealthProvider:
    provider = LayeredAccountHealthProvider()
    for dimension in AccountHealthDimension:
        provider.record_success(dimension, observed_at=NOW)
    return provider


def _runtime(
    commands: _Commands,
    *,
    slots: _Slots | None = None,
    telemetry: _Telemetry | None = None,
    funding: _Funding | None = None,
    external: _External | None = None,
) -> tuple[MarketMakerRuntime, EventBus]:
    bus = EventBus()
    runtime = MarketMakerRuntime(
        strategy_id=SID,
        session_id="session-1",
        sub_account=SUB,
        symbol=SYMBOL,
        event_bus=bus,
        feature_engine=MarketFeatureEngine(),
        policy=MarketMakerPolicy(),
        coordinator=QuoteCoordinator(QuoteCoordinatorConfig()),
        inventory=_Inventory(),
        budget=_Budget(),
        account_health=_health(),
        slots=slots or _Slots(),
        commands=commands,
        telemetry=telemetry,
        funding=funding,
        external_reference=external,
        clock=lambda: NOW,
    )
    return runtime, bus


async def _configure(runtime: MarketMakerRuntime, config: MarketMakerConfig | None = None) -> None:
    selected = config or _config()
    await runtime.apply_config(StrategyConfigSnapshot(SID, selected.version, {"market_maker_config": selected}))


async def _publish_book(bus: EventBus, book: L2BookSnapshot | None = None) -> None:
    await bus.publish(Event(EVENT_L2_BOOK_UPDATE, book or _book()))
    for _ in range(20):
        await asyncio.sleep(0)


async def test_shadow_calculates_and_records_but_never_submits_live_plan() -> None:
    commands = _Commands()
    telemetry = _Telemetry([])
    runtime, bus = _runtime(commands, telemetry=telemetry)
    await runtime.start()
    await _configure(runtime)
    await runtime.set_mode(MarketMakerLifecycle.SHADOW)
    await _publish_book(bus)

    snapshot = runtime.snapshot()
    assert snapshot.plan is not None
    assert snapshot.plan.estimated_incremental_actions == 2
    assert commands.plans == []
    assert telemetry.cycles[-1][0] == MarketMakerLifecycle.SHADOW
    await runtime.stop()


async def test_runtime_passes_live_funding_and_external_reference_into_features() -> None:
    runtime, bus = _runtime(_Commands(), funding=_Funding(), external=_External())
    await runtime.start()
    await _configure(runtime)
    await runtime.set_mode(MarketMakerLifecycle.SHADOW)
    await _publish_book(bus)

    features = runtime.snapshot().features
    assert features is not None
    assert features.funding_rate == Decimal("0.0003")
    assert features.external_adjusted_price == Price("100.2")
    assert features.external_quality == "good"
    assert features.markout_quality == "conservative_default"
    assert features.expected_adverse_markout_bps == Decimal("1")
    await runtime.stop()


async def test_running_submits_only_complete_plan_and_pause_uses_cancel_boundary() -> None:
    commands = _Commands()
    runtime, bus = _runtime(commands)
    await runtime.start()
    await _configure(runtime)
    await runtime.set_mode(MarketMakerLifecycle.RUNNING)
    await _publish_book(bus)
    assert len(commands.plans) == 1
    assert {diff.slot.side for diff in commands.plans[0].diffs} == {Side.BUY, Side.SELL}

    await runtime.set_mode(MarketMakerLifecycle.PAUSED)
    assert commands.cancels[-1].reason == "lifecycle_paused"
    assert runtime.snapshot().mode == MarketMakerLifecycle.PAUSED
    assert runtime.snapshot().desired is not None
    assert runtime.snapshot().desired.bid.decision == QuoteDecision.NO_QUOTE
    await runtime.stop()


async def test_old_market_version_and_generation_are_fenced() -> None:
    commands = _Commands()
    runtime, bus = _runtime(commands)
    await runtime.start()
    await _configure(runtime)
    await runtime.set_mode(MarketMakerLifecycle.RUNNING)
    await _publish_book(bus, _book(5, 2))
    assert len(commands.plans) == 1
    await _publish_book(bus, _book(99, 1))
    assert len(commands.plans) == 1
    assert runtime.snapshot().last_reason == "stale_market_event_fenced"
    await runtime.stop()


async def test_unknown_owner_blocks_live_resubmission() -> None:
    owner = QuoteRiskOwner(
        order_id=OrderId("1"),
        cloid=Cloid("unknown"),
        price=Price("99.8"),
        remaining_size=Size("0.1"),
        status=OrderStatus.SUBMIT_UNKNOWN,
        plan_revision=0,
        live_since=NOW,
    )
    views = (
        QuoteSlotView(QuoteSlotKey(SID, SYMBOL, Side.BUY), 0, 0, (owner,)),
        QuoteSlotView(QuoteSlotKey(SID, SYMBOL, Side.SELL), 0, 0, ()),
    )
    commands = _Commands()
    runtime, bus = _runtime(commands, slots=_Slots(views))
    await runtime.start()
    await _configure(runtime)
    await runtime.set_mode(MarketMakerLifecycle.RUNNING)
    await _publish_book(bus)
    assert len(commands.plans) == 1
    assert commands.plans[0].diffs[0].action.value == "blocked_unknown"
    assert commands.plans[0].diffs[0].estimated_incremental_actions == 0
    await runtime.stop()


async def test_hot_config_switch_fences_plan_with_new_version() -> None:
    commands = _Commands()
    runtime, bus = _runtime(commands)
    await runtime.start()
    await _configure(runtime)
    await runtime.set_mode(MarketMakerLifecycle.RUNNING)
    await _publish_book(bus)
    await _configure(runtime, _config(2, quote_size="0.05"))
    assert runtime.snapshot().config_version == 2
    assert commands.plans[-1].config_version == 2
    await runtime.stop()


async def test_shadow_partial_fill_reduces_remaining_and_does_not_replenish() -> None:
    commands = _Commands()
    runtime, bus = _runtime(commands)
    await runtime.start()
    await _configure(runtime)
    await runtime.set_mode(MarketMakerLifecycle.SHADOW)
    await _publish_book(bus)
    first = runtime.snapshot().plan
    assert first is not None
    bid_cloid = runtime._shadow.views(SID, SYMBOL)[0].current_owner  # noqa: SLF001
    assert bid_cloid is not None
    fill = Fill(
        cloid=bid_cloid.cloid,
        exchange_oid=OrderId("shadow-fill"),
        symbol=SYMBOL,
        side=Side.BUY,
        price=bid_cloid.price,
        size=Size("0.04"),
        fee=Usd("0"),
        is_maker=True,
        timestamp=Timestamp(2),
        strategy_id=SID,
        sub_account=SUB,
    )
    await bus.publish(Event(EVENT_ORDER_PARTIAL_FILL, fill))
    for _ in range(20):
        await asyncio.sleep(0)
    remaining = runtime._shadow.views(SID, SYMBOL)[0].current_owner  # noqa: SLF001
    assert remaining is not None
    assert remaining.remaining_size == Size("0.06")
    await runtime.stop()
