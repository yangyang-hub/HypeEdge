"""Event-driven runtime boundary for the pure market-maker policy."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol, cast

import structlog

from hypeedge.account.health import AccountHealthProvider
from hypeedge.core.enums import MarketMakerLifecycle, QuoteDecision
from hypeedge.core.events import (
    EVENT_ACCOUNT_STATE_UPDATE,
    EVENT_ACTION_CREDITS_LOW,
    EVENT_L2_BOOK_UPDATE,
    EVENT_MM_FILL_MARKOUT,
    EVENT_ORDER_FILLED,
    EVENT_ORDER_PARTIAL_FILL,
    EVENT_POSITION_CHANGED,
    EVENT_RECONCILIATION_COMPLETE,
    EVENT_TRADE_UPDATE,
    EVENT_WS_CONNECTED,
    EVENT_WS_DISCONNECTED,
    Event,
    EventBus,
)
from hypeedge.core.models import Fill, FundingRate, L2BookSnapshot, Position, Trade
from hypeedge.core.types import StrategyId, SubAccount, Symbol, Usd
from hypeedge.market_data.external_reference import ExternalReferenceProvider
from hypeedge.market_data.features import MarketFeatureEngine
from hypeedge.storage.mm_analytics import MarketMakerFillMarkout
from hypeedge.strategy.market_maker.estimators import AdverseMarkoutEstimator, DecisionLatencyEstimator
from hypeedge.strategy.market_maker.models import (
    ActionBudgetSnapshot,
    InventorySnapshot,
    MarketFeatures,
    MarketMakerConfig,
)
from hypeedge.strategy.market_maker.policy import MarketMakerPolicy
from hypeedge.strategy.market_maker.shadow import ShadowActionEstimate, ShadowOrderState
from hypeedge.strategy.registry import StrategyBuildContext, StrategyConfigSnapshot, StrategyRuntimeHandle
from hypeedge.trading.quote_coordinator import QuoteCoordinator
from hypeedge.trading.quotes import DesiredQuote, DesiredQuoteSet, QuotePlan, QuoteSlotView

logger = structlog.get_logger(__name__)

_RELIABLE_EVENTS = frozenset(
    {
        EVENT_ORDER_FILLED,
        EVENT_ORDER_PARTIAL_FILL,
        EVENT_POSITION_CHANGED,
        EVENT_ACCOUNT_STATE_UPDATE,
        EVENT_ACTION_CREDITS_LOW,
        EVENT_RECONCILIATION_COMPLETE,
        EVENT_WS_CONNECTED,
        EVENT_WS_DISCONNECTED,
    }
)
_CANCEL_MODES = frozenset(
    {
        MarketMakerLifecycle.PAUSED,
        MarketMakerLifecycle.DRAINING,
        MarketMakerLifecycle.FAULTED,
        MarketMakerLifecycle.STOPPED,
    }
)


class InventorySnapshotProvider(Protocol):
    def get_inventory(self, sub_account: SubAccount, symbol: Symbol) -> InventorySnapshot: ...


class ActionBudgetSnapshotProvider(Protocol):
    def get_action_budget(self, strategy_id: StrategyId, symbol: Symbol) -> ActionBudgetSnapshot: ...


class QuoteSlotProvider(Protocol):
    async def get_quote_slots(self, strategy_id: StrategyId, symbol: Symbol) -> tuple[QuoteSlotView, QuoteSlotView]: ...


class FundingSnapshotProvider(Protocol):
    def get_funding(self, symbol: Symbol) -> FundingRate | None: ...


@dataclass(frozen=True, slots=True)
class QuoteCancelRequest:
    strategy_id: StrategyId
    session_id: str
    symbol: Symbol
    config_version: int | None
    revision: int
    reason: str
    requested_at: datetime


class QuotePlanCommandClient(Protocol):
    """Only durable quote-set commands cross the live execution boundary."""

    async def submit_quote_plan(self, plan: QuotePlan) -> None: ...

    async def cancel_strategy_quotes(self, request: QuoteCancelRequest) -> None: ...


class MarketMakerTelemetrySink(Protocol):
    async def record_cycle(
        self,
        *,
        mode: MarketMakerLifecycle,
        features: MarketFeatures,
        desired: DesiredQuoteSet,
        plan: QuotePlan,
        shadow_actions: ShadowActionEstimate | None,
    ) -> None: ...


class NullMarketMakerTelemetrySink:
    async def record_cycle(
        self,
        *,
        mode: MarketMakerLifecycle,
        features: MarketFeatures,
        desired: DesiredQuoteSet,
        plan: QuotePlan,
        shadow_actions: ShadowActionEstimate | None,
    ) -> None:
        del mode, features, desired, plan, shadow_actions


@dataclass(frozen=True, slots=True)
class MarketMakerRuntimeSnapshot:
    strategy_id: StrategyId
    session_id: str
    symbol: Symbol
    mode: MarketMakerLifecycle
    config_version: int | None
    quote_revision: int
    market_version: int | None
    connection_generation: int | None
    last_cycle_at: datetime | None
    last_reason: str | None
    desired: DesiredQuoteSet | None
    plan: QuotePlan | None
    features: MarketFeatures | None


class MarketMakerRuntime(StrategyRuntimeHandle):
    """Coalesced market loop; policy stays pure and all live writes stay durable."""

    def __init__(
        self,
        *,
        strategy_id: StrategyId,
        session_id: str,
        sub_account: SubAccount,
        symbol: Symbol,
        event_bus: EventBus,
        feature_engine: MarketFeatureEngine,
        policy: MarketMakerPolicy,
        coordinator: QuoteCoordinator,
        inventory: InventorySnapshotProvider,
        budget: ActionBudgetSnapshotProvider,
        account_health: AccountHealthProvider,
        slots: QuoteSlotProvider,
        commands: QuotePlanCommandClient,
        telemetry: MarketMakerTelemetrySink | None = None,
        config_decoder: Callable[[StrategyConfigSnapshot], MarketMakerConfig] | None = None,
        external_reference: ExternalReferenceProvider | None = None,
        funding: FundingSnapshotProvider | None = None,
        latency_estimator: DecisionLatencyEstimator | None = None,
        markout_estimator: AdverseMarkoutEstimator | None = None,
        market_stale_after: timedelta = timedelta(seconds=2),
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not session_id:
            raise ValueError("session_id is required")
        if market_stale_after <= timedelta(0):
            raise ValueError("market_stale_after must be positive")
        self._strategy_id = strategy_id
        self._session_id = session_id
        self._sub_account = sub_account
        self._symbol = symbol
        self._bus = event_bus
        self._feature_engine = feature_engine
        self._policy = policy
        self._coordinator = coordinator
        self._inventory = inventory
        self._budget = budget
        self._account_health = account_health
        self._slots = slots
        self._commands = commands
        self._telemetry = telemetry or NullMarketMakerTelemetrySink()
        self._config_decoder = config_decoder or decode_market_maker_config
        self._external_reference = external_reference
        self._funding = funding
        self._latency_estimator = latency_estimator or DecisionLatencyEstimator()
        self._markout_estimator = markout_estimator or AdverseMarkoutEstimator()
        self._market_stale_after = market_stale_after
        self._clock = clock or (lambda: datetime.now(UTC))
        self._mode = MarketMakerLifecycle.WARMING
        self._config: MarketMakerConfig | None = None
        self._book: L2BookSnapshot | None = None
        self._revision = 0
        self._last_cycle_at: datetime | None = None
        self._last_reason: str | None = "not_started"
        self._last_desired: DesiredQuoteSet | None = None
        self._last_plan: QuotePlan | None = None
        self._last_features: MarketFeatures | None = None
        self._shadow = ShadowOrderState()
        self._market_queue: asyncio.Queue[Event] | None = None
        self._trade_queue: asyncio.Queue[Event] | None = None
        self._reliable_queue: asyncio.Queue[Event] | None = None
        self._markout_queue: asyncio.Queue[Event] | None = None
        self._task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._market_queue = self._bus.subscribe(EVENT_L2_BOOK_UPDATE, maxsize=1)
        self._trade_queue = self._bus.subscribe(EVENT_TRADE_UPDATE, maxsize=1)
        self._reliable_queue = self._bus.subscribe_many(_RELIABLE_EVENTS)
        self._markout_queue = self._bus.subscribe(EVENT_MM_FILL_MARKOUT, maxsize=256)
        self._task = asyncio.create_task(self._run(), name=f"market-maker:{self._strategy_id}")
        self._last_reason = "warming"

    async def set_mode(self, mode: MarketMakerLifecycle) -> None:
        async with self._lock:
            previous = self._mode
            self._mode = mode
            if mode in _CANCEL_MODES or (
                mode == MarketMakerLifecycle.SHADOW and previous == MarketMakerLifecycle.RUNNING
            ):
                if mode in _CANCEL_MODES:
                    await self._cycle(f"lifecycle_{mode.value}")
                await self._cancel_live_quotes(f"lifecycle_{mode.value}")
            elif mode in {MarketMakerLifecycle.SHADOW, MarketMakerLifecycle.RUNNING}:
                await self._cycle("lifecycle_change")

    async def apply_config(self, config: StrategyConfigSnapshot) -> None:
        if config.strategy_id != self._strategy_id:
            raise ValueError("configuration belongs to another strategy")
        decoded = self._config_decoder(config)
        if decoded.version != config.revision:
            raise ValueError("decoded configuration version does not match registry revision")
        async with self._lock:
            self._config = decoded
            await self._cycle("config_applied")

    async def stop(self) -> None:
        await self.set_mode(MarketMakerLifecycle.STOPPED)
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if self._market_queue is not None:
            self._bus.unsubscribe(EVENT_L2_BOOK_UPDATE, self._market_queue)
        if self._trade_queue is not None:
            self._bus.unsubscribe(EVENT_TRADE_UPDATE, self._trade_queue)
        if self._reliable_queue is not None:
            self._bus.unsubscribe_many(_RELIABLE_EVENTS, self._reliable_queue)
        if self._markout_queue is not None:
            self._bus.unsubscribe(EVENT_MM_FILL_MARKOUT, self._markout_queue)

    def snapshot(self) -> MarketMakerRuntimeSnapshot:
        book = self._book
        return MarketMakerRuntimeSnapshot(
            strategy_id=self._strategy_id,
            session_id=self._session_id,
            symbol=self._symbol,
            mode=self._mode,
            config_version=self._config.version if self._config is not None else None,
            quote_revision=self._revision,
            market_version=book.version if book is not None else None,
            connection_generation=book.connection_generation if book is not None else None,
            last_cycle_at=self._last_cycle_at,
            last_reason=self._last_reason,
            desired=self._last_desired,
            plan=self._last_plan,
            features=self._last_features,
        )

    async def _run(self) -> None:
        assert self._market_queue is not None
        assert self._trade_queue is not None
        assert self._reliable_queue is not None
        assert self._markout_queue is not None
        while True:
            tasks = {
                asyncio.create_task(self._market_queue.get()),
                asyncio.create_task(self._trade_queue.get()),
                asyncio.create_task(self._reliable_queue.get()),
                asyncio.create_task(self._markout_queue.get()),
            }
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                await self._handle_event(task.result())

    async def _handle_event(self, event: Event) -> None:
        async with self._lock:
            payload = event.payload
            if event.event_type == EVENT_L2_BOOK_UPDATE:
                if not isinstance(payload, L2BookSnapshot) or payload.symbol != self._symbol:
                    return
                if self._book is not None and (
                    payload.connection_generation < self._book.connection_generation
                    or (
                        payload.connection_generation == self._book.connection_generation
                        and payload.version <= self._book.version
                    )
                ):
                    self._last_reason = "stale_market_event_fenced"
                    return
                self._book = payload
                await self._cycle("book_update")
                return
            if event.event_type == EVENT_TRADE_UPDATE:
                if isinstance(payload, Trade) and payload.symbol == self._symbol:
                    self._feature_engine.observe_trade(payload)
                    await self._cycle("trade_update")
                return
            if event.event_type in {EVENT_ORDER_FILLED, EVENT_ORDER_PARTIAL_FILL}:
                if isinstance(payload, Fill) and self._matches_fill(payload):
                    if self._mode == MarketMakerLifecycle.SHADOW:
                        self._shadow.simulate_fill_by_cloid(payload.cloid, size=payload.size)
                    await self._cycle("fill_update")
                return
            if event.event_type == EVENT_POSITION_CHANGED:
                if isinstance(payload, Position) and self._matches_position(payload):
                    await self._cycle("position_update")
                return
            if event.event_type == EVENT_MM_FILL_MARKOUT:
                if (
                    isinstance(payload, MarketMakerFillMarkout)
                    and payload.strategy_id == self._strategy_id
                    and payload.symbol == self._symbol
                ):
                    self._markout_estimator.observe(payload, now=self._clock())
                return
            # Account, budget, reconciliation and connection events are reliable
            # invalidation signals. Providers remain the authoritative snapshots.
            await self._cycle(event.event_type)

    async def _cycle(self, reason: str) -> None:
        config = self._config
        book = self._book
        if config is None or book is None:
            self._last_reason = "waiting_for_config_or_book"
            return
        now = self._clock()
        account = self._account_health.get_account_health(now=now)
        book_healthy = self._book_is_healthy(book, now) and account.allows_risk_increase
        receipt_latency = Decimal(str(max(0.0, (now - book.received_at).total_seconds())))
        self._latency_estimator.observe(receipt_latency)
        markout = self._markout_estimator.estimate(
            self._strategy_id,
            self._symbol,
            min_samples=config.min_markout_samples,
            conservative_default_bps=config.conservative_markout_bps,
        )
        funding = self._funding.get_funding(self._symbol) if self._funding is not None else None
        external = (
            self._external_reference.get_external_reference(self._symbol)
            if self._external_reference is not None
            else None
        )
        try:
            features = self._feature_engine.build(
                book,
                healthy=book_healthy,
                funding_rate=Decimal(str(funding.funding_rate)) if funding is not None else Decimal(0),
                expected_adverse_markout_bps=markout.adverse_bps,
                latency_seconds=self._latency_estimator.seconds,
                latency_quality=self._latency_estimator.quality,
                markout_quality=markout.quality,
                external_reference=external,
                config=config,
                decision_at=now,
            )
        except ValueError:
            await self._cancel_live_quotes("invalid_market_snapshot")
            return
        inventory = self._inventory.get_inventory(self._sub_account, self._symbol)
        budget = self._budget.get_action_budget(self._strategy_id, self._symbol)
        bid_view, ask_view = await self._views()
        self._revision += 1
        desired = self._policy.quote(
            strategy_id=self._strategy_id,
            session_id=self._session_id,
            revision=self._revision,
            current_slot_revision=max(bid_view.revision, ask_view.revision),
            features=features,
            inventory=inventory,
            budget=budget,
            config=config,
        )
        if self._mode != MarketMakerLifecycle.RUNNING and self._mode != MarketMakerLifecycle.SHADOW:
            desired = _as_no_quote(desired, f"lifecycle_{self._mode.value}")
        plan = self._coordinator.coordinate(
            desired,
            bid_view,
            ask_view,
            tick_size=config.tick_size,
            now=now,
        )
        shadow_actions: ShadowActionEstimate | None = None
        if self._mode == MarketMakerLifecycle.SHADOW:
            shadow_actions = self._shadow.apply(plan, now=now)
        elif (
            self._mode == MarketMakerLifecycle.RUNNING
            and not plan.fenced
            and any(diff.estimated_incremental_actions for diff in plan.diffs)
        ):
            await self._commands.submit_quote_plan(plan)
        self._last_cycle_at = now
        self._last_reason = reason if not plan.fenced else plan.fence_reason
        self._last_desired = desired
        self._last_plan = plan
        self._last_features = features
        await self._telemetry.record_cycle(
            mode=self._mode,
            features=features,
            desired=desired,
            plan=plan,
            shadow_actions=shadow_actions,
        )

    async def _views(self) -> tuple[QuoteSlotView, QuoteSlotView]:
        if self._mode == MarketMakerLifecycle.SHADOW:
            return self._shadow.views(self._strategy_id, self._symbol)
        return await self._slots.get_quote_slots(self._strategy_id, self._symbol)

    async def _cancel_live_quotes(self, reason: str) -> None:
        self._revision += 1
        await self._commands.cancel_strategy_quotes(
            QuoteCancelRequest(
                strategy_id=self._strategy_id,
                session_id=self._session_id,
                symbol=self._symbol,
                config_version=self._config.version if self._config is not None else None,
                revision=self._revision,
                reason=reason,
                requested_at=self._clock(),
            )
        )
        self._last_reason = reason

    def _book_is_healthy(self, book: L2BookSnapshot, now: datetime) -> bool:
        return bool(
            book.bids
            and book.asks
            and book.bids[0].price < book.asks[0].price
            and now - book.received_at <= self._market_stale_after
            and now >= book.received_at
        )

    def _matches_fill(self, fill: Fill) -> bool:
        return bool(
            fill.symbol == self._symbol
            and (fill.strategy_id is None or fill.strategy_id == self._strategy_id)
            and (fill.sub_account is None or fill.sub_account == self._sub_account)
        )

    def _matches_position(self, position: Position) -> bool:
        return bool(
            position.symbol == self._symbol
            and (position.strategy_id is None or position.strategy_id == self._strategy_id)
            and (position.sub_account is None or position.sub_account == self._sub_account)
        )


@dataclass(frozen=True, slots=True)
class MarketMakerRuntimeDependencies:
    event_bus: EventBus
    feature_engine_factory: Callable[[], MarketFeatureEngine]
    policy_factory: Callable[[], MarketMakerPolicy]
    coordinator_factory: Callable[[], QuoteCoordinator]
    inventory: InventorySnapshotProvider
    budget: ActionBudgetSnapshotProvider
    account_health: AccountHealthProvider
    slots: QuoteSlotProvider
    commands: QuotePlanCommandClient
    telemetry: MarketMakerTelemetrySink | None = None
    config_decoder: Callable[[StrategyConfigSnapshot], MarketMakerConfig] | None = None
    symbol_config_decoder: Callable[[StrategyConfigSnapshot, Symbol], MarketMakerConfig] | None = None
    external_reference: ExternalReferenceProvider | None = None
    funding: FundingSnapshotProvider | None = None
    latency_estimator_factory: Callable[[], DecisionLatencyEstimator] = DecisionLatencyEstimator
    markout_estimator_factory: Callable[[], AdverseMarkoutEstimator] = AdverseMarkoutEstimator
    session_id_factory: Callable[[StrategyBuildContext], str] = lambda context: (
        f"{context.instance.strategy_id}:{context.config.revision}"
    )

    def factory(self, context: StrategyBuildContext) -> StrategyRuntimeHandle:
        symbol_decoder = self.symbol_config_decoder
        return MarketMakerRuntime(
            strategy_id=context.instance.strategy_id,
            session_id=self.session_id_factory(context),
            sub_account=context.instance.sub_account,
            symbol=context.instance.symbol,
            event_bus=self.event_bus,
            feature_engine=self.feature_engine_factory(),
            policy=self.policy_factory(),
            coordinator=self.coordinator_factory(),
            inventory=self.inventory,
            budget=self.budget,
            account_health=self.account_health,
            slots=self.slots,
            commands=self.commands,
            telemetry=self.telemetry,
            external_reference=self.external_reference,
            funding=self.funding,
            latency_estimator=self.latency_estimator_factory(),
            markout_estimator=self.markout_estimator_factory(),
            config_decoder=(
                (lambda snapshot: symbol_decoder(snapshot, context.instance.symbol))
                if symbol_decoder is not None
                else self.config_decoder
            ),
        )


def decode_market_maker_config(snapshot: StrategyConfigSnapshot) -> MarketMakerConfig:
    """Default adapter for typed registry values; applications may inject stricter decoding."""
    embedded = snapshot.values.get("market_maker_config")
    if isinstance(embedded, MarketMakerConfig):
        return replace(embedded, version=snapshot.revision)
    values: Mapping[str, Any] = snapshot.values
    converted = dict(values)
    converted["version"] = snapshot.revision
    return MarketMakerConfig(**cast(Any, converted))


def _as_no_quote(desired: DesiredQuoteSet, reason: str) -> DesiredQuoteSet:
    def no_quote(quote: DesiredQuote) -> DesiredQuote:
        return DesiredQuote(
            slot=quote.slot,
            decision=QuoteDecision.NO_QUOTE,
            price=None,
            size=None,
            gross_edge_usdc=Usd("0"),
            reason=reason,
        )

    return replace(desired, bid=no_quote(desired.bid), ask=no_quote(desired.ask), expected_utility_usdc=Usd("0"))
