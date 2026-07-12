"""Deterministic event-time replay for research-only market-maker evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from hypeedge.backtest.market_maker_metrics import AccountingFill, AccountingLedger, AccountingPnL, ExecutionQuality
from hypeedge.core.enums import Side
from hypeedge.core.types import Price, Size, Usd


class ReplayScenario(StrEnum):
    OPTIMISTIC = "optimistic"
    NEUTRAL = "neutral"
    PESSIMISTIC = "pessimistic"


@dataclass(frozen=True, slots=True)
class ScenarioAssumption:
    latency_ms: int
    queue_multiplier: Decimal


DEFAULT_ASSUMPTIONS = {
    ReplayScenario.OPTIMISTIC: ScenarioAssumption(0, Decimal("0")),
    ReplayScenario.NEUTRAL: ScenarioAssumption(25, Decimal("1")),
    ReplayScenario.PESSIMISTIC: ScenarioAssumption(100, Decimal("2")),
}


@dataclass(frozen=True, slots=True)
class QuoteEvent:
    event_time_ms: int
    order_id: str
    side: Side
    price: Price
    size: Size
    queue_ahead: Size = Size("0")


@dataclass(frozen=True, slots=True)
class CancelEvent:
    event_time_ms: int
    order_id: str


@dataclass(frozen=True, slots=True)
class TradeEvent:
    event_time_ms: int
    aggressor_side: Side
    price: Price
    size: Size


@dataclass(frozen=True, slots=True)
class FundingEvent:
    event_time_ms: int
    amount: Usd


@dataclass(frozen=True, slots=True)
class PaidActionEvent:
    event_time_ms: int
    cost: Usd


ReplayEvent = QuoteEvent | CancelEvent | TradeEvent | FundingEvent | PaidActionEvent


@dataclass(slots=True)
class ShadowReplayOrder:
    order_id: str
    side: Side
    price: Price
    remaining: Decimal
    queue_ahead: Decimal
    active_at_ms: int
    filled: Decimal = Decimal("0")
    cancelled: bool = False


@dataclass(frozen=True, slots=True)
class ReplayFill:
    fill_id: str
    order_id: str
    event_time_ms: int
    side: Side
    price: Price
    size: Size


@dataclass(frozen=True, slots=True)
class MarketMakerReplayResult:
    scenario: ReplayScenario
    fills: tuple[ReplayFill, ...]
    accounting_pnl: AccountingPnL
    execution_quality: ExecutionQuality
    shadow_orders: tuple[ShadowReplayOrder, ...]
    research_disclaimer: str = "Replay is a scenario model and does not prove live profitability."


class MarketMakerReplay:
    """Stable replay: equal timestamps retain caller order, making runs reproducible."""

    def __init__(self, *, maker_rebate_rate: Decimal = Decimal("0")) -> None:
        self._maker_rebate_rate = maker_rebate_rate

    def run(
        self,
        events: list[ReplayEvent] | tuple[ReplayEvent, ...],
        *,
        scenario: ReplayScenario,
        ending_mark_price: Price,
        assumption: ScenarioAssumption | None = None,
    ) -> MarketMakerReplayResult:
        model = assumption or DEFAULT_ASSUMPTIONS[scenario]
        ledger = AccountingLedger()
        orders: dict[str, ShadowReplayOrder] = {}
        fills: list[ReplayFill] = []
        queue_consumed = Decimal("0")
        partial_fills = 0
        indexed_events = enumerate(events)
        for _, event in sorted(indexed_events, key=lambda pair: (pair[1].event_time_ms, pair[0])):
            if isinstance(event, QuoteEvent):
                if event.order_id in orders and not orders[event.order_id].cancelled:
                    raise ValueError(f"duplicate live shadow order: {event.order_id}")
                orders[event.order_id] = ShadowReplayOrder(
                    order_id=event.order_id,
                    side=event.side,
                    price=event.price,
                    remaining=Decimal(event.size),
                    queue_ahead=Decimal(event.queue_ahead) * model.queue_multiplier,
                    active_at_ms=event.event_time_ms + model.latency_ms,
                )
            elif isinstance(event, CancelEvent):
                order = orders.get(event.order_id)
                if order is not None and event.event_time_ms >= order.active_at_ms:
                    order.cancelled = True
            elif isinstance(event, FundingEvent):
                ledger.record_funding(event.amount)
            elif isinstance(event, PaidActionEvent):
                ledger.record_paid_action(event.cost)
            else:
                available = Decimal(event.size)
                resting_side = Side.SELL if event.aggressor_side == Side.BUY else Side.BUY
                eligible = sorted(
                    (
                        order
                        for order in orders.values()
                        if not order.cancelled
                        and order.side == resting_side
                        and event.event_time_ms >= order.active_at_ms
                        and (
                            (order.side == Side.BUY and order.price >= event.price)
                            or (order.side == Side.SELL and order.price <= event.price)
                        )
                    ),
                    key=lambda order: (order.active_at_ms, order.order_id),
                )
                for order in eligible:
                    if available <= 0:
                        break
                    queued = min(order.queue_ahead, available)
                    order.queue_ahead -= queued
                    available -= queued
                    queue_consumed += queued
                    fill_size = min(order.remaining, available)
                    if fill_size <= 0:
                        continue
                    order.remaining -= fill_size
                    order.filled += fill_size
                    available -= fill_size
                    fill = ReplayFill(
                        fill_id=f"{order.order_id}:{len(fills) + 1}",
                        order_id=order.order_id,
                        event_time_ms=event.event_time_ms,
                        side=order.side,
                        price=order.price,
                        size=Size(fill_size),
                    )
                    fills.append(fill)
                    rebate = Decimal(order.price) * fill_size * self._maker_rebate_rate
                    ledger.record_fill(AccountingFill(order.side, order.price, Size(fill_size), Usd(rebate)))
                    if order.remaining > 0:
                        partial_fills += 1
        accounting = ledger.close(ending_mark_price)
        quality = ExecutionQuality(
            queue_ahead_consumed=Size(queue_consumed),
            fills=len(fills),
            partial_fills=partial_fills,
        )
        return MarketMakerReplayResult(
            scenario=scenario,
            fills=tuple(fills),
            accounting_pnl=accounting,
            execution_quality=quality,
            shadow_orders=tuple(orders[key] for key in sorted(orders)),
        )
