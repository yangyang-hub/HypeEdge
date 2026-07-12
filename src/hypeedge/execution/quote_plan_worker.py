"""Single-worker durable execution for market-maker quote-plan children."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

import structlog
from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hypeedge.core.enums import OrderStatus, OrderType, Side, TimeInForce
from hypeedge.core.models import Order, OrderIntent
from hypeedge.core.types import Cloid, Price, Size, StrategyId, SubAccount, Symbol
from hypeedge.execution.batch import ChildActionType, DispatchGuardContext, GuardDecision, evaluate_dispatch_guard
from hypeedge.risk.action_budget import ActionBudgetController, BudgetAction, NetworkAttemptDebit
from hypeedge.storage.market_making import MarketMakingTransactionRepository
from hypeedge.storage.postgres import (
    ExecutionCommandItemRecord,
    ExecutionCommandRecord,
    OrderRecord,
    QuotePlanItemRecord,
    QuotePlanRecord,
    QuoteSlotRecord,
    RiskReservationRecord,
    StrategyRuntimeStateRecord,
)

logger = structlog.get_logger(__name__)


class QuoteActionExecutor(Protocol):
    """The application injects ExecutionEngine, the sole NonceManager outlet."""

    async def submit_order(self, intent: OrderIntent, *, deferred: bool | None = None) -> Order: ...

    async def cancel_order(self, cloid: str) -> bool: ...


class QuoteDispatchGuardProvider(Protocol):
    async def context(self, child: QuoteDispatchChild) -> DispatchGuardContext: ...


class AppQuoteDispatchGuardProvider:
    """Rebuild every placement gate from current authoritative application state."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        runtime_snapshot: Callable[[StrategyId], Any | None],
        market_data: Any,
        account_health: Any,
        safety: Any,
        budget: ActionBudgetController,
        kill_switch_active: Callable[[], bool],
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._runtime_snapshot = runtime_snapshot
        self._market_data = market_data
        self._account_health = account_health
        self._safety = safety
        self._budget = budget
        self._kill_switch_active = kill_switch_active
        self._clock = clock or (lambda: datetime.now(UTC))

    async def context(self, child: QuoteDispatchChild) -> DispatchGuardContext:
        now = self._clock()
        active_session = ""
        active_config = -1
        active_revision = -1
        active_generation = -1
        lifecycle = False
        postgres_fresh = False
        reservation_valid = False
        try:
            runtime = self._runtime_snapshot(child.strategy_id)
            if runtime is not None:
                active_session = str(getattr(runtime, "session_id", ""))
                active_config = int(getattr(runtime, "config_version", -1) or -1)
                active_revision = int(getattr(runtime, "quote_revision", -1))
                lifecycle = getattr(getattr(runtime, "mode", None), "value", None) == "running"
            async with self._session_factory() as session:
                runtime_record = await session.scalar(
                    select(StrategyRuntimeStateRecord).where(
                        StrategyRuntimeStateRecord.strategy_id == str(child.strategy_id)
                    )
                )
                reservation = await session.scalar(
                    select(RiskReservationRecord).where(
                        RiskReservationRecord.command_item_id == child.item_id,
                        RiskReservationRecord.status == "active",
                        RiskReservationRecord.expires_at > now,
                    )
                )
                plan = await session.scalar(
                    select(QuotePlanRecord).where(QuotePlanRecord.plan_id == uuid.UUID(child.plan_id))
                )
                postgres_fresh = runtime_record is not None and plan is not None
                lifecycle = lifecycle and runtime_record is not None and runtime_record.actual_state == "running"
                reservation_valid = reservation is not None
        except Exception:
            logger.exception("quote_dispatch_postgres_guard_failed", item_id=child.item_id)

        book = self._market_data.get_book(child.symbol) if self._market_data is not None else None
        market_fresh = bool(book is not None and book.bids and book.asks and now >= book.received_at)
        if market_fresh and book is not None:
            active_generation = int(book.connection_generation)
            market_fresh = bool(book.version >= child.market_version and (now - book.received_at).total_seconds() <= 2)
        alo_valid = False
        if market_fresh and book is not None and child.price is not None:
            alo_valid = child.price < book.asks[0].price if child.side == Side.BUY else child.price > book.bids[0].price

        health = self._account_health.get_account_health(now=now)
        budget = self._budget.permission(
            BudgetAction.PLACE,
            strategy_id=child.strategy_id,
            symbol=child.symbol,
            child_actions=1,
            ip_weight=1,
        )
        safety_allows = getattr(getattr(self._safety, "mode", None), "value", None) == "normal"
        safety_allows = safety_allows and not self._kill_switch_active()
        return DispatchGuardContext(
            now=now,
            deadline=child.valid_until,
            expected_session_id=child.runtime_session_id,
            active_session_id=active_session,
            expected_config_version=child.config_version,
            active_config_version=active_config,
            expected_plan_revision=child.plan_revision,
            active_plan_revision=active_revision,
            expected_connection_generation=child.connection_generation,
            active_connection_generation=active_generation,
            market_fresh=market_fresh,
            account_fresh=(
                health.inventory.is_fresh and health.clearinghouse.is_fresh and health.reconciliation.is_fresh
            ),
            user_stream_fresh=health.user_stream.is_fresh,
            postgres_fresh=postgres_fresh,
            safety_allows_place=safety_allows,
            lifecycle_allows_place=lifecycle,
            budget_allows_place=budget.allowed,
            reservation_valid=reservation_valid,
            alo_valid=alo_valid,
        )


@dataclass(frozen=True, slots=True)
class QuoteDispatchChild:
    item_id: int
    command_id: str
    action: ChildActionType
    attempt: int
    plan_id: str
    strategy_id: StrategyId
    symbol: Symbol
    runtime_session_id: str
    config_version: int
    plan_revision: int
    market_version: int
    connection_generation: int
    valid_until: datetime
    source_cloid: str | None
    target_cloid: str | None
    side: Side
    level: int
    price: Price | None
    size: Size | None
    sub_account: SubAccount | None

    def request_payload(self) -> bytes:
        return json.dumps(
            {
                "action": self.action.value,
                "source_cloid": self.source_cloid,
                "target_cloid": self.target_cloid,
                "symbol": str(self.symbol),
                "side": self.side.value,
                "price": str(self.price) if self.price is not None else None,
                "size": str(self.size) if self.size is not None else None,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()


class QuotePlanWorker:
    """Claim children with SKIP LOCKED; never retry ambiguous children.

    A replacement placement is deliberately not claimable until the cancel
    child for the same plan item is durably ``succeeded``.  Rejected, blocked,
    expired, late and UNKNOWN cancels therefore cannot leak a replacement.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        executor: QuoteActionExecutor,
        guards: QuoteDispatchGuardProvider,
        budget: ActionBudgetController,
        *,
        worker_id: str = "quote-plan-worker",
        poll_interval_seconds: float = 0.05,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._executor = executor
        self._guards = guards
        self._budget = budget
        self._worker_id = worker_id
        self._poll_interval = poll_interval_seconds
        self._clock = clock or (lambda: datetime.now(UTC))
        self._stopped = asyncio.Event()

    async def run(self) -> None:
        self._stopped.clear()
        while not self._stopped.is_set():
            child = await self.claim_one()
            if child is None:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stopped.wait(), timeout=self._poll_interval)
                continue
            await self.dispatch(child)

    async def stop(self) -> None:
        self._stopped.set()

    async def claim_one(self) -> QuoteDispatchChild | None:
        async with self._session_factory() as session, session.begin():
            sibling_cancel = ExecutionCommandItemRecord.__table__.alias("sibling_cancel")
            statement = (
                select(
                    ExecutionCommandItemRecord,
                    ExecutionCommandRecord,
                    QuotePlanItemRecord,
                    QuotePlanRecord,
                    OrderRecord,
                )
                .join(
                    ExecutionCommandRecord,
                    ExecutionCommandRecord.command_id == ExecutionCommandItemRecord.command_id,
                )
                .join(QuotePlanItemRecord, QuotePlanItemRecord.id == ExecutionCommandItemRecord.plan_item_id)
                .join(QuotePlanRecord, QuotePlanRecord.plan_id == QuotePlanItemRecord.plan_id)
                .outerjoin(OrderRecord, OrderRecord.order_id == QuotePlanItemRecord.target_order_id)
                .where(
                    ExecutionCommandItemRecord.status == "pending",
                    ExecutionCommandItemRecord.available_at <= self._clock(),
                    ExecutionCommandRecord.command_type == "quote_plan",
                    ExecutionCommandRecord.status.in_(("pending", "processing")),
                    ~exists(
                        select(sibling_cancel.c.id).where(
                            sibling_cancel.c.plan_item_id == ExecutionCommandItemRecord.plan_item_id,
                            sibling_cancel.c.action_type == "cancel",
                            sibling_cancel.c.id != ExecutionCommandItemRecord.id,
                            sibling_cancel.c.status != "succeeded",
                        )
                    ),
                )
                .order_by(ExecutionCommandItemRecord.command_id, ExecutionCommandItemRecord.ordinal)
                .limit(1)
                .with_for_update(of=ExecutionCommandItemRecord, skip_locked=True)
            )
            row = (await session.execute(statement)).one_or_none()
            if row is None:
                return None
            item, command, plan_item, plan, target_order = row
            item.status = "processing"
            item.locked_at = self._clock()
            item.locked_by = self._worker_id
            item.attempt_count += 1
            command.status = "processing"
            command.locked_at = self._clock()
            command.locked_by = self._worker_id
            payload = dict(command.payload)
            return QuoteDispatchChild(
                item_id=item.id,
                command_id=str(command.command_id),
                action=ChildActionType(item.action_type),
                attempt=item.attempt_count,
                plan_id=str(plan.plan_id),
                strategy_id=StrategyId(plan.strategy_id),
                symbol=Symbol(plan_item.symbol),
                runtime_session_id=str(payload["runtime_session_id"]),
                config_version=int(payload["config_version"]),
                plan_revision=plan.revision,
                market_version=plan.market_version,
                connection_generation=int(payload["connection_generation"]),
                valid_until=plan.valid_until,
                source_cloid=plan_item.source_cloid,
                target_cloid=plan_item.target_cloid,
                side=Side(plan_item.side),
                level=plan_item.level,
                price=Price(plan_item.desired_price) if plan_item.desired_price is not None else None,
                size=Size(plan_item.desired_size) if plan_item.desired_size is not None else None,
                sub_account=(
                    SubAccount(target_order.sub_account)
                    if target_order and target_order.sub_account
                    else SubAccount(str(payload["sub_account"]))
                    if payload.get("sub_account")
                    else None
                ),
            )

    async def dispatch(self, child: QuoteDispatchChild) -> None:
        if child.action == ChildActionType.PLACE:
            decision = evaluate_dispatch_guard(child.action, await self._guards.context(child))
            if decision != GuardDecision.ALLOW:
                await self._finish_without_send(child, decision)
                return
        sent_at = self._clock()
        request_hash = hashlib.sha256(child.request_payload()).hexdigest()
        outcome = "unknown"
        status = "unknown"
        resolution: str | None = None
        try:
            if child.action == ChildActionType.CANCEL:
                if child.source_cloid is None:
                    outcome, status, resolution = "rejected", "failed", "missing_source_cloid"
                elif await self._executor.cancel_order(child.source_cloid):
                    outcome, status = "succeeded", "succeeded"
                else:
                    resolution = "cancel_result_not_authoritative"
            else:
                order = await self._executor.submit_order(self._intent(child), deferred=False)
                outcome, status, resolution = self._placement_outcome(order)
        except TimeoutError as exc:
            resolution = str(exc) or "network_timeout"
        except Exception as exc:  # the request may have crossed the transport boundary
            logger.exception("quote_child_dispatch_failed", item_id=child.item_id)
            outcome, status, resolution = "transport_error", "unknown", str(exc)

        responded_at = self._clock()
        inserted = await self._record_attempt(
            child,
            request_hash=request_hash,
            sent_at=sent_at,
            responded_at=responded_at,
            outcome=outcome,
            status=status,
            resolution=resolution,
        )
        if inserted:
            self._budget.debit_network_attempt(
                NetworkAttemptDebit(
                    attempt_id=f"quote-item:{child.item_id}:{child.attempt}",
                    child_actions=(BudgetAction(child.action.value),),
                    ip_weight=1,
                    occurred_at=sent_at,
                    strategy_id=child.strategy_id,
                    symbol=child.symbol,
                )
            )

    async def _record_attempt(
        self,
        child: QuoteDispatchChild,
        *,
        request_hash: str,
        sent_at: datetime,
        responded_at: datetime,
        outcome: str,
        status: str,
        resolution: str | None,
    ) -> bool:
        async with self._session_factory() as session, session.begin():
            item = await session.scalar(
                select(ExecutionCommandItemRecord)
                .where(ExecutionCommandItemRecord.id == child.item_id)
                .with_for_update()
            )
            if item is None:
                return False
            repository = MarketMakingTransactionRepository(session)
            from hypeedge.storage.postgres import ExecutionActionRecord

            inserted = await repository.append_execution_action(
                ExecutionActionRecord(
                    command_item_id=child.item_id,
                    attempt=child.attempt,
                    action_type=child.action.value,
                    request_hash=request_hash,
                    sent_at=sent_at,
                    responded_at=responded_at,
                    outcome=outcome,
                    estimated_credit_cost=1,
                )
            )
            item.status = status
            item.resolution = resolution
            item.completed_at = responded_at if status not in {"pending", "processing", "unknown"} else None
            item.locked_at = None
            item.locked_by = None
            if child.action == ChildActionType.PLACE:
                order_record = await session.scalar(select(OrderRecord).where(OrderRecord.cloid == child.target_cloid))
                plan_item = await session.scalar(
                    select(QuotePlanItemRecord).where(QuotePlanItemRecord.id == item.plan_item_id).with_for_update()
                )
                if order_record is not None and plan_item is not None:
                    plan_item.target_order_id = order_record.order_id
                    item.target_order_id = order_record.order_id
                    slot = await session.scalar(
                        select(QuoteSlotRecord)
                        .where(
                            QuoteSlotRecord.strategy_id == str(child.strategy_id),
                            QuoteSlotRecord.symbol == str(child.symbol),
                            QuoteSlotRecord.side == child.side.value,
                            QuoteSlotRecord.level == child.level,
                        )
                        .with_for_update()
                    )
                    if slot is not None and slot.plan_revision == child.plan_revision:
                        slot.owner_order_id = order_record.order_id
                        slot.state = "unknown" if status == "unknown" else "live" if status == "succeeded" else "empty"
                        slot.revision += 1
                        slot.updated_at = responded_at
                reservation = await session.scalar(
                    select(RiskReservationRecord).where(RiskReservationRecord.command_item_id == child.item_id)
                )
                if reservation is not None and order_record is not None:
                    reservation.order_id = order_record.order_id
                if reservation is not None and status != "pending":
                    reservation.status = "consumed" if status in {"succeeded", "unknown"} else "released"
                    reservation.released_at = responded_at if reservation.status == "released" else None
            await self._finish_parent_if_terminal(session, item.command_id, responded_at)
            return inserted

    async def _finish_without_send(self, child: QuoteDispatchChild, decision: GuardDecision) -> None:
        status = {
            GuardDecision.SUPERSEDED: "superseded",
            GuardDecision.EXPIRED: "expired",
            GuardDecision.BLOCKED: "blocked",
        }[decision]
        async with self._session_factory() as session, session.begin():
            item = await session.scalar(
                select(ExecutionCommandItemRecord)
                .where(ExecutionCommandItemRecord.id == child.item_id)
                .with_for_update()
            )
            if item is None or item.status != "processing":
                return
            item.status = status
            item.resolution = f"dispatch_guard_{decision.value}"
            item.completed_at = self._clock()
            item.locked_at = None
            item.locked_by = None
            reservation = await session.scalar(
                select(RiskReservationRecord).where(RiskReservationRecord.command_item_id == child.item_id)
            )
            if reservation is not None:
                reservation.status = "released"
                reservation.released_at = self._clock()
            await self._finish_parent_if_terminal(session, item.command_id, self._clock())

    @staticmethod
    async def _finish_parent_if_terminal(session: AsyncSession, command_id: object, completed_at: datetime) -> None:
        statuses = (
            (
                await session.execute(
                    select(ExecutionCommandItemRecord.status).where(ExecutionCommandItemRecord.command_id == command_id)
                )
            )
            .scalars()
            .all()
        )
        if any(status in {"pending", "processing"} for status in statuses):
            return
        command = await session.scalar(
            select(ExecutionCommandRecord).where(ExecutionCommandRecord.command_id == command_id).with_for_update()
        )
        if command is None:
            return
        if any(status == "unknown" for status in statuses):
            command.status = "unknown"
        elif statuses and all(status == "succeeded" for status in statuses):
            command.status = "succeeded"
        else:
            command.status = "failed"
        command.completed_at = completed_at
        command.locked_at = None
        command.locked_by = None

    @staticmethod
    def _intent(child: QuoteDispatchChild) -> OrderIntent:
        if child.target_cloid is None or child.price is None or child.size is None:
            raise ValueError("placement child lacks a complete durable order")
        return OrderIntent(
            symbol=child.symbol,
            side=child.side,
            size=child.size,
            price=child.price,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.ALO,
            strategy_id=child.strategy_id,
            sub_account=child.sub_account,
            cloid=Cloid(child.target_cloid),
        )

    @staticmethod
    def _placement_outcome(order: Order) -> tuple[str, str, str | None]:
        if order.status in {OrderStatus.ACKNOWLEDGED, OrderStatus.PARTIAL_FILL, OrderStatus.FILLED}:
            return "succeeded", "succeeded", None
        if order.status == OrderStatus.SUBMIT_UNKNOWN:
            return "unknown", "unknown", order.error_message
        if order.status in {OrderStatus.REJECTED, OrderStatus.CANCELLED, OrderStatus.EXPIRED}:
            return "rejected", "failed", order.error_message
        return "unknown", "unknown", f"non_authoritative_order_status:{order.status.value}"
