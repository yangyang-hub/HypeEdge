"""Execution engine — implements ExecutionClient, the sole order submission outlet.

Design doc §9: "The execution module is the sole signing outlet, responsible
for nonce serialization, cloid generation, order submission/cancel/replace,
retries."

This module wraps the HL SDK's Exchange class through NonceManager,
adding order state tracking, kill switch integration, cloid idempotency,
and EventBus event publishing.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol

import structlog

from hypeedge.core.enums import OrderStatus, OrderType, Side, TimeInForce
from hypeedge.core.events import (
    EVENT_ORDER_ACKNOWLEDGED,
    EVENT_ORDER_CANCELLED,
    EVENT_ORDER_FILLED,
    EVENT_ORDER_REJECTED,
    EVENT_ORDER_SUBMITTED,
    Event,
    EventBus,
)
from hypeedge.core.exceptions import ExecutionError, KillSwitchTriggeredError, OrderRejectedError, OrderTimeoutError
from hypeedge.core.models import Fill, Order, OrderIntent, RiskCheckResult
from hypeedge.core.types import Cloid, OrderId, Price, Size, SubAccount, Timestamp, Usd
from hypeedge.execution.cloid import CloidGenerator
from hypeedge.execution.order_state import OrderStateMachine

if TYPE_CHECKING:
    from hypeedge.account.tracker import AccountTracker
    from hypeedge.execution.durable import DurableExecutionCommand, DurableOrderStore
    from hypeedge.execution.nonce import NonceManager
    from hypeedge.execution.normalizer import OrderNormalizer
    from hypeedge.market_data.provider import MarketDataProvider
    from hypeedge.market_data.rate_limiter import RateLimiter
    from hypeedge.risk.checker import RiskChecker
    from hypeedge.risk.kill_switch import KillSwitch
    from hypeedge.risk.safety import SafetyController

logger = structlog.get_logger(__name__)

# Map our TimeInForce to HL SDK format
_TIF_MAP: dict[str, str] = {
    TimeInForce.GTC: "Gtc",
    TimeInForce.IOC: "Ioc",
    TimeInForce.ALO: "Alo",
    TimeInForce.GTX: "Gtx",
}


class ExecutionClient(Protocol):
    """Interface for strategies to submit orders (design doc §9).

    Strategies use this client — they never access the ExecutionEngine directly.
    """

    async def submit_order(self, intent: OrderIntent, *, deferred: bool | None = None) -> Order:
        """Submit an order intent. Returns the created Order."""
        ...

    async def cancel_order(self, cloid: str) -> bool:
        """Cancel an order by cloid. Returns True if cancellation was accepted."""
        ...

    async def cancel_all_orders(self, symbol: str | None = None) -> int:
        """Cancel all open orders, optionally filtered by symbol. Returns count cancelled."""
        ...

    async def get_order(self, cloid: str) -> Order | None:
        """Get current order state by cloid."""
        ...

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Get all open (non-terminal) orders."""
        ...


class ExecutionEngine:
    """Real execution engine that submits orders to Hyperliquid.

    Implements the ExecutionClient protocol. All order mutations flow
    through NonceManager for serial signing. Kill switch is checked
    before every order submission.
    """

    def __init__(
        self,
        nonce_manager: NonceManager,
        event_bus: EventBus,
        kill_switch: KillSwitch,
        order_state_machine: OrderStateMachine | None = None,
        account_address: str = "",
        rate_limiter: RateLimiter | None = None,
        risk_checker: RiskChecker | None = None,
        safety_controller: SafetyController | None = None,
        account_tracker: AccountTracker | None = None,
        durable_store: DurableOrderStore | None = None,
        deferred_execution: bool = False,
        market_data_provider: MarketDataProvider | None = None,
        market_price_stale_seconds: float = 5.0,
        durable_kill_trigger: Callable[[str], Awaitable[bool]] | None = None,
        order_normalizer: OrderNormalizer | None = None,
    ) -> None:
        self._nonce = nonce_manager
        self._event_bus = event_bus
        self._kill_switch = kill_switch
        self._state_machine = order_state_machine or OrderStateMachine()
        self._account_address = account_address
        self._rate_limiter = rate_limiter
        self._risk_checker = risk_checker
        self._safety = safety_controller
        self._tracker = account_tracker
        self._durable_store = durable_store
        self._deferred_execution = deferred_execution
        self._market_data_provider = market_data_provider
        self._market_price_stale_seconds = market_price_stale_seconds
        self._durable_kill_trigger = durable_kill_trigger
        self._order_normalizer = order_normalizer
        self._orders: dict[Cloid, Order] = {}

    async def submit_order(self, intent: OrderIntent, *, deferred: bool | None = None) -> Order:
        """Submit an order intent to the exchange.

        Flow (design doc §9.1, §9.4):
        1. Kill switch check
        2. Generate cloid if not provided
        3. Create Order in PENDING state
        4. Build SDK order request
        5. Submit through NonceManager (serial signing)
        6. Handle response → ACKNOWLEDGED or REJECTED
        7. Publish lifecycle events
        """
        deferred_execution = self._deferred_execution if deferred is None else deferred
        if self._order_normalizer is not None:
            best_bid: Price | None = None
            best_ask: Price | None = None
            if self._market_data_provider is not None:
                book = self._market_data_provider.get_book(intent.symbol)
                if book is not None:
                    best_bid = book.bids[0].price if book.bids else None
                    best_ask = book.asks[0].price if book.asks else None
            intent = self._order_normalizer.normalize(intent, best_bid=best_bid, best_ask=best_ask)

        raw_cloid = intent.cloid or CloidGenerator.generate(intent.strategy_id)
        cloid = Cloid(CloidGenerator.to_hl_cloid(raw_cloid))
        intent = OrderIntent(
            symbol=intent.symbol,
            side=intent.side,
            size=intent.size,
            price=intent.price,
            order_type=intent.order_type,
            time_in_force=intent.time_in_force,
            strategy_id=intent.strategy_id,
            sub_account=intent.sub_account
            or (SubAccount(self._account_address.lower()) if self._account_address else None),
            reduce_only=intent.reduce_only,
            cloid=cloid,
            client_id=intent.client_id,
        )

        # Idempotency precedes every new-placement gate: replaying an already
        # accepted command must return its original result without another risk
        # reservation or exchange mutation. Durable lookup covers restarts.
        existing = self._orders.get(cloid)
        if existing is None and self._durable_store is not None:
            existing = await self._durable_store.get_order(str(cloid))
            if existing is not None:
                self._orders[existing.cloid] = existing
        if existing is not None:
            if self._matches_intent(existing, intent):
                logger.info("order_idempotent_replay", cloid=str(cloid), status=existing.status.value)
                return existing
            raise OrderRejectedError(
                f"Cloid {cloid} is already bound to a different order",
                cloid=str(cloid),
                reason="cloid_payload_conflict",
            )

        # New-placement gates run only after canonical cloid deduplication.
        try:
            self._kill_switch.check()
            if self._safety is not None:
                self._safety.check_placement(intent)
        except (KillSwitchTriggeredError, OrderRejectedError) as exc:
            if not deferred_execution or self._durable_store is None:
                raise
            reason = getattr(exc, "context", {}).get("reason") or str(exc)
            order = self._rejected_order(intent, str(reason))
            await self._persist_placement(order, RiskCheckResult(False, str(reason)), dispatch=False)
            return order

        # Every placement, including API/manual placement, passes the same risk gate.
        reference_price: float | None = float(intent.price) if intent.price is not None else None
        price_observed_at: datetime | None = None
        if reference_price is None and self._market_data_provider is not None:
            snapshot = self._market_data_provider.get_price_snapshot(intent.symbol)
            if snapshot is not None:
                reference_price = snapshot.price
                price_observed_at = snapshot.observed_at
        if (
            reference_price is not None
            and price_observed_at is not None
            and (datetime.now(UTC) - price_observed_at).total_seconds() > self._market_price_stale_seconds
        ):
            risk_result = RiskCheckResult(False, "market_price_stale", ["market_price_fresh"])
            order = self._rejected_order(intent, "market_price_stale")
            await self._persist_placement(
                order,
                risk_result,
                dispatch=False,
                reference_price=reference_price,
                price_observed_at=price_observed_at,
            )
            return order

        risk_result = RiskCheckResult(passed=True)
        if self._risk_checker is not None:
            risk_result = await self._risk_checker.check(intent, reference_price=reference_price)
            if not risk_result.passed:
                await self._handle_risk_rejection(risk_result.reason)
                order = self._rejected_order(intent, risk_result.reason or "risk_check_rejected")
                await self._persist_placement(
                    order,
                    risk_result,
                    dispatch=False,
                    reference_price=reference_price,
                    price_observed_at=price_observed_at,
                )
                return order

        # 1b. Action credit check (design doc §3.2, §8.1)
        if self._rate_limiter and not self._rate_limiter.check_action_credits():
            logger.warning("order_rejected_action_credits_low", cloid=str(intent.cloid))
            risk_result = RiskCheckResult(passed=False, reason="action_credits_below_threshold")
            order = self._rejected_order(intent, "action_credits_below_threshold")
            await self._persist_placement(
                order,
                risk_result,
                dispatch=False,
                reference_price=reference_price,
                price_observed_at=price_observed_at,
            )
            return order

        # 3. Create local Order
        order = Order(
            cloid=cloid,
            symbol=intent.symbol,
            side=intent.side,
            size=intent.size,
            price=intent.price,
            order_type=intent.order_type,
            time_in_force=intent.time_in_force,
            status=OrderStatus.PENDING,
            strategy_id=intent.strategy_id,
            sub_account=intent.sub_account,
            reduce_only=intent.reduce_only,
            created_at=datetime.now(UTC),
        )
        self._orders[cloid] = order

        # 4. Transition to SUBMITTED
        self._state_machine.transition(order, OrderStatus.SUBMITTED, reason="submit_order")
        order.submitted_at = datetime.now(UTC)
        command_id = uuid.uuid4()
        durable_risk = await self._persist_placement(
            order,
            risk_result,
            command_id=command_id,
            dispatch=True,
            reference_price=reference_price,
            price_observed_at=price_observed_at,
        )
        if durable_risk is not None and not durable_risk.passed:
            order.status = OrderStatus.REJECTED
            order.error_message = durable_risk.reason
            self._event_bus.publish_sync(
                Event(event_type=EVENT_ORDER_REJECTED, payload=order, correlation_id=str(cloid))
            )
            return order

        self._event_bus.publish_sync(Event(event_type=EVENT_ORDER_SUBMITTED, payload=order, correlation_id=str(cloid)))

        logger.info(
            "order_submitting",
            cloid=str(cloid),
            symbol=str(intent.symbol),
            side=str(intent.side),
            size=float(intent.size),
            price=float(intent.price) if intent.price else None,
            order_type=str(intent.order_type),
        )

        if deferred_execution:
            return order

        # 5. Build SDK request and submit through NonceManager
        try:
            sdk_result = await self._submit_to_exchange(intent)
        except (KillSwitchTriggeredError, OrderRejectedError) as e:
            self._state_machine.transition(order, OrderStatus.CANCELLED, reason="dispatch_aborted_by_safety_gate")
            order.error_message = str(e)
            await self._persist_transition(order, "dispatch_aborted", command_id=command_id, command_status="cancelled")
            self._event_bus.publish_sync(
                Event(event_type=EVENT_ORDER_CANCELLED, payload=order, correlation_id=str(cloid))
            )
            return order
        except OrderTimeoutError as e:
            self._state_machine.transition(order, OrderStatus.SUBMIT_UNKNOWN, reason=str(e))
            order.error_message = str(e)
            await self._persist_transition(order, "submit_unknown", command_id=command_id, command_status="unknown")
            logger.error("order_submit_unknown", cloid=str(cloid), error=str(e))
            return order
        except Exception as e:
            self._state_machine.transition(order, OrderStatus.REJECTED, reason=str(e))
            order.error_message = str(e)
            await self._persist_transition(order, "rejected", command_id=command_id, command_status="failed")
            self._event_bus.publish_sync(
                Event(event_type=EVENT_ORDER_REJECTED, payload=order, correlation_id=str(cloid))
            )
            logger.error("order_rejected", cloid=str(cloid), error=str(e))
            return order

        # 6. Process response
        await self._handle_submit_response(order, sdk_result, command_id=command_id)
        return order

    async def execute_durable_command(
        self,
        command: DurableExecutionCommand,
        *,
        after_send_hook: Callable[[DurableExecutionCommand], None] | None = None,
    ) -> bool:
        """Execute or resolve a command claimed by the sole durable worker."""
        cloid = str(command.payload.get("cloid", ""))
        if not cloid or self._durable_store is None:
            raise ExecutionError("Durable command is missing its order cloid/store")
        order = await self._durable_store.get_order(cloid)
        if order is None:
            raise ExecutionError(f"Durable order not found for command {command.command_id}")
        self._orders[order.cloid] = order
        if order.is_terminal:
            return True

        if command.requires_resolution:
            response = await self._nonce.query_order_status(cloid)
            if response is None:
                if order.status != OrderStatus.SUBMIT_UNKNOWN:
                    self._state_machine.transition(order, OrderStatus.SUBMIT_UNKNOWN, reason="lease_recovery_unknown")
                order.error_message = "exchange_order_not_found_after_ambiguous_submission"
                await self._persist_transition(
                    order,
                    "submit_unknown",
                    command_id=command.command_id,
                    command_status="unknown",
                )
                return False
            await self._handle_submit_response(order, response, command_id=command.command_id)
            return order.status != OrderStatus.SUBMIT_UNKNOWN

        intent = self._intent_from_order(order)
        try:
            self._kill_switch.check()
            if self._safety is not None:
                self._safety.check_placement(intent)
            if self._rate_limiter is not None and not self._rate_limiter.check_action_credits():
                raise OrderRejectedError(
                    "Action credits are stale or below the low watermark",
                    cloid=cloid,
                    reason="action_credits_below_threshold",
                )
        except (KillSwitchTriggeredError, OrderRejectedError) as exc:
            self._state_machine.transition(order, OrderStatus.CANCELLED, reason="dispatch_aborted_by_safety_gate")
            order.error_message = str(exc)
            await self._persist_transition(
                order,
                "dispatch_aborted",
                command_id=command.command_id,
                command_status="cancelled",
            )
            self._event_bus.publish_sync(Event(event_type=EVENT_ORDER_CANCELLED, payload=order, correlation_id=cloid))
            return True
        try:
            response = await self._submit_to_exchange(intent)
        except (KillSwitchTriggeredError, OrderRejectedError) as exc:
            self._state_machine.transition(order, OrderStatus.CANCELLED, reason="dispatch_aborted_by_safety_gate")
            order.error_message = str(exc)
            await self._persist_transition(
                order,
                "dispatch_aborted",
                command_id=command.command_id,
                command_status="cancelled",
            )
            self._event_bus.publish_sync(Event(event_type=EVENT_ORDER_CANCELLED, payload=order, correlation_id=cloid))
            return True
        except OrderTimeoutError as exc:
            self._state_machine.transition(order, OrderStatus.SUBMIT_UNKNOWN, reason=str(exc))
            order.error_message = str(exc)
            await self._persist_transition(
                order,
                "submit_unknown",
                command_id=command.command_id,
                command_status="unknown",
            )
            return False
        except Exception as exc:
            self._state_machine.transition(order, OrderStatus.REJECTED, reason=str(exc))
            order.error_message = str(exc)
            await self._persist_transition(order, "rejected", command_id=command.command_id, command_status="failed")
            return True
        if after_send_hook is not None:
            after_send_hook(command)
        await self._handle_submit_response(order, response, command_id=command.command_id)
        return order.status != OrderStatus.SUBMIT_UNKNOWN

    async def execute_durable_cancel_command(self, command: DurableExecutionCommand) -> bool:
        """Recover a cancel command whose request owner crashed.

        Cancellation is idempotent, but a recovered attempt first queries
        exchange truth so already-terminal orders are never misreported.
        """
        cloid = str(command.payload.get("cloid", ""))
        if not cloid or self._durable_store is None:
            raise ExecutionError("Durable cancel command is missing its order cloid/store")
        order = await self._durable_store.get_order(cloid)
        if order is None:
            raise ExecutionError(f"Durable cancel order not found for command {command.command_id}")
        self._orders[order.cloid] = order
        if order.is_terminal:
            await self._persist_transition(
                order,
                "cancel_recovered_terminal",
                command_id=command.command_id,
                command_status="succeeded" if order.status == OrderStatus.CANCELLED else "failed",
            )
            return True

        if command.requires_resolution:
            status_response = await self._nonce.query_order_status(cloid)
            if status_response is None:
                await self._mark_cancel_unknown(
                    order,
                    "cancel_recovery_status_unknown",
                    command_id=command.command_id,
                )
                return False
            order_data = status_response.get("order", {}) if isinstance(status_response, dict) else {}
            exchange_status = str(order_data.get("status", "")).lower() if isinstance(order_data, dict) else ""
            if exchange_status not in {"open", "resting", "triggered"}:
                await self._handle_cancel_response(order, status_response, command_id=command.command_id)
                return order.status != OrderStatus.CANCEL_UNKNOWN

        exchange = self._nonce.exchange
        if exchange is None:
            raise ExecutionError("No Exchange instance available")
        try:
            response = await self._nonce.submit(
                exchange.cancel_by_cloid,
                str(order.symbol),
                self._to_sdk_cloid(order.cloid),
                cloid_hint=str(order.cloid),
            )
        except OrderTimeoutError as exc:
            await self._mark_cancel_unknown(order, str(exc), command_id=command.command_id)
            return False
        return await self._handle_cancel_response(order, response, command_id=command.command_id)

    @staticmethod
    def _intent_from_order(order: Order) -> OrderIntent:
        return OrderIntent(
            symbol=order.symbol,
            side=order.side,
            size=order.size,
            price=order.price,
            order_type=order.order_type,
            time_in_force=order.time_in_force,
            strategy_id=order.strategy_id,
            sub_account=order.sub_account,
            reduce_only=order.reduce_only,
            cloid=order.cloid,
        )

    async def cancel_order(self, cloid: str) -> bool:
        """Cancel an order by cloid.

        A transport-level return is not proof that the order was cancelled.  We
        only enter ``CANCELLED`` after parsing an exchange success/status
        response.  Timeout, an open status after a timeout lookup, or an
        unrecognised response is retained as ``CANCEL_UNKNOWN`` for
        reconciliation.
        """
        c = Cloid(cloid)
        order = self._orders.get(c)
        if order is None:
            logger.warning("cancel_order_not_found", cloid=cloid)
            return False

        if self._state_machine.is_terminal(order):
            logger.warning("cancel_order_already_terminal", cloid=cloid, status=str(order.status))
            return False

        exchange = self._nonce.exchange
        if exchange is None:
            raise ExecutionError("No Exchange instance available")

        command_id = uuid.uuid4()
        if self._durable_store is not None:
            await self._durable_store.persist_cancel_requested(order, command_id=command_id)

        try:
            response = await self._nonce.submit(
                exchange.cancel_by_cloid,
                str(order.symbol),
                self._to_sdk_cloid(c),
                cloid_hint=str(c),
            )
            return await self._handle_cancel_response(order, response, command_id=command_id)
        except OrderTimeoutError as e:
            await self._mark_cancel_unknown(order, str(e), command_id=command_id)
            logger.warning("cancel_order_unknown", cloid=cloid, error=str(e))
            return False
        except Exception as e:
            order.error_message = str(e)
            await self._persist_transition(order, "cancel_failed", command_id=command_id, command_status="failed")
            logger.error("cancel_order_failed", cloid=cloid, error=str(e))
            raise ExecutionError(f"Cancel execution failed for cloid={cloid}") from e

    async def cancel_all_orders(self, symbol: str | None = None) -> int:
        """Cancel all open orders, optionally filtered by symbol."""
        to_cancel = []
        for cloid, order in self._orders.items():
            if self._state_machine.is_terminal(order):
                continue
            if symbol is not None and str(order.symbol) != symbol:
                continue
            to_cancel.append(str(cloid))

        cancelled = 0
        for cloid_str in to_cancel:
            if await self.cancel_order(cloid_str):
                cancelled += 1

        return cancelled

    async def _handle_cancel_response(self, order: Order, response: Any, *, command_id: uuid.UUID) -> bool:
        """Apply only exchange-authoritative cancellation outcomes."""
        cloid = str(order.cloid)
        if not isinstance(response, dict):
            await self._mark_cancel_unknown(order, "invalid_cancel_response", command_id=command_id)
            return False

        top_status = str(response.get("status", "")).lower()
        if top_status == "order":
            order_data = response.get("order", {})
            exchange_status = str(order_data.get("status", "")).lower() if isinstance(order_data, dict) else ""
            if exchange_status in {"canceled", "cancelled"}:
                return await self._mark_cancelled(order, "cancel_status_confirmed", command_id=command_id)
            if exchange_status == "filled":
                self._state_machine.transition(order, OrderStatus.FILLED, reason="cancel_status_filled")
                order.filled_at = datetime.now(UTC)
                order.error_message = "cancel_not_applied_order_filled"
                await self._persist_transition(order, "filled", command_id=command_id, command_status="failed")
                logger.warning("cancel_order_already_filled", cloid=cloid)
                return False
            if exchange_status in {"rejected", "margincanceled"}:
                self._state_machine.transition(order, OrderStatus.REJECTED, reason="cancel_status_rejected")
                order.error_message = "cancel_not_applied_order_rejected"
                await self._persist_transition(order, "rejected", command_id=command_id, command_status="failed")
                return False
            # Most commonly this is ``open`` returned by NonceManager's
            # post-timeout lookup. The original cancel may still arrive later.
            await self._mark_cancel_unknown(
                order,
                f"cancel_status_{exchange_status or 'unknown'}",
                command_id=command_id,
            )
            return False

        if top_status == "ok":
            response_data = response.get("response", {})
            data = response_data.get("data", {}) if isinstance(response_data, dict) else {}
            statuses = data.get("statuses", []) if isinstance(data, dict) else []
            first = statuses[0] if isinstance(statuses, list) and statuses else None
            if isinstance(first, str) and first.lower() == "success":
                return await self._mark_cancelled(order, "cancel_exchange_success", command_id=command_id)
            if isinstance(first, dict) and "error" in first:
                order.error_message = str(first["error"])
                await self._persist_transition(order, "cancel_failed", command_id=command_id, command_status="failed")
                logger.warning("cancel_order_rejected", cloid=cloid, error=order.error_message)
                return False

        if top_status == "err":
            order.error_message = str(response.get("response", "cancel_rejected"))
            await self._persist_transition(order, "cancel_failed", command_id=command_id, command_status="failed")
            logger.warning("cancel_order_rejected", cloid=cloid, error=order.error_message)
            return False

        await self._mark_cancel_unknown(order, "unknown_cancel_response", command_id=command_id)
        logger.warning("cancel_order_unknown_response", cloid=cloid, response=response)
        return False

    async def _mark_cancelled(self, order: Order, reason: str, *, command_id: uuid.UUID) -> bool:
        self._state_machine.transition(order, OrderStatus.CANCELLED, reason=reason)
        order.error_message = None
        await self._persist_transition(order, "cancelled", command_id=command_id, command_status="succeeded")
        self._event_bus.publish_sync(
            Event(event_type=EVENT_ORDER_CANCELLED, payload=order, correlation_id=str(order.cloid))
        )
        logger.info("order_cancelled", cloid=str(order.cloid), reason=reason)
        return True

    async def _mark_cancel_unknown(self, order: Order, reason: str, *, command_id: uuid.UUID) -> None:
        if order.status != OrderStatus.CANCEL_UNKNOWN:
            self._state_machine.transition(order, OrderStatus.CANCEL_UNKNOWN, reason=reason)
        order.error_message = reason
        await self._persist_transition(order, "cancel_unknown", command_id=command_id, command_status="unknown")

    async def get_order(self, cloid: str) -> Order | None:
        return self._orders.get(Cloid(cloid))

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        result = []
        for order in self._orders.values():
            if self._state_machine.is_terminal(order):
                continue
            if symbol is not None and str(order.symbol) != symbol:
                continue
            result.append(order)
        return result

    def import_exchange_order(self, order: Order) -> None:
        """Import an exchange-authoritative order discovered by reconciliation."""
        self._orders[order.cloid] = order

    async def import_exchange_order_authoritative(self, order: Order) -> None:
        """Durably import exchange truth before it can be cancelled locally."""
        if self._durable_store is not None:
            await self._durable_store.persist_reconciled_order(order)
        self.import_exchange_order(order)

    async def recover_open_orders(self) -> int:
        """Restore non-terminal local orders before startup reconciliation."""
        if self._durable_store is None:
            return 0
        orders = await self._durable_store.load_open_orders()
        for order in orders:
            self._orders[order.cloid] = order
        logger.info("execution_orders_recovered", count=len(orders))
        return len(orders)

    async def refresh_order_from_durable(self, cloid: str) -> Order | None:
        """Refresh one committed exchange projection into process memory."""
        if self._durable_store is None:
            return self._orders.get(Cloid(cloid))
        order = await self._durable_store.get_order(cloid)
        if order is not None:
            self._orders[order.cloid] = order
        return order

    async def _submit_to_exchange(self, intent: OrderIntent) -> Any:
        """Build SDK order request and submit through NonceManager."""
        exchange = self._nonce.exchange
        if exchange is None:
            raise ExecutionError("No Exchange instance available")

        # Convert OrderIntent to SDK parameters
        is_buy = intent.side == Side.BUY
        name = str(intent.symbol)
        sz = float(intent.size)

        # Price: for market orders use a very aggressive price
        if intent.order_type == OrderType.MARKET:
            # Market order: use IoC with a very aggressive limit price
            limit_px = 0.0  # SDK market_open/market_close handle price calc
            order_type: dict[str, Any] = {"limit": {"tif": "Ioc"}}
            sdk_cloid = self._to_sdk_cloid(intent.cloid or Cloid(""))
            if intent.reduce_only:
                # market_close derives the safe side from the exchange position and
                # always sends reduce-only semantics in the SDK.
                return await self._nonce.submit(
                    exchange.market_close,
                    name,
                    sz,
                    px=None,
                    slippage=0.05,
                    cloid=sdk_cloid,
                    cloid_hint=str(intent.cloid or ""),
                    preflight_check=lambda: self._placement_preflight(intent),
                )
            return await self._nonce.submit(
                exchange.market_open,
                name,
                is_buy,
                sz,
                px=None,
                slippage=0.05,
                cloid=sdk_cloid,
                cloid_hint=str(intent.cloid or ""),
                preflight_check=lambda: self._placement_preflight(intent),
            )

        else:
            limit_px = float(intent.price) if intent.price else 0.0
            tif = _TIF_MAP.get(str(intent.time_in_force), "Gtc")
            order_type = {"limit": {"tif": tif}}

            return await self._nonce.submit(
                exchange.order,
                name,
                is_buy,
                sz,
                limit_px,
                order_type,
                intent.reduce_only,
                self._to_sdk_cloid(intent.cloid or Cloid("")),
                cloid_hint=str(intent.cloid or ""),
                preflight_check=lambda: self._placement_preflight(intent),
            )

    def _placement_preflight(self, intent: OrderIntent) -> None:
        self._kill_switch.check()
        if self._safety is not None:
            self._safety.check_placement(intent)
        if self._rate_limiter is not None and not self._rate_limiter.check_action_credits():
            raise OrderRejectedError(
                "Action credits are stale or below the low watermark",
                cloid=str(intent.cloid or ""),
                reason="action_credits_below_threshold",
            )

    async def _handle_risk_rejection(self, reason: str | None) -> None:
        if not reason:
            return
        if reason.startswith("risk_check_timeout"):
            if self._safety is not None:
                self._safety.enter_cancel_only(reason)
            return
        if not (reason.startswith("risk_check_error") or reason.startswith("drawdown_exceeded")):
            return
        if self._durable_kill_trigger is not None:
            await self._durable_kill_trigger(reason)
        else:
            self._kill_switch.trigger(reason)

    @staticmethod
    def _matches_intent(order: Order, intent: OrderIntent) -> bool:
        """Compare the immutable business fields bound to an idempotency key."""
        return (
            order.symbol == intent.symbol
            and order.side == intent.side
            and order.size == intent.size
            and order.price == intent.price
            and order.order_type == intent.order_type
            and order.time_in_force == intent.time_in_force
            and order.strategy_id == intent.strategy_id
            and order.sub_account == intent.sub_account
            and order.reduce_only == intent.reduce_only
        )

    @staticmethod
    def _to_sdk_cloid(cloid: Cloid) -> Any:
        """Construct the SDK Cloid object at the exchange boundary."""
        from hyperliquid.utils.types import Cloid as HlCloid

        return HlCloid.from_str(CloidGenerator.to_hl_cloid(cloid))

    def _rejected_order(self, intent: OrderIntent, reason: str) -> Order:
        order = Order(
            cloid=intent.cloid or CloidGenerator.generate(intent.strategy_id),
            symbol=intent.symbol,
            side=intent.side,
            size=intent.size,
            price=intent.price,
            order_type=intent.order_type,
            time_in_force=intent.time_in_force,
            status=OrderStatus.REJECTED,
            strategy_id=intent.strategy_id,
            sub_account=intent.sub_account,
            reduce_only=intent.reduce_only,
            error_message=reason,
        )
        self._orders[order.cloid] = order
        self._event_bus.publish_sync(
            Event(event_type=EVENT_ORDER_REJECTED, payload=order, correlation_id=str(order.cloid))
        )
        return order

    async def _handle_submit_response(self, order: Order, response: Any, *, command_id: uuid.UUID) -> None:
        """Process the exchange response after order submission."""
        cloid = str(order.cloid)

        if isinstance(response, dict):
            status = response.get("status")
            if status == "ok":
                # Order accepted by exchange
                statuses = response.get("response", {}).get("data", {}).get("statuses", [])
                if statuses:
                    first = statuses[0]
                    if "resting" in first:
                        # Order is on the book
                        oid = first["resting"].get("oid")
                        self._state_machine.transition(order, OrderStatus.ACKNOWLEDGED, reason="exchange_ack")
                        order.exchange_oid = OrderId(str(oid)) if oid else None
                        order.acknowledged_at = datetime.now(UTC)
                        await self._persist_transition(
                            order, "acknowledged", command_id=command_id, command_status="succeeded"
                        )
                        self._event_bus.publish_sync(
                            Event(event_type=EVENT_ORDER_ACKNOWLEDGED, payload=order, correlation_id=cloid)
                        )
                    elif "filled" in first:
                        # Immediate fill (market or aggressive limit)
                        fill_data = first["filled"]
                        provisional_size = Size(fill_data.get("totalSz", order.size))
                        provisional_price = Price(fill_data.get("avgPx", 0))
                        self._state_machine.transition(order, OrderStatus.FILLED, reason="immediate_fill")
                        order.filled_at = datetime.now(UTC)
                        order.exchange_oid = OrderId(str(fill_data.get("oid", "")))
                        if self._tracker is not None:
                            self._tracker.update_fill(
                                Fill(
                                    cloid=order.cloid,
                                    exchange_oid=order.exchange_oid,
                                    symbol=order.symbol,
                                    side=order.side,
                                    price=provisional_price,
                                    size=provisional_size,
                                    fee=Usd(fill_data.get("fee", 0)),
                                    is_maker=False,
                                    timestamp=Timestamp(int(time.time() * 1000)),
                                    strategy_id=order.strategy_id,
                                    sub_account=order.sub_account,
                                ),
                                provisional=True,
                            )
                        await self._persist_transition(
                            order, "filled", command_id=command_id, command_status="succeeded"
                        )
                        # Keep the immediate SDK aggregate available in memory,
                        # but do not seed the durable filled_size/average. The
                        # authenticated fill facts are the only authority for
                        # those aggregates and will replace this provisional view.
                        order.filled_size = provisional_size
                        order.avg_fill_price = provisional_price
                        self._event_bus.publish_sync(
                            Event(event_type=EVENT_ORDER_FILLED, payload=order, correlation_id=cloid)
                        )
                    elif "error" in first:
                        error_msg = first["error"]
                        self._state_machine.transition(order, OrderStatus.REJECTED, reason=error_msg)
                        order.error_message = error_msg
                        await self._persist_transition(
                            order, "rejected", command_id=command_id, command_status="failed"
                        )
                        self._event_bus.publish_sync(
                            Event(event_type=EVENT_ORDER_REJECTED, payload=order, correlation_id=cloid)
                        )
                else:
                    # Accepted but no detailed status
                    self._state_machine.transition(order, OrderStatus.ACKNOWLEDGED, reason="exchange_ack")
                    order.acknowledged_at = datetime.now(UTC)
                    await self._persist_transition(
                        order, "acknowledged", command_id=command_id, command_status="succeeded"
                    )
                    self._event_bus.publish_sync(
                        Event(event_type=EVENT_ORDER_ACKNOWLEDGED, payload=order, correlation_id=cloid)
                    )
            elif status == "err":
                error_msg = response.get("response", "unknown_error")
                self._state_machine.transition(order, OrderStatus.REJECTED, reason=str(error_msg))
                order.error_message = str(error_msg)
                await self._persist_transition(order, "rejected", command_id=command_id, command_status="failed")
                self._event_bus.publish_sync(
                    Event(event_type=EVENT_ORDER_REJECTED, payload=order, correlation_id=cloid)
                )
            elif status == "order":
                # orderStatus lookup after an uncertain submission.
                status_data = response.get("order", {})
                exchange_status = str(status_data.get("status", "")).lower()
                if exchange_status == "filled":
                    self._state_machine.transition(order, OrderStatus.FILLED, reason="status_query_filled")
                    order.filled_size = order.size
                    order.filled_at = datetime.now(UTC)
                    await self._persist_transition(order, "filled", command_id=command_id, command_status="succeeded")
                    self._event_bus.publish_sync(
                        Event(event_type=EVENT_ORDER_FILLED, payload=order, correlation_id=cloid)
                    )
                elif exchange_status in {"canceled", "cancelled"}:
                    self._state_machine.transition(order, OrderStatus.CANCELLED, reason="status_query_cancelled")
                    await self._persist_transition(
                        order, "cancelled", command_id=command_id, command_status="succeeded"
                    )
                    self._event_bus.publish_sync(
                        Event(event_type=EVENT_ORDER_CANCELLED, payload=order, correlation_id=cloid)
                    )
                elif exchange_status in {"rejected", "margincanceled"}:
                    self._state_machine.transition(order, OrderStatus.REJECTED, reason="status_query_rejected")
                    await self._persist_transition(order, "rejected", command_id=command_id, command_status="failed")
                    self._event_bus.publish_sync(
                        Event(event_type=EVENT_ORDER_REJECTED, payload=order, correlation_id=cloid)
                    )
                else:
                    self._state_machine.transition(order, OrderStatus.ACKNOWLEDGED, reason="status_query_open")
                    order.acknowledged_at = datetime.now(UTC)
                    await self._persist_transition(
                        order, "acknowledged", command_id=command_id, command_status="succeeded"
                    )
                    self._event_bus.publish_sync(
                        Event(event_type=EVENT_ORDER_ACKNOWLEDGED, payload=order, correlation_id=cloid)
                    )
            else:
                # Unknown response must not be treated as an acknowledgement.
                self._state_machine.transition(order, OrderStatus.SUBMIT_UNKNOWN, reason="unknown_response")
                order.error_message = "unknown_exchange_response"
                await self._persist_transition(order, "submit_unknown", command_id=command_id, command_status="unknown")
        else:
            # Non-dict response (e.g. market_open returns raw data)
            self._state_machine.transition(order, OrderStatus.ACKNOWLEDGED, reason="exchange_ack")
            order.acknowledged_at = datetime.now(UTC)
            await self._persist_transition(order, "acknowledged", command_id=command_id, command_status="succeeded")
            self._event_bus.publish_sync(
                Event(event_type=EVENT_ORDER_ACKNOWLEDGED, payload=order, correlation_id=cloid)
            )

    async def _persist_placement(
        self,
        order: Order,
        risk_result: RiskCheckResult,
        *,
        command_id: uuid.UUID | None = None,
        dispatch: bool,
        reference_price: float | None = None,
        price_observed_at: datetime | None = None,
    ) -> RiskCheckResult | None:
        if self._durable_store is None:
            return None
        return await self._durable_store.persist_placement(
            order,
            risk_result,
            command_id=command_id or uuid.uuid4(),
            dispatch=dispatch,
            reference_price=reference_price,
            price_observed_at=price_observed_at,
        )

    async def _persist_transition(
        self,
        order: Order,
        event_type: str,
        *,
        command_id: uuid.UUID | None = None,
        command_status: str | None = None,
    ) -> None:
        if self._durable_store is None:
            return
        await self._durable_store.persist_transition(
            order,
            event_type,
            command_id=command_id,
            command_status=command_status,
        )
