"""Nonce manager — serializes all exchange actions through a single queue.

Design doc §9.1: "All signed actions go through the execution module as a
single serialization point; nonce increments monotonically; concurrent
signing from multiple strategies must converge here."

The HL SDK uses timestamp-ms as nonce. Our NonceManager wraps the SDK's
Exchange class to serialize concurrent order submissions and handle
timeout/retry logic per design doc §9.4.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import structlog

from hypeedge.core.exceptions import (
    ExecutionError,
    KillSwitchTriggeredError,
    NonceError,
    OrderRejectedError,
    OrderTimeoutError,
    SigningError,
)
from hypeedge.market_data.rate_limiter import RateLimiter

logger = structlog.get_logger(__name__)

# Design doc §9.4: submission timeout threshold
_SUBMIT_TIMEOUT_S = 3.0
_MAX_RETRIES = 2
_BACKOFF_DELAYS = [0.0, 1.0, 2.0]


@dataclass
class ActionRequest:
    """A pending action queued for serial execution."""

    action_fn: Callable[..., Any]
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    future: asyncio.Future[Any] | None = None
    cloid: str | None = None
    preflight_check: Callable[[], None] | None = None
    retries: int = 0


class NonceManager:
    """Serial action executor wrapping the HL SDK Exchange.

    All exchange mutations (order, cancel, modify) flow through
    submit(). The queue guarantees nonce monotonicity and prevents
    concurrent signing races.

    Usage:
        result = await nonce_manager.submit(exchange.order, "BTC", True, 0.01, 50000.0, {"limit": {"tif": "Gtc"}})
    """

    def __init__(
        self,
        rate_limiter: RateLimiter | None = None,
    ) -> None:
        self._queue: asyncio.Queue[ActionRequest] = asyncio.Queue()
        self._rate_limiter = rate_limiter
        self._exchange: Any = None  # hyperliquid.Exchange (set via set_exchange())
        self._info: Any = None  # hyperliquid.Info (set via set_info())
        self._running = False
        self._total_actions = 0
        self._total_errors = 0

    def set_exchange(self, exchange: Any) -> None:
        """Set the HL SDK Exchange instance (called during app initialization)."""
        self._exchange = exchange

    def set_info(self, info: Any) -> None:
        """Set the HL SDK Info instance for order status queries."""
        self._info = info

    @property
    def exchange(self) -> Any:
        """Access the underlying HL SDK Exchange."""
        return self._exchange

    @property
    def info(self) -> Any:
        """Access the underlying HL SDK Info client."""
        return self._info

    @property
    def queue_depth(self) -> int:
        return self._queue.qsize()

    @property
    def stats(self) -> dict[str, int]:
        return {
            "total_actions": self._total_actions,
            "total_errors": self._total_errors,
            "queue_depth": self.queue_depth,
        }

    async def submit(
        self,
        action_fn: Callable[..., Any],
        *args: Any,
        cloid_hint: str | None = None,
        preflight_check: Callable[[], None] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Submit an action for serial execution. Returns the result.

        The action_fn is typically a bound method like exchange.order.
        It will be called with the provided args/kwargs inside the
        serialization loop.
        """
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        cloid = cloid_hint or kwargs.get("cloid")
        request = ActionRequest(
            action_fn=action_fn,
            args=args,
            kwargs=kwargs,
            future=future,
            cloid=str(cloid) if cloid else None,
            preflight_check=preflight_check,
        )
        await self._queue.put(request)
        return await future

    async def run(self) -> None:
        """Consumer loop — processes one action at a time."""
        self._running = True
        logger.info("nonce_manager_started")

        try:
            while self._running:
                request = await self._queue.get()
                await self._execute_with_retry(request)
        except asyncio.CancelledError:
            logger.debug("nonce_manager_cancelled")
        finally:
            self._running = False
            logger.info("nonce_manager_stopped", stats=self.stats)

    async def stop(self) -> None:
        """Signal the manager to stop after draining current queue."""
        self._running = False

    async def _execute_with_retry(self, request: ActionRequest) -> None:
        """Execute an action with timeout and retry logic (design doc §9.4).

        Timeout flow:
        1. Execute action with _SUBMIT_TIMEOUT_S timeout
        2. On timeout: query order status by cloid
        3. If not found: retry with same cloid (idempotent)
        4. Max _MAX_RETRIES, then reject
        """
        while request.retries <= _MAX_RETRIES:
            try:
                if request.retries > 0:
                    delay = _BACKOFF_DELAYS[min(request.retries, len(_BACKOFF_DELAYS) - 1)]
                    logger.info("nonce_retry", retries=request.retries, delay_s=delay, cloid=request.cloid)
                    await asyncio.sleep(delay)

                result = await asyncio.wait_for(
                    self._run_action(request),
                    timeout=_SUBMIT_TIMEOUT_S,
                )

                self._total_actions += 1
                if request.future and not request.future.done():
                    request.future.set_result(result)
                return

            except TimeoutError:
                logger.warning("nonce_action_timeout", retries=request.retries, cloid=request.cloid)
                if request.cloid and self._info:
                    found = await self._query_order_status(request.cloid)
                    if found:
                        self._total_actions += 1
                        if request.future and not request.future.done():
                            request.future.set_result(found)
                        return

                # A timed-out synchronous SDK call continues in its worker thread.
                # Retrying here could create a duplicate order, so preserve UNKNOWN
                # for reconciliation instead of blindly resubmitting.
                self._total_errors += 1
                error = OrderTimeoutError(
                    f"Exchange action outcome unknown (cloid={request.cloid})",
                    cloid=request.cloid,
                )
                if request.future and not request.future.done():
                    request.future.set_exception(error)
                return

            except Exception as e:
                self._total_errors += 1
                logger.exception("nonce_action_error", retries=request.retries, cloid=request.cloid, error=str(e))

                if isinstance(e, (SigningError, NonceError, KillSwitchTriggeredError, OrderRejectedError)):
                    if request.future and not request.future.done():
                        request.future.set_exception(e)
                    return

                if request.cloid:
                    if isinstance(e, (TypeError, ValueError)):
                        if request.future and not request.future.done():
                            request.future.set_exception(e)
                        return
                    if self._info:
                        found = await self._query_order_status(request.cloid)
                        if found:
                            if request.future and not request.future.done():
                                request.future.set_result(found)
                            return
                    unknown_error = OrderTimeoutError(
                        f"Exchange action failed with unknown outcome (cloid={request.cloid})",
                        cloid=request.cloid,
                    )
                    if request.future and not request.future.done():
                        request.future.set_exception(unknown_error)
                    return

                request.retries += 1

        self._total_errors += 1
        final_error = ExecutionError(f"Action failed after {_MAX_RETRIES} retries (cloid={request.cloid})")
        logger.error("nonce_max_retries_exhausted", cloid=request.cloid)
        if request.future and not request.future.done():
            request.future.set_exception(final_error)

    async def _run_action(self, request: ActionRequest) -> Any:
        """Execute the action function (handles both sync and async callables)."""
        if self._exchange is None:
            raise NonceError("NonceManager has no Exchange instance — call set_exchange() first")
        if request.preflight_check is not None:
            # This is the final event-loop boundary before the SDK call is
            # scheduled. Placement guards belong here so queued work cannot
            # survive a Kill/Safety transition. Cancel actions omit the guard.
            request.preflight_check()
        if asyncio.iscoroutinefunction(request.action_fn):
            return await request.action_fn(*request.args, **request.kwargs)
        return await asyncio.to_thread(request.action_fn, *request.args, **request.kwargs)

    async def _query_order_status(self, cloid: str) -> Any | None:
        """Query order status by cloid from the exchange.

        Returns the exchange response if the order was found, None otherwise.
        """
        if not self._info:
            return None
        try:
            from hyperliquid.utils.types import Cloid as HlCloid

            from hypeedge.core.types import Cloid
            from hypeedge.execution.cloid import CloidGenerator

            hl_cloid_str = CloidGenerator.to_hl_cloid(Cloid(cloid))
            hl_cloid = HlCloid(hl_cloid_str)

            account_address = getattr(self._exchange, "account_address", None)
            if not account_address:
                wallet = getattr(self._exchange, "wallet", None)
                if wallet:
                    account_address = wallet.address

            if not account_address:
                logger.warning("nonce_query_no_address", cloid=cloid)
                return None

            result = await asyncio.to_thread(self._info.query_order_by_cloid, account_address, hl_cloid)
            if result and result.get("status") == "order":
                return result
            return None
        except Exception:
            logger.debug("nonce_query_failed", cloid=cloid)
            return None

    async def query_order_status(self, cloid: str) -> Any | None:
        """Public read-only recovery query; it never signs or mutates exchange state."""
        return await self._query_order_status(cloid)
