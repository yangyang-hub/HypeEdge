"""Funding-rate arbitrage runtime: single-venue delta-neutral execution.

HL perpetual short (collects funding) delta-hedged by HL spot long on the same
venue. Minimal closed loop: a funding signal opens/closes both legs through the
injected ExecutionClient (risk-gated). Rebalancing is intentionally one-shot at
entry; authoritative spot reconciliation, continuous delta rebalancing and PnL
attribution are later phases (see ``docs/funding_arb_design.md``).
"""

from __future__ import annotations

import asyncio
import contextlib
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import structlog

from hypeedge.core.enums import MarketMakerLifecycle, OrderType, Side, TimeInForce
from hypeedge.core.exceptions import StrategyLifecycleError
from hypeedge.core.models import OrderIntent
from hypeedge.core.types import Price, Size, StrategyId, Symbol
from hypeedge.storage.market_making import default_funding_arb_config, normalize_funding_arb_config
from hypeedge.strategy.funding_arb.models import FundingArbParams
from hypeedge.strategy.registry import StrategyBuildContext, StrategyConfigSnapshot, StrategyRuntimeHandle

if TYPE_CHECKING:
    from hypeedge.account.tracker import AccountTracker
    from hypeedge.execution.engine import ExecutionClient
    from hypeedge.market_data.provider import MarketDataProvider

logger = structlog.get_logger(__name__)

# Funding settles hourly on HL; polling once a minute is responsive enough for the
# minimal loop without burning action weight.
_POLL_INTERVAL_SECONDS = 60.0
_SIZE_STEP = Decimal("0.0001")


def decode_funding_arb_config(snapshot: StrategyConfigSnapshot, *, symbol: str) -> FundingArbParams:
    """Decode a durable config snapshot into ``FundingArbParams``."""

    del symbol  # perp leg uses instance.symbol; spot leg lives in params.spot_coin
    values = dict(snapshot.values)
    return FundingArbParams(
        spot_coin=str(values["spot_coin"]),
        entry_funding_rate=Decimal(str(values["entry_funding_rate"])),
        exit_funding_rate=Decimal(str(values["exit_funding_rate"])),
        max_notional_usd=Decimal(str(values["max_notional_usd"])),
        hedge_ratio=Decimal(str(values["hedge_ratio"])),
        rebalance_threshold_bps=int(values["rebalance_threshold_bps"]),
        leverage=Decimal(str(values["leverage"])),
    )


class FundingArbRuntimeHandle:
    """Minimal closed-loop funding-arb runtime.

    Each poll reads the perpetual funding rate; above the entry threshold it opens a
    delta-neutral pair (perp short + spot long), below the exit threshold it closes
    both legs. Orders flow through the injected ``ExecutionClient`` (risk-gated). If
    execution/provider deps are absent the handle runs as a passive observer,
    preserving the control-plane lifecycle contract.
    """

    def __init__(
        self,
        strategy_id: StrategyId,
        perp_symbol: str,
        params: FundingArbParams,
        *,
        execution: ExecutionClient | None = None,
        provider: MarketDataProvider | None = None,
        tracker: AccountTracker | None = None,
    ) -> None:
        self._strategy_id = strategy_id
        self._perp_symbol = perp_symbol
        self._params = params
        self._execution = execution
        self._provider = provider
        self._tracker = tracker
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._position_open = False
        self._perp_size = Decimal(0)
        self._spot_size = Decimal(0)
        self._log = logger.bind(strategy_id=str(strategy_id), perp=perp_symbol, spot=params.spot_coin)

    def _can_trade(self) -> bool:
        return self._execution is not None and self._provider is not None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        if self._can_trade():
            self._task = asyncio.create_task(self._run_loop(), name=f"funding_arb:{self._strategy_id}")
            self._log.info("funding_arb_runtime_started")
        else:
            self._log.warning("funding_arb_runtime_observer_only", note="execution/provider unavailable; no orders")

    async def set_mode(self, mode: MarketMakerLifecycle) -> None:
        if mode in {MarketMakerLifecycle.WARMING, MarketMakerLifecycle.SHADOW}:
            # Warming is transient; shadow is not supported by this type and gated upstream.
            return
        if mode == MarketMakerLifecycle.RUNNING:
            if not self._running:
                await self.start()
            return
        if mode == MarketMakerLifecycle.PAUSED:
            self._running = False
            await self._cancel_task()
            self._log.info("funding_arb_runtime_paused")
            return
        if mode in {MarketMakerLifecycle.STOPPED, MarketMakerLifecycle.FAULTED, MarketMakerLifecycle.DRAINING}:
            await self.stop()
            return
        raise StrategyLifecycleError(f"Unsupported funding_arb mode: {mode.value}")

    async def apply_config(self, config: StrategyConfigSnapshot) -> None:
        self._params = decode_funding_arb_config(config, symbol=self._perp_symbol)
        self._log = logger.bind(strategy_id=str(self._strategy_id), perp=self._perp_symbol, spot=self._params.spot_coin)
        self._log.info("funding_arb_runtime_config_applied")

    async def stop(self) -> None:
        self._running = False
        await self._cancel_task()
        self._log.info("funding_arb_runtime_stopped", position_open=self._position_open)

    async def _cancel_task(self) -> None:
        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self._evaluate()
            except Exception:
                self._log.exception("funding_arb_evaluate_error")
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)

    async def _evaluate(self) -> None:
        provider = self._provider
        if provider is None:
            return
        funding = provider.get_funding(Symbol(self._perp_symbol))
        if funding is None:
            return
        rate = Decimal(str(funding.funding_rate))
        if not self._position_open and rate > self._params.entry_funding_rate:
            await self._open_legs(rate)
        elif self._position_open and rate < self._params.exit_funding_rate:
            await self._close_legs()

    async def _open_legs(self, rate: Decimal) -> None:
        if self._execution is None or self._provider is None:
            return
        perp_book = self._provider.get_book(Symbol(self._perp_symbol))
        spot_book = self._provider.get_spot_book(Symbol(self._params.spot_coin))
        if perp_book is None or not perp_book.bids or spot_book is None or not spot_book.asks:
            self._log.debug("funding_arb_open_skipped_no_book")
            return
        perp_px = Decimal(str(perp_book.bids[0].price))
        spot_px = Decimal(str(spot_book.asks[0].price))
        notional = self._params.max_notional_usd
        perp_size = (notional / perp_px).quantize(_SIZE_STEP)
        spot_size = ((notional * self._params.hedge_ratio) / spot_px).quantize(_SIZE_STEP)
        if perp_size <= 0 or spot_size <= 0:
            return
        try:
            await self._execution.submit_order(
                self._intent(self._perp_symbol, Side.SELL, perp_size, perp_px, is_spot=False)
            )
            await self._execution.submit_order(
                self._intent(self._params.spot_coin, Side.BUY, spot_size, spot_px, is_spot=True)
            )
        except Exception:
            self._log.exception("funding_arb_open_legs_failed", rate=str(rate))
            return
        self._position_open = True
        self._perp_size = perp_size
        self._spot_size = spot_size
        self._log.info("funding_arb_opened", rate=str(rate), perp_size=str(perp_size), spot_size=str(spot_size))

    async def _close_legs(self) -> None:
        if self._execution is None or self._provider is None:
            return
        perp_book = self._provider.get_book(Symbol(self._perp_symbol))
        spot_book = self._provider.get_spot_book(Symbol(self._params.spot_coin))
        if perp_book is None or not perp_book.asks or spot_book is None or not spot_book.bids:
            self._log.debug("funding_arb_close_skipped_no_book")
            return
        perp_px = Decimal(str(perp_book.asks[0].price))
        spot_px = Decimal(str(spot_book.bids[0].price))
        try:
            await self._execution.submit_order(
                self._intent(self._perp_symbol, Side.BUY, self._perp_size, perp_px, is_spot=False, reduce_only=True)
            )
            await self._execution.submit_order(
                self._intent(self._params.spot_coin, Side.SELL, self._spot_size, spot_px, is_spot=True)
            )
        except Exception:
            self._log.exception("funding_arb_close_legs_failed")
            return
        self._position_open = False
        self._perp_size = Decimal(0)
        self._spot_size = Decimal(0)
        self._log.info("funding_arb_closed")

    def _intent(
        self,
        symbol: str,
        side: Side,
        size: Decimal,
        price: Decimal,
        *,
        is_spot: bool,
        reduce_only: bool = False,
    ) -> OrderIntent:
        return OrderIntent(
            symbol=Symbol(symbol),
            side=side,
            size=Size(float(size)),
            price=Price(float(price)),
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            strategy_id=self._strategy_id,
            reduce_only=reduce_only,
            is_spot=is_spot,
        )


def build_funding_arb_plugin(
    *,
    execution: ExecutionClient | None = None,
    provider: MarketDataProvider | None = None,
    tracker: AccountTracker | None = None,
) -> Any:
    """Build the funding_arb ``StrategyTypePlugin`` registration.

    ``execution``/``provider``/``tracker`` are app-level singletons captured by the
    factory closure; any that are None make the runtime a passive observer.
    """

    from hypeedge.strategy.plugin import FUNDING_ARB_CAPABILITIES, StaticStrategyTypePlugin

    def factory(context: StrategyBuildContext) -> StrategyRuntimeHandle:
        params = decode_funding_arb_config(context.config, symbol=str(context.instance.symbol))
        return FundingArbRuntimeHandle(
            context.instance.strategy_id,
            str(context.instance.symbol),
            params,
            execution=execution,
            provider=provider,
            tracker=tracker,
        )

    return StaticStrategyTypePlugin(
        strategy_type="funding_arb",
        capabilities=FUNDING_ARB_CAPABILITIES,
        factory=factory,
        _default_config=default_funding_arb_config(),
        _validate=normalize_funding_arb_config,
        _decode=lambda snapshot: decode_funding_arb_config(snapshot, symbol="BTC"),
    )


__all__ = [
    "FundingArbRuntimeHandle",
    "build_funding_arb_plugin",
    "decode_funding_arb_config",
]
