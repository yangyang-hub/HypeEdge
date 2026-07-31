"""Testnet-only, fill-aware single-venue funding-rate arbitrage runtime."""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

import structlog

from hypeedge.config.settings import FundingArbSettings
from hypeedge.core.enums import FundingArbCycleState, MarketMakerLifecycle, OrderStatus, OrderType, Side, TimeInForce
from hypeedge.core.exceptions import StrategyLifecycleError
from hypeedge.core.models import L2BookSnapshot, Order, OrderIntent
from hypeedge.core.types import Cloid, Size, StrategyId, SubAccount, Symbol
from hypeedge.execution.cloid import CloidGenerator
from hypeedge.storage.funding_arb import FundingArbCycleStore
from hypeedge.storage.market_making import default_funding_arb_config, normalize_funding_arb_config
from hypeedge.strategy.funding_arb.models import FundingArbCycle, FundingArbParams
from hypeedge.strategy.registry import StrategyBuildContext, StrategyConfigSnapshot

if TYPE_CHECKING:
    from hypeedge.account.health import AccountHealthProvider
    from hypeedge.account.tracker import AccountTracker
    from hypeedge.execution.engine import ExecutionClient
    from hypeedge.market_data.instrument_cache import InstrumentInfo, InstrumentMetaCache
    from hypeedge.market_data.provider import MarketDataProvider

logger = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class FundingArbRuntimeDependencies:
    """Live boundaries captured by the app only for an enabled testnet deployment."""

    execution: ExecutionClient
    provider: MarketDataProvider
    tracker: AccountTracker
    metadata: InstrumentMetaCache
    cycles: FundingArbCycleStore
    account_health: AccountHealthProvider
    reconcile: Callable[[], Awaitable[bool]]
    trading_ready: Callable[[], bool]
    kill_switch_active: Callable[[], bool]
    deployment: FundingArbSettings
    account_address: str


@dataclass(frozen=True, slots=True)
class _EntryPlan:
    funding_rate: Decimal
    basis_bps: Decimal
    perp_size: Decimal
    spot_size: Decimal


@dataclass(frozen=True, slots=True)
class _OrderOutcome:
    cloid: str
    filled_size: Decimal
    status: OrderStatus | None
    unknown: bool = False


def decode_funding_arb_config(snapshot: StrategyConfigSnapshot, *, symbol: str) -> FundingArbParams:
    """Decode a durable config snapshot into validated runtime parameters."""
    del symbol
    normalized = normalize_funding_arb_config(snapshot.values)
    return FundingArbParams(
        spot_coin=str(normalized["spot_coin"]),
        entry_funding_rate=Decimal(normalized["entry_funding_rate"]),
        exit_funding_rate=Decimal(normalized["exit_funding_rate"]),
        max_notional_usd=Decimal(normalized["max_notional_usd"]),
        hedge_ratio=Decimal(normalized["hedge_ratio"]),
        rebalance_threshold_bps=int(normalized["rebalance_threshold_bps"]),
        leverage=Decimal(normalized["leverage"]),
        max_slippage_bps=int(normalized["max_slippage_bps"]),
        max_basis_bps=int(normalized["max_basis_bps"]),
        min_expected_edge_bps=Decimal(normalized["min_expected_edge_bps"]),
        expected_hold_hours=int(normalized["expected_hold_hours"]),
        round_trip_fee_bps=Decimal(normalized["round_trip_fee_bps"]),
        max_unhedged_seconds=int(normalized["max_unhedged_seconds"]),
    )


class FundingArbRuntimeHandle:
    """Own one durable spot-long/perpetual-short funding-arbitrage cycle."""

    def __init__(
        self,
        strategy_id: StrategyId,
        perp_symbol: str,
        params: FundingArbParams,
        *,
        config_revision: int = 1,
        sub_account: str = "",
        dependencies: FundingArbRuntimeDependencies | None = None,
    ) -> None:
        self._strategy_id = strategy_id
        self._perp_symbol = perp_symbol
        self._params = params
        self._config_revision = config_revision
        self._sub_account = sub_account.lower()
        self._deps = dependencies
        self._spot: InstrumentInfo | None = None
        self._perp: InstrumentInfo | None = None
        self._cycle: FundingArbCycle | None = None
        self._task: asyncio.Task[None] | None = None
        self._started = False
        self._allow_entry = False
        self._entry_block_reason: str | None = "not_evaluated"
        self._entry_diagnostics: dict[str, str] = {}
        self._stop_event = asyncio.Event()
        self._evaluation_lock = asyncio.Lock()
        self._log = logger.bind(strategy_id=str(strategy_id), perp=perp_symbol, spot=params.spot_coin)
        if dependencies is not None:
            self._validate_live_dependencies()

    @property
    def live_enabled(self) -> bool:
        return self._deps is not None

    def snapshot(self) -> dict[str, Any]:
        cycle = self._cycle
        return {
            "live_enabled": self.live_enabled,
            "allow_entry": self._allow_entry,
            "cycle_id": str(cycle.cycle_id) if cycle is not None else None,
            "cycle_state": cycle.state.value if cycle is not None else None,
            "perp_open_size": str(cycle.perp_open_size) if cycle is not None else "0",
            "spot_open_size": str(cycle.spot_open_size) if cycle is not None else "0",
            "error_code": cycle.error_code if cycle is not None else None,
            "error_message": cycle.error_message if cycle is not None else None,
            "entry_block_reason": self._entry_block_reason,
            "entry_diagnostics": dict(self._entry_diagnostics),
        }

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        if self._deps is None:
            self._log.warning("funding_arb_observer_started", execution_enabled=False)
            return
        self._cycle = await self._deps.cycles.get_active(str(self._strategy_id))
        if self._cycle is not None:
            await self._recover_cycle()
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop(), name=f"funding_arb:{self._strategy_id}")
        self._log.info("funding_arb_runtime_started", recovered_cycle=self._cycle is not None)

    async def set_mode(self, mode: MarketMakerLifecycle) -> None:
        if mode in {MarketMakerLifecycle.WARMING, MarketMakerLifecycle.SHADOW}:
            return
        if mode == MarketMakerLifecycle.RUNNING:
            self._allow_entry = True
            return
        if mode == MarketMakerLifecycle.PAUSED:
            self._allow_entry = False
            self._log.info("funding_arb_runtime_paused")
            return
        if mode == MarketMakerLifecycle.FAULTED:
            self._allow_entry = False
            return
        if mode in {MarketMakerLifecycle.STOPPED, MarketMakerLifecycle.DRAINING}:
            await self.stop()
            return
        raise StrategyLifecycleError(f"Unsupported funding_arb mode: {mode.value}")

    async def apply_config(self, config: StrategyConfigSnapshot) -> None:
        async with self._evaluation_lock:
            if self._cycle is not None and config.revision != self._config_revision:
                raise StrategyLifecycleError("funding-arb config cannot change while a cycle is active")
            params = decode_funding_arb_config(config, symbol=self._perp_symbol)
            self._params = params
            self._config_revision = config.revision
            if self._deps is not None:
                self._validate_instruments(params)
        self._log.info("funding_arb_runtime_config_applied", revision=config.revision)

    async def stop(self) -> None:
        if not self._started:
            return
        self._allow_entry = False
        async with self._evaluation_lock:
            if self._deps is not None and self._cycle is not None and self._cycle.state != FundingArbCycleState.CLOSED:
                await self._close_cycle("operator_stop")
                if self._cycle is not None:
                    raise StrategyLifecycleError("funding-arb stop could not flatten both legs")
        self._stop_event.set()
        task = self._task
        self._task = None
        if task is not None and task is not asyncio.current_task() and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._started = False
        self._log.info("funding_arb_runtime_stopped")

    def _validate_live_dependencies(self) -> None:
        deps = self._require_deps()
        if not deps.account_address:
            raise StrategyLifecycleError("funding-arb requires a configured testnet account")
        if self._sub_account != deps.account_address.lower():
            raise StrategyLifecycleError("funding-arb instance sub_account must match the routed exchange account")
        self._validate_instruments(self._params)

    def _validate_instruments(self, params: FundingArbParams) -> None:
        deps = self._require_deps()
        spot = deps.metadata.resolve_spot(params.spot_coin)
        perp = deps.metadata.get(Symbol(self._perp_symbol))
        if spot is None or not spot.is_spot:
            raise StrategyLifecycleError(f"spot metadata is unavailable for {params.spot_coin}")
        if perp is None or perp.is_spot:
            raise StrategyLifecycleError(f"perpetual metadata is unavailable for {self._perp_symbol}")
        if spot.base_token != self._perp_symbol or spot.quote_token != "USDC":
            raise StrategyLifecycleError(
                f"spot/perp risk units do not match: spot={spot.display_name} perp={self._perp_symbol}"
            )
        leverage = int(params.leverage)
        if leverage > perp.max_leverage:
            raise StrategyLifecycleError(f"configured leverage {leverage} exceeds exchange maximum {perp.max_leverage}")
        self._spot = spot
        self._perp = perp

    async def _run_loop(self) -> None:
        deps = self._require_deps()
        while not self._stop_event.is_set():
            try:
                async with self._evaluation_lock:
                    await self._evaluate()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._log.exception("funding_arb_evaluate_failed")
                await self._fault("runtime_evaluation_failed", str(exc))
                self._allow_entry = False
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=deps.deployment.poll_interval_seconds)
            except TimeoutError:
                continue

    async def _evaluate(self) -> None:
        if self._cycle is None:
            if not self._allow_entry or not self._entry_runtime_ready():
                return
            plan = self._entry_plan()
            if plan is not None:
                await self._open_cycle(plan)
            return
        if self._cycle.state == FundingArbCycleState.FAULTED:
            return
        if self._cycle.state != FundingArbCycleState.OPEN:
            await self._recover_cycle()
            return
        funding = self._require_deps().provider.get_funding(Symbol(self._perp_symbol))
        if funding is not None and Decimal(str(funding.funding_rate)) <= self._params.exit_funding_rate:
            await self._close_cycle("funding_exit")
            return
        await self._rebalance_if_needed()

    def _entry_runtime_ready(self) -> bool:
        deps = self._require_deps()
        if not deps.trading_ready():
            self._block_entry("trading_not_ready")
            return False
        if deps.kill_switch_active():
            self._block_entry("kill_switch_active")
            return False
        health = deps.account_health.get_account_health()
        if not health.allows_risk_increase:
            self._block_entry(
                "account_health_not_ready",
                blocking_reasons=",".join(getattr(health, "blocking_reasons", ())),
            )
            return False
        return True

    def _entry_plan(self) -> _EntryPlan | None:
        deps = self._require_deps()
        spot = self._require_spot()
        perp = self._require_perp()
        perp_book = deps.provider.get_book(Symbol(self._perp_symbol))
        spot_book = deps.provider.get_spot_book(spot.symbol)
        funding = deps.provider.get_funding(Symbol(self._perp_symbol))
        if funding is None:
            return self._block_entry("funding_unavailable")
        funding_rate = Decimal(str(funding.funding_rate))
        if funding_rate < self._params.entry_funding_rate:
            return self._block_entry(
                "funding_below_entry_threshold",
                funding_rate=funding_rate,
                entry_funding_rate=self._params.entry_funding_rate,
            )
        if not self._valid_book(perp_book):
            return self._block_entry("perp_book_invalid_or_stale")
        if not self._valid_book(spot_book):
            return self._block_entry("spot_book_invalid_or_stale")
        assert perp_book is not None and spot_book is not None
        perp_mid = (Decimal(perp_book.bids[0].price) + Decimal(perp_book.asks[0].price)) / 2
        spot_mid = (Decimal(spot_book.bids[0].price) + Decimal(spot_book.asks[0].price)) / 2
        basis_bps = abs(perp_mid - spot_mid) / spot_mid * Decimal(10_000)
        if basis_bps > self._params.max_basis_bps:
            return self._block_entry(
                "basis_exceeds_limit",
                basis_bps=basis_bps,
                max_basis_bps=self._params.max_basis_bps,
            )
        spread_cost_bps = (
            (Decimal(perp_book.asks[0].price) - Decimal(perp_book.bids[0].price)) / perp_mid
            + (Decimal(spot_book.asks[0].price) - Decimal(spot_book.bids[0].price)) / spot_mid
        ) * Decimal(10_000)
        expected_edge = (
            funding_rate * self._params.expected_hold_hours * Decimal(10_000)
            - self._params.round_trip_fee_bps
            - spread_cost_bps
        )
        if expected_edge < self._params.min_expected_edge_bps:
            return self._block_entry(
                "expected_edge_below_minimum",
                expected_edge_bps=expected_edge,
                min_expected_edge_bps=self._params.min_expected_edge_bps,
                spread_cost_bps=spread_cost_bps,
            )
        current_perp = deps.tracker.get_position(Symbol(self._perp_symbol))
        if current_perp is not None and not current_perp.is_flat:
            return self._block_entry("existing_perp_position", position_size=current_perp.size)
        notional = min(self._params.max_notional_usd, deps.deployment.max_notional_usd)
        perp_size = self._floor(notional / perp_mid, perp.lot_size)
        spot_size = self._floor(perp_size * self._params.hedge_ratio, spot.lot_size)
        if perp_size < perp.min_size or spot_size < spot.min_size:
            return self._block_entry(
                "size_below_exchange_minimum",
                perp_size=perp_size,
                spot_size=spot_size,
            )
        slippage_multiplier = Decimal(10_000 + self._params.max_slippage_bps) / Decimal(10_000)
        quote_balance = deps.tracker.get_spot_balance(spot.quote_token or "")
        required_quote = spot_size * Decimal(spot_book.asks[0].price) * slippage_multiplier
        if quote_balance is None or Decimal(quote_balance.available) < required_quote:
            return self._block_entry(
                "insufficient_spot_quote_balance",
                required_quote=required_quote,
                available_quote=Decimal(quote_balance.available) if quote_balance is not None else Decimal(0),
            )
        account = deps.tracker.get_account_state()
        required_margin = perp_size * perp_mid / self._params.leverage * slippage_multiplier
        if account is None or Decimal(account.available_balance) < required_margin:
            return self._block_entry(
                "insufficient_perp_margin",
                required_margin=required_margin,
                available_margin=Decimal(account.available_balance) if account is not None else Decimal(0),
            )
        self._entry_block_reason = None
        self._entry_diagnostics = {
            "funding_rate": str(funding_rate),
            "basis_bps": str(basis_bps),
            "expected_edge_bps": str(expected_edge),
            "perp_size": str(perp_size),
            "spot_size": str(spot_size),
            "required_quote": str(required_quote),
            "required_margin": str(required_margin),
        }
        return _EntryPlan(funding_rate, basis_bps, perp_size, spot_size)

    def _block_entry(self, reason: str, **diagnostics: Any) -> _EntryPlan | None:
        self._entry_block_reason = reason
        self._entry_diagnostics = {key: str(value) for key, value in diagnostics.items()}
        return None

    def _valid_book(self, book: L2BookSnapshot | None) -> bool:
        if book is None or not book.bids or not book.asks or book.bids[0].price >= book.asks[0].price:
            return False
        age = (datetime.now(UTC) - book.received_at).total_seconds()
        return age <= self._require_deps().deployment.market_stale_seconds

    async def _open_cycle(self, plan: _EntryPlan) -> None:
        deps = self._require_deps()
        spot = self._require_spot()
        if not await self._refresh_authoritative_account():
            return
        refreshed_plan = self._entry_plan()
        if refreshed_plan is None:
            return
        plan = refreshed_plan
        baseline = self._spot_total(spot.base_token or "")
        spot_cloid = self._new_cloid()
        cycle = FundingArbCycle(
            cycle_id=uuid.uuid4(),
            strategy_id=str(self._strategy_id),
            config_revision=self._config_revision,
            sub_account=self._sub_account,
            perp_symbol=self._perp_symbol,
            spot_symbol=str(spot.symbol),
            spot_display=spot.display_name,
            base_token=spot.base_token or "",
            quote_token=spot.quote_token or "",
            state=FundingArbCycleState.ENTERING_SPOT,
            target_perp_size=plan.perp_size,
            target_spot_size=plan.spot_size,
            perp_open_size=Decimal(0),
            spot_open_size=Decimal(0),
            baseline_spot_size=baseline,
            entry_funding_rate=plan.funding_rate,
            entry_basis_bps=plan.basis_bps,
            revision=0,
            spot_entry_cloid=spot_cloid,
        )
        self._cycle = await deps.cycles.create(cycle)
        spot_outcome = await self._execute_leg(
            spot.symbol,
            Side.BUY,
            plan.spot_size,
            cloid=spot_cloid,
            is_spot=True,
        )
        if spot_outcome.unknown:
            await self._fault_after_unknown("spot_entry_unknown")
            return
        if spot_outcome.filled_size <= 0:
            await self._transition(
                FundingArbCycleState.CLOSED,
                "spot_entry_no_fill",
                error_code="spot_entry_no_fill",
                error_message="spot entry produced no authenticated fill",
            )
            self._cycle = None
            return
        perp_target = self._floor(
            spot_outcome.filled_size / self._params.hedge_ratio,
            self._require_perp().lot_size,
        )
        if perp_target <= 0:
            await self._compensate_spot(spot_outcome.filled_size, "spot_fill_below_perp_lot")
            return
        perp_cloid = self._new_cloid()
        await self._transition(
            FundingArbCycleState.ENTERING_PERP,
            "spot_entry_filled",
            payload={"filled_size": str(spot_outcome.filled_size)},
            spot_open_size=spot_outcome.filled_size,
            perp_entry_cloid=perp_cloid,
        )
        await deps.execution.update_leverage(self._perp_symbol, int(self._params.leverage), is_cross=False)
        perp_outcome = await self._execute_leg(
            Symbol(self._perp_symbol),
            Side.SELL,
            min(perp_target, plan.perp_size),
            cloid=perp_cloid,
            is_spot=False,
        )
        if perp_outcome.unknown:
            await self._fault_after_unknown("perp_entry_unknown")
            return
        await self._align_and_open(spot_outcome.filled_size, perp_outcome.filled_size)

    async def _align_and_open(self, spot_size: Decimal, perp_size: Decimal) -> None:
        if self._cycle is None:
            return
        await self._transition(
            FundingArbCycleState.COMPENSATING_ENTRY,
            "entry_alignment_started",
            spot_open_size=spot_size,
            perp_open_size=perp_size,
        )
        if not await self._refresh_authoritative_account():
            await self._fault("entry_reconciliation_failed", "authoritative reconciliation failed before alignment")
            return
        spot_size, perp_size = self._actual_cycle_exposure()
        spot_size, perp_size = await self._reduce_larger_leg(spot_size, perp_size, "entry")
        if not await self._refresh_authoritative_account():
            await self._fault("entry_reconciliation_failed", "authoritative reconciliation failed after alignment")
            return
        spot_size, perp_size = self._actual_cycle_exposure()
        if spot_size <= 0 and perp_size <= 0:
            await self._transition(
                FundingArbCycleState.CLOSED,
                "entry_compensated_flat",
                spot_open_size=Decimal(0),
                perp_open_size=Decimal(0),
            )
            self._cycle = None
            return
        if not self._hedge_matches(spot_size, perp_size):
            await self._fault("entry_compensation_incomplete", "two legs could not be aligned")
            return
        await self._transition(
            FundingArbCycleState.OPEN,
            "cycle_opened",
            spot_open_size=spot_size,
            perp_open_size=perp_size,
            error_code=None,
            error_message=None,
        )

    async def _compensate_spot(self, size: Decimal, reason: str) -> None:
        await self._transition(
            FundingArbCycleState.COMPENSATING_ENTRY,
            reason,
            spot_open_size=size,
            perp_open_size=Decimal(0),
        )
        if not await self._refresh_authoritative_account():
            await self._fault("compensation_reconciliation_failed", reason)
            return
        actual_spot, actual_perp = self._actual_cycle_exposure()
        if actual_perp > self._require_perp().lot_size / 2:
            await self._fault("unexpected_perp_during_spot_compensation", f"perp={actual_perp}")
            return
        size = actual_spot
        if size <= self._require_spot().lot_size / 2:
            await self._transition(
                FundingArbCycleState.CLOSED,
                "spot_compensation_complete",
                spot_open_size=Decimal(0),
                perp_open_size=Decimal(0),
            )
            self._cycle = None
            return
        filled = await self._execute_reducing(
            self._require_spot().symbol,
            Side.SELL,
            size,
            is_spot=True,
            event_prefix="spot_compensation",
        )
        if not await self._refresh_authoritative_account():
            await self._fault("compensation_reconciliation_failed", reason)
            return
        remaining_spot, remaining_perp = self._actual_cycle_exposure()
        if remaining_spot > self._require_spot().lot_size / 2 or remaining_perp > self._require_perp().lot_size / 2:
            await self._fault(
                "spot_compensation_incomplete",
                f"spot={remaining_spot} perp={remaining_perp} authenticated_fill={filled}",
            )
            return
        await self._transition(
            FundingArbCycleState.CLOSED,
            "spot_compensation_complete",
            spot_open_size=Decimal(0),
            perp_open_size=Decimal(0),
        )
        self._cycle = None

    async def _close_cycle(self, reason: str) -> None:
        cycle = self._cycle
        if cycle is None:
            return
        if not await self._refresh_authoritative_account():
            await self._fault("exit_reconciliation_failed", reason)
            return
        spot_size, perp_size = self._actual_cycle_exposure()
        await self._transition(
            FundingArbCycleState.EXITING_PERP,
            "exit_started",
            payload={"reason": reason},
            spot_open_size=spot_size,
            perp_open_size=perp_size,
        )
        if perp_size > 0:
            filled = await self._execute_reducing(
                Symbol(self._perp_symbol),
                Side.BUY,
                perp_size,
                is_spot=False,
                reduce_only=True,
                event_prefix="perp_exit",
            )
            perp_size = max(Decimal(0), perp_size - filled)
        if not await self._refresh_authoritative_account():
            await self._fault("perp_exit_reconciliation_failed", reason)
            return
        spot_size, perp_size = self._actual_cycle_exposure()
        if perp_size > self._require_perp().lot_size / 2:
            await self._fault("perp_exit_incomplete", f"remaining={perp_size}")
            return
        await self._transition(
            FundingArbCycleState.EXITING_SPOT,
            "perp_exit_complete",
            perp_open_size=Decimal(0),
        )
        if spot_size > 0:
            filled = await self._execute_reducing(
                self._require_spot().symbol,
                Side.SELL,
                spot_size,
                is_spot=True,
                event_prefix="spot_exit",
            )
            spot_size = max(Decimal(0), spot_size - filled)
        if spot_size > self._require_spot().lot_size / 2:
            await self._fault("spot_exit_incomplete", f"remaining={spot_size}")
            return
        if not await self._refresh_authoritative_account():
            await self._fault("final_reconciliation_failed", reason)
            return
        actual_spot, actual_perp = self._actual_cycle_exposure()
        if actual_spot > self._require_spot().lot_size / 2 or actual_perp > self._require_perp().lot_size / 2:
            await self._fault(
                "final_exposure_not_flat",
                f"spot={actual_spot} perp={actual_perp}",
            )
            return
        await self._transition(
            FundingArbCycleState.CLOSED,
            "cycle_closed",
            spot_open_size=Decimal(0),
            perp_open_size=Decimal(0),
            error_code=None,
            error_message=None,
        )
        self._cycle = None

    async def _rebalance_if_needed(self) -> None:
        if self._cycle is None or not await self._refresh_authoritative_account():
            return
        spot_size, perp_size = self._actual_cycle_exposure()
        denominator = max(perp_size * self._params.hedge_ratio, self._require_spot().lot_size)
        deviation_bps = abs(spot_size - perp_size * self._params.hedge_ratio) / denominator * Decimal(10_000)
        if deviation_bps <= self._params.rebalance_threshold_bps:
            return
        await self._transition(
            FundingArbCycleState.REBALANCING,
            "rebalance_started",
            payload={"deviation_bps": str(deviation_bps)},
            spot_open_size=spot_size,
            perp_open_size=perp_size,
        )
        spot_size, perp_size = await self._reduce_larger_leg(spot_size, perp_size, "rebalance")
        if not await self._refresh_authoritative_account():
            await self._fault("rebalance_reconciliation_failed", "authoritative reconciliation failed after rebalance")
            return
        spot_size, perp_size = self._actual_cycle_exposure()
        if not self._hedge_matches(spot_size, perp_size):
            await self._fault("rebalance_incomplete", f"spot={spot_size} perp={perp_size}")
            return
        await self._transition(
            FundingArbCycleState.OPEN,
            "rebalance_complete",
            spot_open_size=spot_size,
            perp_open_size=perp_size,
        )

    async def _reduce_larger_leg(
        self,
        spot_size: Decimal,
        perp_size: Decimal,
        event_prefix: str,
    ) -> tuple[Decimal, Decimal]:
        target_spot = self._floor(perp_size * self._params.hedge_ratio, self._require_spot().lot_size)
        if spot_size > target_spot:
            excess = self._floor(spot_size - target_spot, self._require_spot().lot_size)
            if excess > 0:
                filled = await self._execute_reducing(
                    self._require_spot().symbol,
                    Side.SELL,
                    excess,
                    is_spot=True,
                    event_prefix=f"{event_prefix}_spot_reduce",
                )
                spot_size = max(Decimal(0), spot_size - filled)
        elif spot_size < target_spot and self._params.hedge_ratio > 0:
            target_perp = self._floor(spot_size / self._params.hedge_ratio, self._require_perp().lot_size)
            excess = self._floor(perp_size - target_perp, self._require_perp().lot_size)
            if excess > 0:
                filled = await self._execute_reducing(
                    Symbol(self._perp_symbol),
                    Side.BUY,
                    excess,
                    is_spot=False,
                    reduce_only=True,
                    event_prefix=f"{event_prefix}_perp_reduce",
                )
                perp_size = max(Decimal(0), perp_size - filled)
        return spot_size, perp_size

    async def _execute_reducing(
        self,
        symbol: Symbol,
        side: Side,
        size: Decimal,
        *,
        is_spot: bool,
        event_prefix: str,
        reduce_only: bool = False,
    ) -> Decimal:
        remaining = size
        filled_total = Decimal(0)
        deps = self._require_deps()
        for attempt in range(1, deps.deployment.max_leg_attempts + 1):
            if remaining <= 0:
                break
            cloid = self._new_cloid()
            field = self._cloid_field(event_prefix)
            updates = {field: cloid} if field is not None else {}
            await self._transition(
                self._cycle.state if self._cycle is not None else FundingArbCycleState.FAULTED,
                f"{event_prefix}_attempt",
                payload={"attempt": attempt, "cloid": cloid, "size": str(remaining)},
                **updates,
            )
            outcome = await self._execute_leg(
                symbol,
                side,
                remaining,
                cloid=cloid,
                is_spot=is_spot,
                reduce_only=reduce_only,
                risk_reducing=True,
            )
            if outcome.unknown:
                await self._fault(
                    f"{event_prefix}_unknown",
                    f"risk-reducing order outcome is unresolved: cloid={cloid}",
                )
                return filled_total
            filled_total += outcome.filled_size
            remaining = self._floor(max(Decimal(0), size - filled_total), self._lot_size(is_spot))
        return filled_total

    async def _execute_leg(
        self,
        symbol: Symbol,
        side: Side,
        size: Decimal,
        *,
        cloid: str,
        is_spot: bool,
        reduce_only: bool = False,
        risk_reducing: bool = False,
    ) -> _OrderOutcome:
        order = await self._require_deps().execution.submit_order(
            OrderIntent(
                symbol=symbol,
                side=side,
                size=Size(size),
                order_type=OrderType.MARKET,
                time_in_force=TimeInForce.IOC,
                strategy_id=self._strategy_id,
                sub_account=SubAccount(self._sub_account),
                reduce_only=reduce_only,
                cloid=Cloid(cloid),
                is_spot=is_spot,
                risk_reducing=risk_reducing,
                max_slippage_bps=self._params.max_slippage_bps,
            )
        )
        return await self._wait_authoritative_order(str(order.cloid), self._params.max_unhedged_seconds)

    async def _wait_authoritative_order(self, cloid: str, timeout_seconds: int) -> _OrderOutcome:
        deps = self._require_deps()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        settle_seconds = min(
            1.0,
            max(deps.deployment.order_status_poll_interval_seconds * 2, timeout_seconds / 4),
        )
        terminal_since: float | None = None
        terminal_filled: Decimal | None = None
        last: Order | None = None
        while loop.time() < deadline:
            last = await deps.execution.refresh_order_from_durable(cloid)
            if last is not None:
                filled = Decimal(last.filled_size)
                if filled >= Decimal(last.size):
                    return _OrderOutcome(cloid, filled, last.status)
                if last.status in {
                    OrderStatus.CANCELLED,
                    OrderStatus.REJECTED,
                    OrderStatus.EXPIRED,
                }:
                    now = loop.time()
                    if terminal_since is None or terminal_filled != filled:
                        terminal_since = now
                        terminal_filled = filled
                    elif now - terminal_since >= settle_seconds:
                        return _OrderOutcome(cloid, filled, last.status)
                else:
                    terminal_since = None
                    terminal_filled = None
            await asyncio.sleep(deps.deployment.order_status_poll_interval_seconds)
        return _OrderOutcome(
            cloid,
            Decimal(last.filled_size) if last is not None else Decimal(0),
            last.status if last is not None else None,
            unknown=True,
        )

    async def _recover_cycle(self) -> None:
        cycle = self._cycle
        if cycle is None or cycle.state == FundingArbCycleState.CLOSED:
            self._cycle = None
            return
        if cycle.state == FundingArbCycleState.FAULTED:
            return
        if not await self._refresh_authoritative_account():
            await self._fault("recovery_reconciliation_failed", "could not obtain complete account truth")
            return
        spot_size, perp_size = self._actual_cycle_exposure()
        if cycle.state == FundingArbCycleState.OPEN and self._hedge_matches(spot_size, perp_size):
            self._cycle = await self._require_deps().cycles.transition(
                cycle,
                FundingArbCycleState.OPEN,
                "cycle_recovered",
                payload={"spot_size": str(spot_size), "perp_size": str(perp_size)},
                spot_open_size=spot_size,
                perp_open_size=perp_size,
            )
            return
        if cycle.state in {FundingArbCycleState.EXITING_PERP, FundingArbCycleState.EXITING_SPOT}:
            await self._close_cycle("restart_recovery")
            return
        await self._align_and_open(spot_size, perp_size)

    async def _fault_after_unknown(self, code: str) -> None:
        await self._refresh_authoritative_account()
        spot_size, perp_size = self._actual_cycle_exposure()
        if spot_size > 0 or perp_size > 0:
            spot_size, perp_size = await self._reduce_larger_leg(spot_size, perp_size, "unknown_compensation")
        await self._fault(code, f"unresolved order outcome; spot={spot_size} perp={perp_size}")

    async def _fault(self, code: str, message: str) -> None:
        if self._cycle is None or self._cycle.state == FundingArbCycleState.CLOSED:
            return
        self._cycle = await self._require_deps().cycles.transition(
            self._cycle,
            FundingArbCycleState.FAULTED,
            "cycle_faulted",
            payload={"error_code": code, "error_message": message},
            error_code=code,
            error_message=message,
        )
        self._log.error("funding_arb_cycle_faulted", error_code=code, error_message=message)

    async def _transition(
        self,
        state: FundingArbCycleState,
        event_type: str,
        *,
        payload: dict[str, Any] | None = None,
        **updates: Any,
    ) -> None:
        if self._cycle is None:
            raise StrategyLifecycleError("funding-arb cycle is unavailable")
        self._cycle = await self._require_deps().cycles.transition(
            self._cycle,
            state,
            event_type,
            payload=payload,
            **updates,
        )

    async def _refresh_authoritative_account(self) -> bool:
        try:
            return await self._require_deps().reconcile()
        except Exception:
            self._log.exception("funding_arb_reconciliation_failed")
            return False

    def _actual_cycle_exposure(self) -> tuple[Decimal, Decimal]:
        if self._cycle is None:
            return Decimal(0), Decimal(0)
        spot_total = self._spot_total(self._cycle.base_token)
        spot_size = max(Decimal(0), spot_total - self._cycle.baseline_spot_size)
        position = self._require_deps().tracker.get_position(Symbol(self._perp_symbol))
        perp_size = max(Decimal(0), -Decimal(position.size)) if position is not None else Decimal(0)
        return spot_size, perp_size

    def _spot_total(self, token: str) -> Decimal:
        balance = self._require_deps().tracker.get_spot_balance(token)
        return Decimal(balance.total) if balance is not None else Decimal(0)

    def _hedge_matches(self, spot_size: Decimal, perp_size: Decimal) -> bool:
        expected_spot = self._floor(perp_size * self._params.hedge_ratio, self._require_spot().lot_size)
        return abs(spot_size - expected_spot) <= self._require_spot().lot_size / 2

    def _lot_size(self, is_spot: bool) -> Decimal:
        return self._require_spot().lot_size if is_spot else self._require_perp().lot_size

    @staticmethod
    def _floor(value: Decimal, step: Decimal) -> Decimal:
        return (value / step).to_integral_value(rounding=ROUND_DOWN) * step

    def _new_cloid(self) -> str:
        return CloidGenerator.to_hl_cloid(CloidGenerator.generate(self._strategy_id))

    @staticmethod
    def _cloid_field(event_prefix: str) -> str | None:
        if "compensation" in event_prefix or "spot_reduce" in event_prefix:
            return "compensation_cloid"
        if event_prefix == "perp_exit" or "perp_reduce" in event_prefix:
            return "perp_exit_cloid"
        if event_prefix == "spot_exit":
            return "spot_exit_cloid"
        return None

    def _require_deps(self) -> FundingArbRuntimeDependencies:
        if self._deps is None:
            raise StrategyLifecycleError("funding-arb live dependencies are unavailable")
        return self._deps

    def _require_spot(self) -> InstrumentInfo:
        if self._spot is None:
            raise StrategyLifecycleError("funding-arb spot metadata is unavailable")
        return self._spot

    def _require_perp(self) -> InstrumentInfo:
        if self._perp is None:
            raise StrategyLifecycleError("funding-arb perpetual metadata is unavailable")
        return self._perp


def build_funding_arb_plugin(*, dependencies: FundingArbRuntimeDependencies | None = None) -> Any:
    """Build the funding-arbitrage plugin; live dependencies are testnet-gated by the app."""
    from hypeedge.strategy.plugin import FUNDING_ARB_CAPABILITIES, StaticStrategyTypePlugin

    def factory(context: StrategyBuildContext) -> FundingArbRuntimeHandle:
        params = decode_funding_arb_config(context.config, symbol=str(context.instance.symbol))
        return FundingArbRuntimeHandle(
            context.instance.strategy_id,
            str(context.instance.symbol),
            params,
            config_revision=context.config.revision,
            sub_account=str(context.instance.sub_account),
            dependencies=dependencies,
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
    "FundingArbRuntimeDependencies",
    "FundingArbRuntimeHandle",
    "build_funding_arb_plugin",
    "decode_funding_arb_config",
]
