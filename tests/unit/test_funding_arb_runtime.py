"""Unit and failure-injection tests for automatic-market funding arbitrage."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from hypeedge.account.tracker import AccountTracker
from hypeedge.config.settings import FundingArbSettings
from hypeedge.core.enums import FundingArbCycleState, MarketMakerLifecycle, OrderStatus, Side
from hypeedge.core.exceptions import StrategyLifecycleError
from hypeedge.core.models import AccountState, L2BookSnapshot, L2Level, Order, OrderIntent, Position, SpotBalance
from hypeedge.core.types import Cloid, OrderId, Price, Size, StrategyId, SubAccount, Symbol, Timestamp, Usd
from hypeedge.market_data.funding_arb_scanner import FundingArbMarketSnapshot
from hypeedge.market_data.instrument_cache import InstrumentInfo
from hypeedge.strategy.funding_arb import (
    FundingArbParams,
    FundingArbRuntimeDependencies,
    FundingArbRuntimeHandle,
    decode_funding_arb_config,
)
from hypeedge.strategy.funding_arb.models import FundingArbCycle
from hypeedge.strategy.registry import StrategyConfigSnapshot


def _config_snapshot() -> StrategyConfigSnapshot:
    return StrategyConfigSnapshot(
        StrategyId("fa-auto-1"),
        1,
        {
            "entry_funding_rate": Decimal("0.0002"),
            "exit_funding_rate": Decimal("0.00005"),
            "max_notional_usd": Decimal("20"),
            "hedge_ratio": Decimal("1"),
            "rebalance_threshold_bps": 30,
            "leverage": Decimal("2"),
            "max_slippage_bps": 50,
            "max_basis_bps": 500,
            "min_expected_edge_bps": Decimal("0"),
            "expected_hold_hours": 8,
            "round_trip_fee_bps": Decimal("0"),
            "max_unhedged_seconds": 1,
        },
    )


class _Metadata:
    def __init__(self) -> None:
        self.perps = {
            Symbol("HYPE"): self._perp("HYPE"),
            Symbol("PURR"): self._perp("PURR"),
        }
        self.spots = {
            Symbol("@1035"): self._spot("@1035", "HYPE"),
            Symbol("@1"): self._spot("@1", "PURR"),
        }

    @staticmethod
    def _perp(symbol: str) -> InstrumentInfo:
        return InstrumentInfo(
            symbol=Symbol(symbol),
            display_name=symbol,
            sz_decimals=2,
            max_leverage=10,
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            max_price_decimals=4,
            only_isolated=True,
        )

    @staticmethod
    def _spot(symbol: str, base: str) -> InstrumentInfo:
        return InstrumentInfo(
            symbol=Symbol(symbol),
            display_name=f"{base}/USDC",
            sz_decimals=2,
            max_leverage=1,
            tick_size=Decimal("0.000001"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            max_price_decimals=6,
            is_spot=True,
            base_token=base,
            quote_token="USDC",
        )

    def get(self, symbol: Symbol) -> InstrumentInfo | None:
        return self.perps.get(symbol) or self.spots.get(symbol)

    def resolve_spot(self, market: str | Symbol) -> InstrumentInfo | None:
        value = str(market)
        for info in self.spots.values():
            if value in {str(info.symbol), info.display_name}:
                return info
        return None


def _book(symbol: str, *, mid: str = "100", size: str = "10") -> L2BookSnapshot:
    center = Decimal(mid)
    now = datetime.now(UTC)
    return L2BookSnapshot(
        Symbol(symbol),
        (L2Level(Price(center - Decimal("0.1")), Size(size)),),
        (L2Level(Price(center + Decimal("0.1")), Size(size)),),
        Timestamp(int(now.timestamp() * 1000)),
        local_ts=now,
    )


def _market(
    perp: str = "HYPE",
    spot: str = "@1035",
    *,
    funding: str = "0.01",
    perp_volume: str = "100000",
    spot_volume: str = "50000",
    perp_mid: str = "100",
    spot_mid: str = "100",
    book_size: str = "10",
) -> FundingArbMarketSnapshot:
    display = "HYPE/USDC" if perp == "HYPE" else "PURR/USDC"
    return FundingArbMarketSnapshot(
        perp_symbol=Symbol(perp),
        spot_symbol=Symbol(spot),
        spot_display=display,
        funding_rate=Decimal(funding),
        perp_24h_volume_usd=Decimal(perp_volume),
        spot_24h_volume_usd=Decimal(spot_volume),
        perp_book=_book(perp, mid=perp_mid, size=book_size),
        spot_book=_book(spot, mid=spot_mid, size=book_size),
    )


class _Scanner:
    def __init__(self, markets: list[FundingArbMarketSnapshot], *, scan_error: bool = False) -> None:
        self.markets = markets
        self.scan_error = scan_error
        self.scan_calls = 0
        self.get_calls: list[tuple[Symbol, Symbol]] = []

    async def scan(self) -> tuple[FundingArbMarketSnapshot, ...]:
        self.scan_calls += 1
        if self.scan_error:
            raise RuntimeError("scanner unavailable")
        return tuple(self.markets)

    async def get_market(
        self,
        perp_symbol: Symbol,
        spot_symbol: Symbol,
    ) -> FundingArbMarketSnapshot | None:
        self.get_calls.append((perp_symbol, spot_symbol))
        return next(
            (
                market
                for market in self.markets
                if market.perp_symbol == perp_symbol and market.spot_symbol == spot_symbol
            ),
            None,
        )


class _Health:
    def get_account_health(self, *, now: datetime | None = None) -> Any:
        del now
        return SimpleNamespace(allows_risk_increase=True)


class _CycleStore:
    def __init__(self, active: FundingArbCycle | None = None) -> None:
        self.active = active
        self.events: list[str] = []

    async def create(self, cycle: FundingArbCycle) -> FundingArbCycle:
        self.active = replace(cycle, revision=1, created_at=datetime.now(UTC), updated_at=datetime.now(UTC))
        self.events.append("cycle_created")
        return self.active

    async def get_active(self, strategy_id: str) -> FundingArbCycle | None:
        if self.active is None or self.active.strategy_id != strategy_id:
            return None
        return None if self.active.state == FundingArbCycleState.CLOSED else self.active

    async def transition(
        self,
        cycle: FundingArbCycle,
        state: FundingArbCycleState,
        event_type: str,
        *,
        payload: Any = None,
        **updates: Any,
    ) -> FundingArbCycle:
        del payload
        assert self.active is not None
        assert cycle.revision == self.active.revision
        self.active = replace(
            self.active,
            state=state,
            revision=self.active.revision + 1,
            updated_at=datetime.now(UTC),
            **updates,
        )
        self.events.append(event_type)
        return self.active


class _Execution:
    def __init__(self, outcomes: list[Decimal | None], metadata: _Metadata) -> None:
        self.outcomes = list(outcomes)
        self.metadata = metadata
        self.orders: list[OrderIntent] = []
        self.by_cloid: dict[str, Order] = {}
        self.spot_totals: dict[str, Decimal] = {"HYPE": Decimal(0), "PURR": Decimal(0)}
        self.perp_shorts: dict[Symbol, Decimal] = {symbol: Decimal(0) for symbol in metadata.perps}
        self.leverage_updates: list[tuple[str, int, bool]] = []

    async def submit_order(self, intent: OrderIntent, *, deferred: bool | None = None) -> Order:
        del deferred
        self.orders.append(intent)
        outcome = self.outcomes.pop(0)
        filled = Decimal(0) if outcome is None else outcome
        status = (
            OrderStatus.SUBMIT_UNKNOWN
            if outcome is None
            else OrderStatus.FILLED
            if filled >= Decimal(intent.size)
            else OrderStatus.CANCELLED
            if filled > 0
            else OrderStatus.REJECTED
        )
        order = Order(
            cloid=intent.cloid or Cloid("missing"),
            symbol=intent.symbol,
            side=intent.side,
            size=intent.size,
            price=intent.price,
            order_type=intent.order_type,
            time_in_force=intent.time_in_force,
            status=status,
            strategy_id=intent.strategy_id,
            sub_account=intent.sub_account,
            reduce_only=intent.reduce_only,
            is_spot=intent.is_spot,
            risk_reducing=intent.risk_reducing,
            max_slippage_bps=intent.max_slippage_bps,
            exchange_oid=OrderId(str(len(self.orders))),
            filled_size=Size(filled),
        )
        self.by_cloid[str(order.cloid)] = order
        if filled > 0:
            if intent.is_spot:
                info = self.metadata.resolve_spot(intent.symbol)
                assert info is not None and info.base_token is not None
                direction = Decimal(1) if intent.side == Side.BUY else Decimal(-1)
                self.spot_totals[info.base_token] += direction * filled
            else:
                direction = Decimal(1) if intent.side == Side.SELL else Decimal(-1)
                self.perp_shorts[intent.symbol] += direction * filled
        return order

    async def refresh_order_from_durable(self, cloid: str) -> Order | None:
        return self.by_cloid.get(cloid)

    async def update_leverage(self, symbol: str, leverage: int, *, is_cross: bool) -> dict[str, str]:
        self.leverage_updates.append((symbol, leverage, is_cross))
        return {"status": "ok"}


async def _runtime(
    outcomes: list[Decimal | None],
    *,
    markets: list[FundingArbMarketSnapshot] | None = None,
    scan_error: bool = False,
    active_cycle: FundingArbCycle | None = None,
) -> tuple[
    FundingArbRuntimeHandle,
    _Execution,
    _CycleStore,
    _Scanner,
    Callable[[], Awaitable[bool]],
]:
    tracker = AccountTracker()
    metadata = _Metadata()
    execution = _Execution(outcomes, metadata)
    store = _CycleStore(active_cycle)
    scanner = _Scanner([_market()] if markets is None else markets, scan_error=scan_error)

    async def reconcile() -> bool:
        now = datetime.now(UTC)
        tracker.update_account_state(
            AccountState(Usd("500"), Usd("500"), Usd("0"), Usd("0"), Usd("500"), SubAccount("0xabc"))
        )
        balances = [
            SpotBalance("USDC", Size("1000"), sub_account=SubAccount("0xabc"), updated_at=now),
            *(
                SpotBalance(token, Size(total), sub_account=SubAccount("0xabc"), updated_at=now)
                for token, total in execution.spot_totals.items()
            ),
        ]
        tracker.update_spot_balances(tuple(balances), observed_at=now)
        for symbol, short_size in execution.perp_shorts.items():
            if short_size > 0:
                tracker.update_position_from_exchange(
                    symbol,
                    Position(symbol, Size(-short_size), Price("100"), Price("100")),
                )
            else:
                tracker.remove_position(symbol)
        return True

    await reconcile()
    deps = FundingArbRuntimeDependencies(
        execution=execution,  # type: ignore[arg-type]
        scanner=scanner,
        tracker=tracker,
        metadata=metadata,  # type: ignore[arg-type]
        cycles=store,
        account_health=_Health(),  # type: ignore[arg-type]
        reconcile=reconcile,
        trading_ready=lambda: True,
        kill_switch_active=lambda: False,
        deployment=FundingArbSettings(
            max_notional_usd=Decimal("20"),
            poll_interval_seconds=60,
            order_status_poll_interval_seconds=0.05,
            max_leg_attempts=3,
        ),
        account_address="0xabc",
    )
    handle = FundingArbRuntimeHandle(
        StrategyId("fa-auto-1"),
        decode_funding_arb_config(_config_snapshot()),
        config_revision=1,
        sub_account="0xabc",
        dependencies=deps,
    )
    return handle, execution, store, scanner, reconcile


def _active_cycle(*, perp: str = "PURR", spot: str = "@1") -> FundingArbCycle:
    return FundingArbCycle(
        cycle_id=uuid.uuid4(),
        strategy_id="fa-auto-1",
        config_revision=1,
        sub_account="0xabc",
        perp_symbol=perp,
        spot_symbol=spot,
        spot_display=f"{perp}/USDC",
        base_token=perp,
        quote_token="USDC",
        state=FundingArbCycleState.OPEN,
        target_perp_size=Decimal("0.2"),
        target_spot_size=Decimal("0.2"),
        perp_open_size=Decimal("0.2"),
        spot_open_size=Decimal("0.2"),
        baseline_spot_size=Decimal(0),
        entry_funding_rate=Decimal("0.01"),
        entry_basis_bps=Decimal(0),
        revision=1,
    )


def test_decode_funding_arb_config_roundtrip_without_market_fields() -> None:
    params = decode_funding_arb_config(_config_snapshot())
    assert not hasattr(params, "spot_coin")
    assert params.entry_funding_rate == Decimal("0.0002")
    assert params.max_slippage_bps == 50
    assert params.max_unhedged_seconds == 1


def test_funding_arb_params_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="hedge_ratio"):
        FundingArbParams(hedge_ratio=Decimal("2"))
    with pytest.raises(ValueError, match="leverage must be an integer"):
        FundingArbParams(leverage=Decimal("1.5"))


async def test_observer_runtime_lifecycle_remains_non_trading() -> None:
    handle = FundingArbRuntimeHandle(StrategyId("fa-auto-1"), decode_funding_arb_config(_config_snapshot()))
    await handle.start()
    await handle.set_mode(MarketMakerLifecycle.RUNNING)
    assert handle.snapshot()["live_enabled"] is False
    await handle.stop()


async def test_selects_highest_edge_candidate_and_rechecks_the_same_market() -> None:
    markets = [
        _market(funding="0.001", spot_volume="100000"),
        _market("PURR", "@1", funding="0.003", perp_volume="80000", spot_volume="60000"),
    ]
    handle, execution, store, scanner, _ = await _runtime([Decimal("0.20"), Decimal("0.20")], markets=markets)

    plan = await handle._entry_plan()
    assert plan is not None
    assert plan.perp.symbol == Symbol("PURR")
    await handle._open_cycle(plan)

    assert scanner.get_calls == [(Symbol("PURR"), Symbol("@1"))]
    assert [order.symbol for order in execution.orders[:2]] == [Symbol("@1"), Symbol("PURR")]
    assert store.active is not None and store.active.perp_symbol == "PURR"


async def test_live_cycle_opens_spot_first_and_closes_perp_first() -> None:
    handle, execution, store, scanner, _ = await _runtime(
        [Decimal("0.20"), Decimal("0.20"), Decimal("0.20"), Decimal("0.20")]
    )
    plan = await handle._entry_plan()
    assert plan is not None
    await handle._open_cycle(plan)

    assert store.active is not None and store.active.state == FundingArbCycleState.OPEN
    assert [(order.is_spot, order.side, order.reduce_only) for order in execution.orders[:2]] == [
        (True, Side.BUY, False),
        (False, Side.SELL, False),
    ]
    assert execution.leverage_updates == [("HYPE", 2, False)]

    scanner.markets[0] = replace(scanner.markets[0], funding_rate=Decimal(0))
    await handle._evaluate()
    assert handle.snapshot()["cycle_id"] is None
    assert handle._spot is None and handle._perp is None
    assert [(order.is_spot, order.side, order.reduce_only) for order in execution.orders[2:]] == [
        (False, Side.BUY, True),
        (True, Side.SELL, False),
    ]


async def test_second_leg_rejection_compensates_all_acquired_spot() -> None:
    handle, execution, store, _, _ = await _runtime([Decimal("0.20"), Decimal("0"), Decimal("0.20")])
    plan = await handle._entry_plan()
    assert plan is not None
    await handle._open_cycle(plan)

    assert handle.snapshot()["cycle_id"] is None
    assert store.active is not None and store.active.state == FundingArbCycleState.CLOSED
    assert execution.spot_totals["HYPE"] == 0
    assert execution.perp_shorts[Symbol("HYPE")] == 0
    assert execution.orders[-1].is_spot and execution.orders[-1].side == Side.SELL
    assert execution.orders[-1].risk_reducing is True


async def test_partial_perp_fill_keeps_only_common_hedged_size() -> None:
    handle, execution, store, _, _ = await _runtime([Decimal("0.20"), Decimal("0.10"), Decimal("0.10")])
    plan = await handle._entry_plan()
    assert plan is not None
    await handle._open_cycle(plan)

    assert store.active is not None and store.active.state == FundingArbCycleState.OPEN
    assert store.active.spot_open_size == Decimal("0.10")
    assert store.active.perp_open_size == Decimal("0.10")
    assert execution.spot_totals["HYPE"] == Decimal("0.10")
    assert execution.perp_shorts[Symbol("HYPE")] == Decimal("0.10")


async def test_unknown_entry_is_not_reissued_with_a_new_cloid() -> None:
    handle, execution, store, _, _ = await _runtime([None])
    plan = await handle._entry_plan()
    assert plan is not None
    await handle._open_cycle(plan)

    assert len(execution.orders) == 1
    assert store.active is not None and store.active.state == FundingArbCycleState.FAULTED
    assert store.active.error_code == "spot_entry_unknown"


async def test_liquidity_and_book_gates_keep_zero_orders() -> None:
    low_volume = _market(spot_volume="999")
    one_sided = replace(_market(), spot_book=replace(_market().spot_book, asks=()))
    shallow = _market(book_size="0.4")
    cases = [
        (low_volume, "spot_volume_below_minimum"),
        (one_sided, "spot_book_invalid_or_stale"),
        (shallow, "book_depth_below_minimum"),
    ]

    for market, reason in cases:
        handle, execution, _, _, _ = await _runtime([], markets=[market])
        assert await handle._entry_plan() is None
        assert handle.snapshot()["entry_block_reason"] == reason
        assert execution.orders == []


async def test_scanner_failure_is_observable_and_keeps_zero_orders() -> None:
    handle, execution, _, _, _ = await _runtime([], scan_error=True)

    assert await handle._entry_plan() is None
    assert handle.snapshot()["entry_block_reason"] == "market_scan_failed"
    assert execution.orders == []


async def test_entry_plan_requires_spot_quote_and_perp_margin_before_first_leg() -> None:
    handle, _, _, _, _ = await _runtime([])
    deps = handle._deps
    assert deps is not None
    now = datetime.now(UTC)
    deps.tracker.update_spot_balances(
        (SpotBalance("USDC", Size("1"), sub_account=SubAccount("0xabc"), updated_at=now),),
        observed_at=now,
    )

    assert await handle._entry_plan() is None
    assert handle.snapshot()["entry_block_reason"] == "insufficient_spot_quote_balance"

    deps.tracker.update_spot_balances(
        (SpotBalance("USDC", Size("1000"), sub_account=SubAccount("0xabc"), updated_at=now),),
        observed_at=now,
    )
    deps.tracker.update_account_state(
        AccountState(Usd("500"), Usd("1"), Usd("0"), Usd("0"), Usd("500"), SubAccount("0xabc"))
    )
    assert await handle._entry_plan() is None
    assert handle.snapshot()["entry_block_reason"] == "insufficient_perp_margin"


async def test_entry_snapshot_explains_basis_gate() -> None:
    handle, _, _, _, _ = await _runtime([], markets=[_market(spot_mid="42")])

    assert await handle._entry_plan() is None
    snapshot = handle.snapshot()
    assert snapshot["entry_block_reason"] == "basis_exceeds_limit"
    assert Decimal(snapshot["entry_diagnostics"]["basis_bps"]) > Decimal("500")


async def test_exit_never_sells_spot_while_authoritative_perp_short_remains() -> None:
    handle, execution, store, _, _ = await _runtime(
        [Decimal("0.20"), Decimal("0.20"), Decimal("0"), Decimal("0"), Decimal("0")]
    )
    plan = await handle._entry_plan()
    assert plan is not None
    await handle._open_cycle(plan)

    await handle._close_cycle("test_exit")

    assert store.active is not None and store.active.state == FundingArbCycleState.FAULTED
    assert store.active.error_code == "perp_exit_incomplete"
    assert execution.spot_totals["HYPE"] == Decimal("0.20")
    assert execution.perp_shorts[Symbol("HYPE")] == Decimal("0.20")
    assert not any(order.is_spot and order.side == Side.SELL for order in execution.orders[2:])


async def test_recovery_binds_instruments_from_the_persisted_cycle() -> None:
    cycle = _active_cycle()
    handle, execution, store, _, reconcile = await _runtime([], active_cycle=cycle)
    execution.spot_totals["PURR"] = Decimal("0.2")
    execution.perp_shorts[Symbol("PURR")] = Decimal("0.2")
    await reconcile()
    handle._cycle = cycle

    await handle._recover_cycle()

    assert handle._require_spot().symbol == Symbol("@1")
    assert handle._require_perp().symbol == Symbol("PURR")
    assert store.events[-1] == "cycle_recovered"


async def test_live_runtime_rejects_unrouted_subaccount() -> None:
    handle, _, _, _, _ = await _runtime([])
    deps = handle._deps
    assert deps is not None
    with pytest.raises(StrategyLifecycleError, match="sub_account"):
        FundingArbRuntimeHandle(
            StrategyId("fa-auto-2"),
            decode_funding_arb_config(_config_snapshot()),
            sub_account="0xdifferent",
            dependencies=deps,
        )
