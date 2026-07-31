"""Unit and failure-injection tests for funding-rate arbitrage execution."""

from __future__ import annotations

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
from hypeedge.core.models import (
    AccountState,
    FundingRate,
    L2BookSnapshot,
    L2Level,
    Order,
    OrderIntent,
    Position,
    SpotBalance,
)
from hypeedge.core.types import OrderId, Price, Size, StrategyId, SubAccount, Symbol, Timestamp, Usd
from hypeedge.market_data.instrument_cache import InstrumentInfo
from hypeedge.strategy.funding_arb import (
    FundingArbParams,
    FundingArbRuntimeDependencies,
    FundingArbRuntimeHandle,
    decode_funding_arb_config,
)
from hypeedge.strategy.funding_arb.models import FundingArbCycle
from hypeedge.strategy.registry import StrategyConfigSnapshot


def _snapshot() -> StrategyConfigSnapshot:
    return StrategyConfigSnapshot(
        StrategyId("fa-1"),
        1,
        {
            "spot_coin": "HYPE/USDC",
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
        self.perp = InstrumentInfo(
            symbol=Symbol("HYPE"),
            display_name="HYPE",
            sz_decimals=2,
            max_leverage=10,
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            max_price_decimals=4,
            only_isolated=True,
        )
        self.spot = InstrumentInfo(
            symbol=Symbol("@1035"),
            display_name="HYPE/USDC",
            sz_decimals=2,
            max_leverage=1,
            tick_size=Decimal("0.000001"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            max_price_decimals=6,
            is_spot=True,
            base_token="HYPE",
            quote_token="USDC",
        )

    def get(self, symbol: Symbol) -> InstrumentInfo | None:
        return self.perp if symbol == self.perp.symbol else self.spot if symbol == self.spot.symbol else None

    def resolve_spot(self, market: str | Symbol) -> InstrumentInfo | None:
        return self.spot if str(market) in {"HYPE/USDC", "@1035"} else None


class _Provider:
    def __init__(self) -> None:
        now = datetime.now(UTC)
        self.perp_book = L2BookSnapshot(
            Symbol("HYPE"),
            (L2Level(Price("99.9"), Size("10")),),
            (L2Level(Price("100.1"), Size("10")),),
            Timestamp(int(now.timestamp() * 1000)),
            local_ts=now,
        )
        self.spot_book = L2BookSnapshot(
            Symbol("@1035"),
            (L2Level(Price("99.9"), Size("10")),),
            (L2Level(Price("100.1"), Size("10")),),
            Timestamp(int(now.timestamp() * 1000)),
            local_ts=now,
        )
        self.funding = FundingRate(Symbol("HYPE"), 0.01, 0, Price("100"), 1000, Timestamp(1))

    def get_book(self, symbol: Symbol) -> L2BookSnapshot | None:
        return self.perp_book if symbol == Symbol("HYPE") else self.spot_book if symbol == Symbol("@1035") else None

    def get_spot_book(self, symbol: Symbol) -> L2BookSnapshot | None:
        return self.spot_book if symbol == Symbol("@1035") else None

    def get_funding(self, symbol: Symbol) -> FundingRate | None:
        return self.funding if symbol == Symbol("HYPE") else None


class _Health:
    def get_account_health(self, *, now: datetime | None = None) -> Any:
        del now
        return SimpleNamespace(allows_risk_increase=True)


class _CycleStore:
    def __init__(self) -> None:
        self.active: FundingArbCycle | None = None
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
    def __init__(self, outcomes: list[Decimal | None]) -> None:
        self.outcomes = list(outcomes)
        self.orders: list[OrderIntent] = []
        self.by_cloid: dict[str, Order] = {}
        self.spot_total = Decimal("0")
        self.perp_short = Decimal("0")
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
            cloid=intent.cloid or "missing",
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
                self.spot_total += filled if intent.side == Side.BUY else -filled
            else:
                self.perp_short += filled if intent.side == Side.SELL else -filled
        return order

    async def refresh_order_from_durable(self, cloid: str) -> Order | None:
        return self.by_cloid.get(cloid)

    async def update_leverage(self, symbol: str, leverage: int, *, is_cross: bool) -> dict[str, str]:
        self.leverage_updates.append((symbol, leverage, is_cross))
        return {"status": "ok"}


async def _runtime(
    outcomes: list[Decimal | None],
) -> tuple[FundingArbRuntimeHandle, _Execution, _CycleStore, _Provider]:
    tracker = AccountTracker()
    execution = _Execution(outcomes)
    store = _CycleStore()
    provider = _Provider()

    async def reconcile() -> bool:
        now = datetime.now(UTC)
        tracker.update_account_state(
            AccountState(Usd("500"), Usd("500"), Usd("0"), Usd("0"), Usd("500"), SubAccount("0xabc"))
        )
        tracker.update_spot_balances(
            (
                SpotBalance("HYPE", Size(execution.spot_total), sub_account=SubAccount("0xabc"), updated_at=now),
                SpotBalance("USDC", Size("1000"), sub_account=SubAccount("0xabc"), updated_at=now),
            ),
            observed_at=now,
        )
        if execution.perp_short > 0:
            tracker.update_position_from_exchange(
                Symbol("HYPE"),
                Position(Symbol("HYPE"), Size(-execution.perp_short), Price("100"), Price("100")),
            )
        else:
            tracker.remove_position(Symbol("HYPE"))
        return True

    await reconcile()
    deps = FundingArbRuntimeDependencies(
        execution=execution,  # type: ignore[arg-type]
        provider=provider,  # type: ignore[arg-type]
        tracker=tracker,
        metadata=_Metadata(),  # type: ignore[arg-type]
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
        StrategyId("fa-1"),
        "HYPE",
        decode_funding_arb_config(_snapshot(), symbol="HYPE"),
        config_revision=1,
        sub_account="0xabc",
        dependencies=deps,
    )
    return handle, execution, store, provider


def test_decode_funding_arb_config_roundtrip() -> None:
    params = decode_funding_arb_config(_snapshot(), symbol="HYPE")
    assert params.spot_coin == "HYPE/USDC"
    assert params.entry_funding_rate == Decimal("0.0002")
    assert params.max_slippage_bps == 50
    assert params.max_unhedged_seconds == 1


def test_funding_arb_params_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="hedge_ratio"):
        FundingArbParams(hedge_ratio=Decimal("2"))
    with pytest.raises(ValueError, match="leverage must be an integer"):
        FundingArbParams(leverage=Decimal("1.5"))


async def test_observer_runtime_lifecycle_remains_non_trading() -> None:
    handle = FundingArbRuntimeHandle(StrategyId("fa-1"), "HYPE", decode_funding_arb_config(_snapshot(), symbol="HYPE"))
    await handle.start()
    await handle.set_mode(MarketMakerLifecycle.RUNNING)
    assert handle.snapshot()["live_enabled"] is False
    await handle.stop()


async def test_live_cycle_opens_spot_first_and_closes_perp_first() -> None:
    handle, execution, store, provider = await _runtime(
        [Decimal("0.20"), Decimal("0.20"), Decimal("0.20"), Decimal("0.20")]
    )
    plan = handle._entry_plan()
    assert plan is not None
    await handle._open_cycle(plan)

    assert store.active is not None and store.active.state == FundingArbCycleState.OPEN
    assert [(order.is_spot, order.side, order.reduce_only) for order in execution.orders[:2]] == [
        (True, Side.BUY, False),
        (False, Side.SELL, False),
    ]
    assert execution.leverage_updates == [("HYPE", 2, False)]

    provider.funding = replace(provider.funding, funding_rate=0.0)
    await handle._evaluate()
    assert handle.snapshot()["cycle_id"] is None
    assert [(order.is_spot, order.side, order.reduce_only) for order in execution.orders[2:]] == [
        (False, Side.BUY, True),
        (True, Side.SELL, False),
    ]


async def test_second_leg_rejection_compensates_all_acquired_spot() -> None:
    handle, execution, store, _ = await _runtime([Decimal("0.20"), Decimal("0"), Decimal("0.20")])
    plan = handle._entry_plan()
    assert plan is not None
    await handle._open_cycle(plan)

    assert handle.snapshot()["cycle_id"] is None
    assert store.active is not None and store.active.state == FundingArbCycleState.CLOSED
    assert execution.spot_total == 0
    assert execution.perp_short == 0
    assert execution.orders[-1].is_spot and execution.orders[-1].side == Side.SELL
    assert execution.orders[-1].risk_reducing is True


async def test_partial_perp_fill_keeps_only_common_hedged_size() -> None:
    handle, execution, store, _ = await _runtime([Decimal("0.20"), Decimal("0.10"), Decimal("0.10")])
    plan = handle._entry_plan()
    assert plan is not None
    await handle._open_cycle(plan)

    assert store.active is not None and store.active.state == FundingArbCycleState.OPEN
    assert store.active.spot_open_size == Decimal("0.10")
    assert store.active.perp_open_size == Decimal("0.10")
    assert execution.spot_total == Decimal("0.10")
    assert execution.perp_short == Decimal("0.10")


async def test_unknown_entry_is_not_reissued_with_a_new_cloid() -> None:
    handle, execution, store, _ = await _runtime([None])
    plan = handle._entry_plan()
    assert plan is not None
    await handle._open_cycle(plan)

    assert len(execution.orders) == 1
    assert store.active is not None and store.active.state == FundingArbCycleState.FAULTED
    assert store.active.error_code == "spot_entry_unknown"


async def test_entry_plan_requires_spot_quote_and_perp_margin_before_first_leg() -> None:
    handle, _, _, _ = await _runtime([])
    deps = handle._deps
    assert deps is not None
    now = datetime.now(UTC)
    deps.tracker.update_spot_balances(
        (SpotBalance("USDC", Size("1"), sub_account=SubAccount("0xabc"), updated_at=now),),
        observed_at=now,
    )

    assert handle._entry_plan() is None
    assert handle.snapshot()["entry_block_reason"] == "insufficient_spot_quote_balance"

    deps.tracker.update_spot_balances(
        (SpotBalance("USDC", Size("1000"), sub_account=SubAccount("0xabc"), updated_at=now),),
        observed_at=now,
    )
    deps.tracker.update_account_state(
        AccountState(Usd("500"), Usd("1"), Usd("0"), Usd("0"), Usd("500"), SubAccount("0xabc"))
    )
    assert handle._entry_plan() is None
    assert handle.snapshot()["entry_block_reason"] == "insufficient_perp_margin"


async def test_entry_snapshot_explains_basis_gate() -> None:
    handle, _, _, provider = await _runtime([])
    provider.spot_book = replace(
        provider.spot_book,
        bids=(L2Level(Price("40"), Size("10")),),
        asks=(L2Level(Price("44"), Size("10")),),
    )

    assert handle._entry_plan() is None
    snapshot = handle.snapshot()
    assert snapshot["entry_block_reason"] == "basis_exceeds_limit"
    assert Decimal(snapshot["entry_diagnostics"]["basis_bps"]) > Decimal("500")


async def test_exit_never_sells_spot_while_authoritative_perp_short_remains() -> None:
    handle, execution, store, _ = await _runtime(
        [Decimal("0.20"), Decimal("0.20"), Decimal("0"), Decimal("0"), Decimal("0")]
    )
    plan = handle._entry_plan()
    assert plan is not None
    await handle._open_cycle(plan)

    await handle._close_cycle("test_exit")

    assert store.active is not None and store.active.state == FundingArbCycleState.FAULTED
    assert store.active.error_code == "perp_exit_incomplete"
    assert execution.spot_total == Decimal("0.20")
    assert execution.perp_short == Decimal("0.20")
    assert not any(order.is_spot and order.side == Side.SELL for order in execution.orders[2:])


async def test_live_runtime_rejects_unrouted_subaccount() -> None:
    handle, execution, store, provider = await _runtime([])
    deps = handle._deps
    assert deps is not None
    with pytest.raises(StrategyLifecycleError, match="sub_account"):
        FundingArbRuntimeHandle(
            StrategyId("fa-2"),
            "HYPE",
            decode_funding_arb_config(_snapshot(), symbol="HYPE"),
            sub_account="0xdifferent",
            dependencies=deps,
        )
    del execution, store, provider
