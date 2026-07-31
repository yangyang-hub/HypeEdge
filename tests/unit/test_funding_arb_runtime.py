"""Unit tests for the funding-rate-arbitrage runtime stub."""

from __future__ import annotations

from decimal import Decimal

import pytest

from hypeedge.core.enums import MarketMakerLifecycle
from hypeedge.core.types import StrategyId
from hypeedge.strategy.funding_arb import (
    FundingArbParams,
    FundingArbRuntimeHandle,
    decode_funding_arb_config,
)
from hypeedge.strategy.registry import StrategyConfigSnapshot


def _snapshot() -> StrategyConfigSnapshot:
    return StrategyConfigSnapshot(
        StrategyId("fa-1"),
        1,
        {
            "spot_coin": "PURR",
            "entry_funding_rate": Decimal("0.0002"),
            "exit_funding_rate": Decimal("0.00005"),
            "max_notional_usd": Decimal("500"),
            "hedge_ratio": Decimal("0.9"),
            "rebalance_threshold_bps": 30,
            "leverage": Decimal("2"),
        },
    )


def test_decode_funding_arb_config_roundtrip() -> None:
    params = decode_funding_arb_config(_snapshot(), symbol="BTC")
    assert params.spot_coin == "PURR"
    assert params.entry_funding_rate == Decimal("0.0002")
    assert params.hedge_ratio == Decimal("0.9")
    assert params.rebalance_threshold_bps == 30
    assert params.leverage == Decimal("2")


def test_funding_arb_params_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="hedge_ratio"):
        FundingArbParams(hedge_ratio=Decimal("2"))
    with pytest.raises(ValueError, match="max_notional_usd"):
        FundingArbParams(max_notional_usd=Decimal("0"))


@pytest.mark.asyncio
async def test_runtime_handle_lifecycle_is_idempotent_and_no_trading() -> None:
    handle = FundingArbRuntimeHandle(StrategyId("fa-1"), "BTC", decode_funding_arb_config(_snapshot(), symbol="BTC"))
    await handle.start()
    await handle.start()  # idempotent — must not raise
    # shadow / warming are skipped (unsupported; gated upstream by capabilities).
    await handle.set_mode(MarketMakerLifecycle.SHADOW)
    await handle.set_mode(MarketMakerLifecycle.RUNNING)
    await handle.set_mode(MarketMakerLifecycle.PAUSED)
    await handle.set_mode(MarketMakerLifecycle.RUNNING)
    await handle.stop()
    await handle.stop()  # idempotent — must not raise
    # apply_config only re-decodes params; it must not place orders or raise.
    await handle.apply_config(_snapshot())


@pytest.mark.asyncio
async def test_open_and_close_legs_on_funding_signal() -> None:
    from unittest.mock import AsyncMock

    from hypeedge.core.enums import Side
    from hypeedge.core.models import FundingRate, L2BookSnapshot, L2Level
    from hypeedge.core.types import Price, Size, Symbol, Timestamp
    from hypeedge.strategy.funding_arb import FundingArbRuntimeHandle, decode_funding_arb_config

    params = decode_funding_arb_config(_snapshot(), symbol="BTC")  # entry=0.0002, exit=0.00005
    perp_book = L2BookSnapshot(
        Symbol("BTC"), (L2Level(Price(100.0), Size(10)),), (L2Level(Price(101.0), Size(10)),), Timestamp(0)
    )
    spot_book = L2BookSnapshot(
        Symbol("PURR"), (L2Level(Price(1.0), Size(1000)),), (L2Level(Price(1.01), Size(1000)),), Timestamp(0)
    )

    class _FakeProvider:
        def __init__(self) -> None:
            self.funding = FundingRate(Symbol("BTC"), 0.001, 0.0, Price(100.0), 0.0, Timestamp(0))

        def get_funding(self, symbol: Symbol) -> FundingRate:
            return self.funding

        def get_book(self, symbol: Symbol) -> L2BookSnapshot:
            return perp_book

        def get_spot_book(self, symbol: Symbol) -> L2BookSnapshot:
            return spot_book

    provider = _FakeProvider()
    execution = AsyncMock()
    handle = FundingArbRuntimeHandle(
        StrategyId("fa-1"),
        "BTC",
        params,
        execution=execution,
        provider=provider,  # type: ignore[arg-type]
    )

    # Entry: funding 0.001 > entry 0.0002 -> open perp short + spot long.
    await handle._evaluate()
    assert execution.submit_order.await_count == 2
    open_intents = [call.args[0] for call in execution.submit_order.await_args_list]
    assert open_intents[0].is_spot is False
    assert open_intents[0].side == Side.SELL
    assert open_intents[1].is_spot is True
    assert open_intents[1].side == Side.BUY
    assert handle._position_open is True

    execution.submit_order.reset_mock()
    # Exit: funding 0.00001 < exit 0.00005 -> close both legs.
    provider.funding = FundingRate(Symbol("BTC"), 0.00001, 0.0, Price(100.0), 0.0, Timestamp(0))
    await handle._evaluate()
    assert execution.submit_order.await_count == 2
    close_intents = [call.args[0] for call in execution.submit_order.await_args_list]
    assert close_intents[0].is_spot is False
    assert close_intents[0].reduce_only is True
    assert close_intents[0].side == Side.BUY
    assert close_intents[1].is_spot is True
    assert close_intents[1].side == Side.SELL
    assert handle._position_open is False
