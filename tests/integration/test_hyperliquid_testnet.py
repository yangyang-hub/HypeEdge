"""Credential-gated end-to-end tests against Hyperliquid testnet only."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest

from hypeedge.core.enums import OrderStatus, SafetyMode

pytestmark = [pytest.mark.integration, pytest.mark.testnet]


async def test_resting_limit_can_be_queried_and_cancelled(testnet_harness: Any) -> None:
    order = await testnet_harness.place_resting()

    queried = await testnet_harness.query(str(order.cloid))
    assert queried["status"] == "order"
    assert queried["order"]["status"].lower() == "open"

    assert await testnet_harness.engine.cancel_order(str(order.cloid)) is True
    await testnet_harness.wait_until_not_open(str(order.cloid))
    assert order.status == OrderStatus.CANCELLED


async def test_repeated_canonical_cloid_is_idempotent(testnet_harness: Any) -> None:
    canonical_cloid = "0x" + uuid.uuid4().hex
    first = await testnet_harness.place_resting(canonical_cloid)

    second = await testnet_harness.engine.submit_order(testnet_harness.make_intent(canonical_cloid))
    testnet_harness.cleanup_cloids.add(canonical_cloid)

    matching = [item for item in await testnet_harness.open_orders() if str(item.get("cloid", "")) == canonical_cloid]
    assert len(matching) == 1, "repeating a cloid must never create a second exchange order"
    assert second is first, "execution idempotency must return the already-known order for the same canonical cloid"

    assert await testnet_harness.engine.cancel_order(canonical_cloid) is True
    await testnet_harness.wait_until_not_open(canonical_cloid)


async def test_kill_switch_cancels_orders_and_requires_recovery(testnet_harness: Any) -> None:
    order = await testnet_harness.place_resting()

    testnet_harness.app.kill_switch.trigger("testnet_integration_gate")
    async with asyncio.timeout(5.0):
        while not testnet_harness.app._kill_switch_active:
            await asyncio.sleep(0.05)
    await testnet_harness.wait_until_not_open(str(order.cloid))
    assert testnet_harness.app.kill_switch.is_active is True
    assert testnet_harness.app.safety_mode in {SafetyMode.HALTING.value, SafetyMode.HALTED.value}

    rejected = await testnet_harness.engine.submit_order(testnet_harness.make_intent())
    assert rejected.status == OrderStatus.REJECTED
    assert rejected.error_message is not None

    assert await testnet_harness.app.recover_from_kill_switch() is True
    assert testnet_harness.app.kill_switch.is_active is False
    assert testnet_harness.app.trading_enabled is True
    assert testnet_harness.app.safety_mode == SafetyMode.NORMAL.value
