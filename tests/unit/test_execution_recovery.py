"""Orphan and UNKNOWN recovery state tests."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from hypeedge.core.enums import OrderStatus, Side
from hypeedge.core.types import Cloid, OrderId, Price, Size, StrategyId, Symbol
from hypeedge.execution.recovery import (
    RecoveryOwner,
    RecoveryReason,
    RecoveryRegistry,
    RecoveryStatus,
    classify_orphan,
)
from hypeedge.trading.quotes import QuoteRiskOwner, QuoteSlotKey

NOW = datetime(2026, 1, 1, tzinfo=UTC)
SLOT = QuoteSlotKey(StrategyId("mm"), Symbol("BTC"), Side.BUY)


def owner(status: OrderStatus, revision: int = 3) -> QuoteRiskOwner:
    return QuoteRiskOwner(
        OrderId("oid"),
        Cloid("cloid"),
        Price("100"),
        Size("1"),
        status,
        revision,
        NOW,
    )


def test_unknown_and_late_revision_classification() -> None:
    assert classify_orphan(owner(OrderStatus.SUBMIT_UNKNOWN), active_plan_revision=3) == RecoveryReason.SUBMIT_UNKNOWN
    assert classify_orphan(owner(OrderStatus.CANCEL_UNKNOWN), active_plan_revision=3) == RecoveryReason.CANCEL_UNKNOWN
    assert (
        classify_orphan(owner(OrderStatus.ACKNOWLEDGED, 2), active_plan_revision=3) == RecoveryReason.LATE_OLD_REVISION
    )
    assert classify_orphan(owner(OrderStatus.ACKNOWLEDGED), active_plan_revision=3) is None


def test_recovery_owner_blocks_until_exchange_proves_terminal() -> None:
    recovery = RecoveryOwner(SLOT, owner(OrderStatus.CANCEL_UNKNOWN), RecoveryReason.CANCEL_UNKNOWN, NOW)
    registry = RecoveryRegistry().register(recovery).register(recovery)
    assert len(registry.owners) == 1
    assert registry.placement_blocked(SLOT)

    still_live = registry.reconcile(Cloid("cloid"), OrderStatus.ACKNOWLEDGED)
    assert still_live.placement_blocked(SLOT)

    resolved = still_live.reconcile(Cloid("cloid"), OrderStatus.CANCELLED)
    assert not resolved.placement_blocked(SLOT)
    assert resolved.owners[0].status == RecoveryStatus.RESOLVED_TERMINAL


def test_orphan_cancel_pending_still_counts_as_possible_live_owner() -> None:
    recovery = RecoveryOwner(SLOT, owner(OrderStatus.ACKNOWLEDGED, 2), RecoveryReason.LATE_OLD_REVISION, NOW)
    pending = recovery.mark_cancel_pending()
    assert pending.status == RecoveryStatus.CANCEL_PENDING
    assert pending.blocks_placement

    resolved = pending.reconcile(OrderStatus.FILLED)
    with pytest.raises(ValueError, match="terminal"):
        resolved.mark_cancel_pending()


def test_registry_rejects_duplicate_projection_and_unknown_lookup() -> None:
    recovery = RecoveryOwner(SLOT, owner(OrderStatus.ACKNOWLEDGED), RecoveryReason.UNATTRIBUTED_LIVE, NOW)
    with pytest.raises(ValueError, match="unique"):
        RecoveryRegistry((recovery, recovery))
    with pytest.raises(KeyError):
        RecoveryRegistry((recovery,)).reconcile(Cloid("missing"), OrderStatus.CANCELLED)
