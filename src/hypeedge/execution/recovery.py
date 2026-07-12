"""UNKNOWN and orphan recovery projections."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum

from hypeedge.core.enums import OrderStatus
from hypeedge.core.types import Cloid
from hypeedge.trading.quotes import QuoteRiskOwner, QuoteSlotKey


class RecoveryReason(StrEnum):
    SUBMIT_UNKNOWN = "submit_unknown"
    CANCEL_UNKNOWN = "cancel_unknown"
    MODIFY_UNKNOWN = "modify_unknown"
    LATE_OLD_REVISION = "late_old_revision"
    UNATTRIBUTED_LIVE = "unattributed_live"


class RecoveryStatus(StrEnum):
    REQUIRED = "required"
    CANCEL_PENDING = "cancel_pending"
    RESOLVED_TERMINAL = "resolved_terminal"


@dataclass(frozen=True, slots=True)
class RecoveryOwner:
    slot: QuoteSlotKey
    owner: QuoteRiskOwner
    reason: RecoveryReason
    discovered_at: datetime
    status: RecoveryStatus = RecoveryStatus.REQUIRED

    @property
    def blocks_placement(self) -> bool:
        return self.status != RecoveryStatus.RESOLVED_TERMINAL

    def mark_cancel_pending(self) -> RecoveryOwner:
        if self.status == RecoveryStatus.RESOLVED_TERMINAL:
            raise ValueError("terminal recovery owner cannot be cancelled again")
        return replace(self, status=RecoveryStatus.CANCEL_PENDING)

    def reconcile(self, authoritative_status: OrderStatus) -> RecoveryOwner:
        if authoritative_status in {
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }:
            return replace(self, status=RecoveryStatus.RESOLVED_TERMINAL)
        return self


@dataclass(frozen=True, slots=True)
class RecoveryRegistry:
    owners: tuple[RecoveryOwner, ...] = ()

    def __post_init__(self) -> None:
        if len({owner.owner.cloid for owner in self.owners}) != len(self.owners):
            raise ValueError("recovery owner cloids must be unique")

    def register(self, recovery: RecoveryOwner) -> RecoveryRegistry:
        for existing in self.owners:
            if existing.owner.cloid == recovery.owner.cloid:
                return self
        return replace(self, owners=(*self.owners, recovery))

    def placement_blocked(self, slot: QuoteSlotKey) -> bool:
        return any(owner.slot == slot and owner.blocks_placement for owner in self.owners)

    def unresolved(self) -> tuple[RecoveryOwner, ...]:
        """Return durable recovery facts which still represent possible live risk."""
        return tuple(owner for owner in self.owners if owner.blocks_placement)

    def oldest_unresolved_age(self, *, now: datetime) -> timedelta | None:
        """Age of the oldest possible-live owner for SLA and lifecycle gates."""
        unresolved = self.unresolved()
        if not unresolved:
            return None
        discovered_at = min(owner.discovered_at for owner in unresolved)
        return max(timedelta(0), now - discovered_at)

    def sla_exceeded(self, *, now: datetime, sla: timedelta) -> bool:
        """Fail-safe signal consumed by CANCEL_ONLY/FAULTED supervisors."""
        if sla <= timedelta(0):
            raise ValueError("recovery SLA must be positive")
        age = self.oldest_unresolved_age(now=now)
        return age is not None and age > sla

    def reconcile(self, cloid: Cloid, status: OrderStatus) -> RecoveryRegistry:
        found = False
        updated: list[RecoveryOwner] = []
        for owner in self.owners:
            if owner.owner.cloid == cloid:
                found = True
                updated.append(owner.reconcile(status))
            else:
                updated.append(owner)
        if not found:
            raise KeyError(cloid)
        return replace(self, owners=tuple(updated))


def classify_orphan(owner: QuoteRiskOwner, *, active_plan_revision: int) -> RecoveryReason | None:
    if owner.status == OrderStatus.SUBMIT_UNKNOWN:
        return RecoveryReason.SUBMIT_UNKNOWN
    if owner.status == OrderStatus.CANCEL_UNKNOWN:
        return RecoveryReason.CANCEL_UNKNOWN
    if owner.plan_revision < active_plan_revision:
        return RecoveryReason.LATE_OLD_REVISION
    return None
