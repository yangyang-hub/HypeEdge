"""Unified, fail-closed admission boundary for all trading commands."""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol

import structlog

from hypeedge.core.exceptions import TradingCommandConflictError, TradingCommandPersistenceError
from hypeedge.core.models import OrderIntent, RiskCheckResult
from hypeedge.core.types import Price, StrategyId, Symbol
from hypeedge.risk.action_budget import ActionBudgetController, BudgetAction

logger = structlog.get_logger(__name__)


class TradingCommandKind(StrEnum):
    PLACE = "place"
    CANCEL = "cancel"
    CANCEL_ALL = "cancel_all"


class TradingCommandStatus(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class GateDecision:
    """A named admission decision; false or unknown always means rejection."""

    allowed: bool
    reason: str | None = None

    @classmethod
    def allow(cls) -> GateDecision:
        return cls(True)

    @classmethod
    def reject(cls, reason: str) -> GateDecision:
        return cls(False, reason)


@dataclass(frozen=True, slots=True)
class DataHealthDecision:
    """Data-health decision plus the immutable market context used downstream."""

    allowed: bool
    reason: str | None = None
    reference_price: Price | None = None
    best_bid: Price | None = None
    best_ask: Price | None = None
    market_version: int | None = None
    connection_generation: int | None = None

    @classmethod
    def reject(cls, reason: str) -> DataHealthDecision:
        return cls(False, reason)


@dataclass(frozen=True, slots=True)
class TradingCommand:
    """Canonical command persisted before any signed side effect."""

    command_id: uuid.UUID
    kind: TradingCommandKind
    status: TradingCommandStatus
    created_at: datetime
    intent: OrderIntent | None = None
    target_cloid: str | None = None
    symbol: Symbol | None = None
    strategy_id: StrategyId | None = None
    rejection_gate: str | None = None
    rejection_reason: str | None = None
    risk_result: RiskCheckResult | None = None
    market_version: int | None = None
    connection_generation: int | None = None


@dataclass(frozen=True, slots=True)
class TradingCommandReceipt:
    """Durable acknowledgement. ACCEPTED means queued, not exchange-acknowledged."""

    command_id: uuid.UUID
    kind: TradingCommandKind
    status: TradingCommandStatus
    created_at: datetime
    intent: OrderIntent | None = None
    target_cloid: str | None = None
    symbol: Symbol | None = None
    rejection_gate: str | None = None
    rejection_reason: str | None = None

    @property
    def accepted(self) -> bool:
        return self.status == TradingCommandStatus.ACCEPTED


class SafetyPlacementGate(Protocol):
    def check_placement(self, intent: OrderIntent) -> None: ...


class DataHealthGate(Protocol):
    async def check_placement(self, intent: OrderIntent) -> DataHealthDecision: ...


class RiskAdmissionGate(Protocol):
    async def check(self, intent: OrderIntent, *, reference_price: float | None = None) -> RiskCheckResult: ...


class ActionBudgetAdmissionGate(Protocol):
    async def check_placement(self, intent: OrderIntent) -> GateDecision: ...


class OrderIntentNormalizer(Protocol):
    def normalize(
        self,
        intent: OrderIntent,
        *,
        best_bid: Price | None = None,
        best_ask: Price | None = None,
    ) -> OrderIntent: ...


class DurableTradingCommandSink(Protocol):
    async def persist(self, command: TradingCommand) -> TradingCommandReceipt: ...


class TradingCommandClient(Protocol):
    async def submit_order(
        self,
        intent: OrderIntent,
        *,
        command_id: uuid.UUID | None = None,
    ) -> TradingCommandReceipt: ...

    async def cancel_order(
        self,
        cloid: str,
        *,
        strategy_id: StrategyId | None = None,
        command_id: uuid.UUID | None = None,
    ) -> TradingCommandReceipt: ...

    async def cancel_all_orders(
        self,
        symbol: Symbol | None = None,
        *,
        strategy_id: StrategyId | None = None,
        command_id: uuid.UUID | None = None,
    ) -> TradingCommandReceipt: ...


class TradingCommandService:
    """Admit placements in the one authorized order, then persist them."""

    def __init__(
        self,
        *,
        safety: SafetyPlacementGate,
        data_health: DataHealthGate,
        risk: RiskAdmissionGate,
        action_budget: ActionBudgetAdmissionGate,
        normalizer: OrderIntentNormalizer,
        sink: DurableTradingCommandSink,
    ) -> None:
        self._safety = safety
        self._data_health = data_health
        self._risk = risk
        self._action_budget = action_budget
        self._normalizer = normalizer
        self._sink = sink

    async def submit_order(
        self,
        intent: OrderIntent,
        *,
        command_id: uuid.UUID | None = None,
    ) -> TradingCommandReceipt:
        command_id = command_id or uuid.uuid4()
        created_at = datetime.now(UTC)

        try:
            self._safety.check_placement(intent)
        except Exception as exc:
            return await self._persist_rejection(command_id, created_at, intent, "safety", exc)

        try:
            data = await self._data_health.check_placement(intent)
        except Exception as exc:
            return await self._persist_rejection(command_id, created_at, intent, "data_health", exc)
        if not data.allowed:
            return await self._persist_rejection(
                command_id, created_at, intent, "data_health", data.reason or "data_health_rejected"
            )

        reference_price = float(data.reference_price) if data.reference_price is not None else None
        try:
            risk_result = await self._risk.check(intent, reference_price=reference_price)
        except Exception as exc:
            return await self._persist_rejection(command_id, created_at, intent, "risk", exc, data=data)
        if not risk_result.passed:
            return await self._persist_rejection(
                command_id,
                created_at,
                intent,
                "risk",
                risk_result.reason or "risk_rejected",
                risk_result=risk_result,
                data=data,
            )

        try:
            budget = await self._action_budget.check_placement(intent)
        except Exception as exc:
            return await self._persist_rejection(
                command_id, created_at, intent, "action_budget", exc, risk_result=risk_result, data=data
            )
        if not budget.allowed:
            return await self._persist_rejection(
                command_id,
                created_at,
                intent,
                "action_budget",
                budget.reason or "action_budget_rejected",
                risk_result=risk_result,
                data=data,
            )

        try:
            normalized = self._normalizer.normalize(intent, best_bid=data.best_bid, best_ask=data.best_ask)
        except Exception as exc:
            return await self._persist_rejection(
                command_id, created_at, intent, "normalize", exc, risk_result=risk_result, data=data
            )

        return await self._persist(
            TradingCommand(
                command_id=command_id,
                kind=TradingCommandKind.PLACE,
                status=TradingCommandStatus.ACCEPTED,
                created_at=created_at,
                intent=normalized,
                strategy_id=normalized.strategy_id,
                risk_result=risk_result,
                market_version=data.market_version,
                connection_generation=data.connection_generation,
            )
        )

    async def submit_placement(
        self,
        intent: OrderIntent,
        *,
        command_id: uuid.UUID | None = None,
    ) -> TradingCommandReceipt:
        return await self.submit_order(intent, command_id=command_id)

    async def cancel_order(
        self,
        cloid: str,
        *,
        strategy_id: StrategyId | None = None,
        command_id: uuid.UUID | None = None,
    ) -> TradingCommandReceipt:
        if not cloid:
            raise ValueError("cloid is required")
        return await self._persist(
            TradingCommand(
                command_id=command_id or uuid.uuid4(),
                kind=TradingCommandKind.CANCEL,
                status=TradingCommandStatus.ACCEPTED,
                created_at=datetime.now(UTC),
                target_cloid=cloid,
                strategy_id=strategy_id,
            )
        )

    async def cancel_all_orders(
        self,
        symbol: Symbol | None = None,
        *,
        strategy_id: StrategyId | None = None,
        command_id: uuid.UUID | None = None,
    ) -> TradingCommandReceipt:
        return await self._persist(
            TradingCommand(
                command_id=command_id or uuid.uuid4(),
                kind=TradingCommandKind.CANCEL_ALL,
                status=TradingCommandStatus.ACCEPTED,
                created_at=datetime.now(UTC),
                symbol=symbol,
                strategy_id=strategy_id,
            )
        )

    async def _persist_rejection(
        self,
        command_id: uuid.UUID,
        created_at: datetime,
        intent: OrderIntent,
        gate: str,
        error: Exception | str,
        *,
        risk_result: RiskCheckResult | None = None,
        data: DataHealthDecision | None = None,
    ) -> TradingCommandReceipt:
        reason = self._safe_reason(error)
        logger.warning(
            "trading_command_rejected",
            command_id=str(command_id),
            gate=gate,
            reason=reason,
            strategy_id=str(intent.strategy_id) if intent.strategy_id else None,
            symbol=str(intent.symbol),
        )
        return await self._persist(
            TradingCommand(
                command_id=command_id,
                kind=TradingCommandKind.PLACE,
                status=TradingCommandStatus.REJECTED,
                created_at=created_at,
                intent=intent,
                strategy_id=intent.strategy_id,
                rejection_gate=gate,
                rejection_reason=reason,
                risk_result=risk_result,
                market_version=data.market_version if data else None,
                connection_generation=data.connection_generation if data else None,
            )
        )

    async def _persist(self, command: TradingCommand) -> TradingCommandReceipt:
        try:
            return await self._sink.persist(command)
        except TradingCommandConflictError:
            raise
        except Exception as exc:
            raise TradingCommandPersistenceError(
                f"Durable command persistence failed: command_id={command.command_id} kind={command.kind.value}"
            ) from exc

    @staticmethod
    def _safe_reason(error: Exception | str) -> str:
        reason = str(error).strip()
        if reason:
            return reason
        return error.__class__.__name__ if isinstance(error, Exception) else "rejected"


class ActionBudgetControllerAdapter:
    """Expose the synchronous scope controller as the command-service gate."""

    def __init__(self, controller: ActionBudgetController) -> None:
        self._controller = controller

    async def check_placement(self, intent: OrderIntent) -> GateDecision:
        permission = self._controller.permission(
            BudgetAction.PLACE,
            strategy_id=intent.strategy_id,
            symbol=intent.symbol if intent.strategy_id is not None else None,
            child_actions=1,
            ip_weight=1,
            risk_reducing=intent.reduce_only,
        )
        return GateDecision(permission.allowed, permission.reason)


class InMemoryTradingCommandSink:
    """Deterministic idempotent sink for unit tests and non-production simulations."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._commands: dict[uuid.UUID, tuple[str, TradingCommandReceipt]] = {}

    @property
    def receipts(self) -> tuple[TradingCommandReceipt, ...]:
        return tuple(receipt for _, receipt in self._commands.values())

    async def persist(self, command: TradingCommand) -> TradingCommandReceipt:
        fingerprint = self._fingerprint(command)
        async with self._lock:
            existing = self._commands.get(command.command_id)
            if existing is not None:
                existing_fingerprint, receipt = existing
                if existing_fingerprint != fingerprint:
                    raise TradingCommandConflictError(
                        f"Command id {command.command_id} was reused with a different payload"
                    )
                return receipt
            receipt = TradingCommandReceipt(
                command_id=command.command_id,
                kind=command.kind,
                status=command.status,
                created_at=command.created_at,
                intent=command.intent,
                target_cloid=command.target_cloid,
                symbol=command.symbol,
                rejection_gate=command.rejection_gate,
                rejection_reason=command.rejection_reason,
            )
            self._commands[command.command_id] = (fingerprint, receipt)
            return receipt

    @staticmethod
    def _fingerprint(command: TradingCommand) -> str:
        intent = command.intent
        payload: dict[str, Any] = {
            "kind": command.kind.value,
            "status": command.status.value,
            "target_cloid": command.target_cloid,
            "symbol": str(command.symbol) if command.symbol else None,
            "strategy_id": str(command.strategy_id) if command.strategy_id else None,
            "rejection_gate": command.rejection_gate,
            "rejection_reason": command.rejection_reason,
        }
        if intent is not None:
            payload["intent"] = {
                "symbol": str(intent.symbol),
                "side": intent.side.value,
                "size": str(intent.size),
                "price": str(intent.price) if intent.price is not None else None,
                "order_type": intent.order_type.value,
                "time_in_force": intent.time_in_force.value,
                "strategy_id": str(intent.strategy_id) if intent.strategy_id else None,
                "sub_account": str(intent.sub_account) if intent.sub_account else None,
                "reduce_only": intent.reduce_only,
                "cloid": str(intent.cloid) if intent.cloid else None,
                "client_id": intent.client_id,
            }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()
