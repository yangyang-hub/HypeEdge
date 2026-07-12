"""Database-independent, WAL-backed emergency cancellation path.

This module intentionally exposes no placement operation.  It queries
exchange-authoritative open orders and sends cancellations through the same
serialized signing boundary used by normal execution.  Every network attempt
is preceded by an append + fsync journal record so Postgres recovery can later
reconstruct what may have reached the exchange.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import structlog

from hypeedge.core.exceptions import ExecutionError
from hypeedge.core.types import Cloid
from hypeedge.execution.cloid import CloidGenerator

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class EmergencyCancelTarget:
    """Exchange-authoritative order identifier safe for cancellation only."""

    symbol: str
    cloid: str | None = None
    oid: int | str | None = None

    def __post_init__(self) -> None:
        if not self.symbol:
            raise ValueError("emergency cancel target requires a symbol")
        if self.cloid is None and self.oid is None:
            raise ValueError("emergency cancel target requires cloid or oid")

    @property
    def key(self) -> str:
        if self.cloid is not None:
            return f"cloid:{_canonical_cloid(self.cloid)}"
        return f"oid:{self.symbol}:{self.oid}"


@dataclass(frozen=True)
class EmergencyCancelResult:
    """Authoritatively verified outcome for one cancellation target."""

    target: EmergencyCancelTarget
    success: bool
    outcome: str
    attempt_id: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class EmergencyCancelBatchResult:
    """Result of cancel-all or WAL recovery."""

    requested: int
    cancelled: int
    unresolved: tuple[EmergencyCancelTarget, ...]

    @property
    def success(self) -> bool:
        return not self.unresolved


class EmergencyCancelExecutor(Protocol):
    """Strict cancel-only execution boundary used during DB failure/halting."""

    async def cancel(self, target: EmergencyCancelTarget) -> EmergencyCancelResult: ...

    async def cancel_all(self, symbol: str | None = None) -> EmergencyCancelBatchResult: ...

    async def recover_pending(self) -> EmergencyCancelBatchResult: ...


class AuthoritativeOpenOrderProvider(Protocol):
    """Read exchange truth; an invalid response must raise, never become []."""

    async def get_open_orders(self) -> list[EmergencyCancelTarget]: ...


class SerialSignedActionExecutor(Protocol):
    """The existing nonce-serialized signing outlet required by this path."""

    @property
    def exchange(self) -> Any: ...

    async def submit(
        self,
        action_fn: Any,
        *args: Any,
        cloid_hint: str | None = None,
        **kwargs: Any,
    ) -> Any: ...


@dataclass(frozen=True)
class EmergencyJournalRecord:
    """One immutable JSONL emergency journal fact."""

    attempt_id: str
    event: str
    recorded_at: str
    target: dict[str, object]
    outcome: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class PendingEmergencyAttempt:
    attempt_id: str
    target: EmergencyCancelTarget


class EmergencyCancelJournal:
    """Append-only JSONL journal; every append is flushed and fsynced."""

    _TERMINAL_EVENTS = frozenset({"verified_absent", "already_absent", "recovery_resolved"})

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._lock = asyncio.Lock()

    @property
    def path(self) -> Path:
        return self._path

    async def append(
        self,
        *,
        attempt_id: str,
        event: str,
        target: EmergencyCancelTarget,
        outcome: str | None = None,
        error: str | None = None,
    ) -> None:
        record = EmergencyJournalRecord(
            attempt_id=attempt_id,
            event=event,
            recorded_at=datetime.now(UTC).isoformat(),
            target={"symbol": target.symbol, "cloid": target.cloid, "oid": target.oid},
            outcome=outcome,
            error=error,
        )
        payload = (json.dumps(asdict(record), separators=(",", ":"), sort_keys=True) + "\n").encode()
        async with self._lock:
            await asyncio.to_thread(self._append_sync, payload)

    async def read_records(self) -> tuple[EmergencyJournalRecord, ...]:
        return await asyncio.to_thread(self._read_records_sync)

    async def pending_attempts(self) -> tuple[PendingEmergencyAttempt, ...]:
        records = await self.read_records()
        pending: dict[str, EmergencyCancelTarget] = {}
        for record in records:
            if record.event == "dispatch_intent":
                pending[record.attempt_id] = _target_from_mapping(record.target)
            elif record.event in self._TERMINAL_EVENTS:
                pending.pop(record.attempt_id, None)
        return tuple(PendingEmergencyAttempt(attempt_id, target) for attempt_id, target in pending.items())

    def _append_sync(self, payload: bytes) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self._path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        try:
            os.fchmod(fd, 0o600)
            offset = 0
            while offset < len(payload):
                offset += os.write(fd, payload[offset:])
            os.fsync(fd)
        finally:
            os.close(fd)

    def _read_records_sync(self) -> tuple[EmergencyJournalRecord, ...]:
        try:
            data = self._path.read_bytes()
        except FileNotFoundError:
            return ()
        lines = data.splitlines(keepends=True)
        records: list[EmergencyJournalRecord] = []
        for index, raw_line in enumerate(lines):
            if not raw_line.strip():
                continue
            try:
                decoded = json.loads(raw_line)
                records.append(EmergencyJournalRecord(**decoded))
            except (json.JSONDecodeError, TypeError) as exc:
                is_torn_tail = index == len(lines) - 1 and not raw_line.endswith(b"\n")
                if is_torn_tail:
                    logger.warning("emergency_journal_torn_tail_ignored", path=str(self._path))
                    break
                raise ExecutionError(f"Malformed emergency cancel journal: path={self._path} line={index + 1}") from exc
        return tuple(records)


class SdkAuthoritativeOpenOrderProvider:
    """Adapter for the Hyperliquid SDK's synchronous ``open_orders`` query."""

    def __init__(self, info_client: Any, account_address: str) -> None:
        if not account_address:
            raise ValueError("account_address must not be empty")
        self._info = info_client
        self._account_address = account_address

    async def get_open_orders(self) -> list[EmergencyCancelTarget]:
        raw = await asyncio.to_thread(self._info.open_orders, self._account_address)
        if not isinstance(raw, list):
            raise ExecutionError("Invalid authoritative open-orders response")
        targets: list[EmergencyCancelTarget] = []
        for item in raw:
            if not isinstance(item, dict):
                raise ExecutionError("Invalid authoritative open-order item")
            symbol = item.get("coin") or item.get("symbol")
            cloid = item.get("cloid")
            oid = item.get("oid")
            if not isinstance(symbol, str) or not symbol or (not cloid and oid is None):
                raise ExecutionError("Authoritative open order lacks symbol and cancel identifier")
            targets.append(
                EmergencyCancelTarget(
                    symbol=symbol,
                    cloid=str(cloid) if cloid else None,
                    oid=oid if isinstance(oid, (int, str)) else None,
                )
            )
        return targets


class WalEmergencyCancelExecutor:
    """WAL-backed cancel implementation using the sole nonce signing queue."""

    def __init__(
        self,
        signed_actions: SerialSignedActionExecutor,
        open_orders: AuthoritativeOpenOrderProvider,
        journal: EmergencyCancelJournal,
    ) -> None:
        self._signed_actions = signed_actions
        self._open_orders = open_orders
        self._journal = journal
        self._operation_lock = asyncio.Lock()

    async def cancel(self, target: EmergencyCancelTarget) -> EmergencyCancelResult:
        async with self._operation_lock:
            authoritative = await self._open_orders.get_open_orders()
            matched = _find_target(authoritative, target)
            if matched is None:
                attempt_id = str(uuid.uuid4())
                await self._journal.append(attempt_id=attempt_id, event="already_absent", target=target)
                return EmergencyCancelResult(target, True, "already_absent", attempt_id)
            await self._dispatch(matched)
            remaining = await self._open_orders.get_open_orders()
            return await self._verify(matched, remaining)

    async def cancel_all(self, symbol: str | None = None) -> EmergencyCancelBatchResult:
        async with self._operation_lock:
            authoritative = await self._open_orders.get_open_orders()
            targets = [target for target in authoritative if symbol is None or target.symbol == symbol]
            attempts: dict[str, str] = {}
            for target in targets:
                attempts[target.key] = await self._dispatch(target)

            remaining = await self._open_orders.get_open_orders()
            unresolved: list[EmergencyCancelTarget] = []
            for target in targets:
                result = await self._verify(target, remaining, attempt_id=attempts[target.key])
                if not result.success:
                    unresolved.append(target)
            return EmergencyCancelBatchResult(
                requested=len(targets),
                cancelled=len(targets) - len(unresolved),
                unresolved=tuple(unresolved),
            )

    async def recover_pending(self) -> EmergencyCancelBatchResult:
        """Replay unresolved intents only when the target is still authoritatively open."""
        async with self._operation_lock:
            pending = await self._journal.pending_attempts()
            authoritative = await self._open_orders.get_open_orders()
            unresolved: list[EmergencyCancelTarget] = []
            cancelled = 0
            for old_attempt in pending:
                matched = _find_target(authoritative, old_attempt.target)
                if matched is None:
                    await self._journal.append(
                        attempt_id=old_attempt.attempt_id,
                        event="recovery_resolved",
                        target=old_attempt.target,
                        outcome="already_absent",
                    )
                    cancelled += 1
                    continue
                new_attempt_id = await self._dispatch(matched)
                remaining = await self._open_orders.get_open_orders()
                result = await self._verify(matched, remaining, attempt_id=new_attempt_id)
                if result.success:
                    await self._journal.append(
                        attempt_id=old_attempt.attempt_id,
                        event="recovery_resolved",
                        target=old_attempt.target,
                        outcome="cancelled",
                    )
                    cancelled += 1
                    authoritative = remaining
                else:
                    unresolved.append(old_attempt.target)
            return EmergencyCancelBatchResult(len(pending), cancelled, tuple(unresolved))

    async def _dispatch(self, target: EmergencyCancelTarget) -> str:
        attempt_id = str(uuid.uuid4())
        await self._journal.append(attempt_id=attempt_id, event="dispatch_intent", target=target)
        exchange = self._signed_actions.exchange
        if exchange is None:
            error = "signed_action_exchange_not_configured"
            await self._journal.append(
                attempt_id=attempt_id,
                event="transport_error",
                target=target,
                error=error,
            )
            raise ExecutionError(error)
        try:
            if target.cloid is not None:
                response = await self._signed_actions.submit(
                    exchange.cancel_by_cloid,
                    target.symbol,
                    _sdk_cloid(target.cloid),
                    cloid_hint=_canonical_cloid(target.cloid),
                )
            else:
                response = await self._signed_actions.submit(exchange.cancel, target.symbol, target.oid)
            await self._journal.append(
                attempt_id=attempt_id,
                event="transport_result",
                target=target,
                outcome=_transport_outcome(response),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._journal.append(
                attempt_id=attempt_id,
                event="transport_error",
                target=target,
                error=f"{type(exc).__name__}:{exc}",
            )
            logger.exception("emergency_cancel_transport_failed", target=target.key, attempt_id=attempt_id)
        return attempt_id

    async def _verify(
        self,
        target: EmergencyCancelTarget,
        remaining: list[EmergencyCancelTarget],
        *,
        attempt_id: str | None = None,
    ) -> EmergencyCancelResult:
        resolved_attempt_id = attempt_id
        if resolved_attempt_id is None:
            pending = await self._journal.pending_attempts()
            resolved_attempt_id = next(
                (item.attempt_id for item in reversed(pending) if item.target.key == target.key),
                str(uuid.uuid4()),
            )
        if _find_target(remaining, target) is None:
            await self._journal.append(
                attempt_id=resolved_attempt_id,
                event="verified_absent",
                target=target,
                outcome="cancelled",
            )
            return EmergencyCancelResult(target, True, "cancelled", resolved_attempt_id)
        await self._journal.append(
            attempt_id=resolved_attempt_id,
            event="verified_open",
            target=target,
            outcome="still_open",
        )
        return EmergencyCancelResult(target, False, "still_open", resolved_attempt_id, "authoritative_order_still_open")


def _find_target(
    candidates: list[EmergencyCancelTarget],
    target: EmergencyCancelTarget,
) -> EmergencyCancelTarget | None:
    return next((candidate for candidate in candidates if candidate.key == target.key), None)


def _canonical_cloid(cloid: str) -> str:
    return CloidGenerator.to_hl_cloid(Cloid(cloid))


def _sdk_cloid(cloid: str) -> Any:
    from hyperliquid.utils.types import Cloid as HlCloid

    return HlCloid.from_str(_canonical_cloid(cloid))


def _transport_outcome(response: object) -> str:
    if not isinstance(response, dict):
        return "unknown_response"
    return str(response.get("status", "unknown_response"))


def _target_from_mapping(raw: dict[str, object]) -> EmergencyCancelTarget:
    symbol = raw.get("symbol")
    cloid = raw.get("cloid")
    oid = raw.get("oid")
    if not isinstance(symbol, str):
        raise ExecutionError("Emergency journal target has invalid symbol")
    return EmergencyCancelTarget(
        symbol=symbol,
        cloid=str(cloid) if cloid is not None else None,
        oid=oid if isinstance(oid, (int, str)) else None,
    )
