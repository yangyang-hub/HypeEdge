"""Durable API command idempotency and mutation audit service."""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from fastapi import Request
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hypeedge.api.errors import ApiProblem
from hypeedge.storage.postgres import ApiAuditRecord, ExecutionCommandRecord

JsonObject = dict[str, Any]
CommandHandler = Callable[[str], Awaitable[JsonObject]]


@dataclass(frozen=True, slots=True)
class ApiActor:
    actor_type: str
    actor_id: str
    role: str = "operator"


@dataclass(slots=True)
class ApiCommand:
    command_id: uuid.UUID
    actor_id: str
    idempotency_key: str
    request_hash: str
    status: str
    claim_token: str | None = None
    response: JsonObject | None = None
    error: JsonObject | None = None


@dataclass(frozen=True, slots=True)
class ApiAudit:
    request_id: uuid.UUID
    actor: ApiActor
    action: str
    resource_type: str | None
    resource_id: str | None
    outcome: str
    reason: str | None
    ip_address: str | None
    user_agent: str | None
    details: JsonObject


class ApiCommandStore(Protocol):
    async def claim(
        self,
        *,
        actor: ApiActor,
        idempotency_key: str,
        action: str,
        request_hash: str,
        request_payload: JsonObject,
    ) -> tuple[ApiCommand, bool]: ...

    async def complete_success(
        self, command_id: uuid.UUID, claim_token: str, response: JsonObject, audit: ApiAudit
    ) -> None: ...

    async def complete_failure(
        self, command_id: uuid.UUID, claim_token: str, error: JsonObject, audit: ApiAudit
    ) -> None: ...

    async def record_audit(self, audit: ApiAudit) -> None: ...


class ApiCommandService:
    """Execute API mutations exactly once per actor/idempotency key."""

    def __init__(self, store: ApiCommandStore) -> None:
        self.store = store

    async def audit_authorization_denied(self, request: Request, *, required_role: str) -> None:
        """Durably audit a rejected mutation before returning HTTP 403."""
        actor = self._actor(request)
        audit = self._audit(
            request,
            actor,
            action=f"{request.method} {request.url.path}",
            resource_type="api_route",
            resource_id=request.url.path,
            outcome="failure",
            reason="INSUFFICIENT_ROLE",
            details={"required_role": required_role, "actor_role": actor.role},
        )
        await self._record_audit(audit)

    async def execute(
        self,
        *,
        request: Request,
        idempotency_key: str,
        action: str,
        resource_type: str | None,
        resource_id: str | None,
        payload: JsonObject,
        handler: CommandHandler,
    ) -> JsonObject:
        actor = self._actor(request)
        request_hash = self._hash_request(action, resource_type, resource_id, payload)
        try:
            command, created = await self.store.claim(
                actor=actor,
                idempotency_key=idempotency_key,
                action=action,
                request_hash=request_hash,
                request_payload=payload,
            )
        except Exception as exc:
            raise ApiProblem(
                503,
                "IDEMPOTENCY_STORE_UNAVAILABLE",
                "The command store is unavailable; the mutation was not executed",
                retryable=True,
            ) from exc

        if not created:
            return await self._replay(
                request=request,
                actor=actor,
                command=command,
                request_hash=request_hash,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
            )

        command_id = str(command.command_id)
        try:
            response = await handler(command_id)
        except ApiProblem as exc:
            error = self._problem_payload(exc)
            audit = self._audit(
                request,
                actor,
                action,
                resource_type,
                resource_id,
                outcome="failure",
                reason=exc.code,
                details={"command_id": command_id, "replayed": False},
            )
            await self._complete_failure(command, error, audit)
            raise
        except Exception as exc:
            problem = ApiProblem(500, "COMMAND_EXECUTION_FAILED", "The command could not be completed", retryable=True)
            audit = self._audit(
                request,
                actor,
                action,
                resource_type,
                resource_id,
                outcome="failure",
                reason=problem.code,
                details={"command_id": command_id, "replayed": False},
            )
            await self._complete_failure(command, self._problem_payload(problem), audit)
            raise problem from exc

        audit = self._audit(
            request,
            actor,
            action,
            resource_type,
            resource_id,
            outcome="success",
            reason=None,
            details={"command_id": command_id, "replayed": False},
        )
        try:
            await self.store.complete_success(command.command_id, self._claim_token(command), response, audit)
        except Exception as exc:
            raise ApiProblem(
                503,
                "COMMAND_RESULT_NOT_DURABLE",
                "The command ran but its result could not be durably recorded; retry with the same key",
                retryable=True,
                context={"command_id": command_id},
            ) from exc
        return response

    async def _replay(
        self,
        *,
        request: Request,
        actor: ApiActor,
        command: ApiCommand,
        request_hash: str,
        action: str,
        resource_type: str | None,
        resource_id: str | None,
    ) -> JsonObject:
        command_id = str(command.command_id)
        if command.request_hash != request_hash:
            audit = self._audit(
                request,
                actor,
                action,
                resource_type,
                resource_id,
                outcome="failure",
                reason="IDEMPOTENCY_KEY_REUSED",
                details={"command_id": command_id, "replayed": True},
            )
            await self._record_audit(audit)
            raise ApiProblem(
                409,
                "IDEMPOTENCY_KEY_REUSED",
                "Idempotency-Key was already used for a different request",
                context={"command_id": command_id},
            )

        if command.status == "succeeded" and command.response is not None:
            await self._record_audit(
                self._audit(
                    request,
                    actor,
                    action,
                    resource_type,
                    resource_id,
                    outcome="success",
                    reason=None,
                    details={"command_id": command_id, "replayed": True},
                )
            )
            return command.response

        if command.status == "failed" and command.error is not None:
            await self._record_audit(
                self._audit(
                    request,
                    actor,
                    action,
                    resource_type,
                    resource_id,
                    outcome="failure",
                    reason=str(command.error.get("code", "COMMAND_FAILED")),
                    details={"command_id": command_id, "replayed": True},
                )
            )
            raise ApiProblem(
                int(command.error.get("status", 409)),
                str(command.error.get("code", "COMMAND_FAILED")),
                str(command.error.get("detail", "The original command failed")),
                retryable=bool(command.error.get("retryable", False)),
                context=dict(command.error.get("context", {})),
            )

        await self._record_audit(
            self._audit(
                request,
                actor,
                action,
                resource_type,
                resource_id,
                outcome="success",
                reason=None,
                details={"command_id": command_id, "replayed": True, "command_status": command.status},
            )
        )
        return {"ok": True, "data": {"command_id": command_id, "status": command.status}}

    async def _complete_failure(self, command: ApiCommand, error: JsonObject, audit: ApiAudit) -> None:
        try:
            await self.store.complete_failure(command.command_id, self._claim_token(command), error, audit)
        except Exception as exc:
            raise ApiProblem(
                503,
                "COMMAND_FAILURE_NOT_DURABLE",
                "The failed command could not be durably recorded; retry with the same key",
                retryable=True,
                context={"command_id": str(command.command_id)},
            ) from exc

    @staticmethod
    def _claim_token(command: ApiCommand) -> str:
        if command.claim_token is None:
            raise RuntimeError("new API command claim is missing its fencing token")
        return command.claim_token

    async def _record_audit(self, audit: ApiAudit) -> None:
        try:
            await self.store.record_audit(audit)
        except Exception as exc:
            raise ApiProblem(
                503,
                "API_AUDIT_UNAVAILABLE",
                "The API audit store is unavailable",
                retryable=True,
            ) from exc

    @staticmethod
    def _actor(request: Request) -> ApiActor:
        actor_id = str(getattr(request.state, "actor_id", "local"))
        actor_type = str(getattr(request.state, "actor_type", "local"))
        role = str(getattr(request.state, "actor_role", "operator"))
        return ApiActor(actor_type=actor_type, actor_id=actor_id, role=role)

    @staticmethod
    def _hash_request(
        action: str,
        resource_type: str | None,
        resource_id: str | None,
        payload: JsonObject,
    ) -> str:
        canonical = json.dumps(
            {"action": action, "resource_type": resource_type, "resource_id": resource_id, "payload": payload},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()

    @staticmethod
    def _problem_payload(problem: ApiProblem) -> JsonObject:
        return {
            "status": problem.status,
            "code": problem.code,
            "detail": problem.detail,
            "retryable": problem.retryable,
            "context": problem.context,
        }

    @staticmethod
    def _audit(
        request: Request,
        actor: ApiActor,
        action: str,
        resource_type: str | None,
        resource_id: str | None,
        *,
        outcome: str,
        reason: str | None,
        details: JsonObject,
    ) -> ApiAudit:
        raw_request_id = str(getattr(request.state, "request_id", uuid.uuid4()))
        try:
            request_id = uuid.UUID(raw_request_id)
        except ValueError:
            request_id = uuid.uuid5(uuid.NAMESPACE_URL, raw_request_id)
        raw_ip = request.client.host if request.client is not None else None
        try:
            ip_address = str(ipaddress.ip_address(raw_ip)) if raw_ip else None
        except ValueError:
            ip_address = None
        return ApiAudit(
            request_id=request_id,
            actor=actor,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            outcome=outcome,
            reason=reason,
            ip_address=ip_address,
            user_agent=request.headers.get("User-Agent", "")[:512] or None,
            details=details,
        )


class InMemoryApiCommandStore:
    """Development/test fallback with the same concurrency semantics as Postgres."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._commands: dict[tuple[str, str], ApiCommand] = {}
        self.audits: list[ApiAudit] = []

    async def claim(
        self,
        *,
        actor: ApiActor,
        idempotency_key: str,
        action: str,
        request_hash: str,
        request_payload: JsonObject,
    ) -> tuple[ApiCommand, bool]:
        del action, request_payload
        async with self._lock:
            key = (actor.actor_id, idempotency_key)
            existing = self._commands.get(key)
            if existing is not None:
                return existing, False
            command = ApiCommand(
                command_id=uuid.uuid4(),
                actor_id=actor.actor_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                status="processing",
                claim_token=uuid.uuid4().hex,
            )
            self._commands[key] = command
            return command, True

    async def complete_success(
        self, command_id: uuid.UUID, claim_token: str, response: JsonObject, audit: ApiAudit
    ) -> None:
        async with self._lock:
            command = self._by_id(command_id)
            if command.status != "processing" or command.claim_token != claim_token:
                raise RuntimeError("API command claim was superseded")
            command.status = "succeeded"
            command.response = response
            self.audits.append(audit)

    async def complete_failure(
        self, command_id: uuid.UUID, claim_token: str, error: JsonObject, audit: ApiAudit
    ) -> None:
        async with self._lock:
            command = self._by_id(command_id)
            if command.status != "processing" or command.claim_token != claim_token:
                raise RuntimeError("API command claim was superseded")
            command.status = "failed"
            command.error = error
            self.audits.append(audit)

    async def record_audit(self, audit: ApiAudit) -> None:
        async with self._lock:
            self.audits.append(audit)

    def _by_id(self, command_id: uuid.UUID) -> ApiCommand:
        return next(command for command in self._commands.values() if command.command_id == command_id)


class PostgresApiCommandStore:
    """Postgres implementation using the existing durable command and audit tables."""

    def __init__(
        self, session_factory: async_sessionmaker[AsyncSession], *, processing_lease_seconds: int = 30
    ) -> None:
        self._session_factory = session_factory
        self._processing_lease_seconds = processing_lease_seconds

    async def claim(
        self,
        *,
        actor: ApiActor,
        idempotency_key: str,
        action: str,
        request_hash: str,
        request_payload: JsonObject,
    ) -> tuple[ApiCommand, bool]:
        command_id = uuid.uuid4()
        claim_token = uuid.uuid4().hex
        now = datetime.now(UTC)
        payload = {"api_request_hash": request_hash, "api_request": request_payload}
        statement = (
            pg_insert(ExecutionCommandRecord)
            .values(
                command_id=command_id,
                command_type=f"api.{action}",
                actor_type=actor.actor_type,
                actor_id=actor.actor_id,
                idempotency_key=idempotency_key,
                status="processing",
                payload=payload,
                attempt_count=1,
                locked_at=now,
                locked_by=f"api:{actor.actor_id}:{claim_token}",
            )
            .on_conflict_do_nothing(index_elements=["actor_id", "idempotency_key"])
            .returning(ExecutionCommandRecord.command_id)
        )
        async with self._session_factory() as session, session.begin():
            inserted = (await session.execute(statement)).scalar_one_or_none()
        if inserted is not None:
            return ApiCommand(
                command_id,
                actor.actor_id,
                idempotency_key,
                request_hash,
                "processing",
                claim_token=claim_token,
            ), True

        async with self._session_factory() as session, session.begin():
            record = (
                await session.execute(
                    select(ExecutionCommandRecord)
                    .where(
                        ExecutionCommandRecord.actor_id == actor.actor_id,
                        ExecutionCommandRecord.idempotency_key == idempotency_key,
                    )
                    .with_for_update()
                )
            ).scalar_one()
            existing = self._to_command(record)
            if existing.request_hash != request_hash:
                return existing, False
            lease_cutoff = now - timedelta(seconds=self._processing_lease_seconds)
            if record.status == "processing" and (record.locked_at is None or record.locked_at < lease_cutoff):
                claim_token = uuid.uuid4().hex
                record.locked_at = now
                record.locked_by = f"api:{actor.actor_id}:{claim_token}"
                record.attempt_count += 1
                reclaimed = self._to_command(record)
                reclaimed.claim_token = claim_token
                return reclaimed, True
        return self._to_command(record), False

    async def complete_success(
        self, command_id: uuid.UUID, claim_token: str, response: JsonObject, audit: ApiAudit
    ) -> None:
        async with self._session_factory() as session, session.begin():
            command = await self._get_for_update(session, command_id)
            self._require_claim(command, claim_token)
            command.status = "succeeded"
            command.completed_at = datetime.now(UTC)
            command.locked_at = None
            command.locked_by = None
            command.payload = {**command.payload, "api_response": response}
            session.add(self._audit_record(audit))

    async def complete_failure(
        self, command_id: uuid.UUID, claim_token: str, error: JsonObject, audit: ApiAudit
    ) -> None:
        async with self._session_factory() as session, session.begin():
            command = await self._get_for_update(session, command_id)
            self._require_claim(command, claim_token)
            command.status = "failed"
            command.completed_at = datetime.now(UTC)
            command.locked_at = None
            command.locked_by = None
            command.last_error_code = str(error.get("code", "COMMAND_FAILED"))
            command.last_error_message = str(error.get("detail", "The command failed"))
            command.payload = {**command.payload, "api_error": error}
            session.add(self._audit_record(audit))

    async def record_audit(self, audit: ApiAudit) -> None:
        async with self._session_factory() as session, session.begin():
            session.add(self._audit_record(audit))

    @staticmethod
    async def _get_for_update(session: AsyncSession, command_id: uuid.UUID) -> ExecutionCommandRecord:
        return (
            await session.execute(
                select(ExecutionCommandRecord).where(ExecutionCommandRecord.command_id == command_id).with_for_update()
            )
        ).scalar_one()

    @staticmethod
    def _require_claim(command: ExecutionCommandRecord, claim_token: str) -> None:
        if (
            command.status != "processing"
            or command.locked_by is None
            or not command.locked_by.endswith(f":{claim_token}")
        ):
            raise RuntimeError("API command claim was superseded")

    @staticmethod
    def _to_command(record: ExecutionCommandRecord) -> ApiCommand:
        return ApiCommand(
            command_id=record.command_id,
            actor_id=record.actor_id,
            idempotency_key=record.idempotency_key,
            request_hash=str(record.payload.get("api_request_hash", "")),
            status=record.status,
            response=record.payload.get("api_response"),
            error=record.payload.get("api_error"),
        )

    @staticmethod
    def _audit_record(audit: ApiAudit) -> ApiAuditRecord:
        return ApiAuditRecord(
            request_id=audit.request_id,
            actor_type=audit.actor.actor_type,
            actor_id=audit.actor.actor_id,
            role=audit.actor.role,
            action=audit.action,
            resource_type=audit.resource_type,
            resource_id=audit.resource_id,
            outcome=audit.outcome,
            reason=audit.reason,
            ip_address=audit.ip_address,
            user_agent=audit.user_agent,
            details=audit.details,
        )
