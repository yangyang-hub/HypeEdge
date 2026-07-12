"""API command idempotency and audit behavior."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import Request

from hypeedge.api.commands import ApiActor, ApiCommandService, InMemoryApiCommandStore, PostgresApiCommandStore
from hypeedge.api.errors import ApiProblem


def _request(actor_id: str = "operator-1") -> Request:
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/v1/orders",
            "headers": [(b"user-agent", b"pytest")],
            "client": ("127.0.0.1", 12345),
        }
    )
    request.state.request_id = "7f02d4e2-e1d4-4f2c-9a1e-680dc63a826d"
    request.state.actor_type = "api_token"
    request.state.actor_id = actor_id
    request.state.actor_role = "operator"
    return request


class _Result:
    def __init__(self, scalar: object = None) -> None:
        self._scalar = scalar

    def scalar_one_or_none(self) -> object:
        return self._scalar

    def scalar_one(self) -> object:
        assert self._scalar is not None
        return self._scalar


class _Transaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None


class _Session:
    def __init__(self, *results: _Result) -> None:
        self._results = list(results)

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None

    def begin(self) -> _Transaction:
        return _Transaction()

    async def execute(self, statement: object) -> _Result:
        del statement
        return self._results.pop(0)


class _Factory:
    def __init__(self, *sessions: _Session) -> None:
        self._sessions = list(sessions)

    def __call__(self) -> _Session:
        return self._sessions.pop(0)


async def _execute(
    service: ApiCommandService,
    request: Request,
    key: str,
    payload: dict[str, Any],
    handler: Any,
) -> dict[str, Any]:
    return await service.execute(
        request=request,
        idempotency_key=key,
        action="place_order",
        resource_type="order",
        resource_id=None,
        payload=payload,
        handler=handler,
    )


class TestApiCommandService:
    async def test_concurrent_duplicate_executes_handler_once_and_replays_result(self) -> None:
        store = InMemoryApiCommandStore()
        service = ApiCommandService(store)
        request = _request()
        entered = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def handler(command_id: str) -> dict[str, Any]:
            nonlocal calls
            calls += 1
            entered.set()
            await release.wait()
            return {"ok": True, "data": {"command_id": command_id, "status": "accepted"}}

        first = asyncio.create_task(_execute(service, request, "same-key", {"symbol": "BTC"}, handler))
        await entered.wait()
        concurrent = await _execute(service, request, "same-key", {"symbol": "BTC"}, handler)
        assert concurrent["data"]["status"] == "processing"

        release.set()
        original = await first
        replay = await _execute(service, request, "same-key", {"symbol": "BTC"}, handler)

        assert calls == 1
        assert replay == original
        assert [audit.outcome for audit in store.audits] == ["success", "success", "success"]

    async def test_same_actor_and_key_with_different_hash_returns_conflict(self) -> None:
        store = InMemoryApiCommandStore()
        service = ApiCommandService(store)
        calls = 0

        async def handler(command_id: str) -> dict[str, Any]:
            nonlocal calls
            calls += 1
            return {"ok": True, "data": {"command_id": command_id}}

        await _execute(service, _request(), "reused-key", {"symbol": "BTC"}, handler)
        with pytest.raises(ApiProblem) as exc_info:
            await _execute(service, _request(), "reused-key", {"symbol": "ETH"}, handler)

        assert exc_info.value.status == 409
        assert exc_info.value.code == "IDEMPOTENCY_KEY_REUSED"
        assert calls == 1
        assert store.audits[-1].outcome == "failure"

    async def test_failed_command_is_audited_and_replayed_without_reexecution(self) -> None:
        store = InMemoryApiCommandStore()
        service = ApiCommandService(store)
        calls = 0

        async def handler(_command_id: str) -> dict[str, Any]:
            nonlocal calls
            calls += 1
            raise ApiProblem(409, "RISK_DENIED", "Risk denied the command")

        for _ in range(2):
            with pytest.raises(ApiProblem, match="Risk denied") as exc_info:
                await _execute(service, _request(), "failed-key", {"symbol": "BTC"}, handler)
            assert exc_info.value.code == "RISK_DENIED"

        assert calls == 1
        assert [audit.outcome for audit in store.audits] == ["failure", "failure"]

    async def test_same_key_is_independent_for_different_actors(self) -> None:
        store = InMemoryApiCommandStore()
        service = ApiCommandService(store)
        calls = 0

        async def handler(command_id: str) -> dict[str, Any]:
            nonlocal calls
            calls += 1
            return {"ok": True, "data": {"command_id": command_id}}

        first = await _execute(service, _request("operator-1"), "shared-key", {"symbol": "BTC"}, handler)
        second = await _execute(service, _request("operator-2"), "shared-key", {"symbol": "BTC"}, handler)

        assert calls == 2
        assert first["data"]["command_id"] != second["data"]["command_id"]


class TestPostgresApiCommandStore:
    async def test_stale_lease_with_different_hash_is_conflict_not_reclaimed(self) -> None:
        locked_at = datetime.now(UTC) - timedelta(minutes=5)
        record = SimpleNamespace(
            command_id=uuid.uuid4(),
            actor_id="operator-1",
            idempotency_key="same-key",
            status="processing",
            payload={"api_request_hash": "original-hash"},
            locked_at=locked_at,
            locked_by="api:operator-1:old-token",
            attempt_count=1,
        )
        store = PostgresApiCommandStore(
            _Factory(_Session(_Result()), _Session(_Result(record))),  # type: ignore[arg-type]
            processing_lease_seconds=30,
        )

        command, reclaimed = await store.claim(
            actor=ApiActor("api_token", "operator-1"),
            idempotency_key="same-key",
            action="place_order",
            request_hash="different-hash",
            request_payload={"symbol": "ETH"},
        )

        assert reclaimed is False
        assert command.request_hash == "original-hash"
        assert record.locked_at == locked_at
        assert record.locked_by == "api:operator-1:old-token"
        assert record.attempt_count == 1

    async def test_stale_matching_lease_gets_new_fencing_token(self) -> None:
        record = SimpleNamespace(
            command_id=uuid.uuid4(),
            actor_id="operator-1",
            idempotency_key="same-key",
            status="processing",
            payload={"api_request_hash": "same-hash"},
            locked_at=datetime.now(UTC) - timedelta(minutes=5),
            locked_by="api:operator-1:old-token",
            attempt_count=1,
        )
        store = PostgresApiCommandStore(
            _Factory(_Session(_Result()), _Session(_Result(record))),  # type: ignore[arg-type]
            processing_lease_seconds=30,
        )

        command, reclaimed = await store.claim(
            actor=ApiActor("api_token", "operator-1"),
            idempotency_key="same-key",
            action="place_order",
            request_hash="same-hash",
            request_payload={"symbol": "BTC"},
        )

        assert reclaimed is True
        assert command.claim_token is not None
        assert record.locked_by == f"api:operator-1:{command.claim_token}"
        assert record.attempt_count == 2

    def test_superseded_claim_cannot_complete(self) -> None:
        record = SimpleNamespace(status="processing", locked_by="api:operator-1:new-token")
        with pytest.raises(RuntimeError, match="superseded"):
            PostgresApiCommandStore._require_claim(record, "old-token")
