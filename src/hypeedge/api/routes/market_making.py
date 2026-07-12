"""Multi-instance market-making query and command API."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Protocol, cast

from fastapi import APIRouter, Depends, Header, Query, Request

from hypeedge.api.auth import OperatorDep, require_viewer
from hypeedge.api.deps import ApiCommandDep, AppDep
from hypeedge.api.errors import ApiProblem
from hypeedge.api.schemas import (
    DangerousActionConfirmation,
    MarketMakerConfigVersionCreateRequest,
    StrategyCreateRequest,
    StrategyLifecycleRequest,
    StrategyMetadataPatchRequest,
    decimal_string,
)
from hypeedge.core.enums import MarketMakerLifecycle
from hypeedge.core.exceptions import StrategyLifecycleError, StrategyRegistrationError
from hypeedge.core.types import StrategyId, SubAccount, Symbol

router = APIRouter(tags=["market-making"], dependencies=[Depends(require_viewer)])


class MarketMakingRepository(Protocol):
    async def list_strategy_instances(self) -> list[Any]: ...

    async def get_strategy_instance(self, strategy_id: StrategyId) -> Any | None: ...


def _repository(app: Any) -> MarketMakingRepository:  # noqa: ANN401
    repository = getattr(app, "market_making_repository", None)
    if repository is None:
        raise ApiProblem(
            503, "MARKET_MAKING_STORE_UNAVAILABLE", "Market-making state store is unavailable", retryable=True
        )
    return cast(MarketMakingRepository, repository)


def _supervisor(app: Any) -> Any:  # noqa: ANN401
    supervisor = getattr(app, "strategy_supervisor", None)
    if supervisor is None:
        raise ApiProblem(503, "STRATEGY_SUPERVISOR_UNAVAILABLE", "Strategy supervisor is unavailable", retryable=True)
    return supervisor


def _expected_revision(raw: str | None) -> int:
    if raw is None:
        raise ApiProblem(428, "IF_MATCH_REQUIRED", "If-Match revision is required")
    value = raw.strip().removeprefix("W/").strip('"')
    try:
        revision = int(value)
    except ValueError as exc:
        raise ApiProblem(400, "INVALID_IF_MATCH", "If-Match must contain an integer revision") from exc
    if revision < 0:
        raise ApiProblem(400, "INVALID_IF_MATCH", "If-Match revision cannot be negative")
    return revision


def _safe(value: Any) -> Any:  # noqa: ANN401
    if isinstance(value, Decimal):
        return decimal_string(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return _safe(asdict(value))
    if isinstance(value, dict):
        return {str(key): _safe(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_safe(item) for item in value]
    return value


def _strategy_payload(value: Any) -> Any:  # noqa: ANN401
    definition = getattr(value, "definition", None)
    if definition is None:
        return _safe(value)
    return {
        "strategy_id": str(definition.strategy_id),
        "strategy_type": definition.strategy_type,
        "sub_account": str(definition.sub_account),
        "symbol": str(definition.symbol),
        "desired_state": definition.desired_state.value,
        "desired_config_version": definition.desired_config_revision,
        "revision": definition.revision,
        "metadata": _safe(getattr(value, "metadata", {})),
        "archived_at": _safe(getattr(value, "archived_at", None)),
        "created_at": _safe(getattr(value, "created_at", None)),
        "updated_at": _safe(getattr(value, "updated_at", None)),
    }


def _config_payload(value: Any) -> Any:  # noqa: ANN401
    values = getattr(value, "values", None)
    revision = getattr(value, "revision", None)
    strategy_id = getattr(value, "strategy_id", None)
    if values is None or revision is None or strategy_id is None:
        return _safe(value)
    from hypeedge.storage.market_making import market_maker_config_hash

    return {
        "id": revision,
        "strategy_id": str(strategy_id),
        "version": revision,
        "config_hash": market_maker_config_hash(values),
        "config": _safe(values),
        "created_by": None,
        "created_at": None,
        "approved_by": None,
        "approved_at": None,
        "shadow_preview": None,
    }


async def _run_command(
    service: ApiCommandDep,
    request: Request,
    idempotency_key: str | None,
    *,
    action: str,
    resource_id: str | None,
    payload: dict[str, Any],
    handler: Any,
) -> dict[str, Any]:
    if not idempotency_key:
        raise ApiProblem(400, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency-Key header is required")
    return await service.execute(
        request=request,
        idempotency_key=idempotency_key,
        action=action,
        resource_type="strategy",
        resource_id=resource_id,
        payload=payload,
        handler=handler,
    )


@router.get("/strategies")
async def list_strategies(app: AppDep) -> dict[str, Any]:
    repository = getattr(app, "market_making_repository", None)
    if repository is None:
        legacy = getattr(app, "strategy", None)
        if legacy is None:
            return {"ok": True, "data": []}
        return {
            "ok": True,
            "data": [
                {
                    "strategy_id": str(legacy.strategy_id),
                    "strategy_type": "trend_follow",
                    "status": legacy.status.value,
                    "symbol": legacy.params.symbol,
                    "revision": 0,
                }
            ],
        }
    records = await repository.list_strategy_instances()
    return {"ok": True, "data": [_strategy_payload(record) for record in records]}


@router.post("/strategies")
async def create_strategy(
    body: StrategyCreateRequest,
    app: AppDep,
    service: ApiCommandDep,
    request: Request,
    _role: OperatorDep,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=128),
) -> dict[str, Any]:
    repository = _repository(app)

    async def execute(_command_id: str) -> dict[str, Any]:
        create = getattr(repository, "create_strategy_instance", None)
        if create is None:
            raise ApiProblem(503, "MARKET_MAKING_STORE_UNAVAILABLE", "Strategy creation is unavailable", retryable=True)
        try:
            record = await create(
                strategy_id=StrategyId(body.strategy_id),
                sub_account=SubAccount(body.sub_account),
                symbol=Symbol(body.symbol),
                initial_config=body.initial_config.model_dump(),
                created_by=str(request.state.actor_id),
                metadata=body.metadata,
            )
        except (StrategyLifecycleError, StrategyRegistrationError) as exc:
            raise ApiProblem(409, "STRATEGY_CREATE_CONFLICT", str(exc)) from exc
        return {"ok": True, "data": _strategy_payload(record)}

    return await _run_command(
        service,
        request,
        idempotency_key,
        action="create_strategy",
        resource_id=body.strategy_id,
        payload=body.model_dump(mode="json"),
        handler=execute,
    )


@router.get("/strategies/{strategy_id}")
async def get_strategy(strategy_id: str, app: AppDep) -> dict[str, Any]:
    try:
        record = await _repository(app).get_strategy_instance(StrategyId(strategy_id))
    except StrategyRegistrationError as exc:
        raise ApiProblem(404, "STRATEGY_NOT_FOUND", "Strategy was not found") from exc
    if record is None:
        raise ApiProblem(404, "STRATEGY_NOT_FOUND", "Strategy was not found")
    return {"ok": True, "data": _strategy_payload(record)}


@router.patch("/strategies/{strategy_id}")
async def update_strategy(
    strategy_id: str,
    body: StrategyMetadataPatchRequest,
    app: AppDep,
    service: ApiCommandDep,
    request: Request,
    _role: OperatorDep,
    if_match: str | None = Header(default=None, alias="If-Match"),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=128),
) -> dict[str, Any]:
    repository = _repository(app)
    revision = _expected_revision(if_match)

    async def execute(_command_id: str) -> dict[str, Any]:
        update = getattr(repository, "update_strategy_metadata", None)
        if update is None:
            raise ApiProblem(503, "MARKET_MAKING_STORE_UNAVAILABLE", "Strategy update is unavailable", retryable=True)
        try:
            record = await update(StrategyId(strategy_id), body.metadata, expected_revision=revision)
        except (StrategyLifecycleError, StrategyRegistrationError) as exc:
            raise ApiProblem(409, "STRATEGY_REVISION_CONFLICT", str(exc)) from exc
        return {"ok": True, "data": _strategy_payload(record)}

    return await _run_command(
        service,
        request,
        idempotency_key,
        action="update_strategy",
        resource_id=strategy_id,
        payload={"revision": revision, **body.model_dump(mode="json")},
        handler=execute,
    )


@router.post("/strategies/{strategy_id}/archive")
async def archive_strategy(
    strategy_id: str,
    app: AppDep,
    service: ApiCommandDep,
    request: Request,
    _role: OperatorDep,
    if_match: str | None = Header(default=None, alias="If-Match"),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=128),
) -> dict[str, Any]:
    repository = _repository(app)
    revision = _expected_revision(if_match)

    async def execute(_command_id: str) -> dict[str, Any]:
        archive = getattr(repository, "archive_strategy_instance", None)
        if archive is None:
            raise ApiProblem(503, "MARKET_MAKING_STORE_UNAVAILABLE", "Strategy archive is unavailable", retryable=True)
        try:
            record = await archive(StrategyId(strategy_id), expected_revision=revision)
        except (StrategyLifecycleError, StrategyRegistrationError) as exc:
            raise ApiProblem(409, "STRATEGY_ARCHIVE_CONFLICT", str(exc)) from exc
        return {"ok": True, "data": _strategy_payload(record)}

    return await _run_command(
        service,
        request,
        idempotency_key,
        action="archive_strategy",
        resource_id=strategy_id,
        payload={"revision": revision},
        handler=execute,
    )


@router.post("/strategies/{strategy_id}/actions/{action}")
async def strategy_action(
    strategy_id: str,
    action: str,
    body: StrategyLifecycleRequest,
    app: AppDep,
    service: ApiCommandDep,
    request: Request,
    _role: OperatorDep,
    if_match: str | None = Header(default=None, alias="If-Match"),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=128),
) -> dict[str, Any]:
    if action not in {"start", "pause", "resume", "drain", "stop"}:
        raise ApiProblem(404, "STRATEGY_ACTION_NOT_FOUND", "Unsupported strategy lifecycle action")
    revision = _expected_revision(if_match)
    supervisor = _supervisor(app)
    if app.settings.environment == "mainnet":
        expected = f"CONFIRM MAINNET {action.upper()}"
        if body.confirmation != expected:
            raise ApiProblem(409, "MAINNET_CONFIRMATION_REQUIRED", f"Enter {expected} to continue")

    async def execute(_command_id: str) -> dict[str, Any]:
        requested_target = body.target or body.target_state
        target = MarketMakerLifecycle(requested_target) if requested_target else MarketMakerLifecycle.RUNNING
        try:
            if action == "start":
                state = await supervisor.start(StrategyId(strategy_id), target=target, expected_revision=revision)
            elif action == "resume":
                state = await supervisor.resume(StrategyId(strategy_id), target=target)
            else:
                state = await getattr(supervisor, action)(StrategyId(strategy_id))
        except (StrategyLifecycleError, StrategyRegistrationError) as exc:
            raise ApiProblem(409, "STRATEGY_LIFECYCLE_CONFLICT", str(exc)) from exc
        return {"ok": True, "data": _safe(state)}

    return await _run_command(
        service,
        request,
        idempotency_key,
        action=f"strategy_{action}",
        resource_id=strategy_id,
        payload={"revision": revision, **body.model_dump(mode="json")},
        handler=execute,
    )


@router.get("/strategies/{strategy_id}/config-versions")
async def list_config_versions(strategy_id: str, app: AppDep) -> dict[str, Any]:
    repository = _repository(app)
    method = getattr(repository, "list_config_versions", None)
    if method is None:
        raise ApiProblem(503, "MARKET_MAKING_STORE_UNAVAILABLE", "Config versions are unavailable", retryable=True)
    versions = await method(StrategyId(strategy_id))
    return {"ok": True, "data": [_config_payload(version) for version in versions]}


@router.post("/strategies/{strategy_id}/config-versions")
async def create_config_version(
    strategy_id: str,
    body: MarketMakerConfigVersionCreateRequest,
    app: AppDep,
    service: ApiCommandDep,
    request: Request,
    _role: OperatorDep,
    if_match: str | None = Header(default=None, alias="If-Match"),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=128),
) -> dict[str, Any]:
    repository = _repository(app)
    revision = _expected_revision(if_match)

    async def execute(_command_id: str) -> dict[str, Any]:
        method = getattr(repository, "create_market_maker_config_version", None)
        if method is None:
            raise ApiProblem(503, "MARKET_MAKING_STORE_UNAVAILABLE", "Config creation is unavailable", retryable=True)
        try:
            record = await method(
                StrategyId(strategy_id),
                body.config.model_dump(),
                created_by=str(request.state.actor_id),
                expected_revision=revision,
            )
        except (StrategyLifecycleError, StrategyRegistrationError) as exc:
            raise ApiProblem(409, "CONFIG_VERSION_CONFLICT", str(exc)) from exc
        return {"ok": True, "data": _config_payload(record)}

    return await _run_command(
        service,
        request,
        idempotency_key,
        action="create_config_version",
        resource_id=strategy_id,
        payload={"revision": revision, **body.model_dump(mode="json")},
        handler=execute,
    )


@router.post("/strategies/{strategy_id}/config-versions/{version}/{operation}")
async def activate_config_version(
    strategy_id: str,
    version: int,
    operation: str,
    body: DangerousActionConfirmation,
    app: AppDep,
    service: ApiCommandDep,
    request: Request,
    _role: OperatorDep,
    if_match: str | None = Header(default=None, alias="If-Match"),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=128),
) -> dict[str, Any]:
    if operation not in {"activate", "rollback"}:
        raise ApiProblem(404, "CONFIG_ACTION_NOT_FOUND", "Unsupported config action")
    revision = _expected_revision(if_match)
    supervisor = _supervisor(app)
    if app.settings.environment == "mainnet" and body.confirmation != "CONFIRM MAINNET CONFIG":
        raise ApiProblem(409, "MAINNET_CONFIRMATION_REQUIRED", "Enter CONFIRM MAINNET CONFIG to continue")

    async def execute(_command_id: str) -> dict[str, Any]:
        try:
            state = await supervisor.activate_config(
                StrategyId(strategy_id),
                version,
                expected_revision=revision,
            )
        except (StrategyLifecycleError, StrategyRegistrationError) as exc:
            raise ApiProblem(409, "CONFIG_ACTIVATION_CONFLICT", str(exc)) from exc
        return {"ok": True, "data": _safe(state)}

    return await _run_command(
        service,
        request,
        idempotency_key,
        action=f"config_{operation}",
        resource_id=strategy_id,
        payload={"revision": revision, "version": version},
        handler=execute,
    )


@router.get("/market-making/{strategy_id}/{view}")
async def get_market_making_view(
    strategy_id: str,
    view: str,
    app: AppDep,
    limit: int = Query(default=200, ge=1, le=1000),
) -> dict[str, Any]:
    methods = {
        "state": "get_market_making_state",
        "quotes": "get_market_making_quotes",
        "inventory": "get_market_making_inventory",
        "performance": "get_market_making_performance",
        "action-budget": "get_market_making_action_budget",
        "events": "get_market_making_events",
    }
    method_name = methods.get(view)
    if method_name is None:
        raise ApiProblem(404, "MARKET_MAKING_VIEW_NOT_FOUND", "Unsupported market-making view")
    method = getattr(_repository(app), method_name, None)
    if method is None:
        if view == "performance":
            return {
                "ok": True,
                "data": {"as_of": None, "stale": True, "source": "unavailable", "accounting_pnl": None},
            }
        raise ApiProblem(503, "MARKET_MAKING_VIEW_UNAVAILABLE", f"{view} view is unavailable", retryable=True)
    if view == "events":
        data = await method(StrategyId(strategy_id), limit=limit)
    else:
        data = await method(StrategyId(strategy_id))
    if hasattr(data, "data") and hasattr(data, "stale"):
        if view == "events":
            data = data.data or ()
        elif view == "performance":
            data = {
                "strategy_id": strategy_id,
                "accounting": data.data,
                "execution_quality": None,
                "inventory_episodes": [],
                "source": data.source,
                "as_of": data.as_of,
                "stale": data.stale,
                "reason": data.reason,
            }
        else:
            data = data.data
    return {"ok": True, "data": _safe(data)}
