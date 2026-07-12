"""Orders API routes."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Header, Request, status

from hypeedge.api.auth import OperatorDep, require_viewer
from hypeedge.api.deps import ApiCommandDep, AppDep, EngineDep
from hypeedge.api.errors import ApiProblem
from hypeedge.api.precision import require_instrument_rules, validate_price, validate_size
from hypeedge.api.schemas import OrderSubmitRequest, decimal_string
from hypeedge.core.enums import TERMINAL_STATES, OrderStatus, OrderType, Side
from hypeedge.core.exceptions import ExecutionError, OrderNormalizationError
from hypeedge.core.models import OrderIntent
from hypeedge.core.types import Cloid, Price, Size, StrategyId

router = APIRouter(prefix="/orders", tags=["orders"], dependencies=[Depends(require_viewer)])


def _order_to_dict(order: Any) -> dict[str, Any]:
    return {
        "cloid": str(order.cloid),
        "symbol": str(order.symbol),
        "side": str(order.side),
        "size": decimal_string(order.size),
        "price": decimal_string(order.price) if order.price is not None else None,
        "order_type": str(order.order_type),
        "status": str(order.status),
        "filled_size": decimal_string(order.filled_size),
        "avg_fill_price": decimal_string(order.avg_fill_price) if order.avg_fill_price is not None else None,
        "strategy_id": str(order.strategy_id) if order.strategy_id else None,
        "error_message": order.error_message,
        "created_at": order.created_at.isoformat() if order.created_at else None,
    }


@router.get("")
async def get_orders(engine: EngineDep, app: AppDep, status: str = "active", limit: int = 50) -> dict[str, Any]:
    """Get orders filtered by status."""
    projection_reader = getattr(app, "projection_reader", None)
    if projection_reader is not None:
        records = await projection_reader.list_orders(status, max(1, min(limit, 500)))
        return {"ok": True, "data": [_order_to_dict(order) for order in records]}
    if engine is None:
        return {"ok": True, "data": []}

    all_orders = list(engine._orders.values())

    if status == "active":
        filtered = [o for o in all_orders if o.status not in TERMINAL_STATES]
    elif status == "terminal":
        filtered = [o for o in all_orders if o.status in TERMINAL_STATES]
    else:
        statuses = set(status.split(","))
        filtered = [o for o in all_orders if str(o.status) in statuses]

    filtered.sort(key=lambda o: o.created_at or datetime.min, reverse=True)
    filtered = filtered[:limit]

    return {"ok": True, "data": [_order_to_dict(o) for o in filtered]}


@router.post("")
async def submit_order(
    req: OrderSubmitRequest,
    engine: EngineDep,
    app: AppDep,
    command_service: ApiCommandDep,
    request: Request,
    _role: OperatorDep,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=128),
) -> dict[str, Any]:
    """Submit a new order."""
    if request.url.path.startswith("/api/v1") and not idempotency_key:
        raise ApiProblem(400, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency-Key header is required")

    async def execute(command_id: str) -> dict[str, Any]:
        if engine is None:
            raise ApiProblem(503, "EXECUTION_UNAVAILABLE", "Execution engine is not available", retryable=True)
        rules = require_instrument_rules(app, req.symbol)
        validate_size(req.size, rules)
        validate_price(req.price, rules)
        intent = OrderIntent(
            symbol=rules.symbol,
            side=Side(req.side),
            size=Size(req.size),
            price=Price(req.price) if req.price is not None else None,
            order_type=OrderType(req.order_type),
            reduce_only=req.reduce_only,
            strategy_id=StrategyId(req.strategy_id) if req.strategy_id else None,
            cloid=Cloid(f"0x{uuid.UUID(command_id).hex}"),
        )
        try:
            order = await engine.submit_order(intent)
            return {"ok": True, "data": _order_to_dict(order)}
        except OrderNormalizationError as exc:
            raise ApiProblem(
                422,
                "ORDER_NORMALIZATION_FAILED",
                "Order does not satisfy the instrument trading rules",
                context={"symbol": exc.symbol, "reason": exc.reason},
            ) from exc
        except Exception as exc:
            raise ApiProblem(409, "ORDER_REJECTED", "Order could not be accepted") from exc

    assert idempotency_key is not None
    return await command_service.execute(
        request=request,
        idempotency_key=idempotency_key,
        action="place_order",
        resource_type="order",
        resource_id=None,
        payload=req.model_dump(mode="json"),
        handler=execute,
    )


async def _cancel_order(cloid: str, engine: EngineDep) -> dict[str, Any]:
    """Cancel an order by cloid."""
    if engine is None:
        raise ApiProblem(503, "EXECUTION_UNAVAILABLE", "Execution engine is not available", retryable=True)

    try:
        result = await engine.cancel_order(cloid)
    except ExecutionError as exc:
        raise ApiProblem(
            503,
            "CANCEL_EXECUTION_FAILED",
            "The cancel command could not be completed",
            retryable=True,
            context={"cloid": cloid},
        ) from exc
    if result:
        return {"ok": True, "data": {"cancelled": True, "cloid": cloid}}
    order = await engine.get_order(cloid)
    if order is not None and order.status == OrderStatus.CANCEL_UNKNOWN:
        return {
            "ok": True,
            "data": {
                "cancelled": False,
                "cloid": cloid,
                "status": OrderStatus.CANCEL_UNKNOWN.value,
            },
        }
    if order is not None and order.status in TERMINAL_STATES:
        raise ApiProblem(409, "ORDER_ALREADY_TERMINAL", "The order is already terminal", context={"cloid": cloid})
    if order is not None and order.error_message:
        raise ApiProblem(409, "CANCEL_REJECTED", "The exchange rejected the cancel command", context={"cloid": cloid})
    raise ApiProblem(404, "ORDER_NOT_CANCELLABLE", "Order was not found or is already terminal")


@router.post("/{cloid}/cancel", status_code=status.HTTP_202_ACCEPTED)
async def cancel_order_command(
    cloid: str,
    engine: EngineDep,
    command_service: ApiCommandDep,
    request: Request,
    _role: OperatorDep,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=128),
) -> dict[str, Any]:
    """V1 cancel command. Cancellation remains available in safety modes."""
    if not idempotency_key:
        raise ApiProblem(400, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency-Key header is required")

    async def execute(command_id: str) -> dict[str, Any]:
        result = await _cancel_order(cloid, engine)
        return {
            "ok": True,
            "data": {
                **result["data"],
                "command_id": command_id,
                "status": "accepted",
            },
        }

    return await command_service.execute(
        request=request,
        idempotency_key=idempotency_key,
        action="cancel_order",
        resource_type="order",
        resource_id=cloid,
        payload={"cloid": cloid},
        handler=execute,
    )
