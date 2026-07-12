"""Positions API routes."""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, Header, Request, status

from hypeedge.api.auth import OperatorDep, require_viewer
from hypeedge.api.deps import ApiCommandDep, AppDep, EngineDep, TrackerDep
from hypeedge.api.errors import ApiProblem
from hypeedge.api.precision import floor_to_lot, require_instrument_rules, validate_size
from hypeedge.api.routes.orders import _order_to_dict
from hypeedge.api.schemas import ClosePositionRequest, decimal_string
from hypeedge.core.enums import OrderType, Side
from hypeedge.core.models import OrderIntent
from hypeedge.core.types import Cloid, Size

router = APIRouter(prefix="/positions", tags=["positions"], dependencies=[Depends(require_viewer)])


@router.get("")
async def get_positions(tracker: TrackerDep, app: AppDep) -> dict[str, Any]:
    """All active positions with unrealized PnL."""
    projection_reader = getattr(app, "projection_reader", None)
    if projection_reader is not None:
        positions = await projection_reader.list_positions()
        return {
            "ok": True,
            "data": [
                {
                    "symbol": position.symbol,
                    "size": decimal_string(position.size),
                    "entry_price": decimal_string(position.entry_price) if position.entry_price is not None else None,
                    "mark_price": decimal_string(position.mark_price) if position.mark_price is not None else None,
                    "unrealized_pnl": decimal_string(position.unrealized_pnl),
                    "leverage": position.leverage,
                    "side": "long" if position.size > 0 else "short",
                }
                for position in positions
            ],
        }
    if tracker is None:
        return {"ok": True, "data": []}

    positions = tracker.get_all_positions()
    result = []
    for symbol, pos in positions.items():
        pnl = Decimal("0")
        if pos.mark_price and pos.entry_price:
            pnl = Decimal(str(pos.size)) * (Decimal(str(pos.mark_price)) - Decimal(str(pos.entry_price)))

        side = "long" if pos.is_long else ("short" if pos.is_short else "flat")
        result.append(
            {
                "symbol": str(symbol),
                "size": decimal_string(pos.size),
                "entry_price": decimal_string(pos.entry_price) if pos.entry_price is not None else None,
                "mark_price": decimal_string(pos.mark_price) if pos.mark_price is not None else None,
                "unrealized_pnl": decimal_string(pnl),
                "leverage": pos.leverage,
                "side": side,
            }
        )

    return {"ok": True, "data": result}


@router.post("/{symbol}/close", status_code=status.HTTP_202_ACCEPTED)
async def close_position(
    symbol: str,
    req: ClosePositionRequest,
    tracker: TrackerDep,
    engine: EngineDep,
    app: AppDep,
    command_service: ApiCommandDep,
    request: Request,
    _role: OperatorDep,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=128),
) -> dict[str, Any]:
    """Close part or all of a position; side and reduce-only are server controlled."""
    if not idempotency_key:
        raise ApiProblem(400, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency-Key header is required")

    async def execute(command_id: str) -> dict[str, Any]:
        if tracker is None or engine is None:
            raise ApiProblem(
                503,
                "TRADING_STATE_UNAVAILABLE",
                "Position or execution state is unavailable",
                retryable=True,
            )

        rules = require_instrument_rules(app, symbol)
        position = tracker.get_all_positions().get(rules.symbol)
        if position is None or position.is_flat:
            raise ApiProblem(404, "POSITION_NOT_FOUND", "No active position exists for this symbol")

        available = abs(Decimal(str(position.size)))
        if req.quantity is not None:
            quantity = req.quantity
        else:
            assert req.close_fraction is not None
            quantity = floor_to_lot(available * req.close_fraction, rules)
        if quantity > available:
            raise ApiProblem(
                409,
                "CLOSE_SIZE_EXCEEDS_POSITION",
                "Close quantity exceeds the current position",
                context={"available": decimal_string(available)},
            )
        validate_size(quantity, rules)

        intent = OrderIntent(
            symbol=rules.symbol,
            side=Side.SELL if position.is_long else Side.BUY,
            size=Size(quantity),
            price=None,
            order_type=OrderType.MARKET,
            reduce_only=True,
            cloid=Cloid(f"0x{uuid.UUID(command_id).hex}"),
        )
        try:
            order = await engine.submit_order(intent)
        except Exception as exc:
            raise ApiProblem(409, "POSITION_CLOSE_REJECTED", "Close command was rejected") from exc
        return {
            "ok": True,
            "data": {
                "command_id": command_id,
                "status": "accepted",
                "order": _order_to_dict(order),
            },
        }

    return await command_service.execute(
        request=request,
        idempotency_key=idempotency_key,
        action="close_position",
        resource_type="position",
        resource_id=symbol,
        payload={"symbol": symbol, **req.model_dump(mode="json")},
        handler=execute,
    )
