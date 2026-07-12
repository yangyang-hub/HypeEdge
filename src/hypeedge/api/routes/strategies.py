"""Strategy API routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, Request

from hypeedge.api.auth import OperatorDep, require_viewer
from hypeedge.api.deps import ApiCommandDep, AppDep, StrategyDep
from hypeedge.api.errors import ApiProblem
from hypeedge.api.schemas import decimal_string
from hypeedge.core.enums import StrategyStatus

router = APIRouter(prefix="/strategies", tags=["strategies"], dependencies=[Depends(require_viewer)])


@router.get("")
async def get_strategies(strategy: StrategyDep) -> dict[str, Any]:
    """List all strategies with status and parameters."""
    if strategy is None:
        return {"ok": True, "data": []}

    p = strategy.params
    data = {
        "strategy_id": str(strategy.strategy_id),
        "status": strategy.status.value,
        "symbol": p.symbol,
        "position_size": decimal_string(strategy.position_size),
        "entry_price": decimal_string(strategy.entry_price) if strategy.entry_price is not None else None,
        "stop_price": decimal_string(strategy.stop_price) if strategy.stop_price is not None else None,
        "params": {
            "fast_ema_period": p.fast_ema_period,
            "slow_ema_period": p.slow_ema_period,
            "signal_ema_period": p.signal_ema_period,
            "momentum_period": p.momentum_period,
            "atr_period": p.atr_period,
            "atr_position_multiplier": decimal_string(p.atr_position_multiplier),
            "max_position_pct": decimal_string(p.max_position_pct),
            "risk_per_trade_pct": decimal_string(p.risk_per_trade_pct),
            "atr_stop_multiplier": decimal_string(p.atr_stop_multiplier),
        },
    }
    return {"ok": True, "data": [data]}


@router.post("/{strategy_id}/start")
async def start_strategy(
    strategy_id: str,
    app: AppDep,
    strategy: StrategyDep,
    command_service: ApiCommandDep,
    request: Request,
    _role: OperatorDep,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=128),
) -> dict[str, Any]:
    """Start a strategy."""
    if strategy is None:
        raise ApiProblem(404, "STRATEGY_NOT_FOUND", "Strategy was not found")

    if str(strategy.strategy_id) != strategy_id:
        raise ApiProblem(404, "STRATEGY_NOT_FOUND", "Strategy was not found")

    if strategy.status == StrategyStatus.RUNNING:
        raise ApiProblem(409, "STRATEGY_ALREADY_RUNNING", "Strategy is already running")

    if not idempotency_key:
        raise ApiProblem(400, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency-Key header is required")

    async def execute(_command_id: str) -> dict[str, Any]:
        try:
            started = await app.start_strategy()
            if not started:
                raise ApiProblem(409, "STRATEGY_START_NOT_PERMITTED", "Trading gate does not permit strategy start")
            return {"ok": True, "data": {"status": "started", "strategy_id": strategy_id}}
        except ApiProblem:
            raise
        except Exception as exc:
            raise ApiProblem(503, "STRATEGY_START_FAILED", "Strategy could not be started", retryable=True) from exc

    return await command_service.execute(
        request=request,
        idempotency_key=idempotency_key,
        action="start_strategy",
        resource_type="strategy",
        resource_id=strategy_id,
        payload={"strategy_id": strategy_id},
        handler=execute,
    )


@router.post("/{strategy_id}/stop")
async def stop_strategy(
    strategy_id: str,
    app: AppDep,
    strategy: StrategyDep,
    command_service: ApiCommandDep,
    request: Request,
    _role: OperatorDep,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=128),
) -> dict[str, Any]:
    """Stop a strategy."""
    if strategy is None:
        raise ApiProblem(404, "STRATEGY_NOT_FOUND", "Strategy was not found")

    if str(strategy.strategy_id) != strategy_id:
        raise ApiProblem(404, "STRATEGY_NOT_FOUND", "Strategy was not found")

    if strategy.status == StrategyStatus.STOPPED:
        raise ApiProblem(409, "STRATEGY_ALREADY_STOPPED", "Strategy is already stopped")

    if not idempotency_key:
        raise ApiProblem(400, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency-Key header is required")

    async def execute(_command_id: str) -> dict[str, Any]:
        try:
            stopped = await app.stop_strategy()
            if not stopped:
                raise ApiProblem(409, "STRATEGY_NOT_RUNNING", "Strategy runner is not active")
            return {"ok": True, "data": {"status": "stopped", "strategy_id": strategy_id}}
        except ApiProblem:
            raise
        except Exception as exc:
            raise ApiProblem(503, "STRATEGY_STOP_FAILED", "Strategy could not be stopped", retryable=True) from exc

    return await command_service.execute(
        request=request,
        idempotency_key=idempotency_key,
        action="stop_strategy",
        resource_type="strategy",
        resource_id=strategy_id,
        payload={"strategy_id": strategy_id},
        handler=execute,
    )
