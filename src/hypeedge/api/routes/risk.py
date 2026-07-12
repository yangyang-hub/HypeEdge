"""Risk and Kill Switch API routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, Request

from hypeedge.api.auth import AdminDep, require_viewer
from hypeedge.api.deps import ApiCommandDep, AppDep, KillSwitchDep, RiskDep, TrackerDep
from hypeedge.api.errors import ApiProblem
from hypeedge.api.schemas import KillSwitchRequest, decimal_string

router = APIRouter(tags=["risk"], dependencies=[Depends(require_viewer)])


@router.get("/risk/status")
async def get_risk_status(
    app: AppDep,
    risk_checker: RiskDep,
    kill_switch: KillSwitchDep,
    tracker: TrackerDep,
) -> dict[str, Any]:
    """Full risk status: limits, kill switch, check stats."""
    limits: list[dict[str, Any]] = []
    check_stats: dict[str, int] = {}
    strategy_pnl: dict[str, float] = {}

    drawdown = tracker.drawdown_pct if tracker else 0.0
    leverage = tracker.get_leverage() if tracker else 0.0
    equity = float(tracker.current_equity) if tracker else 0.0

    if risk_checker is not None:
        rl = risk_checker.limits
        check_stats = risk_checker.stats
        strategy_pnl = risk_checker.strategy_pnl

        dd_pct = drawdown / rl.max_drawdown_pct if rl.max_drawdown_pct > 0 else 0
        lev_pct = leverage / rl.max_leverage if rl.max_leverage > 0 else 0
        limits = [
            {
                "name": "总回撤",
                "current": decimal_string(drawdown),
                "limit": decimal_string(rl.max_drawdown_pct),
                "unit": "%",
                "pct_used": decimal_string(dd_pct),
            },
            {
                "name": "最大杠杆",
                "current": decimal_string(leverage),
                "limit": decimal_string(rl.max_leverage),
                "unit": "x",
                "pct_used": decimal_string(lev_pct),
            },
        ]

        for sid, pnl in strategy_pnl.items():
            max_loss = equity * rl.max_strategy_loss_pct
            used = abs(pnl) / max_loss if max_loss > 0 and pnl < 0 else 0
            limits.append(
                {
                    "name": f"{sid} 亏损",
                    "current": decimal_string(abs(pnl) if pnl < 0 else 0.0),
                    "limit": decimal_string(max_loss),
                    "unit": "USDC",
                    "pct_used": decimal_string(used),
                }
            )

    known_credits = getattr(app, "action_credits_remaining", None)
    action_credits = int(known_credits) if isinstance(known_credits, int) else 0
    safety = getattr(app, "_safety_controller", None)
    safety_mode = str(getattr(app, "safety_mode", "starting"))
    safety_reason = safety.reason if safety is not None else "safety_controller_unavailable"

    return {
        "ok": True,
        "data": {
            "kill_switch_active": kill_switch.is_active,
            "kill_switch_reason": kill_switch.reason,
            "safety_mode": safety_mode,
            "safety_reason": safety_reason,
            "limits": limits,
            "check_stats": check_stats,
            "strategy_pnl": {key: decimal_string(value) for key, value in strategy_pnl.items()},
            "action_credits_remaining": action_credits,
        },
    }


@router.post("/kill-switch")
async def kill_switch_action(
    req: KillSwitchRequest,
    app: AppDep,
    kill_switch: KillSwitchDep,
    command_service: ApiCommandDep,
    request: Request,
    _role: AdminDep,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=128),
) -> dict[str, Any]:
    """Trigger or reset the kill switch."""
    if not idempotency_key:
        raise ApiProblem(400, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency-Key header is required")

    async def execute(_command_id: str) -> dict[str, Any]:
        if req.action == "trigger":
            reason = req.reason or "manual_trigger_via_api"
            try:
                triggered = await app.trigger_kill_switch(reason)
            except Exception as exc:
                raise ApiProblem(
                    503,
                    "KILL_SWITCH_LATCH_NOT_DURABLE",
                    "Kill switch could not be durably latched",
                    retryable=True,
                ) from exc
            if not triggered:
                raise ApiProblem(
                    503,
                    "KILL_SWITCH_LATCH_NOT_DURABLE",
                    "Kill switch could not be durably latched",
                    retryable=True,
                )
            return {"ok": True, "data": {"action": "triggered", "reason": reason}}
        if not kill_switch.is_active:
            raise ApiProblem(409, "KILL_SWITCH_NOT_ACTIVE", "Kill switch is not active")
        if not hasattr(app, "recover_from_kill_switch"):
            raise ApiProblem(
                409,
                "KILL_SWITCH_RECOVERY_UNAVAILABLE",
                "Kill switch recovery requires a successful reconciliation and is not available",
            )
        try:
            recovered = await app.recover_from_kill_switch()
        except Exception as exc:
            raise ApiProblem(
                503,
                "KILL_SWITCH_RECOVERY_UNAVAILABLE",
                "Kill switch recovery could not complete",
                retryable=True,
            ) from exc
        if not recovered:
            raise ApiProblem(409, "KILL_SWITCH_RECOVERY_FAILED", "Reconciliation did not permit trading recovery")
        return {"ok": True, "data": {"action": "reset", "trading_enabled": bool(app.trading_enabled)}}

    return await command_service.execute(
        request=request,
        idempotency_key=idempotency_key,
        action=f"kill_switch_{req.action}",
        resource_type="system",
        resource_id="kill_switch",
        payload=req.model_dump(mode="json"),
        handler=execute,
    )
