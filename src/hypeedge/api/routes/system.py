"""V1 bootstrap and system query endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from hypeedge.api.auth import require_viewer
from hypeedge.api.deps import AppDep, KillSwitchDep, TrackerDep
from hypeedge.api.schemas import decimal_string

router = APIRouter(tags=["system"], dependencies=[Depends(require_viewer)])


def _system_status(app: Any, kill_switch: Any) -> dict[str, Any]:
    cache = getattr(app, "_instrument_cache", None)
    safety = getattr(app, "_safety_controller", None)
    features = getattr(app.settings, "features", None)
    return {
        "environment": str(app.settings.environment),
        "trading_enabled": bool(app.trading_enabled),
        "kill_switch_active": bool(kill_switch.is_active),
        "kill_switch_reason": kill_switch.reason,
        "safety_mode": str(getattr(app, "safety_mode", "starting")),
        "safety_reason": safety.reason if safety is not None else "safety_controller_unavailable",
        "shutting_down": bool(app.is_shutting_down),
        "meta_loaded": bool(cache and cache.is_loaded),
        "features": {
            name: bool(getattr(features, name, False))
            for name in (
                "durable_ledger_v2",
                "execution_v2",
                "user_stream_v2",
                "reconciliation_v2",
                "api_v1",
                "strategy_runner_v2",
            )
        },
    }


@router.get("/system/status")
async def get_system_status(app: AppDep, kill_switch: KillSwitchDep) -> dict[str, Any]:
    return {"ok": True, "data": _system_status(app, kill_switch)}


@router.get("/bootstrap")
async def get_bootstrap(app: AppDep, kill_switch: KillSwitchDep, tracker: TrackerDep) -> dict[str, Any]:
    positions = []
    if tracker is not None:
        for symbol, position in tracker.get_all_positions().items():
            positions.append(
                {
                    "symbol": str(symbol),
                    "size": decimal_string(position.size),
                    "entry_price": decimal_string(position.entry_price) if position.entry_price is not None else None,
                    "mark_price": decimal_string(position.mark_price) if position.mark_price is not None else None,
                    "leverage": position.leverage,
                }
            )
    return {
        "ok": True,
        "data": {
            "system": _system_status(app, kill_switch),
            "positions": positions,
            "server_time": __import__("datetime").datetime.now(__import__("datetime").UTC).isoformat(),
        },
    }
