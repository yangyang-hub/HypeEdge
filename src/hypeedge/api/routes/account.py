"""Account API routes."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends

from hypeedge.api.auth import require_viewer
from hypeedge.api.deps import AppDep, TrackerDep
from hypeedge.api.schemas import AccountData, decimal_string

router = APIRouter(prefix="/account", tags=["account"], dependencies=[Depends(require_viewer)])


@router.get("")
async def get_account(tracker: TrackerDep, app: AppDep) -> dict[str, Any]:
    """Account overview: equity, margin, PnL, leverage."""
    projection_reader = getattr(app, "projection_reader", None)
    if projection_reader is not None:
        projection = await projection_reader.get_account()
        if projection is not None:
            metrics = await projection_reader.get_account_metrics()
            equity = Decimal(projection.equity)
            peak = Decimal(projection.peak_equity)
            drawdown = ((peak - equity) / peak) if peak > 0 else Decimal("0")
            return {
                "ok": True,
                "data": AccountData(
                    equity=equity,
                    available_balance=Decimal(projection.available_balance),
                    total_margin_used=Decimal(projection.total_margin_used),
                    total_unrealized_pnl=Decimal(projection.total_unrealized_pnl),
                    peak_equity=peak,
                    drawdown_pct=drawdown,
                    leverage=Decimal(str(metrics["leverage"])),
                    total_fees=Decimal(str(metrics["total_fees"])),
                    total_funding=Decimal("0"),
                    fill_count=int(metrics["fill_count"]),
                    position_count=int(metrics["position_count"]),
                    last_update=projection.exchange_updated_at.isoformat(),
                    trading_enabled=app.trading_enabled,
                ).model_dump(mode="json"),
            }
    if tracker is None:
        # Monitor-only / no trading credentials: return an empty snapshot instead of
        # 503 so the dashboard does not retry-spam the console.
        return {
            "ok": True,
            "data": AccountData(
                equity=Decimal("0"),
                available_balance=Decimal("0"),
                total_margin_used=Decimal("0"),
                total_unrealized_pnl=Decimal("0"),
                peak_equity=Decimal("0"),
                drawdown_pct=Decimal("0"),
                leverage=Decimal("0"),
                total_fees=Decimal("0"),
                total_funding=Decimal("0"),
                fill_count=0,
                position_count=0,
                last_update=None,
                trading_enabled=False,
            ).model_dump(mode="json"),
        }

    state = tracker.get_account_state()
    if state is None:
        # Trading disabled / not yet reconciled: keep the dashboard quiet with zeros.
        if not app.trading_enabled:
            return {
                "ok": True,
                "data": AccountData(
                    equity=Decimal("0"),
                    available_balance=Decimal("0"),
                    total_margin_used=Decimal("0"),
                    total_unrealized_pnl=Decimal("0"),
                    peak_equity=Decimal("0"),
                    drawdown_pct=Decimal("0"),
                    leverage=Decimal("0"),
                    total_fees=Decimal("0"),
                    total_funding=Decimal("0"),
                    fill_count=0,
                    position_count=0,
                    last_update=None,
                    trading_enabled=False,
                ).model_dump(mode="json"),
            }
        from hypeedge.api.errors import ApiProblem

        raise ApiProblem(
            503,
            "ACCOUNT_STATE_UNAVAILABLE",
            "Authoritative account state is not available",
            retryable=True,
        )
    status = tracker.get_status()
    data = AccountData(
        equity=Decimal(str(state.equity)),
        available_balance=Decimal(str(state.available_balance)),
        total_margin_used=Decimal(str(state.total_margin_used)),
        total_unrealized_pnl=Decimal(str(state.total_unrealized_pnl)),
        peak_equity=Decimal(str(state.peak_equity)),
        drawdown_pct=Decimal(str(state.drawdown_pct)),
        leverage=Decimal(str(status["leverage"])),
        total_fees=Decimal(str(status["total_fees"])),
        total_funding=Decimal(str(status["total_funding"])),
        fill_count=status["fill_count"],
        position_count=status["position_count"],
        last_update=status["last_update"],
        trading_enabled=app.trading_enabled,
    )
    return {"ok": True, "data": data.model_dump(mode="json")}


@router.get("/equity-curve")
async def get_equity_curve(tracker: TrackerDep, days: int = 30) -> dict[str, Any]:
    """Historical equity curve."""
    if tracker is None:
        return {"ok": True, "data": []}

    from datetime import UTC, datetime

    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    equity = Decimal(str(tracker.current_equity))
    return {"ok": True, "data": [{"timestamp": now_ms, "equity": decimal_string(equity)}]}
