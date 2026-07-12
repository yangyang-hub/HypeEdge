"""Display-only, latest-value market-making WebSocket."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from hypeedge.api.schemas import decimal_string
from hypeedge.core.types import StrategyId

router = APIRouter(tags=["market-making-stream"])


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
    if isinstance(value, (list, tuple)):
        return [_safe(item) for item in value]
    return value


@router.websocket("/ws/v1/market-making")
async def market_making_stream(websocket: WebSocket) -> None:
    """Send only the newest runtime snapshot; REST remains authoritative."""
    raw_strategy_id = websocket.query_params.get("strategy_id", "").strip()
    if not raw_strategy_id or len(raw_strategy_id) > 64:
        await websocket.close(code=1008, reason="invalid strategy subscription")
        return
    app = websocket.app.state.hype_app
    origin = websocket.headers.get("origin")
    if origin is not None and origin not in set(app.settings.api.cors_origins):
        await websocket.close(code=1008, reason="origin not allowed")
        return
    snapshot_provider = getattr(app, "market_making_runtime_snapshot", None)
    if snapshot_provider is None:
        await websocket.close(code=1013, reason="market-making runtime unavailable")
        return

    await websocket.accept()
    sequence = 0
    previous_revision: tuple[int | None, int | None] | None = None
    try:
        while not app.is_shutting_down:
            snapshot = snapshot_provider(StrategyId(raw_strategy_id))
            if asyncio.iscoroutine(snapshot):
                snapshot = await snapshot
            if snapshot is not None:
                quote_revision = getattr(snapshot, "quote_revision", None)
                market_revision = getattr(snapshot, "market_version", None)
                revision = (quote_revision, market_revision)
                if revision != previous_revision:
                    sequence += 1
                    previous_revision = revision
                    desired = getattr(snapshot, "desired", None)
                    features = getattr(snapshot, "features", None)
                    external_reference = None
                    if features is not None and getattr(features, "external_source", None) is not None:
                        external_reference = {
                            "source": features.external_source,
                            "symbol": features.external_symbol,
                            "raw_price": features.external_raw_price,
                            "adjusted_price": features.external_adjusted_price,
                            "basis_bps": features.external_basis_bps,
                            "effective_weight": features.external_effective_weight,
                            "confidence": features.external_confidence,
                            "age_ms": features.external_age_ms,
                            "quality": features.external_quality,
                            "observed_at": features.external_observed_at,
                        }
                    await websocket.send_json(
                        _safe(
                            {
                                "schema_version": 1,
                                "sequence": sequence,
                                "type": "fair_value",
                                "strategy_id": raw_strategy_id,
                                "runtime_revision": quote_revision,
                                "market_revision": market_revision,
                                "observed_at": datetime.now(UTC).isoformat(),
                                "fair_price": getattr(desired, "fair_price", None),
                                "reservation_price": getattr(desired, "reservation_price", None),
                                "best_bid": getattr(features, "best_bid", None),
                                "best_ask": getattr(features, "best_ask", None),
                                "external_reference": external_reference,
                            }
                        )
                    )
            await asyncio.sleep(0.25)
    except WebSocketDisconnect:
        pass
    finally:
        with contextlib.suppress(RuntimeError):
            await websocket.close()
