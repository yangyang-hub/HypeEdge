"""Reconciler — corrects local state against exchange truth.

Design doc §9.1: "On process startup / WS reconnection, first reconcile
local state against the exchange's real open orders + positions, then
resume strategy — otherwise duplicate or missed orders will occur."

Implementation plan §3: "App startup gate: trading_enabled=false; only
after reconciliation succeeds may strategies submit orders."
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import structlog

from hypeedge.account.tracker import AccountTracker
from hypeedge.core.enums import OrderStatus, OrderType, Side, TimeInForce
from hypeedge.core.events import EVENT_RECONCILIATION_COMPLETE, Event, EventBus
from hypeedge.core.models import AccountState, Order, Position
from hypeedge.core.types import Cloid, OrderId, Price, Size, Symbol
from hypeedge.execution.cloid import CloidGenerator
from hypeedge.execution.engine import ExecutionEngine

logger = structlog.get_logger(__name__)

if TYPE_CHECKING:
    from hypeedge.risk.safety import SafetyController
    from hypeedge.storage.postgres import PostgresReconciliationStore


@dataclass
class ReconciliationResult:
    """Result of a reconciliation cycle."""

    success: bool
    orders_corrected: int
    positions_corrected: int
    errors: list[str]


class Reconciler:
    """Reconciles local state with exchange truth.

    Runs:
    - On startup (before strategies resume)
    - On WS reconnection
    - Periodically (configurable interval)

    Checks:
    1. Local open orders vs exchange open orders
    2. Local positions vs exchange positions
    3. Local balance vs exchange balance

    Design doc: "Mismatches are logged and corrected (local → exchange wins)."
    """

    def __init__(
        self,
        event_bus: EventBus,
        tracker: AccountTracker,
        engine: ExecutionEngine,
        info_client: Any = None,
        account_address: str = "",
        safety_controller: SafetyController | None = None,
        reconciliation_store: PostgresReconciliationStore | None = None,
        account_health: Any | None = None,
    ) -> None:
        self._event_bus = event_bus
        self._tracker = tracker
        self._engine = engine
        self._info = info_client
        self._account_address = account_address
        self._safety = safety_controller
        self._reconciliation_store = reconciliation_store
        self._account_health = account_health
        self._running = False

    def set_info_client(self, info: Any) -> None:
        """Set the HL SDK Info client (called after NonceManager initializes)."""
        self._info = info

    async def reconcile(self) -> ReconciliationResult:
        """Run a full reconciliation cycle.

        Returns ReconciliationResult with counts of corrections and any errors.
        On success, publishes EVENT_RECONCILIATION_COMPLETE to the EventBus.
        """
        run_id = await self._reconciliation_store.start() if self._reconciliation_store is not None else None
        if not self._info or not self._account_address:
            logger.warning("reconcile_skipped_no_client")
            result = ReconciliationResult(
                success=False,
                orders_corrected=0,
                positions_corrected=0,
                errors=["info_client_or_address_not_configured"],
            )
            await self._persist_reconciliation(run_id, result, [], {}, None)
            return result

        errors: list[str] = []
        orders_corrected = 0
        positions_corrected = 0

        logger.info("reconciliation_start", address=self._account_address)
        local_orders = await self._engine.get_open_orders()
        local_positions = dict(self._tracker.get_all_positions())

        # Step 1: Fetch exchange state
        try:
            exchange_open_orders = await self._fetch_exchange_open_orders()
        except Exception as e:
            errors.append(f"fetch_orders_failed: {e}")
            logger.error("reconcile_fetch_orders_failed", error=str(e))
            exchange_open_orders = []

        try:
            exchange_positions = await self._fetch_exchange_positions()
            exchange_account = await self._fetch_account_state()
        except Exception as e:
            errors.append(f"fetch_positions_failed: {e}")
            logger.error("reconcile_fetch_positions_failed", error=str(e))
            exchange_positions = {}
            exchange_account = None

        # Never mutate local state from partial/failed snapshots.
        if not errors and exchange_account is not None:
            try:
                orders_corrected = await self._reconcile_orders(exchange_open_orders)
                positions_corrected = self._reconcile_positions(exchange_positions)
                self._tracker.update_account_state(exchange_account)
            except Exception as e:
                errors.append(f"reconcile_apply_failed: {e}")
                logger.error("reconcile_apply_failed", error=str(e))

        success = len(errors) == 0

        result = ReconciliationResult(
            success=success,
            orders_corrected=orders_corrected,
            positions_corrected=positions_corrected,
            errors=errors,
        )
        diffs = self._build_diffs(local_orders, local_positions, exchange_open_orders, exchange_positions)
        await self._persist_reconciliation(run_id, result, diffs, exchange_positions, exchange_account)

        if result.success:
            if self._account_health is not None:
                from hypeedge.account.health import AccountHealthDimension

                self._account_health.record_success(AccountHealthDimension.RECONCILIATION)
                self._account_health.record_success(AccountHealthDimension.INVENTORY)
            if self._safety is not None:
                from hypeedge.core.enums import SafetyMode

                if self._safety.mode == SafetyMode.RECOVERING:
                    self._safety.transition(SafetyMode.NORMAL, "recovery_reconciliation_passed")
            self._event_bus.publish_sync(Event(event_type=EVENT_RECONCILIATION_COMPLETE, payload=result))
            logger.info(
                "reconciliation_complete",
                orders_corrected=orders_corrected,
                positions_corrected=positions_corrected,
            )
        else:
            if self._account_health is not None:
                from hypeedge.account.health import AccountHealthDimension

                self._account_health.record_failure(AccountHealthDimension.RECONCILIATION, "reconciliation_failed")
            logger.error("reconciliation_failed", errors=result.errors)
            if self._safety is not None:
                self._safety.enter_cancel_only("reconciliation_failed")

        return result

    async def _persist_reconciliation(
        self,
        run_id: uuid.UUID | None,
        result: ReconciliationResult,
        diffs: list[dict[str, Any]],
        exchange_positions: dict[str, dict[str, Any]],
        exchange_account: Any | None,
    ) -> None:
        if self._reconciliation_store is None or run_id is None:
            return
        try:
            await self._reconciliation_store.finish(
                run_id,
                success=result.success,
                errors=result.errors,
                diffs=diffs,
                exchange_positions=exchange_positions,
                exchange_account=exchange_account,
            )
        except Exception as exc:
            result.success = False
            result.errors.append(f"persist_reconciliation_failed: {exc}")
            logger.exception("reconciliation_persistence_failed", run_id=str(run_id))

    @staticmethod
    def _build_diffs(
        local_orders: list[Order],
        local_positions: dict[Symbol, Position],
        exchange_orders: list[dict[str, Any]],
        exchange_positions: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        diffs: list[dict[str, Any]] = []
        local_cloids = {str(order.cloid): order for order in local_orders}
        exchange_cloids = {str(order.get("cloid")): order for order in exchange_orders if order.get("cloid")}
        for cloid, order in local_cloids.items():
            canonical = cloid if cloid.startswith("0x") else CloidGenerator.to_hl_cloid(order.cloid)
            if canonical not in exchange_cloids:
                diffs.append(
                    {
                        "entity_type": "order",
                        "entity_key": cloid,
                        "difference_type": "local_open_missing_on_exchange",
                        "local_value": {"status": str(order.status)},
                        "exchange_value": None,
                    }
                )
        canonical_local = {
            cloid if cloid.startswith("0x") else CloidGenerator.to_hl_cloid(order.cloid)
            for cloid, order in local_cloids.items()
        }
        for cloid, exchange_order in exchange_cloids.items():
            if cloid not in canonical_local:
                diffs.append(
                    {
                        "entity_type": "order",
                        "entity_key": cloid,
                        "difference_type": "exchange_open_missing_locally",
                        "local_value": None,
                        "exchange_value": exchange_order,
                    }
                )
        for symbol, exchange in exchange_positions.items():
            local = local_positions.get(Symbol(symbol))
            exchange_size = float(exchange.get("szi", 0))
            if local is None or abs(float(local.size) - exchange_size) > 1e-8:
                diffs.append(
                    {
                        "entity_type": "position",
                        "entity_key": symbol,
                        "difference_type": "size_mismatch",
                        "local_value": None if local is None else {"size": str(local.size)},
                        "exchange_value": {"size": str(exchange_size)},
                        "severity": "critical",
                    }
                )
        for symbol, local in local_positions.items():
            if str(symbol) not in exchange_positions and not local.is_flat:
                diffs.append(
                    {
                        "entity_type": "position",
                        "entity_key": str(symbol),
                        "difference_type": "closed_on_exchange",
                        "local_value": {"size": str(local.size)},
                        "exchange_value": None,
                        "severity": "critical",
                    }
                )
        return diffs

    async def run_periodic(self, interval_seconds: int = 300) -> None:
        """Run reconciliation on a schedule."""
        self._running = True
        logger.info("reconciler_periodic_started", interval_s=interval_seconds)

        try:
            while self._running:
                await asyncio.sleep(interval_seconds)
                try:
                    await self.reconcile()
                except Exception as e:
                    logger.error("reconciliation_error", error=str(e))
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            logger.info("reconciler_periodic_stopped")

    async def stop(self) -> None:
        self._running = False

    async def refresh_exchange_open_orders_for_cancel(self) -> int:
        """Import every exchange-authoritative open order before cancel-all.

        This deliberately does not infer terminal state for local-only orders;
        the Kill Switch needs a complete cancel target set even when another
        reconciliation difference remains unresolved.
        """
        exchange_orders = await self._fetch_exchange_open_orders()
        local_open = await self._engine.get_open_orders()
        local_cloids = {
            str(order.cloid) if str(order.cloid).startswith("0x") else CloidGenerator.to_hl_cloid(order.cloid)
            for order in local_open
        }
        for exchange_order in exchange_orders:
            canonical = str(exchange_order.get("cloid", ""))
            if not canonical:
                raise RuntimeError("exchange_open_order_missing_cloid")
            if canonical in local_cloids:
                continue
            imported = self._parse_exchange_order(exchange_order, canonical)
            await self._engine.import_exchange_order_authoritative(imported)
            local_cloids.add(canonical)
            logger.warning("kill_switch_imported_exchange_order", cloid=canonical, symbol=str(imported.symbol))
        return len(exchange_orders)

    async def authoritative_open_orders_empty(self) -> bool:
        """Return True only for a valid, empty exchange open-order snapshot."""
        return len(await self._fetch_exchange_open_orders()) == 0

    # --- Exchange data fetching ---

    async def _fetch_exchange_open_orders(self) -> list[dict[str, Any]]:
        """Fetch all open orders from the exchange."""
        if not self._info:
            raise RuntimeError("info_client_not_configured")
        result = await asyncio.to_thread(self._info.open_orders, self._account_address)
        if not isinstance(result, list):
            raise RuntimeError("invalid_open_orders_response")
        return result

    async def _fetch_exchange_positions(self) -> dict[str, dict[str, Any]]:
        """Fetch positions from the exchange clearinghouse state."""
        if not self._info:
            raise RuntimeError("info_client_not_configured")
        state = await asyncio.to_thread(self._info.user_state, self._account_address)
        if not isinstance(state, dict) or "assetPositions" not in state:
            raise RuntimeError("invalid_user_state_response")
        positions: dict[str, dict[str, Any]] = {}
        for pos_data in state.get("assetPositions", []):
            pos_info = pos_data.get("position", {})
            coin = pos_info.get("coin", "")
            if coin:
                positions[coin] = pos_info
        return positions

    async def _fetch_account_state(self) -> Any:
        """Fetch account state from exchange."""
        if not self._info:
            raise RuntimeError("info_client_not_configured")
        state = await asyncio.to_thread(self._info.user_state, self._account_address)
        if not isinstance(state, dict) or "marginSummary" not in state:
            raise RuntimeError("invalid_account_state_response")
        margin_summary = state["marginSummary"]
        from hypeedge.core.types import Usd

        unrealized_pnl = sum(
            (
                Decimal(str(item.get("position", {}).get("unrealizedPnl", 0)))
                for item in state.get("assetPositions", [])
            ),
            start=Decimal(0),
        )
        account_value = Decimal(str(margin_summary.get("accountValue", 0)))
        available = Decimal(str(state.get("withdrawable", margin_summary.get("totalMarginAvailable", 0))))
        return AccountState(
            equity=Usd(account_value),
            available_balance=Usd(available),
            total_margin_used=Usd(margin_summary.get("totalMarginUsed", max(Decimal(0), account_value - available))),
            total_unrealized_pnl=Usd(unrealized_pnl),
            peak_equity=max(self._tracker.peak_equity, Usd(account_value)),
        )

    # --- Order reconciliation ---

    async def _reconcile_orders(self, exchange_orders: list[dict[str, Any]]) -> int:
        """Compare local orders with exchange orders and correct discrepancies.

        Returns the number of orders corrected.
        """
        corrected = 0

        # Build lookup by canonical exchange cloid.
        exchange_by_cloid: dict[str, dict[str, Any]] = {}
        for order_data in exchange_orders:
            cloid = order_data.get("cloid", "")
            if cloid:
                exchange_by_cloid[str(cloid)] = order_data

        # Check local open orders against exchange
        local_open = await self._engine.get_open_orders()
        local_exchange_cloids: set[str] = set()
        for local_order in local_open:
            cloid_str = str(local_order.cloid)
            canonical = cloid_str if cloid_str.startswith("0x") else CloidGenerator.to_hl_cloid(local_order.cloid)
            local_exchange_cloids.add(canonical)
            if canonical not in exchange_by_cloid:
                logger.warning(
                    "reconcile_order_not_on_exchange",
                    cloid=cloid_str,
                    local_status=str(local_order.status),
                )
                status = await self._query_order_status(canonical)
                if status is None:
                    raise RuntimeError(f"order_status_unknown:{cloid_str}")
                applied = self._apply_order_status(local_order, status)
                if applied:
                    await self._engine.import_exchange_order_authoritative(local_order)
                corrected += applied

        # Import orders created outside this process instead of silently ignoring them.
        for canonical, exchange_order in exchange_by_cloid.items():
            if canonical in local_exchange_cloids:
                continue
            imported = self._parse_exchange_order(exchange_order, canonical)
            await self._engine.import_exchange_order_authoritative(imported)
            corrected += 1
            logger.warning("reconcile_imported_exchange_order", cloid=canonical, symbol=str(imported.symbol))

        return corrected

    async def _query_order_status(self, canonical_cloid: str) -> dict[str, Any] | None:
        if not self._info:
            raise RuntimeError("info_client_not_configured")
        from hyperliquid.utils.types import Cloid as HlCloid

        result = await asyncio.to_thread(
            self._info.query_order_by_cloid,
            self._account_address,
            HlCloid.from_str(canonical_cloid),
        )
        if not isinstance(result, dict):
            raise RuntimeError("invalid_order_status_response")
        if result.get("status") == "unknownOid":
            return None
        return result

    @staticmethod
    def _apply_order_status(order: Order, response: dict[str, Any]) -> int:
        status_name = str(response.get("order", {}).get("status", response.get("status", ""))).lower()
        mapping = {
            "filled": OrderStatus.FILLED,
            "canceled": OrderStatus.CANCELLED,
            "cancelled": OrderStatus.CANCELLED,
            "rejected": OrderStatus.REJECTED,
            "expired": OrderStatus.EXPIRED,
            "open": OrderStatus.ACKNOWLEDGED,
        }
        target = mapping.get(status_name)
        if target is None:
            raise RuntimeError(f"unsupported_order_status:{status_name}")
        if order.status != target:
            order.status = target
            order.error_message = "reconciler: exchange status applied"
            return 1
        return 0

    @staticmethod
    def _parse_exchange_order(data: dict[str, Any], canonical_cloid: str) -> Order:
        side_raw = str(data.get("side", "B")).lower()
        side = Side.BUY if side_raw in {"b", "buy"} else Side.SELL
        return Order(
            cloid=Cloid(canonical_cloid),
            symbol=Symbol(str(data.get("coin", ""))),
            side=side,
            size=Size(data.get("sz", data.get("origSz", 0))),
            price=Price(data.get("limitPx", 0)),
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            status=OrderStatus.ACKNOWLEDGED,
            exchange_oid=OrderId(str(data.get("oid"))) if data.get("oid") is not None else None,
            reduce_only=bool(data.get("reduceOnly", False)),
        )

    # --- Position reconciliation ---

    def _reconcile_positions(self, exchange_positions: dict[str, dict[str, Any]]) -> int:
        """Compare local positions with exchange positions and correct.

        "Local → exchange wins" — we replace local with exchange truth.
        Returns the number of positions corrected.
        """
        corrected = 0

        # Check each exchange position
        for coin, pos_data in exchange_positions.items():
            symbol = Symbol(coin)
            exchange_size = float(pos_data.get("szi", 0))
            exchange_entry = float(pos_data.get("entryPx", 0))
            leverage_raw = pos_data.get("leverage", {})
            exchange_leverage = int(float(leverage_raw.get("value", 1))) if isinstance(leverage_raw, dict) else 1

            local_pos = self._tracker.get_position(symbol)

            if local_pos is None:
                # Exchange has a position we don't know about
                logger.warning(
                    "reconcile_missing_local_position",
                    symbol=coin,
                    exchange_size=exchange_size,
                )
                self._tracker.update_position_from_exchange(
                    symbol,
                    Position(
                        symbol=symbol,
                        size=Size(exchange_size),
                        entry_price=Price(exchange_entry),
                        leverage=exchange_leverage,
                    ),
                )
                corrected += 1
            elif abs(local_pos.size - exchange_size) > 1e-8:
                # Size mismatch — exchange wins
                logger.warning(
                    "reconcile_position_size_mismatch",
                    symbol=coin,
                    local_size=float(local_pos.size),
                    exchange_size=exchange_size,
                )
                self._tracker.update_position_from_exchange(
                    symbol,
                    Position(
                        symbol=symbol,
                        size=Size(exchange_size),
                        entry_price=Price(exchange_entry),
                        leverage=exchange_leverage,
                    ),
                )
                corrected += 1

        # Check for local positions that don't exist on exchange
        local_positions = self._tracker.get_all_positions()
        for symbol in local_positions:
            if str(symbol) not in exchange_positions:
                logger.warning(
                    "reconcile_position_closed_on_exchange",
                    symbol=str(symbol),
                    local_size=float(local_positions[symbol].size),
                )
                self._tracker.remove_position(symbol)
                corrected += 1

        return corrected
