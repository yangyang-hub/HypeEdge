"""Application adapters for the market-maker runtime boundary.

The adapters deliberately keep the live boundary fail closed.  Shadow mode can
always evaluate from in-process projections, while live placement is accepted
only when a repository exposes one atomic durable quote-plan transaction.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol, cast

from hypeedge.account.health import AccountHealthProvider, FreshnessResult
from hypeedge.account.tracker import AccountTracker
from hypeedge.core.enums import MarketMakerLifecycle, Side
from hypeedge.core.exceptions import StrategyLifecycleError, TradingCommandPersistenceError
from hypeedge.core.types import Size, StrategyId, SubAccount, Symbol, Usd
from hypeedge.risk.action_budget import ActionBudgetController
from hypeedge.strategy.market_maker.models import ActionBudgetSnapshot, InventorySnapshot
from hypeedge.strategy.market_maker.runtime import QuoteCancelRequest
from hypeedge.strategy.supervisor import StrategyStateStore
from hypeedge.trading.quotes import QuotePlan, QuoteSlotKey, QuoteSlotView


class AtomicQuotePlanRepository(Protocol):
    async def submit_quote_plan(self, plan: QuotePlan) -> None: ...


class TrackerInventoryProvider:
    """Convert the authoritative account tracker into policy inventory input."""

    def __init__(self, tracker: AccountTracker, health: AccountHealthProvider) -> None:
        self._tracker = tracker
        self._health = health

    def get_inventory(self, sub_account: SubAccount, symbol: Symbol) -> InventorySnapshot:
        state = self._tracker.get_account_state()
        position = self._tracker.get_position(symbol)
        health = self._health.get_account_health()
        matches_account = state is not None and (state.sub_account is None or state.sub_account == sub_account)
        matches_position = position is None or position.sub_account is None or position.sub_account == sub_account
        return InventorySnapshot(
            position_size=position.size if position is not None and matches_position else Size("0"),
            equity=state.equity if state is not None and matches_account else Usd("0"),
            available_balance=state.available_balance if state is not None and matches_account else Usd("0"),
            margin_used=state.total_margin_used if state is not None and matches_account else Usd("0"),
            observed_at=self._tracker.last_update_ts or datetime.fromtimestamp(0, tz=UTC),
            healthy=matches_account and matches_position and health.allows_risk_increase,
        )


class ControllerBudgetProvider:
    """Expose the conservative three-ledger controller to the pure policy."""

    def __init__(self, controller: ActionBudgetController) -> None:
        self._controller = controller

    def get_action_budget(self, strategy_id: StrategyId, symbol: Symbol) -> ActionBudgetSnapshot:
        del strategy_id, symbol
        now = datetime.now(UTC)
        view = self._controller.snapshot(now=now)
        return ActionBudgetSnapshot(
            mode=view.mode,
            address_actions_remaining=view.placement_actions_available,
            cancel_headroom=view.cancel_headroom_remaining,
            ip_weight_remaining=view.ip_weight_remaining,
            action_shadow_cost_usdc=Usd("0"),
            observed_at=now,
            healthy=view.remote_fresh and view.cancel_headroom_fresh,
        )


class FailClosedQuoteSlotProvider:
    """Block live quoting until an authoritative slot projection is installed."""

    async def get_quote_slots(self, strategy_id: StrategyId, symbol: Symbol) -> tuple[QuoteSlotView, QuoteSlotView]:
        del strategy_id, symbol
        raise TradingCommandPersistenceError("authoritative quote-slot projection is unavailable")

    @staticmethod
    def empty(strategy_id: StrategyId, symbol: Symbol) -> tuple[QuoteSlotView, QuoteSlotView]:
        return tuple(QuoteSlotView(QuoteSlotKey(strategy_id, symbol, side), 0, 0, ()) for side in (Side.BUY, Side.SELL))  # type: ignore[return-value]


class DurableQuotePlanCommandAdapter:
    """Persist a complete live plan before execution; cancellation stays unconditional."""

    def __init__(
        self,
        *,
        repository: object,
        cancel_all: Callable[[], Awaitable[int]],
        live_ready: Callable[[], bool] | None = None,
    ) -> None:
        self._repository = repository
        self._cancel_all = cancel_all
        self._live_ready = live_ready or (lambda: True)

    @property
    def live_enabled(self) -> bool:
        """Whether one atomic durable plan-to-command boundary is installed."""
        return callable(getattr(self._repository, "submit_quote_plan", None)) and self._live_ready()

    async def submit_quote_plan(self, plan: QuotePlan) -> None:
        submit = getattr(self._repository, "submit_quote_plan", None)
        if not callable(submit):
            raise TradingCommandPersistenceError(
                "atomic durable quote-plan persistence is unavailable; live placement rejected"
            )
        await submit(plan)

    async def cancel_strategy_quotes(self, request: QuoteCancelRequest) -> None:
        del request
        # Broad authoritative cancellation is safer than retaining an order
        # when a per-strategy durable cancel primitive is unavailable.
        await self._cancel_all()


class LiveCapabilityStrategySupervisor:
    """Reject unsupported lifecycle actions and RUNNING before live capability exists."""

    def __init__(
        self,
        supervisor: Any,  # noqa: ANN401
        commands: DurableQuotePlanCommandAdapter,
        *,
        registry: Any | None = None,  # noqa: ANN401
    ) -> None:
        self._supervisor = supervisor
        self._commands = commands
        self._registry = registry if registry is not None else getattr(supervisor, "_registry", None)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._supervisor, name)

    async def start(
        self,
        strategy_id: StrategyId,
        *,
        target: MarketMakerLifecycle = MarketMakerLifecycle.RUNNING,
        expected_revision: int | None = None,
    ) -> Any:
        await self._require_action(strategy_id, "start", target=target)
        return await self._supervisor.start(
            strategy_id,
            target=target,
            expected_revision=expected_revision,
        )

    async def resume(
        self,
        strategy_id: StrategyId,
        *,
        target: MarketMakerLifecycle = MarketMakerLifecycle.RUNNING,
    ) -> Any:
        await self._require_action(strategy_id, "resume", target=target)
        return await self._supervisor.resume(strategy_id, target=target)

    async def pause(self, strategy_id: StrategyId) -> Any:
        await self._require_action(strategy_id, "pause")
        return await self._supervisor.pause(strategy_id)

    async def drain(self, strategy_id: StrategyId) -> Any:
        await self._require_action(strategy_id, "drain")
        return await self._supervisor.drain(strategy_id)

    async def stop(self, strategy_id: StrategyId) -> Any:
        await self._require_action(strategy_id, "stop")
        return await self._supervisor.stop(strategy_id)

    async def _require_action(
        self,
        strategy_id: StrategyId,
        action: str,
        *,
        target: MarketMakerLifecycle | None = None,
    ) -> None:
        from hypeedge.strategy.plugin import MARKET_MAKER_CAPABILITIES

        instance = await self._supervisor._store.get_instance(strategy_id)
        capabilities = None
        if self._registry is not None:
            capabilities = self._registry.capabilities(instance.strategy_type)
        if capabilities is None and instance.strategy_type == "market_maker":
            capabilities = MARKET_MAKER_CAPABILITIES
        if capabilities is not None:
            if action not in capabilities.actions:
                raise StrategyLifecycleError(
                    f"Action '{action}' is not supported for strategy_type={instance.strategy_type}"
                )
            if target == MarketMakerLifecycle.SHADOW and not capabilities.supports_shadow:
                raise StrategyLifecycleError(
                    f"Shadow mode is not supported for strategy_type={instance.strategy_type}"
                )
            if action == "drain" and not capabilities.supports_drain:
                raise StrategyLifecycleError(
                    f"Drain is not supported for strategy_type={instance.strategy_type}"
                )
        if instance.strategy_type == "market_maker" and target is not None:
            self._require_live_capability(target)

    def _require_live_capability(self, target: MarketMakerLifecycle) -> None:
        if target == MarketMakerLifecycle.RUNNING and not self._commands.live_enabled:
            raise StrategyLifecycleError(
                "RUNNING is unavailable until atomic durable quote-plan execution is installed"
            )


def _freshness(result: FreshnessResult) -> dict[str, Any]:
    return {
        "status": result.status.value,
        "observed_at": result.observed_at,
        "age_ms": result.age_seconds * 1000 if result.age_seconds is not None else None,
        "threshold_ms": result.max_age_seconds * 1000,
        "reason": result.reason,
    }


class MarketMakingRepositoryFacade:
    """Merge durable truth with ephemeral runtime/health snapshots for the API."""

    def __init__(
        self,
        repository: object,
        state_store: StrategyStateStore,
        *,
        runtime_snapshot: Callable[[StrategyId], Any | None],
        tracker: AccountTracker,
        health: AccountHealthProvider,
        budget: ActionBudgetController,
        environment: str,
        safety_mode: Callable[[], str],
        kill_switch_active: Callable[[], bool],
    ) -> None:
        self._repository = repository
        self._state_store = state_store
        self._runtime_snapshot = runtime_snapshot
        self._tracker = tracker
        self._health = health
        self._budget = budget
        self._environment = environment
        self._safety_mode = safety_mode
        self._kill_switch_active = kill_switch_active

    def __getattr__(self, name: str) -> Any:
        # Prefer durable repository for create_* helpers that use keyword API signatures.
        if name.startswith("create_"):
            repo_method = getattr(self._repository, name, None)
            if repo_method is not None:
                return repo_method
        state_method = getattr(self._state_store, name, None)
        if state_method is not None:
            return state_method
        return getattr(self._repository, name)

    async def create_strategy_instance(
        self,
        *,
        strategy_id: StrategyId,
        sub_account: Any,
        symbol: Any,
        initial_config: Mapping[str, Any],
        created_by: str,
        metadata: Mapping[str, Any] | None = None,
        strategy_type: str = "market_maker",
    ) -> dict[str, Any]:
        create = self._repository.create_strategy_instance
        view = await create(
            strategy_id=strategy_id,
            sub_account=sub_account,
            symbol=symbol,
            initial_config=initial_config,
            created_by=created_by,
            metadata=metadata,
            strategy_type=strategy_type,
        )
        return await self._strategy_instance_payload(view)

    async def list_strategy_instances(self, *, include_archived: bool = False) -> list[dict[str, Any]]:
        list_views = cast(Any, self._state_store).list_strategy_instances
        views = await list_views(include_archived=include_archived)
        return [await self._strategy_instance_payload(view) for view in views]

    async def get_strategy_instance(self, strategy_id: StrategyId) -> dict[str, Any]:
        get_view = cast(Any, self._state_store).get_strategy_instance
        return await self._strategy_instance_payload(await get_view(strategy_id))

    async def _strategy_instance_payload(self, view: Any) -> dict[str, Any]:  # noqa: ANN401
        definition = view.definition
        runtime = await self._state_store.get_runtime(definition.strategy_id)
        live = self._runtime_snapshot(definition.strategy_id)
        live_mode = getattr(getattr(live, "mode", None), "value", None)
        session_mode = (
            "shadow"
            if live_mode == "shadow"
            else self._environment
            if live_mode == "running" and self._environment in {"testnet", "mainnet"}
            else None
        )
        return {
            "strategy_id": str(definition.strategy_id),
            "strategy_type": definition.strategy_type,
            "symbol": str(definition.symbol),
            "sub_account": str(definition.sub_account),
            "desired_state": definition.desired_state.value,
            "actual_state": runtime.actual_state.value,
            "desired_config_version_id": definition.desired_config_revision,
            "effective_config_version_id": runtime.effective_config_revision,
            "revision": definition.revision,
            "archived_at": view.archived_at,
            "created_at": view.created_at,
            "updated_at": view.updated_at,
            "metadata": dict(view.metadata),
            "session_mode": session_mode,
        }

    async def get_market_making_state(self, strategy_id: StrategyId) -> dict[str, Any]:
        instance = await self._state_store.get_instance(strategy_id)
        runtime = await self._state_store.get_runtime(strategy_id)
        live = self._runtime_snapshot(strategy_id)
        health = self._health.get_account_health()
        budget = self._budget.snapshot()
        now = datetime.now(UTC)
        market_at = getattr(live, "last_cycle_at", None)
        market_age_ms = (now - market_at).total_seconds() * 1000 if market_at is not None else None
        market_fresh = market_age_ms is not None and market_age_ms <= 5000
        live_mode = getattr(live, "mode", None)
        if live_mode is not None and live_mode.value == "shadow":
            session_mode = "shadow"
        elif live_mode is not None and live_mode.value == "running" and self._environment in {"testnet", "mainnet"}:
            session_mode = self._environment
        else:
            session_mode = None
        return {
            "strategy_id": str(strategy_id),
            "strategy_type": instance.strategy_type,
            "symbol": str(instance.symbol),
            "sub_account": str(instance.sub_account),
            "environment": self._environment,
            "desired_state": instance.desired_state.value,
            "actual_state": runtime.actual_state.value,
            "runtime_revision": runtime.revision,
            "market_revision": getattr(live, "market_version", None) or 0,
            "config_version": runtime.effective_config_revision or instance.desired_config_revision,
            "session_id": getattr(live, "session_id", None),
            "session_mode": session_mode,
            "quote_uptime_pct": None,
            "kill_switch_active": self._kill_switch_active(),
            "safety_mode": self._safety_mode(),
            "freshness": {
                "market": {
                    "status": "fresh" if market_fresh else "stale",
                    "observed_at": market_at,
                    "age_ms": market_age_ms,
                    "threshold_ms": 5000,
                    "reason": None if market_fresh else "market_snapshot_stale_or_missing",
                },
                "inventory": _freshness(health.inventory),
                "clearinghouse": _freshness(health.clearinghouse),
                "user_stream": _freshness(health.user_stream),
                "reconciliation": _freshness(health.reconciliation),
                "action_budget": {
                    "status": "fresh" if budget.remote_fresh and budget.cancel_headroom_fresh else "stale",
                    "observed_at": now,
                    "age_ms": 0,
                    "threshold_ms": 0,
                    "reason": None if budget.remote_fresh and budget.cancel_headroom_fresh else "action_budget_stale",
                },
                "postgres": {
                    "status": "fresh",
                    "observed_at": now,
                    "age_ms": 0,
                    "threshold_ms": 0,
                    "reason": None,
                },
            },
            "alerts": [],
            "observed_at": now,
            "stale": not (market_fresh and health.allows_risk_increase and budget.remote_fresh),
        }

    async def get_market_making_quotes(self, strategy_id: StrategyId) -> dict[str, Any]:
        live = self._runtime_snapshot(strategy_id)
        instance = await self._state_store.get_instance(strategy_id)
        desired = getattr(live, "desired", None)
        features = getattr(live, "features", None)
        get_slots = getattr(self._repository, "get_quote_slots", None)
        authoritative = await get_slots(strategy_id, instance.symbol) if callable(get_slots) else ()
        views = {view.key.side: view for view in authoritative}
        slots: list[dict[str, Any]] = []
        if desired is not None:
            for quote in (desired.bid, desired.ask):
                view = views.get(quote.slot.side)
                owner = view.current_owner if view is not None else None
                if view is not None and view.has_unknown:
                    state = "unknown"
                elif view is not None and view.has_orphaned_owner:
                    state = "orphaned_live"
                elif view is not None and view.has_inflight:
                    state = "inflight"
                elif owner is not None:
                    state = "live"
                else:
                    state = "empty"
                slots.append(
                    {
                        "side": quote.slot.side.value,
                        "level": quote.slot.level,
                        "state": state,
                        "desired_price": quote.price,
                        "desired_size": quote.size,
                        "live_price": owner.price if owner is not None else None,
                        "live_remaining_size": owner.remaining_size if owner is not None else None,
                        "cloid": str(owner.cloid) if owner is not None else None,
                        "quote_revision": desired.revision,
                        "quote_age_ms": (
                            max(0, int((datetime.now(UTC) - owner.live_since).total_seconds() * 1000))
                            if owner is not None
                            else None
                        ),
                        "gross_edge_bps": None,
                        "no_quote_reason": quote.reason if quote.price is None else None,
                    }
                )
        external_reference = None
        if features is not None and features.external_source is not None:
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
        return {
            "strategy_id": str(strategy_id),
            "symbol": str(instance.symbol),
            "runtime_revision": getattr(live, "quote_revision", 0),
            "market_revision": getattr(live, "market_version", None) or 0,
            "fair_price": getattr(desired, "fair_price", None),
            "reservation_price": getattr(desired, "reservation_price", None),
            "best_bid": getattr(features, "best_bid", None),
            "best_ask": getattr(features, "best_ask", None),
            "slots": slots,
            "external_reference": external_reference,
            "observed_at": getattr(live, "last_cycle_at", None) or datetime.now(UTC),
            "stale": desired is None,
        }

    async def get_market_making_inventory(self, strategy_id: StrategyId) -> dict[str, Any]:
        instance = await self._state_store.get_instance(strategy_id)
        runtime = await self._state_store.get_runtime(strategy_id)
        config = await self._state_store.get_config(
            strategy_id, runtime.effective_config_revision or instance.desired_config_revision
        )
        live = self._runtime_snapshot(strategy_id)
        position = self._tracker.get_position(instance.symbol)
        state = self._tracker.get_account_state()
        size = position.size if position is not None else Size("0")
        mark = position.mark_price if position is not None else None
        notional = Usd(abs(Decimal(size)) * Decimal(mark)) if mark is not None else Usd("0")
        hard = config.values.get("hard_inventory_notional")
        utilization = Decimal(notional) / Decimal(str(hard)) if hard not in (None, 0) else Decimal("0")
        return {
            "strategy_id": str(strategy_id),
            "symbol": str(instance.symbol),
            "runtime_revision": runtime.revision,
            "market_revision": getattr(live, "market_version", None) or 0,
            "position_size": size,
            "inventory_notional": notional,
            "soft_limit_notional": config.values.get("soft_inventory_notional"),
            "hard_limit_notional": hard,
            "emergency_limit_notional": config.values.get("emergency_inventory_notional"),
            "inventory_utilization": utilization,
            "inventory_shift_bps": None,
            "margin_used": state.total_margin_used if state is not None else None,
            "available_margin": state.available_balance if state is not None else None,
            "liquidation_distance_pct": None,
            "funding_carry": self._tracker.total_funding,
            "reduction_mode": "none",
            "observed_at": self._tracker.last_update_ts or datetime.now(UTC),
            "stale": state is None or not self._health.get_account_health().allows_risk_increase,
        }

    async def get_market_making_action_budget(self, strategy_id: StrategyId) -> dict[str, Any]:
        view = self._budget.snapshot()
        windows = {window.window_hours: window for window in view.windows}
        day = windows.get(24)
        return {
            "strategy_id": str(strategy_id),
            "mode": view.mode.value,
            "remote_cap": view.remote_cap,
            "remote_used": view.remote_used,
            "remote_remaining": view.address_remaining,
            "shadow_remaining": view.placement_actions_available,
            "emergency_reserve": view.required_cancel_reserve + view.close_action_reserve,
            "cancel_headroom": view.cancel_headroom_remaining,
            "ip_weight_remaining": view.ip_weight_remaining,
            "burn_rate_1h": windows[1].net_burn_per_hour,
            "burn_rate_6h": windows[6].net_burn_per_hour,
            "burn_rate_24h": windows[24].net_burn_per_hour,
            "earned_rate_24h": Decimal("0") if day is None else day.earned_actions / Decimal("24"),
            "usdc_per_action": None if day is None else day.marginal_usdc_per_action,
            "actions_per_fill": None if day is None else day.actions_per_fill,
            "runway_hours": None if day is None else day.runway_hours,
            "revision": 0,
            "observed_at": datetime.now(UTC),
            "stale": not (view.remote_fresh and view.cancel_headroom_fresh),
        }
