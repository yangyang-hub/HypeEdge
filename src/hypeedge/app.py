"""HypeEdge application lifecycle management."""

from __future__ import annotations

import asyncio
import contextlib
import signal
from typing import TYPE_CHECKING, Any

import structlog

from hypeedge.config.loader import load_settings
from hypeedge.config.settings import AppSettings
from hypeedge.core.events import EVENT_KILL_SWITCH_TRIGGERED, Event, EventBus
from hypeedge.core.exceptions import KillSwitchTriggeredError
from hypeedge.core.types import StrategyId
from hypeedge.execution.order_state import OrderStateMachine
from hypeedge.risk.kill_switch import KillSwitch

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

    from hypeedge.account.exchange_ingestor import ExchangeEventIngestor
    from hypeedge.account.health import AccountStatePoller, LayeredAccountHealthProvider
    from hypeedge.account.reconciler import Reconciler
    from hypeedge.account.tracker import AccountTracker
    from hypeedge.api.commands import ApiCommandService
    from hypeedge.execution.durable import DurableOrderStore
    from hypeedge.execution.emergency_cancel import EmergencyCancelExecutor
    from hypeedge.execution.engine import ExecutionEngine
    from hypeedge.execution.nonce import NonceManager
    from hypeedge.execution.worker import SignedActionExecutor
    from hypeedge.market_data.backfill import BackfillScheduler
    from hypeedge.market_data.binance_feed import BinanceReferenceFeed
    from hypeedge.market_data.checkpoint import BackfillCheckpointStore
    from hypeedge.market_data.external_reference import LatestExternalReferenceProvider
    from hypeedge.market_data.instrument_cache import InstrumentMetaCache
    from hypeedge.market_data.live_provider import LiveMarketDataProvider
    from hypeedge.market_data.rate_limiter import RateLimiter
    from hypeedge.market_data.rest_client import RestClient
    from hypeedge.market_data.ws_feed import WebSocketFeed
    from hypeedge.monitor.metrics import MetricsCollector
    from hypeedge.risk.action_budget import ActionBudgetController
    from hypeedge.risk.checker import RiskChecker
    from hypeedge.storage.clickhouse import ClickHouseWriter
    from hypeedge.storage.data_quality import DataQualityChecker
    from hypeedge.storage.outbox import DurableControlEventWriter, OutboxDispatcher, PostgresOutboxStore
    from hypeedge.storage.postgres import PostgresProjectionReader, PostgresSystemStateStore
    from hypeedge.strategy.params import ParamWatcher
    from hypeedge.strategy.runner import StrategyRunner
    from hypeedge.strategy.trend_follow import TrendFollowStrategy

logger = structlog.get_logger(__name__)

_AUTOMATIC_SAFETY_REASON_PREFIX = "automatic_safety_degradation:"
_HEALTH_MONITOR_INTERVAL_SECONDS = 1.0
_RECOVERY_RETRY_INTERVAL_SECONDS = 5.0


def setup_logging(log_level: str) -> None:
    """Configure structlog for structured JSON logging."""
    import logging

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.dev.ConsoleRenderer() if log_level == "DEBUG" else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Set stdlib logging level
    logging.basicConfig(format="%(message)s", level=getattr(logging, log_level))


class HypeEdgeApp:
    """Main application class. Owns the event loop and all component lifecycles."""

    def __init__(self, settings: AppSettings | None = None) -> None:
        self.settings = settings or load_settings()
        self.event_bus = EventBus()
        self._tasks: list[asyncio.Task[Any]] = []
        # One-shot background tasks that must NOT gate the process lifetime.
        # Unlike ``self._tasks`` (long-lived loops whose completion signals
        # shutdown), these are expected to return on their own and are only
        # cancelled/awaited during graceful shutdown. Putting a finite task in
        # ``self._tasks`` makes ``run()`` shut the whole app down as soon as it
        # finishes.
        self._background_tasks: list[asyncio.Task[Any]] = []
        self._shutdown_event = asyncio.Event()
        self._kill_switch_active = False
        from hypeedge.risk.safety import SafetyController

        self._safety_controller = SafetyController()

        # Components (initialized in _initialize_components())
        self._ws_feed: WebSocketFeed | None = None
        self._ch_writer: ClickHouseWriter | None = None
        self._metrics: MetricsCollector | None = None
        self._rest_client: RestClient | None = None
        self._backfill: BackfillScheduler | None = None
        self._instrument_cache: InstrumentMetaCache | None = None
        self._market_data_provider: LiveMarketDataProvider | None = None
        self._external_reference_provider: LatestExternalReferenceProvider | None = None
        self._binance_reference_feed: BinanceReferenceFeed | None = None
        self._checkpoint_store: BackfillCheckpointStore | None = None
        self._quality_checker: DataQualityChecker | None = None

        # Phase 2 components
        self._kill_switch: KillSwitch = KillSwitch(self.event_bus, self._safety_controller)
        self._order_state_machine: OrderStateMachine = OrderStateMachine()
        self._tracker: AccountTracker | None = None
        self._nonce_manager: NonceManager | None = None
        self._execution_engine: ExecutionEngine | None = None
        self._risk_checker: RiskChecker | None = None
        self._reconciler: Reconciler | None = None
        self._trading_enabled: bool = False
        self._rate_limiter: RateLimiter | None = None
        self._pg_engine: AsyncEngine | None = None
        self._pg_session_factory: async_sessionmaker[AsyncSession] | None = None
        self._projection_reader: PostgresProjectionReader | None = None
        self._system_state_store: PostgresSystemStateStore | None = None
        self._exchange_ingestor: ExchangeEventIngestor | None = None
        self._account_health: LayeredAccountHealthProvider | None = None
        self._account_state_poller: AccountStatePoller | None = None
        self._emergency_cancel_executor: EmergencyCancelExecutor | None = None
        self._action_budget_controller: ActionBudgetController | None = None
        self._durable_order_store: DurableOrderStore | None = None
        self._api_command_service: ApiCommandService | None = None
        self._signed_action_executor: SignedActionExecutor | None = None
        self._api_server: Any = None  # uvicorn.Server — kept so we can request a graceful exit
        self._outbox_store: PostgresOutboxStore | None = None
        self._outbox_dispatcher: OutboxDispatcher | None = None
        self._control_event_writer: DurableControlEventWriter | None = None
        self._api_sse_broker: Any | None = None
        self._strategy_supervisor: Any | None = None
        self._market_making_repository: Any | None = None
        self._market_making_state_store: Any | None = None
        self._quote_plan_worker: Any | None = None
        self._trading_prerequisites_ok = False
        self._automatic_safety_lock = asyncio.Lock()

        # Strategy components
        self._strategy: TrendFollowStrategy | None = None
        self._strategy_runner: StrategyRunner | None = None
        self._strategy_task: asyncio.Task[None] | None = None
        self._param_watcher: ParamWatcher | None = None

    async def run(self) -> None:
        """Main entry point. Starts all components and runs until shutdown."""
        setup_logging(self.settings.log_level)
        logger.info(
            "hypeedge_starting",
            environment=self.settings.environment,
            version="0.2.0",
        )

        # Design doc §16.2: Validate clock against HL server time
        if self.settings.exchange.is_configured and self.settings.features.v2_trading_enabled:
            clock_ok = await self._validate_clock()
            if not clock_ok:
                logger.critical("startup_clock_drift_too_large")
                return

        # Register signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._request_shutdown, sig)

        # Subscribe to kill switch events
        kill_queue = self.event_bus.subscribe(EVENT_KILL_SWITCH_TRIGGERED)
        asyncio.create_task(self._watch_kill_switch(kill_queue))

        try:
            await self._initialize_components()
            self._tasks = await self._start_components()

            logger.info("hypeedge_running", tasks=len(self._tasks))

            # Wait for shutdown signal or first task ending. Any long-lived
            # task in ``self._tasks`` completing (normally or by error) triggers
            # shutdown, so one-shot setup tasks must live in
            # ``self._background_tasks`` — otherwise a task that returns on its
            # own (e.g. market_making_restore) tears the whole app down.
            shutdown_waiter = asyncio.create_task(self._shutdown_event.wait(), name="shutdown_waiter")
            done, pending = await asyncio.wait(
                self._tasks + [shutdown_waiter],
                return_when=asyncio.FIRST_COMPLETED,
            )
            signal_triggered = shutdown_waiter in done
            shutdown_waiter.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await shutdown_waiter

            # Check for unexpected task endings.
            for task in done:
                if task is shutdown_waiter or task.cancelled():
                    continue
                exc = task.exception()
                if exc and not isinstance(exc, (asyncio.CancelledError, KillSwitchTriggeredError)):
                    logger.error("task_failed", task=task.get_name(), error=str(exc))
                elif not signal_triggered:
                    logger.warning("task_exited_triggering_shutdown", task=task.get_name())

        except Exception:
            logger.exception("hypeedge_fatal_error")
        finally:
            await self._graceful_shutdown()

    async def _initialize_components(self) -> None:
        """Initialize all components."""
        # Lazy imports to avoid circular dependencies
        from hypeedge.market_data.backfill import BackfillScheduler
        from hypeedge.market_data.binance_feed import BinanceReferenceFeed
        from hypeedge.market_data.checkpoint import BackfillCheckpointStore
        from hypeedge.market_data.external_reference import LatestExternalReferenceProvider
        from hypeedge.market_data.instrument_cache import InstrumentMetaCache
        from hypeedge.market_data.live_provider import LiveMarketDataProvider
        from hypeedge.market_data.rate_limiter import RateLimiter
        from hypeedge.market_data.rest_client import RestClient
        from hypeedge.market_data.ws_feed import WebSocketFeed
        from hypeedge.monitor.metrics import MetricsCollector
        from hypeedge.storage.clickhouse import ClickHouseWriter
        from hypeedge.storage.data_quality import DataQualityChecker
        from hypeedge.storage.dedup import DedupFilter

        backfill_settings = self.settings.backfill

        # Core infrastructure
        self._rate_limiter = RateLimiter(
            action_credits_low_watermark=self.settings.risk.action_credits_low_watermark,
        )
        dedup_filter = DedupFilter(max_keys=backfill_settings.dedup_max_keys)

        # Checkpoint store — load from disk
        self._checkpoint_store = BackfillCheckpointStore(state_dir=backfill_settings.state_dir)
        self._checkpoint_store.load()

        # Market data components
        self._metrics = MetricsCollector(self.settings, self.event_bus)
        self._ch_writer = ClickHouseWriter(
            self.settings.clickhouse,
            self.event_bus,
            dedup_filter=dedup_filter,
        )
        self._rest_client = RestClient(self.settings, self.event_bus, self._rate_limiter)
        self._instrument_cache = InstrumentMetaCache(self._rest_client)
        metadata_required_for_trading = (
            self.settings.exchange.is_configured and self.settings.features.v2_trading_enabled
        )
        if (
            self.settings.market_data.spot_coins
            or self.settings.features.funding_arb_execution_enabled
            or metadata_required_for_trading
        ):
            try:
                await self._instrument_cache.ensure_loaded()
            except Exception:
                if self.settings.features.funding_arb_execution_enabled or metadata_required_for_trading:
                    raise
                logger.exception("spot_metadata_initial_load_failed")
        self._ws_feed = WebSocketFeed(
            self.settings,
            self.event_bus,
            spot_resolver=self._instrument_cache,
        )
        self._market_data_provider = LiveMarketDataProvider(
            self.settings,
            self.event_bus,
            self._rest_client,
            self._ws_feed.book_manager,
        )
        self._backfill = BackfillScheduler(
            self.settings,
            self.event_bus,
            self._rest_client,
            self._checkpoint_store,
        )
        self._external_reference_provider = LatestExternalReferenceProvider(self.settings.external_reference)
        if self.settings.external_reference.external_reference_enabled:
            self._binance_reference_feed = BinanceReferenceFeed(
                self.settings,
                self.event_bus,
                self._external_reference_provider,
            )

        # Data quality checker (will use ClickHouse client after connection)
        # Initialized with None client; connected after CH writer starts
        self._quality_checker = DataQualityChecker(self.settings, client=None)

        # Phase 2: Execution infrastructure (only if exchange credentials configured)
        if self.settings.exchange.is_configured and self.settings.features.v2_trading_enabled:
            try:
                from hypeedge.api.commands import ApiCommandService, PostgresApiCommandStore
                from hypeedge.risk.checker import RiskLimits
                from hypeedge.storage.postgres import (
                    PostgresDurableOrderStore,
                    PostgresExecutionCommandQueue,
                    PostgresProjectionReader,
                    PostgresSystemStateStore,
                    create_pg_session_factory,
                    verify_postgres_schema,
                )

                self._pg_engine, session_factory = create_pg_session_factory(self.settings.postgres)
                self._pg_session_factory = session_factory
                await verify_postgres_schema(self._pg_engine)
                risk_settings = self.settings.risk
                risk_limits = RiskLimits(
                    max_position_pct=risk_settings.max_position_pct,
                    max_strategy_loss_pct=risk_settings.max_strategy_loss_pct,
                    max_drawdown_pct=risk_settings.max_drawdown_pct,
                    max_leverage=risk_settings.max_leverage,
                    timeout_ms=risk_settings.risk_check_timeout_ms,
                )
                self._durable_order_store = PostgresDurableOrderStore(
                    session_factory,
                    risk_limits=risk_limits,
                    reservation_ttl_seconds=self.settings.postgres.risk_reservation_ttl_seconds,
                )
                command_queue = PostgresExecutionCommandQueue(
                    session_factory,
                    lease_seconds=self.settings.postgres.command_lease_seconds,
                    unknown_recheck_seconds=self.settings.postgres.unknown_recheck_seconds,
                )
                self._projection_reader = PostgresProjectionReader(session_factory)
                self._system_state_store = PostgresSystemStateStore(session_factory)
                durable_state = await self._system_state_store.load()
                if durable_state is not None and durable_state.kill_switch_active:
                    self._kill_switch.restore_active(durable_state.reason)
                self._api_command_service = ApiCommandService(PostgresApiCommandStore(session_factory))
                from hypeedge.api.routes.events import SseBroker
                from hypeedge.storage.outbox import (
                    DurableControlEventWriter,
                    OutboxDispatcher,
                    PostgresOutboxStore,
                )

                self._outbox_store = PostgresOutboxStore(session_factory)
                self._api_sse_broker = SseBroker(self)
                self._outbox_dispatcher = OutboxDispatcher(self._outbox_store, self._api_sse_broker)
                self._control_event_writer = DurableControlEventWriter(self.event_bus, self._outbox_store)
                self._trading_prerequisites_ok = True
                self._init_trading_components()
                # The persistent strategy control plane serves trend-follow and
                # funding-arb even when live market making is disabled.
                self._init_market_making_components()
                if self._execution_engine is None:
                    raise RuntimeError("execution_engine_not_initialized")
                from hypeedge.execution.worker import SignedActionExecutor

                self._signed_action_executor = SignedActionExecutor(
                    command_queue,
                    self._execution_engine,
                    poll_interval_ms=self.settings.postgres.command_poll_interval_ms,
                )
            except Exception:
                self._trading_prerequisites_ok = False
                self._trading_enabled = False
                self._safety_controller.enter_cancel_only("postgres_or_schema_unavailable")
                logger.exception("trading_disabled_postgres_unavailable")
        elif self.settings.exchange.is_configured:
            self._safety_controller.enter_cancel_only("v2_feature_set_incomplete")
            logger.warning(
                "trading_disabled_v2_features_incomplete",
                legacy_execution=self.settings.features.legacy_execution,
                durable_ledger_v2=self.settings.features.durable_ledger_v2,
                execution_v2=self.settings.features.execution_v2,
                user_stream_v2=self.settings.features.user_stream_v2,
                reconciliation_v2=self.settings.features.reconciliation_v2,
                strategy_runner_v2=self.settings.features.strategy_runner_v2,
            )
        else:
            logger.info("trading_disabled_no_credentials")

    def _init_trading_components(self) -> None:
        """Initialize execution, account, and risk components.

        Design doc §9.1: trading_enabled gate — reconciliation must succeed
        before strategies can submit orders.
        """
        from datetime import UTC, datetime, timedelta

        from hypeedge.account.health import AccountFreshnessThresholds, LayeredAccountHealthProvider
        from hypeedge.account.reconciler import Reconciler
        from hypeedge.account.tracker import AccountTracker
        from hypeedge.execution.engine import ExecutionEngine
        from hypeedge.execution.nonce import NonceManager
        from hypeedge.risk.checker import RiskChecker, RiskLimits
        from hypeedge.storage.postgres import PostgresReconciliationStore

        if self._rate_limiter is None:
            raise RuntimeError("shared_rate_limiter_not_initialized")

        # Account tracker
        self._tracker = AccountTracker()
        mm_settings = self.settings.market_making
        self._account_health = LayeredAccountHealthProvider(
            AccountFreshnessThresholds(
                inventory=timedelta(seconds=5),
                clearinghouse=timedelta(seconds=max(6.0, mm_settings.account_poll_interval_seconds * 2)),
                user_stream=timedelta(seconds=5),
                reconciliation=timedelta(seconds=mm_settings.full_reconciliation_interval_seconds * 2),
            )
        )

        quota_owner = self.settings.exchange.account_address.lower()
        from hypeedge.risk.action_budget import ActionBudgetController, CancelHeadroomSnapshot

        self._action_budget_controller = ActionBudgetController(quota_owner, self.settings.action_budget)
        self._action_budget_controller.reconcile_cancel_headroom(
            CancelHeadroomSnapshot(
                cap=self.settings.action_budget.cancel_headroom_initial,
                used=0,
                observed_at=datetime.now(UTC),
            )
        )

        # Nonce manager (serial action queue)
        self._nonce_manager = NonceManager(rate_limiter=self._rate_limiter)

        # Risk checker is constructed before execution so no placement path can
        # exist without the configured fail-closed gate.
        risk_settings = self.settings.risk
        self._risk_checker = RiskChecker(
            tracker=self._tracker,
            limits=RiskLimits(
                max_position_pct=risk_settings.max_position_pct,
                max_strategy_loss_pct=risk_settings.max_strategy_loss_pct,
                max_drawdown_pct=risk_settings.max_drawdown_pct,
                max_leverage=risk_settings.max_leverage,
                timeout_ms=risk_settings.risk_check_timeout_ms,
            ),
        )

        # Execution engine
        from hypeedge.execution.normalizer import OrderNormalizer

        self._execution_engine = ExecutionEngine(
            nonce_manager=self._nonce_manager,
            event_bus=self.event_bus,
            kill_switch=self._kill_switch,
            order_state_machine=self._order_state_machine,
            account_address=self.settings.exchange.account_address,
            rate_limiter=self._rate_limiter,
            risk_checker=self._risk_checker,
            safety_controller=self._safety_controller,
            account_tracker=self._tracker,
            durable_store=self._durable_order_store,
            deferred_execution=True,
            market_data_provider=self._market_data_provider,
            market_price_stale_seconds=risk_settings.market_price_stale_seconds,
            durable_kill_trigger=self.trigger_kill_switch,
            order_normalizer=OrderNormalizer(self._instrument_cache) if self._instrument_cache is not None else None,
            instrument_cache=self._instrument_cache,
            spot_execution_enabled=self.settings.features.funding_arb_execution_enabled,
        )

        # Reconciler
        self._reconciler = Reconciler(
            event_bus=self.event_bus,
            tracker=self._tracker,
            engine=self._execution_engine,
            account_address=self.settings.exchange.account_address,
            safety_controller=self._safety_controller,
            reconciliation_store=(
                PostgresReconciliationStore(self._pg_session_factory, self.settings.exchange.account_address)
                if self._pg_session_factory is not None
                else None
            ),
            account_health=self._account_health,
            on_health_failure=self._on_account_health_failure,
        )

        # Kill Switch first imports the exchange-authoritative target set and
        # only reaches HALTED after a fresh empty exchange snapshot.
        self._kill_switch.register_cancel_all(
            self._cancel_all_authoritative_orders,
            self._exchange_open_orders_empty,
        )

        logger.info("trading_components_initialized")

    def _init_market_making_components(self) -> None:
        """Build the persistent multi-strategy control plane.

        Runtime restoration is intentionally deferred until reconciliation,
        account health, and all three action budgets are fresh.
        """
        if (
            self._pg_session_factory is None
            or self._tracker is None
            or self._account_health is None
            or self._action_budget_controller is None
        ):
            return

        from hypeedge.market_data.features import MarketFeatureEngine
        from hypeedge.storage.market_making import (
            PostgresMarketMakingReadRepository,
            PostgresMarketMakingRepository,
            PostgresStrategyAllocationManager,
            PostgresStrategyStateStore,
        )
        from hypeedge.strategy.market_maker.adapters import (
            ControllerBudgetProvider,
            DurableQuotePlanCommandAdapter,
            LiveCapabilityStrategySupervisor,
            MarketMakingRepositoryFacade,
            TrackerInventoryProvider,
        )
        from hypeedge.strategy.market_maker.policy import MarketMakerPolicy
        from hypeedge.strategy.market_maker.runtime import MarketMakerRuntimeDependencies
        from hypeedge.strategy.registry import StrategyRegistry
        from hypeedge.strategy.supervisor import StrategySupervisor
        from hypeedge.trading.quote_coordinator import QuoteCoordinator, QuoteCoordinatorConfig

        state_store = PostgresStrategyStateStore(self._pg_session_factory)
        read_repository = PostgresMarketMakingReadRepository(self._pg_session_factory)
        durable_repository = PostgresMarketMakingRepository(
            self._pg_session_factory,
            session_mode=self.settings.environment if self.settings.environment in {"testnet", "mainnet"} else "shadow",
        )
        registry = StrategyRegistry()
        commands = DurableQuotePlanCommandAdapter(
            repository=durable_repository,
            cancel_all=self._cancel_all_authoritative_orders,
            live_ready=lambda: self._quote_plan_worker is not None,
        )
        dependencies = MarketMakerRuntimeDependencies(
            event_bus=self.event_bus,
            feature_engine_factory=MarketFeatureEngine,
            policy_factory=MarketMakerPolicy,
            coordinator_factory=lambda: QuoteCoordinator(QuoteCoordinatorConfig()),
            inventory=TrackerInventoryProvider(self._tracker, self._account_health),
            budget=ControllerBudgetProvider(self._action_budget_controller),
            account_health=self._account_health,
            slots=read_repository,
            commands=commands,
            symbol_config_decoder=self._decode_market_maker_config,
            external_reference=self._external_reference_provider,
            funding=self._market_data_provider,
        )
        from hypeedge.storage.market_making import normalize_market_maker_config
        from hypeedge.strategy.funding_arb import FundingArbRuntimeDependencies, build_funding_arb_plugin
        from hypeedge.strategy.plugin import (
            MARKET_MAKER_CAPABILITIES,
            StaticStrategyTypePlugin,
        )
        from hypeedge.strategy.trend_follow_runtime import build_trend_follow_plugin

        def _mm_validate(values: Any) -> Any:
            payload = values.model_dump() if hasattr(values, "model_dump") else dict(values)
            return normalize_market_maker_config(payload)

        registry.register_plugin(
            StaticStrategyTypePlugin(
                strategy_type="market_maker",
                capabilities=MARKET_MAKER_CAPABILITIES,
                factory=dependencies.factory,
                _default_config={},
                _validate=_mm_validate,
                _decode=lambda snapshot: snapshot.values,
            )
        )
        registry.register_plugin(
            build_trend_follow_plugin(
                event_bus=self.event_bus,
                strategy_factory=self._build_trend_follow_strategy,
            )
        )
        funding_dependencies = None
        if (
            self.settings.features.funding_arb_execution_enabled
            and self._execution_engine is not None
            and self._market_data_provider is not None
            and self._instrument_cache is not None
            and self._reconciler is not None
            and self._rest_client is not None
            and self._ws_feed is not None
        ):
            from hypeedge.market_data.funding_arb_scanner import HyperliquidFundingArbMarketScanner
            from hypeedge.storage.funding_arb import PostgresFundingArbCycleStore

            funding_scanner = HyperliquidFundingArbMarketScanner(
                self._rest_client,
                self._instrument_cache,
                self._ws_feed.book_manager,
                self.settings.funding_arb,
            )
            funding_dependencies = FundingArbRuntimeDependencies(
                execution=self._execution_engine,
                scanner=funding_scanner,
                tracker=self._tracker,
                metadata=self._instrument_cache,
                cycles=PostgresFundingArbCycleStore(self._pg_session_factory),
                account_health=self._account_health,
                reconcile=self._reconcile_for_funding_arb,
                trading_ready=lambda: self.trading_enabled,
                kill_switch_active=lambda: self._kill_switch.is_active,
                deployment=self.settings.funding_arb,
                account_address=self.settings.exchange.account_address,
            )
        registry.register_plugin(build_funding_arb_plugin(dependencies=funding_dependencies))
        concrete_supervisor = StrategySupervisor(
            registry,
            state_store,
            PostgresStrategyAllocationManager(self._pg_session_factory),
        )
        supervisor = LiveCapabilityStrategySupervisor(concrete_supervisor, commands, registry=registry)
        if (
            self.settings.features.market_making_enabled
            and self._execution_engine is not None
            and self._market_data_provider is not None
        ):
            from hypeedge.execution.quote_plan_worker import AppQuoteDispatchGuardProvider, QuotePlanWorker

            guard = AppQuoteDispatchGuardProvider(
                self._pg_session_factory,
                runtime_snapshot=supervisor.runtime_snapshot,
                market_data=self._market_data_provider,
                account_health=self._account_health,
                safety=self._safety_controller,
                budget=self._action_budget_controller,
                kill_switch_active=lambda: self._kill_switch.is_active,
            )
            self._quote_plan_worker = QuotePlanWorker(
                self._pg_session_factory,
                self._execution_engine,
                guard,
                self._action_budget_controller,
            )
        self._market_making_state_store = state_store
        self._strategy_supervisor = supervisor
        self._market_making_repository = MarketMakingRepositoryFacade(
            durable_repository,
            state_store,
            runtime_snapshot=supervisor.runtime_snapshot,
            tracker=self._tracker,
            health=self._account_health,
            budget=self._action_budget_controller,
            environment=self.settings.environment,
            safety_mode=lambda: self.safety_mode,
            kill_switch_active=lambda: self._kill_switch.is_active,
        )
        logger.info(
            "strategy_control_plane_initialized",
            market_making_enabled=self.settings.features.market_making_enabled,
            funding_arb_execution_enabled=funding_dependencies is not None,
        )

    async def _reconcile_for_funding_arb(self) -> bool:
        if self._reconciler is None:
            return False
        return (await self._reconciler.reconcile()).success

    def _decode_market_maker_config(self, snapshot: Any, symbol: Any) -> Any:
        """Join durable strategy knobs with authoritative instrument metadata."""
        from decimal import Decimal

        from hypeedge.core.types import Size, Usd
        from hypeedge.strategy.market_maker.models import MarketMakerConfig

        if self._instrument_cache is None:
            raise RuntimeError("instrument metadata cache is unavailable")
        instrument = self._instrument_cache.get(symbol)
        if instrument is None:
            raise RuntimeError(f"instrument metadata is unavailable for {symbol}")
        values = snapshot.values
        return MarketMakerConfig(
            version=snapshot.revision,
            model_version="mm-v1",
            tick_size=instrument.tick_size,
            lot_size=instrument.lot_size,
            min_size=max(instrument.min_size, instrument.lot_size),
            soft_inventory_notional=Usd(values["soft_inventory_notional"]),
            hard_inventory_notional=Usd(values["hard_inventory_notional"]),
            emergency_inventory_notional=Usd(values["emergency_inventory_notional"]),
            quote_size=Size(values["quote_size"]),
            max_depth_participation=Decimal(values["max_depth_participation"]),
            external_reference_weight=Decimal(values["external_reference_weight"]),
            external_max_age_seconds=Decimal(values["external_max_age_seconds"]),
            external_outlier_bps=Decimal(values["external_outlier_bps"]),
            max_external_shift_ticks=Decimal(values["max_external_shift_ticks"]),
            max_total_fair_shift_ticks=Decimal(values["max_total_fair_shift_ticks"]),
            latency_risk_multiplier=Decimal(values["latency_risk_multiplier"]),
            conservative_latency_seconds=Decimal(values["conservative_latency_seconds"]),
            conservative_markout_bps=Decimal(values["conservative_markout_bps"]),
            min_markout_samples=int(values["min_markout_samples"]),
            inventory_skew_bps=Decimal(values["inventory_skew_bps"]),
            max_inventory_shift_bps=Decimal(values["max_inventory_shift_bps"]),
            min_half_spread_bps=Decimal(values["min_half_spread_bps"]),
            toxicity_spread_bps=Decimal(values["toxicity_spread_bps"]),
            min_expected_pnl_usdc=Usd(values["min_expected_pnl_usdc"]),
            max_quote_lifetime_seconds=Decimal(values["max_quote_age_ms"]) / Decimal(1000),
        )

    def _build_trend_follow_strategy(self, context: Any, params: Any) -> Any:
        """Construct a managed trend-follow strategy for the control-plane registry."""
        from hypeedge.strategy.trend_follow import TrendFollowStrategy

        if self._execution_engine is None:
            raise RuntimeError("execution engine is required for trend_follow strategies")
        return TrendFollowStrategy(
            strategy_id=context.instance.strategy_id,
            event_bus=self.event_bus,
            execution_client=self._execution_engine,
            params=params,
            account_tracker=self._tracker,
        )

    def _init_strategy(self) -> None:
        """Initialize the legacy single-instance trend strategy (control plane off only).

        When the multi-strategy control plane is enabled, trend instances are created via
        Postgres + StrategyRegistry instead (``docs/design.md`` §19).
        """
        from hypeedge.strategy.params import load_params
        from hypeedge.strategy.trend_follow import TrendFollowStrategy

        if self._strategy_supervisor is not None:
            logger.info("legacy_trend_init_skipped_control_plane_active")
            return

        if self._execution_engine is None:
            logger.warning("strategy_init_skipped_no_engine")
            return

        # Load strategy parameters
        import os

        param_path = os.path.join("configs", "strategy_trend.yaml")
        params = load_params(param_path)

        # Create strategy instance
        from hypeedge.core.types import StrategyId

        self._strategy = TrendFollowStrategy(
            strategy_id=StrategyId("trend_v1"),
            event_bus=self.event_bus,
            execution_client=self._execution_engine,
            params=params,
            account_tracker=self._tracker,
        )
        from hypeedge.strategy.runner import StrategyRunner

        self._strategy_runner = StrategyRunner(self._strategy, self.event_bus)

        # Set up parameter hot-reload watcher (§15.2 legacy path)
        from hypeedge.strategy.params import ParamWatcher

        self._param_watcher = ParamWatcher(
            path=param_path,
            on_change=lambda old, new: self._strategy.update_params(new) if self._strategy else None,
            check_interval=5.0,
        )

        logger.info(
            "strategy_initialized",
            strategy_id="trend_v1",
            symbol=params.symbol,
            param_path=param_path,
        )

    async def _validate_clock(self) -> bool:
        """Validate local clock and network latency against Hyperliquid.

        Design doc §16.2: "Verify local clock vs Hyperliquid API server time
        deviation; if > 1 second, alert and refuse to start."

        HL doesn't expose a server-time endpoint, so we use NTP-style
        estimation: send multiple requests, measure round-trips, and
        reject if minimum round-trip > 2s (indicating severe network
        issues that would make trading unreliable).
        """
        import time

        try:
            import httpx

            api_url = self.settings.exchange.api_url
            round_trips: list[float] = []

            async with httpx.AsyncClient(timeout=5.0) as client:
                for _ in range(3):
                    local_before = time.monotonic()
                    response = await client.post(f"{api_url}/info", json={"type": "meta"})
                    local_after = time.monotonic()

                    if response.status_code != 200:
                        logger.error("clock_validation_failed", status=response.status_code)
                        return False

                    round_trips.append(local_after - local_before)
                    await asyncio.sleep(0.1)

            min_rt = min(round_trips)
            avg_rt = sum(round_trips) / len(round_trips)

            # Minimum round-trip is the best estimate of one-way network latency
            # If min RT > 2s, network is too unreliable for trading
            if min_rt > 2.0:
                logger.critical(
                    "clock_validation_rejected_high_latency",
                    min_round_trip_ms=min_rt * 1000,
                    avg_round_trip_ms=avg_rt * 1000,
                    threshold_ms=2000,
                )
                return False

            if min_rt > 1.0:
                logger.warning(
                    "clock_validation_elevated_latency",
                    min_round_trip_ms=min_rt * 1000,
                )

            logger.info(
                "clock_validation_passed",
                min_round_trip_ms=min_rt * 1000,
                avg_round_trip_ms=avg_rt * 1000,
            )
            return True

        except Exception:
            logger.exception("clock_validation_error")
            return False

    async def _connect_hl_sdk(self) -> None:
        """Connect to Hyperliquid SDK (Exchange + Info clients).

        Design doc §15.1: agent wallet private key from env var only.
        """
        if not self.settings.exchange.is_configured:
            return

        try:
            import eth_account
            from hyperliquid.exchange import Exchange
            from hyperliquid.info import Info

            # The validated environment configuration is the only authority.
            # Never infer mainnet from a non-testnet label (e.g. ``dev``).
            base_url = self.settings.exchange.api_url.rstrip("/")

            wallet = eth_account.Account.from_key(self.settings.exchange.agent_private_key)
            info = None
            exchange = None
            last_error: Exception | None = None
            for attempt in range(3):
                try:
                    # Both SDK constructors fetch metadata synchronously. Keep
                    # them off the event loop and retry transient testnet/API
                    # gateway failures without ever switching endpoints.
                    info = await asyncio.to_thread(Info, base_url, skip_ws=False)
                    exchange = await asyncio.to_thread(
                        Exchange,
                        wallet=wallet,
                        base_url=base_url,
                        account_address=self.settings.exchange.account_address,
                    )
                    break
                except Exception as exc:
                    last_error = exc
                    if info is not None:
                        with contextlib.suppress(Exception):
                            await asyncio.to_thread(info.disconnect_websocket)
                    info = None
                    exchange = None
                    if attempt < 2:
                        logger.warning("hl_sdk_connection_retry", attempt=attempt + 1, error=str(exc))
                        await asyncio.sleep(0.25 * (2**attempt))
            if info is None or exchange is None:
                raise RuntimeError("Hyperliquid SDK construction failed after retries") from last_error

            # Wire into components
            if self._nonce_manager:
                self._nonce_manager.set_exchange(exchange)
                self._nonce_manager.set_info(info)
            if self._reconciler:
                self._reconciler.set_info_client(info)
            if self._pg_session_factory is not None:
                from hypeedge.account.exchange_ingestor import ExchangeEventIngestor
                from hypeedge.account.health import AccountStatePoller, RestAccountStateSource
                from hypeedge.execution.emergency_cancel import (
                    EmergencyCancelJournal,
                    SdkAuthoritativeOpenOrderProvider,
                    WalEmergencyCancelExecutor,
                )

                self._exchange_ingestor = ExchangeEventIngestor(
                    info,
                    self.settings.exchange.account_address,
                    self._pg_session_factory,
                    tracker=self._tracker,
                    engine=self._execution_engine,
                    event_bus=self.event_bus,
                    account_health=self._account_health,
                    on_health_failure=self._on_account_health_failure,
                )
                if self._tracker is not None and self._account_health is not None and self._rest_client is not None:
                    self._account_state_poller = AccountStatePoller(
                        RestAccountStateSource(
                            self._rest_client,
                            self.settings.exchange.account_address,
                            self._tracker,
                        ),
                        self._tracker,
                        self._account_health,
                        normal_interval_seconds=self.settings.market_making.account_poll_interval_seconds,
                        near_risk_interval_seconds=self.settings.market_making.near_risk_account_poll_interval_seconds,
                        on_health_failure=self._on_account_health_failure,
                    )
                if self._nonce_manager is not None:
                    self._emergency_cancel_executor = WalEmergencyCancelExecutor(
                        self._nonce_manager,
                        SdkAuthoritativeOpenOrderProvider(info, self.settings.exchange.account_address),
                        EmergencyCancelJournal(self.settings.market_making.emergency_cancel_wal_path),
                    )

            logger.info(
                "hl_sdk_connected",
                base_url=base_url,
                account=self.settings.exchange.account_address[:10] + "...",
            )
        except Exception:
            logger.exception("hl_sdk_connection_failed")
            await self.trigger_kill_switch("hl_sdk_connection_failed")

    async def _start_components(self) -> list[asyncio.Task[Any]]:
        """Start all components as concurrent tasks."""
        if self._ws_feed is None or self._ch_writer is None or self._metrics is None:
            raise RuntimeError("components_not_initialized")

        tasks: list[asyncio.Task[Any]] = []

        # Subscribe before components can emit Kill/reconciliation events.
        # Dispatch runs independently of browser connections so committed
        # facts are never stranded in an in-process queue.
        if self._control_event_writer is not None:
            self._control_event_writer.start()
            tasks.append(asyncio.create_task(self._control_event_writer.run(), name="control_event_writer"))
        if self._outbox_dispatcher is not None:
            tasks.append(asyncio.create_task(self._outbox_dispatcher.run(), name="outbox_dispatcher"))

        # Subscribe the shared snapshot provider before the feed can publish.
        if self._market_data_provider is not None:
            await self._market_data_provider.start()

        if self._binance_reference_feed is not None:
            tasks.append(asyncio.create_task(self._binance_reference_feed.run(), name="binance_reference_feed"))

        # Market data WebSocket feed
        tasks.append(asyncio.create_task(self._ws_feed.run(), name="ws_feed"))

        # ClickHouse batch writer
        tasks.append(asyncio.create_task(self._ch_writer.run(), name="ch_writer"))

        # Prometheus metrics server
        tasks.append(asyncio.create_task(self._metrics.serve(), name="metrics"))

        # In trading mode the API starts only after durable recovery/worker
        # setup, so no placement can race the signed-action executor startup.
        if not self.settings.exchange.is_configured:
            api_task = self._start_api_server()
            if api_task:
                tasks.append(api_task)

        # REST backfill scheduler
        if self._backfill is not None:
            tasks.append(asyncio.create_task(self._backfill.run(), name="backfill"))

        # Instrument metadata cache
        if self._instrument_cache is not None:
            tasks.append(asyncio.create_task(self._instrument_cache.run(), name="instrument_cache"))

        # Data quality checker (runs after CH writer has connected)
        if self._quality_checker is not None:
            tasks.append(asyncio.create_task(self._run_quality_checker(), name="quality_checker"))

        # Phase 2: Connect HL SDK and start trading components
        if self.settings.exchange.is_configured and self._trading_prerequisites_ok:
            await self._connect_hl_sdk()

            exchange_history_ok = self._exchange_ingestor is not None
            if self._exchange_ingestor is not None:
                try:
                    await self._exchange_ingestor.recover_history()
                    tasks.append(asyncio.create_task(self._exchange_ingestor.run(), name="exchange_event_ingestor"))
                except Exception:
                    exchange_history_ok = False
                    self._safety_controller.enter_cancel_only("exchange_history_recovery_failed")
                    logger.exception("exchange_history_recovery_failed")

            if self._nonce_manager:
                tasks.append(asyncio.create_task(self._nonce_manager.run(), name="nonce_manager"))

            if self._emergency_cancel_executor is not None:
                recovery = await self._emergency_cancel_executor.recover_pending()
                if not recovery.success:
                    self._safety_controller.enter_cancel_only("emergency_cancel_recovery_unresolved")
                    logger.error("emergency_cancel_recovery_unresolved", count=len(recovery.unresolved))

            if self._execution_engine:
                await self._execution_engine.recover_open_orders()

            # A mainnet restart must preserve a previously durable Kill latch.
            # Re-run authoritative cancellation, remain HALTED, and require the
            # explicit recovery endpoint before any queued placement is sent.
            if self._kill_switch.is_active:
                self._kill_switch.trigger(self._kill_switch.reason or "restored_durable_kill_switch")
                await self._persist_system_state(
                    "halting",
                    self._kill_switch.reason,
                    kill_switch_active=True,
                )
                if await self._kill_switch.wait_until_halted():
                    await self._persist_system_state(
                        "halted",
                        self._kill_switch.reason,
                        kill_switch_active=True,
                    )
                self._trading_enabled = False
                logger.warning("trading_stay_halted_durable_kill_latch")
                api_task = self._start_api_server()
                if api_task:
                    tasks.append(api_task)
                return tasks

            # Startup reconciliation gate (design doc implementation_plan §3)
            if self._reconciler and self._tracker:
                from hypeedge.core.enums import SafetyMode

                self._safety_controller.transition(SafetyMode.RECONCILING, "startup_reconciliation")
                if not await self._persist_system_state(
                    "reconciling", "startup_reconciliation", kill_switch_active=False
                ):
                    self._safety_controller.enter_cancel_only("system_state_persistence_failed")
                    api_task = self._start_api_server()
                    if api_task:
                        tasks.append(api_task)
                    return tasks
                result = await self._reconciler.reconcile()
                credits_ok = await self._refresh_action_budget()
                if result.success and credits_ok and exchange_history_ok:
                    self._safety_controller.transition(SafetyMode.NORMAL, "startup_reconciliation_passed")
                    if not await self._persist_system_state(
                        "normal", "startup_reconciliation_passed", kill_switch_active=False
                    ):
                        self._safety_controller.enter_cancel_only("system_state_persistence_failed")
                        self._trading_enabled = False
                        api_task = self._start_api_server()
                        if api_task:
                            tasks.append(api_task)
                        return tasks
                    self._trading_enabled = True
                    if self._metrics:
                        self._metrics.set_trading_enabled(True)
                    logger.info("trading_enabled", reconciliation="passed")

                    if self._signed_action_executor is None:
                        raise RuntimeError("durable_signed_action_executor_not_initialized")
                    tasks.append(asyncio.create_task(self._signed_action_executor.run(), name="signed_action_executor"))
                    if self._quote_plan_worker is not None:
                        tasks.append(asyncio.create_task(self._quote_plan_worker.run(), name="quote_plan_worker"))

                    # Legacy single-instance trend only when multi-strategy control plane is off.
                    if self._strategy_supervisor is None:
                        self._init_strategy()
                        if self._strategy_runner:
                            self._strategy_task = asyncio.create_task(
                                self._strategy_runner.run(), name="strategy_runner"
                            )
                            tasks.append(self._strategy_task)
                        if self._param_watcher:
                            tasks.append(asyncio.create_task(self._param_watcher.run(), name="param_watcher"))
                    if self._strategy_supervisor is not None:
                        # This is a one-shot setup task: it polls until
                        # prerequisites are fresh, restores durable instances,
                        # then RETURNS. It must not live in ``self._tasks`` (a
                        # return there triggers FIRST_COMPLETED shutdown) — it
                        # goes in ``self._background_tasks`` instead.
                        self._background_tasks.append(
                            asyncio.create_task(
                                self._restore_market_making_when_ready(),
                                name="market_making_restore",
                            )
                        )
                else:
                    if not exchange_history_ok:
                        reason = "exchange_history_recovery_failed"
                    else:
                        reason = "startup_reconciliation_failed" if not result.success else "action_credits_unavailable"
                    await self._enter_automatic_safety_degradation(reason)
                    logger.warning("trading_stay_disabled", reconciliation="failed", errors=result.errors)

                # Account polling and periodic reconciliation start only after
                # the startup snapshot completes, preventing duplicate SDK
                # account calls from racing the startup gate.
                if self._account_state_poller is not None:
                    tasks.append(asyncio.create_task(self._account_state_poller.run(), name="account_state_poller"))
                if self._rest_client is not None:
                    tasks.append(asyncio.create_task(self._poll_action_credits(), name="action_credits_poll"))
                tasks.append(asyncio.create_task(self._monitor_trading_health(), name="trading_health_monitor"))
                tasks.append(
                    asyncio.create_task(
                        self._reconciler.run_periodic(
                            int(self.settings.market_making.full_reconciliation_interval_seconds)
                        ),
                        name="reconciler",
                    )
                )

        if self.settings.exchange.is_configured:
            api_task = self._start_api_server()
            if api_task:
                tasks.append(api_task)

        logger.info("components_started", tasks=[t.get_name() for t in tasks])
        return tasks

    async def _run_quality_checker(self) -> None:
        """Wait for ClickHouse readiness, then run quality checks on a dedicated client.

        clickhouse-connect clients are not safe for concurrent use across threads, so the
        quality checker must not share the writer's client (which inserts in an executor).
        """
        if self._quality_checker is None or self._ch_writer is None:
            return
        while self._ch_writer._client is None:
            await asyncio.sleep(0.1)

        import clickhouse_connect

        ch = self.settings.clickhouse
        quality_client = await asyncio.to_thread(
            clickhouse_connect.get_client,
            host=ch.host,
            port=ch.port,
            username=ch.username,
            password=ch.password,
            database=ch.database,
        )
        self._quality_checker._client = quality_client
        try:
            await self._quality_checker.run()
        finally:
            await asyncio.to_thread(quality_client.close)
            self._quality_checker._client = None

    async def _poll_action_credits(self) -> None:
        """Keep the shared address quota fresh for every REST/execution consumer."""
        while True:
            if not await self._refresh_action_budget():
                await self._enter_automatic_safety_degradation("action_credits_unavailable")
            interval = (
                self._action_budget_controller.next_remote_poll_interval_seconds
                if self._action_budget_controller is not None
                else 60.0
            )
            await asyncio.sleep(interval)

    async def _refresh_action_budget(self) -> bool:
        if self._rest_client is None:
            return False
        from datetime import UTC, datetime

        payload = await self._rest_client.poll_action_credit_snapshot(self.settings.exchange.account_address)
        if payload is None:
            return False
        if self._action_budget_controller is not None:
            from hypeedge.risk.action_budget import RemoteActionSnapshot

            try:
                snapshot = RemoteActionSnapshot.from_user_rate_limit(
                    self.settings.exchange.account_address,
                    payload,
                    observed_at=datetime.now(UTC),
                )
                self._action_budget_controller.reconcile_remote(snapshot)
            except Exception:
                logger.exception("action_budget_remote_reconciliation_failed")
                return False
        return True

    async def _restore_market_making_when_ready(self) -> None:
        """Restore durable MM instances only after every placement prerequisite is fresh."""
        while not self.is_shutting_down and not self._kill_switch.is_active:
            health = self._account_health.get_account_health() if self._account_health is not None else None
            budget = self._action_budget_controller.snapshot() if self._action_budget_controller is not None else None
            if (
                self.trading_enabled
                and health is not None
                and health.allows_risk_increase
                and budget is not None
                and budget.remote_fresh
                and budget.cancel_headroom_fresh
            ):
                break
            await asyncio.sleep(0.25)
        else:
            return
        await self._restore_market_making_in_shadow()

    async def _restore_market_making_in_shadow(self) -> None:
        """Restore durable strategy instances after placement prerequisites are fresh.

        Market makers restart through SHADOW and keep RUNNING as desired intent only.
        Non-market-making types start directly into RUNNING when that was desired.
        """
        if self._strategy_supervisor is None or self._market_making_state_store is None:
            return
        from hypeedge.core.enums import MarketMakerLifecycle

        for instance in await self._market_making_state_store.list_instances():
            desired = instance.desired_state
            if desired in {MarketMakerLifecycle.STOPPED, MarketMakerLifecycle.FAULTED}:
                continue
            try:
                if instance.strategy_type != "market_maker":
                    start_target = (
                        MarketMakerLifecycle.RUNNING
                        if desired in {MarketMakerLifecycle.RUNNING, MarketMakerLifecycle.PAUSED}
                        else MarketMakerLifecycle.RUNNING
                    )
                    await self._strategy_supervisor.start(instance.strategy_id, target=start_target)
                    if desired == MarketMakerLifecycle.PAUSED:
                        await self._strategy_supervisor.pause(instance.strategy_id)
                    continue

                await self._strategy_supervisor.start(instance.strategy_id, target=MarketMakerLifecycle.SHADOW)
                if desired == MarketMakerLifecycle.RUNNING:
                    # Preserve operator intent but require an explicit resume
                    # after restart before any live transition is attempted.
                    await self._market_making_state_store.set_desired(
                        instance.strategy_id,
                        state=MarketMakerLifecycle.RUNNING,
                    )
                elif desired == MarketMakerLifecycle.PAUSED:
                    await self._strategy_supervisor.pause(instance.strategy_id)
                elif desired == MarketMakerLifecycle.DRAINING:
                    await self._strategy_supervisor.drain(instance.strategy_id)
            except Exception:
                logger.exception("market_making_restore_failed", strategy_id=str(instance.strategy_id))

    async def _cancel_all_authoritative_orders(self) -> int:
        if self._emergency_cancel_executor is not None:
            result = await self._emergency_cancel_executor.cancel_all()
            if not result.success:
                raise RuntimeError(f"emergency_cancel_unresolved:{len(result.unresolved)}")
            return result.cancelled
        if self._reconciler is None or self._execution_engine is None:
            raise RuntimeError("authoritative_cancel_components_unavailable")
        await self._reconciler.refresh_exchange_open_orders_for_cancel()
        return await self._execution_engine.cancel_all_orders()

    async def _exchange_open_orders_empty(self) -> bool:
        if self._emergency_cancel_executor is not None:
            result = await self._emergency_cancel_executor.cancel_all()
            return result.success and result.requested == 0
        if self._reconciler is None:
            raise RuntimeError("authoritative_cancel_reconciler_unavailable")
        return await self._reconciler.authoritative_open_orders_empty()

    def _start_api_server(self) -> asyncio.Task[Any] | None:
        """Start the FastAPI HTTP API server."""
        try:
            from hypeedge.api.app import create_api

            api_app = create_api(self, cors_origins=self.settings.api.cors_origins)

            import uvicorn

            config = uvicorn.Config(
                api_app,
                host=self.settings.api.host,
                port=self.settings.api.port,
                log_level="warning",
            )
            server = uvicorn.Server(config)
            self._api_server = server
            logger.info("api_server_starting", port=self.settings.api.port)
            return asyncio.create_task(server.serve(), name="api_server")
        except ImportError:
            logger.warning("api_server_skipped_uvicorn_not_installed")
            return None
        except Exception:
            logger.exception("api_server_start_failed")
            return None

    def _request_shutdown(self, sig: signal.Signals) -> None:
        """Signal handler for graceful shutdown."""
        logger.info("shutdown_signal_received", signal=sig.name)
        self._shutdown_event.set()

    async def _watch_kill_switch(self, queue: asyncio.Queue[Event]) -> None:
        """Watch kill switch events while keeping control/cancel paths alive."""
        while True:
            event: Event = await queue.get()
            logger.critical("kill_switch_triggered", reason=event.payload)
            already_durable = self._kill_switch_active
            self._kill_switch_active = True
            reason = self._kill_switch.reason or "kill_switch_triggered"
            if not already_durable:
                await self._persist_system_state("halting", reason, kill_switch_active=True)
            for task in self._tasks:
                if task.get_name() == "strategy_runner":
                    task.cancel()
            await self._suspend_all_strategies_for_safety(f"kill_switch:{reason}")
            if await self._kill_switch.wait_until_halted():
                await self._persist_system_state("halted", reason, kill_switch_active=True)

    async def _persist_system_state(self, state: str, reason: str | None, *, kill_switch_active: bool) -> bool:
        store = self._system_state_store
        if store is None:
            return not self.settings.exchange.is_configured
        try:
            await store.transition(state, reason, kill_switch_active=kill_switch_active)
            return True
        except Exception:
            self._trading_enabled = False
            logger.exception("durable_system_state_write_failed", state=state)
            return False

    async def _graceful_shutdown(self) -> None:
        """Ordered shutdown sequence (design doc §16.4).

        1. Stop market data (no new data)
        2. Cancel all open orders (if trading was active)
        3. Flush ClickHouse writer
        4. Cancel all tasks
        5. Close connections
        """
        logger.info("graceful_shutdown_starting")
        await self._persist_system_state(
            "stopping",
            "graceful_shutdown",
            kill_switch_active=self._kill_switch.is_active,
        )
        total_timeout = 30.0

        try:
            async with asyncio.timeout(total_timeout):
                # Step 1: Stop market data (no new data coming in)
                if self._ws_feed:
                    await self._ws_feed.stop()
                if self._market_data_provider:
                    await self._market_data_provider.stop()
                if self._binance_reference_feed:
                    await self._binance_reference_feed.stop()

                # Step 2: Cancel all open orders (design doc §16.4)
                if self._trading_enabled and self._execution_engine:
                    try:
                        cancelled = await self._execution_engine.cancel_all_orders()
                        logger.info("shutdown_orders_cancelled", count=cancelled)
                    except Exception:
                        logger.exception("shutdown_cancel_orders_failed")

                # Step 3: Stop strategy (closes positions)
                await self._stop_all_market_making()
                if self._strategy_runner:
                    await self._strategy_runner.stop()
                if self._param_watcher:
                    await self._param_watcher.stop()

                # Step 4: Stop durable dispatch before the nonce signing queue.
                if self._signed_action_executor:
                    await self._signed_action_executor.stop()
                if self._quote_plan_worker:
                    await self._quote_plan_worker.stop()
                if self._nonce_manager:
                    await self._nonce_manager.stop()
                if self._reconciler:
                    await self._reconciler.stop()
                if self._account_state_poller:
                    await self._account_state_poller.stop()
                if self._exchange_ingestor:
                    await self._exchange_ingestor.stop()
                if self._control_event_writer:
                    await self._control_event_writer.stop()
                if self._outbox_dispatcher:
                    await self._outbox_dispatcher.stop()
                if self._api_sse_broker:
                    await self._api_sse_broker.stop()

                # Step 4: Flush ClickHouse writer
                if self._ch_writer:
                    await self._ch_writer.flush()

                # Step 5: Cancel all tasks. The uvicorn api_server is exempt:
                # serve() has no try/finally, so cancelling it skips
                # Server.shutdown() and never sends the ASGI lifespan.shutdown
                # event, leaving starlette's lifespan parked on receive() and
                # surfacing a noisy asyncio.CancelledError traceback during loop
                # teardown. Instead we set should_exit so uvicorn unwinds via its
                # main_loop and runs the full shutdown path (incl. lifespan).
                if self._api_server is not None:
                    self._api_server.should_exit = True
                for task in self._tasks:
                    if task.get_name() == "api_server":
                        continue
                    task.cancel()

                # Step 6: Wait for tasks to finish (api_server exits cleanly;
                # the rest unwind via cancellation above).
                if self._tasks:
                    await asyncio.gather(*self._tasks, return_exceptions=True)

                # One-shot background tasks are cancelled too, but they never
                # kept the process alive.
                for task in self._background_tasks:
                    if not task.done():
                        task.cancel()
                if self._background_tasks:
                    await asyncio.gather(*self._background_tasks, return_exceptions=True)

                # Step 7: Close connections
                if self._ch_writer:
                    await self._ch_writer.close()
                if self._rest_client:
                    await self._rest_client.close()
                if self._nonce_manager is not None and self._nonce_manager.info is not None:
                    disconnect_websocket = getattr(self._nonce_manager.info, "disconnect_websocket", None)
                    if callable(disconnect_websocket):
                        with contextlib.suppress(Exception):
                            await asyncio.to_thread(disconnect_websocket)
                if self._pg_engine:
                    await self._pg_engine.dispose()

                logger.info("graceful_shutdown_complete")

        except TimeoutError:
            logger.critical("graceful_shutdown_timeout", timeout=total_timeout)

    @property
    def is_shutting_down(self) -> bool:
        return self._shutdown_event.is_set()

    @property
    def trading_enabled(self) -> bool:
        """Whether trading is active (reconciliation passed, kill switch not triggered)."""
        return self._trading_enabled and not self._kill_switch_active

    @property
    def safety_mode(self) -> str:
        """Current lifecycle mode exposed to the API as a read-only value."""
        return self._safety_controller.mode.value

    @property
    def action_credits_remaining(self) -> int | None:
        """Latest known exchange action credits, if the limiter is initialized."""
        return self._rate_limiter.action_credits_remaining if self._rate_limiter else None

    @property
    def api_command_service(self) -> ApiCommandService | None:
        """Durable API mutation service; absent when Postgres trading state is unavailable."""
        return self._api_command_service

    @property
    def projection_reader(self) -> PostgresProjectionReader | None:
        """Durable API read model; absent in monitor-only mode."""
        return self._projection_reader

    @property
    def outbox_store(self) -> PostgresOutboxStore | None:
        """Durable event stream used by dispatcher and SSE replay."""

        return self._outbox_store

    @property
    def account_health(self) -> LayeredAccountHealthProvider | None:
        return self._account_health

    @property
    def action_budget_controller(self) -> ActionBudgetController | None:
        return self._action_budget_controller

    @property
    def emergency_cancel_executor(self) -> EmergencyCancelExecutor | None:
        return self._emergency_cancel_executor

    @property
    def strategy_supervisor(self) -> Any | None:
        return self._strategy_supervisor

    @property
    def market_making_repository(self) -> Any | None:
        return self._market_making_repository

    def market_making_runtime_snapshot(self, strategy_id: StrategyId) -> Any | None:
        if self._strategy_supervisor is None:
            return None
        return self._strategy_supervisor.runtime_snapshot(strategy_id)

    async def _on_account_health_failure(self, reason: str) -> None:
        await self._enter_automatic_safety_degradation(reason)

    async def _enter_automatic_safety_degradation(self, reason: str) -> None:
        """Persist a recoverable CANCEL_ONLY state and suspend runtimes without changing desired state."""
        normalized = reason.strip() or "unspecified_health_failure"
        async with self._automatic_safety_lock:
            from hypeedge.core.enums import SafetyMode

            if self._kill_switch.is_active or self._safety_controller.mode in {
                SafetyMode.HALTING,
                SafetyMode.HALTED,
            }:
                self._trading_enabled = False
                if self._metrics is not None:
                    self._metrics.set_trading_enabled(False)
                return
            durable_reason = f"{_AUTOMATIC_SAFETY_REASON_PREFIX}{normalized}"
            already_degraded = (
                self._safety_controller.mode == SafetyMode.CANCEL_ONLY
                and self._safety_controller.reason == durable_reason
            )
            self._trading_enabled = False
            if self._metrics is not None:
                self._metrics.set_trading_enabled(False)
            if already_degraded:
                return
            self._safety_controller.enter_cancel_only(durable_reason)
            persisted = await self._persist_system_state(
                "cancel_only",
                durable_reason,
                kill_switch_active=False,
            )
            await self._suspend_all_strategies_for_safety(normalized)
            if not persisted:
                self._safety_controller.enter_cancel_only("system_state_persistence_failed")

    async def _monitor_trading_health(self) -> None:
        """Continuously enforce freshness and attempt only fully gated automatic recovery."""
        while not self.is_shutting_down:
            retry_delay = _HEALTH_MONITOR_INTERVAL_SECONDS
            try:
                from hypeedge.core.enums import SafetyMode

                if not self._kill_switch.is_active:
                    ready, reason = self._placement_prerequisites_fresh(require_reconciliation=True)
                    if self._safety_controller.mode == SafetyMode.NORMAL and not ready:
                        await self._enter_automatic_safety_degradation(reason)
                    elif self._is_automatic_safety_degradation() and not await self._try_recover_automatic_safety():
                        retry_delay = _RECOVERY_RETRY_INTERVAL_SECONDS
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("trading_health_monitor_failed")
                await self._enter_automatic_safety_degradation("trading_health_monitor_failed")
                retry_delay = _RECOVERY_RETRY_INTERVAL_SECONDS
            await asyncio.sleep(retry_delay)

    def _placement_prerequisites_fresh(self, *, require_reconciliation: bool) -> tuple[bool, str]:
        if self._account_health is None:
            return False, "account_health_unavailable"
        health = self._account_health.get_account_health()
        dimensions = [health.inventory, health.clearinghouse, health.user_stream]
        if require_reconciliation:
            dimensions.append(health.reconciliation)
        blocked = [
            f"{item.dimension.value}:{item.reason or item.status.value}" for item in dimensions if not item.is_fresh
        ]
        if blocked:
            return False, "account_health_unavailable:" + ",".join(blocked)
        if self._action_budget_controller is None:
            return False, "action_budget_unavailable"
        budget = self._action_budget_controller.snapshot()
        if not budget.remote_fresh or not budget.cancel_headroom_fresh:
            return False, "action_budget_stale"
        mode = getattr(budget.mode, "value", budget.mode)
        if mode not in {"normal", "conserve"}:
            return False, f"action_budget_{mode}"
        return True, "ready"

    def _is_automatic_safety_degradation(self) -> bool:
        from hypeedge.core.enums import SafetyMode

        reason = self._safety_controller.reason
        return (
            self._safety_controller.mode == SafetyMode.CANCEL_ONLY
            and reason is not None
            and reason.startswith(_AUTOMATIC_SAFETY_REASON_PREFIX)
        )

    async def _try_recover_automatic_safety(self) -> bool:
        """Recover transient health degradation only after history, budget, and reconciliation gates pass."""
        async with self._automatic_safety_lock:
            from hypeedge.core.enums import SafetyMode

            if (
                not self._is_automatic_safety_degradation()
                or self._kill_switch.is_active
                or self.is_shutting_down
                or not self._trading_prerequisites_ok
                or self._exchange_ingestor is None
                or self._reconciler is None
            ):
                return False
            ready, _ = self._placement_prerequisites_fresh(require_reconciliation=False)
            if not ready:
                return False

            recovery_reason = self._safety_controller.reason or (f"{_AUTOMATIC_SAFETY_REASON_PREFIX}unknown")
            self._safety_controller.transition(SafetyMode.RECOVERING, "automatic_safety_recovery")
            if not await self._persist_system_state(
                "recovering",
                "automatic_safety_recovery",
                kill_switch_active=False,
            ):
                self._safety_controller.enter_cancel_only("system_state_persistence_failed")
                return False

            try:
                await self._exchange_ingestor.recover_history()
            except Exception:
                logger.exception("automatic_recovery_exchange_history_failed")
                await self._return_to_automatic_cancel_only("exchange_history_recovery_failed")
                return False

            ready, reason = self._placement_prerequisites_fresh(require_reconciliation=False)
            if not ready:
                await self._return_to_automatic_cancel_only(reason)
                return False

            self._safety_controller.transition(SafetyMode.RECONCILING, "automatic_safety_reconciliation")
            if not await self._persist_system_state(
                "reconciling",
                "automatic_safety_reconciliation",
                kill_switch_active=False,
            ):
                self._safety_controller.enter_cancel_only("system_state_persistence_failed")
                return False

            result = await self._reconciler.reconcile()
            credits_ok = await self._refresh_action_budget()
            ready, reason = self._placement_prerequisites_fresh(require_reconciliation=True)
            if not result.success or not credits_ok or not ready:
                if not result.success:
                    reason = "automatic_reconciliation_failed"
                elif not credits_ok:
                    reason = "action_credits_unavailable"
                await self._return_to_automatic_cancel_only(reason)
                logger.warning(
                    "automatic_safety_recovery_blocked",
                    original_reason=recovery_reason,
                    reason=reason,
                    reconciliation_errors=result.errors,
                )
                return False

            if not await self._persist_system_state(
                "normal",
                "automatic_safety_recovery_passed",
                kill_switch_active=False,
            ):
                self._safety_controller.enter_cancel_only("system_state_persistence_failed")
                return False
            self._safety_controller.transition(SafetyMode.NORMAL, "automatic_safety_recovery_passed")
            self._trading_enabled = True
            self._ensure_trading_workers_running()
            if self._metrics is not None:
                self._metrics.set_trading_enabled(True)
            await self._resume_system_suspended_strategies()
            logger.info("automatic_safety_recovery_completed", original_reason=recovery_reason)
            return True

    async def _return_to_automatic_cancel_only(self, reason: str) -> None:
        durable_reason = f"{_AUTOMATIC_SAFETY_REASON_PREFIX}{reason}"
        self._trading_enabled = False
        self._safety_controller.enter_cancel_only(durable_reason)
        if self._metrics is not None:
            self._metrics.set_trading_enabled(False)
        if not await self._persist_system_state(
            "cancel_only",
            durable_reason,
            kill_switch_active=False,
        ):
            self._safety_controller.enter_cancel_only("system_state_persistence_failed")

    def _ensure_trading_workers_running(self) -> None:
        active_names = {task.get_name() for task in self._tasks if not task.done()}
        if self._signed_action_executor is not None and "signed_action_executor" not in active_names:
            self._tasks.append(asyncio.create_task(self._signed_action_executor.run(), name="signed_action_executor"))
        if self._quote_plan_worker is not None and "quote_plan_worker" not in active_names:
            self._tasks.append(asyncio.create_task(self._quote_plan_worker.run(), name="quote_plan_worker"))

    async def _suspend_all_strategies_for_safety(self, reason: str) -> None:
        if self._strategy_supervisor is None or self._market_making_state_store is None:
            return
        from hypeedge.core.enums import MarketMakerLifecycle

        for instance in await self._market_making_state_store.list_instances():
            runtime = await self._market_making_state_store.get_runtime(instance.strategy_id)
            if runtime.actual_state in {
                MarketMakerLifecycle.WARMING,
                MarketMakerLifecycle.SHADOW,
                MarketMakerLifecycle.RUNNING,
                MarketMakerLifecycle.DRAINING,
            }:
                try:
                    await self._strategy_supervisor.suspend_for_safety(instance.strategy_id, reason)
                except Exception:
                    logger.exception("strategy_safety_suspend_failed", strategy_id=str(instance.strategy_id))

    async def _resume_system_suspended_strategies(self) -> None:
        if self._strategy_supervisor is None or self._market_making_state_store is None:
            return
        from hypeedge.core.enums import MarketMakerLifecycle
        from hypeedge.strategy.supervisor import is_system_safety_pause_reason

        for instance in await self._market_making_state_store.list_instances():
            if instance.desired_state not in {MarketMakerLifecycle.SHADOW, MarketMakerLifecycle.RUNNING}:
                continue
            runtime = await self._market_making_state_store.get_runtime(instance.strategy_id)
            if runtime.actual_state != MarketMakerLifecycle.PAUSED or not is_system_safety_pause_reason(runtime.reason):
                continue
            try:
                await self._strategy_supervisor.resume_from_safety(instance.strategy_id)
            except Exception:
                logger.exception("strategy_safety_resume_failed", strategy_id=str(instance.strategy_id))

    async def _stop_all_market_making(self) -> None:
        if self._strategy_supervisor is None or self._market_making_state_store is None:
            return
        from hypeedge.core.enums import MarketMakerLifecycle

        for instance in await self._market_making_state_store.list_instances():
            runtime = await self._market_making_state_store.get_runtime(instance.strategy_id)
            if runtime.actual_state != MarketMakerLifecycle.STOPPED:
                with contextlib.suppress(Exception):
                    await self._strategy_supervisor.stop(instance.strategy_id)

    async def trigger_kill_switch(self, reason: str) -> bool:
        """Durably latch HALTING before starting the in-process cancel workflow."""
        if not await self._persist_system_state("halting", reason, kill_switch_active=True):
            self._trading_enabled = False
            return False
        self._kill_switch_active = True
        self._trading_enabled = False
        self._kill_switch.trigger(reason)
        if self._metrics:
            self._metrics.set_trading_enabled(False)
        return True

    async def recover_from_kill_switch(self) -> bool:
        """Reconcile exchange truth before allowing a halted process to trade again."""
        from hypeedge.core.enums import SafetyMode

        if not self._kill_switch.is_active:
            return False
        if not await self._kill_switch.wait_until_halted():
            self._trading_enabled = False
            logger.error("kill_switch_recovery_blocked_halt_incomplete")
            return False
        if not self._trading_prerequisites_ok or self._reconciler is None or self._rest_client is None:
            self._safety_controller.transition(SafetyMode.HALTED, "recovery_prerequisites_unavailable")
            await self._persist_system_state("halted", "recovery_prerequisites_unavailable", kill_switch_active=True)
            self._trading_enabled = False
            return False

        self._safety_controller.transition(SafetyMode.RECOVERING, "kill_switch_recovery_requested")
        if not await self._persist_system_state(
            "recovering", "kill_switch_recovery_requested", kill_switch_active=True
        ):
            self._safety_controller.transition(SafetyMode.HALTED, "system_state_persistence_failed")
            return False
        if self._exchange_ingestor is None:
            self._safety_controller.transition(SafetyMode.HALTED, "recovery_exchange_ingestor_unavailable")
            await self._persist_system_state(
                "halted", "recovery_exchange_ingestor_unavailable", kill_switch_active=True
            )
            return False
        try:
            await self._exchange_ingestor.recover_history()
        except Exception:
            logger.exception("recovery_exchange_history_failed")
            self._safety_controller.transition(SafetyMode.HALTED, "recovery_exchange_history_failed")
            await self._persist_system_state("halted", "recovery_exchange_history_failed", kill_switch_active=True)
            return False
        self._safety_controller.transition(SafetyMode.RECONCILING, "kill_switch_recovery_reconciliation")
        if not await self._persist_system_state(
            "reconciling", "kill_switch_recovery_reconciliation", kill_switch_active=True
        ):
            self._safety_controller.transition(SafetyMode.HALTED, "system_state_persistence_failed")
            return False
        result = await self._reconciler.reconcile()
        credits = await self._rest_client.poll_action_credits(self.settings.exchange.account_address)
        credits_ok = (
            self._rate_limiter is not None
            and self._rate_limiter.action_credits_are_fresh()
            and credits >= self._rate_limiter.action_credits_low_watermark
        )
        if not result.success or not credits_ok:
            self._trading_enabled = False
            self._safety_controller.transition(
                SafetyMode.HALTED,
                "recovery_reconciliation_failed" if not result.success else "recovery_action_credits_not_safe",
            )
            await self._persist_system_state(
                "halted",
                "recovery_reconciliation_failed" if not result.success else "recovery_action_credits_not_safe",
                kill_switch_active=True,
            )
            return False

        if not await self._persist_system_state("normal", "kill_switch_recovery_passed", kill_switch_active=False):
            self._safety_controller.transition(SafetyMode.HALTED, "system_state_persistence_failed")
            return False
        self._kill_switch.reset(recovery_confirmed=True)
        self._kill_switch_active = False
        self._safety_controller.transition(SafetyMode.NORMAL, "kill_switch_recovery_passed")
        self._trading_enabled = True
        if self._signed_action_executor is not None and not any(
            task.get_name() == "signed_action_executor" and not task.done() for task in self._tasks
        ):
            self._tasks.append(asyncio.create_task(self._signed_action_executor.run(), name="signed_action_executor"))
        if self._metrics:
            self._metrics.set_trading_enabled(True)
        await self._resume_system_suspended_strategies()
        return True

    async def start_strategy(self) -> bool:
        """Start the strategy's event-consumer task when trading is permitted."""
        if not self.trading_enabled or self.is_shutting_down:
            return False
        if self._strategy is None or self._strategy_runner is None:
            return False
        if self._strategy_task is not None and not self._strategy_task.done():
            return False

        task = asyncio.create_task(self._strategy_runner.run(), name="strategy_runner")
        self._strategy_task = task
        self._tasks.append(task)
        logger.info("strategy_task_started", strategy_id=str(self._strategy.strategy_id))
        return True

    async def stop_strategy(self) -> bool:
        """Stop and await the strategy task; cancellation remains available in every safety mode."""
        task = self._strategy_task
        if task is None or task.done() or self._strategy_runner is None:
            return False

        await self._strategy_runner.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        logger.info("strategy_task_stopped", strategy_id=str(self._strategy.strategy_id) if self._strategy else None)
        return True

    @property
    def execution_engine(self) -> ExecutionEngine | None:
        return self._execution_engine

    @property
    def account_tracker(self) -> AccountTracker | None:
        return self._tracker

    @property
    def risk_checker(self) -> RiskChecker | None:
        return self._risk_checker

    @property
    def kill_switch(self) -> KillSwitch:
        return self._kill_switch

    @property
    def strategy(self) -> TrendFollowStrategy | None:
        return self._strategy
