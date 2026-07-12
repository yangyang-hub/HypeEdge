"""Safety gates and fixtures for live Hyperliquid testnet integration tests."""

from __future__ import annotations

import asyncio
import contextlib
import math
import os
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import pytest

from hypeedge.app import HypeEdgeApp
from hypeedge.config.settings import AppSettings, ExchangeSettings, FeatureFlagsSettings, PostgresSettings, RiskSettings
from hypeedge.core.enums import OrderStatus, OrderType, Side, TimeInForce
from hypeedge.core.events import EVENT_KILL_SWITCH_TRIGGERED
from hypeedge.core.models import Order, OrderIntent
from hypeedge.core.types import Cloid, Price, Size, StrategyId, Symbol
from hypeedge.execution.engine import ExecutionEngine
from hypeedge.execution.nonce import NonceManager

_RUN_ACK = "I_UNDERSTAND_THIS_PLACES_TESTNET_ORDERS"
_ACCOUNT_ACK = "I_CONFIRM_THIS_IS_A_DEDICATED_TESTNET_ACCOUNT"
_TESTNET_HOST = "api.hyperliquid-testnet.xyz"
_MAX_NOTIONAL_HARD_CAP_USD = 25.0
_MIN_CONFIGURED_NOTIONAL_USD = 12.5
_MIN_ACTION_CREDITS = 100
_ACTION_CREDIT_WATERMARK = 50
_TERMINAL_EXCHANGE_STATUSES = {"canceled", "cancelled", "filled", "rejected", "expired", "margincanceled"}


@dataclass(frozen=True)
class TestnetConfig:
    """Explicit, validated inputs for live testnet mutations."""

    api_url: str
    postgres_url: str
    account_address: str
    agent_private_key: str
    symbol: str
    max_notional_usd: float
    resting_offset: float


@dataclass
class TestnetHarness:
    """A production HypeEdgeApp V2 trading stack connected to testnet."""

    config: TestnetConfig
    app: HypeEdgeApp
    info: Any
    exchange: Any
    nonce: NonceManager
    engine: ExecutionEngine
    mid_price: float
    price: float
    size: float
    tasks: list[asyncio.Task[Any]]
    cleanup_cloids: set[str] = field(default_factory=set)

    def make_intent(self, cloid: str | None = None) -> OrderIntent:
        return OrderIntent(
            symbol=Symbol(self.config.symbol),
            side=Side.BUY,
            size=Size(self.size),
            price=Price(self.price),
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.ALO,
            strategy_id=StrategyId("testnet_gate"),
            cloid=Cloid(cloid) if cloid else None,
        )

    async def place_resting(self, cloid: str | None = None) -> Order:
        submitted = await self.engine.submit_order(self.make_intent(cloid))
        canonical_cloid = str(submitted.cloid)
        self.cleanup_cloids.add(canonical_cloid)

        async with asyncio.timeout(15.0):
            while True:
                current = await self.engine.get_order(canonical_cloid)
                if current is not None and current.status == OrderStatus.ACKNOWLEDGED:
                    await self.wait_until_open(canonical_cloid)
                    return current
                if current is not None and current.is_terminal:
                    pytest.fail(f"testnet order did not rest: status={current.status}, error={current.error_message}")
                await asyncio.sleep(0.05)

    async def query(self, cloid: str) -> dict[str, Any]:
        from hyperliquid.utils.types import Cloid as HlCloid

        result = await asyncio.to_thread(
            self.info.query_order_by_cloid,
            self.config.account_address,
            HlCloid.from_str(cloid),
        )
        assert isinstance(result, dict), f"invalid order query response: {result!r}"
        return result

    async def open_orders(self) -> list[dict[str, Any]]:
        result = await asyncio.to_thread(self.info.open_orders, self.config.account_address)
        assert isinstance(result, list), f"invalid open-orders response: {result!r}"
        return result

    async def wait_until_open(self, cloid: str, timeout: float = 15.0) -> dict[str, Any]:
        async with asyncio.timeout(timeout):
            while True:
                result = await self.query(cloid)
                status = str(result.get("order", {}).get("status", "")).lower()
                if result.get("status") == "order" and status == "open":
                    return result
                if status in _TERMINAL_EXCHANGE_STATUSES:
                    pytest.fail(f"order became terminal before cancellation: cloid={cloid}, status={status}")
                await asyncio.sleep(0.25)

    async def wait_until_not_open(self, cloid: str, timeout: float = 15.0) -> None:
        async with asyncio.timeout(timeout):
            while True:
                orders = await self.open_orders()
                if not any(str(item.get("cloid", "")) == cloid for item in orders):
                    return
                await asyncio.sleep(0.25)

    async def cancel_direct(self, cloid: str) -> None:
        from hyperliquid.utils.types import Cloid as HlCloid

        with contextlib.suppress(Exception):
            await self.nonce.submit(
                self.exchange.cancel_by_cloid,
                self.config.symbol,
                HlCloid.from_str(cloid),
                cloid_hint=cloid,
            )


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        pytest.fail(f"{name} must be explicitly set for live testnet integration tests", pytrace=False)
    return value


def _validate_testnet_url(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != _TESTNET_HOST
        or parsed.port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        pytest.fail(
            "HYPE_EXCHANGE__API_URL must be exactly the official Hyperliquid testnet HTTPS endpoint; "
            "mainnet, proxies, and custom hosts are forbidden",
            pytrace=False,
        )
    return f"https://{_TESTNET_HOST}"


def _testnet_postgres_url() -> str:
    raw_url = os.getenv("HYPE_TESTNET_POSTGRES__URL", "").strip() or os.getenv("HYPE_POSTGRES__URL", "").strip()
    if not raw_url:
        pytest.fail(
            "HYPE_TESTNET_POSTGRES__URL (or HYPE_POSTGRES__URL) is required after live testnet opt-in",
            pytrace=False,
        )
    parsed = urlparse(raw_url)
    database = parsed.path.removeprefix("/").lower()
    if parsed.scheme != "postgresql+asyncpg" or not parsed.hostname or not database:
        pytest.fail("testnet Postgres URL must use postgresql+asyncpg and name a database", pytrace=False)
    if "test" not in database:
        pytest.fail("testnet integration Postgres database name must contain 'test'", pytrace=False)
    return raw_url


@pytest.fixture(scope="session")
def testnet_config() -> TestnetConfig:
    """Skip only when not opted in; every failed live precondition is an error."""
    if os.getenv("HYPEEDGE_RUN_TESTNET_INTEGRATION", "") != _RUN_ACK:
        pytest.skip("live testnet disabled; set HYPEEDGE_RUN_TESTNET_INTEGRATION to the documented acknowledgement")

    if os.getenv("HYPEEDGE_TESTNET_DEDICATED_ACCOUNT", "") != _ACCOUNT_ACK:
        pytest.fail(
            "HYPEEDGE_TESTNET_DEDICATED_ACCOUNT acknowledgement is required; shared accounts are forbidden",
            pytrace=False,
        )
    if os.getenv("HYPE_ENVIRONMENT", "") != "testnet":
        pytest.fail("HYPE_ENVIRONMENT must be explicitly set to testnet", pytrace=False)

    api_url = _validate_testnet_url(_required_env("HYPE_EXCHANGE__API_URL"))
    postgres_url = _testnet_postgres_url()
    account_address = _required_env("HYPE_EXCHANGE__ACCOUNT_ADDRESS")
    private_key = _required_env("HYPE_EXCHANGE__AGENT_PRIVATE_KEY")

    from eth_account import Account
    from eth_utils import is_address

    if not is_address(account_address):
        pytest.fail("HYPE_EXCHANGE__ACCOUNT_ADDRESS is not a valid EVM address", pytrace=False)
    try:
        Account.from_key(private_key)
    except Exception:
        pytest.fail("HYPE_EXCHANGE__AGENT_PRIVATE_KEY is not a valid private key", pytrace=False)

    try:
        max_notional = float(_required_env("HYPEEDGE_TESTNET_MAX_NOTIONAL_USD"))
    except ValueError:
        pytest.fail("HYPEEDGE_TESTNET_MAX_NOTIONAL_USD must be numeric", pytrace=False)
    if not _MIN_CONFIGURED_NOTIONAL_USD <= max_notional <= _MAX_NOTIONAL_HARD_CAP_USD:
        pytest.fail(
            f"HYPEEDGE_TESTNET_MAX_NOTIONAL_USD must be between "
            f"{_MIN_CONFIGURED_NOTIONAL_USD:.2f} and {_MAX_NOTIONAL_HARD_CAP_USD:.2f}",
            pytrace=False,
        )

    symbol = os.getenv("HYPEEDGE_TESTNET_SYMBOL", "BTC").strip().upper()
    if not symbol or not symbol.isalnum():
        pytest.fail("HYPEEDGE_TESTNET_SYMBOL must be a simple perp symbol such as BTC", pytrace=False)
    try:
        resting_offset = float(os.getenv("HYPEEDGE_TESTNET_RESTING_OFFSET", "0.03"))
    except ValueError:
        pytest.fail("HYPEEDGE_TESTNET_RESTING_OFFSET must be numeric", pytrace=False)
    if not 0.02 <= resting_offset <= 0.10:
        pytest.fail("HYPEEDGE_TESTNET_RESTING_OFFSET must be between 0.02 and 0.10", pytrace=False)

    return TestnetConfig(
        api_url=api_url,
        postgres_url=postgres_url,
        account_address=account_address,
        agent_private_key=private_key,
        symbol=symbol,
        max_notional_usd=max_notional,
        resting_offset=resting_offset,
    )


def _position_size(raw_state: dict[str, Any], symbol: str) -> float:
    for item in raw_state.get("assetPositions", []):
        position = item.get("position", {})
        if position.get("coin") == symbol:
            return float(position.get("szi", 0.0))
    return 0.0


def _remaining_action_credits(rate_limit: dict[str, Any]) -> int:
    if "remaining" in rate_limit:
        return int(rate_limit["remaining"])
    return max(0, int(rate_limit.get("nRequestsCap", 0)) - int(rate_limit.get("nRequestsUsed", 0)))


async def _idle_component() -> None:
    """Keep non-trading app services alive without requiring local CH/ports in this live gate."""
    await asyncio.Event().wait()


async def _noop_component() -> None:
    """Skip lifecycle hooks for a daemon intentionally held idle."""


def _disable_non_trading_side_effects(app: HypeEdgeApp) -> None:
    """Keep the production trading branch intact while suppressing unrelated daemons."""
    idle_targets = (
        app._ws_feed,
        app._ch_writer,
        app._metrics,
        app._backfill,
        app._instrument_cache,
        app._quality_checker,
    )
    for target in idle_targets:
        if target is not None:
            target.run = _idle_component  # type: ignore[method-assign, union-attr]
    if app._metrics is not None:
        app._metrics.serve = _idle_component  # type: ignore[method-assign]
    if app._ch_writer is not None:
        app._ch_writer.flush = _noop_component  # type: ignore[method-assign]
        app._ch_writer.close = _noop_component  # type: ignore[method-assign]
    app._start_api_server = lambda: None  # type: ignore[method-assign]
    app._init_strategy = lambda: None  # type: ignore[method-assign]


@pytest.fixture
async def testnet_harness(
    testnet_config: TestnetConfig, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[TestnetHarness]:
    """Start the actual HypeEdgeApp V2 initialization and trading lifecycle."""
    from hyperliquid.info import Info

    # AppSettings gives HYPE_POSTGRES__URL precedence over init values. Make the
    # explicitly selected test database the single authority for app + Alembic.
    monkeypatch.setenv("HYPE_POSTGRES__URL", testnet_config.postgres_url)

    preflight_info = Info(testnet_config.api_url, skip_ws=True)
    try:
        meta, mids, user_state, open_orders, user_rate_limit = await asyncio.gather(
            asyncio.to_thread(preflight_info.meta),
            asyncio.to_thread(preflight_info.all_mids),
            asyncio.to_thread(preflight_info.user_state, testnet_config.account_address),
            asyncio.to_thread(preflight_info.open_orders, testnet_config.account_address),
            asyncio.to_thread(preflight_info.user_rate_limit, testnet_config.account_address),
        )
    except Exception as exc:
        pytest.fail(f"testnet preflight request failed: {exc}", pytrace=False)

    if not isinstance(meta, dict) or not isinstance(mids, dict) or not isinstance(user_state, dict):
        pytest.fail("testnet returned an invalid metadata/account response", pytrace=False)
    if not isinstance(open_orders, list) or not isinstance(user_rate_limit, dict):
        pytest.fail("testnet returned an invalid orders/rate-limit response", pytrace=False)
    if open_orders:
        pytest.fail("dedicated testnet account has pre-existing open orders; refusing to mutate it", pytrace=False)
    if abs(_position_size(user_state, testnet_config.symbol)) > 1e-12:
        pytest.fail(
            f"dedicated testnet account has a pre-existing {testnet_config.symbol} position",
            pytrace=False,
        )

    asset = next((item for item in meta.get("universe", []) if item.get("name") == testnet_config.symbol), None)
    if asset is None or testnet_config.symbol not in mids:
        pytest.fail(f"{testnet_config.symbol} is not an active testnet perpetual", pytrace=False)
    mid_price = float(mids[testnet_config.symbol])
    if mid_price <= 0:
        pytest.fail(f"{testnet_config.symbol} has no positive testnet mid price", pytrace=False)
    sz_decimals = int(asset["szDecimals"])
    target_notional = min(12.0, testnet_config.max_notional_usd * 0.9)
    size_quantum = 10.0**-sz_decimals
    size = math.ceil((target_notional / mid_price) / size_quantum) * size_quantum
    if size * mid_price > testnet_config.max_notional_usd:
        pytest.fail("symbol size precision cannot satisfy the configured notional safety cap", pytrace=False)
    price = round(float(f"{mid_price * (1.0 - testnet_config.resting_offset):.5g}"), 6 - sz_decimals)
    if price * size < 10.0:
        pytest.fail("computed resting order is below Hyperliquid's minimum notional", pytrace=False)

    available = float(
        user_state.get("withdrawable", user_state.get("marginSummary", {}).get("totalMarginAvailable", 0))
    )
    if available < testnet_config.max_notional_usd:
        pytest.fail("dedicated testnet account has insufficient withdrawable balance", pytrace=False)
    credits = _remaining_action_credits(user_rate_limit)
    if credits < _MIN_ACTION_CREDITS:
        pytest.fail(f"testnet action credits too low: remaining={credits}", pytrace=False)

    settings = AppSettings(
        environment="testnet",
        exchange=ExchangeSettings(
            api_url=testnet_config.api_url,
            ws_url="wss://api.hyperliquid-testnet.xyz/ws",
            account_address=testnet_config.account_address,
            agent_private_key=testnet_config.agent_private_key,
        ),
        postgres=PostgresSettings(url=testnet_config.postgres_url),
        risk=RiskSettings(
            max_position_pct=0.50,
            max_strategy_loss_pct=0.20,
            max_drawdown_pct=0.30,
            max_leverage=20,
            risk_check_timeout_ms=1000,
            action_credits_low_watermark=_ACTION_CREDIT_WATERMARK,
        ),
        features=FeatureFlagsSettings(
            durable_ledger_v2=True,
            execution_v2=True,
            user_stream_v2=True,
            reconciliation_v2=True,
            api_v1=True,
            strategy_runner_v2=True,
        ),
    )
    app = HypeEdgeApp(settings)
    await app._initialize_components()
    if not app._trading_prerequisites_ok:
        pytest.fail(
            "HypeEdgeApp V2 initialization failed: Postgres must be reachable and Alembic must be at head",
            pytrace=False,
        )
    if any(
        component is None
        for component in (
            app._pg_engine,
            app._pg_session_factory,
            app._durable_order_store,
            app._signed_action_executor,
            app._execution_engine,
            app._nonce_manager,
        )
    ):
        pytest.fail("HypeEdgeApp did not build the durable V2 execution stack", pytrace=False)

    _disable_non_trading_side_effects(app)
    kill_queue = app.event_bus.subscribe(EVENT_KILL_SWITCH_TRIGGERED)
    watch_task = asyncio.create_task(app._watch_kill_switch(kill_queue), name="kill_switch_watcher")
    tasks = await app._start_components()
    tasks.append(watch_task)
    app._tasks = tasks

    if not app.trading_enabled:
        await _stop_app(app, tasks)
        pytest.fail(
            f"HypeEdgeApp V2 startup gate failed closed: safety_mode={app.safety_mode}",
            pytrace=False,
        )
    required_tasks = {"nonce_manager", "exchange_event_ingestor", "signed_action_executor"}
    running_names = {task.get_name() for task in tasks if not task.done()}
    if not required_tasks <= running_names or app._exchange_ingestor is None:
        await _stop_app(app, tasks)
        pytest.fail(f"HypeEdgeApp V2 workers missing: {sorted(required_tasks - running_names)}", pytrace=False)
    if app.action_credits_remaining is None or app.action_credits_remaining < _MIN_ACTION_CREDITS:
        await _stop_app(app, tasks)
        pytest.fail("startup action-credit refresh did not establish a safe balance", pytrace=False)

    nonce = app._nonce_manager
    engine = app._execution_engine
    if nonce.info is None or nonce.exchange is None:
        await _stop_app(app, tasks)
        pytest.fail("HypeEdgeApp SDK connection was not wired into NonceManager", pytrace=False)
    harness = TestnetHarness(
        config=testnet_config,
        app=app,
        info=nonce.info,
        exchange=nonce.exchange,
        nonce=nonce,
        engine=engine,
        mid_price=mid_price,
        price=price,
        size=size,
        tasks=tasks,
    )

    try:
        yield harness
    finally:
        for cloid in tuple(harness.cleanup_cloids):
            await harness.cancel_direct(cloid)
        for cloid in tuple(harness.cleanup_cloids):
            with contextlib.suppress(TimeoutError):
                await harness.wait_until_not_open(cloid, timeout=10.0)

        final_state = await asyncio.to_thread(harness.info.user_state, testnet_config.account_address)
        final_size = _position_size(final_state, testnet_config.symbol) if isinstance(final_state, dict) else 0.0
        if abs(final_size) > 1e-12:
            cleanup_cloid = "0x" + uuid.uuid4().hex
            from hyperliquid.utils.types import Cloid as HlCloid

            await nonce.submit(
                harness.exchange.market_close,
                testnet_config.symbol,
                abs(final_size),
                px=None,
                slippage=0.05,
                cloid=HlCloid.from_str(cleanup_cloid),
                cloid_hint=cleanup_cloid,
            )
        await _stop_app(app, tasks)


async def _stop_app(app: HypeEdgeApp, tasks: list[asyncio.Task[Any]]) -> None:
    """Stop the app-owned V2 workers and close durable connections."""
    app._tasks = tasks
    await app._graceful_shutdown()
