"""Tests for the FastAPI API layer."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient
from starlette.websockets import WebSocketDisconnect

from hypeedge.api.app import create_api
from hypeedge.api.routes.market_ws import MarketMessageBudget, MarketWsGuard
from hypeedge.app import HypeEdgeApp
from hypeedge.core.enums import MarketMakerLifecycle, OrderStatus, StrategyStatus
from hypeedge.core.exceptions import ExecutionError
from hypeedge.core.models import AccountState, Candle, FundingRate, L2BookSnapshot, L2Level, Order, Position
from hypeedge.core.types import Cloid, Price, Size, StrategyId, Symbol, Timestamp, Usd
from hypeedge.market_data.instrument_cache import InstrumentInfo
from hypeedge.risk.checker import RiskChecker, RiskLimits
from hypeedge.risk.kill_switch import KillSwitch


def _make_mock_app():
    """Create a mock HypeEdgeApp with all components wired."""
    from hypeedge.core.events import EventBus

    bus = EventBus(queue_maxsize=100)

    # AccountTracker
    tracker = MagicMock()
    tracker.get_status.return_value = {
        "equity": 12450.0,
        "peak_equity": 13000.0,
        "drawdown_pct": 0.042,
        "total_fees": 5.30,
        "total_funding": -1.20,
        "fill_count": 15,
        "position_count": 2,
        "leverage": 1.8,
        "positions": {},
        "last_update": "2026-06-02T10:00:00",
    }
    tracker.get_all_positions.return_value = {
        Symbol("BTC"): Position(
            symbol=Symbol("BTC"),
            size=Size(0.15),
            entry_price=Price(68500.0),
            mark_price=Price(69200.0),
        ),
    }
    tracker.get_account_state.return_value = AccountState(
        equity=Usd(12450.0),
        available_balance=Usd(10000.0),
        total_margin_used=Usd(2450.0),
        total_unrealized_pnl=Usd(105.0),
        peak_equity=Usd(13000.0),
    )
    tracker.current_equity = Usd(12450.0)
    tracker.peak_equity = Usd(13000.0)
    tracker.drawdown_pct = 0.042
    tracker.get_leverage.return_value = 1.8

    # ExecutionEngine
    engine = MagicMock()
    engine._orders = {}
    engine.get_open_orders = AsyncMock(return_value=[])
    engine.submit_order = AsyncMock(
        return_value=Order(
            cloid=Cloid("test_cloid"),
            symbol=Symbol("BTC"),
            side="buy",
            size=Size(0.1),
            price=Price(69000.0),
            order_type="limit",
            time_in_force="Gtc",
            status=OrderStatus.ACKNOWLEDGED,
        )
    )
    engine.cancel_order = AsyncMock(return_value=True)

    # RiskChecker
    checker = MagicMock(spec=RiskChecker)
    checker.limits = RiskLimits(
        max_position_pct=0.20,
        max_strategy_loss_pct=0.05,
        max_drawdown_pct=0.10,
        max_leverage=5,
    )
    checker.stats = {"check_count": 100, "reject_count": 2, "pass_count": 98}
    checker.strategy_pnl = {"trend_v1": -200.0}

    # KillSwitch
    kill_switch = MagicMock(spec=KillSwitch)
    kill_switch.is_active = False
    kill_switch.reason = None

    # Strategy
    strategy = MagicMock()
    strategy.strategy_id = StrategyId("trend_v1")
    strategy.status = StrategyStatus.RUNNING
    strategy.params = MagicMock(
        symbol="BTC",
        fast_ema_period=12,
        slow_ema_period=26,
        signal_ema_period=9,
        momentum_period=10,
        momentum_threshold=0.0,
        atr_period=14,
        atr_position_multiplier=0.5,
        max_position_pct=0.15,
        risk_per_trade_pct=0.01,
        atr_stop_multiplier=2.0,
    )
    strategy.position_size = 0.15
    strategy.entry_price = 68500.0
    strategy.stop_price = 67000.0
    strategy.on_start = AsyncMock()
    strategy.on_stop = AsyncMock()

    # HypeEdgeApp mock
    app_mock = MagicMock()
    app_mock.trading_enabled = True
    app_mock.account_tracker = tracker
    app_mock.execution_engine = engine
    app_mock.risk_checker = checker
    app_mock.kill_switch = kill_switch
    app_mock.strategy = strategy
    app_mock.start_strategy = AsyncMock(return_value=True)
    app_mock.stop_strategy = AsyncMock(return_value=True)
    app_mock.trigger_kill_switch = AsyncMock(return_value=True)
    app_mock.event_bus = bus
    app_mock.is_shutting_down = False
    app_mock._rate_limiter = None
    app_mock.action_credits_remaining = None
    app_mock._safety_controller = MagicMock()
    app_mock._safety_controller.mode = "normal"
    app_mock._safety_controller.reason = None
    app_mock.safety_mode = "normal"
    app_mock.settings = MagicMock()
    app_mock.settings.environment = "testnet"
    app_mock.settings.api = MagicMock()
    app_mock.settings.api.auth_token = ""
    app_mock.settings.api.host = "127.0.0.1"
    app_mock.settings.api.cors_origins = ["http://localhost:34001"]
    app_mock.settings.api.request_rate_limit_per_minute = 600
    app_mock.settings.api.mutation_rate_limit_per_minute = 60
    app_mock.settings.api.auth_failure_limit_per_minute = 10
    app_mock.settings.api.market_ws_max_connections = 100
    app_mock.settings.api.market_ws_max_connections_per_ip = 5
    app_mock.settings.api.market_ws_queue_size = 8
    app_mock.settings.api.market_ws_messages_per_second = 50
    app_mock._instrument_cache = MagicMock()
    app_mock._instrument_cache.is_loaded = True
    app_mock._instrument_cache.get.return_value = InstrumentInfo(
        symbol=Symbol("BTC"),
        sz_decimals=5,
        max_leverage=40,
        tick_size=0.1,
        lot_size=0.00001,
        min_size=0.00001,
    )
    app_mock._market_data_provider = MagicMock()
    app_mock._market_data_provider.get_funding.return_value = FundingRate(
        symbol=Symbol("BTC"),
        funding_rate=0.0001,
        premium=0.00005,
        mark_price=Price(69_200),
        open_interest=1234.5,
        timestamp=Timestamp(1_700_000_000_000),
    )
    app_mock._market_data_provider.get_book.return_value = L2BookSnapshot(
        symbol=Symbol("BTC"),
        bids=[L2Level(Price(69_190), Size(1.2))],
        asks=[L2Level(Price(69_210), Size(0.8))],
        timestamp=Timestamp(1_700_000_000_000),
    )
    app_mock._market_data_provider.get_candles.return_value = [
        Candle(
            symbol=Symbol("BTC"),
            interval="1m",
            open=Price(69_000),
            high=Price(69_300),
            low=Price(68_900),
            close=Price(69_200),
            volume=Size(100),
            timestamp=Timestamp(1_700_000_000_000),
        )
    ]
    app_mock.projection_reader = None
    app_mock.market_making_repository = None
    app_mock.strategy_supervisor = None

    return app_mock


def test_market_ws_guard_enforces_global_and_per_ip_limits() -> None:
    guard = MarketWsGuard(total_limit=2, per_ip_limit=1)
    assert guard.acquire("10.0.0.1") is True
    assert guard.acquire("10.0.0.1") is False
    assert guard.acquire("10.0.0.2") is True
    assert guard.acquire("10.0.0.3") is False
    guard.release("10.0.0.1")
    assert guard.acquire("10.0.0.3") is True


@pytest.fixture
def api_client():
    """Create an async test client with mocked HypeEdgeApp."""
    app_mock = _make_mock_app()
    api_app = create_api(app_mock)
    transport = ASGITransport(app=api_app)
    return AsyncClient(transport=transport, base_url="http://test")


class TestHealthEndpoint:
    @pytest.mark.asyncio
    async def test_health(self, api_client):
        async with api_client as client:
            resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["trading_enabled"] is True


class TestMarketMakingControlPlane:
    @pytest.mark.asyncio
    async def test_lists_multi_instance_strategies_from_authoritative_store(self):
        app_mock = _make_mock_app()
        repository = MagicMock()
        repository.list_strategy_instances = AsyncMock(
            return_value=[
                {
                    "strategy_id": "mm-btc-1",
                    "strategy_type": "market_maker",
                    "symbol": "BTC",
                    "desired_state": "shadow",
                    "revision": 3,
                }
            ]
        )
        app_mock.market_making_repository = repository
        api_app = create_api(app_mock)
        async with AsyncClient(transport=ASGITransport(app=api_app), base_url="http://test") as client:
            response = await client.get("/api/v1/strategies")

        assert response.status_code == 200
        assert response.json()["data"][0]["strategy_type"] == "market_maker"

    @pytest.mark.asyncio
    async def test_lifecycle_requires_if_match_and_dispatches_to_supervisor(self):
        app_mock = _make_mock_app()
        app_mock.market_making_repository = MagicMock()
        supervisor = MagicMock()
        supervisor.start = AsyncMock(return_value={"strategy_id": "mm-btc-1", "actual_state": "shadow", "revision": 4})
        app_mock.strategy_supervisor = supervisor
        api_app = create_api(app_mock)
        async with AsyncClient(transport=ASGITransport(app=api_app), base_url="http://test") as client:
            missing = await client.post(
                "/api/v1/strategies/mm-btc-1/actions/start",
                headers={"Idempotency-Key": "mm-start-missing-revision"},
                json={"target": "shadow"},
            )
            started = await client.post(
                "/api/v1/strategies/mm-btc-1/actions/start",
                headers={"Idempotency-Key": "mm-start-1", "If-Match": '"3"'},
                json={"target": "shadow"},
            )

        assert missing.status_code == 428
        assert started.status_code == 200
        supervisor.start.assert_awaited_once_with(
            StrategyId("mm-btc-1"),
            target=MarketMakerLifecycle.SHADOW,
            expected_revision=3,
        )

    @pytest.mark.asyncio
    async def test_events_query_passes_bounded_limit(self):
        app_mock = _make_mock_app()
        repository = MagicMock()
        repository.get_market_making_events = AsyncMock(return_value=[])
        app_mock.market_making_repository = repository
        api_app = create_api(app_mock)
        async with AsyncClient(transport=ASGITransport(app=api_app), base_url="http://test") as client:
            response = await client.get("/api/v1/market-making/mm-btc-1/events?limit=25")

        assert response.status_code == 200
        repository.get_market_making_events.assert_awaited_once_with(StrategyId("mm-btc-1"), limit=25)

    @pytest.mark.asyncio
    async def test_session_mutation_requires_matching_csrf_token(self):
        app_mock = _make_mock_app()
        app_mock.market_making_repository = MagicMock()
        app_mock.strategy_supervisor = MagicMock()
        api_app = create_api(app_mock)
        async with AsyncClient(transport=ASGITransport(app=api_app), base_url="http://test") as client:
            client.cookies.set("hypeedge_session", "session")
            client.cookies.set("hypeedge_csrf", "expected")
            response = await client.post(
                "/api/v1/strategies/mm-btc-1/actions/stop",
                headers={"Idempotency-Key": "csrf-stop", "If-Match": "1", "X-CSRF-Token": "wrong"},
                json={},
            )

        assert response.status_code == 403
        assert response.json()["code"] == "CSRF_VALIDATION_FAILED"


class TestAccountAPI:
    @pytest.mark.asyncio
    async def test_get_account(self, api_client):
        async with api_client as client:
            resp = await client.get("/api/v1/account")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["equity"] == "12450"
        assert data["data"]["available_balance"] == "10000"
        assert data["data"]["total_margin_used"] == "2450"
        assert data["data"]["total_unrealized_pnl"] == "105"
        assert float(data["data"]["drawdown_pct"]) == pytest.approx(0.0423076923)

    @pytest.mark.asyncio
    async def test_get_account_without_tracker_returns_empty_snapshot(self) -> None:
        app_mock = _make_mock_app()
        app_mock.account_tracker = None
        app_mock.projection_reader = None
        app_mock.trading_enabled = False
        api_app = create_api(app_mock)
        async with AsyncClient(transport=ASGITransport(app=api_app), base_url="http://test") as client:
            resp = await client.get("/api/v1/account")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["equity"] == "0"
        assert data["data"]["trading_enabled"] is False
        assert data["data"]["position_count"] == 0

    @pytest.mark.asyncio
    async def test_get_account_without_state_while_trading_disabled(self) -> None:
        app_mock = _make_mock_app()
        app_mock.projection_reader = None
        app_mock.trading_enabled = False
        app_mock.account_tracker.get_account_state.return_value = None
        api_app = create_api(app_mock)
        async with AsyncClient(transport=ASGITransport(app=api_app), base_url="http://test") as client:
            resp = await client.get("/api/v1/account")
        assert resp.status_code == 200
        assert resp.json()["data"]["equity"] == "0"

    @pytest.mark.asyncio
    async def test_get_account_prefers_live_tracker_over_stale_projection(self) -> None:
        """Clearinghouse poller updates tracker; projection only refreshes on reconcile."""
        from datetime import UTC, datetime
        from unittest.mock import AsyncMock, MagicMock

        app_mock = _make_mock_app()
        stale = MagicMock()
        stale.equity = Decimal("0")
        stale.available_balance = Decimal("0")
        stale.total_margin_used = Decimal("0")
        stale.total_unrealized_pnl = Decimal("0")
        stale.peak_equity = Decimal("0")
        stale.exchange_updated_at = datetime(2026, 7, 12, 9, 33, 39, tzinfo=UTC)
        reader = MagicMock()
        reader.get_account = AsyncMock(return_value=stale)
        reader.get_account_metrics = AsyncMock(
            return_value={
                "leverage": 0,
                "total_fees": Decimal("0"),
                "fill_count": 0,
                "position_count": 0,
            }
        )
        app_mock.projection_reader = reader
        api_app = create_api(app_mock)
        async with AsyncClient(transport=ASGITransport(app=api_app), base_url="http://test") as client:
            resp = await client.get("/api/v1/account")
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["equity"] == "12450"
        assert data["available_balance"] == "10000"
        reader.get_account.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_equity_curve(self, api_client):
        async with api_client as client:
            resp = await client.get("/api/v1/account/equity-curve")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert len(data["data"]) >= 1


class TestPositionsAPI:
    @pytest.mark.asyncio
    async def test_get_positions(self, api_client):
        async with api_client as client:
            resp = await client.get("/api/v1/positions")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert len(data["data"]) == 1
        assert data["data"][0]["symbol"] == "BTC"
        assert data["data"][0]["side"] == "long"


class TestOrdersAPI:
    @pytest.mark.asyncio
    async def test_get_orders_active(self, api_client):
        async with api_client as client:
            resp = await client.get("/api/v1/orders?status=active")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    @pytest.mark.asyncio
    async def test_submit_order(self, api_client):
        async with api_client as client:
            resp = await client.post(
                "/api/v1/orders",
                headers={"Idempotency-Key": "place-order-1"},
                json={
                    "symbol": "BTC",
                    "side": "buy",
                    "size": "0.1",
                    "price": "69000",
                    "order_type": "limit",
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["status"] == "acknowledged"

    @pytest.mark.asyncio
    async def test_repeated_order_key_replays_without_resubmitting(self):
        app_mock = _make_mock_app()
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        request = {
            "symbol": "BTC",
            "side": "buy",
            "size": "0.1",
            "price": "69000",
            "order_type": "limit",
        }
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            first = await client.post("/api/v1/orders", headers={"Idempotency-Key": "repeat-1"}, json=request)
            second = await client.post("/api/v1/orders", headers={"Idempotency-Key": "repeat-1"}, json=request)

        assert first.json() == second.json()
        app_mock.execution_engine.submit_order.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_reused_order_key_with_different_body_returns_conflict(self):
        app_mock = _make_mock_app()
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        headers = {"Idempotency-Key": "conflict-1"}
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            first = await client.post(
                "/api/v1/orders",
                headers=headers,
                json={"symbol": "BTC", "side": "buy", "size": "0.1", "price": "69000"},
            )
            conflict = await client.post(
                "/api/v1/orders",
                headers=headers,
                json={"symbol": "BTC", "side": "buy", "size": "0.2", "price": "69000"},
            )

        assert first.status_code == 200
        assert conflict.status_code == 409
        assert conflict.json()["code"] == "IDEMPOTENCY_KEY_REUSED"
        app_mock.execution_engine.submit_order.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cancel_order(self, api_client):
        async with api_client as client:
            resp = await client.post(
                "/api/v1/orders/test_cloid/cancel",
                headers={"Idempotency-Key": "cancel-order-1"},
            )
        assert resp.status_code == 202
        assert resp.json()["ok"] is True

    @pytest.mark.asyncio
    async def test_cancel_execution_failure_is_retryable_service_error(self):
        app_mock = _make_mock_app()
        app_mock.execution_engine.cancel_order.side_effect = ExecutionError("exchange unavailable")
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/orders/test_cloid/cancel",
                headers={"Idempotency-Key": "cancel-service-failure-1"},
            )

        assert response.status_code == 503
        assert response.json()["code"] == "CANCEL_EXECUTION_FAILED"
        assert response.json()["retryable"] is True

    @pytest.mark.asyncio
    async def test_exchange_cancel_rejection_is_conflict_not_not_found(self):
        app_mock = _make_mock_app()
        order = app_mock.execution_engine.submit_order.return_value
        order.error_message = "Order was already cancelled at exchange"
        app_mock.execution_engine.cancel_order.return_value = False
        app_mock.execution_engine.get_order = AsyncMock(return_value=order)
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/orders/test_cloid/cancel",
                headers={"Idempotency-Key": "cancel-rejected-1"},
            )

        assert response.status_code == 409
        assert response.json()["code"] == "CANCEL_REJECTED"

    @pytest.mark.asyncio
    async def test_repeated_cancel_key_does_not_cancel_twice(self):
        app_mock = _make_mock_app()
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        headers = {"Idempotency-Key": "cancel-repeat-1"}
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            first = await client.post("/api/v1/orders/test_cloid/cancel", headers=headers)
            second = await client.post("/api/v1/orders/test_cloid/cancel", headers=headers)

        assert first.json() == second.json()
        app_mock.execution_engine.cancel_order.assert_awaited_once_with("test_cloid")

    @pytest.mark.asyncio
    async def test_v1_order_requires_idempotency_key(self, api_client):
        async with api_client as client:
            resp = await client.post(
                "/api/v1/orders",
                json={"symbol": "BTC", "side": "buy", "size": "0.1", "price": "69000"},
            )
        assert resp.status_code == 400
        assert resp.json()["code"] == "IDEMPOTENCY_KEY_REQUIRED"

    @pytest.mark.asyncio
    async def test_order_rejects_unknown_fields(self, api_client):
        async with api_client as client:
            resp = await client.post(
                "/api/v1/orders",
                headers={"Idempotency-Key": "invalid-order-1"},
                json={"symbol": "BTC", "side": "buy", "size": "0.1", "price": "69000", "unsafe": True},
            )
        assert resp.status_code == 422
        assert resp.headers["content-type"].startswith("application/problem+json")

    @pytest.mark.asyncio
    async def test_order_requires_decimal_strings(self, api_client):
        async with api_client as client:
            response = await client.post(
                "/api/v1/orders",
                headers={"Idempotency-Key": "numeric-order-1"},
                json={"symbol": "BTC", "side": "buy", "size": 0.1, "price": 69000},
            )
        assert response.status_code == 422

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("size", "price", "code"),
        [
            ("0.000001", "69000", "ORDER_SIZE_BELOW_MINIMUM"),
            ("0.100001", "69000", "ORDER_SIZE_NOT_ON_LOT"),
            ("0.1", "69000.05", "ORDER_PRICE_NOT_ON_TICK"),
        ],
    )
    async def test_order_validates_instrument_precision(self, size, price, code):
        app_mock = _make_mock_app()
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/orders",
                headers={"Idempotency-Key": f"precision-{code}"},
                json={"symbol": "BTC", "side": "buy", "size": size, "price": price},
            )
        assert response.status_code == 422
        assert response.json()["code"] == code
        app_mock.execution_engine.submit_order.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_legacy_api_is_removed(self, api_client):
        async with api_client as client:
            response = await client.post(
                "/api/orders",
                json={"symbol": "BTC", "side": "buy", "size": "0.1", "price": "69000"},
            )
        assert response.status_code == 404


class TestV1API:
    @pytest.mark.asyncio
    async def test_close_position_derives_reduce_side(self):
        app_mock = _make_mock_app()
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/v1/positions/BTC/close",
                headers={"Idempotency-Key": "close-btc-1"},
                json={"close_fraction": "1"},
            )
        assert resp.status_code == 202
        intent = app_mock.execution_engine.submit_order.await_args.args[0]
        assert intent.side.value == "sell"
        assert intent.size == Size("0.15")
        assert intent.reduce_only is True

    @pytest.mark.asyncio
    async def test_repeated_close_key_does_not_submit_twice(self):
        app_mock = _make_mock_app()
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        headers = {"Idempotency-Key": "close-repeat-1"}
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            first = await client.post(
                "/api/v1/positions/BTC/close",
                headers=headers,
                json={"close_fraction": "1"},
            )
            second = await client.post(
                "/api/v1/positions/BTC/close",
                headers=headers,
                json={"close_fraction": "1"},
            )

        assert first.json() == second.json()
        app_mock.execution_engine.submit_order.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_system_and_meta_queries(self, api_client):
        async with api_client as client:
            status_resp = await client.get("/api/v1/system/status")
            meta_resp = await client.get("/api/v1/market/BTC/meta")
        assert status_resp.json()["data"]["environment"] == "testnet"
        assert status_resp.json()["data"]["safety_mode"] == "normal"
        assert meta_resp.json()["data"]["size_decimals"] == 5
        assert meta_resp.json()["data"]["price_decimals"] == 1

    @pytest.mark.asyncio
    async def test_market_endpoints_use_normalized_backend_snapshots(self, api_client):
        async with api_client as client:
            funding = await client.get("/api/v1/market/BTC/funding")
            book = await client.get("/api/v1/market/BTC/book")
            candles = await client.get("/api/v1/market/BTC/candles?interval=1m&limit=1")
        assert funding.json()["data"]["mark_price"] == "69200"
        assert book.json()["data"]["source"] == "websocket"
        assert book.json()["data"]["bids"] == [["69190", "1.2"]]
        assert candles.json()["data"][0]["close"] == "69200"

    @pytest.mark.asyncio
    async def test_market_funding_never_returns_fake_zero_snapshot(self):
        app_mock = _make_mock_app()
        app_mock._market_data_provider.get_funding.return_value = None
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/api/v1/market/BTC/funding")
        assert response.status_code == 503
        assert response.json()["code"] == "MARKET_DATA_NOT_READY"

    def test_market_websocket_starts_with_sequenced_backend_snapshot(self):
        app_mock = _make_mock_app()
        api_app = create_api(app_mock)
        with (
            TestClient(api_app) as client,
            client.websocket_connect("/ws/v1/market?symbol=BTC&interval=1m") as websocket,
        ):
            message = websocket.receive_json()
        assert message["type"] == "snapshot"
        assert message["sequence"] == 1
        assert message["symbol"] == "BTC"
        assert message["data"]["book"]["source"] == "websocket"

    def test_market_websocket_subscribes_before_snapshot_without_gap(self):
        from hypeedge.core.events import EVENT_L2_BOOK_UPDATE, Event

        app_mock = _make_mock_app()
        snapshot = app_mock._market_data_provider.get_book.return_value
        update = L2BookSnapshot(
            symbol=Symbol("BTC"),
            bids=[L2Level(Price(69_200), Size(1.0))],
            asks=[L2Level(Price(69_220), Size(1.0))],
            timestamp=Timestamp(int(snapshot.timestamp) + 1),
        )

        def snapshot_with_concurrent_update(_symbol: Symbol) -> L2BookSnapshot:
            app_mock.event_bus.publish_sync(Event(event_type=EVENT_L2_BOOK_UPDATE, payload=update))
            return snapshot

        app_mock._market_data_provider.get_book.side_effect = snapshot_with_concurrent_update
        api_app = create_api(app_mock)
        with (
            TestClient(api_app) as client,
            client.websocket_connect("/ws/v1/market?symbol=BTC&interval=1m") as websocket,
        ):
            first = websocket.receive_json()
            second = websocket.receive_json()

        assert first["type"] == "snapshot"
        assert second["type"] == "book"
        assert second["data"]["timestamp"] == int(update.timestamp)
        assert second["sequence"] == first["sequence"] + 1

    def test_market_websocket_throttle_exposes_sequence_gap(self):
        budget = MarketMessageBudget(2, sequence=1, now=100.0, sent=1)

        assert budget.next_event(100.0) == (2, True)
        assert budget.next_event(100.0) == (3, False)
        assert budget.next_event(101.0) == (4, True)

    def test_market_making_websocket_matches_frontend_fair_value_contract(self):
        app_mock = _make_mock_app()
        app_mock.market_making_runtime_snapshot = MagicMock(
            return_value=SimpleNamespace(
                quote_revision=7,
                market_version=11,
                desired=SimpleNamespace(fair_price=Price("69205"), reservation_price=Price("69200")),
                features=SimpleNamespace(
                    best_bid=Price("69190"),
                    best_ask=Price("69210"),
                    external_source="binance_composite",
                    external_symbol="BTCUSDT",
                    external_raw_price=Price("69208"),
                    external_adjusted_price=Price("69204"),
                    external_basis_bps=Decimal("-0.58"),
                    external_effective_weight=Decimal("0.2"),
                    external_confidence=Decimal("0.8"),
                    external_age_ms=25,
                    external_quality="healthy",
                    external_observed_at=datetime.now(UTC),
                ),
            )
        )
        api_app = create_api(app_mock)
        with (
            TestClient(api_app) as client,
            client.websocket_connect("/ws/v1/market-making?strategy_id=mm-btc") as websocket,
        ):
            message = websocket.receive_json()

        assert message["type"] == "fair_value"
        assert message["runtime_revision"] == 7
        assert message["market_revision"] == 11
        assert message["fair_price"] == "69205"
        assert message["external_reference"]["quality"] == "healthy"
        assert "data" not in message

    def test_market_websocket_rejects_untrusted_browser_origin(self):
        app_mock = _make_mock_app()
        api_app = create_api(app_mock)
        with (
            TestClient(api_app) as client,
            pytest.raises(WebSocketDisconnect),
            client.websocket_connect(
                "/ws/v1/market?symbol=BTC&interval=1m",
                headers={"Origin": "https://attacker.example"},
            ),
        ):
            pass


class TestAuthentication:
    @pytest.mark.asyncio
    async def test_bearer_token_is_required_when_configured(self):
        app_mock = _make_mock_app()
        app_mock.settings.api.auth_token = "secret-token"
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            denied = await client.get("/api/v1/account")
            allowed = await client.get(
                "/api/v1/account",
                headers={"Authorization": "Bearer secret-token"},
            )
        assert denied.status_code == 401
        assert denied.json()["code"] == "AUTHENTICATION_REQUIRED"
        assert denied.headers["x-content-type-options"] == "nosniff"
        assert allowed.status_code == 200

    @pytest.mark.asyncio
    async def test_failed_authentication_is_rate_limited(self):
        app_mock = _make_mock_app()
        app_mock.settings.api.auth_token = "a" * 32
        app_mock.settings.api.auth_failure_limit_per_minute = 1
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            first = await client.get("/api/v1/account")
            second = await client.get("/api/v1/account")
        assert first.status_code == 401
        assert second.status_code == 429
        assert second.json()["code"] == "AUTH_RATE_LIMIT_EXCEEDED"
        assert second.headers["retry-after"] == "60"

    @pytest.mark.asyncio
    async def test_mutations_are_rate_limited_per_actor(self):
        app_mock = _make_mock_app()
        app_mock.settings.api.mutation_rate_limit_per_minute = 1
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        payload = {"symbol": "BTC", "side": "buy", "size": "0.1", "price": "69000"}
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            first = await client.post(
                "/api/v1/orders",
                headers={"Idempotency-Key": "rate-order-1"},
                json=payload,
            )
            second = await client.post(
                "/api/v1/orders",
                headers={"Idempotency-Key": "rate-order-2"},
                json=payload,
            )
        assert first.status_code == 200
        assert second.status_code == 429
        assert second.json()["code"] == "MUTATION_RATE_LIMIT_EXCEEDED"

    def test_mainnet_rejects_missing_auth_token(self):
        app_mock = _make_mock_app()
        app_mock.settings.environment = "mainnet"
        app_mock.settings.api.auth_token = ""
        with pytest.raises(RuntimeError, match="AUTH_TOKEN"):
            create_api(app_mock)

    def test_mainnet_non_loopback_rejects_missing_auth_token(self):
        app_mock = _make_mock_app()
        app_mock.settings.environment = "mainnet"
        app_mock.settings.api.host = "0.0.0.0"
        with pytest.raises(RuntimeError, match="AUTH_TOKEN"):
            create_api(app_mock)

    def test_testnet_non_loopback_allows_missing_auth_token(self):
        app_mock = _make_mock_app()
        app_mock.settings.environment = "testnet"
        app_mock.settings.api.host = "0.0.0.0"
        api_app = create_api(app_mock, cors_origins=[])
        assert api_app.state.role_tokens == ()

    @pytest.mark.asyncio
    async def test_empty_cors_list_disables_cors(self):
        app_mock = _make_mock_app()
        api_app = create_api(app_mock, cors_origins=[])
        transport = ASGITransport(app=api_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.options(
                "/api/v1/account",
                headers={"Origin": "https://attacker.example", "Access-Control-Request-Method": "GET"},
            )
        assert "access-control-allow-origin" not in response.headers

    @pytest.mark.asyncio
    async def test_viewer_can_read_but_cannot_mutate(self):
        app_mock = _make_mock_app()
        app_mock.settings.api.viewer_token = "viewer-secret"
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        headers = {"Authorization": "Bearer viewer-secret"}
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            allowed = await client.get("/api/v1/account", headers=headers)
            denied = await client.post(
                "/api/v1/orders",
                headers={**headers, "Idempotency-Key": "viewer-place-1"},
                json={"symbol": "BTC", "side": "buy", "size": "0.1", "price": "69000"},
            )
        assert allowed.status_code == 200
        assert denied.status_code == 403
        assert denied.json()["code"] == "INSUFFICIENT_ROLE"
        assert api_app.state.api_command_service.store.audits[-1].reason == "INSUFFICIENT_ROLE"

    @pytest.mark.asyncio
    async def test_operator_can_trade_but_cannot_control_kill_switch(self):
        app_mock = _make_mock_app()
        app_mock.settings.api.operator_token = "operator-secret"
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        headers = {"Authorization": "Bearer operator-secret"}
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            placed = await client.post(
                "/api/v1/orders",
                headers={**headers, "Idempotency-Key": "operator-place-1"},
                json={"symbol": "BTC", "side": "buy", "size": "0.1", "price": "69000"},
            )
            denied = await client.post(
                "/api/v1/kill-switch",
                headers={**headers, "Idempotency-Key": "operator-kill-1"},
                json={"action": "trigger", "reason": "not-authorized"},
            )
        assert placed.status_code == 200
        assert denied.status_code == 403

    @pytest.mark.asyncio
    async def test_admin_can_control_kill_switch(self):
        app_mock = _make_mock_app()
        app_mock.settings.api.admin_token = "admin-secret"
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/kill-switch",
                headers={
                    "Authorization": "Bearer admin-secret",
                    "Idempotency-Key": "admin-kill-1",
                },
                json={"action": "trigger", "reason": "authorized"},
            )
        assert response.status_code == 200
        app_mock.trigger_kill_switch.assert_awaited_once_with("authorized")

    @pytest.mark.asyncio
    async def test_api_v1_flag_removes_contract_routes(self):
        app_mock = _make_mock_app()
        app_mock.settings.features.api_v1 = False
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            health = await client.get("/health")
            missing = await client.get("/api/v1/account")
        assert health.json()["api_v1_enabled"] is False
        assert missing.status_code == 404


class TestStrategiesAPI:
    @pytest.mark.asyncio
    async def test_get_strategies(self, api_client):
        async with api_client as client:
            resp = await client.get("/api/v1/strategies")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert len(data["data"]) == 1
        assert data["data"][0]["strategy_id"] == "trend_v1"
        assert data["data"][0]["status"] == "running"

    @pytest.mark.asyncio
    async def test_start_strategy(self, api_client):
        api_client._transport.app.state.hype_app.strategy.status = StrategyStatus.STOPPED
        async with api_client as client:
            resp = await client.post(
                "/api/v1/strategies/trend_v1/start",
                headers={"Idempotency-Key": "start-strategy-1"},
            )
        assert resp.status_code == 200
        api_client._transport.app.state.hype_app.start_strategy.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_strategy(self, api_client):
        async with api_client as client:
            resp = await client.post(
                "/api/v1/strategies/trend_v1/stop",
                headers={"Idempotency-Key": "stop-strategy-1"},
            )
        assert resp.status_code == 200
        api_client._transport.app.state.hype_app.stop_strategy.assert_awaited_once()


class TestRiskAPI:
    @pytest.mark.asyncio
    async def test_get_risk_status(self, api_client):
        async with api_client as client:
            resp = await client.get("/api/v1/risk/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["kill_switch_active"] is False
        assert data["data"]["action_credits_remaining"] == 0
        assert len(data["data"]["limits"]) > 0

    @pytest.mark.asyncio
    async def test_get_risk_status_uses_shared_action_credits(self):
        app_mock = _make_mock_app()
        app_mock.action_credits_remaining = 4321
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/api/v1/risk/status")
        assert response.json()["data"]["action_credits_remaining"] == 4321

    @pytest.mark.asyncio
    async def test_kill_switch_trigger(self, api_client):
        async with api_client as client:
            resp = await client.post(
                "/api/v1/kill-switch",
                headers={"Idempotency-Key": "trigger-kill-switch-1"},
                json={
                    "action": "trigger",
                    "reason": "test",
                },
            )
        assert resp.status_code == 200
        assert resp.json()["data"]["action"] == "triggered"

    @pytest.mark.asyncio
    async def test_kill_switch_trigger_fails_before_in_memory_trigger_when_latch_is_not_durable(self):
        app_mock = _make_mock_app()
        app_mock.trigger_kill_switch = AsyncMock(return_value=False)
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/kill-switch",
                headers={"Idempotency-Key": "trigger-kill-switch-durable-failure"},
                json={"action": "trigger", "reason": "database unavailable"},
            )

        assert response.status_code == 503
        assert response.json()["code"] == "KILL_SWITCH_LATCH_NOT_DURABLE"
        app_mock.kill_switch.trigger.assert_not_called()

    @pytest.mark.asyncio
    async def test_kill_switch_reset(self, api_client):
        async with api_client as client:
            resp = await client.post(
                "/api/v1/kill-switch",
                headers={"Idempotency-Key": "reset-kill-switch-1"},
                json={"action": "reset"},
            )
        assert resp.status_code == 409
        assert resp.json()["code"] == "KILL_SWITCH_NOT_ACTIVE"

    @pytest.mark.asyncio
    async def test_kill_switch_reset_recovers_before_reporting_success(self):
        app_mock = _make_mock_app()
        app_mock.kill_switch.is_active = True
        app_mock.recover_from_kill_switch = AsyncMock(return_value=True)
        api_app = create_api(app_mock)
        transport = ASGITransport(app=api_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/kill-switch",
                headers={"Idempotency-Key": "reset-kill-switch-2"},
                json={"action": "reset"},
            )
        assert response.status_code == 200
        app_mock.recover_from_kill_switch.assert_awaited_once()


class TestStrategyTaskLifecycle:
    @pytest.mark.asyncio
    async def test_start_and_stop_manage_one_runner_task(self):
        app = HypeEdgeApp.__new__(HypeEdgeApp)
        app._trading_enabled = True
        app._kill_switch_active = False
        app._shutdown_event = asyncio.Event()
        app._tasks = []
        app._strategy_task = None
        app._strategy = MagicMock(strategy_id="trend_v1")
        runner_stopped = asyncio.Event()

        async def run_until_cancelled():
            await runner_stopped.wait()

        runner = MagicMock()
        runner.run = run_until_cancelled
        runner.stop = AsyncMock(side_effect=runner_stopped.set)
        app._strategy_runner = runner

        assert await app.start_strategy() is True
        assert await app.start_strategy() is False
        assert app._strategy_task is not None
        assert app._strategy_task in app._tasks
        assert await app.stop_strategy() is True
        runner.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_start_strategy_is_blocked_when_trading_gate_is_closed(self):
        app = HypeEdgeApp.__new__(HypeEdgeApp)
        app._trading_enabled = False
        app._kill_switch_active = False
        app._shutdown_event = asyncio.Event()
        app._tasks = []
        app._strategy_task = None
        app._strategy = MagicMock(strategy_id="trend_v1")
        app._strategy_runner = MagicMock()

        assert await app.start_strategy() is False
        assert app._tasks == []
