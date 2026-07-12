"""Tests for layered account freshness and adaptive state polling."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from hypeedge.account.health import (
    AccountFreshnessThresholds,
    AccountHealthDimension,
    AccountStatePoller,
    FreshnessStatus,
    LayeredAccountHealthProvider,
    PolledAccountSnapshot,
    RestAccountStateSource,
)
from hypeedge.account.tracker import AccountTracker
from hypeedge.core.models import AccountState, Position
from hypeedge.core.types import Price, Size, Symbol, Usd


def _account_state(*, equity: float = 1_000.0, available: float = 800.0) -> AccountState:
    return AccountState(
        equity=Usd(equity),
        available_balance=Usd(available),
        total_margin_used=Usd(equity - available),
        total_unrealized_pnl=Usd(0.0),
        peak_equity=Usd(equity),
    )


def _mark_all_fresh(provider: LayeredAccountHealthProvider, observed_at: datetime) -> None:
    for dimension in AccountHealthDimension:
        provider.record_success(dimension, observed_at=observed_at)


class StaticAccountStateSource:
    def __init__(self, snapshot: PolledAccountSnapshot | None = None, error: Exception | None = None) -> None:
        self.snapshot = snapshot
        self.error = error

    async def fetch_account_state(self) -> PolledAccountSnapshot:
        if self.error is not None:
            raise self.error
        assert self.snapshot is not None
        return self.snapshot


class StaticClearinghouseClient:
    def __init__(self, response: dict[str, object]) -> None:
        self.response = response

    async def get_clearinghouse_state(self, user: str) -> dict[str, object]:
        assert user == "0xaccount"
        return self.response


class TestLayeredAccountHealthProvider:
    def test_all_dimensions_fresh_allow_risk(self) -> None:
        now = datetime(2026, 7, 11, tzinfo=UTC)
        provider = LayeredAccountHealthProvider()
        _mark_all_fresh(provider, now)

        health = provider.get_account_health(now=now)

        assert health.allows_risk_increase is True
        assert health.requires_cancel is False
        assert health.blocking_reasons == ()

    def test_unknown_dimensions_fail_closed(self) -> None:
        provider = LayeredAccountHealthProvider()

        health = provider.get_account_health(now=datetime(2026, 7, 11, tzinfo=UTC))

        assert health.allows_risk_increase is False
        assert health.requires_cancel is True
        assert all(item.status == FreshnessStatus.UNKNOWN for item in health.dimensions)

    def test_dimensions_age_independently(self) -> None:
        start = datetime(2026, 7, 11, tzinfo=UTC)
        provider = LayeredAccountHealthProvider(
            AccountFreshnessThresholds(
                inventory=timedelta(seconds=2),
                clearinghouse=timedelta(seconds=5),
                user_stream=timedelta(seconds=5),
                reconciliation=timedelta(seconds=30),
            )
        )
        _mark_all_fresh(provider, start)

        health = provider.get_account_health(now=start + timedelta(seconds=3))

        assert health.inventory.status == FreshnessStatus.STALE
        assert health.clearinghouse.status == FreshnessStatus.FRESH
        assert health.user_stream.status == FreshnessStatus.FRESH
        assert health.reconciliation.status == FreshnessStatus.FRESH
        assert health.blocking_reasons == ("inventory:observation_stale",)

    def test_explicit_stream_failure_is_immediately_unhealthy(self) -> None:
        now = datetime(2026, 7, 11, tzinfo=UTC)
        provider = LayeredAccountHealthProvider()
        _mark_all_fresh(provider, now)
        provider.record_failure(
            AccountHealthDimension.USER_STREAM, "authenticated_stream_disconnected", observed_at=now
        )

        health = provider.get_account_health(now=now)

        assert health.user_stream.status == FreshnessStatus.UNHEALTHY
        assert health.user_stream.reason == "authenticated_stream_disconnected"
        assert health.requires_cancel is True

    def test_future_timestamp_beyond_clock_skew_is_unhealthy(self) -> None:
        now = datetime(2026, 7, 11, tzinfo=UTC)
        provider = LayeredAccountHealthProvider()
        _mark_all_fresh(provider, now)
        provider.record_success(AccountHealthDimension.CLEARINGHOUSE, observed_at=now + timedelta(seconds=2))

        health = provider.get_account_health(now=now)

        assert health.clearinghouse.status == FreshnessStatus.UNHEALTHY
        assert health.clearinghouse.reason == "observed_at_in_future"

    def test_rejects_invalid_thresholds_and_naive_timestamps(self) -> None:
        with pytest.raises(ValueError, match="inventory freshness threshold"):
            AccountFreshnessThresholds(inventory=timedelta(0))
        with pytest.raises(ValueError, match="max_future_skew"):
            AccountFreshnessThresholds(max_future_skew=timedelta(seconds=-1))

        provider = LayeredAccountHealthProvider()
        with pytest.raises(ValueError, match="timezone-aware"):
            provider.record_success(AccountHealthDimension.INVENTORY, observed_at=datetime(2026, 7, 11))
        with pytest.raises(ValueError, match="must not be empty"):
            provider.record_failure(AccountHealthDimension.INVENTORY, "")


class TestAccountStatePoller:
    async def test_poll_updates_clearinghouse_without_masking_stream_freshness(self) -> None:
        now = datetime.now(UTC)
        tracker = AccountTracker()
        tracker.update_position_from_exchange(Symbol("ETH"), Position(Symbol("ETH"), Size(2.0)))
        position = Position(
            symbol=Symbol("BTC"),
            size=Size(0.1),
            mark_price=Price(50_000.0),
            liquidation_price=Price(30_000.0),
        )
        snapshot = PolledAccountSnapshot(_account_state(), (position,), now)
        provider = LayeredAccountHealthProvider()
        poller = AccountStatePoller(StaticAccountStateSource(snapshot), tracker, provider)

        next_interval = await poller.poll_once()

        assert next_interval == 3.0
        assert tracker.get_position(Symbol("BTC")) == position
        assert tracker.get_position(Symbol("ETH")) is None
        health = provider.get_account_health(now=now)
        assert health.clearinghouse.status == FreshnessStatus.FRESH
        assert health.inventory.status == FreshnessStatus.UNKNOWN
        assert health.allows_risk_increase is False

    async def test_poll_accelerates_near_margin_risk(self) -> None:
        now = datetime.now(UTC)
        snapshot = PolledAccountSnapshot(_account_state(available=200.0), (), now)
        poller = AccountStatePoller(
            StaticAccountStateSource(snapshot),
            AccountTracker(),
            LayeredAccountHealthProvider(),
            normal_interval_seconds=4.0,
            near_risk_interval_seconds=0.75,
        )

        assert await poller.poll_once() == 0.75

    async def test_poll_accelerates_near_liquidation(self) -> None:
        now = datetime.now(UTC)
        position = Position(
            symbol=Symbol("BTC"),
            size=Size(0.1),
            mark_price=Price(100.0),
            liquidation_price=Price(92.0),
        )
        snapshot = PolledAccountSnapshot(_account_state(), (position,), now)
        poller = AccountStatePoller(
            StaticAccountStateSource(snapshot),
            AccountTracker(),
            LayeredAccountHealthProvider(),
            near_risk_interval_seconds=1.25,
        )

        assert await poller.poll_once() == 1.25

    async def test_poll_failure_marks_only_clearinghouse_and_requests_degradation(self) -> None:
        now = datetime.now(UTC)
        provider = LayeredAccountHealthProvider()
        _mark_all_fresh(provider, now)
        failures: list[str] = []

        async def on_failure(reason: str) -> None:
            failures.append(reason)

        poller = AccountStatePoller(
            StaticAccountStateSource(error=OSError("network down")),
            AccountTracker(),
            provider,
            near_risk_interval_seconds=1.0,
            on_health_failure=on_failure,
        )

        assert await poller.poll_once() == 1.0
        health = provider.get_account_health()
        assert health.clearinghouse.status == FreshnessStatus.UNHEALTHY
        assert health.inventory.status == FreshnessStatus.FRESH
        assert failures == ["clearinghouse_poll_failed:OSError"]

    async def test_run_polls_immediately_and_stops_interruptibly(self) -> None:
        now = datetime.now(UTC)
        snapshot = PolledAccountSnapshot(_account_state(), (), now)
        poller = AccountStatePoller(
            StaticAccountStateSource(snapshot),
            AccountTracker(),
            LayeredAccountHealthProvider(),
        )

        task = asyncio.create_task(poller.run())
        for _ in range(10):
            if poller.is_running:
                break
            await asyncio.sleep(0)
        assert poller.is_running is True
        with pytest.raises(RuntimeError, match="already running"):
            await poller.run()
        await poller.stop()
        await task
        assert poller.is_running is False

    def test_rejects_poll_intervals_outside_design_envelope(self) -> None:
        source = StaticAccountStateSource()
        tracker = AccountTracker()
        provider = LayeredAccountHealthProvider()
        with pytest.raises(ValueError, match="between 2 and 5"):
            AccountStatePoller(source, tracker, provider, normal_interval_seconds=1.0)
        with pytest.raises(ValueError, match="between 0.5 and 2"):
            AccountStatePoller(source, tracker, provider, near_risk_interval_seconds=0.25)
        with pytest.raises(ValueError, match="must be lower"):
            AccountStatePoller(
                source,
                tracker,
                provider,
                normal_interval_seconds=2.0,
                near_risk_interval_seconds=2.0,
            )

    async def test_flat_position_and_zero_equity_use_near_risk_cadence(self) -> None:
        now = datetime.now(UTC)
        tracker = AccountTracker()
        tracker.update_position_from_exchange(Symbol("BTC"), Position(Symbol("BTC"), Size(1.0)))
        flat = Position(Symbol("BTC"), Size(0.0))
        snapshot = PolledAccountSnapshot(_account_state(equity=0.0, available=0.0), (flat,), now)
        poller = AccountStatePoller(
            StaticAccountStateSource(snapshot),
            tracker,
            LayeredAccountHealthProvider(),
            near_risk_interval_seconds=0.5,
        )

        assert await poller.poll_once() == 0.5
        assert tracker.get_position(Symbol("BTC")) is None


class TestRestAccountStateSource:
    async def test_parses_account_positions_and_liquidation_state(self) -> None:
        tracker = AccountTracker()
        response: dict[str, object] = {
            "withdrawable": "750",
            "marginSummary": {"accountValue": "1000", "totalMarginUsed": "250"},
            "assetPositions": [
                {
                    "position": {
                        "coin": "BTC",
                        "szi": "0.1",
                        "entryPx": "49000",
                        "positionValue": "5000",
                        "unrealizedPnl": "100",
                        "liquidationPx": "30000",
                        "leverage": {"value": "2"},
                    }
                }
            ],
        }
        source = RestAccountStateSource(StaticClearinghouseClient(response), "0xaccount", tracker)

        snapshot = await source.fetch_account_state()

        assert snapshot.account_state.equity == Usd(1_000.0)
        assert snapshot.account_state.available_balance == Usd(750.0)
        assert snapshot.account_state.total_unrealized_pnl == Usd(100.0)
        assert snapshot.positions[0].mark_price == Price(50_000.0)
        assert snapshot.positions[0].liquidation_price == Price(30_000.0)
        assert snapshot.positions[0].leverage == 2

    async def test_invalid_clearinghouse_response_fails_closed(self) -> None:
        source = RestAccountStateSource(StaticClearinghouseClient({"marginSummary": {}}), "0xaccount", AccountTracker())

        with pytest.raises(ValueError, match="invalid_clearinghouse_state_response"):
            await source.fetch_account_state()

    async def test_invalid_position_numeric_field_is_rejected(self) -> None:
        response: dict[str, object] = {
            "marginSummary": {"accountValue": "1000", "totalMarginAvailable": "900"},
            "assetPositions": [{"position": {"coin": "BTC", "szi": "not-a-number"}}],
        }
        source = RestAccountStateSource(StaticClearinghouseClient(response), "0xaccount", AccountTracker())

        with pytest.raises(ValueError, match="invalid numeric field: szi"):
            await source.fetch_account_state()

    def test_requires_account_address(self) -> None:
        with pytest.raises(ValueError, match="account_address"):
            RestAccountStateSource(StaticClearinghouseClient({}), "", AccountTracker())

    async def test_invalid_position_shape_and_coin_are_rejected(self) -> None:
        base: dict[str, object] = {
            "marginSummary": {"accountValue": "1000", "totalMarginAvailable": "900"},
            "assetPositions": ["invalid"],
        }
        with pytest.raises(ValueError, match="invalid_asset_position"):
            await RestAccountStateSource(
                StaticClearinghouseClient(base), "0xaccount", AccountTracker()
            ).fetch_account_state()

        base["assetPositions"] = [{"position": {"coin": "", "szi": "0"}}]
        with pytest.raises(ValueError, match="asset_position_missing_coin"):
            await RestAccountStateSource(
                StaticClearinghouseClient(base), "0xaccount", AccountTracker()
            ).fetch_account_state()
