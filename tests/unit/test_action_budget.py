"""Boundary tests for the market-making action budget controller."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from hypeedge.config.settings import ActionBudgetSettings
from hypeedge.core.enums import ActionBudgetMode
from hypeedge.core.types import StrategyId, Symbol, Usd
from hypeedge.risk.action_budget import (
    ActionBudgetController,
    ActionBudgetRecoveryState,
    BudgetAction,
    BudgetAllocation,
    CancelHeadroomSnapshot,
    NetworkAttemptDebit,
    RemoteActionSnapshot,
)

OWNER = "0x1111111111111111111111111111111111111111"
STRATEGY = StrategyId("mm_btc")
BTC = Symbol("BTC")


class MutableClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def make_controller(
    *,
    now: datetime | None = None,
    settings: ActionBudgetSettings | None = None,
    cap: int = 10_000,
    used: int = 0,
    cancel_cap: int = 10_000,
    cancel_used: int = 0,
) -> tuple[ActionBudgetController, MutableClock]:
    current = now or datetime(2026, 1, 1, tzinfo=UTC)
    clock = MutableClock(current)
    controller = ActionBudgetController(OWNER, settings or ActionBudgetSettings(), clock=clock)
    controller.reconcile_remote(RemoteActionSnapshot(OWNER, cap, used, current))
    controller.reconcile_cancel_headroom(CancelHeadroomSnapshot(cancel_cap, cancel_used, current))
    return controller, clock


def debit(
    attempt_id: str,
    occurred_at: datetime,
    *actions: BudgetAction,
    ip_weight: int = 1,
    strategy_id: StrategyId | None = None,
    symbol: Symbol | None = None,
) -> NetworkAttemptDebit:
    return NetworkAttemptDebit(
        attempt_id=attempt_id,
        child_actions=actions,
        ip_weight=ip_weight,
        occurred_at=occurred_at,
        strategy_id=strategy_id,
        symbol=symbol,
    )


class TestActionBudgetController:
    def test_starts_fail_closed_until_both_remote_snapshots_exist(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=UTC)
        controller = ActionBudgetController(OWNER, ActionBudgetSettings(), clock=lambda: now)

        assert controller.mode == ActionBudgetMode.CANCEL_ONLY
        controller.reconcile_remote(RemoteActionSnapshot(OWNER, 10_000, 0, now))
        assert controller.mode == ActionBudgetMode.CANCEL_ONLY

    def test_batch_child_actions_and_ip_request_weight_are_separate(self) -> None:
        controller, clock = make_controller()
        clock.now += timedelta(seconds=1)
        attempt = debit(
            "batch-1",
            clock.now,
            BudgetAction.PLACE,
            BudgetAction.PLACE,
            BudgetAction.CANCEL,
            ip_weight=1,
        )

        assert controller.debit_network_attempt(attempt) is True
        view = controller.snapshot()
        assert view.address_remaining == 9_997
        assert view.cancel_headroom_remaining == 9_999
        assert view.ip_weight_remaining == 1_199

    def test_network_attempt_is_shadow_debited_exactly_once_even_after_timeout(self) -> None:
        controller, clock = make_controller()
        clock.now += timedelta(seconds=1)
        attempt = debit("command-item-7-attempt-1", clock.now, BudgetAction.PLACE, ip_weight=1)

        assert controller.debit_network_attempt(attempt) is True
        assert controller.debit_network_attempt(attempt) is False
        assert controller.snapshot().address_remaining == 9_999

        conflicting = debit("command-item-7-attempt-1", clock.now, BudgetAction.CANCEL, ip_weight=1)
        with pytest.raises(ValueError, match="reused"):
            controller.debit_network_attempt(conflicting)

    def test_new_remote_snapshot_corrects_already_observed_shadow_debits(self) -> None:
        controller, clock = make_controller()
        clock.now += timedelta(seconds=1)
        controller.debit_network_attempt(
            debit("batch-1", clock.now, BudgetAction.PLACE, BudgetAction.CANCEL, ip_weight=1)
        )
        assert controller.snapshot().address_remaining == 9_998

        clock.now += timedelta(seconds=1)
        controller.reconcile_remote(RemoteActionSnapshot(OWNER, 10_000, 2, clock.now))

        assert controller.snapshot().address_remaining == 9_998

    def test_dynamic_cancel_reserve_is_scope_level_not_per_allocation(self) -> None:
        controller, _ = make_controller()
        controller.update_possible_live_orders(7)
        controller.set_allocation(BudgetAllocation(STRATEGY, BTC, soft_limit=100, hard_limit=200))
        controller.set_allocation(BudgetAllocation(StrategyId("mm_eth"), Symbol("ETH"), soft_limit=100, hard_limit=200))

        view = controller.snapshot()
        assert view.required_cancel_reserve == 17
        assert view.placement_actions_available == 10_000 - 17 - 5

    def test_cancel_is_never_blocked_by_stale_or_exhausted_placement_budget(self) -> None:
        settings = ActionBudgetSettings(remote_snapshot_max_age_seconds=5)
        controller, clock = make_controller(settings=settings, cap=10, used=10, cancel_cap=1, cancel_used=1)
        clock.now += timedelta(seconds=10)

        assert controller.mode == ActionBudgetMode.CANCEL_ONLY
        permission = controller.permission(BudgetAction.CANCEL, child_actions=500, ip_weight=50_000)
        assert permission.allowed is True

    @pytest.mark.parametrize(
        ("used", "expected"),
        [
            (0, ActionBudgetMode.NORMAL),
            (6_500, ActionBudgetMode.CONSERVE),
            (8_000, ActionBudgetMode.CRITICAL),
            (9_000, ActionBudgetMode.CANCEL_ONLY),
            (10_000, ActionBudgetMode.EXHAUSTED),
        ],
    )
    def test_address_threshold_modes(self, used: int, expected: ActionBudgetMode) -> None:
        settings = ActionBudgetSettings(
            cancel_retry_buffer=0,
            close_action_reserve=0,
            address_conserve_threshold=4_000,
            address_critical_threshold=2_000,
            address_cancel_only_threshold=1_000,
        )
        controller, _ = make_controller(settings=settings, used=used)
        assert controller.mode == expected

    def test_cancel_headroom_degrades_before_cancel_capacity_is_spent(self) -> None:
        controller, _ = make_controller(cancel_cap=100, cancel_used=90)
        controller.update_possible_live_orders(2)

        assert controller.required_cancel_reserve == 12
        assert controller.mode == ActionBudgetMode.CANCEL_ONLY

    def test_ip_emergency_reserve_is_independent_and_sliding(self) -> None:
        settings = ActionBudgetSettings(remote_snapshot_max_age_seconds=180)
        controller, clock = make_controller(settings=settings)
        clock.now += timedelta(seconds=1)
        controller.debit_network_attempt(debit("heavy-info", clock.now, BudgetAction.PLACE, ip_weight=1_101))

        assert controller.snapshot().ip_weight_remaining == 99
        assert controller.mode == ActionBudgetMode.CANCEL_ONLY
        assert controller.ip_request_permission(50).allowed is False
        assert controller.ip_request_permission(50, emergency=True).allowed is True

        clock.now += timedelta(seconds=61)
        assert controller.snapshot().ip_weight_remaining == 1_200
        assert controller.mode == ActionBudgetMode.NORMAL

    def test_info_request_debits_only_ip_weight(self) -> None:
        controller, clock = make_controller()
        clock.now += timedelta(seconds=1)
        controller.debit_network_attempt(debit("order-status", clock.now, ip_weight=2))

        view = controller.snapshot()
        assert view.address_remaining == 10_000
        assert view.cancel_headroom_remaining == 10_000
        assert view.ip_weight_remaining == 1_198

    def test_allocation_hard_limit_gates_placement_but_not_cancel(self) -> None:
        controller, clock = make_controller()
        controller.set_allocation(BudgetAllocation(STRATEGY, BTC, soft_limit=1, hard_limit=2))
        clock.now += timedelta(seconds=1)
        controller.debit_network_attempt(
            debit(
                "owned-batch",
                clock.now,
                BudgetAction.PLACE,
                BudgetAction.CANCEL,
                strategy_id=STRATEGY,
                symbol=BTC,
            )
        )

        assert controller.permission(BudgetAction.PLACE, strategy_id=STRATEGY, symbol=BTC).allowed is False
        assert controller.permission(BudgetAction.CANCEL, strategy_id=STRATEGY, symbol=BTC).allowed is True

    def test_burn_earn_runway_and_marginal_usdc_per_action(self) -> None:
        settings = ActionBudgetSettings(
            minimum_actions_for_economic_gate=2,
            address_conserve_threshold=0,
            address_critical_threshold=0,
            address_cancel_only_threshold=0,
        )
        controller, clock = make_controller(settings=settings, cap=100_000)
        clock.now += timedelta(seconds=1)
        controller.debit_network_attempt(
            debit("two-actions", clock.now, BudgetAction.PLACE, BudgetAction.CANCEL, ip_weight=1)
        )
        controller.record_fill(Usd("2"), occurred_at=clock.now)

        one_hour = controller.snapshot().windows[0]
        assert one_hour.burned_actions == 2
        assert one_hour.earned_actions == 2
        assert one_hour.actions_per_fill == 2
        assert one_hour.marginal_usdc_per_action == 1
        assert one_hour.runway_hours == pytest.approx(float("inf"))
        assert controller.mode == ActionBudgetMode.CONSERVE

    def test_restore_replays_only_post_snapshot_durable_attempts(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=UTC)
        controller = ActionBudgetController(OWNER, ActionBudgetSettings(), clock=lambda: now + timedelta(seconds=2))
        remote = RemoteActionSnapshot(OWNER, 10_000, 100, now)
        cancel = CancelHeadroomSnapshot(10_000, 50, now)
        state = ActionBudgetRecoveryState(
            remote_snapshot=remote,
            cancel_snapshot=cancel,
            attempts_after_snapshot=(debit("post-restart", now + timedelta(seconds=1), BudgetAction.PLACE),),
        )

        assert controller.restore(state) is True
        assert controller.snapshot().address_remaining == 9_899
        assert controller.snapshot().restored_conservatively is True

        invalid = ActionBudgetRecoveryState(
            remote_snapshot=remote,
            cancel_snapshot=cancel,
            attempts_after_snapshot=(debit("old", now - timedelta(seconds=1), BudgetAction.PLACE),),
        )
        assert controller.restore(invalid) is False
        assert controller.mode == ActionBudgetMode.CANCEL_ONLY

    def test_paid_reserve_is_disabled_by_default_and_hard_capped_when_enabled(self) -> None:
        controller, clock = make_controller()
        assert controller.paid_reserve_permission(1, purpose="unknown_recovery", admin_authorized=True).allowed is False

        enabled = ActionBudgetSettings(
            paid_reserve_enabled=True,
            paid_reserve_max_single_usdc=0.001,
            paid_reserve_max_daily_usdc=0.001,
            paid_reserve_max_monthly_usdc=0.002,
        )
        controller, clock = make_controller(settings=enabled)
        assert controller.record_paid_reserve(
            2,
            purpose="unknown_recovery",
            admin_authorized=True,
            now=clock.now,
        ) == Usd("0.0010")
        assert (
            controller.paid_reserve_permission(
                1,
                purpose="unknown_recovery",
                admin_authorized=True,
                now=clock.now,
            ).allowed
            is False
        )


def test_user_rate_limit_snapshot_parser_and_owner_scope() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    snapshot = RemoteActionSnapshot.from_user_rate_limit(
        OWNER.upper().replace("0X", "0x"),
        {"nRequestsCap": "12000", "nRequestsUsed": 345},
        observed_at=now,
    )
    assert snapshot.quota_owner_address == OWNER
    assert snapshot.remaining == 11_655


def test_budget_settings_reject_misordered_thresholds() -> None:
    with pytest.raises(ValueError, match="cancel_only <= critical <= conserve"):
        ActionBudgetSettings(
            address_conserve_threshold=100,
            address_critical_threshold=200,
            address_cancel_only_threshold=50,
        )
