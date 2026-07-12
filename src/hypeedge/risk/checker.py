"""Risk checker — validates order intents against configurable limits.

Design doc §8.1: Position limits, strategy loss, account drawdown, max leverage.
Design doc §8.4: Fail-safe — timeout = rejection, data unavailable = cancel-only mode.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import structlog

from hypeedge.core.models import OrderIntent, RiskCheckResult

if TYPE_CHECKING:
    from hypeedge.account.tracker import AccountTracker

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class RiskLimits:
    """Configurable risk limits (design doc §8.1)."""

    max_position_pct: float = 0.20  # Max position as % of equity
    max_strategy_loss_pct: float = 0.05  # Max loss per strategy as % of equity
    max_drawdown_pct: float = 0.10  # Max total drawdown from peak (triggers shutdown)
    max_leverage: int = 5  # Max effective leverage
    timeout_ms: int = 500  # Risk check timeout (fail-safe)
    account_stale_seconds: float = 360.0  # Must exceed the current 5-minute reconciliation cadence.


class RiskChecker:
    """Risk checker with real AccountTracker integration.

    Design doc §8.4: "The risk module must return a clear result (pass/reject)
    within its own timeout window; timeout is treated as rejection."

    Checks (in order):
    1. Max drawdown — reject if current drawdown >= max_drawdown_pct
    2. Max leverage — reject if post-order leverage > max_leverage
    3. Max position per coin — reject if order would exceed max_position_pct of equity
    4. Account state freshness — reject if account state is stale

    Fail-safe behavior:
    - Timeout → reject
    - Exception → reject
    - Missing data → reject (fail-closed)
    """

    def __init__(
        self,
        tracker: AccountTracker,
        limits: RiskLimits | None = None,
    ) -> None:
        self._tracker = tracker
        self._limits = limits or RiskLimits()
        self._check_count = 0
        self._reject_count = 0
        self._strategy_realized_pnl: dict[str, float] = {}  # strategy_id -> cumulative PnL

    def record_strategy_pnl(self, strategy_id: str, pnl: float) -> None:
        """Record a realized PnL for a strategy (called by execution on fills)."""
        self._strategy_realized_pnl[strategy_id] = self._strategy_realized_pnl.get(strategy_id, 0.0) + pnl

    @property
    def strategy_pnl(self) -> dict[str, float]:
        return dict(self._strategy_realized_pnl)

    @property
    def limits(self) -> RiskLimits:
        return self._limits

    @property
    def stats(self) -> dict[str, int]:
        return {
            "check_count": self._check_count,
            "reject_count": self._reject_count,
            "pass_count": self._check_count - self._reject_count,
        }

    async def check(self, intent: OrderIntent, *, reference_price: float | None = None) -> RiskCheckResult:
        """Run all risk checks with timeout. Fail-safe on timeout."""
        self._check_count += 1
        try:
            result = await asyncio.wait_for(
                self._run_checks(intent, reference_price=reference_price),
                timeout=self._limits.timeout_ms / 1000.0,
            )
            if not result.passed:
                self._reject_count += 1
            return result
        except TimeoutError:
            self._reject_count += 1
            logger.error("risk_check_timeout", symbol=str(intent.symbol))
            return RiskCheckResult(passed=False, reason="risk_check_timeout")
        except Exception as e:
            self._reject_count += 1
            logger.exception("risk_check_error", error=str(e))
            return RiskCheckResult(passed=False, reason=f"risk_check_error: {e}")

    async def _run_checks(self, intent: OrderIntent, *, reference_price: float | None = None) -> RiskCheckResult:
        """Execute all risk checks sequentially."""
        checked: list[str] = []

        # Check 0: Account state must exist
        account = self._tracker.get_account_state()
        if account is None:
            checked.append("account_state_missing")
            return RiskCheckResult(
                passed=False,
                reason="account_state_not_available",
                checked_limits=checked,
            )
        checked.append("account_state_available")

        checked.append("account_state_fresh")
        last_update = self._tracker.last_update_ts
        if (
            last_update is None
            or (datetime.now(UTC) - last_update).total_seconds() > self._limits.account_stale_seconds
        ):
            return RiskCheckResult(
                passed=False,
                reason="account_state_stale",
                checked_limits=checked,
            )

        existing_pos = self._tracker.get_position(intent.symbol)
        effective_reference_price = (
            float(intent.price)
            if intent.price
            else reference_price
            if reference_price is not None
            else (float(existing_pos.mark_price) if existing_pos and existing_pos.mark_price else 0.0)
        )
        if effective_reference_price <= 0:
            return RiskCheckResult(
                passed=False,
                reason="market_price_not_available",
                checked_limits=checked,
            )

        existing_size = float(existing_pos.size) if existing_pos else 0.0
        signed_delta = float(intent.size) if intent.side.value == "buy" else -float(intent.size)
        resulting_size = existing_size + signed_delta
        if intent.reduce_only:
            reduces_position = existing_size != 0 and abs(resulting_size) < abs(existing_size)
            does_not_flip = resulting_size == 0 or (resulting_size > 0) == (existing_size > 0)
            if not reduces_position or not does_not_flip:
                return RiskCheckResult(
                    passed=False,
                    reason="invalid_reduce_only_order",
                    checked_limits=checked,
                )

        # Check 1: Max drawdown from peak
        checked.append("max_drawdown")
        if account.drawdown_pct >= self._limits.max_drawdown_pct:
            return RiskCheckResult(
                passed=False,
                reason=f"drawdown_exceeded: {account.drawdown_pct:.4f} >= {self._limits.max_drawdown_pct}",
                checked_limits=checked,
            )

        # Check 2: Max leverage (estimate post-order leverage)
        checked.append("max_leverage")
        equity = account.equity
        if equity > 0:
            # Current position value + new order value
            current_pos_value = float(self._tracker.get_total_position_value())
            existing_symbol_value = abs(existing_size) * effective_reference_price
            resulting_symbol_value = abs(resulting_size) * effective_reference_price
            estimated_leverage = (current_pos_value - existing_symbol_value + resulting_symbol_value) / equity
            if estimated_leverage > self._limits.max_leverage:
                return RiskCheckResult(
                    passed=False,
                    reason=f"leverage_exceeded: {estimated_leverage:.2f} > {self._limits.max_leverage}",
                    checked_limits=checked,
                )

        # Check 3: Max position per coin
        checked.append("max_position_pct")
        if equity > 0:
            total_position_value = abs(resulting_size) * effective_reference_price
            position_pct = total_position_value / equity

            if position_pct > self._limits.max_position_pct:
                return RiskCheckResult(
                    passed=False,
                    reason=f"position_pct_exceeded: {position_pct:.4f} > {self._limits.max_position_pct}",
                    checked_limits=checked,
                )

        # All checks passed
        # Check 4: Per-strategy max loss (design doc §8.1)
        strategy_id = str(intent.strategy_id) if intent.strategy_id else None
        if strategy_id and equity > 0:
            checked.append("max_strategy_loss")
            strategy_pnl = self._strategy_realized_pnl.get(strategy_id, 0.0)
            # Check if strategy has lost more than max_strategy_loss_pct of equity
            if strategy_pnl < 0 and abs(strategy_pnl) > equity * self._limits.max_strategy_loss_pct:
                return RiskCheckResult(
                    passed=False,
                    reason=(
                        f"strategy_loss_exceeded: {strategy_id} "
                        f"lost {abs(strategy_pnl):.2f} "
                        f"> {self._limits.max_strategy_loss_pct:.4f} * equity"
                    ),
                    checked_limits=checked,
                )

        return RiskCheckResult(passed=True, checked_limits=checked)
