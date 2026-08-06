"""Funding-rate arbitrage parameters (single-venue delta-neutral skeleton).

Mirrors the typed Postgres config (``funding_arb_config_versions``). The shape is
HL perpetual short (collects funding) delta-hedged by HL spot long on the same
venue; see ``docs/funding_arb_design.md`` and ``docs/design.md`` §7.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from hypeedge.core.constants import AUTO_MARKET_SYMBOL, AUTO_SPOT_MARKET
from hypeedge.core.enums import FundingArbCycleState


@dataclass(frozen=True)
class FundingArbParams:
    """Funding-rate arbitrage parameters.

    Persisted, validated and surfaced in the UI, then consumed by the
    testnet-only fill-aware two-leg runtime.
    """

    entry_funding_rate: Decimal = Decimal("0.0001")
    exit_funding_rate: Decimal = Decimal("0")
    max_notional_usd: Decimal = Decimal("1000")
    hedge_ratio: Decimal = Decimal("1")
    rebalance_threshold_bps: int = 50
    leverage: Decimal = Decimal("1")
    max_slippage_bps: int = 50
    max_basis_bps: int = 500
    min_expected_edge_bps: Decimal = Decimal("5")
    expected_hold_hours: int = 8
    round_trip_fee_bps: Decimal = Decimal("20")
    max_unhedged_seconds: int = 15

    def __post_init__(self) -> None:
        """Validate parameter constraints (mirror Postgres CHECKs)."""
        errors: list[str] = []
        if self.entry_funding_rate <= 0:
            errors.append(f"entry_funding_rate must be > 0, got {self.entry_funding_rate}")
        if self.exit_funding_rate < 0:
            errors.append(f"exit_funding_rate must be >= 0, got {self.exit_funding_rate}")
        if self.exit_funding_rate >= self.entry_funding_rate:
            errors.append(
                "exit_funding_rate must be < entry_funding_rate, "
                f"got exit={self.exit_funding_rate} entry={self.entry_funding_rate}"
            )
        if self.max_notional_usd <= 0:
            errors.append(f"max_notional_usd must be > 0, got {self.max_notional_usd}")
        if not (Decimal("0") < self.hedge_ratio <= Decimal("1")):
            errors.append(f"hedge_ratio must be in (0, 1], got {self.hedge_ratio}")
        if self.rebalance_threshold_bps <= 0:
            errors.append(f"rebalance_threshold_bps must be > 0, got {self.rebalance_threshold_bps}")
        if self.leverage <= 0:
            errors.append(f"leverage must be > 0, got {self.leverage}")
        if self.leverage != self.leverage.to_integral_value():
            errors.append(f"leverage must be an integer, got {self.leverage}")
        if not 1 <= self.max_slippage_bps <= 500:
            errors.append(f"max_slippage_bps must be in [1, 500], got {self.max_slippage_bps}")
        if self.max_basis_bps <= 0:
            errors.append(f"max_basis_bps must be > 0, got {self.max_basis_bps}")
        if self.min_expected_edge_bps < 0:
            errors.append(f"min_expected_edge_bps must be >= 0, got {self.min_expected_edge_bps}")
        if not 1 <= self.expected_hold_hours <= 168:
            errors.append(f"expected_hold_hours must be in [1, 168], got {self.expected_hold_hours}")
        if self.round_trip_fee_bps < 0:
            errors.append(f"round_trip_fee_bps must be >= 0, got {self.round_trip_fee_bps}")
        if not 1 <= self.max_unhedged_seconds <= 60:
            errors.append(f"max_unhedged_seconds must be in [1, 60], got {self.max_unhedged_seconds}")
        if errors:
            raise ValueError("; ".join(errors))


@dataclass(frozen=True, slots=True)
class FundingArbCycle:
    """Durable state of one spot/perpetual hedge lifecycle."""

    cycle_id: UUID
    strategy_id: str
    config_revision: int
    sub_account: str
    perp_symbol: str
    spot_symbol: str
    spot_display: str
    base_token: str
    quote_token: str
    state: FundingArbCycleState
    target_perp_size: Decimal
    target_spot_size: Decimal
    perp_open_size: Decimal
    spot_open_size: Decimal
    baseline_spot_size: Decimal
    entry_funding_rate: Decimal
    entry_basis_bps: Decimal
    revision: int
    spot_entry_cloid: str | None = None
    perp_entry_cloid: str | None = None
    compensation_cloid: str | None = None
    perp_exit_cloid: str | None = None
    spot_exit_cloid: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    opened_at: datetime | None = None
    closed_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


__all__ = ["AUTO_MARKET_SYMBOL", "AUTO_SPOT_MARKET", "FundingArbCycle", "FundingArbParams"]
