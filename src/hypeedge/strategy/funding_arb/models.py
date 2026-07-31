"""Funding-rate arbitrage parameters (single-venue delta-neutral skeleton).

Mirrors the typed Postgres config (``funding_arb_config_versions``). The shape is
HL perpetual short (collects funding) delta-hedged by HL spot long on the same
venue; see ``docs/funding_arb_design.md`` and ``docs/design.md`` §7.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class FundingArbParams:
    """Funding-rate arbitrage parameters.

    Skeleton values only: the stub runtime does not trade on them yet. They are
    persisted, validated and surfaced in the UI; real delta-neutral execution
    (perp short + spot long, rebalancing, funding accrual) is a later phase.
    """

    spot_coin: str = "PURR"
    entry_funding_rate: Decimal = Decimal("0.0001")
    exit_funding_rate: Decimal = Decimal("0")
    max_notional_usd: Decimal = Decimal("1000")
    hedge_ratio: Decimal = Decimal("1")
    rebalance_threshold_bps: int = 50
    leverage: Decimal = Decimal("1")

    def __post_init__(self) -> None:
        """Validate parameter constraints (mirror Postgres CHECKs)."""
        errors: list[str] = []
        if not self.spot_coin.strip():
            errors.append("spot_coin must be a non-empty string")
        if self.entry_funding_rate < 0:
            errors.append(f"entry_funding_rate must be >= 0, got {self.entry_funding_rate}")
        if self.exit_funding_rate < 0:
            errors.append(f"exit_funding_rate must be >= 0, got {self.exit_funding_rate}")
        if self.max_notional_usd <= 0:
            errors.append(f"max_notional_usd must be > 0, got {self.max_notional_usd}")
        if not (Decimal("0") < self.hedge_ratio <= Decimal("1")):
            errors.append(f"hedge_ratio must be in (0, 1], got {self.hedge_ratio}")
        if self.rebalance_threshold_bps <= 0:
            errors.append(f"rebalance_threshold_bps must be > 0, got {self.rebalance_threshold_bps}")
        if self.leverage <= 0:
            errors.append(f"leverage must be > 0, got {self.leverage}")
        if errors:
            raise ValueError("; ".join(errors))
