"""Funding-rate arbitrage strategy (single-venue delta-neutral skeleton).

HL perpetual short funded by funding income, delta-hedged long spot on the same
venue. The runtime here is a lifecycle stub; real execution is a later phase.
"""

from hypeedge.strategy.funding_arb.models import FundingArbParams
from hypeedge.strategy.funding_arb.runtime import (
    FundingArbRuntimeHandle,
    build_funding_arb_plugin,
    decode_funding_arb_config,
)

__all__ = [
    "FundingArbParams",
    "FundingArbRuntimeHandle",
    "build_funding_arb_plugin",
    "decode_funding_arb_config",
]
