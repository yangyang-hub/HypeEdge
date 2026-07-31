"""Single-venue, testnet-gated funding-rate arbitrage strategy."""

from hypeedge.strategy.funding_arb.models import FundingArbParams
from hypeedge.strategy.funding_arb.runtime import (
    FundingArbRuntimeDependencies,
    FundingArbRuntimeHandle,
    build_funding_arb_plugin,
    decode_funding_arb_config,
)

__all__ = [
    "FundingArbParams",
    "FundingArbRuntimeDependencies",
    "FundingArbRuntimeHandle",
    "build_funding_arb_plugin",
    "decode_funding_arb_config",
]
