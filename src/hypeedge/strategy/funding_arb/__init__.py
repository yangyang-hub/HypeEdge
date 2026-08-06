"""Single-venue, testnet-gated funding-rate arbitrage strategy."""

from hypeedge.core.constants import AUTO_MARKET_SYMBOL, AUTO_SPOT_MARKET
from hypeedge.strategy.funding_arb.models import FundingArbParams
from hypeedge.strategy.funding_arb.runtime import (
    FundingArbRuntimeDependencies,
    FundingArbRuntimeHandle,
    build_funding_arb_plugin,
    decode_funding_arb_config,
)

__all__ = [
    "AUTO_MARKET_SYMBOL",
    "AUTO_SPOT_MARKET",
    "FundingArbParams",
    "FundingArbRuntimeDependencies",
    "FundingArbRuntimeHandle",
    "build_funding_arb_plugin",
    "decode_funding_arb_config",
]
