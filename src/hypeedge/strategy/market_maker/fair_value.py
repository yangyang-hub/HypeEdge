"""Bounded explainable fair-value model."""

from __future__ import annotations

from decimal import Decimal

from hypeedge.core.types import Price
from hypeedge.strategy.market_maker.models import MarketFeatures, MarketMakerConfig


class FairValueModel:
    """Combine microprice and short-horizon flow with a hard tick cap."""

    def calculate(self, features: MarketFeatures, config: MarketMakerConfig) -> Price:
        mid = Decimal(features.mid_price)
        tick = config.tick_size
        local_raw_shift = (
            config.beta_microprice * (Decimal(features.microprice) - mid)
            + config.beta_ofi_ticks * features.normalized_ofi * tick
            + config.beta_trade_flow_ticks * features.trade_flow * tick
            + config.beta_short_return_ticks * features.short_return * mid
        )
        local_cap = config.max_fair_shift_ticks * tick
        local_shift = max(-local_cap, min(local_cap, local_raw_shift))
        external_shift = Decimal(0)
        if features.external_adjusted_price is not None and features.external_effective_weight > 0:
            external_raw_shift = (Decimal(features.external_adjusted_price) - mid) * features.external_effective_weight
            external_cap = config.max_external_shift_ticks * tick
            external_shift = max(-external_cap, min(external_cap, external_raw_shift))
        total_cap = config.max_total_fair_shift_ticks * tick
        total_shift = max(-total_cap, min(total_cap, local_shift + external_shift))
        return Price(mid + total_shift)
