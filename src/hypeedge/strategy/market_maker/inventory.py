"""Inventory bands and dimensionally explicit reservation-price skew."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from hypeedge.core.types import Price, Usd
from hypeedge.strategy.market_maker.models import InventorySnapshot, MarketFeatures, MarketMakerConfig


@dataclass(frozen=True, slots=True)
class InventoryDecision:
    inventory_notional: Usd
    normalized_inventory: Decimal
    shift_bps: Decimal
    reservation_price: Price
    allow_bid: bool
    allow_ask: bool
    emergency: bool


class InventoryController:
    """Move reservation price and disable inventory-increasing sides at limits."""

    def calculate(
        self,
        fair_price: Price,
        inventory: InventorySnapshot,
        features: MarketFeatures,
        config: MarketMakerConfig,
    ) -> InventoryDecision:
        if not inventory.healthy or inventory.equity <= 0 or fair_price <= 0:
            raise ValueError("inventory state, equity, and fair price must be healthy")

        notional = Usd(inventory.position_size * fair_price)
        z = Decimal(notional / config.soft_inventory_notional)
        z = max(Decimal("-2"), min(Decimal("2"), z))
        shift = (
            config.inventory_skew_bps * z
            + config.inventory_gamma_bps * z * features.return_variance_per_second * config.horizon_seconds
        )
        shift = max(-config.max_inventory_shift_bps, min(config.max_inventory_shift_bps, shift))
        reservation = Price(Decimal(fair_price) * (Decimal("1") - shift / Decimal("10000")))

        absolute = abs(notional)
        long_inventory = notional > 0
        short_inventory = notional < 0
        at_soft = absolute >= config.soft_inventory_notional
        at_hard = absolute >= config.hard_inventory_notional
        emergency = absolute >= config.emergency_inventory_notional

        allow_bid = not (at_soft and long_inventory)
        allow_ask = not (at_soft and short_inventory)
        if at_hard:
            allow_bid = short_inventory
            allow_ask = long_inventory

        return InventoryDecision(
            inventory_notional=notional,
            normalized_inventory=z,
            shift_bps=shift,
            reservation_price=reservation,
            allow_bid=allow_bid and not emergency,
            allow_ask=allow_ask and not emergency,
            emergency=emergency,
        )
