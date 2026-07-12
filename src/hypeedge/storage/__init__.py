"""Storage package public boundaries, loaded lazily to avoid ORM import cycles."""

from __future__ import annotations

from typing import Any

__all__ = [
    "ActionBudgetView",
    "AuthoritativeRead",
    "InventoryView",
    "MarketMakingEventView",
    "MarketMakingStateView",
    "MarketMakingTransactionRepository",
    "PostgresMarketMakingReadRepository",
    "PostgresMarketMakingRepository",
    "PostgresStrategyAllocationManager",
    "PostgresStrategyStateStore",
    "QuoteSlotView",
    "StrategyInstanceView",
    "market_maker_config_hash",
    "normalize_market_maker_config",
]


def __getattr__(name: str) -> Any:
    if name not in __all__:
        raise AttributeError(name)
    from hypeedge.storage import market_making

    return getattr(market_making, name)
