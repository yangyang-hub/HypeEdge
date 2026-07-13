"""Strategy type registration and runtime construction boundaries."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Protocol

from hypeedge.core.enums import MarketMakerLifecycle
from hypeedge.core.exceptions import StrategyRegistrationError
from hypeedge.core.types import StrategyId, SubAccount, Symbol
from hypeedge.strategy.plugin import StrategyTypeCapabilities, StrategyTypePlugin


@dataclass(frozen=True, slots=True)
class StrategyInstanceDefinition:
    strategy_id: StrategyId
    strategy_type: str
    sub_account: SubAccount
    symbol: Symbol
    desired_state: MarketMakerLifecycle = MarketMakerLifecycle.STOPPED
    desired_config_revision: int = 1
    revision: int = 0

    def __post_init__(self) -> None:
        if not self.strategy_type.strip():
            raise ValueError("strategy_type is required")
        if self.desired_config_revision <= 0:
            raise ValueError("desired_config_revision must be positive")
        if self.revision < 0:
            raise ValueError("instance revision cannot be negative")


@dataclass(frozen=True, slots=True)
class StrategyConfigSnapshot:
    strategy_id: StrategyId
    revision: int
    values: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.revision <= 0:
            raise ValueError("config revision must be positive")
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))


@dataclass(frozen=True, slots=True)
class StrategyBuildContext:
    instance: StrategyInstanceDefinition
    config: StrategyConfigSnapshot


class StrategyRuntimeHandle(Protocol):
    """Adapter implemented by a concrete StrategyRunner/quote runtime."""

    async def start(self) -> None: ...

    async def set_mode(self, mode: MarketMakerLifecycle) -> None: ...

    async def apply_config(self, config: StrategyConfigSnapshot) -> None: ...

    async def stop(self) -> None: ...


StrategyFactory = Callable[[StrategyBuildContext], StrategyRuntimeHandle]


class StrategyRegistry:
    """Maps stable strategy type names to factories/plugins; instances remain independent."""

    def __init__(self) -> None:
        self._factories: dict[str, StrategyFactory] = {}
        self._plugins: dict[str, StrategyTypePlugin] = {}

    def register(self, strategy_type: str, factory: StrategyFactory) -> None:
        """Register a runtime factory (backward-compatible with market-maker wiring)."""
        normalized = strategy_type.strip().lower()
        if not normalized:
            raise StrategyRegistrationError("Strategy type is required")
        if normalized in self._factories:
            raise StrategyRegistrationError(f"Strategy type is already registered: {normalized}")
        self._factories[normalized] = factory

    def register_plugin(self, plugin: StrategyTypePlugin) -> None:
        normalized = plugin.strategy_type.strip().lower()
        if not normalized:
            raise StrategyRegistrationError("Strategy type is required")
        if normalized in self._factories or normalized in self._plugins:
            raise StrategyRegistrationError(f"Strategy type is already registered: {normalized}")
        self._plugins[normalized] = plugin
        self._factories[normalized] = plugin.factory

    def unregister(self, strategy_type: str) -> None:
        normalized = strategy_type.strip().lower()
        if normalized not in self._factories:
            raise StrategyRegistrationError(f"Strategy type is not registered: {normalized}")
        del self._factories[normalized]
        self._plugins.pop(normalized, None)

    def create(self, context: StrategyBuildContext) -> StrategyRuntimeHandle:
        normalized = context.instance.strategy_type.strip().lower()
        factory = self._factories.get(normalized)
        if factory is None:
            raise StrategyRegistrationError(f"Strategy type is not registered: {normalized}")
        return factory(context)

    def get_plugin(self, strategy_type: str) -> StrategyTypePlugin | None:
        return self._plugins.get(strategy_type.strip().lower())

    def capabilities(self, strategy_type: str) -> StrategyTypeCapabilities | None:
        plugin = self.get_plugin(strategy_type)
        return plugin.capabilities if plugin is not None else None

    @property
    def strategy_types(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories))

    def __contains__(self, strategy_type: object) -> bool:
        return isinstance(strategy_type, str) and strategy_type.strip().lower() in self._factories
