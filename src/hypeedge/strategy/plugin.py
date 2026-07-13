"""Strategy type plugin contract for the multi-strategy control plane.

See ``docs/strategy_control_plane.md`` and ``docs/design.md`` §19.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Protocol

from hypeedge.core.enums import MarketMakerLifecycle

if TYPE_CHECKING:
    from hypeedge.strategy.registry import StrategyBuildContext, StrategyConfigSnapshot, StrategyRuntimeHandle

    StrategyFactory = Callable[[StrategyBuildContext], StrategyRuntimeHandle]
else:
    StrategyFactory = Callable[..., Any]


@dataclass(frozen=True, slots=True)
class StrategyTypeCapabilities:
    """Per-type lifecycle and product surface declaration."""

    creatable: bool = True
    desired_states: frozenset[MarketMakerLifecycle] = frozenset(
        {
            MarketMakerLifecycle.STOPPED,
            MarketMakerLifecycle.RUNNING,
            MarketMakerLifecycle.PAUSED,
        }
    )
    actions: frozenset[str] = frozenset({"start", "stop", "pause", "resume"})
    supports_shadow: bool = False
    supports_drain: bool = False
    workspace: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "desired_states", frozenset(self.desired_states))
        object.__setattr__(self, "actions", frozenset(self.actions))


MARKET_MAKER_CAPABILITIES = StrategyTypeCapabilities(
    creatable=True,
    desired_states=frozenset(
        {
            MarketMakerLifecycle.STOPPED,
            MarketMakerLifecycle.SHADOW,
            MarketMakerLifecycle.RUNNING,
            MarketMakerLifecycle.PAUSED,
        }
    ),
    actions=frozenset({"start", "pause", "resume", "drain", "stop"}),
    supports_shadow=True,
    supports_drain=True,
    workspace="market-making",
)

TREND_FOLLOW_CAPABILITIES = StrategyTypeCapabilities(
    creatable=True,
    desired_states=frozenset(
        {
            MarketMakerLifecycle.STOPPED,
            MarketMakerLifecycle.RUNNING,
            MarketMakerLifecycle.PAUSED,
        }
    ),
    actions=frozenset({"start", "stop", "pause", "resume"}),
    supports_shadow=False,
    supports_drain=False,
    workspace=None,
)


class StrategyTypePlugin(Protocol):
    """Registered strategy type: config persistence + runtime factory + capabilities."""

    @property
    def strategy_type(self) -> str: ...

    @property
    def capabilities(self) -> StrategyTypeCapabilities: ...

    def default_config(self) -> Mapping[str, Any]: ...

    def validate_create_config(self, values: Mapping[str, Any]) -> Mapping[str, Any]: ...

    def decode_config(self, snapshot: StrategyConfigSnapshot) -> Any: ...

    def factory(self, context: StrategyBuildContext) -> StrategyRuntimeHandle: ...


@dataclass(frozen=True, slots=True)
class StaticStrategyTypePlugin:
    """Concrete plugin value object used by builtin registrations."""

    strategy_type: str
    capabilities: StrategyTypeCapabilities
    factory: StrategyFactory
    _default_config: Mapping[str, Any]
    _validate: Callable[[Mapping[str, Any]], Mapping[str, Any]]
    _decode: Callable[[StrategyConfigSnapshot], Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "_default_config", MappingProxyType(dict(self._default_config)))

    def default_config(self) -> Mapping[str, Any]:
        return self._default_config

    def validate_create_config(self, values: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._validate(values)

    def decode_config(self, snapshot: StrategyConfigSnapshot) -> Any:
        return self._decode(snapshot)
