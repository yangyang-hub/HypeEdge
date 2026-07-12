"""FastAPI dependency injection for HypeEdge components.

Uses Annotated types for proper mypy compatibility.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import Depends, Request

from hypeedge.account.tracker import AccountTracker
from hypeedge.api.commands import ApiCommandService
from hypeedge.market_data.live_provider import LiveMarketDataProvider
from hypeedge.market_data.rest_client import RestClient
from hypeedge.risk.checker import RiskChecker
from hypeedge.risk.kill_switch import KillSwitch
from hypeedge.strategy.trend_follow import TrendFollowStrategy


def _get_app(request: Request) -> Any:
    """Get the HypeEdgeApp instance from FastAPI app state."""
    return request.app.state.hype_app


def _get_tracker(request: Request) -> AccountTracker | None:
    return _get_app(request).account_tracker  # type: ignore[no-any-return]


def _get_engine(request: Request) -> Any:
    return _get_app(request).execution_engine


def _get_risk_checker(request: Request) -> RiskChecker | None:
    return _get_app(request).risk_checker  # type: ignore[no-any-return]


def _get_kill_switch(request: Request) -> KillSwitch:
    return _get_app(request).kill_switch  # type: ignore[no-any-return]


def _get_strategy(request: Request) -> TrendFollowStrategy | None:
    return _get_app(request).strategy  # type: ignore[no-any-return]


def _get_rest_client(request: Request) -> RestClient | None:
    return _get_app(request)._rest_client  # type: ignore[no-any-return]


def _get_market_data_provider(request: Request) -> LiveMarketDataProvider | None:
    return _get_app(request)._market_data_provider  # type: ignore[no-any-return]


def _get_app_instance(request: Request) -> Any:
    return _get_app(request)


def _get_api_command_service(request: Request) -> ApiCommandService:
    return request.app.state.api_command_service  # type: ignore[no-any-return]


# Annotated dependency types for route signatures
TrackerDep = Annotated[AccountTracker | None, Depends(_get_tracker)]
EngineDep = Annotated[Any, Depends(_get_engine)]
RiskDep = Annotated[RiskChecker | None, Depends(_get_risk_checker)]
KillSwitchDep = Annotated[KillSwitch, Depends(_get_kill_switch)]
StrategyDep = Annotated[TrendFollowStrategy | None, Depends(_get_strategy)]
RestClientDep = Annotated[RestClient | None, Depends(_get_rest_client)]
MarketDataDep = Annotated[LiveMarketDataProvider | None, Depends(_get_market_data_provider)]
AppDep = Annotated[Any, Depends(_get_app_instance)]
ApiCommandDep = Annotated[ApiCommandService, Depends(_get_api_command_service)]
