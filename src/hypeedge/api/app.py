"""FastAPI application factory for HypeEdge HTTP API layer.

Design doc §10: "FastAPI (Phase 2B+, 为前端仪表盘提供 REST)"
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import Response

from hypeedge.api.auth import ApiRole, authenticate_bearer, configured_role_tokens
from hypeedge.api.commands import ApiCommandService, InMemoryApiCommandStore
from hypeedge.api.errors import (
    ApiProblem,
    api_problem_handler,
    problem_response,
    validation_problem_handler,
)
from hypeedge.api.security import SlidingWindowLimiter

logger = structlog.get_logger(__name__)


def create_api(hype_app: Any, cors_origins: list[str] | None = None) -> FastAPI:  # noqa: ANN401
    """Create the FastAPI application wired to HypeEdgeApp components.

    Args:
        hype_app: The HypeEdgeApp instance (provides access to all components).
        cors_origins: Allowed CORS origins (default: localhost:34001 for Next.js dev).

    Returns:
        Configured FastAPI app ready to serve.
    """
    environment = str(hype_app.settings.environment)
    api_settings = getattr(hype_app.settings, "api", None)
    role_tokens = configured_role_tokens(api_settings) if api_settings is not None else ()
    configured_host = getattr(api_settings, "host", "127.0.0.1") if api_settings is not None else "127.0.0.1"
    api_host = configured_host if isinstance(configured_host, str) else "127.0.0.1"
    admin_tokens = [token for token, role in role_tokens if role is ApiRole.ADMIN]
    if environment == "mainnet" and not any(len(token) >= 32 for token in admin_tokens):
        raise RuntimeError(
            "HYPE_API__ADMIN_TOKEN or HYPE_API__AUTH_TOKEN with at least 32 characters is required on mainnet"
        )
    if api_host not in {"127.0.0.1", "::1", "localhost"} and not role_tokens:
        raise RuntimeError("An API role token is required when the API listens on a non-loopback address")

    feature_settings = getattr(hype_app.settings, "features", None)
    configured_api_v1 = getattr(feature_settings, "api_v1", None)
    api_v1_enabled = configured_api_v1 if isinstance(configured_api_v1, bool) else True

    configured_command_service = getattr(hype_app, "api_command_service", None)
    command_service = configured_command_service if isinstance(configured_command_service, ApiCommandService) else None
    if environment == "mainnet" and command_service is None:
        raise RuntimeError("Postgres ApiCommandService is required on mainnet")
    if command_service is None:
        command_service = ApiCommandService(InMemoryApiCommandStore())

    app = FastAPI(
        title="HypeEdge API",
        description="HypeEdge 量化交易系统 HTTP API",
        version="0.2.0",
        docs_url=None if environment == "mainnet" else "/api/docs",
        redoc_url=None if environment == "mainnet" else "/api/redoc",
        openapi_url=None if environment == "mainnet" else "/api/openapi.json",
    )

    # Store HypeEdgeApp reference for dependency injection
    app.state.hype_app = hype_app
    app.state.role_tokens = role_tokens
    app.state.api_command_service = command_service
    app.state.request_limiter = SlidingWindowLimiter()
    app.state.mutation_limiter = SlidingWindowLimiter()
    app.state.auth_failure_limiter = SlidingWindowLimiter()

    app.add_exception_handler(ApiProblem, api_problem_handler)  # type: ignore[arg-type]
    app.add_exception_handler(RequestValidationError, validation_problem_handler)  # type: ignore[arg-type]

    @app.middleware("http")
    async def request_security(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request.state.request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        request.state.actor_type = "anonymous"
        request.state.actor_id = "anonymous"
        request.state.actor_role = ""
        protected = request.url.path.startswith("/api")
        is_mutation = request.method not in {"GET", "HEAD", "OPTIONS"}
        client_host = request.client.host if request.client is not None else "unknown"
        response: Response | None = None
        request_limit = int(getattr(api_settings, "request_rate_limit_per_minute", 600))
        if not app.state.request_limiter.allow(f"request:{client_host}", request_limit):
            response = problem_response(
                request,
                status=429,
                code="RATE_LIMIT_EXCEEDED",
                detail="Too many API requests; retry later",
                retryable=True,
            )
        if protected and role_tokens:
            principal = authenticate_bearer(request.headers.get("Authorization", ""), role_tokens)
            if response is None and principal is None:
                auth_limit = int(getattr(api_settings, "auth_failure_limit_per_minute", 10))
                allowed = app.state.auth_failure_limiter.allow(f"auth:{client_host}", auth_limit)
                response = problem_response(
                    request,
                    status=401 if allowed else 429,
                    code="AUTHENTICATION_REQUIRED" if allowed else "AUTH_RATE_LIMIT_EXCEEDED",
                    detail="A valid Bearer token is required" if allowed else "Too many failed authentication attempts",
                    retryable=not allowed,
                )
            elif response is None and principal is not None:
                actor_id, role = principal
                request.state.actor_type = "api_token"
                request.state.actor_id = actor_id
                request.state.actor_role = role.value
        elif response is None and protected:
            request.state.actor_type = "local"
            request.state.actor_id = "local-admin"
            request.state.actor_role = ApiRole.ADMIN.value
        if response is None and protected and is_mutation:
            mutation_limit = int(getattr(api_settings, "mutation_rate_limit_per_minute", 60))
            actor_key = str(getattr(request.state, "actor_id", client_host))
            if not app.state.mutation_limiter.allow(f"mutation:{actor_key}", mutation_limit):
                response = problem_response(
                    request,
                    status=429,
                    code="MUTATION_RATE_LIMIT_EXCEEDED",
                    detail="Too many mutation requests; retry later",
                    retryable=True,
                )
        if response is None and is_mutation and request.url.path.startswith("/api/v1/"):
            idempotency_key = request.headers.get("Idempotency-Key", "")
            if not idempotency_key or len(idempotency_key) > 128:
                response = problem_response(
                    request,
                    status=400,
                    code="IDEMPOTENCY_KEY_REQUIRED",
                    detail="A valid Idempotency-Key header is required",
                )
        if response is None and is_mutation and request.cookies.get("hypeedge_session"):
            csrf_cookie = request.cookies.get("hypeedge_csrf", "")
            csrf_header = request.headers.get("X-CSRF-Token", "")
            if not csrf_cookie or not csrf_header or csrf_cookie != csrf_header:
                response = problem_response(
                    request,
                    status=403,
                    code="CSRF_VALIDATION_FAILED",
                    detail="A matching CSRF token is required for session mutations",
                )
        if response is None:
            response = await call_next(request)
        response.headers["X-Request-ID"] = request.state.request_id
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        response.headers["Cache-Control"] = response.headers.get("Cache-Control", "no-store")
        if response.status_code == 429:
            response.headers["Retry-After"] = "60"
        return response

    # CORS for frontend
    origins = (
        cors_origins
        if cors_origins is not None
        else [
            "http://localhost:34001",  # Next.js dev server
            "http://localhost:34002",
            "http://127.0.0.1:34001",
            "http://192.168.31.5:34001",
        ]
    )
    if origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=False,
            allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            allow_headers=[
                "Authorization",
                "Content-Type",
                "Idempotency-Key",
                "If-Match",
                "Last-Event-ID",
                "X-CSRF-Token",
                "X-Request-ID",
            ],
        )

    # Register only the V1 contract. The legacy /api routes are intentionally
    # absent so no un-audited mutation path can reappear.
    from hypeedge.api.routes.account import router as account_router
    from hypeedge.api.routes.events import router as events_router
    from hypeedge.api.routes.market import router as market_router
    from hypeedge.api.routes.market_making import router as market_making_router
    from hypeedge.api.routes.market_making_ws import router as market_making_ws_router
    from hypeedge.api.routes.market_ws import router as market_ws_router
    from hypeedge.api.routes.orders import router as orders_router
    from hypeedge.api.routes.positions import router as positions_router
    from hypeedge.api.routes.risk import router as risk_router
    from hypeedge.api.routes.strategies import router as strategies_router
    from hypeedge.api.routes.system import router as system_router

    if api_v1_enabled:
        routers = (
            account_router,
            positions_router,
            orders_router,
            market_making_router,
            strategies_router,
            risk_router,
            market_router,
            events_router,
            system_router,
        )
        for router in routers:
            app.include_router(router, prefix="/api/v1")
        app.include_router(market_ws_router)
        app.include_router(market_making_ws_router)

    # Health check
    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "trading_enabled": hype_app.trading_enabled,
            "environment": hype_app.settings.environment,
            "api_v1_enabled": api_v1_enabled,
        }

    logger.info("api_created", routes=len(app.routes))
    return app
