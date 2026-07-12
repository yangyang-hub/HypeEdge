"""API bearer-token authentication and role-based authorization."""

from __future__ import annotations

import hashlib
import secrets
from enum import StrEnum
from typing import Annotated, Any

from fastapi import Depends, Request

from hypeedge.api.errors import ApiProblem


class ApiRole(StrEnum):
    VIEWER = "viewer"
    OPERATOR = "operator"
    ADMIN = "admin"


_ROLE_RANK = {
    ApiRole.VIEWER: 10,
    ApiRole.OPERATOR: 20,
    ApiRole.ADMIN: 30,
}


def configured_role_tokens(api_settings: Any) -> tuple[tuple[str, ApiRole], ...]:  # noqa: ANN401
    """Return configured tokens, with the legacy token treated as admin."""
    candidates = (
        (getattr(api_settings, "viewer_token", ""), ApiRole.VIEWER),
        (getattr(api_settings, "operator_token", ""), ApiRole.OPERATOR),
        (getattr(api_settings, "admin_token", ""), ApiRole.ADMIN),
        (getattr(api_settings, "auth_token", ""), ApiRole.ADMIN),
    )
    return tuple((token, role) for token, role in candidates if isinstance(token, str) and token)


def authenticate_bearer(authorization: str, tokens: tuple[tuple[str, ApiRole], ...]) -> tuple[str, ApiRole] | None:
    """Authenticate a bearer without returning early from token comparisons."""
    scheme, _, supplied = authorization.partition(" ")
    if scheme.lower() != "bearer" or not supplied:
        return None

    matched_role: ApiRole | None = None
    for configured, role in tokens:
        matched = secrets.compare_digest(supplied, configured)
        if matched and (matched_role is None or _ROLE_RANK[role] > _ROLE_RANK[matched_role]):
            matched_role = role
    if matched_role is None:
        return None
    actor_id = f"api-token:{hashlib.sha256(supplied.encode()).hexdigest()[:24]}"
    return actor_id, matched_role


async def _require_role(request: Request, required: ApiRole) -> ApiRole:
    raw_role = str(getattr(request.state, "actor_role", ""))
    try:
        actual = ApiRole(raw_role)
    except ValueError as exc:
        raise ApiProblem(401, "AUTHENTICATION_REQUIRED", "An authenticated API principal is required") from exc
    if _ROLE_RANK[actual] < _ROLE_RANK[required]:
        command_service = getattr(request.app.state, "api_command_service", None)
        if request.method not in {"GET", "HEAD", "OPTIONS"} and command_service is not None:
            await command_service.audit_authorization_denied(request, required_role=required.value)
        raise ApiProblem(
            403,
            "INSUFFICIENT_ROLE",
            f"The {required.value} role is required for this operation",
            context={"required_role": required.value, "actor_role": actual.value},
        )
    return actual


async def require_viewer(request: Request) -> ApiRole:
    return await _require_role(request, ApiRole.VIEWER)


async def require_operator(request: Request) -> ApiRole:
    return await _require_role(request, ApiRole.OPERATOR)


async def require_admin(request: Request) -> ApiRole:
    return await _require_role(request, ApiRole.ADMIN)


ViewerDep = Annotated[ApiRole, Depends(require_viewer)]
OperatorDep = Annotated[ApiRole, Depends(require_operator)]
AdminDep = Annotated[ApiRole, Depends(require_admin)]
