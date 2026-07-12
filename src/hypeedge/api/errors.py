"""Stable RFC 9457-style API errors."""

from __future__ import annotations

from typing import Any

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse


class ApiProblem(Exception):
    """A safe, stable API error that can be returned to clients."""

    def __init__(
        self,
        status: int,
        code: str,
        detail: str,
        *,
        retryable: bool = False,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(detail)
        self.status = status
        self.code = code
        self.detail = detail
        self.retryable = retryable
        self.context = context or {}


def problem_response(
    request: Request,
    *,
    status: int,
    code: str,
    detail: str,
    retryable: bool = False,
    context: dict[str, Any] | None = None,
) -> JSONResponse:
    """Build a consistent problem response without leaking internals."""
    request_id = getattr(request.state, "request_id", "")
    return JSONResponse(
        status_code=status,
        media_type="application/problem+json",
        content={
            "type": f"https://hypeedge.local/problems/{code.lower()}",
            "title": code,
            "status": status,
            "code": code,
            "detail": detail,
            "request_id": request_id,
            "retryable": retryable,
            "context": context or {},
        },
    )


async def api_problem_handler(request: Request, exc: ApiProblem) -> JSONResponse:
    return problem_response(
        request,
        status=exc.status,
        code=exc.code,
        detail=exc.detail,
        retryable=exc.retryable,
        context=exc.context,
    )


async def validation_problem_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    fields = [".".join(str(part) for part in error["loc"]) for error in exc.errors()]
    return problem_response(
        request,
        status=422,
        code="REQUEST_VALIDATION_FAILED",
        detail="Request data failed validation",
        context={"fields": fields},
    )
