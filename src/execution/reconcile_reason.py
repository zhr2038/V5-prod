from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import httpx


Category = str  # OK|AUTH|HARD|SOFT


@dataclass
class FailureContext:
    """FailureContext类"""
    ok: bool
    reason: Optional[str]
    error: Optional[Dict[str, Any]] = None
    exc: Optional[BaseException] = None
    status_age_ms: Optional[int] = None
    stale_threshold_ms: Optional[int] = None


def _okx_code_from_error(error: Optional[Dict[str, Any]]) -> Optional[str]:
    if not error:
        return None
    c = error.get("okx_code")
    return str(c) if c is not None else None


def classify_reconcile_failure(ctx: FailureContext) -> Tuple[str, Category]:
    """Return (normalized_reason, category).

    Priority order:
    1) stale status
    2) OKX error code
    3) exception class
    4) existing reason

    Categories:
      OK, AUTH, HARD, SOFT
    """

    # 1) stale status wins (even if ok=true)
    if (
        ctx.status_age_ms is not None
        and ctx.stale_threshold_ms is not None
        and int(ctx.status_age_ms) > int(ctx.stale_threshold_ms)
    ):
        return "stale_status", "SOFT"

    # OK
    if bool(ctx.ok):
        return "ok", "OK"

    # 2) OKX code mapping
    code = _okx_code_from_error(ctx.error)
    if code:
        if code == "50011":
            return "rate_limited", "SOFT"
        if code == "50026":
            return "api_system_error", "SOFT"
        if code.startswith("501") or code == "50041":
            return "auth_error", "AUTH"
        if code.startswith("5"):
            return "api_error_unknown", "SOFT"

    # 3) exceptions
    if ctx.exc is not None:
        if isinstance(ctx.exc, httpx.TimeoutException):
            return "timeout", "SOFT"
        if isinstance(ctx.exc, httpx.ConnectError):
            return "network_error", "SOFT"

    # 4) fallback by reason string
    r = (ctx.reason or "").strip()
    if r in {"usdt_mismatch", "base_mismatch"}:
        return r, "HARD"

    if r:
        return r, "SOFT"

    return "unknown", "SOFT"
