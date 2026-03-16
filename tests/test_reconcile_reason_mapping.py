from __future__ import annotations

import httpx

from src.execution.reconcile_reason import FailureContext, classify_reconcile_failure


def test_code_50011_rate_limited() -> None:
    r, c = classify_reconcile_failure(FailureContext(ok=False, reason=None, error={"okx_code": "50011"}))
    assert r == "rate_limited" and c == "SOFT"


def test_code_50026_system_error() -> None:
    r, c = classify_reconcile_failure(FailureContext(ok=False, reason=None, error={"okx_code": "50026"}))
    assert r == "api_system_error" and c == "SOFT"


def test_code_501xx_auth_error() -> None:
    r, c = classify_reconcile_failure(FailureContext(ok=False, reason=None, error={"okx_code": "50113"}))
    assert r == "auth_error" and c == "AUTH"


def test_code_59999_unknown_api_error() -> None:
    r, c = classify_reconcile_failure(FailureContext(ok=False, reason=None, error={"okx_code": "59999"}))
    assert r == "api_error_unknown" and c == "SOFT"


def test_httpx_timeout_maps_to_timeout() -> None:
    r, c = classify_reconcile_failure(FailureContext(ok=False, reason=None, exc=httpx.ReadTimeout("x")))
    assert r == "timeout" and c == "SOFT"


def test_stale_status_overrides() -> None:
    r, c = classify_reconcile_failure(FailureContext(ok=True, reason=None, status_age_ms=2000, stale_threshold_ms=1000))
    assert r == "stale_status" and c == "SOFT"
