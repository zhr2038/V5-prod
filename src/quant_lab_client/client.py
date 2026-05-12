from __future__ import annotations

import os
import warnings
from ipaddress import ip_address
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests

from src.reporting.quant_lab_audit import (
    append_quant_lab_request,
    sanitize_quant_lab_obj,
)

from .cache import TTLCache
from .exceptions import (
    QuantLabHTTPError,
    QuantLabTimeout,
    QuantLabUnavailable,
    QuantLabValidationError,
)
from .models import (
    CostEstimate,
    GateDecision,
    QuantLabHealth,
    RiskPermission,
    symbol_to_quant_lab_symbol,
)
from .permissions import normalize_permission


STRICT_GATE_MODES = {"cost_only", "permission_only", "enforce"}


def summarize_response(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {"type": type(payload).__name__}
    keys = (
        "status",
        "service",
        "mode",
        "permission",
        "decision",
        "allowed_modes",
        "cost_model_version",
        "gate_version",
        "source",
        "fallback_level",
        "sample_count",
        "total_cost_bps",
        "cost_bps",
        "symbol",
        "alpha_id",
    )
    summary = {key: payload.get(key) for key in keys if key in payload}
    for nested_key in ("data", "result", "payload"):
        nested = payload.get(nested_key)
        if isinstance(nested, Mapping):
            for key in keys:
                if key in nested and key not in summary:
                    summary[key] = nested.get(key)
    return sanitize_quant_lab_obj(summary)


@dataclass
class QuantLabResponse:
    endpoint: str
    ok: bool
    status_code: Optional[int] = None
    data: Any = None
    error: Optional[str] = None
    latency_ms: Optional[float] = None
    request_id: Optional[str] = None
    fallback_used: bool = False
    cached: bool = False

    def summary(self) -> Dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "ok": bool(self.ok),
            "status_code": self.status_code,
            "latency_ms": self.latency_ms,
            "error": self.error,
            "fallback_used": bool(self.fallback_used),
            "cached": bool(self.cached),
            "response": summarize_response(self.data),
        }


@dataclass
class QuantLabClient:
    base_url: str
    api_token: Optional[str] = None
    mode: str = "shadow"
    allow_insecure_http_with_token: bool = False
    timeout_seconds: float = 2.0
    max_retries: int = 1
    cache_ttl_seconds: int = 60
    http_client: Optional[Any] = None
    request_log_path: str | Path = "reports/quant_lab_requests.jsonl"
    run_id: Optional[str] = None
    phase: str = "live"
    _cache: TTLCache = field(init=False, repr=False)
    token_auth_disabled_reason: Optional[str] = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.base_url = str(self.base_url or "").strip().rstrip("/")
        if not self.base_url:
            raise QuantLabValidationError("quant-lab base_url is required")
        if not (self.base_url.startswith("http://") or self.base_url.startswith("https://")):
            raise QuantLabValidationError("quant-lab base_url must start with http:// or https://")
        self.mode = str(self.mode or "shadow").strip().lower().replace("-", "_")
        self.api_token = str(self.api_token or "").strip() or None
        self._validate_token_transport()
        self.timeout_seconds = float(self.timeout_seconds or 2.0)
        if self.timeout_seconds <= 0 or self.timeout_seconds > 10:
            raise QuantLabValidationError("quant-lab timeout_seconds must be > 0 and <= 10")
        self.max_retries = max(0, int(self.max_retries or 0))
        self.cache_ttl_seconds = max(0, int(self.cache_ttl_seconds or 0))
        self.http_client = self.http_client or requests.Session()
        self._cache = TTLCache(ttl_seconds=self.cache_ttl_seconds)

    @classmethod
    def from_config(
        cls,
        cfg: Any,
        *,
        run_id: Optional[str] = None,
        phase: str = "live",
        http_client: Optional[Any] = None,
        mode: Optional[str] = None,
    ) -> "QuantLabClient":
        token_env = str(getattr(cfg, "api_token_env", "QUANT_LAB_API_TOKEN") or "QUANT_LAB_API_TOKEN")
        token = os.getenv(token_env, "").strip()
        return cls(
            base_url=str(getattr(cfg, "base_url", "") or ""),
            api_token=token or None,
            mode=str(mode or getattr(cfg, "mode", "shadow") or "shadow"),
            allow_insecure_http_with_token=bool(getattr(cfg, "allow_insecure_http_with_token", False)),
            timeout_seconds=float(getattr(cfg, "timeout_seconds", 2.0) or 2.0),
            max_retries=int(getattr(cfg, "max_retries", 1) or 0),
            cache_ttl_seconds=int(getattr(cfg, "cache_ttl_seconds", 60) or 0),
            http_client=http_client,
            request_log_path=str(getattr(cfg, "request_log_path", "reports/quant_lab_requests.jsonl") or "reports/quant_lab_requests.jsonl"),
            run_id=run_id,
            phase=phase,
        )

    @staticmethod
    def _public_http_host(base_url: str) -> Optional[str]:
        parsed = urlparse(base_url)
        if parsed.scheme.lower() != "http":
            return None
        host = str(parsed.hostname or "").strip().lower()
        if not host:
            return "<missing>"
        if host == "localhost":
            return None
        try:
            addr = ip_address(host)
        except ValueError:
            return host
        if addr.is_loopback or addr.is_private:
            return None
        return host

    def _validate_token_transport(self) -> None:
        if not self.api_token:
            return
        if bool(self.allow_insecure_http_with_token):
            return
        public_http_host = self._public_http_host(self.base_url)
        if public_http_host is None:
            return
        message = (
            "quant-lab api_token is configured for public HTTP base_url; "
            "token will not be sent in shadow/local_only mode and is forbidden in gated modes"
        )
        if self.mode in STRICT_GATE_MODES:
            raise QuantLabValidationError(message)
        self.api_token = None
        self.token_auth_disabled_reason = "public_http_token_stripped"
        warnings.warn(message, RuntimeWarning, stacklevel=2)

    def _headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        return headers

    @staticmethod
    def _cache_key(endpoint_path: str, params: Mapping[str, Any]) -> Tuple[str, Tuple[Tuple[str, str], ...]]:
        clean = {str(k): str(v) for k, v in dict(params or {}).items() if v is not None}
        return endpoint_path, tuple(sorted(clean.items()))

    def _log_request(
        self,
        *,
        endpoint_path: str,
        params: Mapping[str, Any],
        status_code: Optional[int],
        latency_ms: Optional[float],
        success: bool,
        error_type: Optional[str],
        cached: bool,
        response_summary: Mapping[str, Any],
    ) -> None:
        append_quant_lab_request(
            self.request_log_path,
            {
                "run_id": self.run_id,
                "phase": self.phase,
                "endpoint_path": endpoint_path,
                "query_keys": sorted(str(k) for k, v in dict(params or {}).items() if v is not None),
                "status_code": status_code,
                "latency_ms": latency_ms,
                "success": bool(success),
                "error_type": error_type,
                "cached": bool(cached),
                "response_summary": dict(response_summary or {}),
            },
        )

    def get_json(self, endpoint_path: str, params: Optional[Mapping[str, Any]] = None) -> QuantLabResponse:
        endpoint = "/" + str(endpoint_path or "").lstrip("/")
        clean_params = {k: v for k, v in dict(params or {}).items() if v is not None}
        key = self._cache_key(endpoint, clean_params)
        cached_value = self._cache.get(key)
        if cached_value is not None:
            response = QuantLabResponse(
                endpoint=endpoint,
                ok=True,
                status_code=200,
                data=cached_value,
                latency_ms=0.0,
                cached=True,
            )
            self._log_request(
                endpoint_path=endpoint,
                params=clean_params,
                status_code=200,
                latency_ms=0.0,
                success=True,
                error_type=None,
                cached=True,
                response_summary=summarize_response(cached_value),
            )
            return response

        url = urljoin(self.base_url + "/", endpoint.lstrip("/"))
        attempts = self.max_retries + 1
        last_error: Optional[BaseException] = None
        started_all = time.perf_counter()
        status_code: Optional[int] = None
        data: Any = None
        for attempt in range(attempts):
            started = time.perf_counter()
            try:
                resp = self.http_client.get(
                    url,
                    params=clean_params,
                    headers=self._headers(),
                    timeout=self.timeout_seconds,
                )
                status_code = int(getattr(resp, "status_code", 0) or 0)
                try:
                    data = resp.json()
                except ValueError:
                    data = {"text": str(getattr(resp, "text", ""))[:500]}
                latency_ms = round((time.perf_counter() - started) * 1000.0, 3)
                if 200 <= status_code < 300:
                    self._cache.set(key, data)
                    response = QuantLabResponse(
                        endpoint=endpoint,
                        ok=True,
                        status_code=status_code,
                        data=data,
                        latency_ms=latency_ms,
                    )
                    self._log_request(
                        endpoint_path=endpoint,
                        params=clean_params,
                        status_code=status_code,
                        latency_ms=latency_ms,
                        success=True,
                        error_type=None,
                        cached=False,
                        response_summary=summarize_response(data),
                    )
                    return response
                last_error = QuantLabHTTPError(f"quant-lab HTTP {status_code}")
            except requests.Timeout as exc:
                last_error = QuantLabTimeout(str(exc)[:300])
            except Exception as exc:
                last_error = QuantLabUnavailable(f"{type(exc).__name__}: {str(exc)[:300]}")
            if attempt + 1 < attempts:
                time.sleep(min(0.2, 0.05 * (attempt + 1)))

        latency_ms = round((time.perf_counter() - started_all) * 1000.0, 3)
        error_type = type(last_error).__name__ if last_error is not None else "QuantLabUnavailable"
        self._log_request(
            endpoint_path=endpoint,
            params=clean_params,
            status_code=status_code,
            latency_ms=latency_ms,
            success=False,
            error_type=error_type,
            cached=False,
            response_summary=summarize_response(data),
        )
        response = QuantLabResponse(
            endpoint=endpoint,
            ok=False,
            status_code=status_code,
            data=data,
            error=error_type,
            latency_ms=latency_ms,
        )
        if isinstance(last_error, QuantLabTimeout):
            raise QuantLabTimeout(str(last_error))
        if isinstance(last_error, QuantLabHTTPError):
            raise QuantLabHTTPError(str(last_error))
        raise QuantLabUnavailable(str(last_error or "quant-lab unavailable"))

    def get_health(self) -> QuantLabHealth:
        response = self.get_json("/v1/health")
        health = QuantLabHealth.from_payload(response.data)
        if str(health.mode).lower() != "read-only":
            raise QuantLabValidationError(f"quant-lab health mode must be read-only, got {health.mode!r}")
        return health

    def get_live_permission(self, *, strategy: str, version: str) -> RiskPermission:
        response = self.get_json(
            "/v1/risk/live-permission",
            params={"strategy": str(strategy or "").strip(), "version": str(version or "").strip()},
        )
        permission = RiskPermission.from_payload(response.data)
        permission.permission = normalize_permission(permission.permission)
        return permission

    def estimate_cost(
        self,
        *,
        symbol: str,
        regime: str,
        notional_usdt: float,
        quantile: str = "p75",
    ) -> CostEstimate:
        response = self.get_json(
            "/v1/costs/estimate",
            params={
                "symbol": symbol_to_quant_lab_symbol(symbol),
                "regime": str(regime or "normal").strip() or "normal",
                "notional_usdt": float(notional_usdt or 0.0),
                "quantile": str(quantile or "p75").strip() or "p75",
            },
        )
        estimate = CostEstimate.from_payload(response.data)
        if not estimate.symbol:
            estimate.symbol = symbol_to_quant_lab_symbol(symbol)
        if not estimate.regime:
            estimate.regime = str(regime or "normal")
        if not estimate.notional_usdt:
            estimate.notional_usdt = float(notional_usdt or 0.0)
        if not estimate.quantile:
            estimate.quantile = str(quantile or "p75")
        return estimate

    def get_gate_decision(self, alpha_id: str) -> GateDecision:
        safe_alpha = str(alpha_id or "").strip()
        if not safe_alpha:
            raise QuantLabValidationError("alpha_id is required for quant-lab gate decision")
        response = self.get_json(f"/v1/gates/decision/{safe_alpha}")
        return GateDecision.from_payload(response.data)

    # Backward-compatible method names used by the first V5 integration.
    def health(self) -> QuantLabResponse:
        return self.get_json("/v1/health")

    def live_permission(self, *, strategy: str, version: str) -> QuantLabResponse:
        return self.get_json(
            "/v1/risk/live-permission",
            params={"strategy": str(strategy or "").strip(), "version": str(version or "").strip()},
        )

    def cost_estimate(
        self,
        *,
        symbol: str,
        side: str = "",
        notional_usdt: float,
        regime: str,
        quantile: str = "p75",
        signal_price: Optional[float] = None,
        alpha_id: Optional[str] = None,
        notional_bucket: Optional[str] = None,
    ) -> QuantLabResponse:
        return self.get_json(
            "/v1/costs/estimate",
            params={
                "symbol": symbol_to_quant_lab_symbol(symbol),
                "regime": str(regime or "normal").strip() or "normal",
                "notional_usdt": float(notional_usdt or 0.0),
                "quantile": str(quantile or "p75").strip() or "p75",
                "notional_bucket": notional_bucket,
            },
        )

    def gate_decision(self, alpha_id: str) -> QuantLabResponse:
        return self.get_json(f"/v1/gates/decision/{str(alpha_id).strip()}")


def append_jsonl(path: str | Path, payload: Mapping[str, Any]) -> None:
    from src.reporting.quant_lab_audit import append_quant_lab_usage

    append_quant_lab_usage(path, payload)
