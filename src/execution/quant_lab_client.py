from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional
from urllib.parse import urljoin

import requests


SECRET_KEY_PARTS = (
    "authorization",
    "api_key",
    "api-key",
    "api_secret",
    "api-secret",
    "secret",
    "token",
    "passphrase",
    "password",
    "private_key",
    "private-key",
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sanitize_quant_lab_obj(value: Any) -> Any:
    if isinstance(value, Mapping):
        out: Dict[str, Any] = {}
        for key, item in value.items():
            key_s = str(key)
            key_l = key_s.lower()
            if any(part in key_l for part in SECRET_KEY_PARTS):
                out[key_s] = "<REDACTED>"
            else:
                out[key_s] = sanitize_quant_lab_obj(item)
        return out
    if isinstance(value, list):
        return [sanitize_quant_lab_obj(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_quant_lab_obj(item) for item in value]
    return value


def append_jsonl(path: str | Path, payload: Mapping[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    safe_payload = sanitize_quant_lab_obj(dict(payload))
    with target.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(safe_payload, ensure_ascii=False, sort_keys=True) + "\n")


def summarize_response(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {"type": type(payload).__name__}
    keys = [
        "status",
        "ok",
        "decision",
        "permission",
        "action",
        "mode",
        "risk_mode",
        "source",
        "fallback",
        "cost_bps",
        "total_bps",
        "total_cost_bps",
        "estimated_cost_bps",
        "fallback_level",
        "reason",
        "alpha_id",
        "symbol",
    ]
    summary = {key: payload.get(key) for key in keys if key in payload}
    for nested_key in ("data", "result"):
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

    def summary(self) -> Dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "ok": bool(self.ok),
            "status_code": self.status_code,
            "latency_ms": self.latency_ms,
            "error": self.error,
            "fallback_used": bool(self.fallback_used),
            "response": summarize_response(self.data),
        }


@dataclass
class QuantLabClient:
    base_url: str
    timeout_sec: float = 2.0
    token_env: str = "QUANT_LAB_API_TOKEN"
    request_log_path: str | Path = "reports/quant_lab_requests.jsonl"
    run_id: Optional[str] = None
    phase: str = "live"
    session: requests.Session = field(default_factory=requests.Session)

    def __post_init__(self) -> None:
        self.base_url = str(self.base_url or "").strip().rstrip("/")
        if not self.base_url:
            raise ValueError("quant-lab base_url is required when quant_lab_enabled=true")
        if not (self.base_url.startswith("http://") or self.base_url.startswith("https://")):
            raise ValueError("quant-lab base_url must start with http:// or https://")
        self.timeout_sec = float(self.timeout_sec or 2.0)

    def _headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        token = os.getenv(str(self.token_env or "QUANT_LAB_API_TOKEN"), "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def get_json(self, endpoint: str, params: Optional[Mapping[str, Any]] = None) -> QuantLabResponse:
        endpoint_s = "/" + str(endpoint or "").lstrip("/")
        url = urljoin(self.base_url + "/", endpoint_s.lstrip("/"))
        started = time.perf_counter()
        status_code: Optional[int] = None
        data: Any = None
        error: Optional[str] = None
        ok = False
        try:
            response = self.session.get(
                url,
                params={k: v for k, v in dict(params or {}).items() if v is not None},
                headers=self._headers(),
                timeout=self.timeout_sec,
            )
            status_code = int(response.status_code)
            try:
                data = response.json()
            except ValueError:
                data = {"text": response.text[:500]}
            ok = bool(200 <= status_code < 300)
            if not ok:
                error = f"http_{status_code}"
        except Exception as exc:
            error = f"{type(exc).__name__}: {str(exc)[:300]}"
        latency_ms = round((time.perf_counter() - started) * 1000.0, 3)
        request_id = f"ql-{int(time.time() * 1000)}-{abs(hash((endpoint_s, latency_ms))) % 1000000}"
        result = QuantLabResponse(
            endpoint=endpoint_s,
            ok=ok,
            status_code=status_code,
            data=data,
            error=error,
            latency_ms=latency_ms,
            request_id=request_id,
        )
        self.record_request(result, params=params or {})
        return result

    def record_request(self, response: QuantLabResponse, *, params: Mapping[str, Any]) -> None:
        append_jsonl(
            self.request_log_path,
            {
                "ts": _utc_now_iso(),
                "run_id": self.run_id,
                "phase": self.phase,
                "request_id": response.request_id,
                "method": "GET",
                "endpoint": response.endpoint,
                "params": sanitize_quant_lab_obj(dict(params or {})),
                "status_code": response.status_code,
                "ok": bool(response.ok),
                "latency_ms": response.latency_ms,
                "error": response.error,
                "fallback_used": bool(response.fallback_used),
                "response_summary": summarize_response(response.data),
                "auth_present": bool(os.getenv(str(self.token_env or "QUANT_LAB_API_TOKEN"), "").strip()),
            },
        )

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
        side: str,
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
                "symbol": str(symbol or "").replace("/", "-"),
                "regime": str(regime or "UNKNOWN").strip() or "UNKNOWN",
                "notional_usdt": float(notional_usdt or 0.0),
                "quantile": str(quantile or "p75").strip() or "p75",
                "notional_bucket": notional_bucket,
            },
        )

    def gate_decision(self, alpha_id: str) -> QuantLabResponse:
        return self.get_json(f"/v1/gates/decision/{str(alpha_id).strip()}")
