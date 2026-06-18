from __future__ import annotations

import json
import os
import socket
import stat
import warnings
from ipaddress import ip_address
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests

from src.reporting.quant_lab_audit import (
    CONTRACT_VERSION,
    EVENT_TYPE_REQUEST,
    SCHEMA_VERSION,
    append_quant_lab_request,
    normalize_quant_lab_event,
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


def _header_lookup(headers: Mapping[str, Any] | None, name: str) -> str:
    target = str(name or "").strip().lower()
    for key, value in dict(headers or {}).items():
        if str(key).strip().lower() == target:
            return str(value or "").strip()
    return ""


def _header_float(headers: Mapping[str, Any] | None, *names: str) -> Optional[float]:
    for name in names:
        value = _header_lookup(headers, name)
        if not value:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _header_bool(headers: Mapping[str, Any] | None, *names: str) -> Optional[bool]:
    for name in names:
        value = _header_lookup(headers, name)
        if not value:
            continue
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _response_elapsed_ms(resp: Any) -> Optional[float]:
    elapsed = getattr(resp, "elapsed", None)
    total_seconds = getattr(elapsed, "total_seconds", None)
    if callable(total_seconds):
        try:
            return round(float(total_seconds()) * 1000.0, 3)
        except (TypeError, ValueError):
            return None
    return None


def _response_byte_count(resp: Any, data: Any = None) -> Optional[int]:
    content = getattr(resp, "content", None)
    if isinstance(content, (bytes, bytearray)):
        return len(content)
    text = getattr(resp, "text", None)
    if isinstance(text, str):
        return len(text.encode("utf-8", errors="replace"))
    if data is not None:
        try:
            return len(json.dumps(sanitize_quant_lab_obj(data), ensure_ascii=False).encode("utf-8"))
        except Exception:
            return None
    return None


@dataclass
class ApiEnvTokenReadResult:
    token: Optional[str] = None
    path_present: bool = False
    secure_permissions: Optional[bool] = None
    token_loaded: bool = False
    warning: Optional[str] = None
    mode: Optional[str] = None
    symlink: bool = False
    regular_file: bool = False


def _mode_text(mode_bits: Optional[int]) -> Optional[str]:
    if mode_bits is None:
        return None
    return format(int(mode_bits) & 0o777, "04o")


def inspect_api_env_file(
    path_value: Any,
    *,
    allow_symlink: bool = False,
    require_secure_permissions: bool = True,
) -> ApiEnvTokenReadResult:
    if path_value is None or str(path_value).strip() == "":
        return ApiEnvTokenReadResult()
    path = Path(str(path_value)).expanduser()
    try:
        st_lstat = path.lstat()
    except FileNotFoundError:
        return ApiEnvTokenReadResult(path_present=False)
    except OSError as exc:
        return ApiEnvTokenReadResult(path_present=False, secure_permissions=False, warning=f"api_env_stat_failed:{type(exc).__name__}")

    is_symlink = stat.S_ISLNK(st_lstat.st_mode)
    if is_symlink and not bool(allow_symlink):
        return ApiEnvTokenReadResult(
            path_present=True,
            secure_permissions=False,
            warning="api_env_symlink_disallowed",
            mode=_mode_text(stat.S_IMODE(st_lstat.st_mode)),
            symlink=True,
            regular_file=False,
        )

    try:
        st = path.stat() if is_symlink else st_lstat
    except OSError as exc:
        return ApiEnvTokenReadResult(
            path_present=True,
            secure_permissions=False,
            warning=f"api_env_stat_failed:{type(exc).__name__}",
            mode=_mode_text(stat.S_IMODE(st_lstat.st_mode)),
            symlink=is_symlink,
        )
    mode_bits = stat.S_IMODE(st.st_mode) & 0o777
    is_regular = stat.S_ISREG(st.st_mode)
    if not is_regular:
        return ApiEnvTokenReadResult(
            path_present=True,
            secure_permissions=False,
            warning="api_env_not_regular_file",
            mode=_mode_text(mode_bits),
            symlink=is_symlink,
            regular_file=False,
        )
    if bool(require_secure_permissions):
        if mode_bits & 0o022:
            return ApiEnvTokenReadResult(
                path_present=True,
                secure_permissions=False,
                warning=f"api_env_group_or_world_writable:{_mode_text(mode_bits)}",
                mode=_mode_text(mode_bits),
                symlink=is_symlink,
                regular_file=True,
            )
        if mode_bits > 0o640:
            return ApiEnvTokenReadResult(
                path_present=True,
                secure_permissions=False,
                warning=f"api_env_permissions_too_open:{_mode_text(mode_bits)}",
                mode=_mode_text(mode_bits),
                symlink=is_symlink,
                regular_file=True,
            )
    return ApiEnvTokenReadResult(
        path_present=True,
        secure_permissions=True,
        mode=_mode_text(mode_bits),
        symlink=is_symlink,
        regular_file=True,
    )


def read_token_from_env_file(
    path_value: Any,
    token_env: str,
    *,
    allow_symlink: bool = False,
    require_secure_permissions: bool = True,
    mode: str = "shadow",
) -> ApiEnvTokenReadResult:
    status = inspect_api_env_file(
        path_value,
        allow_symlink=allow_symlink,
        require_secure_permissions=require_secure_permissions,
    )
    if not status.path_present:
        return status
    if status.secure_permissions is False:
        message = status.warning or "api_env_insecure_permissions"
        if str(mode or "shadow").strip().lower().replace("-", "_") in STRICT_GATE_MODES:
            raise QuantLabValidationError(f"quant-lab api_env_path is not secure: {message}")
        warnings.warn(f"quant-lab api_env_path skipped: {message}", RuntimeWarning, stacklevel=2)
        return status
    return _read_token_from_env_file_unchecked(path_value, token_env, status)


def _read_token_from_env_file_unchecked(path_value: Any, token_env: str, status: ApiEnvTokenReadResult) -> ApiEnvTokenReadResult:
    if path_value is None or str(path_value).strip() == "":
        return status
    path = Path(str(path_value)).expanduser()
    target_key = str(token_env or "").strip()
    if not target_key:
        status.warning = "api_env_token_env_missing"
        return status
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        status.warning = f"api_env_read_failed:{type(exc).__name__}"
        return status
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() != target_key:
            continue
        token = value.strip().strip('"').strip("'")
        status.token = token or None
        status.token_loaded = bool(status.token)
        return status
    status.warning = "api_env_token_key_missing"
    return status


def _read_token_from_env_file(path_value: Any, token_env: str) -> Optional[str]:
    return read_token_from_env_file(path_value, token_env).token


def summarize_response(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {"type": type(payload).__name__}
    keys = (
        "status",
        "service",
        "mode",
        "warnings",
        "permission",
        "decision",
        "allowed_modes",
        "cost_model_version",
        "gate_version",
        "source",
        "fallback_level",
        "sample_count",
        "total_cost_bps",
        "total_cost_bps_p50",
        "total_cost_bps_p75",
        "total_cost_bps_p90",
        "required_edge_bps",
        "cost_bps",
        "symbol",
        "normalized_symbol",
        "alpha_id",
        "contract_version",
    )
    summary = {key: payload.get(key) for key in keys if key in payload}
    for nested_key in ("data", "result", "payload"):
        nested = payload.get(nested_key)
        if isinstance(nested, Mapping):
            for key in keys:
                if key in nested and key not in summary:
                    summary[key] = nested.get(key)
    return sanitize_quant_lab_obj(summary)


def _permission_expires_epoch(value: Any) -> Optional[float]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _permission_cache_expires_epoch(value: Any, cache_ttl_seconds: int) -> Optional[float]:
    ttl_seconds = max(0, int(cache_ttl_seconds or 0))
    if ttl_seconds <= 0:
        return None
    remote_expires_epoch = _permission_expires_epoch(value)
    if remote_expires_epoch is None:
        return None
    now = time.time()
    if remote_expires_epoch <= now:
        return None
    return min(remote_expires_epoch, now + float(ttl_seconds))


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
    headers: Dict[str, str] = field(default_factory=dict)

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
    cost_cache_ttl_seconds: int = 300
    http_client: Optional[Any] = None
    request_log_path: str | Path = "reports/quant_lab_requests.jsonl"
    http_cache_path: str | Path = "reports/quant_lab_http_cache.json"
    run_id: Optional[str] = None
    phase: str = "live"
    client_id: str = "v5.quant_lab_client"
    user_agent: str = "v5-quant-lab-client/1.0"
    _cache: TTLCache = field(init=False, repr=False)
    _cache_headers: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], Dict[str, str]] = field(init=False, repr=False)
    _stale_cache: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], Any] = field(init=False, repr=False)
    _permission_cache: Dict[Tuple[str, str], Tuple[float, RiskPermission]] = field(init=False, repr=False)
    _cost_cache: Dict[
        Tuple[str, Tuple[Tuple[str, str], ...]],
        Tuple[float, Any, Dict[str, str]],
    ] = field(init=False, repr=False)
    _resolved_host_cache: Dict[str, str] = field(init=False, repr=False)
    token_auth_disabled_reason: Optional[str] = field(default=None, init=False)
    api_env_path_present: bool = False
    api_env_secure_permissions: Optional[bool] = None
    api_env_token_loaded: bool = False
    api_env_warning: Optional[str] = None

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
        self.cost_cache_ttl_seconds = max(0, int(self.cost_cache_ttl_seconds or 0))
        self.http_client = self.http_client or requests.Session()
        self._cache = TTLCache(ttl_seconds=self.cache_ttl_seconds)
        self._cache_headers = {}
        self._stale_cache = {}
        self._permission_cache = {}
        self._cost_cache = {}
        self._resolved_host_cache = {}
        self._load_persistent_http_cache()

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
        env_status = ApiEnvTokenReadResult()
        mode_value = str(mode or getattr(cfg, "mode", "shadow") or "shadow")
        if not token:
            env_status = read_token_from_env_file(
                getattr(cfg, "api_env_path", None),
                token_env,
                allow_symlink=bool(getattr(cfg, "allow_api_env_symlink", False)),
                require_secure_permissions=bool(getattr(cfg, "api_env_require_secure_permissions", True)),
                mode=mode_value,
            )
            token = env_status.token or ""
        else:
            env_status = inspect_api_env_file(
                getattr(cfg, "api_env_path", None),
                allow_symlink=bool(getattr(cfg, "allow_api_env_symlink", False)),
                require_secure_permissions=bool(getattr(cfg, "api_env_require_secure_permissions", True)),
            )
        client = cls(
            base_url=str(getattr(cfg, "base_url", "") or ""),
            api_token=token or None,
            mode=mode_value,
            allow_insecure_http_with_token=bool(getattr(cfg, "allow_insecure_http_with_token", False)),
            timeout_seconds=float(getattr(cfg, "timeout_seconds", 2.0) or 2.0),
            max_retries=int(getattr(cfg, "max_retries", 1) or 0),
            cache_ttl_seconds=int(getattr(cfg, "cache_ttl_seconds", 60) or 0),
            cost_cache_ttl_seconds=int(getattr(cfg, "cost_cache_ttl_seconds", 300) or 0),
            http_client=http_client,
            request_log_path=str(getattr(cfg, "request_log_path", "reports/quant_lab_requests.jsonl") or "reports/quant_lab_requests.jsonl"),
            http_cache_path=str(getattr(cfg, "http_cache_path", "reports/quant_lab_http_cache.json") or "reports/quant_lab_http_cache.json"),
            run_id=run_id,
            phase=phase,
        )
        client.api_env_path_present = bool(env_status.path_present)
        client.api_env_secure_permissions = env_status.secure_permissions
        client.api_env_token_loaded = bool(env_status.token_loaded)
        client.api_env_warning = env_status.warning
        return client

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
        self.token_auth_disabled_reason = "public_http_token_stripped"  # noqa: S105 - status reason, not a secret
        warnings.warn(message, RuntimeWarning, stacklevel=2)

    def _headers(self, *, etag: Optional[str] = None) -> Dict[str, str]:
        headers = {
            "Accept": "application/json",
            "User-Agent": self.user_agent,
            "X-Quant-Lab-Client-Id": self.client_id,
        }
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        if etag:
            headers["If-None-Match"] = str(etag)
        return headers

    @staticmethod
    def _cache_key(endpoint_path: str, params: Mapping[str, Any]) -> Tuple[str, Tuple[Tuple[str, str], ...]]:
        clean = {str(k): str(v) for k, v in dict(params or {}).items() if v is not None}
        return endpoint_path, tuple(sorted(clean.items()))

    @staticmethod
    def _persistent_cache_enabled_for_endpoint(endpoint_path: str) -> bool:
        endpoint = "/" + str(endpoint_path or "").lstrip("/")
        endpoint = endpoint.split("?", 1)[0].rstrip("/")
        return endpoint.startswith(
            (
                "/v1/strategy-opportunity-advisory",
                "/v1/strategy_opportunity_advisory",
                "/v1/reports/strategy-opportunity-advisory",
            )
        )

    @staticmethod
    def _persistent_key_payload(
        key: Tuple[str, Tuple[Tuple[str, str], ...]]
    ) -> dict[str, Any]:
        return {"endpoint": key[0], "params": dict(key[1])}

    @classmethod
    def _persistent_key_text(
        cls,
        key: Tuple[str, Tuple[Tuple[str, str], ...]],
    ) -> str:
        return json.dumps(cls._persistent_key_payload(key), sort_keys=True, separators=(",", ":"))

    @classmethod
    def _persistent_key_from_text(
        cls,
        text: str,
    ) -> Optional[Tuple[str, Tuple[Tuple[str, str], ...]]]:
        try:
            payload = json.loads(str(text or ""))
        except Exception:
            return None
        if not isinstance(payload, Mapping):
            return None
        endpoint = str(payload.get("endpoint") or "").strip()
        params = payload.get("params") or {}
        if not endpoint or not isinstance(params, Mapping):
            return None
        return cls._cache_key(endpoint, params)

    def _load_persistent_http_cache(self) -> None:
        path = Path(str(self.http_cache_path or "")).expanduser()
        if not str(path):
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(payload, Mapping):
            return
        if str(payload.get("base_url") or "") != self.base_url:
            return
        now = time.time()
        entries = payload.get("entries") or {}
        if not isinstance(entries, Mapping):
            return
        for key_text, entry in entries.items():
            key = self._persistent_key_from_text(str(key_text))
            if key is None or not isinstance(entry, Mapping):
                continue
            if not self._persistent_cache_enabled_for_endpoint(key[0]):
                continue
            data = entry.get("data")
            headers = entry.get("headers") if isinstance(entry.get("headers"), Mapping) else {}
            self._stale_cache[key] = data
            self._cache_headers[key] = {str(k).lower(): str(v) for k, v in dict(headers or {}).items()}
            try:
                expires_at = float(entry.get("expires_at") or 0.0)
            except (TypeError, ValueError):
                expires_at = 0.0
            if expires_at > now and self.cache_ttl_seconds > 0:
                self._cache._items[key] = (expires_at, data)

    def _save_persistent_http_cache(self) -> None:
        path = Path(str(self.http_cache_path or "")).expanduser()
        if not str(path):
            return
        entries: dict[str, Any] = {}
        now = time.time()
        max_entries = 200
        persistent_items = [
            (key, data)
            for key, data in self._stale_cache.items()
            if self._persistent_cache_enabled_for_endpoint(key[0])
        ]
        for key, data in persistent_items[-max_entries:]:
            headers = dict(self._cache_headers.get(key) or {})
            expires_at = now + float(max(0, self.cache_ttl_seconds))
            cached_item = self._cache._items.get(key)
            if cached_item is not None:
                try:
                    expires_at = float(cached_item[0])
                except (TypeError, ValueError):
                    pass
            entries[self._persistent_key_text(key)] = {
                "endpoint": key[0],
                "params": dict(key[1]),
                "headers": headers,
                "data": data,
                "expires_at": expires_at,
                "updated_at": _utc_now_iso(),
            }
        payload = {
            "schema_version": "v5.quant_lab_http_cache.v1",
            "base_url": self.base_url,
            "updated_at": _utc_now_iso(),
            "entries": entries,
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = path.with_name(f".{path.name}.tmp")
            tmp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
            tmp_path.replace(path)
        except Exception:
            return

    def _resolved_host_for_url(self, url: str) -> str:
        parsed = urlparse(url)
        host = str(parsed.hostname or "").strip()
        if not host:
            return ""
        cached = self._resolved_host_cache.get(host)
        if cached is not None:
            return cached
        try:
            infos = socket.getaddrinfo(host, parsed.port or (443 if parsed.scheme == "https" else 80), type=socket.SOCK_STREAM)
            addresses = []
            for info in infos:
                sockaddr = info[-1]
                if sockaddr:
                    addresses.append(str(sockaddr[0]))
            value = ",".join(sorted(set(addresses))[:4])
        except Exception:
            value = ""
        self._resolved_host_cache[host] = value
        return value

    @staticmethod
    def _cost_semantic_cache_key(
        params: Mapping[str, Any],
    ) -> Tuple[str, Tuple[Tuple[str, str], ...]]:
        raw = dict(params or {})
        symbol = symbol_to_quant_lab_symbol(
            raw.get("normalized_symbol")
            or raw.get("symbol")
            or raw.get("request_symbol")
            or ""
        )
        try:
            notional = round(float(raw.get("notional_usdt") or 0.0), 2)
        except (TypeError, ValueError):
            notional = 0.0
        semantic = (
            ("symbol", symbol),
            ("regime", str(raw.get("regime") or raw.get("requested_regime") or "normal").strip()),
            ("notional_bucket", str(raw.get("notional_bucket") or "").strip()),
            ("notional_usdt_rounded", f"{notional:.2f}"),
            ("quantile", str(raw.get("quantile") or raw.get("requested_quantile") or "p75").strip()),
            ("side", str(raw.get("side") or "").strip().lower()),
            ("instrument_type", str(raw.get("instrument_type") or "spot").strip().lower()),
            ("venue", str(raw.get("venue") or "OKX").strip().upper()),
            ("strategy_id", str(raw.get("strategy_id") or "v5").strip()),
        )
        return "/v1/costs/estimate", semantic

    def _cached_cost_response(
        self,
        params: Mapping[str, Any],
    ) -> Optional[QuantLabResponse]:
        if self.cost_cache_ttl_seconds <= 0:
            return None
        key = self._cost_semantic_cache_key(params)
        cached = self._cost_cache.get(key)
        if cached is None:
            return None
        expires_at, data, headers = cached
        if expires_at <= time.time():
            self._cost_cache.pop(key, None)
            return None
        response_headers = dict(headers or {})
        response_headers["x-quant-lab-client-cost-cache-hit"] = "true"
        response = QuantLabResponse(
            endpoint="/v1/costs/estimate",
            ok=True,
            status_code=200,
            data=data,
            latency_ms=0.0,
            request_id=str(dict(params or {}).get("request_id") or "") or None,
            cached=True,
            headers=response_headers,
        )
        self._log_request(
            endpoint_path="/v1/costs/estimate",
            params=params,
            status_code=200,
            latency_ms=0.0,
            success=True,
            error_type=None,
            cached=True,
            response_summary=summarize_response(data),
            response_headers=response_headers,
            network_meta={"response_bytes": _response_byte_count(None, data), "download_ms": 0.0},
        )
        return response

    def _store_cost_response(
        self,
        params: Mapping[str, Any],
        response: QuantLabResponse,
    ) -> None:
        if self.cost_cache_ttl_seconds <= 0 or not response.ok:
            return
        key = self._cost_semantic_cache_key(params)
        self._cost_cache[key] = (
            time.time() + self.cost_cache_ttl_seconds,
            response.data,
            dict(response.headers or {}),
        )

    def _get_cost_json(self, params: Mapping[str, Any]) -> QuantLabResponse:
        cached_response = self._cached_cost_response(params)
        if cached_response is not None:
            return cached_response
        response = self.get_json("/v1/costs/estimate", params=params)
        self._store_cost_response(params, response)
        return response

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
        response_headers: Optional[Mapping[str, Any]] = None,
        network_meta: Optional[Mapping[str, Any]] = None,
    ) -> None:
        headers = dict(response_headers or {})
        net = dict(network_meta or {})
        append_quant_lab_request(
            self.request_log_path,
            {
                "run_id": self.run_id,
                "phase": self.phase,
                "request_id": dict(params or {}).get("request_id"),
                "event_id": dict(params or {}).get("event_id"),
                "ts_utc": dict(params or {}).get("ts_utc"),
                "contract_version": dict(params or {}).get("contract_version"),
                "event_type": EVENT_TYPE_REQUEST,
                "endpoint_path": endpoint_path,
                "query_keys": sorted(str(k) for k, v in dict(params or {}).items() if v is not None),
                "status_code": status_code,
                "latency_ms": latency_ms,
                "success": bool(success),
                "fallback_used": False,
                "error_type": error_type,
                "error_message_short": error_type or "",
                "cached": bool(cached),
                "resolved_host": net.get("resolved_host") or "",
                "connect_ms": net.get("connect_ms"),
                "ttfb_ms": net.get("ttfb_ms"),
                "download_ms": net.get("download_ms"),
                "response_bytes": net.get("response_bytes"),
                "server_header_lake_scan_ms": _header_float(
                    headers,
                    "x-quant-lab-lake-scan-ms",
                    "x-advisory-lake-scan-ms",
                ),
                "server_header_serialize_ms": _header_float(
                    headers,
                    "x-quant-lab-serialize-ms",
                    "x-advisory-serialize-ms",
                ),
                "server_header_source_signature_ms": _header_float(
                    headers,
                    "x-quant-lab-source-signature-ms",
                    "x-advisory-source-signature-ms",
                ),
                "server_cache_hit": _header_bool(
                    headers,
                    "x-quant-lab-api-cache-hit",
                    "x-advisory-cache-hit",
                ),
                "response_cache_hit": _header_bool(
                    headers,
                    "x-quant-lab-response-cache-hit",
                    "x-advisory-response-cache-hit",
                ),
                "client_cache_hit": bool(cached),
                "response_summary": dict(response_summary or {}),
            },
        )

    def get_json(self, endpoint_path: str, params: Optional[Mapping[str, Any]] = None) -> QuantLabResponse:
        endpoint = "/" + str(endpoint_path or "").lstrip("/")
        clean_params = {k: v for k, v in dict(params or {}).items() if v is not None}
        key = self._cache_key(endpoint, clean_params)
        cached_value = self._cache.get(key)
        if cached_value is not None:
            cached_headers = dict(self._cache_headers.get(key) or {})
            cached_headers["x-quant-lab-client-cache-hit"] = "true"
            response = QuantLabResponse(
                endpoint=endpoint,
                ok=True,
                status_code=200,
                data=cached_value,
                latency_ms=0.0,
                cached=True,
                headers=cached_headers,
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
                response_headers=cached_headers,
                network_meta={"response_bytes": _response_byte_count(None, cached_value), "download_ms": 0.0},
            )
            return response

        url = urljoin(self.base_url + "/", endpoint.lstrip("/"))
        attempts = self.max_retries + 1
        last_error: Optional[BaseException] = None
        started_all = time.perf_counter()
        status_code: Optional[int] = None
        data: Any = None
        resolved_host = self._resolved_host_for_url(url)
        for attempt in range(attempts):
            started = time.perf_counter()
            try:
                cached_headers = self._cache_headers.get(key) or {}
                request_etag = cached_headers.get("etag")
                resp = self.http_client.get(
                    url,
                    params=clean_params,
                    headers=self._headers(etag=request_etag),
                    timeout=self.timeout_seconds,
                )
                status_code = int(getattr(resp, "status_code", 0) or 0)
                response_headers = {
                    str(header_key).lower(): str(value)
                    for header_key, value in dict(getattr(resp, "headers", {}) or {}).items()
                }
                latency_ms = round((time.perf_counter() - started) * 1000.0, 3)
                ttfb_ms = _response_elapsed_ms(resp)
                if status_code == 304 and key in self._stale_cache:
                    data = self._stale_cache[key]
                    self._cache.set(key, data)
                    self._cache_headers[key] = {**cached_headers, **response_headers}
                    self._save_persistent_http_cache()
                    response_bytes = _response_byte_count(resp, data)
                    download_ms = (
                        max(0.0, round(latency_ms - ttfb_ms, 3))
                        if ttfb_ms is not None
                        else None
                    )
                    response = QuantLabResponse(
                        endpoint=endpoint,
                        ok=True,
                        status_code=status_code,
                        data=data,
                        latency_ms=latency_ms,
                        cached=True,
                        headers={**self._cache_headers.get(key, {}), "x-quant-lab-client-cache-hit": "true"},
                    )
                    self._log_request(
                        endpoint_path=endpoint,
                        params=clean_params,
                        status_code=status_code,
                        latency_ms=latency_ms,
                        success=True,
                        error_type=None,
                        cached=True,
                        response_summary=summarize_response(data),
                        response_headers=response.headers,
                        network_meta={
                            "resolved_host": resolved_host,
                            "connect_ms": None,
                            "ttfb_ms": ttfb_ms,
                            "download_ms": download_ms,
                            "response_bytes": response_bytes,
                        },
                    )
                    return response
                try:
                    data = resp.json()
                except ValueError:
                    data = {"text": str(getattr(resp, "text", ""))[:500]}
                response_bytes = _response_byte_count(resp, data)
                download_ms = (
                    max(0.0, round(latency_ms - ttfb_ms, 3))
                    if ttfb_ms is not None
                    else None
                )
                if 200 <= status_code < 300:
                    self._cache.set(key, data)
                    self._stale_cache[key] = data
                    self._cache_headers[key] = response_headers
                    self._save_persistent_http_cache()
                    response = QuantLabResponse(
                        endpoint=endpoint,
                        ok=True,
                        status_code=status_code,
                        data=data,
                        latency_ms=latency_ms,
                        headers=response_headers,
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
                        response_headers=response_headers,
                        network_meta={
                            "resolved_host": resolved_host,
                            "connect_ms": None,
                            "ttfb_ms": ttfb_ms,
                            "download_ms": download_ms,
                            "response_bytes": response_bytes,
                        },
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
            network_meta={"resolved_host": resolved_host},
        )
        response = QuantLabResponse(
            endpoint=endpoint,
            ok=False,
            status_code=status_code,
            data=data,
            error=error_type,
            latency_ms=latency_ms,
            headers={},
        )
        if isinstance(last_error, QuantLabTimeout):
            raise QuantLabTimeout(str(last_error))
        if isinstance(last_error, QuantLabHTTPError):
            raise QuantLabHTTPError(str(last_error))
        raise QuantLabUnavailable(str(last_error or "quant-lab unavailable"))

    def get_health(self) -> QuantLabHealth:
        response = self.get_json("/v1/health")
        health = QuantLabHealth.from_payload(response.data)
        self._validate_health(health, endpoint="/v1/health", allow_warning=False)
        return health

    def get_deep_health(self) -> QuantLabHealth:
        response = self.get_json("/v1/health/deep")
        health = QuantLabHealth.from_payload(response.data)
        self._validate_health(health, endpoint="/v1/health/deep", allow_warning=True)
        return health

    @staticmethod
    def _validate_health(
        health: QuantLabHealth,
        *,
        endpoint: str,
        allow_warning: bool,
    ) -> None:
        allowed_status = {"ok", "healthy"}
        if allow_warning:
            allowed_status.add("warning")
        status_text = str(health.status or "").strip().lower()
        if status_text not in allowed_status:
            raise QuantLabValidationError(
                f"quant-lab {endpoint} status is not ok: {health.status!r}"
            )
        if str(health.mode).lower() != "read-only":
            raise QuantLabValidationError(
                f"quant-lab {endpoint} mode must be read-only, got {health.mode!r}"
            )

    def get_live_permission(
        self,
        *,
        strategy: str,
        version: str,
        request_id: Optional[str] = None,
        event_id: Optional[str] = None,
        ts_utc: Optional[str] = None,
    ) -> RiskPermission:
        strategy_text = str(strategy or "").strip()
        version_text = str(version or "").strip()
        permission_request_id = str(request_id or "").strip() or f"{self.run_id or 'v5'}:permission:{strategy_text}:{version_text}"
        request_event = normalize_quant_lab_event(
            {
                "schema_version": SCHEMA_VERSION,
                "contract_version": CONTRACT_VERSION,
                "run_id": self.run_id,
                "event_type": EVENT_TYPE_REQUEST,
                "request_id": permission_request_id,
                "event_id": str(event_id or "").strip() or None,
                "ts_utc": str(ts_utc or "").strip() or None,
                "endpoint_path": "/v1/risk/live-permission",
            },
            default_event_type=EVENT_TYPE_REQUEST,
        )
        permission_params = {
            "schema_version": request_event["schema_version"],
            "contract_version": request_event["contract_version"],
            "event_id": request_event["event_id"],
            "request_id": permission_request_id,
            "run_id": self.run_id or "",
            "ts_utc": request_event["ts_utc"],
            "strategy": strategy_text,
            "version": version_text,
        }
        permission_cache_key = (strategy_text, version_text)
        cached_permission = self._permission_cache.get(permission_cache_key)
        if cached_permission is not None and cached_permission[0] > time.time():
            cached_payload = cached_permission[1].to_dict()
            self._log_request(
                endpoint_path="/v1/risk/live-permission",
                params=permission_params,
                status_code=200,
                latency_ms=0.0,
                success=True,
                error_type=None,
                cached=True,
                response_summary=summarize_response(cached_payload),
                response_headers={"x-quant-lab-client-permission-cache-hit": "true"},
                network_meta={"response_bytes": _response_byte_count(None, cached_payload), "download_ms": 0.0},
            )
            return cached_permission[1]
        response = self.get_json(
            "/v1/risk/live-permission",
            params=permission_params,
        )
        permission = RiskPermission.from_payload(response.data)
        permission.permission = normalize_permission(permission.permission)
        expires_epoch = _permission_cache_expires_epoch(
            permission.expires_at,
            self.cache_ttl_seconds,
        )
        if expires_epoch is not None:
            self._permission_cache[permission_cache_key] = (expires_epoch, permission)
        return permission

    def estimate_cost(
        self,
        *,
        symbol: str,
        regime: str,
        notional_usdt: float,
        quantile: str = "p75",
        side: str = "",
        strategy_id: str = "v5",
        expected_edge_bps: Optional[float] = None,
        request_id: Optional[str] = None,
        event_id: Optional[str] = None,
        ts_utc: Optional[str] = None,
        venue: str = "OKX",
        instrument_type: str = "spot",
    ) -> CostEstimate:
        normalized_symbol = symbol_to_quant_lab_symbol(symbol)
        request_symbol = str(symbol or "").strip()
        requested_regime = str(regime or "normal").strip() or "normal"
        requested_quantile = str(quantile or "p75").strip() or "p75"
        strategy = str(strategy_id or "v5").strip() or "v5"
        cost_request_id = str(request_id or "").strip()
        if not cost_request_id:
            cost_request_id = f"{self.run_id or 'v5'}:{normalized_symbol}:{time.time_ns()}"
        request_event = normalize_quant_lab_event(
            {
                "schema_version": SCHEMA_VERSION,
                "contract_version": CONTRACT_VERSION,
                "run_id": self.run_id,
                "event_type": EVENT_TYPE_REQUEST,
                "request_id": cost_request_id,
                "event_id": str(event_id or "").strip() or None,
                "ts_utc": str(ts_utc or "").strip() or None,
                "endpoint_path": "/v1/costs/estimate",
                "symbol": request_symbol,
                "normalized_symbol": normalized_symbol,
            },
            default_event_type=EVENT_TYPE_REQUEST,
        )
        params = {
            "schema_version": request_event["schema_version"],
            "contract_version": request_event["contract_version"],
            "event_id": request_event["event_id"],
            "request_id": cost_request_id,
            "run_id": self.run_id or "",
            "ts_utc": request_event["ts_utc"],
            "symbol": request_symbol,
            "request_symbol": request_symbol,
            "normalized_symbol": normalized_symbol,
            "venue": str(venue or "OKX").strip() or "OKX",
            "instrument_type": str(instrument_type or "spot").strip() or "spot",
            "side": str(side or "").strip().lower(),
            "regime": requested_regime,
            "requested_regime": requested_regime,
            "notional_usdt": float(notional_usdt or 0.0),
            "quantile": requested_quantile,
            "requested_quantile": requested_quantile,
            "strategy_id": strategy,
            "expected_edge_bps": expected_edge_bps if expected_edge_bps is not None else "",
        }
        response = self._get_cost_json(params)
        estimate = CostEstimate.from_payload(response.data)
        if not estimate.symbol:
            estimate.symbol = normalized_symbol
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
        expected_edge_bps: Optional[float] = None,
        request_id: Optional[str] = None,
        event_id: Optional[str] = None,
        ts_utc: Optional[str] = None,
        strategy_id: Optional[str] = None,
        venue: str = "OKX",
        instrument_type: str = "spot",
    ) -> QuantLabResponse:
        normalized_symbol = symbol_to_quant_lab_symbol(symbol)
        request_symbol = str(symbol or "").strip()
        requested_regime = str(regime or "normal").strip() or "normal"
        requested_quantile = str(quantile or "p75").strip() or "p75"
        strategy = str(strategy_id or alpha_id or "v5").strip() or "v5"
        cost_request_id = str(request_id or "").strip()
        if not cost_request_id:
            cost_request_id = f"{self.run_id or 'v5'}:{normalized_symbol}:{time.time_ns()}"
        request_event = normalize_quant_lab_event(
            {
                "schema_version": SCHEMA_VERSION,
                "contract_version": CONTRACT_VERSION,
                "run_id": self.run_id,
                "event_type": EVENT_TYPE_REQUEST,
                "request_id": cost_request_id,
                "event_id": str(event_id or "").strip() or None,
                "ts_utc": str(ts_utc or "").strip() or None,
                "endpoint_path": "/v1/costs/estimate",
                "symbol": request_symbol,
                "normalized_symbol": normalized_symbol,
            },
            default_event_type=EVENT_TYPE_REQUEST,
        )
        params = {
            "schema_version": request_event["schema_version"],
            "contract_version": request_event["contract_version"],
            "event_id": request_event["event_id"],
            "request_id": cost_request_id,
            "run_id": self.run_id or "",
            "ts_utc": request_event["ts_utc"],
            "symbol": request_symbol,
            "request_symbol": request_symbol,
            "normalized_symbol": normalized_symbol,
            "venue": str(venue or "OKX").strip() or "OKX",
            "instrument_type": str(instrument_type or "spot").strip() or "spot",
            "side": str(side or "").strip().lower(),
            "regime": requested_regime,
            "requested_regime": requested_regime,
            "notional_usdt": float(notional_usdt or 0.0),
            "quantile": requested_quantile,
            "requested_quantile": requested_quantile,
            "strategy_id": strategy,
            "expected_edge_bps": expected_edge_bps if expected_edge_bps is not None else "",
            "notional_bucket": notional_bucket,
        }
        return self._get_cost_json(params)

    def gate_decision(self, alpha_id: str) -> QuantLabResponse:
        return self.get_json(f"/v1/gates/decision/{str(alpha_id).strip()}")


def append_jsonl(path: str | Path, payload: Mapping[str, Any]) -> None:
    from src.reporting.quant_lab_audit import append_quant_lab_usage

    append_quant_lab_usage(path, payload)
