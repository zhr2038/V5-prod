from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from configs.runtime_config import load_runtime_config, resolve_runtime_config_path, resolve_runtime_path
from src.execution.fill_store import derive_runtime_named_artifact_path


log = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RATE_LIMIT_OKX_CODES = {"50011", "50061"}
_STORE_CACHE_LOCK = threading.Lock()
_STORE_CACHE: dict[str, "APITelemetryStore"] = {}


def is_rate_limited(
    *,
    http_status: Optional[int],
    okx_code: Optional[str],
    error_text: str | None = None,
) -> bool:
    if int(http_status or 0) == 429:
        return True
    if str(okx_code or "").strip() in RATE_LIMIT_OKX_CODES:
        return True

    text = str(error_text or "").strip().lower()
    if not text:
        return False
    return (
        "429" in text
        or "too many requests" in text
        or "rate limit" in text
        or "too frequent" in text
    )


def classify_api_status(*, http_status: Optional[int], okx_code: Optional[str]) -> str:
    if is_rate_limited(http_status=http_status, okx_code=okx_code):
        return "429"

    code = str(okx_code or "").strip()
    status = int(http_status or 0)
    if status >= 500:
        return "5xx"
    if status >= 400:
        return "4xx"
    if status >= 200:
        if code and code != "0":
            return "okx_error"
        return "2xx"
    return "transport_error"


def _resolve_path(path: str | Path, *, project_root: Path) -> Path:
    resolved = Path(str(path))
    if not resolved.is_absolute():
        resolved = (project_root / resolved).resolve()
    return resolved


def resolve_api_telemetry_path(
    raw_path: str | None = None,
    *,
    project_root: Path | None = None,
) -> Path:
    root = (project_root or PROJECT_ROOT).resolve()
    candidate = str(raw_path or "").strip() or str(os.getenv("V5_API_TELEMETRY_DB_PATH") or "").strip()
    if candidate:
        return _resolve_path(candidate, project_root=root)

    config_path = Path(resolve_runtime_config_path(project_root=root)).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"runtime config not found: {config_path}")
    cfg = load_runtime_config(project_root=root)
    if not isinstance(cfg, dict) or not cfg:
        raise ValueError(f"runtime config is empty or invalid: {config_path}")
    execution_cfg = cfg.get("execution")
    if not isinstance(execution_cfg, dict):
        raise ValueError(f"runtime config missing execution section: {config_path}")
    order_store_path = Path(
        resolve_runtime_path(
            execution_cfg.get("order_store_path"),
            default="reports/orders.sqlite",
            project_root=root,
        )
    ).resolve()
    return derive_runtime_named_artifact_path(order_store_path, "api_telemetry", ".sqlite").resolve()


@dataclass(frozen=True)
class APITelemetryRecord:
    ts_ms: int
    exchange: str
    method: str
    endpoint: str
    status_class: str
    http_status: Optional[int]
    okx_code: Optional[str]
    okx_msg: Optional[str]
    duration_ms: float
    rate_limited: bool
    attempt: int
    error_type: Optional[str]


class APITelemetryStore:
    def __init__(self, path: str | Path | None = None):
        self.path = resolve_api_telemetry_path(str(path) if path is not None else None)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path), timeout=5.0)
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    def _init_db(self) -> None:
        try:
            with self._connect() as conn:
                conn.execute("PRAGMA journal_mode = WAL")
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_request_log (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      ts_ms INTEGER NOT NULL,
                      exchange TEXT NOT NULL,
                      method TEXT NOT NULL,
                      endpoint TEXT NOT NULL,
                      status_class TEXT NOT NULL,
                      http_status INTEGER,
                      okx_code TEXT,
                      okx_msg TEXT,
                      duration_ms REAL NOT NULL,
                      rate_limited INTEGER NOT NULL DEFAULT 0,
                      attempt INTEGER NOT NULL DEFAULT 1,
                      error_type TEXT
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_api_request_log_ts_ms ON api_request_log(ts_ms)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_api_request_log_endpoint ON api_request_log(endpoint, method)"
                )
        except Exception as exc:
            log.warning("api telemetry init failed for %s: %s", self.path, exc)
            raise

    def record(self, record: APITelemetryRecord) -> None:
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO api_request_log(
                      ts_ms, exchange, method, endpoint, status_class,
                      http_status, okx_code, okx_msg, duration_ms,
                      rate_limited, attempt, error_type
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        int(record.ts_ms),
                        str(record.exchange),
                        str(record.method),
                        str(record.endpoint),
                        str(record.status_class),
                        int(record.http_status) if record.http_status is not None else None,
                        str(record.okx_code) if record.okx_code is not None else None,
                        str(record.okx_msg) if record.okx_msg is not None else None,
                        float(record.duration_ms),
                        1 if record.rate_limited else 0,
                        max(1, int(record.attempt)),
                        str(record.error_type) if record.error_type is not None else None,
                    ),
                )
        except Exception as exc:
            log.warning(
                "api telemetry write failed for %s %s %s: %s",
                record.method,
                record.endpoint,
                record.status_class,
                exc,
            )


def get_api_telemetry_store(path: str | Path | None = None) -> APITelemetryStore:
    resolved = resolve_api_telemetry_path(str(path) if path is not None else None)
    cache_key = str(resolved)
    with _STORE_CACHE_LOCK:
        store = _STORE_CACHE.get(cache_key)
        if store is None:
            store = APITelemetryStore(resolved)
            _STORE_CACHE[cache_key] = store
        return store


def record_api_request(
    *,
    exchange: str,
    method: str,
    endpoint: str,
    duration_ms: float,
    status_class: str,
    http_status: Optional[int] = None,
    okx_code: Optional[str] = None,
    okx_msg: Optional[str] = None,
    rate_limited: bool = False,
    attempt: int = 1,
    error_type: Optional[str] = None,
    path: str | Path | None = None,
) -> None:
    try:
        store = get_api_telemetry_store(path)
    except Exception as exc:
        log.warning("api telemetry unavailable for %s %s: %s", method, endpoint, exc)
        return

    store.record(
        APITelemetryRecord(
            ts_ms=int(time.time() * 1000),
            exchange=str(exchange),
            method=str(method).upper(),
            endpoint=str(endpoint),
            status_class=str(status_class),
            http_status=(int(http_status) if http_status is not None else None),
            okx_code=(str(okx_code) if okx_code is not None else None),
            okx_msg=(str(okx_msg) if okx_msg is not None else None),
            duration_ms=max(0.0, float(duration_ms)),
            rate_limited=bool(rate_limited),
            attempt=max(1, int(attempt)),
            error_type=(str(error_type) if error_type is not None else None),
        )
    )
