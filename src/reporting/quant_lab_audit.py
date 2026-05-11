from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional


SECRET_KEY_PARTS = (
    "api_key",
    "apikey",
    "api-secret",
    "api_secret",
    "authorization",
    "bearer",
    "ok-access-key",
    "ok-access-passphrase",
    "ok-access-sign",
    "passphrase",
    "password",
    "private_key",
    "private-key",
    "quant_lab_api_token",
    "secret",
    "secret_key",
    "token",
)

SECRET_VALUE_MARKERS = (
    "BEGIN PRIVATE KEY",
    "Bearer ",
    "OK-ACCESS-KEY",
    "OK-ACCESS-PASSPHRASE",
    "OK-ACCESS-SIGN",
)


def utc_now_iso() -> str:
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
    if isinstance(value, str):
        if any(marker.lower() in value.lower() for marker in SECRET_VALUE_MARKERS):
            return "<REDACTED>"
    return value


def _append_jsonl(path: str | Path, row: Mapping[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = sanitize_quant_lab_obj(dict(row))
    with target.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def append_quant_lab_usage(path: str | Path, row: Mapping[str, Any]) -> None:
    payload = {"ts": utc_now_iso(), **dict(row)}
    _append_jsonl(path, payload)


def append_quant_lab_request(path: str | Path, row: Mapping[str, Any]) -> None:
    payload = {"ts": utc_now_iso(), "method": "GET", **dict(row)}
    payload["method"] = "GET"
    _append_jsonl(path, payload)


def read_quant_lab_usage(path: str | Path) -> list[Dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    rows: list[Dict[str, Any]] = []
    for line in target.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def summarize_quant_lab_usage(path: str | Path, since_ts: Optional[str] = None) -> Dict[str, Any]:
    rows = read_quant_lab_usage(path)
    if since_ts:
        rows = [row for row in rows if str(row.get("ts") or "") >= str(since_ts)]
    latest_permission = None
    latest_final = None
    cost_model_version = None
    gate_version = None
    for row in rows:
        permission = row.get("permission") or row.get("quant_lab_permission") or row.get("quant_lab_decision")
        final = row.get("final_permission") or row.get("effective_decision")
        if permission:
            latest_permission = permission
        if final:
            latest_final = final
        if row.get("cost_model_version"):
            cost_model_version = row.get("cost_model_version")
        if row.get("gate_version"):
            gate_version = row.get("gate_version")
    return {
        "request_count": len([row for row in rows if row.get("endpoint")]),
        "error_count": len([row for row in rows if row.get("success") is False or row.get("ok") is False]),
        "fallback_count": len([row for row in rows if row.get("fallback_used")]),
        "filtered_by_cost_count": len(
            [row for row in rows if row.get("order_filtered") and str(row.get("filter_reason", "")).startswith("quant_lab_cost")]
        ),
        "filtered_by_permission_count": len(
            [row for row in rows if row.get("order_filtered") and "permission" in str(row.get("filter_reason", ""))]
        ),
        "latest_permission": latest_permission,
        "final_permission": latest_final,
        "cost_model_version": cost_model_version,
        "gate_version": gate_version,
    }


def iter_recent_rows(rows: Iterable[Mapping[str, Any]], since_ts: Optional[str] = None) -> Iterable[Mapping[str, Any]]:
    for row in rows:
        if since_ts and str(row.get("ts") or "") < str(since_ts):
            continue
        yield row
