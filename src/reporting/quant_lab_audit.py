from __future__ import annotations

import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional


SCHEMA_VERSION = "1.0.0"
CONTRACT_VERSION = "v5.quant_lab.telemetry.v2"
EVENT_ID_GENERATION_VERSION = "quant_lab_event_id_v1"
EVENT_TYPE_REQUEST = "request"
EVENT_TYPE_FALLBACK = "fallback"
EVENT_TYPE_COST_USAGE = "cost_usage"
EVENT_TYPE_PERMISSION_AUDIT = "permission_audit"
EVENT_TYPE_COMPLIANCE = "compliance"
EVENT_TYPE_HEALTH_CHECK = "health_check"
EVENT_TYPES = {
    EVENT_TYPE_REQUEST,
    EVENT_TYPE_FALLBACK,
    EVENT_TYPE_COST_USAGE,
    EVENT_TYPE_PERMISSION_AUDIT,
    EVENT_TYPE_COMPLIANCE,
    EVENT_TYPE_HEALTH_CHECK,
}
EVENT_TYPE_ALIASES = {
    "quant_lab_request": EVENT_TYPE_REQUEST,
    "cost_estimate_request": EVENT_TYPE_REQUEST,
    "request_not_ok": EVENT_TYPE_REQUEST,
    "health": EVENT_TYPE_HEALTH_CHECK,
    "live_permission": EVENT_TYPE_PERMISSION_AUDIT,
    "final_permission": EVENT_TYPE_PERMISSION_AUDIT,
    "filter_order": EVENT_TYPE_PERMISSION_AUDIT,
    "order_filter": EVENT_TYPE_PERMISSION_AUDIT,
    "permission": EVENT_TYPE_PERMISSION_AUDIT,
    "run_summary": EVENT_TYPE_COMPLIANCE,
    "cost_estimate": EVENT_TYPE_COST_USAGE,
}

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
    "secret-token",
    "super-secret-token",
)

NON_SECRET_EXACT_KEYS = {
    "api_env_path_present",
    "api_env_secure_permissions",
    "api_env_token_loaded",
    "api_env_warning",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _stable_digest(payload: Mapping[str, Any], fields: Iterable[str]) -> str:
    material = {field: payload.get(field) for field in fields if payload.get(field) not in (None, "")}
    raw = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def normalize_quant_lab_event(row: Mapping[str, Any], *, default_event_type: str) -> Dict[str, Any]:
    payload = dict(row)
    ts = str(payload.get("ts_utc") or payload.get("ts") or utc_now_iso())
    input_event_type = str(payload.get("event_type") or default_event_type)
    event_type = EVENT_TYPE_ALIASES.get(input_event_type, input_event_type)
    if event_type not in EVENT_TYPES:
        event_type = EVENT_TYPE_ALIASES.get(default_event_type, default_event_type)
    if event_type not in EVENT_TYPES:
        event_type = EVENT_TYPE_PERMISSION_AUDIT
    payload["schema_version"] = str(payload.get("schema_version") or SCHEMA_VERSION)
    payload["contract_version"] = str(payload.get("contract_version") or CONTRACT_VERSION)
    payload["event_id_generation_version"] = str(
        payload.get("event_id_generation_version") or EVENT_ID_GENERATION_VERSION
    )
    payload["run_id"] = str(payload.get("run_id") or "unknown")
    payload["legacy_event_type"] = str(payload.get("legacy_event_type") or input_event_type)
    payload["event_type"] = event_type
    payload["ts_utc"] = ts
    payload["ts"] = str(payload.get("ts") or ts)
    if not payload.get("endpoint_path") and payload.get("endpoint"):
        payload["endpoint_path"] = payload.get("endpoint")
    payload.setdefault("endpoint_path", "")
    payload.setdefault("status_code", None)
    payload.setdefault("latency_ms", None)
    if "success" not in payload:
        payload["success"] = False if event_type == EVENT_TYPE_FALLBACK or payload.get("error_type") else True
    if event_type == EVENT_TYPE_FALLBACK:
        payload["fallback_used"] = True
    else:
        payload["fallback_used"] = bool(payload.get("fallback_used", False))
    payload.setdefault("error_type", None)
    error_message = (
        payload.get("error_message_short")
        or payload.get("error_message_sanitized")
        or payload.get("error")
        or ""
    )
    payload["error_message_short"] = str(sanitize_quant_lab_obj(str(error_message)[:240])) if error_message else ""
    payload.setdefault("original_request_id", payload.get("request_id") if event_type == EVENT_TYPE_FALLBACK else "")
    payload.setdefault("original_event_id", "")
    if not payload.get("request_id"):
        payload["request_id"] = "qlreq_" + _stable_digest(
            payload,
            (
                "event_id_generation_version",
                "schema_version",
                "contract_version",
                "run_id",
                "endpoint_path",
                "endpoint",
                "symbol",
                "normalized_symbol",
                "ts_utc",
            ),
        )[:24]
    if not payload.get("event_id"):
        payload["event_id"] = "qlevent_" + _stable_digest(
            payload,
            (
                "event_id_generation_version",
                "schema_version",
                "contract_version",
                "run_id",
                "event_type",
                "legacy_event_type",
                "request_id",
                "endpoint_path",
                "endpoint",
                "symbol",
                "normalized_symbol",
                "ts_utc",
                "status_code",
                "filter_reason",
                "error_type",
            ),
        )[:32]
    if event_type == EVENT_TYPE_FALLBACK and not payload.get("original_event_id"):
        payload["original_event_id"] = payload["event_id"]
    return payload


def sanitize_quant_lab_obj(value: Any) -> Any:
    if isinstance(value, Mapping):
        out: Dict[str, Any] = {}
        for key, item in value.items():
            key_s = str(key)
            key_l = key_s.lower()
            if key_l in NON_SECRET_EXACT_KEYS:
                out[key_s] = sanitize_quant_lab_obj(item)
            elif any(part in key_l for part in SECRET_KEY_PARTS):
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
    payload = normalize_quant_lab_event(row, default_event_type=EVENT_TYPE_PERMISSION_AUDIT)
    _append_jsonl(path, payload)


def append_quant_lab_request(path: str | Path, row: Mapping[str, Any]) -> None:
    payload = normalize_quant_lab_event({"method": "GET", **dict(row)}, default_event_type=EVENT_TYPE_REQUEST)
    payload["method"] = "GET"
    if payload.get("status_code") == 200 and payload.get("success") is True:
        payload["fallback_used"] = False
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
