from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class QuantLabMode(str, Enum):
    LOCAL_ONLY = "local_only"
    SHADOW = "shadow"
    COST_ONLY = "cost_only"
    PERMISSION_ONLY = "permission_only"
    ENFORCE = "enforce"


STRICT_PERMISSION_MODES = {QuantLabMode.PERMISSION_ONLY, QuantLabMode.ENFORCE}
CONTRACT_VERSION = "v5.quant_lab.telemetry.v2"
TELEMETRY_SCHEMA_VERSION = "1.0.0"


@dataclass
class QuantLabReadinessResult:
    status: str = "NOT_CHECKED"
    reasons: list[str] = None
    source: str = "not_checked"
    contract_version_match: Optional[bool] = None
    telemetry_schema_version_match: Optional[bool] = None
    remote_permission_status: Optional[str] = None
    remote_permission_enforceable: Optional[bool] = None
    remote_permission_expires_at: Optional[str] = None
    cost_degraded_rate: Optional[float] = None
    global_default_cost_count: Optional[int] = None
    fallback_rate: Optional[float] = None
    summary_trade_count_mismatch_count: Optional[int] = None
    snapshot: dict[str, Any] = None

    def __post_init__(self) -> None:
        if self.reasons is None:
            self.reasons = []
        if self.snapshot is None:
            self.snapshot = {}

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class QuantLabModeResolution:
    mode: QuantLabMode
    requested_mode: Optional[QuantLabMode] = None
    mode_source: str = "config"
    override_path: Optional[str] = None
    override_reason: Optional[str] = None
    override_updated_by: Optional[str] = None
    override_updated_at: Optional[str] = None
    warning: Optional[str] = None
    enforce_readiness: Optional[QuantLabReadinessResult] = None

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["mode"] = self.mode.value
        out["requested_mode"] = (self.requested_mode or self.mode).value
        out["quant_lab_requested_mode"] = out["requested_mode"]
        out["quant_lab_effective_mode"] = out["mode"]
        readiness = self.enforce_readiness or QuantLabReadinessResult()
        out["enforce_readiness_status"] = readiness.status
        out["enforce_blocked_reasons"] = list(readiness.reasons)
        out["contract_version_match"] = readiness.contract_version_match
        out["telemetry_schema_version_match"] = readiness.telemetry_schema_version_match
        return out


def normalize_quant_lab_mode(value: Any) -> QuantLabMode:
    raw = str(value or QuantLabMode.SHADOW.value).strip().lower().replace("-", "_")
    for mode in QuantLabMode:
        if raw == mode.value:
            return mode
    raise ValueError(f"invalid quant_lab mode: {value!r}")


def normalize_quant_lab_fail_policy(value: Any) -> str:
    policy = str(value or "sell_only").strip().lower()
    if policy == "allow":
        return "allow_local_fallback"
    return policy


def quant_lab_mode_needs_fallback_confirmation(qcfg: Any, mode: QuantLabMode) -> bool:
    return (
        mode in STRICT_PERMISSION_MODES
        and normalize_quant_lab_fail_policy(getattr(qcfg, "fail_policy", "sell_only")) == "allow_local_fallback"
        and not bool(getattr(qcfg, "allow_local_fallback_in_enforce", False))
    )


def _ql_cfg(cfg: Any) -> Any:
    return getattr(cfg, "quant_lab", cfg)


def _get(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


def resolve_mode_path(path_value: Any) -> Path:
    path = Path(str(path_value or "state/quant_lab_mode.json"))
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


def _parse_utc(ts: Any) -> Optional[datetime]:
    if ts in (None, ""):
        return None
    text = str(ts).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _as_float(value: Any) -> Optional[float]:
    if value in (None, "", "not_observable", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> Optional[int]:
    number = _as_float(value)
    return int(number) if number is not None else None


def _first_int(*values: Any) -> Optional[int]:
    for value in values:
        number = _as_int(value)
        if number is not None:
            return number
    return None


def _truthy(value: Any) -> Optional[bool]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _read_json_file(path: Path) -> Optional[dict[str, Any]]:
    try:
        if path.is_file():
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else None
    except Exception:
        return None
    return None


def _default_readiness_payload(qcfg: Any) -> tuple[dict[str, Any], str]:
    configured = str(getattr(qcfg, "enforce_readiness_path", "state/quant_lab_enforce_readiness.json") or "").strip()
    candidates = []
    if configured:
        candidates.append(resolve_mode_path(configured))
    candidates.extend(
        [
            PROJECT_ROOT / "summaries" / "window_summary.json",
            PROJECT_ROOT / "reports" / "summaries" / "window_summary.json",
            PROJECT_ROOT / "reports" / "window_summary.json",
        ]
    )
    for path in candidates:
        payload = _read_json_file(path)
        if payload is not None:
            return payload, str(path)
    return {}, "inferred"


def evaluate_enforce_readiness(
    qcfg: Any,
    *,
    permission: Any = None,
    readiness_payload: Optional[Mapping[str, Any]] = None,
) -> QuantLabReadinessResult:
    if not bool(getattr(qcfg, "enforce_readiness_enabled", True)):
        return QuantLabReadinessResult(status="READY", source="disabled", snapshot={"disabled": True})

    snapshot, source = (dict(readiness_payload), "provided") if readiness_payload is not None else _default_readiness_payload(qcfg)
    reasons: list[str] = []

    required_contract = str(getattr(qcfg, "enforce_readiness_required_contract_version", CONTRACT_VERSION) or CONTRACT_VERSION)
    required_schema = str(getattr(qcfg, "enforce_readiness_required_schema_version", TELEMETRY_SCHEMA_VERSION) or TELEMETRY_SCHEMA_VERSION)

    permission_status = (
        _get(permission, "permission_status")
        or _get(permission, "status")
        or snapshot.get("remote_permission_status")
        or snapshot.get("raw_permission_status")
        or snapshot.get("permission_status")
    )
    permission_status_text = str(permission_status or "").strip().upper()
    permission_expires_at = _get(permission, "expires_at") or snapshot.get("remote_permission_expires_at") or snapshot.get("expires_at")
    permission_enforceable = _truthy(
        _get(permission, "enforceable")
        if permission is not None
        else snapshot.get("raw_permission_enforceable", snapshot.get("remote_permission_enforceable"))
    )
    expires_dt = _parse_utc(permission_expires_at)

    if not permission_status_text.startswith("ACTIVE_"):
        reasons.append("remote_permission_not_active")
    if expires_dt is None:
        reasons.append("remote_permission_expiry_missing")
    elif expires_dt <= datetime.now(timezone.utc):
        reasons.append("remote_permission_expired")
    if permission_enforceable is not True:
        reasons.append("remote_permission_not_enforceable")

    cost_rows = _first_int(
        snapshot.get("post_deployment_cost_usage_rows"),
        snapshot.get("cost_usage_current_contract_rows"),
        snapshot.get("readiness_cost_usage_rows"),
        snapshot.get("quant_lab_cost_usage_rows", snapshot.get("cost_usage_rows")),
    )
    degraded_count = _first_int(
        snapshot.get("post_deployment_cost_degraded_count"),
        snapshot.get("current_contract_cost_degraded_count"),
        snapshot.get("readiness_cost_degraded_count"),
        snapshot.get("cost_degraded_count", snapshot.get("quant_lab_cost_degraded_count")),
    )
    if snapshot.get("cost_degraded_rate") not in (None, ""):
        cost_degraded_rate = _as_float(snapshot.get("cost_degraded_rate"))
    elif cost_rows and degraded_count is not None:
        cost_degraded_rate = float(degraded_count) / float(cost_rows) if cost_rows > 0 else 0.0
    else:
        cost_degraded_rate = None
    max_cost_degraded_rate = float(getattr(qcfg, "enforce_readiness_max_cost_degraded_rate", 0.0) or 0.0)
    if cost_degraded_rate is None:
        reasons.append("cost_degraded_rate_missing")
    elif cost_degraded_rate > max_cost_degraded_rate:
        reasons.append("cost_degraded_rate_high")

    global_default_count = _first_int(
        snapshot.get("post_deployment_global_default_cost_count"),
        snapshot.get("current_contract_global_default_cost_count"),
        snapshot.get("readiness_global_default_cost_count"),
        snapshot.get("global_default_cost_count", snapshot.get("quant_lab_global_default_cost_count")),
    )
    max_global_default_count = int(getattr(qcfg, "enforce_readiness_max_global_default_cost_count", 0) or 0)
    if global_default_count is None:
        reasons.append("global_default_cost_count_missing")
    elif global_default_count > max_global_default_count:
        reasons.append("global_default_cost_count_high")

    request_count = _as_int(snapshot.get("quant_lab_request_count", snapshot.get("request_count")))
    fallback_count = _as_int(
        snapshot.get("quant_lab_fallback_count", snapshot.get("quant_lab_actual_fallback_count", snapshot.get("fallback_count")))
    )
    if snapshot.get("fallback_rate") not in (None, ""):
        fallback_rate = _as_float(snapshot.get("fallback_rate"))
    elif request_count and fallback_count is not None:
        fallback_rate = float(fallback_count) / float(request_count) if request_count > 0 else 0.0
    else:
        fallback_rate = None
    max_fallback_rate = float(getattr(qcfg, "enforce_readiness_max_fallback_rate", 0.0) or 0.0)
    if fallback_rate is None:
        reasons.append("fallback_rate_missing")
    elif fallback_rate > max_fallback_rate:
        reasons.append("fallback_rate_high")

    observed_contract = (
        _get(permission, "contract_version")
        or snapshot.get("telemetry_contract_version")
        or snapshot.get("contract_version")
        or snapshot.get("quant_lab_contract_version")
        or snapshot.get("cost_contract_version")
    )
    contract_match = bool(observed_contract and str(observed_contract) == required_contract)
    if not contract_match:
        reasons.append("contract_version_mismatch")

    observed_schema = snapshot.get("telemetry_schema_version") or snapshot.get("schema_version")
    schema_match = bool(observed_schema and str(observed_schema) == required_schema)
    if not schema_match:
        reasons.append("telemetry_schema_version_mismatch")

    mismatch_count = _as_int(snapshot.get("summary_trade_count_mismatch_count"))
    if mismatch_count is None:
        reasons.append("summary_trade_count_mismatch_missing")
    elif mismatch_count != 0:
        reasons.append("summary_trade_count_mismatch_present")

    status = "READY" if not reasons else "BLOCKED"
    result_snapshot = dict(snapshot)
    result_snapshot.update(
        {
            "required_contract_version": required_contract,
            "required_telemetry_schema_version": required_schema,
            "observed_contract_version": observed_contract,
            "observed_telemetry_schema_version": observed_schema,
        }
    )
    return QuantLabReadinessResult(
        status=status,
        reasons=reasons,
        source=source,
        contract_version_match=contract_match,
        telemetry_schema_version_match=schema_match,
        remote_permission_status=permission_status_text or None,
        remote_permission_enforceable=permission_enforceable,
        remote_permission_expires_at=str(permission_expires_at) if permission_expires_at else None,
        cost_degraded_rate=cost_degraded_rate,
        global_default_cost_count=global_default_count,
        fallback_rate=fallback_rate,
        summary_trade_count_mismatch_count=mismatch_count,
        snapshot=result_snapshot,
    )


def resolve_quant_lab_mode(cfg: Any) -> QuantLabModeResolution:
    qcfg = _ql_cfg(cfg)
    config_mode = normalize_quant_lab_mode(getattr(qcfg, "mode", QuantLabMode.SHADOW.value))
    override_path = resolve_mode_path(getattr(qcfg, "runtime_override_path", "state/quant_lab_mode.json"))
    resolution = QuantLabModeResolution(
        mode=config_mode,
        requested_mode=config_mode,
        mode_source="config",
        override_path=str(override_path),
    )
    if not bool(getattr(qcfg, "allow_runtime_override", True)):
        return _apply_readiness_file_override(qcfg, resolution)
    if not override_path.exists():
        return _apply_readiness_file_override(qcfg, resolution)
    try:
        payload = json.loads(override_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("override payload must be an object")
        override_mode = normalize_quant_lab_mode(payload.get("mode"))
        confirmed_unsafe_fallback = bool(payload.get("confirm_unsafe_fallback"))
        if quant_lab_mode_needs_fallback_confirmation(qcfg, override_mode) and not confirmed_unsafe_fallback:
            resolution.mode_source = "config_unsafe_override"
            resolution.warning = (
                "unsafe quant-lab mode override ignored: permission_only/enforce with "
                "fail_policy=allow_local_fallback requires allow_local_fallback_in_enforce=true "
                "or confirm_unsafe_fallback=true"
            )
            return resolution
        warning = None
        if quant_lab_mode_needs_fallback_confirmation(qcfg, override_mode) and confirmed_unsafe_fallback:
            warning = "unsafe allow_local_fallback accepted by runtime override confirmation"
        resolution = QuantLabModeResolution(
            mode=override_mode,
            requested_mode=override_mode,
            mode_source="runtime_override",
            override_path=str(override_path),
            override_reason=str(payload.get("reason") or "") or None,
            override_updated_by=str(payload.get("updated_by") or "") or None,
            override_updated_at=str(payload.get("updated_at") or "") or None,
            warning=warning,
        )
        return _apply_readiness_file_override(qcfg, resolution)
    except Exception as exc:
        resolution.mode_source = "config_invalid_override"
        resolution.warning = f"invalid quant-lab mode override ignored: {type(exc).__name__}"
        return resolution


def _apply_readiness_file_override(qcfg: Any, resolution: QuantLabModeResolution) -> QuantLabModeResolution:
    if resolution.mode != QuantLabMode.ENFORCE:
        return resolution
    if not bool(getattr(qcfg, "enforce_readiness_enabled", True)):
        return resolution
    readiness_path = resolve_mode_path(getattr(qcfg, "enforce_readiness_path", "state/quant_lab_enforce_readiness.json"))
    payload = _read_json_file(readiness_path)
    if payload is None:
        return resolution
    readiness = evaluate_enforce_readiness(qcfg, readiness_payload=payload)
    resolution.enforce_readiness = readiness
    if readiness.status != "READY":
        resolution.mode = QuantLabMode.SHADOW
        resolution.mode_source = f"{resolution.mode_source}_enforce_blocked"
        resolution.warning = "quant-lab enforce requested but readiness is BLOCKED; effective mode downgraded to shadow"
    return resolution


def load_quant_lab_mode(cfg: Any) -> QuantLabMode:
    return resolve_quant_lab_mode(cfg).mode


def write_quant_lab_mode_override(
    *,
    mode: str,
    reason: str,
    updated_by: str = "operator",
    path: str | Path = "state/quant_lab_mode.json",
    confirm_unsafe_fallback: bool = False,
    confirmed: bool = False,
    confirmation_method: Optional[str] = None,
) -> Path:
    target = resolve_mode_path(path)
    payload = {
        "mode": normalize_quant_lab_mode(mode).value,
        "reason": str(reason or ""),
        "updated_by": str(updated_by or "operator"),
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "confirmed": bool(confirmed),
    }
    if confirmation_method:
        payload["confirmation_method"] = str(confirmation_method)
    if confirm_unsafe_fallback:
        payload["confirm_unsafe_fallback"] = True
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(target)
    return target
