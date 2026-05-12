from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class QuantLabMode(str, Enum):
    LOCAL_ONLY = "local_only"
    SHADOW = "shadow"
    COST_ONLY = "cost_only"
    PERMISSION_ONLY = "permission_only"
    ENFORCE = "enforce"


STRICT_PERMISSION_MODES = {QuantLabMode.PERMISSION_ONLY, QuantLabMode.ENFORCE}


@dataclass
class QuantLabModeResolution:
    mode: QuantLabMode
    mode_source: str = "config"
    override_path: Optional[str] = None
    override_reason: Optional[str] = None
    override_updated_by: Optional[str] = None
    override_updated_at: Optional[str] = None
    warning: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["mode"] = self.mode.value
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


def resolve_mode_path(path_value: Any) -> Path:
    path = Path(str(path_value or "state/quant_lab_mode.json"))
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


def resolve_quant_lab_mode(cfg: Any) -> QuantLabModeResolution:
    qcfg = _ql_cfg(cfg)
    config_mode = normalize_quant_lab_mode(getattr(qcfg, "mode", QuantLabMode.SHADOW.value))
    override_path = resolve_mode_path(getattr(qcfg, "runtime_override_path", "state/quant_lab_mode.json"))
    resolution = QuantLabModeResolution(
        mode=config_mode,
        mode_source="config",
        override_path=str(override_path),
    )
    if not bool(getattr(qcfg, "allow_runtime_override", True)):
        return resolution
    if not override_path.exists():
        return resolution
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
        return QuantLabModeResolution(
            mode=override_mode,
            mode_source="runtime_override",
            override_path=str(override_path),
            override_reason=str(payload.get("reason") or "") or None,
            override_updated_by=str(payload.get("updated_by") or "") or None,
            override_updated_at=str(payload.get("updated_at") or "") or None,
            warning=warning,
        )
    except Exception as exc:
        resolution.mode_source = "config_invalid_override"
        resolution.warning = f"invalid quant-lab mode override ignored: {type(exc).__name__}"
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
) -> Path:
    target = resolve_mode_path(path)
    payload = {
        "mode": normalize_quant_lab_mode(mode).value,
        "reason": str(reason or ""),
        "updated_by": str(updated_by or "operator"),
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    if confirm_unsafe_fallback:
        payload["confirm_unsafe_fallback"] = True
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(target)
    return target
