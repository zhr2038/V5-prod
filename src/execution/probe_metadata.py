from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional


PROBE_POSITION_TYPES = {"market_impulse_probe", "btc_leadership_probe"}


def _float_or_none(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _explicit_false(value: Any) -> bool:
    if isinstance(value, bool):
        return not value
    return str(value or "").strip().lower() in {"0", "false", "no", "off"}


def _dict_or_none(value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return None
        return dict(parsed) if isinstance(parsed, dict) else None
    return None


def probe_type_from_meta(meta: Any) -> Optional[str]:
    if not isinstance(meta, dict):
        return None
    raw_probe_type = str(meta.get("probe_type") or "").strip()
    if raw_probe_type in PROBE_POSITION_TYPES:
        return raw_probe_type
    raw_entry_reason = str(meta.get("entry_reason") or "").strip()
    if raw_entry_reason in PROBE_POSITION_TYPES:
        return raw_entry_reason
    for probe_type in PROBE_POSITION_TYPES:
        if bool(meta.get(probe_type, False)):
            return probe_type
    return None


def probe_tags_from_order_meta(
    meta: Any,
    *,
    entry_px: float | None = None,
    entry_ts: str | None = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(meta, dict):
        return None
    probe_type = probe_type_from_meta(meta)
    if probe_type is None:
        return None

    ts = str(entry_ts or meta.get("entry_ts") or "").strip()
    if not ts:
        ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    out: Dict[str, Any] = {
        "entry_reason": str(meta.get("entry_reason") or probe_type),
        "entry_ts": ts,
        "probe_type": probe_type,
    }
    px_raw = entry_px if entry_px is not None else meta.get("entry_px")
    try:
        px = float(px_raw)
        if px > 0:
            out["entry_px"] = px
    except Exception:
        pass
    try:
        target_w = meta.get("target_w")
        if target_w is not None:
            out["target_w"] = float(target_w)
    except Exception:
        pass
    try:
        out["highest_net_bps"] = float(meta.get("highest_net_bps", 0.0) or 0.0)
    except Exception:
        out["highest_net_bps"] = 0.0
    for key in PROBE_POSITION_TYPES:
        if key in meta:
            out[key] = bool(meta.get(key))
    return out


def swing_tags_from_order_meta(
    meta: Any,
    *,
    entry_ts: str | None = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(meta, dict):
        return None
    if "swing_hold_position" in meta and _explicit_false(meta.get("swing_hold_position")):
        return None
    candidates = [meta]
    for key in (
        "swing_meta",
        "swing_decision_meta",
        "entry_router_decision",
        "router_decision",
        "raw_meta",
        "raw_json",
    ):
        nested = _dict_or_none(meta.get(key))
        if nested:
            candidates.append(nested)

    source = next((candidate for candidate in candidates if _truthy(candidate.get("swing_hold_position"))), None)
    if source is None:
        return None

    ts = str(
        entry_ts
        or meta.get("swing_entry_ts")
        or meta.get("entry_ts")
        or source.get("swing_entry_ts")
        or source.get("entry_ts")
        or ""
    ).strip()
    if not ts:
        ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    out: Dict[str, Any] = {
        "swing_hold_position": True,
        "swing_entry_ts": ts,
        "entry_reason": str(meta.get("entry_reason") or source.get("entry_reason") or "normal_entry"),
    }
    min_hold = _float_or_none(meta.get("swing_min_hold_hours"))
    if min_hold is None:
        min_hold = _float_or_none(source.get("swing_min_hold_hours"))
    if min_hold is not None:
        out["swing_min_hold_hours"] = float(min_hold)

    for key in (
        "alpha6_score",
        "alpha6_side",
        "f4_volume_expansion",
        "f5_rsi_trend_confirm",
        "current_level",
    ):
        if key in meta and meta.get(key) is not None:
            out[key] = meta.get(key)
        elif key in source and source.get(key) is not None:
            out[key] = source.get(key)
    return out


def position_tags_from_order_meta(
    meta: Any,
    *,
    entry_px: float | None = None,
    entry_ts: str | None = None,
) -> Optional[Dict[str, Any]]:
    tags: Dict[str, Any] = {}
    probe_tags = probe_tags_from_order_meta(meta, entry_px=entry_px, entry_ts=entry_ts)
    if probe_tags:
        tags.update(probe_tags)
    swing_tags = swing_tags_from_order_meta(meta, entry_ts=entry_ts)
    if swing_tags:
        tags.update(swing_tags)
        px = _float_or_none(entry_px if entry_px is not None else meta.get("entry_px") if isinstance(meta, dict) else None)
        if px is not None and px > 0 and "entry_px" not in tags:
            tags["entry_px"] = float(px)
    return tags or None
