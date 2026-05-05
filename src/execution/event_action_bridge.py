from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

from src.execution.fill_store import derive_runtime_named_json_path


DEFAULT_EVENT_ACTIONS_PATH = "reports/event_driven_actions.json"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = (PROJECT_ROOT / resolved).resolve()
    return resolved


def _resolve_event_actions_path(
    *,
    path: str = DEFAULT_EVENT_ACTIONS_PATH,
    order_store_path: str | Path | None = None,
) -> Path:
    raw_path = str(path or DEFAULT_EVENT_ACTIONS_PATH).strip() or DEFAULT_EVENT_ACTIONS_PATH
    if order_store_path is not None and raw_path == DEFAULT_EVENT_ACTIONS_PATH:
        return _resolve_path(derive_runtime_named_json_path(order_store_path, "event_driven_actions"))
    return _resolve_path(raw_path)


def _normalize_close_action(action: Dict[str, Any]) -> Dict[str, Any] | None:
    if not isinstance(action, dict):
        return None
    symbol = str(action.get("symbol") or "").strip()
    side_action = str(action.get("action") or "").strip().lower()
    if not symbol or side_action != "close":
        return None
    try:
        raw_priority = action.get("priority", 99)
        priority = 99 if raw_priority is None else int(raw_priority)
    except Exception:
        priority = 99
    normalized = {
        "symbol": symbol,
        "action": "close",
        "reason": str(action.get("reason") or "event_close"),
        "priority": priority,
        "event_type": str(action.get("event_type") or ""),
    }
    for key in ("current_rank", "rank", "rank_source", "source", "price"):
        if key in action:
            normalized[key] = action.get(key)
    return normalized


def persist_event_actions(
    *,
    actions: Iterable[Dict[str, Any]],
    target_run_id: str,
    path: str = DEFAULT_EVENT_ACTIONS_PATH,
    order_store_path: str | Path | None = None,
    generated_at_ms: int | None = None,
) -> bool:
    normalized = [
        item
        for item in (_normalize_close_action(action) for action in (actions or []))
        if item is not None
    ]
    if not normalized or not str(target_run_id or "").strip():
        return False

    payload = {
        "version": 1,
        "target_run_id": str(target_run_id).strip(),
        "generated_at_ms": int(generated_at_ms or int(time.time() * 1000)),
        "actions": normalized,
    }
    out_path = _resolve_event_actions_path(path=path, order_store_path=order_store_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(out_path)
    return True


def clear_event_actions(
    *,
    path: str = DEFAULT_EVENT_ACTIONS_PATH,
    order_store_path: str | Path | None = None,
) -> bool:
    action_path = _resolve_event_actions_path(path=path, order_store_path=order_store_path)
    if not action_path.exists():
        return False
    action_path.unlink(missing_ok=True)
    return True


def consume_event_actions_for_run(
    *,
    run_id: str,
    path: str = DEFAULT_EVENT_ACTIONS_PATH,
    order_store_path: str | Path | None = None,
    max_age_minutes: int = 90,
) -> List[Dict[str, Any]]:
    action_path = _resolve_event_actions_path(path=path, order_store_path=order_store_path)
    if not action_path.exists():
        return []

    try:
        payload = json.loads(action_path.read_text(encoding="utf-8"))
    except Exception:
        return []

    target_run_id = str(payload.get("target_run_id") or "").strip()
    if not target_run_id or target_run_id != str(run_id or "").strip():
        return []

    try:
        generated_at_ms = int(payload.get("generated_at_ms") or 0)
    except Exception:
        generated_at_ms = 0
    if generated_at_ms > 0:
        age_min = max(0.0, (time.time() * 1000 - generated_at_ms) / 60000.0)
        if age_min > float(max_age_minutes):
            try:
                action_path.unlink(missing_ok=True)
            except Exception:
                pass
            return []

    actions = []
    for item in (_normalize_close_action(action) for action in (payload.get("actions") or [])):
        if item is None:
            continue
        item["generated_at_ms"] = generated_at_ms
        item["source_file"] = str(action_path)
        item["target_run_id"] = target_run_id
        actions.append(item)
    try:
        action_path.unlink(missing_ok=True)
    except Exception:
        pass
    return actions
