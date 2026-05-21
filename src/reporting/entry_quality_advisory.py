from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

from configs.schema import AppConfig


PROJECT_ROOT = Path(__file__).resolve().parents[2]

ENTRY_QUALITY_FILES = {
    "missed_low_audit": "missed_low_audit.csv",
    "late_entry_chase_shadow": "late_entry_chase_shadow.csv",
    "late_entry_chase_threshold_advisory": "late_entry_chase_threshold_advisory.json",
    "pullback_reversal_shadow_outcomes": "pullback_reversal_shadow_outcomes.csv",
    "pullback_reversal_readiness": "pullback_reversal_readiness.json",
    "entry_quality_summary": "entry_quality_summary.md",
}

ENTRY_QUALITY_SOURCE_DIRS = (
    PROJECT_ROOT / "reports",
    PROJECT_ROOT / "reports" / "entry_quality",
    PROJECT_ROOT / "reports" / "quant_lab",
    PROJECT_ROOT / "reports" / "quant_lab_latest",
    PROJECT_ROOT / "reports" / "quant_lab" / "latest" / "reports",
    Path("/var/lib/v5-prod"),
    Path("/var/lib/v5-prod/entry_quality"),
    Path("/var/lib/v5-prod/quant_lab"),
    Path("/var/lib/v5-prod/quant_lab/latest/reports"),
)


def _csv_count(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
            return sum(1 for _ in csv.DictReader(fh))
    except Exception:
        return 0


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _find_file(filename: str) -> Path | None:
    for base in ENTRY_QUALITY_SOURCE_DIRS:
        candidate = base / filename
        if candidate.is_file():
            return candidate
    return None


def _nested_value(data: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in data and data.get(key) not in (None, ""):
            return data.get(key)
    for value in data.values():
        if isinstance(value, Mapping):
            found = _nested_value(value, keys)
            if found not in (None, ""):
                return found
    return None


def read_entry_quality_advisory(cfg: AppConfig) -> Dict[str, Any]:
    """Read quant-lab entry quality advisory as audit-only metadata.

    This function intentionally has no order side effects. It only summarizes
    whether advisory inputs are present and records that live switches remain
    disabled unless explicitly changed in config.
    """

    found: Dict[str, str] = {}
    row_counts: Dict[str, int] = {}
    json_payloads: Dict[str, Dict[str, Any]] = {}

    for name, filename in ENTRY_QUALITY_FILES.items():
        path = _find_file(filename)
        if path is None:
            continue
        found[name] = str(path)
        if filename.endswith(".csv"):
            row_counts[name] = _csv_count(path)
        elif filename.endswith(".json"):
            json_payloads[name] = _load_json(path)
        else:
            row_counts[name] = 1

    late_entry = json_payloads.get("late_entry_chase_threshold_advisory", {})
    pullback = json_payloads.get("pullback_reversal_readiness", {})
    available = bool(found)
    execution = getattr(cfg, "execution", None)
    return {
        "status": "available" if available else "quant_lab_entry_quality_unavailable",
        "available": available,
        "live_order_effect": "read_only_no_hard_block",
        "late_entry_chase_guard_enabled": bool(getattr(execution, "late_entry_chase_guard_enabled", False)),
        "pullback_reversal_live_enabled": bool(getattr(execution, "pullback_reversal_live_enabled", False)),
        "source_files": found,
        "row_counts": row_counts,
        "late_entry_chase_ready_for_live_guard": _nested_value(
            late_entry,
            ("ready_for_live_guard", "late_entry_chase_ready_for_live_guard", "ready_for_live"),
        ),
        "pullback_reversal_ready_for_paper": _nested_value(
            pullback,
            ("ready_for_paper", "pullback_reversal_ready_for_paper"),
        ),
        "pullback_reversal_ready_for_live_probe": _nested_value(
            pullback,
            ("ready_for_live_probe", "pullback_reversal_ready_for_live_probe", "ready_for_live"),
        ),
    }
