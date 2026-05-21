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
ENTRY_QUALITY_STRATEGY_CANDIDATES = {
    "v5.entry_quality_missed_low_audit",
    "v5.late_entry_chase_guard_shadow",
    "v5.pullback_reversal_shadow_btc",
    "v5.pullback_reversal_shadow_sol",
    "v5.pullback_reversal_shadow_eth",
    "v5.pullback_reversal_shadow_bnb",
}
STRATEGY_ADVISORY_READER_PATHS = (
    PROJECT_ROOT / "reports" / "summaries" / "strategy_opportunity_advisory_reader.csv",
    PROJECT_ROOT / "reports" / "strategy_opportunity_advisory.csv",
    PROJECT_ROOT / "reports" / "quant_lab" / "strategy_opportunity_advisory.csv",
    PROJECT_ROOT / "reports" / "quant_lab_latest" / "strategy_opportunity_advisory.csv",
    PROJECT_ROOT / "reports" / "quant_lab" / "latest" / "reports" / "strategy_opportunity_advisory.csv",
    Path("/var/lib/v5-prod/strategy_opportunity_advisory.csv"),
)


def _csv_count(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
            return sum(1 for _ in csv.DictReader(fh))
    except Exception:
        return 0


def _read_csv(path: Path) -> list[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
            return [dict(row) for row in csv.DictReader(fh) if row]
    except Exception:
        return []


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


def _strategy_key(row: Mapping[str, Any]) -> str:
    values = (
        row.get("strategy_candidate"),
        row.get("advisory_strategy_candidate"),
        row.get("strategy_id"),
        row.get("advisory_strategy_id"),
        row.get("experiment_name"),
    )
    for value in values:
        text = str(value or "").strip().lower()
        if text in ENTRY_QUALITY_STRATEGY_CANDIDATES:
            return text
    return ""


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _read_strategy_advisory_rows() -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    for path in STRATEGY_ADVISORY_READER_PATHS:
        if not path.is_file():
            continue
        for row in _read_csv(path):
            strategy_key = _strategy_key(row)
            if not strategy_key:
                continue
            payload = {
                "strategy_candidate": strategy_key,
                "symbol": str(row.get("symbol") or ""),
                "decision": str(row.get("decision") or row.get("advisory_decision") or ""),
                "recommended_mode": str(row.get("recommended_mode") or row.get("advisory_recommended_mode") or ""),
                "advisory_source": str(row.get("advisory_source") or ""),
                "advisory_fresh": row.get("advisory_fresh"),
                "stale_advisory_used": row.get("stale_advisory_used"),
                "would_block_if_enabled": row.get("would_block_if_enabled") or row.get("would_block_if_enforced"),
                "would_enter": row.get("would_enter"),
                "no_sample_reason": str(
                    row.get("no_sample_reason")
                    or row.get("advisory_reason")
                    or row.get("live_block_reasons")
                    or ""
                ),
                "response_action": str(row.get("response_action") or row.get("advisory_response_action") or ""),
                "source_path": str(row.get("source_path") or path),
            }
            rows.append(payload)
        if rows:
            break
    return rows


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

    strategy_rows = _read_strategy_advisory_rows()
    late_entry = json_payloads.get("late_entry_chase_threshold_advisory", {})
    pullback = json_payloads.get("pullback_reversal_readiness", {})
    available = bool(found or strategy_rows)
    execution = getattr(cfg, "execution", None)
    return {
        "status": "available" if available else "quant_lab_entry_quality_unavailable",
        "available": available,
        "live_order_effect": "read_only_no_hard_block",
        "late_entry_chase_guard_enabled": bool(getattr(execution, "late_entry_chase_guard_enabled", False)),
        "pullback_reversal_live_enabled": bool(getattr(execution, "pullback_reversal_live_enabled", False)),
        "source_files": found,
        "row_counts": row_counts,
        "strategy_advisory_count": len(strategy_rows),
        "would_block_if_enabled_count": sum(1 for row in strategy_rows if _truthy(row.get("would_block_if_enabled"))),
        "would_enter_count": sum(1 for row in strategy_rows if _truthy(row.get("would_enter"))),
        "strategy_advisories": strategy_rows[:50],
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
