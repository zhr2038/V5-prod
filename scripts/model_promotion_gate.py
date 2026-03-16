#!/usr/bin/env python3
"""Model promotion gate for V5.

Purpose:
- Turn research/training outputs into a clear go/no-go promotion decision.
- Persist decision to reports/model_promotion_decision.json
- Maintain a stable pointer models/ml_factor_model_active -> selected model path

Gate inputs (from reports/ml_training_history.json latest entry):
- valid_ic
- cv_mean_ic
- cv_std_ic
- ic_gap

Optional stability check:
- last_k valid_ic mean
"""

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime

HISTORY_PATH = Path("reports/ml_training_history.json")
DECISION_PATH = Path("reports/model_promotion_decision.json")
ACTIVE_POINTER_PATH = Path("models/ml_factor_model_active.txt")
MODEL_PATH = Path("models/ml_factor_model")


def _model_artifact_exists(base_path: Path) -> bool:
    return any(
        p.exists()
        for p in (
            base_path,
            Path(f"{base_path}.pkl"),
            Path(f"{base_path}.txt"),
            Path(f"{base_path}_config.json"),
        )
    )


def _load_history():
    if not HISTORY_PATH.exists():
        return []
    try:
        obj = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        return obj if isinstance(obj, list) else []
    except Exception:
        return []


def _load_latest_training_entry() -> dict | None:
    try:
        from src.research.recorder import load_latest_task_record

        record = load_latest_task_record("ml_training", "analysis/model_training_record.json")
        if isinstance(record, dict):
            legacy = record.get("legacy_history_entry")
            if isinstance(legacy, dict):
                return legacy

        metrics = load_latest_task_record("ml_training", "metrics.json")
        if isinstance(metrics, dict):
            return metrics
    except Exception:
        return None
    return None


def _safe_float(v, default=-999.0):
    try:
        x = float(v)
        if x != x:
            return default
        return x
    except Exception:
        return default


def _comparable_history_runs(hist: list[dict], latest: dict) -> list[dict]:
    latest_cfg = latest.get("config") or {}
    latest_model_type = latest.get("selected_model_type") or latest_cfg.get("model_type")
    latest_target_mode = latest_cfg.get("target_mode")
    latest_include_time = latest_cfg.get("include_time_features")

    comparable = []
    for item in hist:
        item_cfg = item.get("config") or {}
        item_model_type = item.get("selected_model_type") or item_cfg.get("model_type")
        if item_model_type != latest_model_type:
            continue
        if item_cfg.get("target_mode") != latest_target_mode:
            continue
        if item_cfg.get("include_time_features") != latest_include_time:
            continue
        if "grouped_holdout" not in item:
            continue
        comparable.append(item)
    return comparable


def main() -> int:
    hist = _load_history()
    latest_run_entry = _load_latest_training_entry()
    if latest_run_entry:
        if not hist or str(hist[-1].get("run_id")) != str(latest_run_entry.get("run_id")):
            hist = [*hist, latest_run_entry]
    if not hist:
        decision = {
            "ts": datetime.now().isoformat(),
            "passed": False,
            "reason": "no_training_history",
        }
        DECISION_PATH.parent.mkdir(parents=True, exist_ok=True)
        DECISION_PATH.write_text(json.dumps(decision, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(decision, ensure_ascii=False))
        return 1

    latest = hist[-1]
    valid_ic = _safe_float(latest.get("valid_ic"))
    cv_mean_ic = _safe_float(latest.get("cv_mean_ic"))
    cv_std_ic = _safe_float(latest.get("cv_std_ic"))
    ic_gap = _safe_float(latest.get("train_ic")) - valid_ic

    gate_cfg = latest.get("gate") or {}
    min_valid_ic = _safe_float(gate_cfg.get("min_valid_ic"), default=0.00)
    min_cv_mean_ic = _safe_float(gate_cfg.get("min_cv_mean_ic"), default=0.01)
    max_cv_std = _safe_float(gate_cfg.get("max_cv_std"), default=0.15)
    max_ic_gap = _safe_float(gate_cfg.get("max_ic_gap"), default=0.25)

    k = 5
    comparable_hist = _comparable_history_runs(hist, latest)
    recent = comparable_hist[-k:] if comparable_hist else [latest]
    recent_valid_ics = [_safe_float(x.get("valid_ic"), default=0.0) for x in recent]
    recent_mean_valid_ic = sum(recent_valid_ics) / max(1, len(recent_valid_ics))

    fail_reasons = []
    if valid_ic < min_valid_ic:
        fail_reasons.append(f"valid_ic<{min_valid_ic:.2f}")
    if cv_mean_ic < min_cv_mean_ic:
        fail_reasons.append(f"cv_mean_ic<{min_cv_mean_ic:.2f}")
    if cv_std_ic > max_cv_std:
        fail_reasons.append(f"cv_std_ic>{max_cv_std:.2f}")
    if ic_gap > max_ic_gap:
        fail_reasons.append(f"ic_gap>{max_ic_gap:.2f}")
    if recent_mean_valid_ic < 0.0:
        fail_reasons.append("recent_mean_valid_ic<0")

    passed = len(fail_reasons) == 0 and _model_artifact_exists(MODEL_PATH)

    decision = {
        "ts": datetime.now().isoformat(),
        "passed": passed,
        "selected_model_path": str(MODEL_PATH),
        "metrics": {
            "valid_ic": valid_ic,
            "cv_mean_ic": cv_mean_ic,
            "cv_std_ic": cv_std_ic,
            "ic_gap": ic_gap,
            "recent_mean_valid_ic": recent_mean_valid_ic,
        },
        "thresholds": {
            "min_valid_ic": min_valid_ic,
            "min_cv_mean_ic": min_cv_mean_ic,
            "max_cv_std": max_cv_std,
            "max_ic_gap": max_ic_gap,
        },
        "fail_reasons": fail_reasons,
    }

    DECISION_PATH.parent.mkdir(parents=True, exist_ok=True)
    DECISION_PATH.write_text(json.dumps(decision, ensure_ascii=False, indent=2), encoding="utf-8")

    if passed:
        ACTIVE_POINTER_PATH.parent.mkdir(parents=True, exist_ok=True)
        ACTIVE_POINTER_PATH.write_text(str(MODEL_PATH), encoding="utf-8")

    print(json.dumps(decision, ensure_ascii=False))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
