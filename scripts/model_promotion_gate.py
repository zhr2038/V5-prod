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


def _load_history():
    if not HISTORY_PATH.exists():
        return []
    try:
        obj = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        return obj if isinstance(obj, list) else []
    except Exception:
        return []


def _safe_float(v, default=-999.0):
    try:
        x = float(v)
        if x != x:
            return default
        return x
    except Exception:
        return default


def main() -> int:
    hist = _load_history()
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

    # configurable gates via env-like fallback constants
    min_valid_ic = 0.00
    min_cv_mean_ic = 0.01
    max_cv_std = 0.15
    max_ic_gap = 0.25

    k = 5
    recent = hist[-k:]
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

    passed = len(fail_reasons) == 0 and MODEL_PATH.exists()

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
