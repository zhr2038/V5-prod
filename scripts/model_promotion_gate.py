#!/usr/bin/env python3
"""Model promotion gate for V5."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_path
from src.execution.fill_store import (
    derive_runtime_named_artifact_path,
    derive_runtime_reports_dir,
    derive_runtime_runs_dir,
)


@dataclass(frozen=True)
class PromotionPaths:
    workspace: Path
    history_path: Path
    decision_path: Path
    active_pointer_path: Path
    model_path: Path
    runs_dir: Path


def resolve_workspace() -> Path:
    raw = os.getenv("V5_WORKSPACE", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return PROJECT_ROOT


def _resolve_runtime_ml_artifact_path(
    *,
    root: Path,
    order_store_path: str | Path,
    raw_path: object,
    legacy_default: str,
) -> Path:
    text = str(raw_path or "").strip()
    if not text or text == legacy_default:
        name = Path(legacy_default).name
        suffix = ".jsonl" if name.endswith(".jsonl") else Path(name).suffix
        base_name = name[: -len(suffix)] if suffix else name
        return derive_runtime_named_artifact_path(order_store_path, base_name, suffix).resolve()
    return Path(resolve_runtime_path(text, default=legacy_default, project_root=root)).resolve()


def build_paths(workspace: str | Path | None = None, raw_config_path: str | None = None) -> PromotionPaths:
    root = Path(workspace).expanduser().resolve() if workspace is not None else resolve_workspace()
    config_path = Path(resolve_runtime_config_path(raw_config_path, project_root=root)).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"runtime config not found: {config_path}")
    try:
        cfg = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        raise ValueError(f"runtime config is invalid: {config_path}: {exc}") from exc
    if not isinstance(cfg, dict) or not cfg:
        raise ValueError(f"runtime config is empty or invalid: {config_path}")
    execution_cfg = cfg.get("execution")
    if not isinstance(execution_cfg, dict):
        raise ValueError(f"runtime config missing execution section: {config_path}")
    alpha_cfg = cfg.get("alpha") if isinstance(cfg.get("alpha"), dict) else {}
    ml_cfg = alpha_cfg.get("ml_factor") if isinstance(alpha_cfg.get("ml_factor"), dict) else {}
    order_store_path = resolve_runtime_path(
        execution_cfg.get("order_store_path") if isinstance(execution_cfg, dict) else None,
        default="reports/orders.sqlite",
        project_root=root,
    )
    reports_dir = derive_runtime_reports_dir(order_store_path).resolve()
    return PromotionPaths(
        workspace=root,
        history_path=(reports_dir / "ml_training_history.json").resolve(),
        decision_path=_resolve_runtime_ml_artifact_path(
            root=root,
            order_store_path=order_store_path,
            raw_path=ml_cfg.get("promotion_decision_path") if isinstance(ml_cfg, dict) else None,
            legacy_default="reports/model_promotion_decision.json",
        ),
        active_pointer_path=Path(
            resolve_runtime_path(
                ml_cfg.get("active_model_pointer_path") if isinstance(ml_cfg, dict) else None,
                default="models/ml_factor_model_active.txt",
                project_root=root,
            )
        ).resolve(),
        model_path=Path(
            resolve_runtime_path(
                ml_cfg.get("model_path") if isinstance(ml_cfg, dict) else None,
                default="models/ml_factor_model",
                project_root=root,
            )
        ).resolve(),
        runs_dir=derive_runtime_runs_dir(order_store_path).resolve(),
    )


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


def _load_history(workspace: str | Path | None = None):
    history_path = build_paths(workspace).history_path
    if not history_path.exists():
        return []
    try:
        obj = json.loads(history_path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, list) else []
    except Exception:
        return []


def _load_latest_training_entry(workspace: str | Path | None = None) -> dict | None:
    paths = build_paths(workspace)
    try:
        from src.research.recorder import load_latest_task_record

        record = load_latest_task_record(
            "ml_training",
            "analysis/model_training_record.json",
            base_dir=paths.runs_dir,
        )
        if isinstance(record, dict):
            legacy = record.get("legacy_history_entry")
            if isinstance(legacy, dict):
                return legacy

        metrics = load_latest_task_record(
            "ml_training",
            "metrics.json",
            base_dir=paths.runs_dir,
        )
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


def main(workspace: str | Path | None = None) -> int:
    paths = build_paths(workspace)
    hist = _load_history(paths.workspace)
    latest_run_entry = _load_latest_training_entry(paths.workspace)
    if latest_run_entry:
        if not hist or str(hist[-1].get("run_id")) != str(latest_run_entry.get("run_id")):
            hist = [*hist, latest_run_entry]
    if not hist:
        decision = {
            "ts": datetime.now().isoformat(),
            "passed": False,
            "reason": "no_training_history",
        }
        paths.decision_path.parent.mkdir(parents=True, exist_ok=True)
        paths.decision_path.write_text(json.dumps(decision, ensure_ascii=False, indent=2), encoding="utf-8")
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

    passed = len(fail_reasons) == 0 and _model_artifact_exists(paths.model_path)

    decision = {
        "ts": datetime.now().isoformat(),
        "passed": passed,
        "selected_model_path": str(paths.model_path),
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

    paths.decision_path.parent.mkdir(parents=True, exist_ok=True)
    paths.decision_path.write_text(json.dumps(decision, ensure_ascii=False, indent=2), encoding="utf-8")

    if passed:
        paths.active_pointer_path.parent.mkdir(parents=True, exist_ok=True)
        paths.active_pointer_path.write_text(str(paths.model_path), encoding="utf-8")

    print(json.dumps(decision, ensure_ascii=False))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
