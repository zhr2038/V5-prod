from __future__ import annotations

import itertools
import json
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.research.recorder import ResearchRecorder
from src.research.task_runner import load_task_config, run_ml_training_task


def _project_path(project_root: Path, raw_path: str, fallback: str) -> Path:
    value = str(raw_path or fallback)
    target = Path(value)
    if target.is_absolute():
        return target
    return (project_root / value).resolve()


def _expand_numeric_range(spec: dict[str, Any]) -> list[Any]:
    start = float(spec["start"])
    stop = float(spec["stop"])
    step = float(spec["step"])
    if step == 0:
        raise ValueError("parameter step cannot be 0")

    values: list[Any] = []
    current = start
    cmp = (lambda x: x <= stop + 1e-12) if step > 0 else (lambda x: x >= stop - 1e-12)
    while cmp(current):
        rounded = round(current, 10)
        if abs(rounded - round(rounded)) < 1e-9:
            values.append(int(round(rounded)))
        else:
            values.append(float(rounded))
        current += step
    return values


def resolve_parameter_values(spec: Any) -> list[Any]:
    if isinstance(spec, list):
        return list(spec)
    if isinstance(spec, tuple):
        return list(spec)
    if isinstance(spec, dict):
        if "values" in spec:
            values = spec.get("values") or []
            return list(values) if isinstance(values, list) else [values]
        if {"start", "stop", "step"}.issubset(spec.keys()):
            return _expand_numeric_range(spec)
    return [spec]


def build_sweep_candidates(
    parameter_grid: dict[str, Any] | None,
    *,
    max_candidates: int | None = None,
    sample_strategy: str = "sequential",
    random_seed: int = 42,
) -> list[dict[str, Any]]:
    grid = parameter_grid or {}
    if not grid:
        return [{}]

    keys = list(grid.keys())
    value_lists = [resolve_parameter_values(grid[key]) for key in keys]
    combos = [
        {key: value for key, value in zip(keys, combo)}
        for combo in itertools.product(*value_lists)
    ]
    if max_candidates is None or max_candidates >= len(combos):
        return combos

    mode = str(sample_strategy or "sequential").strip().lower()
    if mode == "random":
        rng = random.Random(int(random_seed))
        idxs = list(range(len(combos)))
        rng.shuffle(idxs)
        selected = sorted(idxs[:max_candidates])
        return [combos[idx] for idx in selected]
    return combos[:max_candidates]


def apply_dict_overrides(payload: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    out = deepcopy(payload)
    for raw_path, value in (overrides or {}).items():
        parts = [part for part in str(raw_path).split(".") if part]
        if not parts:
            raise ValueError("override path cannot be empty")
        cursor = out
        for part in parts[:-1]:
            child = cursor.get(part)
            if not isinstance(child, dict):
                child = {}
                cursor[part] = child
            cursor = child
        cursor[parts[-1]] = value
    return out


def is_gpu_task(task_config: dict[str, Any]) -> bool:
    model_cfg = task_config.get("model") or {}
    candidates = [str(x).strip().lower() for x in (model_cfg.get("candidates") or []) if str(x).strip()]
    if "xgboost" not in candidates:
        return False
    device = str(model_cfg.get("xgboost_compute_device", "auto") or "auto").strip().lower()
    return device != "cpu"


def score_training_result(
    metrics: dict[str, Any],
    *,
    objective_weights: dict[str, float] | None = None,
) -> tuple[float, dict[str, float]]:
    weights = objective_weights or {
        "valid_ic": 3.0,
        "cv_mean_ic": 2.0,
        "cv_std": -1.5,
        "ic_gap": -1.0,
    }
    train_ic = float(metrics.get("train_ic") or 0.0)
    valid_ic = float(metrics.get("valid_ic") or 0.0)
    cv_mean = float(metrics.get("cv_mean_ic") or 0.0)
    cv_std = float(metrics.get("cv_std_ic") or 0.0)
    ic_gap = max(0.0, train_ic - valid_ic)
    components = {
        "valid_ic": valid_ic,
        "cv_mean_ic": cv_mean,
        "cv_std": cv_std,
        "ic_gap": ic_gap,
    }
    score = float(sum(float(weights.get(key, 0.0)) * components[key] for key in components.keys()))
    return score, components


def select_best_gate_passed_candidate(results: list[dict[str, Any]] | None) -> dict[str, Any]:
    passed = [item for item in (results or []) if bool(item.get("gate_passed"))]
    if not passed:
        return {}
    passed.sort(key=lambda item: float(item.get("score", float("-inf"))), reverse=True)
    return passed[0]


def _utc_now_token() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")


def _prepare_candidate_task_config(
    *,
    base_task_config: dict[str, Any],
    overrides: dict[str, Any],
    batch_root: Path,
    scenario_name: str,
    model_n_jobs: int | None,
    candidate_workers: int | None,
    cv_workers: int | None,
    gpu_cv_workers: int | None,
) -> dict[str, Any]:
    cfg = apply_dict_overrides(base_task_config, overrides)
    cfg.setdefault("task", {})
    cfg.setdefault("paths", {})
    cfg.setdefault("parallel", {})
    cfg.setdefault("model", {})

    scenario_root = batch_root / scenario_name
    cfg["task"]["name"] = str(f"{cfg['task'].get('name') or 'ml_training'}_{scenario_name}")
    cfg["paths"]["history_path"] = str((scenario_root / "ml_training_history.json").as_posix())
    cfg["paths"]["csv_path"] = str((scenario_root / "ml_training_data.csv").as_posix())
    cfg["paths"]["model_path"] = str((scenario_root / "model" / "ml_factor_model").as_posix())
    cfg["paths"]["output_report_path"] = str((scenario_root / "summary.json").as_posix())

    if model_n_jobs is not None:
        cfg["model"]["n_jobs"] = int(model_n_jobs)
    if candidate_workers is not None:
        cfg["parallel"]["candidate_workers"] = int(candidate_workers)
    if cv_workers is not None:
        cfg["parallel"]["cv_workers"] = int(cv_workers)
    if gpu_cv_workers is not None:
        cfg["parallel"]["gpu_cv_workers"] = int(gpu_cv_workers)
    return cfg


def _run_single_candidate(
    *,
    project_root: Path,
    task_config: dict[str, Any],
    scenario_name: str,
    overrides: dict[str, Any],
    runs_dir: Path,
    objective_weights: dict[str, float],
) -> dict[str, Any]:
    result = run_ml_training_task(project_root=project_root, task_config=task_config)
    run_id = str(result.get("run_id") or "")
    run_dir = runs_dir / run_id if run_id else None

    metrics: dict[str, Any] = {}
    if run_dir is not None:
        metrics_path = run_dir / "metrics.json"
        if metrics_path.exists():
            try:
                obj = json.loads(metrics_path.read_text(encoding="utf-8"))
                if isinstance(obj, dict):
                    metrics = obj
            except Exception:
                metrics = {}

    score, components = score_training_result(metrics, objective_weights=objective_weights)
    return {
        "scenario_name": scenario_name,
        "run_id": run_id,
        "run_dir": str(run_dir) if run_dir is not None else None,
        "overrides": dict(overrides),
        "gate_passed": bool(result.get("gate_passed", False)),
        "selected_model_type": result.get("selected_model_type"),
        "metrics": metrics,
        "score": score,
        "score_components": components,
    }


def run_ml_training_sweep(
    *,
    project_root: str | Path,
    sweep_config: dict[str, Any],
) -> dict[str, Any]:
    project_root = Path(project_root)
    task_meta = sweep_config.get("task") or {}
    paths_cfg = sweep_config.get("paths") or {}
    sweep_cfg = sweep_config.get("sweep") or {}
    concurrency_cfg = sweep_cfg.get("concurrency") or {}

    recorder = ResearchRecorder(base_dir=_project_path(project_root, paths_cfg.get("runs_dir", "reports/runs"), "reports/runs"))
    run = recorder.start_run(task_name=str(task_meta.get("name", "ml_training_sweep")), task_config=sweep_config)

    base_task_config_path = _project_path(
        project_root,
        sweep_cfg.get("base_task_config", "configs/research/ml_training_gpu.yaml"),
        "configs/research/ml_training_gpu.yaml",
    )
    base_task_config = load_task_config(base_task_config_path)
    if not base_task_config:
        raise ValueError(f"unable to load base task config: {base_task_config_path}")

    runs_dir = _project_path(project_root, paths_cfg.get("runs_dir", "reports/runs"), "reports/runs")
    batch_root = _project_path(
        project_root,
        paths_cfg.get("batch_root", f"reports/sweeps/ml_training/{run.run_id}"),
        f"reports/sweeps/ml_training/{run.run_id}",
    )
    batch_root.mkdir(parents=True, exist_ok=True)

    candidates = build_sweep_candidates(
        sweep_cfg.get("parameter_grid") or {},
        max_candidates=int(sweep_cfg["max_candidates"]) if sweep_cfg.get("max_candidates") is not None else None,
        sample_strategy=str(sweep_cfg.get("sample_strategy", "sequential")),
        random_seed=int(sweep_cfg.get("random_seed", 42)),
    )
    objective_weights = {
        str(key): float(value)
        for key, value in (sweep_cfg.get("objective_weights") or {}).items()
    }

    gpu_workers = max(1, int(concurrency_cfg.get("gpu_workers", 1)))
    cpu_workers = max(1, int(concurrency_cfg.get("cpu_workers", 2)))
    gpu_model_n_jobs = int(concurrency_cfg.get("gpu_model_n_jobs", 8))
    cpu_model_n_jobs = int(concurrency_cfg.get("cpu_model_n_jobs", 6))
    gpu_candidate_workers = int(concurrency_cfg.get("gpu_candidate_workers", 1))
    cpu_candidate_workers = int(concurrency_cfg.get("cpu_candidate_workers", 2))
    gpu_cv_workers = int(concurrency_cfg.get("gpu_cv_workers", 2))
    cpu_cv_workers = int(concurrency_cfg.get("cpu_cv_workers", 4))

    job_specs: list[tuple[str, dict[str, Any], dict[str, Any], bool]] = []
    for idx, overrides in enumerate(candidates, start=1):
        scenario_name = f"cand_{idx:03d}"
        task_cfg = _prepare_candidate_task_config(
            base_task_config=base_task_config,
            overrides=overrides,
            batch_root=batch_root,
            scenario_name=scenario_name,
            model_n_jobs=gpu_model_n_jobs,
            candidate_workers=gpu_candidate_workers,
            cv_workers=gpu_cv_workers,
            gpu_cv_workers=gpu_cv_workers,
        )
        gpu_task = is_gpu_task(task_cfg)
        if not gpu_task:
            task_cfg = _prepare_candidate_task_config(
                base_task_config=base_task_config,
                overrides=overrides,
                batch_root=batch_root,
                scenario_name=scenario_name,
                model_n_jobs=cpu_model_n_jobs,
                candidate_workers=cpu_candidate_workers,
                cv_workers=cpu_cv_workers,
                gpu_cv_workers=1,
            )
        job_specs.append((scenario_name, overrides, task_cfg, gpu_task))

    results: list[dict[str, Any]] = []
    gpu_jobs = [(name, overrides, cfg) for name, overrides, cfg, gpu in job_specs if gpu]
    cpu_jobs = [(name, overrides, cfg) for name, overrides, cfg, gpu in job_specs if not gpu]

    with ThreadPoolExecutor(max_workers=gpu_workers) as gpu_executor, ThreadPoolExecutor(max_workers=cpu_workers) as cpu_executor:
        futures = {}
        for scenario_name, overrides, task_cfg in gpu_jobs:
            future = gpu_executor.submit(
                _run_single_candidate,
                project_root=project_root,
                task_config=task_cfg,
                scenario_name=scenario_name,
                overrides=overrides,
                runs_dir=runs_dir,
                objective_weights=objective_weights,
            )
            futures[future] = scenario_name
        for scenario_name, overrides, task_cfg in cpu_jobs:
            future = cpu_executor.submit(
                _run_single_candidate,
                project_root=project_root,
                task_config=task_cfg,
                scenario_name=scenario_name,
                overrides=overrides,
                runs_dir=runs_dir,
                objective_weights=objective_weights,
            )
            futures[future] = scenario_name

        for future in as_completed(futures):
            scenario_name = str(futures[future])
            try:
                results.append(future.result())
            except Exception as exc:
                results.append(
                    {
                        "scenario_name": scenario_name,
                        "run_id": None,
                        "run_dir": None,
                        "overrides": {},
                        "gate_passed": False,
                        "selected_model_type": None,
                        "metrics": {},
                        "score": float("-inf"),
                        "score_components": {},
                        "error": str(exc),
                    }
                )

    results.sort(key=lambda item: float(item.get("score", float("-inf"))), reverse=True)
    best = results[0] if results else None
    best_gate_passed = select_best_gate_passed_candidate(results)
    gate_passed_count = sum(1 for item in results if bool(item.get("gate_passed")))
    report = {
        "run_id": run.run_id,
        "status": "completed" if results else "failed",
        "base_task_config": str(base_task_config_path),
        "batch_root": str(batch_root),
        "candidate_count": int(len(candidates)),
        "gate_passed_count": int(gate_passed_count),
        "concurrency": {
            "gpu_workers": gpu_workers,
            "cpu_workers": cpu_workers,
            "gpu_model_n_jobs": gpu_model_n_jobs,
            "cpu_model_n_jobs": cpu_model_n_jobs,
            "gpu_candidate_workers": gpu_candidate_workers,
            "cpu_candidate_workers": cpu_candidate_workers,
            "gpu_cv_workers": gpu_cv_workers,
            "cpu_cv_workers": cpu_cv_workers,
        },
        "best_candidate": best or {},
        "best_gate_passed_candidate": best_gate_passed,
        "leaderboard": results,
    }

    output_report_path = _project_path(
        project_root,
        paths_cfg.get("output_report_path", "reports/ml_training_sweep.json"),
        "reports/ml_training_sweep.json",
    )
    output_report_path.parent.mkdir(parents=True, exist_ok=True)
    output_report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    run.write_json("leaderboard.json", results)
    run.write_json("report.json", report)
    if best:
        run.write_json("artifacts/best_overrides.json", best.get("overrides") or {})
        run.write_json("artifacts/best_metrics.json", best.get("metrics") or {})
    if best_gate_passed:
        run.write_json("artifacts/best_gate_passed_overrides.json", best_gate_passed.get("overrides") or {})
        run.write_json("artifacts/best_gate_passed_metrics.json", best_gate_passed.get("metrics") or {})

    recorder.finalize_run(
        run,
        status="completed" if results else "failed",
        summary={
            "candidate_count": int(len(candidates)),
            "gate_passed_count": int(gate_passed_count),
            "best_score": float(best.get("score")) if best else None,
            "best_scenario": best.get("scenario_name") if best else None,
            "best_run_id": best.get("run_id") if best else None,
            "best_gate_passed_scenario": best_gate_passed.get("scenario_name") if best_gate_passed else None,
            "best_gate_passed_run_id": best_gate_passed.get("run_id") if best_gate_passed else None,
        },
    )
    return {
        "exit_code": 0 if results else 1,
        "run_id": run.run_id,
        "output_report_path": str(output_report_path),
        "best_run_id": best.get("run_id") if best else None,
        "best_score": float(best.get("score")) if best else None,
    }
