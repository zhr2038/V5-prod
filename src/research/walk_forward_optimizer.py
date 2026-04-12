from __future__ import annotations

import itertools
import json
import math
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from configs.loader import load_config
from configs.schema import AppConfig
from src.backtest.walk_forward import (
    build_portfolio_analysis_record,
    build_walk_forward_report,
    run_walk_forward,
)
from src.data.mock_provider import MockProvider
from src.data.okx_ccxt_provider import OKXCCXTProvider
from src.research.cache_loader import load_cached_market_data, summarize_market_data
from src.research.recorder import ResearchRecorder


def _project_path(project_root: Path, raw_path: str, fallback: str) -> Path:
    value = str(raw_path or fallback)
    target = Path(value)
    if target.is_absolute():
        return target
    return (project_root / value).resolve()


def _load_market_data_for_task(
    *,
    project_root: Path,
    task_config: dict[str, Any],
) -> tuple[AppConfig, dict[str, Any], dict[str, Any]]:
    walk_cfg = task_config.get("walk_forward") or {}

    app_cfg_path = _project_path(project_root, walk_cfg.get("config_path", "configs/config.yaml"), "configs/config.yaml")
    env_path = str(walk_cfg.get("env_path", ".env"))
    provider_name = str(walk_cfg.get("provider", "mock")).strip().lower()
    ohlcv_limit = int(walk_cfg.get("ohlcv_limit", 24 * 120))

    cfg = load_config(str(app_cfg_path), env_path=env_path)
    cfg.execution.collect_ml_training_data = bool(walk_cfg.get("collect_ml_training_data", False))

    provider = None
    dataset_meta: dict[str, object]
    if provider_name == "okx":
        provider = OKXCCXTProvider()
        market_data = provider.fetch_ohlcv(cfg.symbols, timeframe=cfg.timeframe_main, limit=ohlcv_limit)
        dataset_meta = summarize_market_data(market_data, source="okx")
    elif provider_name == "mock":
        provider = MockProvider(seed=int(walk_cfg.get("mock_seed", 7)))
        market_data = provider.fetch_ohlcv(cfg.symbols, timeframe=cfg.timeframe_main, limit=ohlcv_limit)
        dataset_meta = summarize_market_data(market_data, source="mock")
    elif provider_name == "cache":
        cache_dir = _project_path(project_root, walk_cfg.get("cache_dir", "data/cache"), "data/cache")
        market_data = load_cached_market_data(cache_dir, cfg.symbols, cfg.timeframe_main, limit=ohlcv_limit)
        dataset_meta = summarize_market_data(market_data, source="cache", source_path=str(cache_dir))
    else:
        raise ValueError(f"unsupported walk-forward provider: {provider_name}")

    context = {
        "provider_name": provider_name,
        "folds": int(walk_cfg.get("folds", getattr(cfg.backtest, "walk_forward_folds", 4))),
        "collect_ml_training_data": bool(cfg.execution.collect_ml_training_data),
        "provider": provider,
    }
    return cfg, market_data, {"dataset": dataset_meta, "context": context}


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
        if math.isclose(rounded, round(rounded)):
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


def build_parameter_candidates(
    parameter_grid: dict[str, Any] | None,
    *,
    max_candidates: int | None = None,
) -> list[dict[str, Any]]:
    grid = parameter_grid or {}
    if not grid:
        return [{}]

    keys = list(grid.keys())
    values = [resolve_parameter_values(grid[key]) for key in keys]
    combos = itertools.product(*values)
    out: list[dict[str, Any]] = []
    for idx, combo in enumerate(combos):
        if max_candidates is not None and idx >= max_candidates:
            break
        out.append({key: value for key, value in zip(keys, combo)})
    return out


def apply_config_overrides(base_cfg: AppConfig, overrides: dict[str, Any]) -> AppConfig:
    payload = base_cfg.model_dump(mode="python")
    for raw_path, value in (overrides or {}).items():
        parts = [part for part in str(raw_path).split(".") if part]
        if not parts:
            raise ValueError("override path cannot be empty")
        cursor: dict[str, Any] = payload
        for part in parts[:-1]:
            child = cursor.get(part)
            if not isinstance(child, dict):
                child = {}
                cursor[part] = child
            cursor = child
        cursor[parts[-1]] = value
    return AppConfig.model_validate(payload)


def _lookup_metric(payload: dict[str, Any], metric_path: str, default: float = 0.0) -> float:
    cursor: Any = payload
    for part in str(metric_path).split("."):
        if not isinstance(cursor, dict):
            return default
        cursor = cursor.get(part)
    try:
        return float(cursor)
    except Exception:
        return default


def score_walk_forward_summary(
    summary: dict[str, Any],
    *,
    metric_weights: dict[str, float] | None = None,
    min_fold_count: int = 1,
) -> tuple[float, dict[str, float]]:
    weights = metric_weights or {
        "metrics.sharpe.mean": 1.0,
        "metrics.cagr.mean": 0.30,
        "metrics.max_dd.mean": -1.20,
        "metrics.turnover.mean": -0.10,
        "metrics.sharpe.std": -0.20,
    }
    fold_count = int(summary.get("fold_count") or 0)
    components = {
        str(metric): _lookup_metric(summary, metric)
        for metric in weights.keys()
    }
    score = float(sum(float(weights[metric]) * components[metric] for metric in weights.keys()))
    if fold_count < int(min_fold_count):
        return float("-inf"), components
    return score, components


def _build_cost_meta(cfg: AppConfig, provider_name: str) -> dict[str, Any]:
    return {
        "mode": str(cfg.backtest.cost_model),
        "fee_quantile": str(cfg.backtest.fee_quantile),
        "slippage_quantile": str(cfg.backtest.slippage_quantile),
        "min_fills_global": int(cfg.backtest.min_fills_global),
        "min_fills_bucket": int(cfg.backtest.min_fills_bucket),
        "max_stats_age_days": int(cfg.backtest.max_stats_age_days),
        "cost_stats_dir": str(cfg.backtest.cost_stats_dir),
        "provider": provider_name,
    }


def evaluate_walk_forward_candidate(
    *,
    base_cfg: AppConfig,
    market_data: dict[str, Any],
    provider_name: str,
    folds: int,
    overrides: dict[str, Any],
    metric_weights: dict[str, float],
    min_fold_count: int,
    candidate_id: str,
) -> dict[str, Any]:
    cfg = apply_config_overrides(base_cfg, overrides)
    cfg.execution.collect_ml_training_data = False

    folds_result = run_walk_forward(
        market_data,
        folds=folds,
        cfg=cfg,
        data_provider=None,
    )
    report = build_walk_forward_report(
        folds_result,
        cost_meta=_build_cost_meta(cfg, provider_name),
    )
    summary = build_portfolio_analysis_record(report)
    score, components = score_walk_forward_summary(
        summary,
        metric_weights=metric_weights,
        min_fold_count=min_fold_count,
    )
    return {
        "candidate_id": candidate_id,
        "overrides": dict(overrides),
        "score": float(score),
        "score_components": components,
        "summary": summary,
        "report": report,
        "config": cfg.model_dump(mode="python"),
    }


def _evaluate_walk_forward_candidate_worker(payload: dict[str, Any]) -> dict[str, Any]:
    base_cfg = AppConfig.model_validate(payload["base_cfg"])
    return evaluate_walk_forward_candidate(
        base_cfg=base_cfg,
        market_data=payload["market_data"],
        provider_name=str(payload["provider_name"]),
        folds=int(payload["folds"]),
        overrides=dict(payload["overrides"]),
        metric_weights=dict(payload["metric_weights"]),
        min_fold_count=int(payload["min_fold_count"]),
        candidate_id=str(payload["candidate_id"]),
    )


def _sanitize_candidate(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": result.get("candidate_id"),
        "overrides": result.get("overrides") or {},
        "score": float(result.get("score", float("-inf"))),
        "score_components": result.get("score_components") or {},
        "summary": result.get("summary") or {},
    }


def _resolve_executor_mode(raw_mode: str, worker_count: int, candidate_count: int) -> str:
    mode = str(raw_mode or "process").strip().lower()
    if mode not in {"process", "thread", "serial"}:
        raise ValueError("optimizer.executor must be one of: process, thread, serial")
    if worker_count <= 1 or candidate_count <= 1:
        return "serial"
    return mode


def _run_candidate_batch(payloads: list[dict[str, Any]], executor_mode: str, worker_count: int) -> list[dict[str, Any]]:
    if executor_mode == "serial":
        return [_evaluate_walk_forward_candidate_worker(payload) for payload in payloads]

    executor_cls = ProcessPoolExecutor if executor_mode == "process" else ThreadPoolExecutor
    results: list[dict[str, Any]] = []

    # Use futures to keep partial progress even if one candidate fails.
    with executor_cls(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(_evaluate_walk_forward_candidate_worker, payload): payload["candidate_id"]
            for payload in payloads
        }
        for future in as_completed(future_map):
            candidate_id = str(future_map[future])
            try:
                results.append(future.result())
            except Exception as exc:
                results.append(
                    {
                        "candidate_id": candidate_id,
                        "overrides": {},
                        "score": float("-inf"),
                        "score_components": {},
                        "summary": {"status": "failed", "fold_count": 0},
                        "report": {"error": str(exc)},
                        "config": {},
                        "error": str(exc),
                    }
                )
    return results


def run_walk_forward_optimizer_task(
    *,
    project_root: str | Path,
    task_config: dict[str, Any],
) -> dict[str, Any]:
    project_root = Path(project_root)
    task_meta = task_config.get("task") or {}
    paths_cfg = task_config.get("paths") or {}
    optimizer_cfg = task_config.get("optimizer") or {}

    recorder = ResearchRecorder(base_dir=_project_path(project_root, paths_cfg.get("runs_dir", "reports/runs"), "reports/runs"))
    run = recorder.start_run(task_name=str(task_meta.get("name", "walk_forward_optimizer")), task_config=task_config)

    try:
        base_cfg, market_data, market_meta = _load_market_data_for_task(project_root=project_root, task_config=task_config)
        context = market_meta["context"]
        dataset_meta = market_meta["dataset"]

        max_candidates_cfg = optimizer_cfg.get("max_candidates")
        max_candidates = int(max_candidates_cfg) if max_candidates_cfg is not None else None
        candidates = build_parameter_candidates(
            optimizer_cfg.get("parameter_grid") or {},
            max_candidates=max_candidates,
        )
        if not candidates:
            candidates = [{}]

        metric_weights = {
            str(key): float(value)
            for key, value in (optimizer_cfg.get("objective_weights") or {}).items()
        }
        if not metric_weights:
            metric_weights = {
                "metrics.sharpe.mean": 1.0,
                "metrics.cagr.mean": 0.30,
                "metrics.max_dd.mean": -1.20,
                "metrics.turnover.mean": -0.10,
                "metrics.sharpe.std": -0.20,
            }

        min_fold_count = int(optimizer_cfg.get("min_fold_count", 1))
        cpu_total = os.cpu_count() or 1
        requested_workers = int(optimizer_cfg.get("max_workers", 0) or 0)
        worker_count = requested_workers if requested_workers > 0 else min(cpu_total, len(candidates))
        worker_count = max(1, min(worker_count, len(candidates)))
        executor_mode = _resolve_executor_mode(str(optimizer_cfg.get("executor", "process")), worker_count, len(candidates))

        payloads = [
            {
                "candidate_id": f"cand_{idx + 1:04d}",
                "base_cfg": base_cfg.model_dump(mode="python"),
                "market_data": market_data,
                "provider_name": context["provider_name"],
                "folds": int(context["folds"]),
                "overrides": candidate,
                "metric_weights": metric_weights,
                "min_fold_count": min_fold_count,
            }
            for idx, candidate in enumerate(candidates)
        ]

        results = _run_candidate_batch(payloads, executor_mode=executor_mode, worker_count=worker_count)
        results.sort(key=lambda item: float(item.get("score", float("-inf"))), reverse=True)

        leaderboard = [_sanitize_candidate(result) for result in results]
        best = results[0] if results else None
        best_summary = _sanitize_candidate(best) if best is not None else {}

        optimizer_report = {
            "run_id": run.run_id,
            "status": "completed" if best is not None else "failed",
            "dataset": dataset_meta,
            "optimizer": {
                "candidate_count": int(len(candidates)),
                "executor": executor_mode,
                "max_workers": int(worker_count),
                "cpu_count": int(cpu_total),
                "min_fold_count": int(min_fold_count),
                "objective_weights": metric_weights,
            },
            "best_candidate": best_summary,
            "leaderboard": leaderboard,
        }

        run.write_json("dataset_meta.json", dataset_meta)
        run.write_json("metrics.json", best_summary.get("summary") or {})
        run.write_json("analysis/walk_forward_optimizer_record.json", optimizer_report)
        run.write_json("leaderboard.json", leaderboard)

        if best is not None:
            run.write_json("artifacts/best_overrides.json", best.get("overrides") or {})
            run.write_json("artifacts/best_config.json", best.get("config") or {})
            run.write_json("artifacts/best_walk_forward_report.json", best.get("report") or {})

        output_report_path = _project_path(
            project_root,
            paths_cfg.get("output_report_path", "reports/walk_forward_optimizer.json"),
            "reports/walk_forward_optimizer.json",
        )
        output_report_path.parent.mkdir(parents=True, exist_ok=True)
        output_report_path.write_text(json.dumps(optimizer_report, ensure_ascii=False, indent=2), encoding="utf-8")

        recorder.finalize_run(
            run,
            status="completed" if best is not None else "failed",
            summary={
                "candidate_count": int(len(candidates)),
                "executor": executor_mode,
                "max_workers": int(worker_count),
                "best_score": float(best.get("score")) if best is not None else None,
                "best_overrides": best.get("overrides") if best is not None else {},
            },
        )

        return {
            "exit_code": 0 if best is not None else 1,
            "run_id": run.run_id,
            "candidate_count": int(len(candidates)),
            "best_score": float(best.get("score")) if best is not None else None,
            "best_overrides": best.get("overrides") if best is not None else {},
            "output_report_path": str(output_report_path),
        }
    except Exception as exc:
        failure_summary = {
            "reason": "walk_forward_optimizer_failed",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        run.write_json("error.json", failure_summary)
        recorder.finalize_run(run, status="failed", summary=failure_summary)
        raise
