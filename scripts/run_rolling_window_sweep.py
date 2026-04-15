#!/usr/bin/env python3
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import redirect_stderr, redirect_stdout
from functools import lru_cache
from itertools import combinations
import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config
from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path
from src.research.cache_loader import load_cached_market_data
from src.research.recorder import ResearchRecorder
from src.research.task_runner import load_task_config
from src.research.trend_quality_experiment import (
    build_baseline_config,
    sandbox_working_directory,
    seed_sandbox_read_only_artifacts,
)
from src.research.window_diagnostics import (
    _apply_overrides,
    _project_path,
    _resolve_workers,
    _slice_market_data_tail_window,
    run_window_diagnostic,
)


@lru_cache(maxsize=8)
def _load_base_config_cached(base_config_path: str, env_path: str):
    return load_config(base_config_path, env_path=env_path)


def _variant_name(symbols: list[str]) -> str:
    compact = [symbol.split("/")[0].lower() for symbol in symbols]
    return f"k{len(symbols)}_" + "_".join(compact)


def _generated_variants(task_config: dict) -> list[dict]:
    exp_cfg = task_config.get("experiment") or {}
    symbol_pool = [str(symbol) for symbol in (exp_cfg.get("symbol_pool") or [])]
    combo_sizes = [int(size) for size in (exp_cfg.get("combo_sizes") or [])]
    variants: list[dict] = []
    for combo_size in combo_sizes:
        for combo in combinations(symbol_pool, combo_size):
            symbols = [str(symbol) for symbol in combo]
            variants.append(
                {
                    "name": _variant_name(symbols),
                    "symbols": symbols,
                    "overrides": {},
                }
            )
    return variants


def _probe_symbols(task_config: dict, variants: list[dict]) -> list[str]:
    if variants:
        return [str(symbol) for symbol in (variants[0].get("symbols") or [])]
    exp_cfg = task_config.get("experiment") or {}
    return [str(symbol) for symbol in (exp_cfg.get("symbol_pool") or [])]


def _available_bars(
    *,
    base_config_path: str,
    env_path: str,
    cache_dir: str,
    symbols: list[str],
) -> int:
    if not symbols:
        return 0
    base_cfg = _load_base_config_cached(base_config_path, env_path)
    market_data = load_cached_market_data(
        Path(cache_dir),
        symbols,
        base_cfg.timeframe_main,
        limit=100_000,
    )
    if not market_data:
        return 0
    return int(min(len(series.close) for series in market_data.values()))


def _generated_evaluations(
    task_config: dict,
    *,
    available_bars: int,
) -> list[dict[str, Any]]:
    exp_cfg = task_config.get("experiment") or {}
    explicit = list(exp_cfg.get("evaluations") or [])
    if explicit:
        resolved: list[dict[str, Any]] = []
        for evaluation in explicit:
            shift = max(0, int(evaluation.get("window_shift_bars") or 0))
            raw_limit = evaluation.get("ohlcv_limit", 720)
            if isinstance(raw_limit, str) and str(raw_limit).strip().lower() in {"full", "all", "auto", "max"}:
                ohlcv_limit = max(1, int(available_bars) - shift)
            else:
                ohlcv_limit = max(1, int(raw_limit or 720))
            resolved.append(
                {
                    **dict(evaluation),
                    "ohlcv_limit": int(ohlcv_limit),
                    "window_shift_bars": int(shift),
                }
            )
        return resolved

    grid_cfg = exp_cfg.get("evaluation_grid") or {}
    window_lengths = [int(value) for value in (grid_cfg.get("window_lengths") or []) if int(value) > 0]
    if not window_lengths:
        return []
    shift_values = grid_cfg.get("shift_bars")
    if shift_values:
        shifts = sorted({max(0, int(value)) for value in shift_values})
    else:
        stride = max(1, int(grid_cfg.get("stride_bars", 240) or 240))
        start_shift = max(0, int(grid_cfg.get("start_shift_bars", 0) or 0))
        raw_max_shift = grid_cfg.get("max_shift_bars", "auto")
        if str(raw_max_shift).strip().lower() in {"", "auto", "max"}:
            requested_max_shift = max(0, available_bars - min(window_lengths))
        else:
            requested_max_shift = max(0, int(raw_max_shift))
        shifts = list(range(start_shift, requested_max_shift + 1, stride))

    generated: list[dict[str, Any]] = []
    for window_length in sorted(window_lengths):
        max_shift_for_window = max(0, available_bars - window_length)
        for shift in shifts:
            if shift > max_shift_for_window:
                continue
            name = f"w{window_length}_s{shift:04d}"
            generated.append(
                {
                    "name": name,
                    "ohlcv_limit": int(window_length),
                    "window_shift_bars": int(shift),
                }
            )
    return generated


def _variant_aggregate(variant_result: dict) -> dict:
    windows = list(variant_result.get("windows") or [])
    total_returns = [
        float((((window.get("summary") or {}).get("metrics") or {}).get("total_return") or 0.0))
        for window in windows
    ]
    sharpes = [
        float((((window.get("summary") or {}).get("metrics") or {}).get("sharpe") or 0.0))
        for window in windows
    ]
    max_dds = [
        float((((window.get("summary") or {}).get("metrics") or {}).get("max_dd") or 0.0))
        for window in windows
    ]
    turnovers = [
        float((((window.get("summary") or {}).get("metrics") or {}).get("turnover") or 0.0))
        for window in windows
    ]
    positives = sum(1 for value in total_returns if value > 0)
    negatives = sum(1 for value in total_returns if value < 0)
    return {
        "window_count": int(len(windows)),
        "positive_windows": int(positives),
        "negative_windows": int(negatives),
        "flat_windows": int(len(windows) - positives - negatives),
        "mean_total_return": float(sum(total_returns) / max(1, len(total_returns))),
        "median_total_return": float(statistics.median(total_returns)) if total_returns else 0.0,
        "min_total_return": float(min(total_returns)) if total_returns else 0.0,
        "max_total_return": float(max(total_returns)) if total_returns else 0.0,
        "mean_sharpe": float(sum(sharpes) / max(1, len(sharpes))),
        "max_max_dd": float(max(max_dds)) if max_dds else 0.0,
        "mean_turnover": float(sum(turnovers) / max(1, len(turnovers))),
    }


def _run_variant_job(
    *,
    variant: dict,
    evaluations: list[dict],
    base_config_path: str,
    env_path: str,
    cache_dir: str,
    project_root: str,
    run_dir: str,
) -> dict:
    variant_name = str(variant.get("name") or "variant")
    symbols = [str(symbol) for symbol in (variant.get("symbols") or [])]
    overrides = dict(variant.get("overrides") or {})

    base_cfg = _load_base_config_cached(base_config_path, env_path)
    cfg = build_baseline_config(base_cfg, project_root=Path(project_root), research_symbols=symbols)
    _apply_overrides(cfg, overrides)
    max_bars_needed = max(
        int(evaluation.get("ohlcv_limit") or 720) + int(evaluation.get("window_shift_bars") or 0)
        for evaluation in evaluations
    )
    base_market_data = load_cached_market_data(
        Path(cache_dir),
        symbols,
        cfg.timeframe_main,
        limit=max_bars_needed,
    )

    variant_dir = Path(run_dir)
    variant_dir.mkdir(parents=True, exist_ok=True)
    windows: list[dict[str, Any]] = []
    for evaluation in evaluations:
        window_name = str(evaluation.get("name") or "window")
        ohlcv_limit = int(evaluation.get("ohlcv_limit") or 720)
        shift = int(evaluation.get("window_shift_bars") or 0)
        window_market_data = _slice_market_data_tail_window(
            base_market_data,
            limit=ohlcv_limit,
            shift=shift,
        )
        window_dir = variant_dir / window_name
        window_dir.mkdir(parents=True, exist_ok=True)
        seed_sandbox_read_only_artifacts(Path(project_root), window_dir)
        log_path = window_dir / "scenario.log"
        with log_path.open("w", encoding="utf-8") as handle:
            with sandbox_working_directory(window_dir):
                with redirect_stdout(handle), redirect_stderr(handle):
                    summary = run_window_diagnostic(
                        market_data=window_market_data,
                        cfg=cfg,
                        window_name=window_name,
                        output_dir=window_dir,
                    )
        windows.append(
            {
                "name": window_name,
                "ohlcv_limit": int(ohlcv_limit),
                "window_shift_bars": int(shift),
                "summary": summary,
                "window_dir": str(window_dir),
                "scenario_log_path": str(log_path),
            }
        )

    result = {
        "name": variant_name,
        "symbols": symbols,
        "overrides": overrides,
        "windows": windows,
    }
    result["aggregate"] = _variant_aggregate(result)
    return result


def _run_variant_window_job(
    *,
    variant: dict,
    evaluation: dict,
    base_config_path: str,
    env_path: str,
    cache_dir: str,
    project_root: str,
    window_dir: str,
) -> dict:
    variant_name = str(variant.get("name") or "variant")
    symbols = [str(symbol) for symbol in (variant.get("symbols") or [])]
    overrides = dict(variant.get("overrides") or {})

    base_cfg = _load_base_config_cached(base_config_path, env_path)
    cfg = build_baseline_config(base_cfg, project_root=Path(project_root), research_symbols=symbols)
    _apply_overrides(cfg, overrides)
    ohlcv_limit = int(evaluation.get("ohlcv_limit") or 720)
    shift = int(evaluation.get("window_shift_bars") or 0)
    base_market_data = load_cached_market_data(
        Path(cache_dir),
        symbols,
        cfg.timeframe_main,
        limit=max(1, ohlcv_limit + shift),
    )
    window_name = str(evaluation.get("name") or "window")
    window_market_data = _slice_market_data_tail_window(
        base_market_data,
        limit=ohlcv_limit,
        shift=shift,
    )
    window_dir_path = Path(window_dir)
    window_dir_path.mkdir(parents=True, exist_ok=True)
    seed_sandbox_read_only_artifacts(Path(project_root), window_dir_path)
    log_path = window_dir_path / "scenario.log"
    with log_path.open("w", encoding="utf-8") as handle:
        with sandbox_working_directory(window_dir_path):
            with redirect_stdout(handle), redirect_stderr(handle):
                summary = run_window_diagnostic(
                    market_data=window_market_data,
                    cfg=cfg,
                    window_name=window_name,
                    output_dir=window_dir_path,
                )
    return {
        "variant_name": variant_name,
        "symbols": symbols,
        "overrides": overrides,
        "window": {
            "name": window_name,
            "ohlcv_limit": int(ohlcv_limit),
            "window_shift_bars": int(shift),
            "summary": summary,
            "window_dir": str(window_dir_path),
            "scenario_log_path": str(log_path),
        },
    }


def main() -> int:
    task_config_path = sys.argv[1] if len(sys.argv) > 1 else "configs/research/liquid_core_combo_rolling720.yaml"
    task_config = load_task_config(PROJECT_ROOT / task_config_path)
    if not task_config:
        print(f"unable to load sweep task config: {task_config_path}")
        return 1

    task_meta = task_config.get("task") or {}
    paths_cfg = task_config.get("paths") or {}
    exp_cfg = task_config.get("experiment") or {}
    variants = list(exp_cfg.get("variants") or [])
    if not variants:
        variants = _generated_variants(task_config)

    recorder = ResearchRecorder(
        base_dir=_project_path(PROJECT_ROOT, str(paths_cfg.get("runs_dir", "reports/runs")), "reports/runs")
    )
    run = recorder.start_run(
        task_name=str(task_meta.get("name", "rolling_window_sweep")),
        task_config=task_config,
    )

    try:
        base_config_path = Path(
            resolve_runtime_config_path(
                str(exp_cfg.get("base_config_path", "")).strip() or None,
                project_root=PROJECT_ROOT,
            )
        )
        env_path = resolve_runtime_env_path(
            str(exp_cfg.get("env_path", ".env")),
            project_root=PROJECT_ROOT,
        )
        cache_dir = _project_path(PROJECT_ROOT, str(exp_cfg.get("cache_dir", "data/cache")), "data/cache")
        available_bars = _available_bars(
            base_config_path=str(base_config_path),
            env_path=env_path,
            cache_dir=str(cache_dir),
            symbols=_probe_symbols(task_config, variants),
        )
        evaluations = _generated_evaluations(task_config, available_bars=available_bars)
        if not evaluations:
            failure_summary = {
                "reason": "no_evaluations_generated",
                "available_bars": int(available_bars),
                "variant_count": int(len(variants)),
            }
            recorder.finalize_run(run, status="failed", summary=failure_summary)
            print("no evaluations generated for sweep task")
            return 1
        parallel_granularity = str(exp_cfg.get("parallel_granularity", "variant")).strip().lower()
        output_report_path = _project_path(
            PROJECT_ROOT,
            str(paths_cfg.get("output_report_path", "reports/research/liquid_core_combo_rolling720/latest.json")),
            "reports/research/liquid_core_combo_rolling720/latest.json",
        )
        variants_root = run.run_dir / "variants"
        variants_root.mkdir(parents=True, exist_ok=True)

        ordered_results: dict[int, dict] = {}
        if parallel_granularity == "job":
            variant_windows: dict[int, list[dict]] = {idx: [] for idx in range(len(variants))}
            job_specs: list[tuple[int, int, dict, dict]] = [
                (variant_idx, evaluation_idx, variant, evaluation)
                for variant_idx, variant in enumerate(variants)
                for evaluation_idx, evaluation in enumerate(evaluations)
            ]
            workers = _resolve_workers(exp_cfg.get("workers", 1), len(job_specs))
            with ProcessPoolExecutor(max_workers=workers) as executor:
                future_map = {
                    executor.submit(
                        _run_variant_window_job,
                        variant=variant,
                        evaluation=evaluation,
                        base_config_path=str(base_config_path),
                        env_path=env_path,
                        cache_dir=str(cache_dir),
                        project_root=str(PROJECT_ROOT),
                        window_dir=str(
                            variants_root
                            / str(variant.get("name") or f"variant_{variant_idx}")
                            / str(evaluation.get("name") or f"window_{evaluation_idx}")
                        ),
                    ): (variant_idx, evaluation_idx)
                    for variant_idx, evaluation_idx, variant, evaluation in job_specs
                }
                for future in as_completed(future_map):
                    variant_idx, evaluation_idx = future_map[future]
                    payload = future.result()
                    variant_windows[int(variant_idx)].append((int(evaluation_idx), payload))

            for idx, variant in enumerate(variants):
                windows = [
                    payload["window"]
                    for _, payload in sorted(variant_windows[idx], key=lambda item: item[0])
                ]
                result = {
                    "name": str(variant.get("name") or f"variant_{idx}"),
                    "symbols": [str(symbol) for symbol in (variant.get("symbols") or [])],
                    "overrides": dict(variant.get("overrides") or {}),
                    "windows": windows,
                }
                result["aggregate"] = _variant_aggregate(result)
                ordered_results[idx] = result
        else:
            workers = _resolve_workers(exp_cfg.get("workers", 1), len(variants))
            with ProcessPoolExecutor(max_workers=workers) as executor:
                future_map = {
                    executor.submit(
                        _run_variant_job,
                        variant=variant,
                        evaluations=evaluations,
                        base_config_path=str(base_config_path),
                        env_path=env_path,
                        cache_dir=str(cache_dir),
                        project_root=str(PROJECT_ROOT),
                        run_dir=str(variants_root / str(variant.get("name") or f"variant_{idx}")),
                    ): idx
                    for idx, variant in enumerate(variants)
                }
                for future in as_completed(future_map):
                    ordered_results[int(future_map[future])] = future.result()

        results = [ordered_results[idx] for idx in range(len(variants))]
        results.sort(
            key=lambda item: (
                int(((item.get("aggregate") or {}).get("positive_windows") or 0)),
                float(((item.get("aggregate") or {}).get("mean_total_return") or 0.0)),
                float(((item.get("aggregate") or {}).get("median_total_return") or 0.0)),
                float(((item.get("aggregate") or {}).get("min_total_return") or 0.0)),
                -float(((item.get("aggregate") or {}).get("max_max_dd") or 0.0)),
            ),
            reverse=True,
        )

        result = {
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "task_config_path": str(PROJECT_ROOT / task_config_path),
            "base_config_path": str(base_config_path),
            "available_bars": int(available_bars),
            "workers": int(workers),
            "parallel_granularity": parallel_granularity,
            "evaluations": evaluations,
            "results": results,
        }
        output_report_path.parent.mkdir(parents=True, exist_ok=True)
        output_report_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        run.write_json("result.json", result)
        recorder.finalize_run(
            run,
            status="completed",
            summary={
                "output_report_path": str(output_report_path),
                "top_variant": results[0]["name"] if results else None,
            },
        )

        preview = [
            {
                "name": item["name"],
                "symbols": item["symbols"],
                "aggregate": item["aggregate"],
                "run_dir": str(variants_root / item["name"]),
            }
            for item in results[:12]
        ]
        print(json.dumps(preview, ensure_ascii=False, indent=2))
        print(f"report_written={output_report_path}")
        return 0
    except Exception as exc:
        failure_summary = {
            "reason": "rolling_window_sweep_failed",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        run.write_json("error.json", failure_summary)
        recorder.finalize_run(run, status="failed", summary=failure_summary)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
