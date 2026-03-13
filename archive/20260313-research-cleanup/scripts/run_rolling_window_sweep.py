#!/usr/bin/env python3
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import redirect_stderr, redirect_stdout
from itertools import combinations
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config
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

    base_cfg = load_config(base_config_path, env_path=env_path)
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
    window_results: list[dict] = []
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
        window_results.append(
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
        "windows": window_results,
    }
    result["aggregate"] = _variant_aggregate(result)
    return result


def main() -> int:
    task_config_path = sys.argv[1] if len(sys.argv) > 1 else "configs/research/liquid_core_combo_rolling720.yaml"
    task_config = load_task_config(PROJECT_ROOT / task_config_path)
    if not task_config:
        print(f"unable to load sweep task config: {task_config_path}")
        return 1

    task_meta = task_config.get("task") or {}
    paths_cfg = task_config.get("paths") or {}
    exp_cfg = task_config.get("experiment") or {}
    evaluations = list(exp_cfg.get("evaluations") or [])
    variants = list(exp_cfg.get("variants") or [])
    if not variants:
        variants = _generated_variants(task_config)
    workers = _resolve_workers(exp_cfg.get("workers", 1), len(variants))

    recorder = ResearchRecorder(
        base_dir=_project_path(PROJECT_ROOT, str(paths_cfg.get("runs_dir", "reports/runs")), "reports/runs")
    )
    run = recorder.start_run(
        task_name=str(task_meta.get("name", "rolling_window_sweep")),
        task_config=task_config,
    )

    base_config_path = _project_path(PROJECT_ROOT, str(exp_cfg.get("base_config_path", "configs/live_prod.yaml")), "configs/live_prod.yaml")
    env_path = str(exp_cfg.get("env_path", ".env"))
    cache_dir = _project_path(PROJECT_ROOT, str(exp_cfg.get("cache_dir", "data/cache")), "data/cache")
    output_report_path = _project_path(
        PROJECT_ROOT,
        str(paths_cfg.get("output_report_path", "reports/research/liquid_core_combo_rolling720/latest.json")),
        "reports/research/liquid_core_combo_rolling720/latest.json",
    )
    variants_root = run.run_dir / "variants"
    variants_root.mkdir(parents=True, exist_ok=True)

    ordered_results: dict[int, dict] = {}
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
            float(((item.get("aggregate") or {}).get("min_total_return") or 0.0)),
            -float(((item.get("aggregate") or {}).get("max_max_dd") or 0.0)),
        ),
        reverse=True,
    )

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "task_config_path": str(PROJECT_ROOT / task_config_path),
        "base_config_path": str(base_config_path),
        "workers": int(workers),
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


if __name__ == "__main__":
    raise SystemExit(main())
