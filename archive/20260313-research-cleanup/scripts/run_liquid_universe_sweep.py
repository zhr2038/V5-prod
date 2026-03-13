#!/usr/bin/env python3
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import redirect_stderr, redirect_stdout
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config
from src.backtest.walk_forward import (
    build_portfolio_analysis_record,
    build_walk_forward_report,
    run_walk_forward,
)
from src.core.models import MarketSeries
from src.research.recorder import ResearchRecorder
from src.research.task_runner import load_task_config
from src.research.trend_quality_experiment import (
    build_baseline_config,
    sandbox_working_directory,
    seed_sandbox_read_only_artifacts,
)
from src.research.cache_loader import load_cached_market_data


def _project_path(raw_path: str, fallback: str) -> Path:
    value = str(raw_path or fallback).strip() or fallback
    path = Path(value)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def _resolve_workers(raw_workers, variant_count: int) -> int:
    if variant_count <= 1:
        return 1
    value = str(raw_workers or "1").strip().lower()
    if value in {"auto", "max"}:
        return max(1, min(os.cpu_count() or 1, variant_count))
    try:
        workers = int(value)
    except Exception:
        return 1
    return max(1, min(workers, variant_count))


def _sanitize_config_dump(cfg) -> dict:
    dumped = cfg.model_dump(mode="json")
    exchange = dumped.get("exchange") or {}
    if isinstance(exchange, dict):
        for key in ("api_key", "api_secret", "passphrase"):
            if exchange.get(key):
                exchange[key] = "***REDACTED***"
    return dumped


def _apply_overrides(cfg, overrides: dict[str, object]) -> None:
    for raw_path, value in (overrides or {}).items():
        path = str(raw_path).split(".")
        target = cfg
        for attr in path[:-1]:
            target = getattr(target, attr)
        setattr(target, path[-1], value)


def _metric_mean(summary: dict, metric_name: str) -> float:
    metrics = (summary.get("metrics") or {}).get(metric_name) or {}
    return float(metrics.get("mean") or 0.0)


def _evaluation_summary(summary: dict) -> dict[str, float]:
    return {
        "sharpe": round(_metric_mean(summary, "sharpe"), 6),
        "cagr": round(_metric_mean(summary, "cagr"), 6),
        "max_dd": round(_metric_mean(summary, "max_dd"), 6),
        "profit_factor": round(_metric_mean(summary, "profit_factor"), 6),
        "turnover": round(_metric_mean(summary, "turnover"), 6),
    }


def _slice_market_data_tail_window(
    market_data: dict[str, MarketSeries],
    *,
    limit: int,
    shift: int = 0,
) -> dict[str, MarketSeries]:
    shift = max(0, int(shift or 0))
    limit = max(1, int(limit or 1))
    sliced: dict[str, MarketSeries] = {}
    for symbol, series in market_data.items():
        end = None if shift == 0 else -shift
        start = -limit - shift
        sliced[symbol] = MarketSeries(
            symbol=series.symbol,
            timeframe=series.timeframe,
            ts=series.ts[start:end],
            open=series.open[start:end],
            high=series.high[start:end],
            low=series.low[start:end],
            close=series.close[start:end],
            volume=series.volume[start:end],
        )
    return sliced


def _run_variant_job(
    *,
    variant: dict,
    evaluations: list[dict],
    base_config_path: str,
    env_path: str,
    cache_dir: str,
    runs_dir: str,
) -> dict:
    variant_name = str(variant.get("name") or "variant")
    symbols = [str(symbol) for symbol in (variant.get("symbols") or [])]
    overrides = dict(variant.get("overrides") or {})

    base_cfg = load_config(base_config_path, env_path=env_path)
    cfg = build_baseline_config(base_cfg, project_root=PROJECT_ROOT, research_symbols=symbols)
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

    variant_run_dir = Path(
        tempfile.mkdtemp(
            prefix=f"{variant_name}_",
            dir=str(Path(runs_dir)),
        )
    )
    config_dump = _sanitize_config_dump(cfg)
    evaluation_results: dict[str, dict] = {}

    for evaluation in evaluations:
        eval_name = str(evaluation.get("name") or "eval")
        ohlcv_limit = int(evaluation.get("ohlcv_limit") or 720)
        folds = int(evaluation.get("folds") or 4)
        window_shift_bars = int(evaluation.get("window_shift_bars") or 0)
        market_data = _slice_market_data_tail_window(
            base_market_data,
            limit=ohlcv_limit,
            shift=window_shift_bars,
        )
        sandbox_dir = variant_run_dir / eval_name
        sandbox_dir.mkdir(parents=True, exist_ok=True)
        seed_sandbox_read_only_artifacts(PROJECT_ROOT, sandbox_dir)
        log_path = sandbox_dir / "scenario.log"
        with log_path.open("w", encoding="utf-8") as handle:
            with sandbox_working_directory(sandbox_dir):
                with redirect_stdout(handle), redirect_stderr(handle):
                    wf_folds = run_walk_forward(market_data, folds=folds, cfg=cfg, data_provider=None)
        report = build_walk_forward_report(
            wf_folds,
            cost_meta={
                "mode": str(cfg.backtest.cost_model),
                "provider": "cache",
                "scenario_log_path": str(log_path),
            },
        )
        summary = build_portfolio_analysis_record(report)
        evaluation_results[eval_name] = {
            "config": {
                "ohlcv_limit": ohlcv_limit,
                "folds": folds,
                "window_shift_bars": window_shift_bars,
            },
            "summary": summary,
            "summary_metrics": _evaluation_summary(summary),
            "report": report,
        }

    return {
        "name": variant_name,
        "symbols": symbols,
        "overrides": overrides,
        "config": config_dump,
        "evaluations": evaluation_results,
        "run_dir": str(variant_run_dir),
    }


def main() -> int:
    task_config_path = sys.argv[1] if len(sys.argv) > 1 else "configs/research/liquid_universe_sweep.yaml"
    task_config = load_task_config(PROJECT_ROOT / task_config_path)
    if not task_config:
        print(f"unable to load sweep task config: {task_config_path}")
        return 1

    task_meta = task_config.get("task") or {}
    paths_cfg = task_config.get("paths") or {}
    exp_cfg = task_config.get("experiment") or {}
    variants = list(exp_cfg.get("variants") or [])
    evaluations = list(exp_cfg.get("evaluations") or [])
    workers = _resolve_workers(exp_cfg.get("workers", 1), len(variants))

    recorder = ResearchRecorder(
        base_dir=_project_path(str(paths_cfg.get("runs_dir", "reports/runs")), "reports/runs")
    )
    run = recorder.start_run(
        task_name=str(task_meta.get("name", "liquid_universe_sweep")),
        task_config=task_config,
    )

    base_config_path = _project_path(str(exp_cfg.get("base_config_path", "configs/live_prod.yaml")), "configs/live_prod.yaml")
    env_path = str(exp_cfg.get("env_path", ".env"))
    cache_dir = _project_path(str(exp_cfg.get("cache_dir", "data/cache")), "data/cache")
    output_report_path = _project_path(
        str(paths_cfg.get("output_report_path", "reports/research/liquid_universe_sweep/latest.json")),
        "reports/research/liquid_universe_sweep/latest.json",
    )
    variants_root = run.run_dir / "variants"
    variants_root.mkdir(parents=True, exist_ok=True)

    results = []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _run_variant_job,
                variant=variant,
                evaluations=evaluations,
                base_config_path=str(base_config_path),
                env_path=env_path,
                cache_dir=str(cache_dir),
                runs_dir=str(variants_root),
            ): str(variant.get("name") or "variant")
            for variant in variants
        }
        for future in as_completed(futures):
            results.append(future.result())

    primary_eval_name = str(evaluations[0].get("name") or "eval") if evaluations else ""
    results.sort(
        key=lambda item: (
            item["evaluations"][primary_eval_name]["summary_metrics"]["cagr"],
            item["evaluations"][primary_eval_name]["summary_metrics"]["sharpe"],
            -item["evaluations"][primary_eval_name]["summary_metrics"]["max_dd"],
        ),
        reverse=True,
    )

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "task_config_path": str(PROJECT_ROOT / task_config_path),
        "base_config_path": str(base_config_path),
        "workers": workers,
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
            "screening": item["evaluations"][primary_eval_name]["summary_metrics"],
            "validation": item["evaluations"].get("validation_full", {}).get("summary_metrics"),
            "run_dir": item["run_dir"],
        }
        for item in results
    ]
    print(json.dumps(preview, ensure_ascii=False, indent=2))
    print(f"report_written={output_report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
