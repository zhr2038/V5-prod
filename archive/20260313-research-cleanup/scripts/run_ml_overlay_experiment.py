#!/usr/bin/env python3
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import redirect_stderr, redirect_stdout
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config
from src.backtest.walk_forward import (
    build_portfolio_analysis_record,
    build_walk_forward_report,
    run_walk_forward,
)
from src.research.cache_loader import load_cached_market_data, summarize_market_data
from src.research.ml_overlay_experiment import (
    DEFAULT_RESEARCH_SYMBOLS,
    build_experiment_configs,
    sandbox_working_directory,
)
from src.research.recorder import ResearchRecorder
from src.research.task_runner import load_task_config

SCENARIO_LABELS = {
    "no_ml": "baseline_no_ml_overlay",
    "active_ml": "current_promoted_ridge_overlay",
    "tuned_ml": "candidate_xgboost_overlay",
}


def _project_path(raw_path: str, fallback: str) -> Path:
    value = str(raw_path or fallback).strip() or fallback
    path = Path(value)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def _metric_mean(summary: dict, metric_name: str) -> float:
    metrics = (summary.get("metrics") or {}).get(metric_name) or {}
    return float(metrics.get("mean") or 0.0)


def _sanitize_config_dump(cfg) -> dict:
    dumped = cfg.model_dump(mode="json")
    exchange = dumped.get("exchange") or {}
    if isinstance(exchange, dict):
        for key in ("api_key", "api_secret", "passphrase"):
            if exchange.get(key):
                exchange[key] = "***REDACTED***"
    return dumped


def _normalize_dataset_meta(dataset_meta: dict) -> dict:
    out = dict(dataset_meta or {})
    time_range = dict(out.get("time_range") or {})
    start_ts = time_range.get("start_ts")
    end_ts = time_range.get("end_ts")
    try:
        start_i = int(start_ts)
        end_i = int(end_ts)
    except Exception:
        return out

    if start_i < 1_000_000_000_000 and end_i < 1_000_000_000_000:
        start_ms = start_i * 1000
        end_ms = end_i * 1000
        time_range.update(
            {
                "ts_unit": "seconds",
                "start_ts": start_ms,
                "end_ts": end_ms,
                "start_iso": datetime.fromtimestamp(start_i, tz=timezone.utc).isoformat(),
                "end_iso": datetime.fromtimestamp(end_i, tz=timezone.utc).isoformat(),
            }
        )
        out["time_range"] = time_range
    return out


def _build_comparison(left_summary: dict, right_summary: dict, *, left_name: str, right_name: str) -> dict:
    metrics = ("sharpe", "cagr", "max_dd", "profit_factor", "turnover")
    delta = {
        metric: round(_metric_mean(right_summary, metric) - _metric_mean(left_summary, metric), 6)
        for metric in metrics
    }
    return {
        "left": left_name,
        "right": right_name,
        "right_minus_left": delta,
        "right_better": {
            "sharpe": delta["sharpe"] > 0,
            "cagr": delta["cagr"] > 0,
            "profit_factor": delta["profit_factor"] > 0,
            "max_dd": delta["max_dd"] < 0,
            "turnover": delta["turnover"] < 0,
        },
    }


def _run_single_scenario(
    *,
    name: str,
    cfg,
    market_data,
    folds: int,
    fold_parallel_workers: int,
    sandbox_root: Path,
):
    started_at = perf_counter()
    log_dir = sandbox_root / name
    log_dir.mkdir(parents=True, exist_ok=True)
    reports_dir = log_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "scenario.log"
    repo_reports_dir = PROJECT_ROOT / "reports"
    instrument_cache = repo_reports_dir / "okx_spot_instruments.json"
    if instrument_cache.exists():
        shutil.copy2(instrument_cache, reports_dir / "okx_spot_instruments.json")
    with log_path.open("w", encoding="utf-8") as handle:
        with sandbox_working_directory(log_dir):
            with redirect_stdout(handle), redirect_stderr(handle):
                wf_folds = run_walk_forward(
                    market_data,
                    folds=folds,
                    cfg=cfg,
                    data_provider=None,
                    parallel_workers=fold_parallel_workers,
                )
    report = build_walk_forward_report(
        wf_folds,
        cost_meta={
            "mode": str(cfg.backtest.cost_model),
            "fee_quantile": str(cfg.backtest.fee_quantile),
            "slippage_quantile": str(cfg.backtest.slippage_quantile),
            "min_fills_global": int(cfg.backtest.min_fills_global),
            "min_fills_bucket": int(cfg.backtest.min_fills_bucket),
            "max_stats_age_days": int(cfg.backtest.max_stats_age_days),
            "cost_stats_dir": str(cfg.backtest.cost_stats_dir),
            "provider": "cache",
            "scenario_log_path": str(log_path),
        },
    )
    summary = build_portfolio_analysis_record(report)
    return {
        "report": report,
        "summary": summary,
        "elapsed_sec": round(perf_counter() - started_at, 3),
    }


def main() -> int:
    task_config_path = sys.argv[1] if len(sys.argv) > 1 else "configs/research/ml_overlay_experiment.yaml"
    task_config = load_task_config(PROJECT_ROOT / task_config_path)
    if not task_config:
        print(f"unable to load experiment task config: {task_config_path}")
        return 1

    task_meta = task_config.get("task") or {}
    paths_cfg = task_config.get("paths") or {}
    exp_cfg = task_config.get("experiment") or {}

    recorder = ResearchRecorder(
        base_dir=_project_path(str(paths_cfg.get("runs_dir", "reports/runs")), "reports/runs")
    )
    run = recorder.start_run(
        task_name=str(task_meta.get("name", "ml_overlay_experiment")),
        task_config=task_config,
    )

    base_config_path = _project_path(str(exp_cfg.get("base_config_path", "configs/live_prod.yaml")), "configs/live_prod.yaml")
    env_path = str(exp_cfg.get("env_path", ".env"))
    cache_dir = _project_path(str(exp_cfg.get("cache_dir", "data/cache")), "data/cache")
    output_report_path = _project_path(
        str(paths_cfg.get("output_report_path", "reports/research/ml_overlay_experiment/latest.json")),
        "reports/research/ml_overlay_experiment/latest.json",
    )
    total_started_at = perf_counter()
    folds = int(exp_cfg.get("folds", 4))
    parallel_workers = max(1, int(exp_cfg.get("parallel_workers", 1)))
    fold_parallel_workers = max(1, int(exp_cfg.get("fold_parallel_workers", 1)))
    ohlcv_limit = int(exp_cfg.get("ohlcv_limit", 720))
    research_symbols = [str(symbol) for symbol in (exp_cfg.get("research_symbols") or list(DEFAULT_RESEARCH_SYMBOLS))]

    base_cfg = load_config(str(base_config_path), env_path=env_path)
    configs = build_experiment_configs(
        base_cfg,
        project_root=PROJECT_ROOT,
        research_symbols=research_symbols,
    )

    market_data = load_cached_market_data(
        cache_dir,
        research_symbols,
        configs["no_ml"].timeframe_main,
        limit=ohlcv_limit,
    )
    dataset_meta = _normalize_dataset_meta(
        summarize_market_data(market_data, source="cache", source_path=str(cache_dir))
    )

    sandbox_root = run.run_dir / "sandbox"
    scenario_payloads = {}
    if parallel_workers <= 1 or len(configs) <= 1:
        for scenario_name, cfg in configs.items():
            scenario_payloads[scenario_name] = _run_single_scenario(
                name=scenario_name,
                cfg=cfg,
                market_data=market_data,
                folds=folds,
                fold_parallel_workers=fold_parallel_workers,
                sandbox_root=sandbox_root,
            )
    else:
        max_workers = min(parallel_workers, len(configs))
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(
                    _run_single_scenario,
                    name=scenario_name,
                    cfg=cfg,
                    market_data=market_data,
                    folds=folds,
                    fold_parallel_workers=fold_parallel_workers,
                    sandbox_root=sandbox_root,
                ): scenario_name
                for scenario_name, cfg in configs.items()
            }
            for future in as_completed(future_map):
                scenario_payloads[str(future_map[future])] = future.result()

    scenario_results = {}
    for scenario_name, cfg in configs.items():
        payload = scenario_payloads[scenario_name]
        scenario_results[scenario_name] = {
            "name": SCENARIO_LABELS.get(scenario_name, scenario_name),
            "config": _sanitize_config_dump(cfg),
            "summary": payload["summary"],
            "report": payload["report"],
            "elapsed_sec": float(payload["elapsed_sec"]),
        }

    comparison = {
        "active_ml_vs_no_ml": _build_comparison(
            scenario_results["no_ml"]["summary"],
            scenario_results["active_ml"]["summary"],
            left_name="no_ml",
            right_name="active_ml",
        ),
        "tuned_ml_vs_no_ml": _build_comparison(
            scenario_results["no_ml"]["summary"],
            scenario_results["tuned_ml"]["summary"],
            left_name="no_ml",
            right_name="tuned_ml",
        ),
        "tuned_ml_vs_active_ml": _build_comparison(
            scenario_results["active_ml"]["summary"],
            scenario_results["tuned_ml"]["summary"],
            left_name="active_ml",
            right_name="tuned_ml",
        ),
    }

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "base_config_path": str(base_config_path),
        "dataset": dataset_meta,
        "timing": {
            "parallel_workers": int(parallel_workers),
            "fold_parallel_workers": int(fold_parallel_workers),
            "total_elapsed_sec": round(perf_counter() - total_started_at, 3),
            "scenario_elapsed_sec": {
                scenario_name: float((scenario_payloads.get(scenario_name) or {}).get("elapsed_sec") or 0.0)
                for scenario_name in configs.keys()
            },
        },
        "scenarios": scenario_results,
        "comparison": comparison,
    }

    output_report_path.parent.mkdir(parents=True, exist_ok=True)
    output_report_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    run.write_json("dataset_meta.json", dataset_meta)
    for scenario_name, payload in scenario_results.items():
        run.write_json(f"{scenario_name}/report.json", payload["report"])
        run.write_json(f"{scenario_name}/summary.json", payload["summary"])
        run.write_json(f"{scenario_name}/config.json", payload["config"])
    run.write_json("comparison.json", comparison)
    run.write_json("result.json", result)
    recorder.finalize_run(
        run,
        status="completed",
        summary={
            "output_report_path": str(output_report_path),
            "comparison": comparison,
        },
    )

    print(json.dumps(comparison, ensure_ascii=False, indent=2))
    print(f"report_written={output_report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
