#!/usr/bin/env python3
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import redirect_stderr, redirect_stdout
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config
from configs.schema import AppConfig
from src.backtest.walk_forward import (
    build_portfolio_analysis_record,
    build_walk_forward_report,
    run_walk_forward,
)
from src.research.cache_loader import load_cached_market_data, summarize_market_data
from src.research.recorder import ResearchRecorder
from src.research.task_runner import load_task_config
from src.research.trend_quality_experiment import (
    DEFAULT_RESEARCH_SYMBOLS,
    build_experiment_configs,
    sandbox_working_directory,
    seed_sandbox_read_only_artifacts,
)

SCENARIO_LABELS = {
    "baseline": "baseline_static_liquid_cache",
    "trend_quality": "trend_quality_low_churn_v1",
    "trend_quality_v2": "trend_quality_balanced_v2",
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

    # Cached market data may already be stored in seconds. summarize_market_data
    # assumes milliseconds, so normalize here for experiment output readability.
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
    sandbox_root: Path,
):
    log_dir = sandbox_root / name
    log_dir.mkdir(parents=True, exist_ok=True)
    seed_sandbox_read_only_artifacts(PROJECT_ROOT, log_dir)
    log_path = log_dir / "scenario.log"
    with log_path.open("w", encoding="utf-8") as handle:
        with sandbox_working_directory(log_dir):
            with redirect_stdout(handle), redirect_stderr(handle):
                wf_folds = run_walk_forward(market_data, folds=folds, cfg=cfg, data_provider=None)
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
    return report, summary


def _resolve_workers(raw_workers, scenario_count: int) -> int:
    if scenario_count <= 1:
        return 1
    value = str(raw_workers or "1").strip().lower()
    if value in {"auto", "max"}:
        return max(1, min(os.cpu_count() or 1, scenario_count))
    try:
        workers = int(value)
    except Exception:
        return 1
    return max(1, min(workers, scenario_count))


def _run_single_scenario_job(
    *,
    name: str,
    cfg_payload: dict,
    cache_dir: str,
    folds: int,
    ohlcv_limit: int,
    sandbox_root: str,
) -> dict:
    cfg = AppConfig.model_validate(cfg_payload)
    market_data = load_cached_market_data(
        Path(cache_dir),
        cfg.symbols,
        cfg.timeframe_main,
        limit=ohlcv_limit,
    )
    report, summary = _run_single_scenario(
        name=name,
        cfg=cfg,
        market_data=market_data,
        folds=folds,
        sandbox_root=Path(sandbox_root),
    )
    return {
        "report": report,
        "summary": summary,
    }


def main() -> int:
    task_config_path = "configs/research/trend_quality_experiment.yaml"
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
        task_name=str(task_meta.get("name", "trend_quality_experiment")),
        task_config=task_config,
    )

    base_config_path = _project_path(str(exp_cfg.get("base_config_path", "configs/live_prod.yaml")), "configs/live_prod.yaml")
    env_path = str(exp_cfg.get("env_path", ".env"))
    cache_dir = _project_path(str(exp_cfg.get("cache_dir", "data/cache")), "data/cache")
    output_report_path = _project_path(
        str(paths_cfg.get("output_report_path", "reports/research/trend_quality_experiment/latest.json")),
        "reports/research/trend_quality_experiment/latest.json",
    )
    folds = int(exp_cfg.get("folds", 4))
    ohlcv_limit = int(exp_cfg.get("ohlcv_limit", 720))
    research_symbols = [str(symbol) for symbol in (exp_cfg.get("research_symbols") or list(DEFAULT_RESEARCH_SYMBOLS))]

    base_cfg = load_config(str(base_config_path), env_path=env_path)
    configs = build_experiment_configs(
        base_cfg,
        project_root=PROJECT_ROOT,
        research_symbols=research_symbols,
    )
    workers = _resolve_workers(exp_cfg.get("workers", 1), scenario_count=len(configs))

    market_data = load_cached_market_data(
        cache_dir,
        research_symbols,
        configs["baseline"].timeframe_main,
        limit=ohlcv_limit,
    )
    dataset_meta = _normalize_dataset_meta(
        summarize_market_data(market_data, source="cache", source_path=str(cache_dir))
    )

    sandbox_root = run.run_dir / "sandbox"
    scenario_results = {}
    scenario_payloads = {
        scenario_name: cfg.model_dump(mode="json")
        for scenario_name, cfg in configs.items()
    }
    if workers <= 1:
        for scenario_name, cfg in configs.items():
            report, summary = _run_single_scenario(
                name=scenario_name,
                cfg=cfg,
                market_data=market_data,
                folds=folds,
                sandbox_root=sandbox_root,
            )
            scenario_results[scenario_name] = {
                "name": SCENARIO_LABELS.get(scenario_name, scenario_name),
                "config": _sanitize_config_dump(cfg),
                "summary": summary,
                "report": report,
            }
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _run_single_scenario_job,
                    name=scenario_name,
                    cfg_payload=scenario_payloads[scenario_name],
                    cache_dir=str(cache_dir),
                    folds=folds,
                    ohlcv_limit=ohlcv_limit,
                    sandbox_root=str(sandbox_root),
                ): scenario_name
                for scenario_name in configs.keys()
            }
            for future in as_completed(futures):
                scenario_name = futures[future]
                payload = future.result()
                scenario_results[scenario_name] = {
                    "name": SCENARIO_LABELS.get(scenario_name, scenario_name),
                    "config": _sanitize_config_dump(configs[scenario_name]),
                    "summary": payload["summary"],
                    "report": payload["report"],
                }

    comparison = {}
    baseline_summary = scenario_results["baseline"]["summary"]
    for scenario_name in configs.keys():
        if scenario_name == "baseline":
            continue
        comparison[f"{scenario_name}_vs_baseline"] = _build_comparison(
            baseline_summary,
            scenario_results[scenario_name]["summary"],
            left_name="baseline",
            right_name=scenario_name,
        )
    if "trend_quality" in scenario_results and "trend_quality_v2" in scenario_results:
        comparison["trend_quality_v2_vs_v1"] = _build_comparison(
            scenario_results["trend_quality"]["summary"],
            scenario_results["trend_quality_v2"]["summary"],
            left_name="trend_quality",
            right_name="trend_quality_v2",
        )

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "base_config_path": str(base_config_path),
        "dataset": dataset_meta,
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
