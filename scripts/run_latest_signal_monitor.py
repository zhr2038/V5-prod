#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.research.latest_signal_monitor import (
    build_latest_signal_markdown,
    build_latest_signal_summary,
    run_latest_signal_variant,
)
from src.research.recorder import ResearchRecorder
from src.research.task_runner import load_task_config


def _project_path(raw_path: str, fallback: str) -> Path:
    value = str(raw_path or fallback).strip() or fallback
    target = Path(value)
    if target.is_absolute():
        return target
    return (PROJECT_ROOT / target).resolve()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("task_config_path", nargs="?", default="configs/research/core6_avax_latest_signal_monitor.yaml")
    args = parser.parse_args()

    task_config_path = _project_path(args.task_config_path, "configs/research/core6_avax_latest_signal_monitor.yaml")
    task_config = load_task_config(task_config_path)
    if not task_config:
        raise SystemExit(f"unable to load task config: {task_config_path}")

    exp_cfg = task_config.get("experiment") or {}
    paths_cfg = task_config.get("paths") or {}
    variants = list(exp_cfg.get("variants") or [])
    if len(variants) < 2:
        raise SystemExit("latest signal monitor needs at least 2 variants")

    base_config_path = str(_project_path(str(exp_cfg.get("base_config_path", "configs/live_prod.yaml")), "configs/live_prod.yaml"))
    env_path = str(_project_path(str(exp_cfg.get("env_path", ".env")), ".env"))
    cache_dir = str(_project_path(str(exp_cfg.get("cache_dir", "data/cache")), "data/cache"))
    runs_dir = _project_path(str(paths_cfg.get("runs_dir", "reports/runs")), "reports/runs")
    output_json_path = _project_path(str(paths_cfg.get("output_report_path", "reports/research/core6_avax_latest_signal_monitor/latest.json")), "reports/research/core6_avax_latest_signal_monitor/latest.json")
    output_md_path = _project_path(str(paths_cfg.get("output_markdown_path", "reports/research/core6_avax_latest_signal_monitor/latest.md")), "reports/research/core6_avax_latest_signal_monitor/latest.md")
    ohlcv_limit = int(exp_cfg.get("ohlcv_limit", 720) or 720)
    initial_equity_usdt = float(exp_cfg.get("initial_equity_usdt", 100.0) or 100.0)
    top_scores_limit = int(exp_cfg.get("top_scores_limit", 5) or 5)
    baseline_name = str(exp_cfg.get("baseline_name") or variants[0].get("name") or "baseline")
    champion_name = str(exp_cfg.get("champion_name") or variants[1].get("name") or "champion")

    recorder = ResearchRecorder(base_dir=runs_dir)
    run = recorder.start_run(
        task_name=str((task_config.get("task") or {}).get("name") or "latest_signal_monitor"),
        task_config={
            "task": task_config.get("task") or {},
            "paths": {
                "output_report_path": str(output_json_path),
                "output_markdown_path": str(output_md_path),
                "runs_dir": str(runs_dir),
            },
            "experiment": {
                "base_config_path": base_config_path,
                "env_path": env_path,
                "cache_dir": cache_dir,
                "ohlcv_limit": ohlcv_limit,
                "initial_equity_usdt": initial_equity_usdt,
                "top_scores_limit": top_scores_limit,
                "baseline_name": baseline_name,
                "champion_name": champion_name,
                "variants": variants,
            },
        },
    )

    try:
        by_name = {str(item.get("name")): item for item in variants}
        baseline_variant = by_name[baseline_name]
        champion_variant = by_name[champion_name]
        baseline_result = run_latest_signal_variant(
            variant=baseline_variant,
            base_config_path=base_config_path,
            env_path=env_path,
            cache_dir=cache_dir,
            project_root=PROJECT_ROOT,
            output_dir=run.run_dir / "variants" / baseline_name,
            ohlcv_limit=ohlcv_limit,
            initial_equity_usdt=initial_equity_usdt,
            top_scores_limit=top_scores_limit,
        )
        champion_result = run_latest_signal_variant(
            variant=champion_variant,
            base_config_path=base_config_path,
            env_path=env_path,
            cache_dir=cache_dir,
            project_root=PROJECT_ROOT,
            output_dir=run.run_dir / "variants" / champion_name,
            ohlcv_limit=ohlcv_limit,
            initial_equity_usdt=initial_equity_usdt,
            top_scores_limit=top_scores_limit,
        )

        generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        summary = build_latest_signal_summary(
            generated_at=generated_at,
            baseline=baseline_result,
            champion=champion_result,
            baseline_name=baseline_name,
            champion_name=champion_name,
        )
        markdown = build_latest_signal_markdown(summary)

        output_json_path.parent.mkdir(parents=True, exist_ok=True)
        output_md_path.parent.mkdir(parents=True, exist_ok=True)
        output_json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        output_md_path.write_text(markdown, encoding="utf-8")
        run.write_json("result.json", summary)
        run.write_text("summary.md", markdown)
        recorder.finalize_run(
            run,
            status="completed",
            summary={
                "selection_changed": bool((summary.get("compare") or {}).get("selection_changed")),
                "orders_changed": bool((summary.get("compare") or {}).get("orders_changed")),
                "needs_review": bool((summary.get("compare") or {}).get("needs_review")),
                "output_json_path": str(output_json_path),
                "output_md_path": str(output_md_path),
            },
        )

        print(json.dumps(summary, ensure_ascii=False, indent=2))
        print(f"report_written={output_json_path}")
        print(f"markdown_written={output_md_path}")
        print(f"run_dir={run.run_dir}")
        return 0
    except Exception as exc:
        failure_summary = {
            "reason": "latest_signal_monitor_failed",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        run.write_json("error.json", failure_summary)
        recorder.finalize_run(run, status="failed", summary=failure_summary)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
