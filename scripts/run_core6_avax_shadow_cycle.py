#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.research.recorder import ResearchRecorder
from src.research.shadow_ab_monitor import (
    build_shadow_cycle_markdown,
    build_shadow_cycle_summary,
)
from src.research.task_runner import load_task_config


SCRIPT_TIMEOUT_SECONDS = 3600


def _project_path(raw_path: str, fallback: str) -> Path:
    value = str(raw_path or fallback).strip() or fallback
    target = Path(value)
    if target.is_absolute():
        return target
    return (PROJECT_ROOT / target).resolve()


def _report_path_from_config(config_path: Path) -> Path:
    task_config = load_task_config(config_path)
    paths_cfg = task_config.get("paths") or {}
    return _project_path(
        str(paths_cfg.get("output_report_path", "reports/research/core6_avax_shadow_cycle/latest.json")),
        "reports/research/core6_avax_shadow_cycle/latest.json",
    )


def _run_script(script_name: str, config_path: Path, *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts" / script_name), str(config_path)],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=SCRIPT_TIMEOUT_SECONDS,
        check=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ab-config",
        default="configs/research/core6_avax_shadow_compare_hotpath.yaml",
    )
    parser.add_argument(
        "--shadow-config",
        default="configs/research/core6_avax015_shadow.yaml",
    )
    parser.add_argument(
        "--champion-name",
        default="avax_015",
    )
    parser.add_argument(
        "--baseline-name",
        default="core6_cost018",
    )
    parser.add_argument(
        "--output-json",
        default="reports/research/core6_avax_shadow_cycle/latest.json",
    )
    parser.add_argument(
        "--output-md",
        default="reports/research/core6_avax_shadow_cycle/latest.md",
    )
    args = parser.parse_args()

    ab_config_path = _project_path(args.ab_config, "configs/research/core6_avax_shadow_compare_hotpath.yaml")
    shadow_config_path = _project_path(args.shadow_config, "configs/research/core6_avax015_shadow.yaml")
    output_json_path = _project_path(args.output_json, "reports/research/core6_avax_shadow_cycle/latest.json")
    output_md_path = _project_path(args.output_md, "reports/research/core6_avax_shadow_cycle/latest.md")

    recorder = ResearchRecorder(base_dir=PROJECT_ROOT / "reports" / "runs")
    task_config = {
        "task": {
            "name": "core6_avax_shadow_cycle",
        },
        "inputs": {
            "ab_config_path": str(ab_config_path),
            "shadow_config_path": str(shadow_config_path),
            "champion_name": str(args.champion_name),
            "baseline_name": str(args.baseline_name),
        },
        "outputs": {
            "output_json_path": str(output_json_path),
            "output_md_path": str(output_md_path),
        },
    }
    run = recorder.start_run(task_name="core6_avax_shadow_cycle", task_config=task_config)

    ab_result = _run_script("run_rolling_window_sweep.py", ab_config_path, cwd=PROJECT_ROOT)
    shadow_result = _run_script("run_core6_window_diagnostics.py", shadow_config_path, cwd=PROJECT_ROOT)
    run.write_text("artifacts/hotpath.stdout.txt", ab_result.stdout)
    run.write_text("artifacts/hotpath.stderr.txt", ab_result.stderr)
    run.write_text("artifacts/shadow.stdout.txt", shadow_result.stdout)
    run.write_text("artifacts/shadow.stderr.txt", shadow_result.stderr)

    hotpath_report_path = _report_path_from_config(ab_config_path)
    shadow_report_path = _report_path_from_config(shadow_config_path)
    hotpath_report = json.loads(hotpath_report_path.read_text(encoding="utf-8"))
    shadow_report = json.loads(shadow_report_path.read_text(encoding="utf-8"))

    summary = build_shadow_cycle_summary(
        hotpath_report=hotpath_report,
        shadow_report=shadow_report,
        champion_name=str(args.champion_name),
        baseline_name=str(args.baseline_name),
    )
    summary["inputs"] = {
        "ab_config_path": str(ab_config_path),
        "shadow_config_path": str(shadow_config_path),
        "hotpath_report_path": str(hotpath_report_path),
        "shadow_report_path": str(shadow_report_path),
    }
    markdown = build_shadow_cycle_markdown(
        summary=summary,
        hotpath_report_path=hotpath_report_path,
        shadow_report_path=shadow_report_path,
    )

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
            "recommend_shadow": bool((summary.get("decision") or {}).get("recommend_shadow")),
            "reason": (summary.get("decision") or {}).get("reason"),
            "output_json_path": str(output_json_path),
            "output_md_path": str(output_md_path),
        },
    )

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"report_written={output_json_path}")
    print(f"markdown_written={output_md_path}")
    print(f"run_dir={run.run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
