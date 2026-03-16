#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.research.window_diagnostics import run_window_diagnostic_task


def main() -> int:
    task_config_path = sys.argv[1] if len(sys.argv) > 1 else "configs/research/core6_window_diagnostics.yaml"
    result = run_window_diagnostic_task(project_root=PROJECT_ROOT, task_config_path=task_config_path)
    preview = [
        {
            "name": window.get("name"),
            "metrics": ((window.get("summary") or {}).get("metrics") or {}),
            "activity": ((window.get("summary") or {}).get("activity") or {}),
            "skip_reason_counts": ((window.get("summary") or {}).get("skip_reason_counts") or {}),
            "window_dir": window.get("window_dir"),
        }
        for window in (result.result.get("windows") or [])
    ]
    print(json.dumps(preview, ensure_ascii=False, indent=2))
    print(f"report_written={result.latest_report_path}")
    print(f"run_dir={result.run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
