#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.research.ml_training_sweep import run_ml_training_sweep
from src.research.task_runner import load_task_config


def main() -> int:
    config_path = os.getenv("V5_ML_SWEEP_CONFIG", "configs/research/ml_training_sweep.yaml")
    task_config = load_task_config(PROJECT_ROOT / config_path)
    if not task_config:
        print(f"unable to load ml training sweep config: {config_path}")
        return 1
    result = run_ml_training_sweep(project_root=PROJECT_ROOT, sweep_config=task_config)
    return int(result.get("exit_code", 1))


if __name__ == "__main__":
    raise SystemExit(main())
