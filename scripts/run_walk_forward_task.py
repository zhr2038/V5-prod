#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.task_config_compat import load_task_config_with_compat as load_task_config_with_compat_legacy
from src.research.task_runner import load_task_config, run_walk_forward_task


def _load_walk_forward_task_config(raw_config_path: str) -> dict:
    return load_task_config_with_compat_legacy(PROJECT_ROOT, raw_config_path, load_task_config)


def _resolve_task_config_path(argv: list[str] | None = None) -> str:
    parser = argparse.ArgumentParser()
    parser.add_argument("task_config_path", nargs="?", default=None)
    args = parser.parse_args(argv)
    if args.task_config_path:
        return str(args.task_config_path)
    return os.getenv("V5_RESEARCH_TASK_CONFIG", "configs/research/walk_forward.yaml")


def main(argv: list[str] | None = None) -> int:
    config_path = _resolve_task_config_path(argv)
    task_config = _load_walk_forward_task_config(config_path)
    if not task_config:
        print(f"unable to load walk-forward task config: {config_path}")
        return 1
    result = run_walk_forward_task(project_root=PROJECT_ROOT, task_config=task_config)
    return int(result.get("exit_code", 1))


if __name__ == "__main__":
    raise SystemExit(main())
