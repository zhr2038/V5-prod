#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from main import main as run_main


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"config file not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"config file must contain a mapping: {path}")
    return data


def _deep_merge(base: Any, overrides: Any) -> Any:
    if isinstance(base, dict) and isinstance(overrides, dict):
        merged = dict(base)
        for key, value in overrides.items():
            merged[key] = _deep_merge(merged.get(key), value)
        return merged
    return overrides


def _build_shadow_config(base_cfg_path: Path, overrides_path: Path, output_path: Path) -> Path:
    merged = _deep_merge(_load_yaml(base_cfg_path), _load_yaml(overrides_path))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        yaml.safe_dump(merged, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    return output_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run tuned XGBoost in paper/shadow mode using live_prod as the base config."
    )
    parser.add_argument(
        "--provider",
        default="okx",
        choices=("okx", "mock"),
        help="market data provider to use for the shadow run",
    )
    parser.add_argument(
        "--keep-toplevel-artifacts",
        action="store_true",
        help="allow top-level reports/alpha_snapshot.json style artifacts to be overwritten",
    )
    args = parser.parse_args()

    shadow_root = PROJECT_ROOT / "reports" / "shadow_tuned_xgboost"
    merged_cfg_path = shadow_root / "generated_config.yaml"
    base_cfg_path = PROJECT_ROOT / "configs" / "live_prod.yaml"
    overrides_path = PROJECT_ROOT / "configs" / "shadow_tuned_xgboost_overrides.yaml"

    final_cfg_path = _build_shadow_config(base_cfg_path, overrides_path, merged_cfg_path)

    os.environ["V5_CONFIG"] = str(final_cfg_path)
    os.environ["V5_DATA_PROVIDER"] = str(args.provider)
    os.environ.setdefault(
        "V5_RUN_ID",
        "shadow_tuned_xgboost_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
    )
    if not args.keep_toplevel_artifacts:
        os.environ["V5_DISABLE_TOPLEVEL_ARTIFACTS"] = "1"

    print(f"shadow_config={final_cfg_path}")
    print(f"shadow_root={shadow_root}")
    print("shadow_mode=dry_run")
    print("shadow_model=models/ml_factor_model_gpu_tuned")
    run_main()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
