#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.runtime_config import resolve_runtime_config_path
from main import main as run_main


SHADOW_REPORT_REDIRECTS: tuple[tuple[str, str], ...] = (
    ("reports/runs", "reports/shadow_tuned_xgboost/runs"),
    ("reports/budget_state", "reports/shadow_tuned_xgboost/budget_state"),
    ("reports/cost_events", "reports/shadow_tuned_xgboost/cost_events"),
    ("reports/cost_stats", "reports/shadow_tuned_xgboost/cost_stats"),
    ("reports/spread_snapshots", "reports/shadow_tuned_xgboost/spread_snapshots"),
    ("reports/ml_training_data.db", "reports/shadow_tuned_xgboost/ml_training_data.db"),
    ("reports/okx_spot_instruments.json", "reports/shadow_tuned_xgboost/okx_spot_instruments.json"),
    ("reports/universe_cache.json", "reports/shadow_tuned_xgboost/universe_cache.json"),
    ("reports/trend_cache.json", "reports/shadow_tuned_xgboost/trend_cache.json"),
    ("reports/orders.sqlite", "reports/shadow_tuned_xgboost/orders.sqlite"),
    ("reports/fills.sqlite", "reports/shadow_tuned_xgboost/fills.sqlite"),
    ("reports/positions.sqlite", "reports/shadow_tuned_xgboost/positions.sqlite"),
    ("reports/bills.sqlite", "reports/shadow_tuned_xgboost/bills.sqlite"),
    ("reports/kill_switch.json", "reports/shadow_tuned_xgboost/kill_switch.json"),
    ("reports/reconcile_status.json", "reports/shadow_tuned_xgboost/reconcile_status.json"),
    ("reports/reconcile_failure_state.json", "reports/shadow_tuned_xgboost/reconcile_failure_state.json"),
    ("reports/ledger_state.json", "reports/shadow_tuned_xgboost/ledger_state.json"),
    ("reports/ledger_status.json", "reports/shadow_tuned_xgboost/ledger_status.json"),
    ("reports/portfolio_optimizer_state.json", "reports/shadow_tuned_xgboost/portfolio_optimizer_state.json"),
    ("reports/topk_dropout_state.json", "reports/shadow_tuned_xgboost/topk_dropout_state.json"),
    ("reports/alpha_dynamic_weights_by_regime.json", "reports/shadow_tuned_xgboost/alpha_dynamic_weights_by_regime.json"),
    ("reports/alpha_ic_monitor.json", "reports/shadow_tuned_xgboost/alpha_ic_monitor.json"),
    ("reports/alpha_ic_history.jsonl", "reports/shadow_tuned_xgboost/alpha_ic_history.jsonl"),
    ("reports/alpha_ic_timeseries.jsonl", "reports/shadow_tuned_xgboost/alpha_ic_timeseries.jsonl"),
    ("reports/model_promotion_decision.json", "reports/shadow_tuned_xgboost/unused_model_promotion_decision.json"),
    ("reports/ml_runtime_status.json", "reports/shadow_tuned_xgboost/ml_runtime_status.json"),
    ("reports/ml_overlay_impact.json", "reports/shadow_tuned_xgboost/ml_overlay_impact.json"),
    ("reports/ml_overlay_impact_history.jsonl", "reports/shadow_tuned_xgboost/ml_overlay_impact_history.jsonl"),
    ("reports/ml_overlay_impact_state.json", "reports/shadow_tuned_xgboost/ml_overlay_impact_state.json"),
    ("reports/regime_history.db", "reports/shadow_tuned_xgboost/regime_history.db"),
    ("reports/slippage.sqlite", "reports/shadow_tuned_xgboost/slippage.sqlite"),
    ("reports/negative_expectancy_cooldown.json", "reports/shadow_tuned_xgboost/negative_expectancy_cooldown.json"),
    ("reports/order_state_machine.json", "reports/shadow_tuned_xgboost/order_state_machine.json"),
    ("reports/stop_loss_state.json", "reports/shadow_tuned_xgboost/stop_loss_state.json"),
    ("reports/fixed_stop_loss_state.json", "reports/shadow_tuned_xgboost/fixed_stop_loss_state.json"),
    ("reports/profit_taking_state.json", "reports/shadow_tuned_xgboost/profit_taking_state.json"),
    ("reports/highest_px_state.json", "reports/shadow_tuned_xgboost/highest_px_state.json"),
    ("reports/rank_exit_cooldown_state.json", "reports/shadow_tuned_xgboost/rank_exit_cooldown_state.json"),
    ("reports/take_profit_cooldown_state.json", "reports/shadow_tuned_xgboost/take_profit_cooldown_state.json"),
    ("reports/auto_risk_eval.json", "reports/shadow_tuned_xgboost/auto_risk_eval.json"),
    ("reports/auto_blacklist.json", "reports/shadow_tuned_xgboost/auto_blacklist.json"),
)


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


def _merge_directory_contents(src: Path, dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for child in list(src.iterdir()):
        target = dst / child.name
        if child.is_dir() and not child.is_symlink():
            _merge_directory_contents(child, target)
            try:
                child.rmdir()
            except OSError:
                pass
            continue
        if target.exists():
            continue
        shutil.move(str(child), str(target))


def _rel_symlink(source: Path, target: Path) -> None:
    rel_target = os.path.relpath(target, start=source.parent)
    os.symlink(rel_target, source, target_is_directory=target.is_dir())


def _redirect_shadow_report_path(project_root: Path, source_rel: str, target_rel: str) -> None:
    source = project_root / source_rel
    target = project_root / target_rel

    target.parent.mkdir(parents=True, exist_ok=True)
    if target.suffix:
        target.parent.mkdir(parents=True, exist_ok=True)
    else:
        target.mkdir(parents=True, exist_ok=True)

    if source.is_symlink():
        try:
            if source.resolve() == target.resolve():
                return
        except OSError:
            pass
        source.unlink()
    elif source.exists():
        if source.is_dir():
            _merge_directory_contents(source, target)
            try:
                source.rmdir()
            except OSError:
                shutil.rmtree(source)
        else:
            if not target.exists():
                shutil.move(str(source), str(target))
            else:
                source.unlink()

    source.parent.mkdir(parents=True, exist_ok=True)
    _rel_symlink(source, target)


def _prepare_shadow_reports_namespace(project_root: Path) -> None:
    for source_rel, target_rel in SHADOW_REPORT_REDIRECTS:
        _redirect_shadow_report_path(project_root, source_rel, target_rel)


def _build_shadow_config(base_cfg_path: Path, overrides_path: Path, output_path: Path) -> Path:
    merged = _deep_merge(_load_yaml(base_cfg_path), _load_yaml(overrides_path))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        yaml.safe_dump(merged, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    return output_path


def _resolve_shadow_base_config_path(raw_base_config_path: str | None = None) -> Path:
    return Path(resolve_runtime_config_path(raw_base_config_path, project_root=PROJECT_ROOT)).resolve()


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
        "--base-config",
        default=None,
        help="base config to merge with shadow_tuned_xgboost_overrides.yaml; defaults to the active runtime config",
    )
    parser.add_argument(
        "--keep-toplevel-artifacts",
        action="store_true",
        help="allow top-level reports/alpha_snapshot.json style artifacts to be overwritten",
    )
    args = parser.parse_args()

    shadow_root = PROJECT_ROOT / "reports" / "shadow_tuned_xgboost"
    merged_cfg_path = shadow_root / "generated_config.yaml"
    base_cfg_path = _resolve_shadow_base_config_path(args.base_config)
    overrides_path = PROJECT_ROOT / "configs" / "shadow_tuned_xgboost_overrides.yaml"

    _prepare_shadow_reports_namespace(PROJECT_ROOT)
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
