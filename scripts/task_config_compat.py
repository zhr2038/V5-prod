from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Callable


LEGACY_TASK_CONFIGS: dict[str, dict[str, Any]] = {
    "configs/research/ml_training.yaml": {
        "task": {"name": "ml_training"},
        "paths": {
            "runs_dir": "reports/runs",
            "db_path": "reports/ml_training_data.db",
            "csv_path": "reports/ml_training_data.csv",
            "history_path": "reports/ml_training_history.json",
            "model_path": "models/ml_factor_model",
        },
        "dataset": {
            "min_samples": 200,
            "target_col": "future_return_6h",
            "target_mode": "forward_edge_rank",
            "feature_groups": ["classic"],
            "include_time_features": False,
            "feature_selector": "stable",
            "selected_feature_count": 12,
            "rolling_window_days": 60,
            "prediction_horizon": 6,
            "min_symbol_samples": 48,
            "min_symbol_target_std": 1.0e-6,
            "min_group_size": 2,
            "min_group_coverage_ratio": 0.9,
            "align_cycles": True,
        },
        "model": {
            "candidates": ["ridge"],
            "ridge_alpha": 50.0,
            "hist_gbm_max_depth": 1,
            "hist_gbm_learning_rate": 0.05,
            "hist_gbm_max_iter": 120,
            "hist_gbm_min_samples_leaf": 120,
        },
        "gate": {
            "min_valid_ic": 0.0,
            "max_ic_gap": 0.25,
            "min_cv_mean_ic": 0.01,
            "max_cv_std": 0.15,
        },
        "recency_weighting": {
            "half_life_days": 5,
            "max_weight": 3.0,
        },
    },
    "configs/research/ml_training_gpu.yaml": {
        "task": {"name": "ml_training_gpu"},
        "paths": {
            "runs_dir": "reports/runs",
            "db_path": "reports/ml_training_data.db",
            "csv_path": "reports/ml_training_data.csv",
            "history_path": "reports/ml_training_history.json",
            "model_path": "models/ml_factor_model",
        },
        "dataset": {
            "min_samples": 200,
            "target_col": "future_return_6h",
            "target_mode": "forward_edge_rank",
            "feature_groups": ["classic"],
            "include_time_features": False,
            "feature_selector": "stable",
            "selected_feature_count": 12,
            "rolling_window_days": 60,
            "prediction_horizon": 6,
            "min_symbol_samples": 48,
            "min_symbol_target_std": 1.0e-6,
            "min_group_size": 2,
            "min_group_coverage_ratio": 0.9,
            "align_cycles": True,
        },
        "model": {
            "candidates": ["xgboost", "ridge"],
            "n_jobs": -1,
            "xgboost_n_estimators": 300,
            "xgboost_max_depth": 4,
            "xgboost_learning_rate": 0.05,
            "xgboost_subsample": 0.8,
            "xgboost_colsample_bytree": 0.8,
            "xgboost_reg_alpha": 1.0,
            "xgboost_reg_lambda": 4.0,
            "xgboost_max_bin": 256,
            "xgboost_compute_device": "auto",
            "ridge_alpha": 50.0,
        },
        "gate": {
            "min_valid_ic": 0.0,
            "max_ic_gap": 0.25,
            "min_cv_mean_ic": 0.01,
            "max_cv_std": 0.15,
        },
        "recency_weighting": {
            "half_life_days": 5,
            "max_weight": 3.0,
        },
        "parallel": {
            "candidate_workers": 2,
            "cv_workers": 4,
        },
    },
    "configs/research/walk_forward.yaml": {
        "task": {"name": "walk_forward"},
        "paths": {
            "runs_dir": "reports/runs",
            "output_report_path": "reports/walk_forward.json",
        },
        "walk_forward": {
            "env_path": ".env",
            "provider": "mock",
            "mock_seed": 7,
            "collect_ml_training_data": False,
            "ohlcv_limit": 2880,
            "folds": 4,
        },
    },
    "configs/research/walk_forward_prod_cache.yaml": {
        "task": {"name": "walk_forward_prod_cache"},
        "paths": {
            "runs_dir": "reports/runs",
            "output_report_path": "reports/research/walk_forward/prod_cache_live_prod.json",
        },
        "walk_forward": {
            "env_path": ".env",
            "provider": "cache",
            "cache_dir": "data/cache",
            "collect_ml_training_data": False,
            "ohlcv_limit": 720,
            "folds": 4,
        },
    },
}


def _task_config_key(project_root: Path, raw_config_path: str | Path) -> str:
    raw_text = str(raw_config_path or "").strip().replace("\\", "/")
    raw_path = Path(raw_text)
    if raw_path.is_absolute():
        try:
            return raw_path.resolve().relative_to(project_root.resolve()).as_posix()
        except ValueError:
            return raw_path.as_posix()
    return raw_text.lstrip("./")


def load_task_config_with_compat(
    project_root: Path,
    raw_config_path: str | Path,
    loader: Callable[[str | Path], dict[str, Any]],
) -> dict[str, Any]:
    target = Path(raw_config_path)
    if not target.is_absolute():
        target = project_root / target

    task_config = loader(target)
    if task_config:
        return task_config

    compat = LEGACY_TASK_CONFIGS.get(_task_config_key(project_root, raw_config_path))
    return deepcopy(compat) if compat else {}
