#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.execution.ml_factor_model import LIGHTGBM_AVAILABLE, XGBOOST_AVAILABLE
from src.research.processors import (
    align_cycle_samples as _align_cycle_samples_impl,
    apply_rolling_window as _apply_rolling_window_impl,
    build_recency_sample_weights as _build_recency_sample_weights_impl,
)
from src.research.task_runner import load_task_config, run_ml_training_task


HISTORY_PATH = PROJECT_ROOT / "reports/ml_training_history.json"
CSV_PATH = PROJECT_ROOT / "reports/ml_training_data.csv"
MODEL_PATH = PROJECT_ROOT / "models/ml_factor_model"


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _align_cycle_samples(df: pd.DataFrame):
    return _align_cycle_samples_impl(df)


def _apply_rolling_window(
    X: pd.DataFrame,
    y: pd.Series,
    groups: pd.Series,
    *,
    lookback_days: float,
):
    return _apply_rolling_window_impl(X, y, groups, lookback_days=lookback_days)


def _build_recency_sample_weights(
    groups: pd.Series,
    *,
    half_life_days: float,
    max_weight: float,
):
    return _build_recency_sample_weights_impl(
        groups,
        half_life_days=half_life_days,
        max_weight=max_weight,
    )


def _candidate_models() -> list[str]:
    raw = os.getenv("V5_ML_CANDIDATES", "ridge")
    out = [x.strip().lower() for x in raw.split(",") if x.strip()]
    if not LIGHTGBM_AVAILABLE:
        out = [x for x in out if x != "lightgbm"]
    if not XGBOOST_AVAILABLE:
        out = [x for x in out if x != "xgboost"]
    return out or ["ridge"]


def _build_task_config() -> dict:
    config_path = os.getenv("V5_ML_TASK_CONFIG", "configs/research/ml_training.yaml")
    task_config = load_task_config(PROJECT_ROOT / config_path)
    if not task_config:
        task_config = {}

    task = task_config.setdefault("task", {})
    paths = task_config.setdefault("paths", {})
    dataset = task_config.setdefault("dataset", {})
    model = task_config.setdefault("model", {})
    gate = task_config.setdefault("gate", {})
    recency = task_config.setdefault("recency_weighting", {})

    task["name"] = str(task.get("name") or "ml_training")
    paths["history_path"] = str(paths.get("history_path") or "reports/ml_training_history.json")
    paths["csv_path"] = str(paths.get("csv_path") or "reports/ml_training_data.csv")
    paths["db_path"] = str(paths.get("db_path") or "reports/ml_training_data.db")
    paths["model_path"] = str(paths.get("model_path") or "models/ml_factor_model")
    paths["runs_dir"] = str(paths.get("runs_dir") or "reports/runs")

    feature_groups_raw = os.getenv("V5_ML_FEATURE_GROUPS", ",".join(dataset.get("feature_groups") or ["classic"]))
    dataset["feature_groups"] = [x.strip().lower() for x in feature_groups_raw.split(",") if x.strip()] or ["classic"]
    dataset["min_samples"] = int(os.getenv("V5_ML_MIN_SAMPLES", str(dataset.get("min_samples", 200))))
    dataset["target_col"] = str(dataset.get("target_col") or "future_return_6h")
    dataset["target_mode"] = os.getenv(
        "V5_ML_TARGET_MODE",
        str(dataset.get("target_mode", "forward_edge_rank")),
    ).strip().lower()
    dataset["include_time_features"] = _env_bool("V5_ML_INCLUDE_TIME_FEATURES", bool(dataset.get("include_time_features", False)))
    dataset["min_symbol_samples"] = int(os.getenv("V5_ML_MIN_SYMBOL_SAMPLES", str(dataset.get("min_symbol_samples", 48))))
    dataset["min_symbol_target_std"] = float(
        os.getenv("V5_ML_MIN_SYMBOL_TARGET_STD", str(dataset.get("min_symbol_target_std", 1e-6)))
    )
    dataset["min_group_size"] = int(os.getenv("V5_ML_MIN_GROUP_SIZE", str(dataset.get("min_group_size", 2))))
    dataset["min_group_coverage_ratio"] = float(
        os.getenv("V5_ML_MIN_GROUP_COVERAGE_RATIO", str(dataset.get("min_group_coverage_ratio", 0.9)))
    )
    dataset["prediction_horizon"] = int(os.getenv("V5_ML_PREDICTION_HORIZON", str(dataset.get("prediction_horizon", 6))))
    rolling_window_default = str(dataset.get("rolling_window_days", 60))
    dataset["rolling_window_days"] = float(
        os.getenv("V5_ML_ROLLING_WINDOW_DAYS", os.getenv("V5_ML_TRAIN_LOOKBACK_DAYS", rolling_window_default))
    )
    dataset["feature_selector"] = os.getenv(
        "V5_ML_FEATURE_SELECTOR",
        str(dataset.get("feature_selector", "stable")),
    ).strip().lower()
    dataset["selected_feature_count"] = int(os.getenv("V5_ML_SELECTED_FEATURE_COUNT", str(dataset.get("selected_feature_count", 12))))
    dataset["align_cycles"] = _env_bool("V5_ML_ALIGN_CYCLES", bool(dataset.get("align_cycles", True)))

    env_candidates = os.getenv("V5_ML_CANDIDATES")
    if env_candidates is None:
        configured = [str(x).strip().lower() for x in (model.get("candidates") or ["ridge"]) if str(x).strip()]
        if not LIGHTGBM_AVAILABLE:
            configured = [x for x in configured if x != "lightgbm"]
        if not XGBOOST_AVAILABLE:
            configured = [x for x in configured if x != "xgboost"]
        model["candidates"] = configured or ["ridge"]
    else:
        model["candidates"] = _candidate_models()
    model["ridge_alpha"] = float(os.getenv("V5_ML_RIDGE_ALPHA", str(model.get("ridge_alpha", 50.0))))
    model["hist_gbm_max_depth"] = int(os.getenv("V5_ML_HGB_MAX_DEPTH", str(model.get("hist_gbm_max_depth", 1))))
    model["hist_gbm_learning_rate"] = float(
        os.getenv("V5_ML_HGB_LEARNING_RATE", str(model.get("hist_gbm_learning_rate", 0.05)))
    )
    model["hist_gbm_max_iter"] = int(os.getenv("V5_ML_HGB_MAX_ITER", str(model.get("hist_gbm_max_iter", 120))))
    model["hist_gbm_min_samples_leaf"] = int(
        os.getenv("V5_ML_HGB_MIN_SAMPLES_LEAF", str(model.get("hist_gbm_min_samples_leaf", 120)))
    )
    model["lightgbm_n_estimators"] = int(os.getenv("V5_ML_LGBM_N_ESTIMATORS", str(model.get("lightgbm_n_estimators", 50))))
    model["lightgbm_max_depth"] = int(os.getenv("V5_ML_LGBM_MAX_DEPTH", str(model.get("lightgbm_max_depth", 4))))
    model["lightgbm_learning_rate"] = float(
        os.getenv("V5_ML_LGBM_LEARNING_RATE", str(model.get("lightgbm_learning_rate", 0.05)))
    )
    model["xgboost_n_estimators"] = int(os.getenv("V5_ML_XGB_N_ESTIMATORS", str(model.get("xgboost_n_estimators", 300))))
    model["xgboost_max_depth"] = int(os.getenv("V5_ML_XGB_MAX_DEPTH", str(model.get("xgboost_max_depth", 4))))
    model["xgboost_learning_rate"] = float(
        os.getenv("V5_ML_XGB_LEARNING_RATE", str(model.get("xgboost_learning_rate", 0.05)))
    )
    model["xgboost_subsample"] = float(os.getenv("V5_ML_XGB_SUBSAMPLE", str(model.get("xgboost_subsample", 0.8))))
    model["xgboost_colsample_bytree"] = float(
        os.getenv("V5_ML_XGB_COLSAMPLE_BYTREE", str(model.get("xgboost_colsample_bytree", 0.8)))
    )
    model["xgboost_reg_alpha"] = float(os.getenv("V5_ML_XGB_REG_ALPHA", str(model.get("xgboost_reg_alpha", 1.0))))
    model["xgboost_reg_lambda"] = float(os.getenv("V5_ML_XGB_REG_LAMBDA", str(model.get("xgboost_reg_lambda", 4.0))))
    model["xgboost_max_bin"] = int(os.getenv("V5_ML_XGB_MAX_BIN", str(model.get("xgboost_max_bin", 256))))
    model["xgboost_compute_device"] = str(
        os.getenv("V5_ML_XGB_DEVICE", str(model.get("xgboost_compute_device", "auto")))
    ).strip().lower()
    model["n_jobs"] = int(os.getenv("V5_ML_N_JOBS", str(model.get("n_jobs", -1))))

    gate["min_valid_ic"] = float(os.getenv("V5_ML_MIN_VALID_IC", str(gate.get("min_valid_ic", 0.0))))
    gate["max_ic_gap"] = float(os.getenv("V5_ML_MAX_IC_GAP", str(gate.get("max_ic_gap", 0.25))))
    gate["min_cv_mean_ic"] = float(os.getenv("V5_ML_MIN_CV_MEAN_IC", str(gate.get("min_cv_mean_ic", 0.01))))
    gate["max_cv_std"] = float(os.getenv("V5_ML_MAX_CV_STD", str(gate.get("max_cv_std", 0.15))))

    recency["half_life_days"] = float(
        os.getenv("V5_ML_RECENCY_HALFLIFE_DAYS", str(recency.get("half_life_days", 5)))
    )
    recency["max_weight"] = float(os.getenv("V5_ML_RECENCY_MAX_WEIGHT", str(recency.get("max_weight", 3.0))))

    parallel = task_config.setdefault("parallel", {})
    parallel["candidate_workers"] = int(os.getenv("V5_ML_CANDIDATE_WORKERS", str(parallel.get("candidate_workers", 1))))
    parallel["cv_workers"] = int(os.getenv("V5_ML_CV_WORKERS", str(parallel.get("cv_workers", 1))))
    parallel["gpu_cv_workers"] = int(os.getenv("V5_ML_GPU_CV_WORKERS", str(parallel.get("gpu_cv_workers", 1))))

    return task_config


def main() -> int:
    task_config = _build_task_config()
    result = run_ml_training_task(project_root=PROJECT_ROOT, task_config=task_config)
    if not result.get("gate_passed", False) and result.get("fail_reasons"):
        print(f"gate blocked model update: {', '.join(result['fail_reasons'])}")
    return int(result.get("exit_code", 1))


if __name__ == "__main__":
    raise SystemExit(main())
