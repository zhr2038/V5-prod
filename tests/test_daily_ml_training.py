from pathlib import Path

import pandas as pd
import pytest

from scripts import daily_ml_training as daily_training


SCRIPT_PATH = Path("scripts/daily_ml_training.py")
ENV_KEYS = [
    "V5_ML_TASK_CONFIG",
    "V5_ML_FEATURE_GROUPS",
    "V5_ML_MIN_SAMPLES",
    "V5_ML_TARGET_MODE",
    "V5_ML_INCLUDE_TIME_FEATURES",
    "V5_ML_MIN_SYMBOL_SAMPLES",
    "V5_ML_MIN_SYMBOL_TARGET_STD",
    "V5_ML_MIN_GROUP_SIZE",
    "V5_ML_MIN_GROUP_COVERAGE_RATIO",
    "V5_ML_PREDICTION_HORIZON",
    "V5_ML_ROLLING_WINDOW_DAYS",
    "V5_ML_TRAIN_LOOKBACK_DAYS",
    "V5_ML_FEATURE_SELECTOR",
    "V5_ML_SELECTED_FEATURE_COUNT",
    "V5_ML_ALIGN_CYCLES",
    "V5_ML_CANDIDATES",
    "V5_ML_RIDGE_ALPHA",
    "V5_ML_HGB_MAX_DEPTH",
    "V5_ML_HGB_LEARNING_RATE",
    "V5_ML_HGB_MAX_ITER",
    "V5_ML_HGB_MIN_SAMPLES_LEAF",
    "V5_ML_LGBM_N_ESTIMATORS",
    "V5_ML_LGBM_MAX_DEPTH",
    "V5_ML_LGBM_LEARNING_RATE",
    "V5_ML_XGB_N_ESTIMATORS",
    "V5_ML_XGB_MAX_DEPTH",
    "V5_ML_XGB_LEARNING_RATE",
    "V5_ML_XGB_SUBSAMPLE",
    "V5_ML_XGB_COLSAMPLE_BYTREE",
    "V5_ML_XGB_REG_ALPHA",
    "V5_ML_XGB_REG_LAMBDA",
    "V5_ML_XGB_MAX_BIN",
    "V5_ML_XGB_DEVICE",
    "V5_ML_N_JOBS",
    "V5_ML_MIN_VALID_IC",
    "V5_ML_MAX_IC_GAP",
    "V5_ML_MIN_CV_MEAN_IC",
    "V5_ML_MAX_CV_STD",
    "V5_ML_RECENCY_HALFLIFE_DAYS",
    "V5_ML_RECENCY_MAX_WEIGHT",
    "V5_ML_CANDIDATE_WORKERS",
    "V5_ML_CV_WORKERS",
    "V5_ML_GPU_CV_WORKERS",
]


def _clear_ml_env(monkeypatch) -> None:
    for key in ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_daily_ml_training_defaults_to_rank_target(monkeypatch):
    _clear_ml_env(monkeypatch)

    task_config = daily_training._build_task_config()

    assert task_config["dataset"]["target_mode"] == "forward_edge_rank"


def test_daily_ml_training_defaults_to_ridge_only(monkeypatch):
    _clear_ml_env(monkeypatch)

    task_config = daily_training._build_task_config()

    assert task_config["model"]["candidates"] == ["ridge"]


def test_daily_ml_training_respects_config_candidates_when_env_not_set(monkeypatch):
    _clear_ml_env(monkeypatch)
    monkeypatch.delenv("V5_ML_CANDIDATES", raising=False)
    monkeypatch.setenv("V5_ML_TASK_CONFIG", "configs/research/ml_training_gpu.yaml")

    task_config = daily_training._build_task_config()

    assert "ridge" in task_config["model"]["candidates"]
    if daily_training.XGBOOST_AVAILABLE:
        assert "xgboost" in task_config["model"]["candidates"]


def test_daily_ml_training_uses_stronger_ridge_regularization(monkeypatch):
    _clear_ml_env(monkeypatch)

    task_config = daily_training._build_task_config()

    assert task_config["model"]["ridge_alpha"] == 50.0


def test_daily_ml_training_uses_wider_symbol_coverage_and_stable_feature_selection(monkeypatch):
    _clear_ml_env(monkeypatch)

    task_config = daily_training._build_task_config()

    assert task_config["dataset"]["min_symbol_samples"] == 48
    assert task_config["dataset"]["feature_selector"] == "stable"


def test_daily_ml_training_uses_rolling_window_and_recency_weighting_defaults(monkeypatch):
    _clear_ml_env(monkeypatch)

    task_config = daily_training._build_task_config()

    assert task_config["dataset"]["rolling_window_days"] == 60.0
    assert task_config["dataset"]["min_group_size"] == 2
    assert task_config["dataset"]["min_group_coverage_ratio"] == 0.9
    assert task_config["recency_weighting"]["half_life_days"] == 5.0
    assert task_config["recency_weighting"]["max_weight"] == 3.0


def test_daily_ml_training_preserves_yaml_defaults_without_env_overrides(tmp_path, monkeypatch):
    _clear_ml_env(monkeypatch)
    config_path = tmp_path / "ml_training_custom.yaml"
    config_path.write_text(
        "\n".join(
            [
                "dataset:",
                "  target_mode: raw",
                "  min_symbol_samples: 64",
                "  min_group_size: 3",
                "  min_group_coverage_ratio: 0.8",
                "  prediction_horizon: 12",
                "  rolling_window_days: 45",
                "  feature_selector: optimize",
                "model:",
                "  candidates: [xgboost]",
                "  xgboost_n_estimators: 256",
                "  xgboost_max_depth: 2",
                "  xgboost_reg_lambda: 8.0",
                "gate:",
                "  max_cv_std: 0.12",
                "recency_weighting:",
                "  half_life_days: 3",
                "  max_weight: 2.5",
                "parallel:",
                "  gpu_cv_workers: 2",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("V5_ML_TASK_CONFIG", str(config_path))

    task_config = daily_training._build_task_config()

    assert task_config["dataset"]["target_mode"] == "raw"
    assert task_config["dataset"]["min_symbol_samples"] == 64
    assert task_config["dataset"]["min_group_size"] == 3
    assert task_config["dataset"]["min_group_coverage_ratio"] == 0.8
    assert task_config["dataset"]["prediction_horizon"] == 12
    assert task_config["dataset"]["rolling_window_days"] == 45.0
    assert task_config["dataset"]["feature_selector"] == "optimize"
    assert task_config["model"]["candidates"] == ["xgboost"]
    assert task_config["model"]["xgboost_n_estimators"] == 256
    assert task_config["model"]["xgboost_max_depth"] == 2
    assert task_config["model"]["xgboost_reg_lambda"] == 8.0
    assert task_config["gate"]["max_cv_std"] == 0.12
    assert task_config["recency_weighting"]["half_life_days"] == 3.0
    assert task_config["recency_weighting"]["max_weight"] == 2.5
    assert task_config["parallel"]["gpu_cv_workers"] == 2


def test_recency_weights_favor_latest_groups():
    groups = pd.Series([
        1_700_000_000_000,
        1_700_000_000_000,
        1_700_043_200_000,
        1_700_086_400_000,
    ])

    weights = daily_training._build_recency_sample_weights(
        groups,
        half_life_days=1.0,
        max_weight=3.0,
    )

    assert len(weights) == len(groups)
    assert weights.iloc[-1] > weights.iloc[0]
    assert float(weights.mean()) == pytest.approx(1.0)


def test_rolling_window_keeps_only_recent_groups():
    X = pd.DataFrame({"f": range(6)})
    y = pd.Series(range(6))
    groups = pd.Series([
        1_700_000_000_000,
        1_700_000_000_000,
        1_700_086_400_000,
        1_700_086_400_000,
        1_700_172_800_000,
        1_700_172_800_000,
    ])

    X_out, y_out, groups_out, meta = daily_training._apply_rolling_window(
        X,
        y,
        groups,
        lookback_days=1.5,
    )

    assert meta["enabled"] is True
    assert len(X_out) == len(y_out) == len(groups_out) == 4
    assert int(groups_out.nunique()) == 2


def test_align_cycle_samples_dedupes_same_hour_duplicates():
    base_ts = 1_700_000_300_000
    df = pd.DataFrame(
        {
            "timestamp": [
                base_ts,
                base_ts + 15 * 60 * 1000,
                base_ts,
                base_ts + 20 * 60 * 1000,
            ],
            "symbol": ["BTC/USDT", "BTC/USDT", "ETH/USDT", "ETH/USDT"],
            "future_return_6h": [0.01, 0.02, 0.03, 0.04],
        }
    )

    out, meta = daily_training._align_cycle_samples(df)

    assert meta["duplicates_removed"] == 2
    assert len(out) == 2
    assert out["timestamp"].nunique() == 1
    assert set(out["symbol"]) == {"BTC/USDT", "ETH/USDT"}
