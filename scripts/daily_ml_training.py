#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.execution.ml_data_collector import MLDataCollector
from src.execution.ml_factor_model import LIGHTGBM_AVAILABLE, MLFactorConfig, MLFactorModel
from src.execution.ml_feature_optimizer import optimize_features_for_training
from src.execution.ml_time_series_cv import GroupedTimeSeriesSplit, cross_sectional_ic


HISTORY_PATH = PROJECT_ROOT / "reports/ml_training_history.json"
CSV_PATH = PROJECT_ROOT / "reports/ml_training_data.csv"
MODEL_PATH = PROJECT_ROOT / "models/ml_factor_model"


def _load_history():
    if not HISTORY_PATH.exists():
        return []
    try:
        return json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []


def _append_history(entry: dict) -> None:
    hist = _load_history()
    hist.append(entry)
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    HISTORY_PATH.write_text(json.dumps(hist, ensure_ascii=False, indent=2), encoding="utf-8")


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _build_group_series(prep_meta: dict, expected_len: int) -> pd.Series:
    groups = pd.Series(prep_meta.get("timestamps") or []).reset_index(drop=True)
    if len(groups) != expected_len:
        raise ValueError(f"timestamp groups mismatch: {len(groups)} != {expected_len}")
    return groups


def _split_holdout_by_groups(
    X: pd.DataFrame,
    y: pd.Series,
    groups: pd.Series,
    holdout_fraction: float = 0.2,
    gap_groups: int = 0,
):
    unique_groups = pd.Index(groups.drop_duplicates().tolist())
    if len(unique_groups) < 2:
        raise ValueError("need at least 2 timestamp groups for holdout split")

    n_valid_groups = max(1, int(round(len(unique_groups) * holdout_fraction)))
    n_valid_groups = min(n_valid_groups, len(unique_groups) - 1)
    valid_start = len(unique_groups) - n_valid_groups
    train_end = valid_start - int(gap_groups)
    if train_end <= 0:
        raise ValueError("not enough timestamp groups after purge gap")

    train_groups = set(unique_groups[:train_end].tolist())
    valid_groups = set(unique_groups[valid_start:].tolist())
    train_mask = groups.isin(train_groups)
    valid_mask = groups.isin(valid_groups)
    if int(train_mask.sum()) == 0 or int(valid_mask.sum()) == 0:
        raise ValueError("invalid grouped holdout split")

    return (
        X.loc[train_mask].reset_index(drop=True),
        X.loc[valid_mask].reset_index(drop=True),
        y.loc[train_mask].reset_index(drop=True),
        y.loc[valid_mask].reset_index(drop=True),
        groups.loc[train_mask].reset_index(drop=True),
        groups.loc[valid_mask].reset_index(drop=True),
    )


def _trim_prep_meta(prep_meta: dict) -> dict:
    return {k: v for k, v in prep_meta.items() if k != "timestamps"}


def _candidate_models() -> list[str]:
    raw = os.getenv("V5_ML_CANDIDATES", "ridge")
    out = [x.strip().lower() for x in raw.split(",") if x.strip()]
    if not LIGHTGBM_AVAILABLE:
        out = [x for x in out if x != "lightgbm"]
    return out or ["ridge"]


def _build_base_config() -> MLFactorConfig:
    return MLFactorConfig(
        target_mode=os.getenv("V5_ML_TARGET_MODE", "forward_edge_rank").strip().lower(),
        include_time_features=_env_bool("V5_ML_INCLUDE_TIME_FEATURES", False),
        min_symbol_samples=int(os.getenv("V5_ML_MIN_SYMBOL_SAMPLES", "48")),
        min_symbol_target_std=float(os.getenv("V5_ML_MIN_SYMBOL_TARGET_STD", "1e-6")),
        prediction_horizon=int(os.getenv("V5_ML_PREDICTION_HORIZON", "6")),
    )


def _config_for_candidate(model_type: str, base_cfg: MLFactorConfig) -> MLFactorConfig:
    params = dict(base_cfg.__dict__)
    params["model_type"] = model_type
    if model_type == "ridge":
        params["alpha"] = float(os.getenv("V5_ML_RIDGE_ALPHA", "50.0"))
    elif model_type == "hist_gbm":
        params["max_depth"] = int(os.getenv("V5_ML_HGB_MAX_DEPTH", "1"))
        params["learning_rate"] = float(os.getenv("V5_ML_HGB_LEARNING_RATE", "0.05"))
        params["hgb_max_iter"] = int(os.getenv("V5_ML_HGB_MAX_ITER", "120"))
        params["hgb_min_samples_leaf"] = int(os.getenv("V5_ML_HGB_MIN_SAMPLES_LEAF", "120"))
    return MLFactorConfig(**params)


def _train_candidate_model(
    cfg: MLFactorConfig,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_valid: pd.DataFrame,
    y_valid: pd.Series,
    *,
    quiet: bool = False,
) -> MLFactorModel:
    model = MLFactorModel(cfg)
    model.feature_names = list(X_train.columns)
    if quiet:
        with redirect_stdout(StringIO()):
            model.train(X_train, y_train, X_valid, y_valid)
    else:
        model.train(X_train, y_train, X_valid, y_valid)
    return model


def _candidate_cv_result(
    cfg: MLFactorConfig,
    X: pd.DataFrame,
    y: pd.Series,
    groups: pd.Series,
    cv: GroupedTimeSeriesSplit,
) -> dict:
    scores = []
    for train_idx, test_idx in cv.split(X, y, groups=groups):
        X_train = X.iloc[train_idx].reset_index(drop=True)
        y_train = y.iloc[train_idx].reset_index(drop=True)
        X_test = X.iloc[test_idx].reset_index(drop=True)
        y_test = y.iloc[test_idx].reset_index(drop=True)
        test_groups = groups.iloc[test_idx].reset_index(drop=True)

        model = _train_candidate_model(cfg, X_train, y_train, X_test, y_test, quiet=True)
        pred = model.predict_batch(X_test)
        scores.append(float(cross_sectional_ic(test_groups, y_test, pred)))

    return {
        "scores": scores,
        "mean_score": float(pd.Series(scores).mean()) if scores else 0.0,
        "std_score": float(pd.Series(scores).std(ddof=0)) if scores else 0.0,
    }


def main() -> int:
    min_samples = int(os.getenv("V5_ML_MIN_SAMPLES", "200"))
    min_valid_ic = float(os.getenv("V5_ML_MIN_VALID_IC", "0.00"))
    max_ic_gap = float(os.getenv("V5_ML_MAX_IC_GAP", "0.25"))
    min_cv_mean_ic = float(os.getenv("V5_ML_MIN_CV_MEAN_IC", "0.01"))
    max_cv_std = float(os.getenv("V5_ML_MAX_CV_STD", "0.15"))

    collector = MLDataCollector(db_path=str(PROJECT_ROOT / "reports/ml_training_data.db"))
    csv_fallback_rows = 0
    if CSV_PATH.exists():
        try:
            csv_fallback_rows = int(len(pd.read_csv(CSV_PATH)))
        except Exception:
            csv_fallback_rows = 0

    stats = collector.get_statistics()
    labeled_records = int(stats.get("labeled_records", 0))
    if labeled_records >= min_samples:
        if not collector.export_training_data(str(CSV_PATH), min_samples=min_samples):
            if csv_fallback_rows < min_samples:
                print("failed to export training data and no usable csv fallback")
                return 1
    elif csv_fallback_rows >= min_samples:
        print(
            f"db has only {labeled_records} labeled records; "
            f"falling back to existing csv with {csv_fallback_rows} rows"
        )
    else:
        print(f"insufficient labeled records: db={labeled_records}, csv={csv_fallback_rows}, need>={min_samples}")
        return 0

    df = pd.read_csv(CSV_PATH)
    cfg = _build_base_config()
    feature_selector = os.getenv("V5_ML_FEATURE_SELECTOR", "stable").strip().lower()
    base_model = MLFactorModel(cfg)
    X_base, y, prep_meta = base_model.build_training_frame(df, target_col="future_return_6h")
    X = optimize_features_for_training(X_base, y, selector=feature_selector, n_features=12)
    if X.empty or len(X) < min_samples:
        print(f"training frame too small after cleaning: {len(X)}")
        return 1

    groups = _build_group_series(prep_meta, len(X))
    unique_groups = pd.Index(groups.drop_duplicates().tolist())
    purge_gap = int(cfg.prediction_horizon)
    X_train, X_valid, y_train, y_valid, train_groups, valid_groups = _split_holdout_by_groups(
        X,
        y,
        groups,
        gap_groups=purge_gap,
    )

    cv = GroupedTimeSeriesSplit(
        n_splits=min(5, max(2, len(unique_groups) - 1)),
        gap_groups=purge_gap,
    )

    candidates = _candidate_models()
    candidate_results = []
    for model_type in candidates:
        candidate_cfg = _config_for_candidate(model_type, cfg)
        try:
            model = _train_candidate_model(candidate_cfg, X_train, y_train, X_valid, y_valid)
            train_pred = model.predict_batch(X_train)
            valid_pred = model.predict_batch(X_valid)
            cv_res = _candidate_cv_result(candidate_cfg, X, y, groups, cv)
            candidate_results.append(
                {
                    "model_type": model_type,
                    "config": candidate_cfg.__dict__,
                    "model": model,
                    "train_ic": cross_sectional_ic(train_groups, y_train, train_pred),
                    "valid_ic": cross_sectional_ic(valid_groups, y_valid, valid_pred),
                    "cv_mean_ic": float(cv_res["mean_score"]),
                    "cv_std_ic": float(cv_res["std_score"]),
                    "cv_scores": [float(x) for x in cv_res["scores"]],
                }
            )
            print(
                f"candidate={model_type} grouped_valid_ic={candidate_results[-1]['valid_ic']:.4f} "
                f"cv_mean={candidate_results[-1]['cv_mean_ic']:.4f} "
                f"cv_std={candidate_results[-1]['cv_std_ic']:.4f} "
                f"gap={candidate_results[-1]['train_ic'] - candidate_results[-1]['valid_ic']:.4f}"
            )
        except Exception as e:
            candidate_results.append(
                {
                    "model_type": model_type,
                    "error": str(e),
                    "train_ic": -1.0,
                    "valid_ic": -1.0,
                    "cv_mean_ic": -1.0,
                    "cv_std_ic": 999.0,
                    "cv_scores": [],
                }
            )

    successful = [x for x in candidate_results if "model" in x]
    if not successful:
        print("no candidate model trained successfully")
        return 1

    successful.sort(
        key=lambda x: (x["valid_ic"], x["cv_mean_ic"], -(x["train_ic"] - x["valid_ic"])),
        reverse=True,
    )
    best = successful[0]
    ic_gap = float(best["train_ic"] - best["valid_ic"])
    cv_mean_ic = float(best["cv_mean_ic"])
    cv_std_ic = float(best["cv_std_ic"])
    print(
        f"selected={best['model_type']} grouped_valid_ic={best['valid_ic']:.4f} "
        f"cv_mean={cv_mean_ic:.4f} cv_std={cv_std_ic:.4f} ic_gap={ic_gap:.4f}"
    )

    fail_reasons = []
    if float(best["valid_ic"]) < min_valid_ic:
        fail_reasons.append(f"valid_ic<{min_valid_ic:.2f}")
    if ic_gap > max_ic_gap:
        fail_reasons.append(f"ic_gap>{max_ic_gap:.2f}")
    if cv_mean_ic < min_cv_mean_ic:
        fail_reasons.append(f"cv_mean_ic<{min_cv_mean_ic:.2f}")
    if cv_std_ic > max_cv_std:
        fail_reasons.append(f"cv_std_ic>{max_cv_std:.2f}")

    gate_passed = len(fail_reasons) == 0
    history_entry = {
        "timestamp": datetime.now().isoformat(),
        "samples": int(len(X)),
        "train_ic": float(best["train_ic"]),
        "valid_ic": float(best["valid_ic"]),
        "cv_mean_ic": cv_mean_ic,
        "cv_std_ic": cv_std_ic,
        "cv_scores": [float(x) for x in best["cv_scores"]],
        "feature_mode": "optimized",
        "selected_features": list(X.columns),
        "selector_reason": "optimize_features_for_training",
        "grouped_holdout": {
            "unique_groups": int(len(unique_groups)),
            "train_groups": int(train_groups.nunique()),
            "valid_groups": int(valid_groups.nunique()),
            "purge_gap_groups": purge_gap,
        },
        "candidate_models": [
            {
                "model_type": item["model_type"],
                "train_ic": float(item.get("train_ic", -1.0)),
                "valid_ic": float(item.get("valid_ic", -1.0)),
                "cv_mean_ic": float(item.get("cv_mean_ic", -1.0)),
                "cv_std_ic": float(item.get("cv_std_ic", -1.0)),
                "cv_scores": [float(x) for x in item.get("cv_scores", [])],
                "ic_gap": float(item.get("train_ic", -1.0) - item.get("valid_ic", -1.0)),
                **({"config": item["config"]} if "config" in item else {}),
                **({"error": item["error"]} if "error" in item else {}),
            }
            for item in candidate_results
        ],
        "selected_model_type": best["model_type"],
        "prep": _trim_prep_meta(prep_meta),
        "gate": {
            "min_valid_ic": min_valid_ic,
            "max_ic_gap": max_ic_gap,
            "min_cv_mean_ic": min_cv_mean_ic,
            "max_cv_std": max_cv_std,
            "passed": gate_passed,
            "fail_reasons": fail_reasons,
        },
        "model_saved": gate_passed,
        "config": {
            "model_type": best["model_type"],
            "candidates": candidates,
            "target_mode": cfg.target_mode,
            "include_time_features": cfg.include_time_features,
            "feature_selector": feature_selector,
        },
    }
    _append_history(history_entry)

    if not gate_passed:
        print(f"gate blocked model update: {', '.join(fail_reasons)}")
        return 0

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    best["model"].save_model(str(MODEL_PATH))
    print(
        f"saved model={best['model_type']} valid_ic={best['valid_ic']:.4f} "
        f"cv_mean={cv_mean_ic:.4f} cv_std={cv_std_ic:.4f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
