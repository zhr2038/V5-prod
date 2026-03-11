from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from src.execution.ml_feature_optimizer import optimize_features_for_training
from src.research.dataset_builder import DatasetBuildConfig, ResearchDatasetBuilder
from src.research.processors import (
    align_cycle_samples,
    apply_rolling_window,
    build_recency_sample_weights,
    safe_json_float,
    summarize_numeric_series,
)
from src.research.recorder import ResearchRecorder


def load_task_config(path: str | Path) -> dict[str, Any]:
    target = Path(path)
    if not target.exists():
        return {}
    try:
        obj = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _project_path(project_root: Path, raw_path: str, fallback: str) -> Path:
    value = str(raw_path or fallback)
    target = Path(value)
    if target.is_absolute():
        return target
    return (project_root / value).resolve()


def _load_history(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, list) else []
    except Exception:
        return []


def _append_history(path: Path, entry: dict[str, Any]) -> None:
    history = _load_history(path)
    history.append(entry)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_group_series(prep_meta: dict[str, Any], expected_len: int) -> pd.Series:
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


def _trim_prep_meta(prep_meta: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in prep_meta.items() if k != "timestamps"}


def _candidate_model_config(base_cfg: MLFactorConfig, model_type: str, task_config: dict[str, Any]) -> MLFactorConfig:
    from src.execution.ml_factor_model import MLFactorConfig

    model_cfg = task_config.get("model") or {}
    params = dict(base_cfg.__dict__)
    params["model_type"] = str(model_type)

    if model_type == "ridge":
        params["alpha"] = float(model_cfg.get("ridge_alpha", base_cfg.alpha))
    elif model_type == "hist_gbm":
        params["max_depth"] = int(model_cfg.get("hist_gbm_max_depth", base_cfg.max_depth))
        params["learning_rate"] = float(model_cfg.get("hist_gbm_learning_rate", base_cfg.learning_rate))
        params["hgb_max_iter"] = int(model_cfg.get("hist_gbm_max_iter", base_cfg.hgb_max_iter))
        params["hgb_min_samples_leaf"] = int(model_cfg.get("hist_gbm_min_samples_leaf", base_cfg.hgb_min_samples_leaf))
    elif model_type == "lightgbm":
        params["n_estimators"] = int(model_cfg.get("lightgbm_n_estimators", base_cfg.n_estimators))
        params["max_depth"] = int(model_cfg.get("lightgbm_max_depth", base_cfg.max_depth))
        params["learning_rate"] = float(model_cfg.get("lightgbm_learning_rate", base_cfg.learning_rate))
        params["subsample"] = float(model_cfg.get("lightgbm_subsample", base_cfg.subsample))
        params["colsample_bytree"] = float(model_cfg.get("lightgbm_colsample_bytree", base_cfg.colsample_bytree))
        params["num_leaves"] = int(model_cfg.get("lightgbm_num_leaves", base_cfg.num_leaves))
        params["min_data_in_leaf"] = int(model_cfg.get("lightgbm_min_data_in_leaf", base_cfg.min_data_in_leaf))
        params["min_child_samples"] = int(model_cfg.get("lightgbm_min_child_samples", base_cfg.min_child_samples))
        params["reg_alpha"] = float(model_cfg.get("lightgbm_reg_alpha", base_cfg.reg_alpha))
        params["reg_lambda"] = float(model_cfg.get("lightgbm_reg_lambda", base_cfg.reg_lambda))

    return MLFactorConfig(**params)


def _train_candidate_model(
    cfg: MLFactorConfig,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_valid: pd.DataFrame,
    y_valid: pd.Series,
    sample_weight: pd.Series | None = None,
) -> MLFactorModel:
    from src.execution.ml_factor_model import MLFactorModel

    model = MLFactorModel(cfg)
    model.feature_names = list(X_train.columns)
    model.train(X_train, y_train, X_valid, y_valid, sample_weight=sample_weight)
    return model


def _candidate_cv_result(
    cfg: MLFactorConfig,
    X: pd.DataFrame,
    y: pd.Series,
    groups: pd.Series,
    cv: GroupedTimeSeriesSplit,
    *,
    recency_half_life_days: float,
    recency_max_weight: float,
) -> dict[str, Any]:
    from src.execution.ml_time_series_cv import cross_sectional_ic

    scores: list[float] = []
    for train_idx, test_idx in cv.split(X, y, groups=groups):
        X_train = X.iloc[train_idx].reset_index(drop=True)
        y_train = y.iloc[train_idx].reset_index(drop=True)
        X_test = X.iloc[test_idx].reset_index(drop=True)
        y_test = y.iloc[test_idx].reset_index(drop=True)
        train_groups = groups.iloc[train_idx].reset_index(drop=True)
        test_groups = groups.iloc[test_idx].reset_index(drop=True)
        sample_weight = build_recency_sample_weights(
            train_groups,
            half_life_days=recency_half_life_days,
            max_weight=recency_max_weight,
        )
        model = _train_candidate_model(cfg, X_train, y_train, X_test, y_test, sample_weight=sample_weight)
        pred = model.predict_batch(X_test)
        scores.append(float(cross_sectional_ic(test_groups, y_test, pred)))
    return {
        "scores": scores,
        "mean_score": float(pd.Series(scores).mean()) if scores else 0.0,
        "std_score": float(pd.Series(scores).std(ddof=0)) if scores else 0.0,
    }


def _build_model_training_record(
    *,
    run_id: str,
    history_entry: dict[str, Any],
    dataset_meta: dict[str, Any],
    best_candidate: dict[str, Any],
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "timestamp": history_entry.get("timestamp"),
        "status": "completed",
        "selected_model_type": history_entry.get("selected_model_type"),
        "metrics": {
            "train_ic": history_entry.get("train_ic"),
            "valid_ic": history_entry.get("valid_ic"),
            "cv_mean_ic": history_entry.get("cv_mean_ic"),
            "cv_std_ic": history_entry.get("cv_std_ic"),
        },
        "gate": history_entry.get("gate") or {},
        "dataset": dataset_meta,
        "feature_selection": {
            "mode": history_entry.get("feature_mode"),
            "selected_features": history_entry.get("selected_features") or [],
            "selector_reason": history_entry.get("selector_reason"),
        },
        "best_candidate": {
            "model_type": best_candidate.get("model_type"),
            "config": best_candidate.get("config") or {},
        },
        "legacy_history_entry": history_entry,
    }


def _build_signal_analysis_record(
    *,
    run_id: str,
    y_valid: pd.Series,
    valid_pred: pd.Series,
    valid_groups: pd.Series,
    cv_scores: list[float],
) -> dict[str, Any]:
    from src.execution.ml_time_series_cv import cross_sectional_ic

    pred_series = pd.Series(valid_pred).reset_index(drop=True)
    target_series = pd.Series(y_valid).reset_index(drop=True)
    return {
        "run_id": run_id,
        "timestamp": datetime.now().isoformat(),
        "status": "completed",
        "validation": {
            "rows": int(len(target_series)),
            "group_count": int(pd.Series(valid_groups).nunique()),
            "target": summarize_numeric_series(target_series),
            "prediction": summarize_numeric_series(pred_series),
            "grouped_ic": float(cross_sectional_ic(valid_groups, target_series, pred_series)),
        },
        "cross_validation": {
            "fold_scores": [float(x) for x in cv_scores],
            "summary": summarize_numeric_series(pd.Series(cv_scores, dtype=float)),
        },
    }


def run_ml_training_task(
    *,
    project_root: str | Path,
    task_config: dict[str, Any],
) -> dict[str, Any]:
    from src.execution.ml_data_collector import MLDataCollector
    from src.execution.ml_factor_model import MLFactorConfig, MLFactorModel
    from src.execution.ml_time_series_cv import GroupedTimeSeriesSplit, cross_sectional_ic

    project_root = Path(project_root)
    task_meta = task_config.get("task") or {}
    paths_cfg = task_config.get("paths") or {}
    dataset_cfg = task_config.get("dataset") or {}
    model_cfg = task_config.get("model") or {}
    gate_cfg = task_config.get("gate") or {}
    recency_cfg = task_config.get("recency_weighting") or {}

    recorder = ResearchRecorder(base_dir=_project_path(project_root, paths_cfg.get("runs_dir", "reports/runs"), "reports/runs"))
    run = recorder.start_run(task_name=str(task_meta.get("name", "ml_training")), task_config=task_config)

    history_path = _project_path(project_root, paths_cfg.get("history_path", "reports/ml_training_history.json"), "reports/ml_training_history.json")
    csv_path = _project_path(project_root, paths_cfg.get("csv_path", "reports/ml_training_data.csv"), "reports/ml_training_data.csv")
    db_path = _project_path(project_root, paths_cfg.get("db_path", "reports/ml_training_data.db"), "reports/ml_training_data.db")
    model_path = _project_path(project_root, paths_cfg.get("model_path", "models/ml_factor_model"), "models/ml_factor_model")

    min_samples = int(dataset_cfg.get("min_samples", 200))
    target_col = str(dataset_cfg.get("target_col", "future_return_6h"))
    feature_groups = tuple(str(x) for x in (dataset_cfg.get("feature_groups") or ["classic"]))
    include_time_features = bool(dataset_cfg.get("include_time_features", False))
    feature_selector = str(dataset_cfg.get("feature_selector", "stable"))
    selected_feature_count = int(dataset_cfg.get("selected_feature_count", 12))
    rolling_window_days = float(dataset_cfg.get("rolling_window_days", 60))
    recency_half_life_days = float(recency_cfg.get("half_life_days", 5))
    recency_max_weight = float(recency_cfg.get("max_weight", 3.0))

    min_valid_ic = float(gate_cfg.get("min_valid_ic", 0.0))
    max_ic_gap = float(gate_cfg.get("max_ic_gap", 0.25))
    min_cv_mean_ic = float(gate_cfg.get("min_cv_mean_ic", 0.01))
    max_cv_std = float(gate_cfg.get("max_cv_std", 0.15))

    collector = MLDataCollector(db_path=str(db_path))
    csv_fallback_rows = 0
    if csv_path.exists():
        try:
            csv_fallback_rows = int(len(pd.read_csv(csv_path)))
        except Exception:
            csv_fallback_rows = 0

    stats = collector.get_statistics()
    labeled_records = int(stats.get("labeled_records", 0))
    if labeled_records >= min_samples:
        exported = collector.export_training_data(str(csv_path), min_samples=min_samples)
        if not exported and csv_fallback_rows < min_samples:
            recorder.finalize_run(run, status="failed", summary={"reason": "export_training_data_failed"})
            return {"exit_code": 1, "run_id": run.run_id}
    elif csv_fallback_rows < min_samples:
        recorder.finalize_run(
            run,
            status="blocked",
            summary={
                "reason": "insufficient_labeled_records",
                "labeled_records": labeled_records,
                "csv_rows": csv_fallback_rows,
                "min_samples": min_samples,
            },
        )
        return {"exit_code": 0, "run_id": run.run_id}

    raw_df = pd.read_csv(csv_path)
    cycle_meta = {"rows_before": int(len(raw_df)), "rows_after": int(len(raw_df)), "duplicates_removed": 0}
    if bool(dataset_cfg.get("align_cycles", True)):
        raw_df, cycle_meta = align_cycle_samples(raw_df)

    builder = ResearchDatasetBuilder(
        DatasetBuildConfig(
            feature_groups=feature_groups,
            include_time_features=include_time_features,
            target_mode=str(dataset_cfg.get("target_mode", "forward_edge_rank")),
            min_symbol_samples=int(dataset_cfg.get("min_symbol_samples", 48)),
            min_symbol_target_std=float(dataset_cfg.get("min_symbol_target_std", 1e-6)),
            min_cross_sectional_group_size=int(dataset_cfg.get("min_group_size", 2)),
            min_group_coverage_ratio=float(dataset_cfg.get("min_group_coverage_ratio", 0.9)),
        )
    )

    X_base, y, prep_meta = builder.build_training_frame(raw_df, target_col=target_col)
    X = optimize_features_for_training(X_base, y, selector=feature_selector, n_features=selected_feature_count)
    if X.empty or len(X) < min_samples:
        recorder.finalize_run(
            run,
            status="failed",
            summary={"reason": "training_frame_too_small", "rows": int(len(X))},
        )
        return {"exit_code": 1, "run_id": run.run_id}

    groups = _build_group_series(prep_meta, len(X))
    X, y, groups, window_meta = apply_rolling_window(X, y, groups, lookback_days=rolling_window_days)
    if X.empty or len(X) < min_samples:
        recorder.finalize_run(
            run,
            status="failed",
            summary={"reason": "rolling_window_too_small", "rows": int(len(X))},
        )
        return {"exit_code": 1, "run_id": run.run_id}

    unique_groups = pd.Index(groups.drop_duplicates().tolist())
    prediction_horizon = int(dataset_cfg.get("prediction_horizon", 6))
    purge_gap = prediction_horizon
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

    candidates = [str(x).strip().lower() for x in (model_cfg.get("candidates") or ["ridge"]) if str(x).strip()]
    base_factor_cfg = MLFactorConfig(
        model_type=candidates[0] if candidates else "ridge",
        target_mode=str(dataset_cfg.get("target_mode", "forward_edge_rank")),
        include_time_features=include_time_features,
        min_symbol_samples=int(dataset_cfg.get("min_symbol_samples", 48)),
        min_symbol_target_std=float(dataset_cfg.get("min_symbol_target_std", 1e-6)),
        min_cross_sectional_group_size=int(dataset_cfg.get("min_group_size", 2)),
        min_group_coverage_ratio=float(dataset_cfg.get("min_group_coverage_ratio", 0.9)),
        prediction_horizon=prediction_horizon,
        train_lookback_days=int(dataset_cfg.get("rolling_window_days", 60)),
        alpha=float(model_cfg.get("ridge_alpha", 50.0)),
    )
    base_factor_cfg.feature_groups = feature_groups

    candidate_results: list[dict[str, Any]] = []
    for model_type in candidates:
        candidate_cfg = _candidate_model_config(base_factor_cfg, model_type, task_config)
        candidate_cfg.feature_groups = feature_groups
        try:
            train_sample_weight = build_recency_sample_weights(
                train_groups,
                half_life_days=recency_half_life_days,
                max_weight=recency_max_weight,
            )
            model = _train_candidate_model(
                candidate_cfg,
                X_train,
                y_train,
                X_valid,
                y_valid,
                sample_weight=train_sample_weight,
            )
            train_pred = model.predict_batch(X_train)
            valid_pred = model.predict_batch(X_valid)
            cv_res = _candidate_cv_result(
                candidate_cfg,
                X,
                y,
                groups,
                cv,
                recency_half_life_days=recency_half_life_days,
                recency_max_weight=recency_max_weight,
            )
            candidate_results.append(
                {
                    "model_type": model_type,
                    "config": candidate_cfg.__dict__,
                    "model": model,
                    "train_ic": float(cross_sectional_ic(train_groups, y_train, train_pred)),
                    "valid_ic": float(cross_sectional_ic(valid_groups, y_valid, valid_pred)),
                    "cv_mean_ic": float(cv_res["mean_score"]),
                    "cv_std_ic": float(cv_res["std_score"]),
                    "cv_scores": [float(x) for x in cv_res["scores"]],
                    "train_weight_mean": float(train_sample_weight.mean()),
                    "train_weight_max": float(train_sample_weight.max()),
                    "valid_pred": valid_pred.reset_index(drop=True),
                }
            )
        except Exception as exc:
            candidate_results.append(
                {
                    "model_type": model_type,
                    "error": str(exc),
                    "train_ic": -1.0,
                    "valid_ic": -1.0,
                    "cv_mean_ic": -1.0,
                    "cv_std_ic": 999.0,
                    "cv_scores": [],
                }
            )

    successful = [item for item in candidate_results if "model" in item]
    if not successful:
        recorder.write_json("analysis/model_training_record.json", {"run_id": run.run_id, "status": "failed", "reason": "no_candidate_model_trained"})
        recorder.finalize_run(run, status="failed", summary={"reason": "no_candidate_model_trained"})
        return {"exit_code": 1, "run_id": run.run_id}

    successful.sort(
        key=lambda item: (
            safe_json_float(item.get("valid_ic")),
            safe_json_float(item.get("cv_mean_ic")),
            -(safe_json_float(item.get("train_ic")) - safe_json_float(item.get("valid_ic"))),
        ),
        reverse=True,
    )
    best = successful[0]
    ic_gap = float(safe_json_float(best.get("train_ic")) - safe_json_float(best.get("valid_ic")))
    cv_mean_ic = float(safe_json_float(best.get("cv_mean_ic")))
    cv_std_ic = float(safe_json_float(best.get("cv_std_ic")))

    fail_reasons: list[str] = []
    if float(best["valid_ic"]) < min_valid_ic:
        fail_reasons.append(f"valid_ic<{min_valid_ic:.2f}")
    if ic_gap > max_ic_gap:
        fail_reasons.append(f"ic_gap>{max_ic_gap:.2f}")
    if cv_mean_ic < min_cv_mean_ic:
        fail_reasons.append(f"cv_mean_ic<{min_cv_mean_ic:.2f}")
    if cv_std_ic > max_cv_std:
        fail_reasons.append(f"cv_std_ic>{max_cv_std:.2f}")

    gate_passed = len(fail_reasons) == 0

    dataset_meta = {
        "source_csv": str(csv_path),
        "db_path": str(db_path),
        "samples": int(len(X)),
        "labeled_records": labeled_records,
        "csv_fallback_rows": csv_fallback_rows,
        "grouped_holdout": {
            "unique_groups": int(len(unique_groups)),
            "train_groups": int(train_groups.nunique()),
            "valid_groups": int(valid_groups.nunique()),
            "purge_gap_groups": purge_gap,
        },
        "prep": _trim_prep_meta(prep_meta),
        "rolling_window": window_meta,
        "cycle_alignment": cycle_meta,
        "feature_groups": list(feature_groups),
    }
    history_entry = {
        "run_id": run.run_id,
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
        "grouped_holdout": dataset_meta["grouped_holdout"],
        "rolling_window": window_meta,
        "cycle_alignment": cycle_meta,
        "recency_weighting": {
            "half_life_days": recency_half_life_days,
            "max_weight": recency_max_weight,
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
                "train_weight_mean": float(item.get("train_weight_mean", 0.0)),
                "train_weight_max": float(item.get("train_weight_max", 0.0)),
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
            "target_mode": builder.config.target_mode,
            "include_time_features": include_time_features,
            "feature_selector": feature_selector,
            "rolling_window_days": rolling_window_days,
            "recency_half_life_days": recency_half_life_days,
            "recency_max_weight": recency_max_weight,
            "feature_groups": list(feature_groups),
        },
    }

    artifact_base = run.artifact_base("ml_factor_model")
    best["model"].save_model(str(artifact_base))
    run.write_text("artifacts/selected_model_path.txt", str(artifact_base))
    run.write_json("dataset_meta.json", dataset_meta)
    run.write_json("metrics.json", history_entry)
    run.write_json(
        "analysis/model_training_record.json",
        _build_model_training_record(
            run_id=run.run_id,
            history_entry=history_entry,
            dataset_meta=dataset_meta,
            best_candidate=best,
        ),
    )
    run.write_json(
        "analysis/signal_analysis_record.json",
        _build_signal_analysis_record(
            run_id=run.run_id,
            y_valid=y_valid,
            valid_pred=best["valid_pred"],
            valid_groups=valid_groups,
            cv_scores=best["cv_scores"],
        ),
    )
    run.write_json(
        "analysis/portfolio_analysis_record.json",
        {
            "run_id": run.run_id,
            "status": "not_run",
            "reason": "walk_forward_task_not_executed",
        },
    )

    _append_history(history_path, history_entry)

    if gate_passed:
        model_path.parent.mkdir(parents=True, exist_ok=True)
        best["model"].save_model(str(model_path))

    recorder.finalize_run(
        run,
        status="completed" if gate_passed else "blocked",
        summary={
            "selected_model_type": best["model_type"],
            "valid_ic": float(best["valid_ic"]),
            "cv_mean_ic": cv_mean_ic,
            "cv_std_ic": cv_std_ic,
            "gate_passed": gate_passed,
        },
    )

    return {
        "exit_code": 0,
        "run_id": run.run_id,
        "gate_passed": gate_passed,
        "fail_reasons": fail_reasons,
        "selected_model_type": best["model_type"],
        "valid_ic": float(best["valid_ic"]),
        "cv_mean_ic": cv_mean_ic,
        "cv_std_ic": cv_std_ic,
    }


def run_walk_forward_task(
    *,
    project_root: str | Path,
    task_config: dict[str, Any],
) -> dict[str, Any]:
    from configs.loader import load_config
    from src.backtest.walk_forward import (
        build_portfolio_analysis_record,
        build_walk_forward_report,
        run_walk_forward,
    )
    from src.data.mock_provider import MockProvider
    from src.data.okx_ccxt_provider import OKXCCXTProvider

    project_root = Path(project_root)
    task_meta = task_config.get("task") or {}
    paths_cfg = task_config.get("paths") or {}
    walk_cfg = task_config.get("walk_forward") or {}

    recorder = ResearchRecorder(base_dir=_project_path(project_root, paths_cfg.get("runs_dir", "reports/runs"), "reports/runs"))
    run = recorder.start_run(task_name=str(task_meta.get("name", "walk_forward")), task_config=task_config)

    app_cfg_path = _project_path(project_root, walk_cfg.get("config_path", "configs/config.yaml"), "configs/config.yaml")
    env_path = str(walk_cfg.get("env_path", ".env"))
    provider_name = str(walk_cfg.get("provider", "mock")).strip().lower()
    ohlcv_limit = int(walk_cfg.get("ohlcv_limit", 24 * 120))
    output_report_path = _project_path(project_root, paths_cfg.get("output_report_path", "reports/walk_forward.json"), "reports/walk_forward.json")

    cfg = load_config(str(app_cfg_path), env_path=env_path)
    provider = OKXCCXTProvider() if provider_name == "okx" else MockProvider(seed=int(walk_cfg.get("mock_seed", 7)))

    market_data = provider.fetch_ohlcv(cfg.symbols, timeframe=cfg.timeframe_main, limit=ohlcv_limit)
    folds = run_walk_forward(
        market_data,
        folds=int(walk_cfg.get("folds", getattr(cfg.backtest, "walk_forward_folds", 4))),
        cfg=cfg,
    )
    report = build_walk_forward_report(
        folds,
        cost_meta={
            "mode": str(cfg.backtest.cost_model),
            "fee_quantile": str(cfg.backtest.fee_quantile),
            "slippage_quantile": str(cfg.backtest.slippage_quantile),
            "min_fills_global": int(cfg.backtest.min_fills_global),
            "min_fills_bucket": int(cfg.backtest.min_fills_bucket),
            "max_stats_age_days": int(cfg.backtest.max_stats_age_days),
            "cost_stats_dir": str(cfg.backtest.cost_stats_dir),
        },
    )
    output_report_path.parent.mkdir(parents=True, exist_ok=True)
    output_report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    summary = build_portfolio_analysis_record(report)
    run.write_json("metrics.json", summary)
    run.write_json("report.json", report)
    run.write_json("analysis/portfolio_analysis_record.json", summary)
    recorder.finalize_run(run, status="completed", summary=summary)
    return {"exit_code": 0, "run_id": run.run_id, "folds": len(report.get("folds") or [])}
