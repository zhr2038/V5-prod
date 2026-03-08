from __future__ import annotations

from pathlib import Path

from scripts.model_promotion_gate import _comparable_history_runs, _model_artifact_exists


def test_model_artifact_exists_detects_pickle_artifact(tmp_path: Path) -> None:
    base = tmp_path / "ml_factor_model"
    base.with_suffix(".pkl").write_bytes(b"test")
    (tmp_path / "ml_factor_model_config.json").write_text("{}", encoding="utf-8")

    assert _model_artifact_exists(base)


def test_comparable_history_runs_filters_old_metric_regimes() -> None:
    latest = {
        "selected_model_type": "hist_gbm",
        "grouped_holdout": {"unique_groups": 10},
        "config": {
            "model_type": "hist_gbm",
            "target_mode": "raw",
            "include_time_features": False,
        },
    }
    hist = [
        {
            "selected_model_type": "ridge",
            "grouped_holdout": {"unique_groups": 10},
            "config": {"model_type": "ridge", "target_mode": "raw", "include_time_features": False},
        },
        {
            "selected_model_type": "hist_gbm",
            "config": {"model_type": "hist_gbm", "target_mode": "raw", "include_time_features": False},
        },
        latest,
    ]

    out = _comparable_history_runs(hist, latest)

    assert out == [latest]
