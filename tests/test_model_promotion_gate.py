from __future__ import annotations

import json
from pathlib import Path

from scripts.model_promotion_gate import _comparable_history_runs, _load_latest_training_entry, _model_artifact_exists


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


def test_load_latest_training_entry_prefers_research_run_record(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    run_dir = tmp_path / "reports" / "runs" / "research_ml_training_20260311_000000_000000"
    run_dir.mkdir(parents=True)
    (run_dir / "meta.json").write_text(
        json.dumps(
            {
                "run_id": run_dir.name,
                "task_name": "ml_training",
                "status": "completed",
                "started_at": "2026-03-11T00:00:00Z",
                "ended_at": "2026-03-11T00:00:01Z",
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "analysis").mkdir()
    legacy_entry = {"run_id": "research_ml_training_test", "valid_ic": 0.12}
    (run_dir / "analysis" / "model_training_record.json").write_text(
        json.dumps({"legacy_history_entry": legacy_entry}),
        encoding="utf-8",
    )

    assert _load_latest_training_entry() == legacy_entry
