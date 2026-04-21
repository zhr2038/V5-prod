from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.model_promotion_gate as promotion_gate


def test_build_paths_uses_runtime_order_store(monkeypatch, tmp_path: Path) -> None:
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "execution:\n  order_store_path: reports/shadow_runtime/orders.sqlite\nalpha:\n  ml_factor: {}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        promotion_gate,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(config_path),
    )
    monkeypatch.setattr(
        promotion_gate,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    paths = promotion_gate.build_paths(tmp_path)

    assert paths.history_path == (tmp_path / "reports" / "shadow_runtime" / "ml_training_history.json").resolve()
    assert paths.runs_dir == (tmp_path / "reports" / "shadow_runtime" / "runs").resolve()


def test_build_paths_fails_fast_when_runtime_config_is_empty(monkeypatch, tmp_path: Path) -> None:
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        promotion_gate,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(config_path),
    )

    with pytest.raises(ValueError, match="live_prod.yaml"):
        promotion_gate.build_paths(tmp_path)


def test_build_paths_fails_fast_when_runtime_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    monkeypatch.setattr(
        promotion_gate,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(config_path),
    )

    with pytest.raises(FileNotFoundError, match="runtime config not found"):
        promotion_gate.build_paths(tmp_path)


def test_main_prefers_latest_history_entry_by_timestamp_when_history_is_unsorted(monkeypatch, tmp_path: Path) -> None:
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "execution:\n  order_store_path: reports/orders.sqlite\nalpha:\n  ml_factor: {}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        promotion_gate,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(config_path),
    )
    monkeypatch.setattr(
        promotion_gate,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )
    monkeypatch.setattr(promotion_gate, "_load_latest_training_entry", lambda workspace=None: None)

    history_path = (tmp_path / "reports" / "ml_training_history.json").resolve()
    history_path.parent.mkdir(parents=True, exist_ok=True)
    history_path.write_text(
        json.dumps(
            [
                {
                    "run_id": "20260420_10",
                    "timestamp": "2026-04-20T10:00:00Z",
                    "valid_ic": 0.12,
                    "cv_mean_ic": 0.12,
                    "cv_std_ic": 0.05,
                    "train_ic": 0.18,
                    "grouped_holdout": True,
                    "config": {"model_type": "xgboost", "target_mode": "raw", "include_time_features": False},
                    "gate": {"min_valid_ic": 0.05, "min_cv_mean_ic": 0.01, "max_cv_std": 0.15, "max_ic_gap": 0.25},
                },
                {
                    "run_id": "20260420_09",
                    "timestamp": "2026-04-20T09:00:00Z",
                    "valid_ic": 0.01,
                    "cv_mean_ic": 0.12,
                    "cv_std_ic": 0.05,
                    "train_ic": 0.18,
                    "grouped_holdout": True,
                    "config": {"model_type": "xgboost", "target_mode": "raw", "include_time_features": False},
                    "gate": {"min_valid_ic": 0.05, "min_cv_mean_ic": 0.01, "max_cv_std": 0.15, "max_ic_gap": 0.25},
                },
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    (tmp_path / "models").mkdir(parents=True, exist_ok=True)
    (tmp_path / "models" / "ml_factor_model.pkl").write_bytes(b"model")

    exit_code = promotion_gate.main(tmp_path)

    decision = json.loads((tmp_path / "reports" / "model_promotion_decision.json").read_text(encoding="utf-8"))
    assert exit_code == 0
    assert decision["passed"] is True
    assert decision["metrics"]["valid_ic"] == 0.12
    assert (tmp_path / "models" / "ml_factor_model_active.txt").read_text(encoding="utf-8") == str(
        (tmp_path / "models" / "ml_factor_model").resolve()
    )


def test_main_deduplicates_latest_run_entry_before_recent_mean(monkeypatch, tmp_path: Path) -> None:
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "execution:\n  order_store_path: reports/orders.sqlite\nalpha:\n  ml_factor: {}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        promotion_gate,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(config_path),
    )
    monkeypatch.setattr(
        promotion_gate,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    latest_entry = {
        "run_id": "20260420_11",
        "timestamp": "2026-04-20T11:00:00Z",
        "valid_ic": 0.20,
        "cv_mean_ic": 0.12,
        "cv_std_ic": 0.05,
        "train_ic": 0.22,
        "grouped_holdout": True,
        "config": {"model_type": "xgboost", "target_mode": "raw", "include_time_features": False},
        "gate": {"min_valid_ic": 0.05, "min_cv_mean_ic": 0.01, "max_cv_std": 0.15, "max_ic_gap": 0.25},
    }
    monkeypatch.setattr(promotion_gate, "_load_latest_training_entry", lambda workspace=None: latest_entry)

    history_path = (tmp_path / "reports" / "ml_training_history.json").resolve()
    history_path.parent.mkdir(parents=True, exist_ok=True)
    history_path.write_text(
        json.dumps(
            [
                {
                    "run_id": "20260420_10",
                    "timestamp": "2026-04-20T10:00:00Z",
                    "valid_ic": 0.10,
                    "cv_mean_ic": 0.12,
                    "cv_std_ic": 0.05,
                    "train_ic": 0.16,
                    "grouped_holdout": True,
                    "config": {"model_type": "xgboost", "target_mode": "raw", "include_time_features": False},
                    "gate": {"min_valid_ic": 0.05, "min_cv_mean_ic": 0.01, "max_cv_std": 0.15, "max_ic_gap": 0.25},
                },
                {
                    "run_id": "20260420_11",
                    "timestamp": "2026-04-20T11:00:00Z",
                    "valid_ic": -0.40,
                    "cv_mean_ic": 0.12,
                    "cv_std_ic": 0.05,
                    "train_ic": -0.30,
                    "grouped_holdout": True,
                    "config": {"model_type": "xgboost", "target_mode": "raw", "include_time_features": False},
                    "gate": {"min_valid_ic": 0.05, "min_cv_mean_ic": 0.01, "max_cv_std": 0.15, "max_ic_gap": 0.25},
                },
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    (tmp_path / "models").mkdir(parents=True, exist_ok=True)
    (tmp_path / "models" / "ml_factor_model.pkl").write_bytes(b"model")

    exit_code = promotion_gate.main(tmp_path)

    decision = json.loads((tmp_path / "reports" / "model_promotion_decision.json").read_text(encoding="utf-8"))
    assert exit_code == 0
    assert decision["passed"] is True
    assert decision["metrics"]["valid_ic"] == 0.20
    assert decision["metrics"]["recent_mean_valid_ic"] == pytest.approx(0.15)
