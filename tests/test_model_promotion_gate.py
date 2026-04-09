from __future__ import annotations

import json
from pathlib import Path

import scripts.model_promotion_gate as model_promotion_gate


def test_model_artifact_exists_detects_pickle_artifact(tmp_path: Path) -> None:
    base = tmp_path / "ml_factor_model"
    base.with_suffix(".pkl").write_bytes(b"test")
    (tmp_path / "ml_factor_model_config.json").write_text("{}", encoding="utf-8")

    assert model_promotion_gate._model_artifact_exists(base)


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

    out = model_promotion_gate._comparable_history_runs(hist, latest)

    assert out == [latest]


def test_build_paths_anchor_model_promotion_gate_to_workspace(tmp_path: Path) -> None:
    paths = model_promotion_gate.build_paths(tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.history_path == (tmp_path / "reports" / "ml_training_history.json")
    assert paths.decision_path == (tmp_path / "reports" / "model_promotion_decision.json")
    assert paths.active_pointer_path == (tmp_path / "models" / "ml_factor_model_active.txt")
    assert paths.model_path == (tmp_path / "models" / "ml_factor_model")
    assert paths.runs_dir == (tmp_path / "reports" / "runs")


def test_build_paths_follow_runtime_and_ml_config_paths(tmp_path: Path) -> None:
    config_path = tmp_path / "configs" / "shadow.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "\n".join(
            [
                "alpha:",
                "  ml_factor:",
                "    model_path: models/ml_factor_model_gpu_tuned",
                "    active_model_pointer_path: reports/shadow_runtime/unused_ml_model_pointer.txt",
                "    promotion_decision_path: reports/shadow_runtime/unused_model_promotion_decision.json",
                "execution:",
                "  order_store_path: reports/shadow_runtime/orders.sqlite",
            ]
        ),
        encoding="utf-8",
    )

    paths = model_promotion_gate.build_paths(tmp_path, str(config_path))

    assert paths.history_path == (tmp_path / "reports" / "shadow_runtime" / "ml_training_history.json").resolve()
    assert paths.decision_path == (tmp_path / "reports" / "shadow_runtime" / "unused_model_promotion_decision.json").resolve()
    assert paths.active_pointer_path == (tmp_path / "reports" / "shadow_runtime" / "unused_ml_model_pointer.txt").resolve()
    assert paths.model_path == (tmp_path / "models" / "ml_factor_model_gpu_tuned").resolve()
    assert paths.runs_dir == (tmp_path / "reports" / "shadow_runtime" / "runs").resolve()


def test_load_latest_training_entry_prefers_research_run_record_from_workspace(tmp_path: Path, monkeypatch) -> None:
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir(parents=True)
    monkeypatch.setenv("V5_WORKSPACE", str(tmp_path))
    monkeypatch.chdir(elsewhere)

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

    assert model_promotion_gate._load_latest_training_entry() == legacy_entry


def test_load_latest_training_entry_uses_runtime_runs_from_active_config(tmp_path: Path, monkeypatch) -> None:
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir(parents=True)
    config_path = tmp_path / "configs" / "shadow.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "execution:\n  order_store_path: reports/shadow_runtime/orders.sqlite\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("V5_WORKSPACE", str(tmp_path))
    monkeypatch.setenv("V5_CONFIG", str(config_path))
    monkeypatch.chdir(elsewhere)

    run_dir = tmp_path / "reports" / "shadow_runtime" / "runs" / "research_ml_training_20260311_000000_000000"
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
    legacy_entry = {"run_id": "research_ml_training_shadow", "valid_ic": 0.18}
    (run_dir / "analysis" / "model_training_record.json").write_text(
        json.dumps({"legacy_history_entry": legacy_entry}),
        encoding="utf-8",
    )

    assert model_promotion_gate._load_latest_training_entry() == legacy_entry


def test_model_promotion_gate_main_writes_workspace_outputs_when_cwd_differs(tmp_path: Path, monkeypatch) -> None:
    workspace = tmp_path / "workspace"
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir(parents=True, exist_ok=True)
    (workspace / "reports").mkdir(parents=True, exist_ok=True)
    (workspace / "models").mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("V5_WORKSPACE", str(workspace))
    monkeypatch.chdir(elsewhere)

    (workspace / "reports" / "ml_training_history.json").write_text(
        json.dumps(
            [
                {
                    "run_id": "r1",
                    "valid_ic": 0.12,
                    "cv_mean_ic": 0.12,
                    "cv_std_ic": 0.01,
                    "train_ic": 0.15,
                    "grouped_holdout": {"unique_groups": 10},
                    "config": {
                        "model_type": "ridge",
                        "target_mode": "raw",
                        "include_time_features": False,
                    },
                }
            ]
        ),
        encoding="utf-8",
    )
    (workspace / "models" / "ml_factor_model.pkl").write_bytes(b"model")

    rc = model_promotion_gate.main()

    assert rc == 0
    decision_path = workspace / "reports" / "model_promotion_decision.json"
    pointer_path = workspace / "models" / "ml_factor_model_active.txt"
    assert decision_path.exists()
    assert pointer_path.exists()
    decision = json.loads(decision_path.read_text(encoding="utf-8"))
    assert decision["passed"] is True
    assert decision["selected_model_path"] == str((workspace / "models" / "ml_factor_model").resolve())
    assert pointer_path.read_text(encoding="utf-8") == str((workspace / "models" / "ml_factor_model").resolve())
    assert not (elsewhere / "reports" / "model_promotion_decision.json").exists()


def test_model_promotion_gate_main_uses_runtime_history_and_configured_outputs(tmp_path: Path, monkeypatch) -> None:
    workspace = tmp_path / "workspace"
    elsewhere = tmp_path / "elsewhere"
    config_path = workspace / "configs" / "shadow.yaml"
    elsewhere.mkdir(parents=True, exist_ok=True)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    (workspace / "models").mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("V5_WORKSPACE", str(workspace))
    monkeypatch.setenv("V5_CONFIG", str(config_path))
    monkeypatch.chdir(elsewhere)

    config_path.write_text(
        "\n".join(
            [
                "alpha:",
                "  ml_factor:",
                "    model_path: models/ml_factor_model_gpu_tuned",
                "    active_model_pointer_path: reports/shadow_runtime/unused_ml_model_pointer.txt",
                "    promotion_decision_path: reports/shadow_runtime/unused_model_promotion_decision.json",
                "execution:",
                "  order_store_path: reports/shadow_runtime/orders.sqlite",
            ]
        ),
        encoding="utf-8",
    )
    runtime_reports = workspace / "reports" / "shadow_runtime"
    runtime_reports.mkdir(parents=True, exist_ok=True)
    (runtime_reports / "ml_training_history.json").write_text(
        json.dumps(
            [
                {
                    "run_id": "r-shadow",
                    "valid_ic": 0.12,
                    "cv_mean_ic": 0.12,
                    "cv_std_ic": 0.01,
                    "train_ic": 0.15,
                    "grouped_holdout": {"unique_groups": 10},
                    "config": {
                        "model_type": "ridge",
                        "target_mode": "raw",
                        "include_time_features": False,
                    },
                }
            ]
        ),
        encoding="utf-8",
    )
    (workspace / "models" / "ml_factor_model_gpu_tuned.pkl").write_bytes(b"model")

    rc = model_promotion_gate.main()

    assert rc == 0
    decision_path = runtime_reports / "unused_model_promotion_decision.json"
    pointer_path = runtime_reports / "unused_ml_model_pointer.txt"
    assert decision_path.exists()
    assert pointer_path.exists()
    decision = json.loads(decision_path.read_text(encoding="utf-8"))
    assert decision["passed"] is True
    assert decision["selected_model_path"] == str((workspace / "models" / "ml_factor_model_gpu_tuned").resolve())
    assert pointer_path.read_text(encoding="utf-8") == str((workspace / "models" / "ml_factor_model_gpu_tuned").resolve())
    assert not (workspace / "reports" / "model_promotion_decision.json").exists()
