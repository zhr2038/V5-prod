from __future__ import annotations

from pathlib import Path
from types import ModuleType
import sys

import pytest

from src.research.task_runner import run_ml_training_task


def test_run_ml_training_task_finalizes_failed_run(monkeypatch, tmp_path: Path) -> None:
    finalized: dict[str, object] = {}

    class FakeRun:
        def __init__(self):
            self.run_dir = tmp_path / "run"
            self.run_dir.mkdir(parents=True, exist_ok=True)
            self.run_id = "run_123"

        def write_json(self, relative_path: str, payload):
            finalized.setdefault("writes", []).append((relative_path, payload))
            return self.run_dir / relative_path

    class FakeRecorder:
        def __init__(self, *args, **kwargs):
            pass

        def start_run(self, **kwargs):
            finalized["task_name"] = kwargs["task_name"]
            return FakeRun()

        def finalize_run(self, run, *, status: str, summary):
            finalized["status"] = status
            finalized["summary"] = summary
            return run.run_dir / "meta.json"

    class BrokenCollector:
        def __init__(self, *args, **kwargs):
            pass

        def get_statistics(self):
            raise RuntimeError("collector stats failed")

    fake_collector_module = ModuleType("src.execution.ml_data_collector")
    fake_collector_module.MLDataCollector = BrokenCollector
    fake_model_module = ModuleType("src.execution.ml_factor_model")
    fake_model_module.MLFactorConfig = object
    fake_model_module.MLFactorModel = object
    fake_cv_module = ModuleType("src.execution.ml_time_series_cv")
    fake_cv_module.GroupedTimeSeriesSplit = object
    fake_cv_module.cross_sectional_ic = lambda *args, **kwargs: 0.0

    monkeypatch.setattr("src.research.task_runner.ResearchRecorder", FakeRecorder)
    monkeypatch.setitem(sys.modules, "src.execution.ml_data_collector", fake_collector_module)
    monkeypatch.setitem(sys.modules, "src.execution.ml_factor_model", fake_model_module)
    monkeypatch.setitem(sys.modules, "src.execution.ml_time_series_cv", fake_cv_module)

    with pytest.raises(RuntimeError, match="collector stats failed"):
        run_ml_training_task(
            project_root=tmp_path,
            task_config={
                "task": {"name": "ml_training"},
                "paths": {},
                "dataset": {},
                "model": {},
                "gate": {},
                "recency_weighting": {},
                "parallel": {},
            },
        )

    assert finalized["task_name"] == "ml_training"
    assert finalized["status"] == "failed"
    assert finalized["summary"] == {
        "reason": "ml_training_failed",
        "error_type": "RuntimeError",
        "error": "collector stats failed",
    }
