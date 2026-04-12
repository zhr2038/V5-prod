from __future__ import annotations

from pathlib import Path

import pytest

import src.research.window_diagnostics as wd


def test_run_window_diagnostic_task_finalizes_failed_run_when_no_evaluations(monkeypatch, tmp_path: Path) -> None:
    finalized: dict[str, object] = {}

    class FakeRun:
        def __init__(self):
            self.run_dir = tmp_path / "run"
            self.run_dir.mkdir(parents=True, exist_ok=True)

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
            finalized["finalize_calls"] = int(finalized.get("finalize_calls", 0)) + 1
            finalized["status"] = status
            finalized["summary"] = summary
            return run.run_dir / "meta.json"

    monkeypatch.setattr(
        wd,
        "load_task_config",
        lambda path: {
            "task": {"name": "window_diagnostics"},
            "paths": {},
            "experiment": {"symbols": ["BTC/USDT"], "evaluations": []},
        },
    )
    monkeypatch.setattr(wd, "ResearchRecorder", FakeRecorder)

    with pytest.raises(ValueError, match="requires at least one evaluation"):
        wd.run_window_diagnostic_task(project_root=tmp_path, task_config_path="task.yaml")

    assert finalized["task_name"] == "window_diagnostics"
    assert finalized["finalize_calls"] == 1
    assert finalized["status"] == "failed"
    assert finalized["summary"] == {
        "reason": "window_diagnostics_failed",
        "error_type": "ValueError",
        "error": "window diagnostics requires at least one evaluation",
    }


def test_run_window_diagnostic_task_finalizes_failed_run_when_job_raises(monkeypatch, tmp_path: Path) -> None:
    finalized: dict[str, object] = {}

    class FakeRun:
        def __init__(self):
            self.run_dir = tmp_path / "run"
            self.run_dir.mkdir(parents=True, exist_ok=True)

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
            finalized["finalize_calls"] = int(finalized.get("finalize_calls", 0)) + 1
            finalized["status"] = status
            finalized["summary"] = summary
            return run.run_dir / "meta.json"

    monkeypatch.setattr(
        wd,
        "load_task_config",
        lambda path: {
            "task": {"name": "window_diagnostics"},
            "paths": {},
            "experiment": {
                "symbols": ["BTC/USDT"],
                "evaluations": [{"name": "window_1", "ohlcv_limit": 24, "window_shift_bars": 0}],
                "workers": 1,
            },
        },
    )
    monkeypatch.setattr(wd, "ResearchRecorder", FakeRecorder)
    monkeypatch.setattr(wd, "_run_window_job", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("window task failed")))

    with pytest.raises(RuntimeError, match="window task failed"):
        wd.run_window_diagnostic_task(project_root=tmp_path, task_config_path="task.yaml")

    assert finalized["task_name"] == "window_diagnostics"
    assert finalized["finalize_calls"] == 1
    assert finalized["status"] == "failed"
    assert finalized["summary"] == {
        "reason": "window_diagnostics_failed",
        "error_type": "RuntimeError",
        "error": "window task failed",
    }
