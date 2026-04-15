from __future__ import annotations

from pathlib import Path

import pytest

import scripts.run_rolling_window_sweep as sweep


def test_main_finalizes_failed_run_when_no_evaluations(monkeypatch, tmp_path: Path) -> None:
    finalized: dict[str, object] = {}

    class FakeRun:
        def __init__(self):
            self.run_dir = tmp_path / "run"
            self.run_dir.mkdir(parents=True, exist_ok=True)

        def write_json(self, relative_path: str, payload):
            finalized["error_path"] = relative_path
            finalized["error_payload"] = payload
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

    monkeypatch.setattr(
        sweep,
        "load_task_config",
        lambda path: {
            "task": {"name": "rolling_window_sweep"},
            "paths": {},
            "experiment": {
                "variants": [{"name": "v1", "symbols": ["BTC/USDT"], "overrides": {}}],
            },
        },
    )
    monkeypatch.setattr(sweep, "ResearchRecorder", FakeRecorder)
    monkeypatch.setattr(sweep, "_available_bars", lambda **kwargs: 0)
    monkeypatch.setattr(sweep, "_generated_evaluations", lambda *args, **kwargs: [])
    monkeypatch.setattr(sweep.sys, "argv", ["run_rolling_window_sweep.py"])

    assert sweep.main() == 1
    assert finalized["task_name"] == "rolling_window_sweep"
    assert finalized["status"] == "failed"
    assert finalized["summary"] == {
        "reason": "no_evaluations_generated",
        "available_bars": 0,
        "variant_count": 1,
    }


def test_main_finalizes_failed_run_when_job_raises(monkeypatch, tmp_path: Path) -> None:
    finalized: dict[str, object] = {}

    class FakeRun:
        def __init__(self):
            self.run_dir = tmp_path / "run"
            self.run_dir.mkdir(parents=True, exist_ok=True)

        def write_json(self, relative_path: str, payload):
            finalized["error_path"] = relative_path
            finalized["error_payload"] = payload
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

    monkeypatch.setattr(
        sweep,
        "load_task_config",
        lambda path: {
            "task": {"name": "rolling_window_sweep"},
            "paths": {},
            "experiment": {
                "variants": [{"name": "v1", "symbols": ["BTC/USDT"], "overrides": {}}],
                "evaluations": [{"name": "window_1", "ohlcv_limit": 24, "window_shift_bars": 0}],
                "workers": 1,
                "parallel_granularity": "variant",
            },
        },
    )
    monkeypatch.setattr(sweep, "ResearchRecorder", FakeRecorder)
    monkeypatch.setattr(sweep, "_available_bars", lambda **kwargs: 24)
    monkeypatch.setattr(
        sweep,
        "_generated_evaluations",
        lambda *args, **kwargs: [{"name": "window_1", "ohlcv_limit": 24, "window_shift_bars": 0}],
    )

    class FakeFuture:
        def result(self):
            raise RuntimeError("window job failed")

    class FakeExecutor:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, *args, **kwargs):
            return FakeFuture()

    monkeypatch.setattr(sweep, "ProcessPoolExecutor", FakeExecutor)
    monkeypatch.setattr(sweep, "as_completed", lambda futures: list(futures))
    monkeypatch.setattr(sweep.sys, "argv", ["run_rolling_window_sweep.py"])

    with pytest.raises(RuntimeError, match="window job failed"):
        sweep.main()

    assert finalized["task_name"] == "rolling_window_sweep"
    assert finalized["status"] == "failed"
    assert finalized["error_path"] == "error.json"
    assert finalized["summary"] == {
        "reason": "rolling_window_sweep_failed",
        "error_type": "RuntimeError",
        "error": "window job failed",
    }


def test_main_uses_runtime_config_and_env_helpers(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class FakeRun:
        def __init__(self):
            self.run_dir = tmp_path / "run"
            self.run_dir.mkdir(parents=True, exist_ok=True)

        def write_json(self, relative_path: str, payload):
            return self.run_dir / relative_path

    class FakeRecorder:
        def __init__(self, *args, **kwargs):
            pass

        def start_run(self, **kwargs):
            return FakeRun()

        def finalize_run(self, run, *, status: str, summary):
            return run.run_dir / "meta.json"

    monkeypatch.setattr(
        sweep,
        "load_task_config",
        lambda path: {
            "task": {"name": "rolling_window_sweep"},
            "paths": {},
            "experiment": {
                "variants": [{"name": "v1", "symbols": ["BTC/USDT"], "overrides": {}}],
                "evaluations": [{"name": "window_1", "ohlcv_limit": 24, "window_shift_bars": 0}],
                "env_path": ".env.runtime",
                "workers": 1,
                "parallel_granularity": "variant",
            },
        },
    )
    monkeypatch.setattr(sweep, "ResearchRecorder", FakeRecorder)
    monkeypatch.setattr(
        sweep,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str((tmp_path / "configs" / "runtime.yaml").resolve()),
    )
    monkeypatch.setattr(
        sweep,
        "resolve_runtime_env_path",
        lambda raw_env_path=None, project_root=None: str((tmp_path / ".env.runtime").resolve()),
    )
    monkeypatch.setattr(
        sweep,
        "_available_bars",
        lambda **kwargs: captured.update(kwargs) or 24,
    )
    monkeypatch.setattr(
        sweep,
        "_generated_evaluations",
        lambda *args, **kwargs: [{"name": "window_1", "ohlcv_limit": 24, "window_shift_bars": 0}],
    )
    monkeypatch.setattr(
        sweep,
        "_run_variant_job",
        lambda **kwargs: {
            "name": kwargs["variant"]["name"],
            "symbols": list(kwargs["variant"].get("symbols") or []),
            "overrides": dict(kwargs["variant"].get("overrides") or {}),
            "windows": [],
            "aggregate": {
                "positive_windows": 0,
                "mean_total_return": 0.0,
                "median_total_return": 0.0,
                "min_total_return": 0.0,
                "max_max_dd": 0.0,
            },
        },
    )
    class FakeFuture:
        def __init__(self, result):
            self._result = result

        def result(self):
            return self._result

    class FakeExecutor:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args, **kwargs):
            return FakeFuture(fn(*args, **kwargs))

    monkeypatch.setattr(sweep, "ProcessPoolExecutor", FakeExecutor)
    monkeypatch.setattr(sweep, "as_completed", lambda futures: list(futures))
    monkeypatch.setattr(sweep.sys, "argv", ["run_rolling_window_sweep.py"])

    assert sweep.main() == 0
    assert captured["base_config_path"] == str((tmp_path / "configs" / "runtime.yaml").resolve())
    assert captured["env_path"] == str((tmp_path / ".env.runtime").resolve())
