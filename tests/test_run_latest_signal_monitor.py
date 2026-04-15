from __future__ import annotations

from pathlib import Path

import pytest

import scripts.run_latest_signal_monitor as monitor


def test_main_finalizes_failed_run_when_variant_execution_raises(monkeypatch, tmp_path: Path) -> None:
    finalized: dict[str, object] = {}

    class FakeRun:
        def __init__(self):
            self.run_dir = tmp_path / "run"
            self.run_dir.mkdir(parents=True, exist_ok=True)

        def write_json(self, relative_path: str, payload):
            finalized["error_path"] = relative_path
            finalized["error_payload"] = payload
            return self.run_dir / relative_path

        def write_text(self, relative_path: str, content: str):
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

    task_config = {
        "task": {"name": "latest_signal_monitor"},
        "experiment": {
            "variants": [{"name": "baseline"}, {"name": "champion"}],
            "baseline_name": "baseline",
            "champion_name": "champion",
        },
        "paths": {},
    }

    monkeypatch.setattr(monitor, "ResearchRecorder", FakeRecorder)
    monkeypatch.setattr(monitor, "load_task_config", lambda path: task_config)
    monkeypatch.setattr(monitor, "run_latest_signal_variant", lambda **kwargs: (_ for _ in ()).throw(TimeoutError("variant timed out")))
    monkeypatch.setattr(monitor.sys, "argv", ["run_latest_signal_monitor.py"])

    with pytest.raises(TimeoutError, match="variant timed out"):
        monitor.main()

    assert finalized["task_name"] == "latest_signal_monitor"
    assert finalized["status"] == "failed"
    assert finalized["error_path"] == "error.json"
    assert finalized["summary"] == {
        "reason": "latest_signal_monitor_failed",
        "error_type": "TimeoutError",
        "error": "variant timed out",
    }


def test_main_uses_runtime_config_and_env_helpers(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class FakeRun:
        def __init__(self):
            self.run_dir = tmp_path / "run"
            self.run_dir.mkdir(parents=True, exist_ok=True)

        def write_json(self, relative_path: str, payload):
            return self.run_dir / relative_path

        def write_text(self, relative_path: str, content: str):
            return self.run_dir / relative_path

    class FakeRecorder:
        def __init__(self, *args, **kwargs):
            pass

        def start_run(self, **kwargs):
            return FakeRun()

        def finalize_run(self, run, *, status: str, summary):
            return run.run_dir / "meta.json"

    task_config = {
        "task": {"name": "latest_signal_monitor"},
        "experiment": {
            "variants": [{"name": "baseline"}, {"name": "champion"}],
            "baseline_name": "baseline",
            "champion_name": "champion",
            "env_path": ".env.runtime",
        },
        "paths": {},
    }

    monkeypatch.setattr(monitor, "ResearchRecorder", FakeRecorder)
    monkeypatch.setattr(monitor, "load_task_config", lambda path: task_config)
    monkeypatch.setattr(
        monitor,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str((tmp_path / "configs" / "runtime.yaml").resolve()),
    )
    monkeypatch.setattr(
        monitor,
        "resolve_runtime_env_path",
        lambda raw_env_path=None, project_root=None: str((tmp_path / ".env.runtime").resolve()),
    )
    monkeypatch.setattr(
        monitor,
        "run_latest_signal_variant",
        lambda **kwargs: captured.setdefault(kwargs["variant"]["name"], kwargs) or {
            "name": kwargs["variant"]["name"],
            "selected": [],
            "orders": [],
            "target_weights": {},
            "counts": {},
            "rejects": {},
            "skip_reasons": {},
            "notes_tail": [],
        },
    )
    monkeypatch.setattr(monitor, "build_latest_signal_summary", lambda **kwargs: {"compare": {}, "baseline": {}, "champion": {}})
    monkeypatch.setattr(monitor, "build_latest_signal_markdown", lambda summary: "ok")
    monkeypatch.setattr(monitor.sys, "argv", ["run_latest_signal_monitor.py"])

    assert monitor.main() == 0
    assert captured["baseline"]["base_config_path"] == str((tmp_path / "configs" / "runtime.yaml").resolve())
    assert captured["baseline"]["env_path"] == str((tmp_path / ".env.runtime").resolve())
    assert captured["champion"]["base_config_path"] == str((tmp_path / "configs" / "runtime.yaml").resolve())
    assert captured["champion"]["env_path"] == str((tmp_path / ".env.runtime").resolve())
