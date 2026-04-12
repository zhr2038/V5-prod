from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path

import pytest

import scripts.run_core6_avax_shadow_cycle as cycle


def test_run_script_uses_timeout(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return SimpleNamespace(stdout="ok", stderr="")

    monkeypatch.setattr(cycle.subprocess, "run", fake_run)

    result = cycle._run_script("run_rolling_window_sweep.py", tmp_path / "cfg.yaml", cwd=tmp_path)

    assert captured["cmd"] == [
        cycle.sys.executable,
        str(cycle.PROJECT_ROOT / "scripts" / "run_rolling_window_sweep.py"),
        str(tmp_path / "cfg.yaml"),
    ]
    assert captured["kwargs"]["cwd"] == str(tmp_path)
    assert captured["kwargs"]["capture_output"] is True
    assert captured["kwargs"]["text"] is True
    assert captured["kwargs"]["timeout"] == cycle.SCRIPT_TIMEOUT_SECONDS
    assert captured["kwargs"]["check"] is True
    assert result.stdout == "ok"


def test_main_finalizes_failed_run_when_subprocess_raises(monkeypatch, tmp_path: Path) -> None:
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

    monkeypatch.setattr(cycle, "ResearchRecorder", FakeRecorder)
    monkeypatch.setattr(cycle, "_run_script", lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError("sweep timed out")))
    monkeypatch.setattr(cycle.sys, "argv", ["run_core6_avax_shadow_cycle.py"])

    with pytest.raises(TimeoutError, match="sweep timed out"):
        cycle.main()

    assert finalized["task_name"] == "core6_avax_shadow_cycle"
    assert finalized["status"] == "failed"
    assert finalized["error_path"] == "error.json"
    assert finalized["summary"] == {
        "reason": "shadow_cycle_failed",
        "error_type": "TimeoutError",
        "error": "sweep timed out",
    }
