from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path

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
