from __future__ import annotations

from types import SimpleNamespace

import scripts.run_pressure_probe as run_pressure_probe


def test_top_processes_uses_parameterized_ps_call(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return SimpleNamespace(
            stdout="PID COMMAND %CPU %MEM\n1 python 50.0 1.0\n2 bash 10.0 0.5\n3 ssh 5.0 0.2\n"
        )

    monkeypatch.setattr(run_pressure_probe.subprocess, "run", fake_run)

    top = run_pressure_probe._top_processes(limit=2)

    assert captured["cmd"] == ["ps", "-eo", "pid,comm,%cpu,%mem", "--sort=-%cpu"]
    assert captured["kwargs"]["check"] is False
    assert captured["kwargs"]["capture_output"] is True
    assert captured["kwargs"]["text"] is True
    assert top == [
        "PID COMMAND %CPU %MEM",
        "1 python 50.0 1.0",
    ]
