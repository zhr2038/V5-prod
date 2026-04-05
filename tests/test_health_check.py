from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import scripts.health_check as health_check


def test_health_check_uses_current_timer_unit_names(monkeypatch) -> None:
    commands = []
    now_usec = int(datetime.now().timestamp() * 1_000_000)

    def _fake_run(cmd, capture_output=True, text=True, timeout=5):
        commands.append(cmd)
        if cmd[:3] == ["systemctl", "--user", "status"]:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if cmd[:3] == ["systemctl", "--user", "show"]:
            return SimpleNamespace(returncode=0, stdout=f"LastTriggerUSec={now_usec}\n", stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(health_check.shutil, "which", lambda name: "/usr/bin/systemctl" if name == "systemctl" else None)
    monkeypatch.setattr(health_check.subprocess, "run", _fake_run)

    result = health_check.HealthChecker().check_timer_health()

    assert result["status"] == "healthy"
    checked_units = [cmd[3] for cmd in commands if cmd[:3] == ["systemctl", "--user", "show"]]
    assert checked_units == [
        "v5-prod.user.timer",
        "v5-reconcile.timer",
        "v5-trade-monitor.timer",
    ]
