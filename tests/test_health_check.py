from __future__ import annotations

from types import SimpleNamespace

import scripts.health_check as health_check


def test_health_check_uses_current_timer_unit_names(monkeypatch) -> None:
    commands = []
    now_mono_usec = 500_000_000

    def _fake_run(cmd, capture_output=True, text=True, timeout=5):
        commands.append(cmd)
        if cmd[:3] == ["systemctl", "--user", "status"]:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if cmd[:3] == ["systemctl", "--user", "show"]:
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    "LastTriggerUSec=Mon 2026-04-06 12:00:00 CST\n"
                    f"LastTriggerUSecMonotonic={now_mono_usec}\n"
                ),
                stderr="",
            )
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(health_check.shutil, "which", lambda name: "/usr/bin/systemctl" if name == "systemctl" else None)
    monkeypatch.setattr(health_check.subprocess, "run", _fake_run)
    monkeypatch.setattr(health_check.time, "monotonic", lambda: now_mono_usec / 1_000_000)

    result = health_check.HealthChecker().check_timer_health()

    assert result["status"] == "healthy"
    checked_units = [cmd[3] for cmd in commands if cmd[:3] == ["systemctl", "--user", "show"]]
    assert checked_units == [
        "v5-prod.user.timer",
        "v5-reconcile.timer",
        "v5-trade-monitor.timer",
    ]


def test_health_check_marks_delayed_timer_from_monotonic_usec(monkeypatch) -> None:
    now_mono_usec = 10_000_000_000

    def _fake_run(cmd, capture_output=True, text=True, timeout=5):
        if cmd[:3] == ["systemctl", "--user", "status"]:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if cmd[:3] == ["systemctl", "--user", "show"]:
            timer_name = cmd[3]
            last_trigger_mono_usec = 0 if timer_name == "v5-prod.user.timer" else now_mono_usec
            if timer_name == "v5-trade-monitor.timer":
                last_trigger_mono_usec = now_mono_usec - 71 * 60 * 1_000_000
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    "LastTriggerUSec=Mon 2026-04-06 10:00:00 CST\n"
                    f"LastTriggerUSecMonotonic={last_trigger_mono_usec}\n"
                ),
                stderr="",
            )
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(health_check.shutil, "which", lambda name: "/usr/bin/systemctl" if name == "systemctl" else None)
    monkeypatch.setattr(health_check.subprocess, "run", _fake_run)
    monkeypatch.setattr(health_check.time, "monotonic", lambda: now_mono_usec / 1_000_000)

    result = health_check.HealthChecker().check_timer_health()

    assert result["status"] == "warning"
    assert result["details"] == [
        {
            "timer": "v5-prod.user.timer",
            "status": "unknown",
            "detail": "no trigger time",
        },
        {
            "timer": "v5-trade-monitor.timer",
            "status": "delayed",
            "last_run": "Mon 2026-04-06 10:00:00 CST",
            "delay_min": 71.0,
        },
    ]
