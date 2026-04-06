from __future__ import annotations

import subprocess

import scripts.v5_status_report as v5_status_report


def _completed(returncode: int) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout="", stderr="")


def test_get_service_status_reports_running_when_service_active(monkeypatch) -> None:
    def _fake_run(cmd, **kwargs):
        unit = cmd[-1]
        if unit == "v5-prod.user.service":
            return _completed(0)
        return _completed(1)

    monkeypatch.setattr(v5_status_report.subprocess, "run", _fake_run)

    assert v5_status_report.get_service_status() == "running"


def test_get_service_status_reports_scheduled_when_timer_active(monkeypatch) -> None:
    def _fake_run(cmd, **kwargs):
        unit = cmd[-1]
        if unit == "v5-prod.user.timer":
            return _completed(0)
        return _completed(1)

    monkeypatch.setattr(v5_status_report.subprocess, "run", _fake_run)

    assert v5_status_report.get_service_status() == "scheduled"


def test_get_service_status_reports_stopped_when_units_inactive(monkeypatch) -> None:
    monkeypatch.setattr(v5_status_report.subprocess, "run", lambda *args, **kwargs: _completed(1))

    assert v5_status_report.get_service_status() == "stopped"
