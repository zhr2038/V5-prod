from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import scripts.health_check as health_check


def test_health_check_uses_current_timer_unit_names(monkeypatch) -> None:
    commands = []
    now_mono_usec = 500_000_000

    def _fake_run(cmd, capture_output=True, text=True, timeout=5):
        commands.append(cmd)
        if cmd[:3] == ["systemctl", "--user", "show"] and cmd[-1] == "--property=LoadState":
            return SimpleNamespace(returncode=0, stdout="LoadState=loaded\n", stderr="")
        if cmd[:3] == ["systemctl", "--user", "show"]:
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    "LoadState=loaded\n"
                    "ActiveState=active\n"
                    "UnitFileState=enabled\n"
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
    checked_units = [cmd[3] for cmd in commands if cmd[:3] == ["systemctl", "--user", "show"] and cmd[-1] != "--property=LoadState"]
    assert checked_units == [
        "v5-prod.user.timer",
        "v5-reconcile.timer",
        "v5-trade-monitor.timer",
    ]


def test_health_check_marks_delayed_timer_from_monotonic_usec(monkeypatch) -> None:
    now_mono_usec = 10_000_000_000

    def _fake_run(cmd, capture_output=True, text=True, timeout=5):
        if cmd[:3] == ["systemctl", "--user", "show"] and cmd[-1] == "--property=LoadState":
            return SimpleNamespace(returncode=0, stdout="LoadState=loaded\n", stderr="")
        if cmd[:3] == ["systemctl", "--user", "show"]:
            timer_name = cmd[3]
            last_trigger_mono_usec = 0 if timer_name == "v5-prod.user.timer" else now_mono_usec
            if timer_name == "v5-trade-monitor.timer":
                last_trigger_mono_usec = now_mono_usec - 71 * 60 * 1_000_000
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    "LoadState=loaded\n"
                    "ActiveState=active\n"
                    "UnitFileState=enabled\n"
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


def test_health_check_marks_missing_or_disabled_timer_critical(monkeypatch) -> None:
    def _fake_run(cmd, capture_output=True, text=True, timeout=5):
        if cmd[:3] == ["systemctl", "--user", "show"] and cmd[-1] == "--property=LoadState":
            return SimpleNamespace(returncode=0, stdout="LoadState=loaded\n", stderr="")
        if cmd[:3] == ["systemctl", "--user", "show"]:
            timer_name = cmd[3]
            if timer_name == "v5-reconcile.timer":
                return SimpleNamespace(
                    returncode=1,
                    stdout="LoadState=not-found\nActiveState=inactive\nUnitFileState=disabled\n",
                    stderr="Unit v5-reconcile.timer could not be found.\n",
                )
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    "LoadState=loaded\n"
                    "ActiveState=active\n"
                    "UnitFileState=enabled\n"
                    "LastTriggerUSec=Mon 2026-04-06 12:00:00 CST\n"
                    "LastTriggerUSecMonotonic=500000000\n"
                ),
                stderr="",
            )
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(health_check.shutil, "which", lambda name: "/usr/bin/systemctl" if name == "systemctl" else None)
    monkeypatch.setattr(health_check.subprocess, "run", _fake_run)
    monkeypatch.setattr(health_check.time, "monotonic", lambda: 500.0)

    result = health_check.HealthChecker().check_timer_health()

    assert result["status"] == "critical"
    assert result["details"] == [
        {
            "timer": "v5-reconcile.timer",
            "status": "missing",
            "detail": "unit not found",
        }
    ]


def test_health_check_does_not_fall_back_to_legacy_timer_when_prod_exists(monkeypatch) -> None:
    def _fake_run(cmd, capture_output=True, text=True, timeout=5):
        if cmd[:3] == ["systemctl", "--user", "show"] and cmd[-1] == "--property=LoadState":
            unit = cmd[3]
            if unit in {"v5-prod.user.timer", "v5-live-20u.user.timer"}:
                return SimpleNamespace(returncode=0, stdout="LoadState=loaded\n", stderr="")
            return SimpleNamespace(returncode=0, stdout="LoadState=loaded\n", stderr="")
        if cmd[:3] == ["systemctl", "--user", "show"]:
            timer_name = cmd[3]
            if timer_name == "v5-prod.user.timer":
                return SimpleNamespace(
                    returncode=0,
                    stdout=(
                        "LoadState=loaded\n"
                        "ActiveState=inactive\n"
                        "UnitFileState=enabled\n"
                        "LastTriggerUSec=n/a\n"
                        "LastTriggerUSecMonotonic=n/a\n"
                    ),
                    stderr="",
                )
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    "LoadState=loaded\n"
                    "ActiveState=active\n"
                    "UnitFileState=enabled\n"
                    "LastTriggerUSec=Mon 2026-04-06 12:00:00 CST\n"
                    "LastTriggerUSecMonotonic=500000000\n"
                ),
                stderr="",
            )
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(health_check.shutil, "which", lambda name: "/usr/bin/systemctl" if name == "systemctl" else None)
    monkeypatch.setattr(health_check.subprocess, "run", _fake_run)
    monkeypatch.setattr(health_check.time, "monotonic", lambda: 500.0)

    result = health_check.HealthChecker().check_timer_health()

    assert result["status"] == "critical"
    assert result["details"] == [
        {
            "timer": "v5-prod.user.timer",
            "status": "inactive",
            "detail": "inactive",
        }
    ]


def test_health_check_database_uses_runtime_db_paths_from_active_config(monkeypatch, tmp_path) -> None:
    workspace = tmp_path
    reports_dir = workspace / "reports"
    shadow_dir = reports_dir / "shadow_runtime"
    configs_dir = workspace / "configs"
    shadow_dir.mkdir(parents=True, exist_ok=True)
    configs_dir.mkdir(parents=True, exist_ok=True)
    (configs_dir / "live_prod.yaml").write_text(
        "\n".join(
            [
                "execution:",
                "  order_store_path: reports/shadow_runtime/orders.sqlite",
                "",
            ]
        ),
        encoding="utf-8",
    )

    for db_name, table_name in (
        ("orders.sqlite", "orders"),
        ("positions.sqlite", "positions"),
        ("fills.sqlite", "fills"),
    ):
        db_path = shadow_dir / db_name
        con = sqlite3.connect(str(db_path))
        try:
            con.execute(f"CREATE TABLE {table_name} (id INTEGER)")
            con.commit()
        finally:
            con.close()

    monkeypatch.setattr(health_check, "WORKSPACE", workspace)
    monkeypatch.setattr(health_check, "REPORTS_DIR", reports_dir)

    result = health_check.HealthChecker().check_database_health()

    assert result["status"] == "healthy"
    assert [item["db"] for item in result["details"]] == [
        "orders.sqlite",
        "positions.sqlite",
        "fills.sqlite",
    ]
    assert all(item["status"] == "healthy" for item in result["details"])
