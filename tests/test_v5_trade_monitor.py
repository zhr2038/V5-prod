from __future__ import annotations

import sqlite3
from datetime import datetime

import scripts.v5_trade_monitor as trade_monitor


def test_build_paths_anchor_monitor_artifacts_to_repo_root(tmp_path) -> None:
    paths = trade_monitor.build_paths(tmp_path)

    assert paths.project_root == tmp_path.resolve()
    assert paths.reports_dir == tmp_path / "reports"
    assert paths.logs_dir == tmp_path / "logs"
    assert paths.fills_db_path == tmp_path / "reports" / "fills.sqlite"
    assert paths.env_path == tmp_path / ".env"
    assert paths.alert_file == tmp_path / "reports" / "monitor_alert.txt"


def test_get_last_trade_time_reads_repo_fill_store_ts_ms(tmp_path, monkeypatch) -> None:
    paths = trade_monitor.build_paths(tmp_path)
    paths.reports_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(paths.fills_db_path))
    conn.execute("CREATE TABLE fills (ts_ms INTEGER)")
    conn.execute("INSERT INTO fills(ts_ms) VALUES (?)", (1_710_000_000_123,))
    conn.commit()
    conn.close()

    def _unexpected_run_command(*args, **kwargs):
        raise AssertionError("journalctl fallback should not run when fill store has data")

    monkeypatch.setattr(trade_monitor, "run_command", _unexpected_run_command)

    assert trade_monitor.get_last_trade_time(paths) == datetime.fromtimestamp(1_710_000_000_123 / 1000)


def test_send_telegram_alert_falls_back_to_repo_reports_dir(tmp_path) -> None:
    paths = trade_monitor.build_paths(tmp_path)

    ok = trade_monitor.send_telegram_alert("test alert", paths=paths)

    assert ok is True
    assert paths.alert_file.exists()
    assert "test alert" in paths.alert_file.read_text(encoding="utf-8")


def test_resolve_live_service_unit_name_prefers_prod_when_legacy_exists(monkeypatch) -> None:
    def _fake_run(cmd, **kwargs):
        if cmd[:3] == ["systemctl", "--user", "show"]:
            unit = cmd[3]
            if unit in {"v5-prod.user.service", "v5-live-20u.user.service"}:
                return type("Result", (), {"stdout": "LoadState=loaded\n"})()
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(trade_monitor.shutil, "which", lambda name: "/usr/bin/systemctl" if name == "systemctl" else None)
    monkeypatch.setattr(trade_monitor.subprocess, "run", _fake_run)

    assert trade_monitor.resolve_live_service_unit_name() == "v5-prod.user.service"
