from __future__ import annotations

import sqlite3
import subprocess

import scripts.v5_status_report as v5_status_report


def _completed(returncode: int) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout="", stderr="")


def _show_completed(load_state: str) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[],
        returncode=0 if load_state != "not-found" else 1,
        stdout=f"LoadState={load_state}\n",
        stderr="",
    )


def test_get_service_status_reports_running_when_service_active(monkeypatch) -> None:
    def _fake_run(cmd, **kwargs):
        if cmd[:3] == ["systemctl", "--user", "show"]:
            return _show_completed("loaded")
        unit = cmd[-1]
        if unit == "v5-prod.user.service":
            return _completed(0)
        return _completed(1)

    monkeypatch.setattr(v5_status_report.subprocess, "run", _fake_run)

    assert v5_status_report.get_service_status() == "running"


def test_get_service_status_reports_scheduled_when_timer_active(monkeypatch) -> None:
    def _fake_run(cmd, **kwargs):
        if cmd[:3] == ["systemctl", "--user", "show"]:
            return _show_completed("loaded")
        unit = cmd[-1]
        if unit == "v5-prod.user.timer":
            return _completed(0)
        return _completed(1)

    monkeypatch.setattr(v5_status_report.subprocess, "run", _fake_run)

    assert v5_status_report.get_service_status() == "scheduled"


def test_get_service_status_reports_stopped_when_units_inactive(monkeypatch) -> None:
    def _fake_run(cmd, **kwargs):
        if cmd[:3] == ["systemctl", "--user", "show"]:
            return _show_completed("loaded")
        return _completed(1)

    monkeypatch.setattr(v5_status_report.subprocess, "run", _fake_run)

    assert v5_status_report.get_service_status() == "stopped"


def test_get_service_status_does_not_fall_back_to_legacy_units_when_prod_exists(monkeypatch) -> None:
    def _fake_run(cmd, **kwargs):
        if cmd[:3] == ["systemctl", "--user", "show"]:
            return _show_completed("loaded")
        unit = cmd[-1]
        if unit in {"v5-live-20u.user.service", "v5-live-20u.user.timer"}:
            return _completed(0)
        return _completed(1)

    monkeypatch.setattr(v5_status_report.subprocess, "run", _fake_run)

    assert v5_status_report.get_service_status() == "stopped"


def test_get_last_filled_trade_ts_prefers_fill_store_timestamp(tmp_path, monkeypatch) -> None:
    fills_db = tmp_path / "fills.sqlite"
    orders_db = tmp_path / "orders.sqlite"

    conn = sqlite3.connect(str(fills_db))
    conn.execute("CREATE TABLE fills (ts_ms INTEGER)")
    conn.execute("INSERT INTO fills(ts_ms) VALUES (?)", (1_710_000_300_000,))
    conn.commit()
    conn.close()

    conn = sqlite3.connect(str(orders_db))
    conn.execute("CREATE TABLE orders (state TEXT, created_ts INTEGER, updated_ts INTEGER)")
    conn.execute("INSERT INTO orders(state, created_ts, updated_ts) VALUES ('FILLED', ?, ?)", (1_710_000_000_000, 1_710_000_100_000))
    conn.commit()
    conn.close()

    monkeypatch.setattr(v5_status_report, "FILLS_DB", fills_db)
    monkeypatch.setattr(v5_status_report, "ORDERS_DB", orders_db)

    assert v5_status_report.get_last_filled_trade_ts() == v5_status_report._format_ts_ms(1_710_000_300_000)


def test_get_last_filled_trade_ts_falls_back_to_order_updated_ts(tmp_path, monkeypatch) -> None:
    fills_db = tmp_path / "fills.sqlite"
    orders_db = tmp_path / "orders.sqlite"

    conn = sqlite3.connect(str(orders_db))
    conn.execute("CREATE TABLE orders (state TEXT, created_ts INTEGER, updated_ts INTEGER)")
    conn.execute("INSERT INTO orders(state, created_ts, updated_ts) VALUES ('FILLED', ?, ?)", (1_710_000_000_000, 1_710_000_600_000))
    conn.commit()
    conn.close()

    monkeypatch.setattr(v5_status_report, "FILLS_DB", fills_db)
    monkeypatch.setattr(v5_status_report, "ORDERS_DB", orders_db)

    assert v5_status_report.get_last_filled_trade_ts() == v5_status_report._format_ts_ms(1_710_000_600_000)
