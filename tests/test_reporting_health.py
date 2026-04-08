from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from flask import Flask

import src.reporting.health as reporting_health


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _prepare_reports_dir(tmp_path: Path) -> Path:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()

    conn = sqlite3.connect(reports_dir / "positions.sqlite")
    conn.execute("SELECT 1")
    conn.close()

    _write_json(reports_dir / "kill_switch.json", {"enabled": False})
    _write_json(reports_dir / "reconcile_status.json", {"ok": True, "reason": ""})
    _write_json(
        reports_dir / "auto_risk_eval.json",
        {"current_level": "LOW", "metrics": {"dd_pct": 0.0}},
    )
    return reports_dir


def _make_client(monkeypatch, reports_dir: Path, now_ts_s: float):
    monkeypatch.setattr(reporting_health, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(reporting_health.time, "time", lambda: now_ts_s)
    app = Flask(__name__)
    app.register_blueprint(reporting_health.health_bp)
    return app.test_client()


def test_health_prefers_fill_timestamp_over_order_created_ts(monkeypatch, tmp_path: Path) -> None:
    reports_dir = _prepare_reports_dir(tmp_path)

    orders_conn = sqlite3.connect(reports_dir / "orders.sqlite")
    orders_conn.execute(
        "CREATE TABLE orders (state TEXT, created_ts INTEGER, updated_ts INTEGER)"
    )
    orders_conn.execute(
        "INSERT INTO orders(state, created_ts, updated_ts) VALUES (?, ?, ?)",
        ("FILLED", 1_000, 1_000),
    )
    orders_conn.commit()
    orders_conn.close()

    fill_ts_ms = 9_940_000
    fills_conn = sqlite3.connect(reports_dir / "fills.sqlite")
    fills_conn.execute("CREATE TABLE fills (ts_ms INTEGER)")
    fills_conn.execute("INSERT INTO fills(ts_ms) VALUES (?)", (fill_ts_ms,))
    fills_conn.commit()
    fills_conn.close()

    client = _make_client(monkeypatch, reports_dir, now_ts_s=10_000.0)
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["checks"]["last_trade"]["status"] == "ok"
    assert payload["checks"]["last_trade"]["last_ts"] == fill_ts_ms
    assert payload["checks"]["last_trade"]["age_minutes"] == 1.0


def test_health_falls_back_to_order_updated_ts_when_fills_missing(monkeypatch, tmp_path: Path) -> None:
    reports_dir = _prepare_reports_dir(tmp_path)

    updated_ts_ms = 9_700_000
    orders_conn = sqlite3.connect(reports_dir / "orders.sqlite")
    orders_conn.execute(
        "CREATE TABLE orders (state TEXT, created_ts INTEGER, updated_ts INTEGER)"
    )
    orders_conn.execute(
        "INSERT INTO orders(state, created_ts, updated_ts) VALUES (?, ?, ?)",
        ("FILLED", 1_000, updated_ts_ms),
    )
    orders_conn.commit()
    orders_conn.close()

    client = _make_client(monkeypatch, reports_dir, now_ts_s=10_000.0)
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["checks"]["last_trade"]["status"] == "ok"
    assert payload["checks"]["last_trade"]["last_ts"] == updated_ts_ms
    assert payload["checks"]["last_trade"]["age_minutes"] == 5.0


def test_health_uses_newer_filled_order_when_fill_store_lags(monkeypatch, tmp_path: Path) -> None:
    reports_dir = _prepare_reports_dir(tmp_path)

    orders_conn = sqlite3.connect(reports_dir / "orders.sqlite")
    orders_conn.execute(
        "CREATE TABLE orders (state TEXT, created_ts INTEGER, updated_ts INTEGER)"
    )
    orders_conn.execute(
        "INSERT INTO orders(state, created_ts, updated_ts) VALUES (?, ?, ?)",
        ("FILLED", 1_000, 9_880_000),
    )
    orders_conn.commit()
    orders_conn.close()

    fills_conn = sqlite3.connect(reports_dir / "fills.sqlite")
    fills_conn.execute("CREATE TABLE fills (ts_ms INTEGER)")
    fills_conn.execute("INSERT INTO fills(ts_ms) VALUES (?)", (9_700_000,))
    fills_conn.commit()
    fills_conn.close()

    client = _make_client(monkeypatch, reports_dir, now_ts_s=10_000.0)
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["checks"]["last_trade"]["status"] == "ok"
    assert payload["checks"]["last_trade"]["last_ts"] == 9_880_000
    assert payload["checks"]["last_trade"]["age_minutes"] == 2.0


def test_health_reports_nested_enabled_kill_switch(monkeypatch, tmp_path: Path) -> None:
    reports_dir = _prepare_reports_dir(tmp_path)
    _write_json(
        reports_dir / "kill_switch.json",
        {"kill_switch": {"enabled": True, "trigger": "manual"}},
    )

    client = _make_client(monkeypatch, reports_dir, now_ts_s=10_000.0)
    response = client.get("/health")

    assert response.status_code == 503
    payload = response.get_json()
    assert payload["status"] == "degraded"
    assert payload["checks"]["kill_switch"]["enabled"] is True
    assert payload["checks"]["kill_switch"]["trigger"] == "manual"


def test_health_degrades_when_reconcile_ok_is_string_false(monkeypatch, tmp_path: Path) -> None:
    reports_dir = _prepare_reports_dir(tmp_path)
    _write_json(reports_dir / "reconcile_status.json", {"ok": "false", "reason": "drift"})

    client = _make_client(monkeypatch, reports_dir, now_ts_s=10_000.0)
    response = client.get("/health")

    assert response.status_code == 503
    payload = response.get_json()
    assert payload["status"] == "degraded"
    assert payload["checks"]["reconcile"]["status"] == "warning"
    assert payload["checks"]["reconcile"]["ok"] is False
