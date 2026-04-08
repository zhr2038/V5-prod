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
    conn.execute("CREATE TABLE positions (symbol TEXT)")
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
    monkeypatch.setattr(
        reporting_health,
        "_resolve_health_paths",
        lambda: reporting_health.HealthPaths(
            orders_db=reports_dir / "orders.sqlite",
            fills_db=reports_dir / "fills.sqlite",
            positions_db=reports_dir / "positions.sqlite",
            kill_switch_path=reports_dir / "kill_switch.json",
            reconcile_status_path=reports_dir / "reconcile_status.json",
        ),
    )
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


def test_health_uses_active_config_runtime_paths(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    reports_dir = fake_root / "reports"
    reports_dir.mkdir(parents=True)
    configs_dir = fake_root / "configs"
    configs_dir.mkdir(parents=True)

    conn = sqlite3.connect(reports_dir / "positions.sqlite")
    conn.execute("SELECT 1")
    conn.close()

    _write_json(reports_dir / "kill_switch.json", {"enabled": False})
    _write_json(reports_dir / "reconcile_status.json", {"ok": True, "reason": ""})
    _write_json(
        reports_dir / "auto_risk_eval.json",
        {"current_level": "LOW", "metrics": {"dd_pct": 0.0}},
    )

    root_orders_conn = sqlite3.connect(reports_dir / "orders.sqlite")
    root_orders_conn.execute(
        "CREATE TABLE orders (state TEXT, created_ts INTEGER, updated_ts INTEGER)"
    )
    root_orders_conn.execute(
        "INSERT INTO orders(state, created_ts, updated_ts) VALUES (?, ?, ?)",
        ("FILLED", 1_000, 1_000),
    )
    root_orders_conn.commit()
    root_orders_conn.close()

    root_fills_conn = sqlite3.connect(reports_dir / "fills.sqlite")
    root_fills_conn.execute("CREATE TABLE fills (ts_ms INTEGER)")
    root_fills_conn.execute("INSERT INTO fills(ts_ms) VALUES (?)", (9_000_000,))
    root_fills_conn.commit()
    root_fills_conn.close()

    shadow_orders = reports_dir / "shadow_orders.sqlite"
    shadow_orders_conn = sqlite3.connect(shadow_orders)
    shadow_orders_conn.execute(
        "CREATE TABLE orders (state TEXT, created_ts INTEGER, updated_ts INTEGER)"
    )
    shadow_orders_conn.execute(
        "INSERT INTO orders(state, created_ts, updated_ts) VALUES (?, ?, ?)",
        ("FILLED", 1_000, 9_800_000),
    )
    shadow_orders_conn.commit()
    shadow_orders_conn.close()

    shadow_fills = reports_dir / "shadow_fills.sqlite"
    shadow_fills_conn = sqlite3.connect(shadow_fills)
    shadow_fills_conn.execute("CREATE TABLE fills (ts_ms INTEGER)")
    shadow_fills_conn.execute("INSERT INTO fills(ts_ms) VALUES (?)", (9_940_000,))
    shadow_fills_conn.commit()
    shadow_fills_conn.close()

    shadow_positions = reports_dir / "shadow_positions.sqlite"
    shadow_positions_conn = sqlite3.connect(shadow_positions)
    shadow_positions_conn.execute("CREATE TABLE positions (symbol TEXT)")
    shadow_positions_conn.close()

    _write_json(reports_dir / "shadow_kill_switch.json", {"enabled": True, "trigger": "manual"})
    _write_json(reports_dir / "shadow_reconcile_status.json", {"ok": False, "reason": "shadow drift"})

    (configs_dir / "live_prod.yaml").write_text(
        "\n".join(
            [
                "execution:",
                "  order_store_path: reports/shadow_orders.sqlite",
                "  kill_switch_path: reports/shadow_kill_switch.json",
                "  reconcile_status_path: reports/shadow_reconcile_status.json",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(reporting_health, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(reporting_health, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(reporting_health, "CONFIGS_DIR", configs_dir)
    monkeypatch.setattr(reporting_health.time, "time", lambda: 10_000.0)
    app = Flask(__name__)
    app.register_blueprint(reporting_health.health_bp)
    client = app.test_client()
    response = client.get("/health")

    assert response.status_code == 503
    payload = response.get_json()
    assert payload["status"] == "degraded"
    assert payload["checks"]["database"]["path"] == str(shadow_positions)
    assert payload["checks"]["kill_switch"]["enabled"] is True
    assert payload["checks"]["kill_switch"]["trigger"] == "manual"
    assert payload["checks"]["reconcile"]["ok"] is False
    assert payload["checks"]["reconcile"]["reason"] == "shadow drift"
    assert payload["checks"]["last_trade"]["last_ts"] == 9_940_000
    assert payload["checks"]["last_trade"]["age_minutes"] == 1.0


def test_health_and_ready_use_runtime_positions_db(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    reports_dir = fake_root / "reports"
    reports_dir.mkdir(parents=True)
    configs_dir = fake_root / "configs"
    configs_dir.mkdir(parents=True)

    root_positions = reports_dir / "positions.sqlite"
    root_conn = sqlite3.connect(root_positions)
    root_conn.execute("SELECT 1")
    root_conn.close()

    _write_json(reports_dir / "kill_switch.json", {"enabled": False})
    _write_json(reports_dir / "reconcile_status.json", {"ok": True, "reason": ""})
    _write_json(
        reports_dir / "auto_risk_eval.json",
        {"current_level": "LOW", "metrics": {"dd_pct": 0.0}},
    )

    (configs_dir / "live_prod.yaml").write_text(
        "\n".join(
            [
                "execution:",
                "  order_store_path: reports/shadow_orders.sqlite",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(reporting_health, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(reporting_health, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(reporting_health, "CONFIGS_DIR", configs_dir)
    monkeypatch.setattr(reporting_health.time, "time", lambda: 10_000.0)
    app = Flask(__name__)
    app.register_blueprint(reporting_health.health_bp)
    client = app.test_client()

    health_response = client.get("/health")
    ready_response = client.get("/ready")

    assert health_response.status_code == 503
    health_payload = health_response.get_json()
    assert health_payload["status"] == "unhealthy"
    assert health_payload["checks"]["database"]["status"] == "error"
    assert health_payload["checks"]["database"]["path"] == str(reports_dir / "shadow_positions.sqlite")

    assert ready_response.status_code == 503
    ready_payload = ready_response.get_json()
    assert ready_payload["ready"] is False
    assert "shadow_positions.sqlite" in ready_payload["reasons"][0]
