from __future__ import annotations

from pathlib import Path

from flask import Flask

from src.reporting import health as reporting_health


def test_resolve_active_config_path_uses_runtime_config_helper(monkeypatch, tmp_path: Path) -> None:
    expected = (tmp_path / "configs" / "runtime_live.yaml").resolve()
    monkeypatch.setattr(
        reporting_health,
        "resolve_runtime_config_path",
        lambda project_root=None: str(expected),
    )

    path = reporting_health._resolve_active_config_path()

    assert path == expected


def test_load_active_config_fails_fast_when_runtime_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    expected = (tmp_path / "configs" / "runtime_live.yaml").resolve()
    monkeypatch.setattr(
        reporting_health,
        "resolve_runtime_config_path",
        lambda project_root=None: str(expected),
    )

    try:
        reporting_health._load_active_config()
    except FileNotFoundError as exc:
        assert str(expected) in str(exc)
    else:
        raise AssertionError("expected FileNotFoundError")


def test_health_check_falls_back_to_runtime_guard_path_when_eval_missing(monkeypatch, tmp_path: Path) -> None:
    app = Flask(__name__)
    app.register_blueprint(reporting_health.health_bp)

    reports_dir = tmp_path / "reports"
    orders_db = reports_dir / "shadow_runtime" / "orders.sqlite"
    orders_db.parent.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    health_paths = reporting_health.HealthPaths(
        orders_db=orders_db,
        fills_db=reports_dir / "shadow_runtime" / "fills.sqlite",
        positions_db=reports_dir / "shadow_runtime" / "positions.sqlite",
        kill_switch_path=reports_dir / "shadow_runtime" / "kill_switch.json",
        reconcile_status_path=reports_dir / "shadow_runtime" / "reconcile_status.json",
        auto_risk_guard_path=reports_dir / "shadow_runtime" / "auto_risk_guard.json",
        auto_risk_eval_path=reports_dir / "shadow_runtime" / "auto_risk_eval.json",
    )

    health_paths.positions_db.parent.mkdir(parents=True, exist_ok=True)
    health_paths.kill_switch_path.write_text("{}", encoding="utf-8")
    health_paths.reconcile_status_path.write_text('{"ok": true}', encoding="utf-8")
    health_paths.auto_risk_guard_path.write_text(
        '{"current_level":"PROTECT","metrics":{"last_dd_pct":0.25}}',
        encoding="utf-8",
    )

    def fake_resolve_health_paths():
        return health_paths

    def fake_check_runtime_positions_db(_path: Path) -> None:
        return None

    def fake_load_last_trade_ts_ms() -> int:
        return 0

    monkeypatch.setattr(reporting_health, "_resolve_health_paths", fake_resolve_health_paths)
    monkeypatch.setattr(reporting_health, "_check_runtime_positions_db", fake_check_runtime_positions_db)
    monkeypatch.setattr(reporting_health, "_load_last_trade_ts_ms", fake_load_last_trade_ts_ms)

    client = app.test_client()
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["checks"]["risk_guard"]["level"] == "PROTECT"
    assert payload["checks"]["risk_guard"]["drawdown"] == 0.25


def test_health_check_suppresses_stale_last_trade_when_risk_guard_protect(monkeypatch, tmp_path: Path) -> None:
    app = Flask(__name__)
    app.register_blueprint(reporting_health.health_bp)

    reports_dir = tmp_path / "reports"
    orders_db = reports_dir / "shadow_runtime" / "orders.sqlite"
    orders_db.parent.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    health_paths = reporting_health.HealthPaths(
        orders_db=orders_db,
        fills_db=reports_dir / "shadow_runtime" / "fills.sqlite",
        positions_db=reports_dir / "shadow_runtime" / "positions.sqlite",
        kill_switch_path=reports_dir / "shadow_runtime" / "kill_switch.json",
        reconcile_status_path=reports_dir / "shadow_runtime" / "reconcile_status.json",
        auto_risk_guard_path=reports_dir / "shadow_runtime" / "auto_risk_guard.json",
        auto_risk_eval_path=reports_dir / "shadow_runtime" / "auto_risk_eval.json",
    )

    health_paths.positions_db.parent.mkdir(parents=True, exist_ok=True)
    health_paths.kill_switch_path.write_text("{}", encoding="utf-8")
    health_paths.reconcile_status_path.write_text('{"ok": true}', encoding="utf-8")
    health_paths.auto_risk_guard_path.write_text('{"current_level":"PROTECT"}', encoding="utf-8")

    now = 1_777_307_118.0
    stale_trade_ts_ms = int((now - 3 * 3600) * 1000)
    monkeypatch.setattr(reporting_health, "_resolve_health_paths", lambda: health_paths)
    monkeypatch.setattr(reporting_health, "_check_runtime_positions_db", lambda _path: None)
    monkeypatch.setattr(reporting_health, "_load_last_trade_ts_ms", lambda: stale_trade_ts_ms)
    monkeypatch.setattr(reporting_health.time, "time", lambda: now)

    response = app.test_client().get("/health")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "healthy"
    assert payload["checks"]["last_trade"]["status"] == "ok"
    assert payload["checks"]["last_trade"]["context"] == "risk_guard_protect"


def test_health_check_keeps_stale_last_trade_warning_outside_protect(monkeypatch, tmp_path: Path) -> None:
    app = Flask(__name__)
    app.register_blueprint(reporting_health.health_bp)

    reports_dir = tmp_path / "reports"
    orders_db = reports_dir / "shadow_runtime" / "orders.sqlite"
    orders_db.parent.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    health_paths = reporting_health.HealthPaths(
        orders_db=orders_db,
        fills_db=reports_dir / "shadow_runtime" / "fills.sqlite",
        positions_db=reports_dir / "shadow_runtime" / "positions.sqlite",
        kill_switch_path=reports_dir / "shadow_runtime" / "kill_switch.json",
        reconcile_status_path=reports_dir / "shadow_runtime" / "reconcile_status.json",
        auto_risk_guard_path=reports_dir / "shadow_runtime" / "auto_risk_guard.json",
        auto_risk_eval_path=reports_dir / "shadow_runtime" / "auto_risk_eval.json",
    )

    health_paths.positions_db.parent.mkdir(parents=True, exist_ok=True)
    health_paths.kill_switch_path.write_text("{}", encoding="utf-8")
    health_paths.reconcile_status_path.write_text('{"ok": true}', encoding="utf-8")
    health_paths.auto_risk_guard_path.write_text('{"current_level":"DEFENSE"}', encoding="utf-8")

    now = 1_777_307_118.0
    stale_trade_ts_ms = int((now - 3 * 3600) * 1000)
    monkeypatch.setattr(reporting_health, "_resolve_health_paths", lambda: health_paths)
    monkeypatch.setattr(reporting_health, "_check_runtime_positions_db", lambda _path: None)
    monkeypatch.setattr(reporting_health, "_load_last_trade_ts_ms", lambda: stale_trade_ts_ms)
    monkeypatch.setattr(reporting_health.time, "time", lambda: now)

    response = app.test_client().get("/health")

    assert response.status_code == 503
    payload = response.get_json()
    assert payload["status"] == "degraded"
    assert payload["checks"]["last_trade"]["status"] == "warning"
    assert "context" not in payload["checks"]["last_trade"]


def test_health_check_prefers_newer_guard_state_over_stale_eval(monkeypatch, tmp_path: Path) -> None:
    app = Flask(__name__)
    app.register_blueprint(reporting_health.health_bp)

    reports_dir = tmp_path / "reports"
    orders_db = reports_dir / "shadow_runtime" / "orders.sqlite"
    orders_db.parent.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    health_paths = reporting_health.HealthPaths(
        orders_db=orders_db,
        fills_db=reports_dir / "shadow_runtime" / "fills.sqlite",
        positions_db=reports_dir / "shadow_runtime" / "positions.sqlite",
        kill_switch_path=reports_dir / "shadow_runtime" / "kill_switch.json",
        reconcile_status_path=reports_dir / "shadow_runtime" / "reconcile_status.json",
        auto_risk_guard_path=reports_dir / "shadow_runtime" / "auto_risk_guard.json",
        auto_risk_eval_path=reports_dir / "shadow_runtime" / "auto_risk_eval.json",
    )

    health_paths.positions_db.parent.mkdir(parents=True, exist_ok=True)
    health_paths.kill_switch_path.write_text("{}", encoding="utf-8")
    health_paths.reconcile_status_path.write_text('{"ok": true}', encoding="utf-8")
    health_paths.auto_risk_eval_path.write_text(
        '{"ts":"2026-04-19T13:00:00","current_level":"PROTECT","metrics":{"dd_pct":0.25}}',
        encoding="utf-8",
    )
    health_paths.auto_risk_guard_path.write_text(
        '{"current_level":"DEFENSE","metrics":{"last_dd_pct":0.12},"last_update":"2026-04-19T14:05:00"}',
        encoding="utf-8",
    )

    monkeypatch.setattr(reporting_health, "_resolve_health_paths", lambda: health_paths)
    monkeypatch.setattr(reporting_health, "_check_runtime_positions_db", lambda _path: None)
    monkeypatch.setattr(reporting_health, "_load_last_trade_ts_ms", lambda: 0)

    client = app.test_client()
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["checks"]["risk_guard"]["level"] == "DEFENSE"
    assert payload["checks"]["risk_guard"]["drawdown"] == 0.12


def test_health_check_prefers_latest_history_ts_when_eval_history_is_unsorted(monkeypatch, tmp_path: Path) -> None:
    app = Flask(__name__)
    app.register_blueprint(reporting_health.health_bp)

    reports_dir = tmp_path / "reports"
    orders_db = reports_dir / "shadow_runtime" / "orders.sqlite"
    orders_db.parent.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    health_paths = reporting_health.HealthPaths(
        orders_db=orders_db,
        fills_db=reports_dir / "shadow_runtime" / "fills.sqlite",
        positions_db=reports_dir / "shadow_runtime" / "positions.sqlite",
        kill_switch_path=reports_dir / "shadow_runtime" / "kill_switch.json",
        reconcile_status_path=reports_dir / "shadow_runtime" / "reconcile_status.json",
        auto_risk_guard_path=reports_dir / "shadow_runtime" / "auto_risk_guard.json",
        auto_risk_eval_path=reports_dir / "shadow_runtime" / "auto_risk_eval.json",
    )

    health_paths.positions_db.parent.mkdir(parents=True, exist_ok=True)
    health_paths.kill_switch_path.write_text("{}", encoding="utf-8")
    health_paths.reconcile_status_path.write_text('{"ok": true}', encoding="utf-8")
    health_paths.auto_risk_eval_path.write_text(
        '{"current_level":"PROTECT","metrics":{"dd_pct":0.25},"history":[{"ts":"2026-04-19T14:05:00","to":"PROTECT"},{"ts":"2026-04-19T13:00:00","to":"DEFENSE"}]}',
        encoding="utf-8",
    )

    monkeypatch.setattr(reporting_health, "_resolve_health_paths", lambda: health_paths)
    monkeypatch.setattr(reporting_health, "_check_runtime_positions_db", lambda _path: None)
    monkeypatch.setattr(reporting_health, "_load_last_trade_ts_ms", lambda: 0)

    client = app.test_client()
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["checks"]["risk_guard"]["level"] == "PROTECT"
    assert payload["checks"]["risk_guard"]["drawdown"] == 0.25


def test_health_check_accepts_legacy_guard_level_field(monkeypatch, tmp_path: Path) -> None:
    app = Flask(__name__)
    app.register_blueprint(reporting_health.health_bp)

    reports_dir = tmp_path / "reports"
    orders_db = reports_dir / "shadow_runtime" / "orders.sqlite"
    orders_db.parent.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    health_paths = reporting_health.HealthPaths(
        orders_db=orders_db,
        fills_db=reports_dir / "shadow_runtime" / "fills.sqlite",
        positions_db=reports_dir / "shadow_runtime" / "positions.sqlite",
        kill_switch_path=reports_dir / "shadow_runtime" / "kill_switch.json",
        reconcile_status_path=reports_dir / "shadow_runtime" / "reconcile_status.json",
        auto_risk_guard_path=reports_dir / "shadow_runtime" / "auto_risk_guard.json",
        auto_risk_eval_path=reports_dir / "shadow_runtime" / "auto_risk_eval.json",
    )

    health_paths.positions_db.parent.mkdir(parents=True, exist_ok=True)
    health_paths.kill_switch_path.write_text("{}", encoding="utf-8")
    health_paths.reconcile_status_path.write_text('{"ok": true}', encoding="utf-8")
    health_paths.auto_risk_guard_path.write_text(
        '{"level":"PROTECT","metrics":{"last_dd_pct":0.25},"last_update":"2026-04-19T14:05:00"}',
        encoding="utf-8",
    )

    monkeypatch.setattr(reporting_health, "_resolve_health_paths", lambda: health_paths)
    monkeypatch.setattr(reporting_health, "_check_runtime_positions_db", lambda _path: None)
    monkeypatch.setattr(reporting_health, "_load_last_trade_ts_ms", lambda: 0)

    client = app.test_client()
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["checks"]["risk_guard"]["level"] == "PROTECT"
    assert payload["checks"]["risk_guard"]["drawdown"] == 0.25


def test_health_check_marks_unknown_risk_guard_as_warning(monkeypatch, tmp_path: Path) -> None:
    app = Flask(__name__)
    app.register_blueprint(reporting_health.health_bp)

    reports_dir = tmp_path / "reports"
    orders_db = reports_dir / "shadow_runtime" / "orders.sqlite"
    orders_db.parent.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    health_paths = reporting_health.HealthPaths(
        orders_db=orders_db,
        fills_db=reports_dir / "shadow_runtime" / "fills.sqlite",
        positions_db=reports_dir / "shadow_runtime" / "positions.sqlite",
        kill_switch_path=reports_dir / "shadow_runtime" / "kill_switch.json",
        reconcile_status_path=reports_dir / "shadow_runtime" / "reconcile_status.json",
        auto_risk_guard_path=reports_dir / "shadow_runtime" / "auto_risk_guard.json",
        auto_risk_eval_path=reports_dir / "shadow_runtime" / "auto_risk_eval.json",
    )

    health_paths.positions_db.parent.mkdir(parents=True, exist_ok=True)
    health_paths.kill_switch_path.write_text("{}", encoding="utf-8")
    health_paths.reconcile_status_path.write_text('{"ok": true}', encoding="utf-8")

    monkeypatch.setattr(reporting_health, "_resolve_health_paths", lambda: health_paths)
    monkeypatch.setattr(reporting_health, "_check_runtime_positions_db", lambda _path: None)
    monkeypatch.setattr(reporting_health, "_load_last_trade_ts_ms", lambda: 0)

    client = app.test_client()
    response = client.get("/health")

    assert response.status_code == 503
    payload = response.get_json()
    assert payload["status"] == "degraded"
    assert payload["checks"]["risk_guard"]["status"] == "warning"
    assert payload["checks"]["risk_guard"]["level"] == "UNKNOWN"
