from __future__ import annotations

import json
from pathlib import Path

import scripts.health_check as health_check


def test_resolve_health_output_path_uses_prefixed_runtime_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        health_check,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )
    monkeypatch.setattr(
        health_check,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )
    monkeypatch.setattr(health_check, "WORKSPACE", tmp_path)
    monkeypatch.setattr(health_check, "REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(health_check, "HEALTH_FILE", (tmp_path / "reports" / "health_status.json"))

    path = health_check._resolve_health_output_path()

    assert path == (tmp_path / "reports" / "shadow_health_status.json").resolve()


def test_resolve_health_output_path_uses_suffixed_runtime_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        health_check,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}},
    )
    monkeypatch.setattr(
        health_check,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )
    monkeypatch.setattr(health_check, "WORKSPACE", tmp_path)
    monkeypatch.setattr(health_check, "REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(health_check, "HEALTH_FILE", (tmp_path / "reports" / "health_status.json"))

    path = health_check._resolve_health_output_path()

    assert path == (tmp_path / "reports" / "health_status_accelerated.json").resolve()


def test_resolve_health_env_path_uses_runtime_env_helper(monkeypatch, tmp_path: Path) -> None:
    expected = (tmp_path / "configs" / "live.env").resolve()
    monkeypatch.setattr(
        health_check,
        "resolve_runtime_env_path",
        lambda project_root=None: str(expected),
    )
    monkeypatch.setattr(health_check, "WORKSPACE", tmp_path)

    path = health_check._resolve_health_env_path()

    assert path == expected


def test_resolve_live_timer_unit_name_ignores_retired_live_20u(monkeypatch) -> None:
    monkeypatch.setattr(health_check.shutil, "which", lambda _: "/bin/systemctl")
    monkeypatch.setattr(
        health_check,
        "_get_unit_load_state",
        lambda unit: "loaded" if unit == "v5-live-20u.user.timer" else "not-found",
    )

    assert health_check.resolve_live_timer_unit_name() == "v5-prod.user.timer"


def test_check_okx_api_warns_with_runtime_env_filename(monkeypatch, tmp_path: Path) -> None:
    expected = (tmp_path / "configs" / "live.env").resolve()
    monkeypatch.setattr(
        health_check,
        "_resolve_health_env_path",
        lambda: expected,
    )
    monkeypatch.delenv("EXCHANGE_API_KEY", raising=False)
    monkeypatch.delenv("OKX_API_KEY", raising=False)
    monkeypatch.delenv("EXCHANGE_API_SECRET", raising=False)
    monkeypatch.delenv("OKX_API_SECRET", raising=False)
    monkeypatch.delenv("EXCHANGE_PASSPHRASE", raising=False)
    monkeypatch.delenv("OKX_API_PASSPHRASE", raising=False)

    result = health_check.HealthChecker().check_okx_api()

    assert result["status"] == "warning"
    assert result["details"] == "API credentials missing in runtime env file: live.env"


def test_check_risk_guard_prefers_newer_guard_state_over_eval(monkeypatch, tmp_path: Path) -> None:
    eval_path = tmp_path / "reports" / "shadow_runtime" / "auto_risk_eval.json"
    guard_path = tmp_path / "reports" / "shadow_runtime" / "auto_risk_guard.json"
    guard_path.parent.mkdir(parents=True, exist_ok=True)
    eval_path.write_text(
        json.dumps({"ts": "2026-04-19T13:00:00", "current_level": "PROTECT", "metrics": {"dd_pct": 0.25}}),
        encoding="utf-8",
    )
    guard_path.write_text(
        json.dumps({"current_level": "DEFENSE", "metrics": {"last_dd_pct": 0.12}, "last_update": "2026-04-19T14:05:00"}),
        encoding="utf-8",
    )

    monkeypatch.setattr(health_check, "_resolve_health_risk_paths", lambda: (eval_path, guard_path))

    result = health_check.HealthChecker().check_risk_guard()

    assert result["status"] == "healthy"
    assert result["details"]["level"] == "DEFENSE"
    assert result["details"]["drawdown"] == 0.12
    assert result["details"]["source"] == "guard"


def test_check_risk_guard_accepts_legacy_guard_level_field(monkeypatch, tmp_path: Path) -> None:
    eval_path = tmp_path / "reports" / "shadow_runtime" / "auto_risk_eval.json"
    guard_path = tmp_path / "reports" / "shadow_runtime" / "auto_risk_guard.json"
    guard_path.parent.mkdir(parents=True, exist_ok=True)
    guard_path.write_text(
        json.dumps({"level": "PROTECT", "metrics": {"last_dd_pct": 0.25}, "last_update": "2026-04-19T14:05:00"}),
        encoding="utf-8",
    )

    monkeypatch.setattr(health_check, "_resolve_health_risk_paths", lambda: (eval_path, guard_path))

    result = health_check.HealthChecker().check_risk_guard()

    assert result["status"] == "healthy"
    assert result["details"]["level"] == "PROTECT"
    assert result["details"]["source"] == "guard"


def test_run_all_checks_promotes_risk_guard_warning_to_overall_warning(monkeypatch) -> None:
    checker = health_check.HealthChecker()
    monkeypatch.setattr(checker, "check_timer_health", lambda: {"name": "timers", "status": "healthy", "details": "ok"})
    monkeypatch.setattr(checker, "check_database_health", lambda: {"name": "database", "status": "healthy", "details": []})
    monkeypatch.setattr(checker, "check_risk_guard", lambda: {"name": "risk_guard", "status": "warning", "details": {"level": "UNKNOWN"}})
    monkeypatch.setattr(checker, "check_okx_api", lambda: {"name": "okx_api", "status": "healthy", "details": {"latency_ms": 1}})
    monkeypatch.setattr(checker, "check_disk_space", lambda: {"name": "disk", "status": "healthy", "details": {}})

    result = checker.run_all_checks()

    assert result["overall_status"] == "warning"
