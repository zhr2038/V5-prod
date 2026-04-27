from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import scripts.health_check as health_check


def test_resolve_health_output_path_uses_prefixed_runtime_file(monkeypatch, tmp_path: Path) -> None:
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("execution:\n  order_store_path: reports/shadow_orders.sqlite\n", encoding="utf-8")
    monkeypatch.setattr(
        health_check,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )
    monkeypatch.setattr(
        health_check,
        "resolve_runtime_config_path",
        lambda project_root=None: str(config_path),
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
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("execution:\n  order_store_path: reports/orders_accelerated.sqlite\n", encoding="utf-8")
    monkeypatch.setattr(
        health_check,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}},
    )
    monkeypatch.setattr(
        health_check,
        "resolve_runtime_config_path",
        lambda project_root=None: str(config_path),
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


def test_load_active_runtime_config_fails_fast_when_config_is_empty(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(health_check, "WORKSPACE", tmp_path)
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(health_check, "load_runtime_config", lambda project_root=None: {})
    monkeypatch.setattr(
        health_check,
        "resolve_runtime_config_path",
        lambda project_root=None: str(config_path),
    )

    try:
        health_check._load_active_runtime_config()
    except ValueError as exc:
        assert "live_prod.yaml" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_load_active_runtime_config_fails_fast_when_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "missing.yaml").resolve()
    monkeypatch.setattr(health_check, "WORKSPACE", tmp_path)
    monkeypatch.setattr(
        health_check,
        "resolve_runtime_config_path",
        lambda project_root=None: str(missing),
    )

    try:
        health_check._load_active_runtime_config()
    except FileNotFoundError as exc:
        assert str(missing) in str(exc)
    else:
        raise AssertionError("expected FileNotFoundError")


def test_resolve_live_timer_unit_name_ignores_retired_live_20u(monkeypatch) -> None:
    monkeypatch.setattr(health_check.shutil, "which", lambda _: "/bin/systemctl")
    monkeypatch.setattr(
        health_check,
        "_get_unit_load_state",
        lambda unit: "loaded" if unit == "v5-live-20u.user.timer" else "not-found",
    )

    assert health_check.resolve_live_timer_unit_name() == "v5-prod.user.timer"


def test_parse_timer_show_output_accepts_systemd_duration_monotonic() -> None:
    props, last_trigger_text, last_trigger_at = health_check.HealthChecker._parse_timer_show_output(
        "\n".join(
            [
                "LoadState=loaded",
                "ActiveState=active",
                "UnitFileState=enabled",
                "LastTriggerUSec=Tue 2026-04-28 00:10:12 CST",
                "LastTriggerUSecMonotonic=2w 6d 9h 28min 54.785116s",
            ]
        )
    )

    assert props["LoadState"] == "loaded"
    assert last_trigger_text == "Tue 2026-04-28 00:10:12 CST"
    assert last_trigger_at == datetime(2026, 4, 28, 0, 10, 12)


def test_check_timer_health_uses_wallclock_trigger_when_monotonic_is_duration(monkeypatch) -> None:
    class Result:
        returncode = 0

        def __init__(self, stdout: str) -> None:
            self.stdout = stdout

    now = datetime.now()
    timer_stdout = "\n".join(
        [
            "LoadState=loaded",
            "ActiveState=active",
            "UnitFileState=enabled",
            f"LastTriggerUSec=Tue {now:%Y-%m-%d %H:%M:%S} CST",
            "LastTriggerUSecMonotonic=2w 6d 9h 28min 54.785116s",
        ]
    )

    monkeypatch.setattr(health_check.shutil, "which", lambda _: "/bin/systemctl")
    monkeypatch.setattr(health_check, "resolve_live_timer_unit_name", lambda: "v5-prod.user.timer")
    monkeypatch.setattr(health_check.subprocess, "run", lambda *args, **kwargs: Result(timer_stdout))

    result = health_check.HealthChecker().check_timer_health()

    assert result["status"] == "healthy"
    assert result["details"] == "all timers healthy"


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


def test_check_okx_api_reports_latency_when_credentials_are_available(monkeypatch, tmp_path: Path) -> None:
    class Response:
        status_code = 200

    times = iter([100.0, 100.123])

    monkeypatch.setattr(health_check, "_resolve_health_env_path", lambda: tmp_path / "missing.env")
    monkeypatch.setenv("EXCHANGE_API_KEY", "key")
    monkeypatch.setenv("EXCHANGE_API_SECRET", "secret")
    monkeypatch.setenv("EXCHANGE_PASSPHRASE", "passphrase")
    monkeypatch.setattr(health_check.time, "time", lambda: next(times))
    monkeypatch.setattr(health_check.requests, "get", lambda url, timeout=10: Response())

    result = health_check.HealthChecker().check_okx_api()

    assert result == {"name": "okx_api", "status": "healthy", "details": {"latency_ms": 123.0}}


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


def test_check_risk_guard_prefers_latest_eval_history_ts_when_history_is_unsorted(monkeypatch, tmp_path: Path) -> None:
    eval_path = tmp_path / "reports" / "shadow_runtime" / "auto_risk_eval.json"
    guard_path = tmp_path / "reports" / "shadow_runtime" / "auto_risk_guard.json"
    guard_path.parent.mkdir(parents=True, exist_ok=True)
    eval_path.write_text(
        json.dumps(
            {
                "current_level": "PROTECT",
                "metrics": {"dd_pct": 0.25},
                "history": [
                    {"ts": "2026-04-19T15:05:00", "to": "PROTECT"},
                    {"ts": "2026-04-19T13:00:00", "to": "DEFENSE"},
                ],
            }
        ),
        encoding="utf-8",
    )
    guard_path.write_text(
        json.dumps({"current_level": "DEFENSE", "metrics": {"last_dd_pct": 0.12}, "last_update": "2026-04-19T14:05:00"}),
        encoding="utf-8",
    )

    monkeypatch.setattr(health_check, "_resolve_health_risk_paths", lambda: (eval_path, guard_path))

    result = health_check.HealthChecker().check_risk_guard()

    assert result["status"] == "healthy"
    assert result["details"]["level"] == "PROTECT"
    assert result["details"]["drawdown"] == 0.25
    assert result["details"]["source"] == "eval"


def test_run_all_checks_promotes_risk_guard_warning_to_overall_warning(monkeypatch) -> None:
    checker = health_check.HealthChecker()
    monkeypatch.setattr(checker, "check_timer_health", lambda: {"name": "timers", "status": "healthy", "details": "ok"})
    monkeypatch.setattr(checker, "check_database_health", lambda: {"name": "database", "status": "healthy", "details": []})
    monkeypatch.setattr(checker, "check_risk_guard", lambda: {"name": "risk_guard", "status": "warning", "details": {"level": "UNKNOWN"}})
    monkeypatch.setattr(checker, "check_okx_api", lambda: {"name": "okx_api", "status": "healthy", "details": {"latency_ms": 1}})
    monkeypatch.setattr(checker, "check_disk_space", lambda: {"name": "disk", "status": "healthy", "details": {}})

    result = checker.run_all_checks()

    assert result["overall_status"] == "warning"
