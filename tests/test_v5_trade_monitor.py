from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import scripts.v5_trade_monitor as trade_monitor


def test_build_paths_uses_prefixed_runtime_alert_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        trade_monitor,
        "_load_active_config",
        lambda project_root: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )
    monkeypatch.setattr(
        trade_monitor,
        "_resolve_runtime_path",
        lambda raw_path, default, project_root: (project_root / (raw_path or default)).resolve(),
    )
    monkeypatch.setattr(
        trade_monitor,
        "resolve_runtime_env_path",
        lambda project_root=None: str((tmp_path / ".env").resolve()),
    )

    paths = trade_monitor.build_paths(tmp_path)

    assert paths.orders_db_path == (tmp_path / "reports" / "shadow_orders.sqlite").resolve()
    assert paths.alert_file == (tmp_path / "reports" / "shadow_monitor_alert.txt").resolve()


def test_build_paths_uses_suffixed_runtime_alert_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        trade_monitor,
        "_load_active_config",
        lambda project_root: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}},
    )
    monkeypatch.setattr(
        trade_monitor,
        "_resolve_runtime_path",
        lambda raw_path, default, project_root: (project_root / (raw_path or default)).resolve(),
    )
    monkeypatch.setattr(
        trade_monitor,
        "resolve_runtime_env_path",
        lambda project_root=None: str((tmp_path / ".env").resolve()),
    )

    paths = trade_monitor.build_paths(tmp_path)

    assert paths.orders_db_path == (tmp_path / "reports" / "orders_accelerated.sqlite").resolve()
    assert paths.alert_file == (tmp_path / "reports" / "monitor_alert_accelerated.txt").resolve()


def test_shell_wrapper_delegates_to_python_monitor(tmp_path: Path) -> None:
    import os
    import subprocess

    project_root = tmp_path
    scripts_dir = project_root / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    wrapper_src = Path(__file__).resolve().parents[1] / "scripts" / "v5_trade_monitor.sh"
    wrapper_dst = scripts_dir / "v5_trade_monitor.sh"
    wrapper_dst.write_text(wrapper_src.read_text(encoding="utf-8"), encoding="utf-8")
    wrapper_dst.chmod(0o755)

    fake_python = project_root / "fake_python.sh"
    args_log = project_root / "args.log"
    fake_python.write_text(
        "#!/bin/bash\n"
        "printf '%s\\n' \"$@\" > \"$ARGS_LOG\"\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)

    env = {
        **os.environ,
        "V5_PYTHON_BIN": str(fake_python),
        "ARGS_LOG": str(args_log),
    }
    subprocess.run(
        ["/bin/bash", str(wrapper_dst)],
        cwd=project_root,
        env=env,
        check=True,
    )

    args = args_log.read_text(encoding="utf-8").splitlines()
    assert args[0] == str(project_root / "scripts" / "v5_trade_monitor.py")
    assert args[1] == "--silent"


def test_send_telegram_alert_reports_runtime_alert_path(monkeypatch, tmp_path: Path, capsys) -> None:
    paths = trade_monitor.MonitorPaths(
        project_root=tmp_path,
        reports_dir=(tmp_path / "reports").resolve(),
        logs_dir=(tmp_path / "logs").resolve(),
        fills_db_path=(tmp_path / "reports" / "fills.sqlite").resolve(),
        orders_db_path=(tmp_path / "reports" / "shadow_orders.sqlite").resolve(),
        env_path=(tmp_path / ".env").resolve(),
        alert_file=(tmp_path / "reports" / "shadow_monitor_alert.txt").resolve(),
    )

    monkeypatch.setattr(trade_monitor, "_load_telegram_settings", lambda paths: (None, None))

    assert trade_monitor.send_telegram_alert("test-message", paths=paths) is True

    output = capsys.readouterr().out
    assert str(paths.alert_file) in output


def test_get_current_risk_level_prefers_newer_guard_state(monkeypatch, tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    paths = trade_monitor.MonitorPaths(
        project_root=tmp_path,
        reports_dir=reports_dir.resolve(),
        logs_dir=(tmp_path / "logs").resolve(),
        fills_db_path=(reports_dir / "fills.sqlite").resolve(),
        orders_db_path=(reports_dir / "shadow_orders.sqlite").resolve(),
        env_path=(tmp_path / ".env").resolve(),
        alert_file=(reports_dir / "shadow_monitor_alert.txt").resolve(),
    )
    eval_path = reports_dir / "shadow_auto_risk_eval.json"
    guard_path = reports_dir / "shadow_auto_risk_guard.json"
    eval_path.write_text(
        json.dumps({"ts": "2026-04-19T13:00:00", "current_level": "PROTECT"}),
        encoding="utf-8",
    )
    guard_path.write_text(
        json.dumps({"current_level": "DEFENSE", "last_update": "2026-04-19T14:05:00"}),
        encoding="utf-8",
    )

    level = trade_monitor.get_current_risk_level(paths)

    assert level == "DEFENSE"


def test_check_and_alert_suppresses_no_trade_alert_when_protect(monkeypatch, tmp_path: Path) -> None:
    paths = trade_monitor.MonitorPaths(
        project_root=tmp_path,
        reports_dir=(tmp_path / "reports").resolve(),
        logs_dir=(tmp_path / "logs").resolve(),
        fills_db_path=(tmp_path / "reports" / "fills.sqlite").resolve(),
        orders_db_path=(tmp_path / "reports" / "orders.sqlite").resolve(),
        env_path=(tmp_path / ".env").resolve(),
        alert_file=(tmp_path / "reports" / "monitor_alert.txt").resolve(),
    )
    sent: list[tuple[str, str]] = []

    monkeypatch.setattr(trade_monitor, "get_current_risk_level", lambda _paths=paths: "PROTECT")
    monkeypatch.setattr(trade_monitor, "get_last_trade_time", lambda _paths=paths, service_unit=None: datetime.now() - timedelta(hours=13))
    monkeypatch.setattr(trade_monitor, "get_recent_trades_count", lambda service_unit=None: (1, 1))
    monkeypatch.setattr(trade_monitor, "get_recent_errors", lambda service_unit=None: [])
    monkeypatch.setattr(trade_monitor, "resolve_live_service_unit_name", lambda: "v5-prod.user.service")
    monkeypatch.setattr(trade_monitor, "send_telegram_alert", lambda message, priority="normal", paths=paths: sent.append((message, priority)) or True)

    alerted = trade_monitor.check_and_alert(paths)

    assert alerted is False
    assert sent == []


def test_resolve_live_service_unit_name_ignores_retired_live_20u(monkeypatch) -> None:
    monkeypatch.setattr(trade_monitor.shutil, "which", lambda _: "/bin/systemctl")
    monkeypatch.setattr(
        trade_monitor,
        "_get_unit_load_state",
        lambda unit: "loaded" if unit == "v5-live-20u.user.service" else "not-found",
    )

    assert trade_monitor.resolve_live_service_unit_name() == "v5-prod.user.service"
