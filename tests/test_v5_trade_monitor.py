from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pytest

import scripts.v5_trade_monitor as trade_monitor


def _bash_bin() -> str:
    bash = shutil.which("bash")
    if not bash:
        pytest.skip("bash is required for shell wrapper tests")
    return bash


def _bash_path(path: Path) -> str:
    resolved = str(path.resolve())
    if os.name != "nt":
        return resolved
    quoted = shlex.quote(resolved)
    result = subprocess.run(
        [
            _bash_bin(),
            "-lc",
            f"if command -v wslpath >/dev/null 2>&1; then wslpath -u {quoted}; "
            f"elif command -v cygpath >/dev/null 2>&1; then cygpath -u {quoted}; "
            f"else printf '%s\\n' {quoted}; fi",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _chmod_executable(path: Path) -> None:
    path.chmod(0o755)
    if os.name == "nt":
        subprocess.run([_bash_bin(), "-lc", f"chmod +x {shlex.quote(_bash_path(path))}"], check=True)


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


def test_load_active_config_fails_fast_when_runtime_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = tmp_path / "configs" / "missing.yaml"
    monkeypatch.setattr(trade_monitor, "resolve_runtime_config_path", lambda project_root=None: str(missing))

    try:
        trade_monitor._load_active_config(project_root=tmp_path)
    except FileNotFoundError as exc:
        assert str(missing) in str(exc)
    else:
        raise AssertionError("expected FileNotFoundError")


def test_shell_wrapper_delegates_to_python_monitor(tmp_path: Path) -> None:
    project_root = tmp_path
    scripts_dir = project_root / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    wrapper_src = Path(__file__).resolve().parents[1] / "scripts" / "v5_trade_monitor.sh"
    wrapper_dst = scripts_dir / "v5_trade_monitor.sh"
    wrapper_dst.write_text(wrapper_src.read_text(encoding="utf-8").replace("\r\n", "\n"), encoding="utf-8", newline="\n")
    _chmod_executable(wrapper_dst)

    fake_python = project_root / "fake_python.sh"
    args_log = project_root / "args.log"
    fake_python.write_text(
        "#!/bin/bash\n"
        "printf '%s\\n' \"$@\" > \"$ARGS_LOG\"\n",
        encoding="utf-8",
        newline="\n",
    )
    _chmod_executable(fake_python)

    subprocess.run(
        [
            _bash_bin(),
            "-lc",
            " ".join(
                [
                    "env",
                    f"V5_PYTHON_BIN={shlex.quote(_bash_path(fake_python))}",
                    f"ARGS_LOG={shlex.quote(_bash_path(args_log))}",
                    shlex.quote(_bash_path(wrapper_dst)),
                ]
            ),
        ],
        check=True,
    )

    args = args_log.read_text(encoding="utf-8").splitlines()
    assert args[0] == _bash_path(project_root / "scripts" / "v5_trade_monitor.py")
    assert args[1] == "--silent"


def test_get_recent_trades_count_counts_live_completions_when_fill_sync_marker_absent(monkeypatch) -> None:
    journal = "\n".join(
        [
            "Apr 27 15:00:52 qyun flock[1]: 2026-04-27 15:00:52,110 INFO v5 - V5 live run completed",
            "Apr 27 16:00:52 qyun flock[2]: 2026-04-27 16:00:52,104 INFO v5 - V5 live run completed",
        ]
    )
    monkeypatch.setattr(trade_monitor, "run_command", lambda cmd: journal)

    assert trade_monitor.get_recent_trades_count(service_unit="v5-prod.user.service") == (2, 0)


def test_get_recent_trades_count_uses_fill_sync_count_when_available(monkeypatch) -> None:
    journal = "\n".join(
        [
            "Apr 27 15:00:35 qyun flock[1]: FILLS_SYNC new_fills=3 total=10",
            "Apr 27 15:00:52 qyun flock[1]: 2026-04-27 15:00:52,110 INFO v5 - V5 live run completed",
        ]
    )
    monkeypatch.setattr(trade_monitor, "run_command", lambda cmd: journal)

    assert trade_monitor.get_recent_trades_count(service_unit="v5-prod.user.service") == (1, 3)


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


def test_send_telegram_alert_persists_local_file_when_telegram_succeeds(monkeypatch, tmp_path: Path) -> None:
    paths = trade_monitor.MonitorPaths(
        project_root=tmp_path,
        reports_dir=(tmp_path / "reports").resolve(),
        logs_dir=(tmp_path / "logs").resolve(),
        fills_db_path=(tmp_path / "reports" / "fills.sqlite").resolve(),
        orders_db_path=(tmp_path / "reports" / "orders.sqlite").resolve(),
        env_path=(tmp_path / ".env").resolve(),
        alert_file=(tmp_path / "reports" / "monitor_alert.txt").resolve(),
    )

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setattr(trade_monitor.request, "urlopen", lambda req, timeout=10: Response())

    assert trade_monitor.send_telegram_alert("fresh alert", paths=paths) is True
    assert "fresh alert" in paths.alert_file.read_text(encoding="utf-8")


def test_main_silent_exits_zero_when_alert_is_sent(monkeypatch) -> None:
    monkeypatch.setattr(trade_monitor, "check_and_alert", lambda: True)

    assert trade_monitor.main(["--silent"]) == 0


def test_main_fail_on_alert_keeps_nonzero_exit(monkeypatch) -> None:
    monkeypatch.setattr(trade_monitor, "check_and_alert", lambda: True)

    assert trade_monitor.main(["--fail-on-alert"]) == 1


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


def test_get_current_risk_level_accepts_legacy_guard_level_field(tmp_path: Path) -> None:
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
    guard_path = reports_dir / "shadow_auto_risk_guard.json"
    guard_path.write_text(
        json.dumps({"level": "PROTECT", "last_update": "2026-04-19T14:05:00"}),
        encoding="utf-8",
    )

    level = trade_monitor.get_current_risk_level(paths)

    assert level == "PROTECT"


def test_get_current_risk_level_prefers_latest_eval_history_ts_when_history_is_unsorted(tmp_path: Path) -> None:
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
        json.dumps(
            {
                "current_level": "PROTECT",
                "history": [
                    {"ts": "2026-04-19T15:05:00", "to": "PROTECT"},
                    {"ts": "2026-04-19T13:00:00", "to": "DEFENSE"},
                ],
            }
        ),
        encoding="utf-8",
    )
    guard_path.write_text(
        json.dumps({"current_level": "DEFENSE", "last_update": "2026-04-19T14:05:00"}),
        encoding="utf-8",
    )

    level = trade_monitor.get_current_risk_level(paths)

    assert level == "PROTECT"


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


def test_check_and_alert_suppresses_missing_trade_time_warning_when_protect(monkeypatch, tmp_path: Path) -> None:
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
    monkeypatch.setattr(trade_monitor, "get_last_trade_time", lambda _paths=paths, service_unit=None: None)
    monkeypatch.setattr(trade_monitor, "get_recent_trades_count", lambda service_unit=None: (1, 1))
    monkeypatch.setattr(trade_monitor, "get_recent_errors", lambda service_unit=None: [])
    monkeypatch.setattr(trade_monitor, "resolve_live_service_unit_name", lambda: "v5-prod.user.service")
    monkeypatch.setattr(trade_monitor, "send_telegram_alert", lambda message, priority="normal", paths=paths: sent.append((message, priority)) or True)

    alerted = trade_monitor.check_and_alert(paths)

    assert alerted is False
    assert sent == []


def test_check_and_alert_suppresses_zero_fill_info_when_protect(monkeypatch, tmp_path: Path) -> None:
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
    monkeypatch.setattr(trade_monitor, "get_last_trade_time", lambda _paths=paths, service_unit=None: None)
    monkeypatch.setattr(trade_monitor, "get_recent_trades_count", lambda service_unit=None: (6, 0))
    monkeypatch.setattr(trade_monitor, "get_recent_errors", lambda service_unit=None: [])
    monkeypatch.setattr(trade_monitor, "resolve_live_service_unit_name", lambda: "v5-prod.user.service")
    monkeypatch.setattr(trade_monitor, "send_telegram_alert", lambda message, priority="normal", paths=paths: sent.append((message, priority)) or True)

    alerted = trade_monitor.check_and_alert(paths)

    assert alerted is False
    assert sent == []


def test_check_and_alert_does_not_alert_for_risk_off_context_only(monkeypatch, tmp_path: Path) -> None:
    paths = trade_monitor.MonitorPaths(
        project_root=tmp_path,
        reports_dir=(tmp_path / "reports").resolve(),
        logs_dir=(tmp_path / "logs").resolve(),
        fills_db_path=(tmp_path / "reports" / "fills.sqlite").resolve(),
        orders_db_path=(tmp_path / "reports" / "orders.sqlite").resolve(),
        env_path=(tmp_path / ".env").resolve(),
        alert_file=(tmp_path / "reports" / "monitor_alert.txt").resolve(),
    )
    paths.reports_dir.mkdir(parents=True)
    (paths.reports_dir / "regime.json").write_text(
        json.dumps({"state": "Risk-Off"}),
        encoding="utf-8",
    )
    sent: list[tuple[str, str]] = []

    monkeypatch.setattr(trade_monitor, "get_current_risk_level", lambda _paths=paths: "PROTECT")
    monkeypatch.setattr(trade_monitor, "get_last_trade_time", lambda _paths=paths, service_unit=None: None)
    monkeypatch.setattr(trade_monitor, "get_recent_trades_count", lambda service_unit=None: (6, 0))
    monkeypatch.setattr(trade_monitor, "get_recent_errors", lambda service_unit=None: [])
    monkeypatch.setattr(trade_monitor, "resolve_live_service_unit_name", lambda: "v5-prod.user.service")
    monkeypatch.setattr(trade_monitor, "send_telegram_alert", lambda message, priority="normal", paths=paths: sent.append((message, priority)) or True)

    alerted = trade_monitor.check_and_alert(paths)

    assert alerted is False
    assert sent == []


def test_check_and_alert_clears_stale_alert_file_when_ok(monkeypatch, tmp_path: Path) -> None:
    paths = trade_monitor.MonitorPaths(
        project_root=tmp_path,
        reports_dir=(tmp_path / "reports").resolve(),
        logs_dir=(tmp_path / "logs").resolve(),
        fills_db_path=(tmp_path / "reports" / "fills.sqlite").resolve(),
        orders_db_path=(tmp_path / "reports" / "orders.sqlite").resolve(),
        env_path=(tmp_path / ".env").resolve(),
        alert_file=(tmp_path / "reports" / "monitor_alert.txt").resolve(),
    )
    paths.alert_file.parent.mkdir(parents=True, exist_ok=True)
    paths.alert_file.write_text("old alert", encoding="utf-8")

    monkeypatch.setattr(trade_monitor, "get_current_risk_level", lambda _paths=paths: "PROTECT")
    monkeypatch.setattr(trade_monitor, "get_last_trade_time", lambda _paths=paths, service_unit=None: None)
    monkeypatch.setattr(trade_monitor, "get_recent_trades_count", lambda service_unit=None: (6, 0))
    monkeypatch.setattr(trade_monitor, "get_recent_errors", lambda service_unit=None: [])
    monkeypatch.setattr(trade_monitor, "resolve_live_service_unit_name", lambda: "v5-prod.user.service")
    monkeypatch.setattr(
        trade_monitor,
        "send_telegram_alert",
        lambda message, priority="normal", paths=paths: (_ for _ in ()).throw(AssertionError("unexpected alert")),
    )

    alerted = trade_monitor.check_and_alert(paths)

    assert alerted is False
    assert not paths.alert_file.exists()


def test_check_and_alert_keeps_zero_fill_info_outside_protect(monkeypatch, tmp_path: Path) -> None:
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

    monkeypatch.setattr(trade_monitor, "get_current_risk_level", lambda _paths=paths: "DEFENSE")
    monkeypatch.setattr(trade_monitor, "get_last_trade_time", lambda _paths=paths, service_unit=None: datetime.now())
    monkeypatch.setattr(trade_monitor, "get_recent_trades_count", lambda service_unit=None: (6, 0))
    monkeypatch.setattr(trade_monitor, "get_recent_errors", lambda service_unit=None: [])
    monkeypatch.setattr(trade_monitor, "resolve_live_service_unit_name", lambda: "v5-prod.user.service")
    monkeypatch.setattr(trade_monitor, "send_telegram_alert", lambda message, priority="normal", paths=paths: sent.append((message, priority)) or True)

    alerted = trade_monitor.check_and_alert(paths)

    assert alerted is True
    assert sent
    assert "zero fills" in sent[0][0]


def test_resolve_live_service_unit_name_ignores_retired_live_20u(monkeypatch) -> None:
    monkeypatch.setattr(trade_monitor.shutil, "which", lambda _: "/bin/systemctl")
    monkeypatch.setattr(
        trade_monitor,
        "_get_unit_load_state",
        lambda unit: "loaded" if unit == "v5-live-20u.user.service" else "not-found",
    )

    assert trade_monitor.resolve_live_service_unit_name() == "v5-prod.user.service"
