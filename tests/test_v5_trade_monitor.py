from __future__ import annotations

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
