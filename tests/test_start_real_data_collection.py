from __future__ import annotations

from pathlib import Path

import scripts.start_real_data_collection as real_data


def test_create_real_trading_config_writes_under_project_root(tmp_path: Path) -> None:
    config_path = real_data.create_real_trading_config(project_root=tmp_path)

    assert config_path == (tmp_path / "configs" / "live_20u_real_data.yaml").resolve()
    assert config_path.exists()
    assert "mode: live" in config_path.read_text(encoding="utf-8")


def test_create_monitoring_script_writes_under_project_root(tmp_path: Path) -> None:
    monitor_path = real_data.create_monitoring_script(project_root=tmp_path)

    assert monitor_path == (tmp_path / "scripts" / "monitor_real_data.py").resolve()
    assert monitor_path.exists()


def test_main_prints_repo_root_main_entrypoint(capsys, monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(real_data, "PROJECT_ROOT", tmp_path)
    real_data.main()
    output = capsys.readouterr().out

    assert "python3 main.py --config configs/live_20u_real_data.yaml --start" in output
    assert "python3 src/main.py" not in output
