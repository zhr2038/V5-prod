from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import scripts.run_data_quality_check as data_quality


def test_build_paths_uses_root_reports_dir_even_when_runtime_orders_are_namespaced(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        data_quality,
        "PROJECT_ROOT",
        tmp_path,
    )
    paths = data_quality.build_paths(tmp_path)

    assert paths.reports_dir == (tmp_path / "reports").resolve()
    assert paths.alpha_history_db == (tmp_path / "reports" / "alpha_history.db").resolve()


def test_help_exits_cleanly_without_touching_database(tmp_path: Path) -> None:
    script = Path(data_quality.__file__).resolve()
    proc = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 0
    assert "Run alpha history data quality checks." in proc.stdout
