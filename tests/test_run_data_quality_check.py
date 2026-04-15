from __future__ import annotations

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
