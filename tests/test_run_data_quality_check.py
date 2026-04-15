from __future__ import annotations

from pathlib import Path

import scripts.run_data_quality_check as data_quality


def test_build_paths_uses_active_runtime_reports_dir(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        data_quality,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )
    monkeypatch.setattr(
        data_quality,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    paths = data_quality.build_paths(tmp_path)

    assert paths.reports_dir == (tmp_path / "reports" / "shadow_runtime").resolve()
    assert paths.alpha_history_db == (tmp_path / "reports" / "shadow_runtime" / "alpha_history.db").resolve()


def test_build_paths_falls_back_to_root_reports_dir(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        data_quality,
        "load_runtime_config",
        lambda project_root=None: (_ for _ in ()).throw(RuntimeError("config unavailable")),
    )

    paths = data_quality.build_paths(tmp_path)

    assert paths.reports_dir == (tmp_path / "reports").resolve()
    assert paths.alpha_history_db == (tmp_path / "reports" / "alpha_history.db").resolve()
