from __future__ import annotations

from pathlib import Path

import pytest

from scripts.dust_cleaner import DustCleaner


def test_dust_cleaner_manual_reports_dir_uses_prefixed_runtime_files(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "shadow_orders.sqlite").write_text("", encoding="utf-8")

    cleaner = DustCleaner(reports_dir=reports_dir)

    assert cleaner.reports_dir == reports_dir.resolve()
    assert cleaner.orders_db == (reports_dir / "shadow_orders.sqlite").resolve()
    assert cleaner.positions_db == (reports_dir / "shadow_positions.sqlite").resolve()
    assert cleaner.dust_config_path == (reports_dir / "shadow_dust_config.json").resolve()


def test_dust_cleaner_manual_reports_dir_uses_suffixed_runtime_files(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "orders_accelerated.sqlite").write_text("", encoding="utf-8")

    cleaner = DustCleaner(reports_dir=reports_dir)

    assert cleaner.reports_dir == reports_dir.resolve()
    assert cleaner.orders_db == (reports_dir / "orders_accelerated.sqlite").resolve()
    assert cleaner.positions_db == (reports_dir / "positions_accelerated.sqlite").resolve()
    assert cleaner.dust_config_path == (reports_dir / "dust_config_accelerated.json").resolve()


def test_dust_cleaner_default_paths_fail_fast_when_runtime_config_is_empty(monkeypatch, tmp_path: Path) -> None:
    import scripts.dust_cleaner as dust_cleaner

    monkeypatch.setattr(dust_cleaner, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(dust_cleaner, "REPORTS_DIR", (tmp_path / "reports").resolve())
    monkeypatch.setattr(dust_cleaner, "ORDERS_DB", (tmp_path / "reports" / "orders.sqlite").resolve())
    monkeypatch.setattr(dust_cleaner, "POSITIONS_DB", (tmp_path / "reports" / "positions.sqlite").resolve())
    monkeypatch.setattr(dust_cleaner, "load_runtime_config", lambda project_root=None: {})

    with pytest.raises(ValueError, match="live_prod.yaml"):
        dust_cleaner.DustCleaner()
