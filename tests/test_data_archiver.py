from __future__ import annotations

from datetime import datetime

import scripts.data_archiver as data_archiver


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 4, 21, 23, 30, 0)


def test_parse_run_date_handles_archive_suffix() -> None:
    archiver = data_archiver.DataArchiver()

    assert archiver.parse_run_date("20260322_23.tar") == datetime(2026, 3, 22, 23, 0, 0)
    assert archiver.parse_run_date("20260322_23.tar.gz") == datetime(2026, 3, 22, 23, 0, 0)


def test_run_archives_runs_older_than_keep_days_by_exact_timedelta(monkeypatch, tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    archive_dir = reports_dir / "archive"
    run_dir = runs_dir / "20260322_23"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "decision_audit.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(data_archiver, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(data_archiver, "RUNS_DIR", runs_dir)
    monkeypatch.setattr(data_archiver, "ARCHIVE_DIR", archive_dir)
    monkeypatch.setattr(data_archiver, "datetime", _FixedDateTime)

    archiver = data_archiver.DataArchiver()
    stats = archiver.run(dry_run=False)

    assert stats["archived"] == 1
    assert not run_dir.exists()
    assert (archive_dir / "20260322_23.tar.gz").exists()
