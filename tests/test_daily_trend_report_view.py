import json
import sqlite3

from src.reporting.dashboard_command_center import _daily_trend_paper


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def study(tmp_path):
    root = tmp_path / "daily_trend_paper"
    directory = root / "v5-daily-trend-paper-20260914-v1"
    identity = "daily-source-v1"
    write(root / "current.json", {"directory": directory.name, "experiment_id": directory.name, "identity": identity})
    write(directory / "manifest.json", {"experiment_id": directory.name, "identity": identity})
    report = {
        "schema_version": "v5.daily_trend_paper.v1",
        "experiment_id": directory.name,
        "identity": identity,
        "observed_at": 1_000,
        "paper_only": True,
        "live_order_effect": "none",
        "live_execution_eligible": False,
        "automatic_live_scaling": False,
    }
    write(directory / "latest.json", report)
    write(directory / "worker-status.json", {"ok": True, "observed_at": 1_000, "identity": identity})
    with sqlite3.connect(directory / "ledger.sqlite") as con:
        con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        con.execute("INSERT INTO meta VALUES('source_identity',?)", (identity,))
    return directory, report


def test_daily_trend_view_requires_pointer_manifest_report_and_ledger_identity(tmp_path):
    directory, report = study(tmp_path)
    result = _daily_trend_paper(tmp_path, 1_100)
    assert result["status"] == "observed"
    assert result["report"] == report
    assert result["live_execution_eligible"] is False

    report["identity"] = "other"
    write(directory / "latest.json", report)
    result = _daily_trend_paper(tmp_path, 1_100)
    assert result["status"] == "invalid" and result["report"] is None


def test_daily_trend_view_preserves_last_report_but_marks_worker_failure_or_freeze(tmp_path):
    directory, _ = study(tmp_path)
    write(
        directory / "worker-status.json",
        {"ok": False, "detail": "public quote unavailable", "identity": "daily-source-v1"},
    )
    failed = _daily_trend_paper(tmp_path, 1_100)
    assert failed["status"] == "worker_failed" and failed["report"] is not None

    write(directory / "FROZEN.json", {"status": "FROZEN", "identity": "daily-source-v1"})
    frozen = _daily_trend_paper(tmp_path, 1_100)
    assert frozen["status"] == "frozen" and frozen["report"] is not None
