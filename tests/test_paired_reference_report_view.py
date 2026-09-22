import json
import sqlite3

from src.reporting.dashboard_command_center import _paired_reference_paper


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def study(tmp_path):
    directory = "v5-reference-paired-20260922-v1"
    root = tmp_path / "paired_reference_paper" / directory
    report = {
        "schema_version": "v5.paired_reference_paper.v1", "experiment_id": directory,
        "identity": "new-paper-identity", "input_hash": "input-v1",
        "ledger_start_ts": 900, "latest_observed_at": 1_000,
        "paper_only": True, "live_order_effect": "none", "live_execution_eligible": False,
        "automatic_live_scaling": False,
    }
    write(root / "latest.json", report)
    write(root / "manifest.json", {"identity": report["identity"], "ledger_start_ts": 900,
                                   "experiment": {"experiment_id": directory}})
    with sqlite3.connect(root / "comparison.sqlite") as con:
        con.execute("CREATE TABLE meta(key TEXT,value TEXT)")
        con.execute("INSERT INTO meta VALUES('identity',?)", (report["identity"],))
        con.execute("CREATE TABLE events(observed_at REAL,input_hash TEXT,report TEXT)")
        con.execute("INSERT INTO events VALUES(?,?,?)", (1_000, report["input_hash"], json.dumps(report)))
    return root, report


def test_paired_report_requires_exact_committed_evidence(tmp_path):
    root, report = study(tmp_path)
    before = (root / "comparison.sqlite").read_bytes()
    result = _paired_reference_paper(tmp_path, 1_010)
    assert result["status"] == "observed" and result["report"] == report
    assert (root / "comparison.sqlite").read_bytes() == before
    write(root / "latest.json", {**report, "accounts": {"invented_profit": 10}})
    invalid = _paired_reference_paper(tmp_path, 1_010)
    assert invalid["status"] == "invalid" and invalid["report"] is None
    assert invalid["reason"] == "paired_reference_report_not_committed"


def test_paired_stale_and_worker_failure_preserve_historical_report(tmp_path):
    root, report = study(tmp_path)
    assert _paired_reference_paper(tmp_path, 1_181)["status"] == "stale"
    write(root / "worker-status.json", {"ok": False, "detail": "quote unavailable"})
    failed = _paired_reference_paper(tmp_path, 1_010)
    assert failed["status"] == "worker_failed" and failed["report"] == report
    write(root / "worker-status.json", {"ok": True, "identity": "wrong-cohort"})
    assert _paired_reference_paper(tmp_path, 1_010)["status"] == "invalid"


def test_missing_paired_ledger_never_picks_a_different_newer_experiment(tmp_path):
    write(tmp_path / "paired_reference_paper" / "v5-reference-paired-other" / "latest.json", {"profit": 100})
    result = _paired_reference_paper(tmp_path, 1_010)
    assert result["status"] == "missing" and result["report"] is None
    assert result["live_execution_eligible"] is False
