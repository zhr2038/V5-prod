import json
import sqlite3

import pytest

from scripts.freeze_paper_evidence import freeze_participation, freeze_review


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_review_freeze_backs_up_committed_boundary_and_is_idempotent(tmp_path):
    root = tmp_path / "review-v3"
    write(root / "manifest.json", {"identity": "review-id"})
    write(root / "latest.json", {"identity": "review-id", "experiment_id": "review-v3"})
    with sqlite3.connect(root / "comparison.sqlite") as con:
        con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        con.execute("CREATE TABLE events(observed_at REAL PRIMARY KEY)")
        con.execute("INSERT INTO meta VALUES('identity','review-id')")
        con.execute("INSERT INTO meta VALUES('checkpoint',?)", (json.dumps({"last_observed": 100}),))
        con.execute("INSERT INTO events VALUES(100)")
    result = freeze_review(root, reason="superseded", successor="daily-v1", frozen_at="now")
    assert result["status"] == "FROZEN" and result["event_count"] == 1
    assert result["history_recalculated"] is False and result["account_state_migrated"] is False
    assert freeze_review(root, reason="superseded", successor="daily-v1") == result
    with sqlite3.connect(root / "retirement-ledger.sqlite") as con:
        assert con.execute("SELECT count(*) FROM events").fetchone() == (1,)


def test_review_freeze_rejects_interrupted_processing(tmp_path):
    root = tmp_path / "review-v3"
    write(root / "manifest.json", {"identity": "review-id"})
    write(root / "latest.json", {"identity": "review-id"})
    with sqlite3.connect(root / "comparison.sqlite") as con:
        con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        con.execute("CREATE TABLE events(observed_at REAL PRIMARY KEY)")
        con.execute("INSERT INTO meta VALUES('identity','review-id')")
        con.execute("INSERT INTO meta VALUES('processing','incomplete')")
        con.execute("INSERT INTO meta VALUES('checkpoint',?)", (json.dumps({"last_observed": 100}),))
        con.execute("INSERT INTO events VALUES(100)")
    with pytest.raises(ValueError, match="interrupted"):
        freeze_review(root, reason="superseded", successor="daily-v1")


def test_participation_freeze_binds_report_worker_and_sqlite_identity(tmp_path):
    ledger = tmp_path / "forward.sqlite"
    with sqlite3.connect(ledger) as con:
        con.execute("CREATE TABLE portfolio(id INTEGER PRIMARY KEY,identity TEXT,state TEXT)")
        con.execute("CREATE TABLE decisions(sequence INTEGER PRIMARY KEY,observed_ts REAL)")
        con.execute("INSERT INTO portfolio VALUES(1,'participation-id','{}')")
        con.execute("INSERT INTO decisions VALUES(1,200)")
    write(tmp_path / "forward.latest.json", {"identity": "participation-id"})
    write(tmp_path / "forward.worker.json", {"identity": "participation-id"})
    result = freeze_participation(ledger, reason="superseded", successor="daily-v1", frozen_at="now")
    assert result["status"] == "FROZEN" and result["event_count"] == 1
    assert result["retirement_backup_sha256"]

    write(tmp_path / "forward.worker.json", {"identity": "other"})
    (tmp_path / "forward.FROZEN.json").unlink()
    (tmp_path / "forward.retirement.sqlite").unlink()
    with pytest.raises(ValueError, match="identity mismatch"):
        freeze_participation(ledger, reason="superseded", successor="daily-v1")
