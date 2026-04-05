from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import scripts.dust_cleaner as dust_cleaner


def test_dust_cleaner_uses_repo_reports_dir_and_marks_dust(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    fake_reports = fake_root / "reports"
    fake_reports.mkdir(parents=True)

    positions_db = fake_reports / "positions.sqlite"
    conn = sqlite3.connect(str(positions_db))
    try:
        conn.execute(
            """
            CREATE TABLE positions (
              symbol TEXT PRIMARY KEY,
              qty REAL NOT NULL,
              avg_px REAL NOT NULL,
              last_mark_px REAL NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT INTO positions(symbol, qty, avg_px, last_mark_px) VALUES (?, ?, ?, ?)",
            ("PEPE/USDT", 1000.0, 0.000004, 0.000004),
        )
        conn.execute(
            "INSERT INTO positions(symbol, qty, avg_px, last_mark_px) VALUES (?, ?, ?, ?)",
            ("BTC/USDT", 0.5, 60000.0, 60000.0),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(dust_cleaner, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(dust_cleaner, "REPORTS_DIR", fake_reports)
    monkeypatch.setattr(dust_cleaner, "POSITIONS_DB", positions_db)
    monkeypatch.setattr(dust_cleaner, "ORDERS_DB", fake_reports / "orders.sqlite")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["dust_cleaner.py"])

    dust_cleaner.main()

    dust_config = fake_reports / "dust_config.json"
    reports = list(fake_reports.glob("dust_cleanup_*.json"))
    assert dust_config.exists()
    assert len(reports) == 1
    assert not (tmp_path / "reports" / "dust_config.json").exists()
    assert not list((tmp_path / "reports").glob("dust_cleanup_*.json")) if (tmp_path / "reports").exists() else True

    payload = json.loads(reports[0].read_text(encoding="utf-8"))
    assert payload["stats"]["marked"] == 1
    assert payload["marked_positions"][0]["symbol"] == "PEPE/USDT"

    conn = sqlite3.connect(str(positions_db))
    try:
        tags_json = conn.execute("SELECT tags_json FROM positions WHERE symbol = ?", ("PEPE/USDT",)).fetchone()[0]
    finally:
        conn.close()
    assert json.loads(tags_json)["dust"] is True
