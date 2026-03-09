import json
import sqlite3

import pytest

from scripts.backfill_regime_history_rss import backfill_regime_history_rss


def test_backfill_regime_history_rss_updates_recent_rows(tmp_path):
    reports = tmp_path / "reports"
    reports.mkdir()
    cache_dir = tmp_path / "data" / "sentiment_cache"
    cache_dir.mkdir(parents=True)

    db_path = reports / "regime_history.db"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE regime_history (
            id INTEGER PRIMARY KEY,
            ts_ms INTEGER NOT NULL,
            rss_state TEXT,
            rss_confidence REAL,
            rss_sentiment REAL
        )
        """
    )
    cur.execute(
        "INSERT INTO regime_history (ts_ms, rss_state, rss_confidence, rss_sentiment) VALUES (?, ?, ?, ?)",
        (1773038005000, "RISK_OFF", 0.75, -0.3),
    )
    conn.commit()
    conn.close()

    cache_payload = {
        "f6_sentiment_confidence": 0.7,
        "collected_at": "2026-03-09T14:30:17",
    }
    (cache_dir / "rss_MARKET_20260309_14.json").write_text(
        json.dumps(cache_payload),
        encoding="utf-8",
    )

    result = backfill_regime_history_rss(db_path, cache_dir, hours=24)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT rss_state, rss_confidence FROM regime_history")
    row = cur.fetchone()
    conn.close()

    assert result["rows_updated"] == 1
    assert row[0] == "RISK_OFF"
    assert row[1] == pytest.approx(0.35)
