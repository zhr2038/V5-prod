import json
import sqlite3
import sys

import pytest

import scripts.backfill_regime_history_rss as rss_script
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


def test_backfill_regime_history_rss_main_uses_runtime_db_from_active_config(monkeypatch, tmp_path):
    fake_root = tmp_path / "repo"
    runtime_dir = fake_root / "reports" / "shadow_runtime"
    cache_dir = fake_root / "data" / "sentiment_cache"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    (fake_root / "configs").mkdir(parents=True, exist_ok=True)
    (fake_root / "configs" / "live_prod.yaml").write_text(
        "\n".join(
            [
                "regime:",
                "  regime_history_db_path: reports/shadow_runtime/regime_history.db",
            ]
        ),
        encoding="utf-8",
    )

    db_path = runtime_dir / "regime_history.db"
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

    (cache_dir / "rss_MARKET_20260309_14.json").write_text(
        json.dumps(
            {
                "f6_sentiment_confidence": 0.7,
                "collected_at": "2026-03-09T14:30:17",
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(rss_script, "PROJECT_ROOT", fake_root)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backfill_regime_history_rss.py",
            "--hours",
            "24",
        ],
    )

    assert rss_script.main() == 0

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT rss_confidence FROM regime_history")
    row = cur.fetchone()
    conn.close()

    assert row[0] == pytest.approx(0.35)
    assert not (fake_root / "reports" / "regime_history.db").exists()


def test_backfill_regime_history_rss_main_respects_explicit_path_overrides(monkeypatch, tmp_path):
    fake_root = tmp_path / "repo"
    runtime_dir = fake_root / "reports" / "shadow_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (fake_root / "configs").mkdir(parents=True, exist_ok=True)
    (fake_root / "configs" / "live_prod.yaml").write_text(
        "\n".join(
            [
                "regime:",
                "  regime_history_db_path: reports/shadow_runtime/regime_history.db",
            ]
        ),
        encoding="utf-8",
    )

    explicit_dir = tmp_path / "custom"
    explicit_cache_dir = explicit_dir / "sentiment_cache"
    explicit_cache_dir.mkdir(parents=True, exist_ok=True)
    explicit_db = explicit_dir / "regime_history.db"

    conn = sqlite3.connect(explicit_db)
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

    (explicit_cache_dir / "rss_MARKET_20260309_14.json").write_text(
        json.dumps(
            {
                "f6_sentiment_confidence": 0.7,
                "collected_at": "2026-03-09T14:30:17",
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(rss_script, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backfill_regime_history_rss.py",
            "--db-path",
            str(explicit_db),
            "--cache-dir",
            str(explicit_cache_dir),
            "--hours",
            "24",
        ],
    )

    assert rss_script.main() == 0

    conn = sqlite3.connect(explicit_db)
    cur = conn.cursor()
    cur.execute("SELECT rss_confidence FROM regime_history")
    row = cur.fetchone()
    conn.close()

    assert row[0] == pytest.approx(0.35)
    assert not (runtime_dir / "regime_history.db").exists()
