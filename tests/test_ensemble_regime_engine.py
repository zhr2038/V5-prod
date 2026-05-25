from __future__ import annotations

import os
import sqlite3
import time

from configs.schema import RegimeConfig
from src.regime.ensemble_regime_engine import EnsembleRegimeEngine


def test_latest_fresh_file_prefers_filename_timestamp_over_mtime(tmp_path) -> None:
    cache_dir = tmp_path / "data" / "sentiment_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    older = cache_dir / "funding_COMPOSITE_20260419_22.json"
    newer = cache_dir / "funding_COMPOSITE_20260419_23.json"
    older.write_text('{"f6_sentiment": 0.1}', encoding="utf-8")
    newer.write_text('{"f6_sentiment": 0.2}', encoding="utf-8")
    os.utime(older, (2_000_000_000, 2_000_000_000))
    os.utime(newer, (2_000_000_000, 2_000_000_000))

    engine = EnsembleRegimeEngine(RegimeConfig())
    engine.sentiment_cache_dir = cache_dir

    latest = engine._latest_fresh_file("funding_COMPOSITE_*.json", max_age_minutes=10_000_000)

    assert latest == newer


def test_latest_fresh_file_uses_filename_timestamp_for_freshness(tmp_path) -> None:
    cache_dir = tmp_path / "data" / "sentiment_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    stale = cache_dir / "funding_COMPOSITE_20200101_00.json"
    stale.write_text('{"f6_sentiment": 0.1}', encoding="utf-8")
    os.utime(stale, (time.time(), time.time()))

    engine = EnsembleRegimeEngine(RegimeConfig())
    engine.sentiment_cache_dir = cache_dir

    latest = engine._latest_fresh_file("funding_COMPOSITE_*.json", max_age_minutes=180)

    assert latest is None


def test_recent_column_values_rejects_unknown_column(tmp_path) -> None:
    cfg = RegimeConfig(regime_history_db_path=str(tmp_path / "regime_history.db"))
    engine = EnsembleRegimeEngine(cfg)

    assert engine._recent_column_values("final_state; DROP TABLE regime_history", 5) == []

    with sqlite3.connect(str(engine.regime_history_db)) as con:
        assert con.execute("SELECT name FROM sqlite_master WHERE name='regime_history'").fetchone() is not None
