from __future__ import annotations

import os

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
