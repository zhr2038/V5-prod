from __future__ import annotations

import os

from configs.schema import RegimeConfig
from src.regime.regime_engine import RegimeEngine


def test_load_market_sentiment_prefers_filename_timestamp_over_mtime(tmp_path) -> None:
    cache_dir = tmp_path / "data" / "sentiment_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    older = cache_dir / "funding_BTC-USDT_20260419_22.json"
    newer = cache_dir / "funding_BTC-USDT_20260419_23.json"
    older.write_text('{"f6_sentiment": -0.4}', encoding="utf-8")
    newer.write_text('{"f6_sentiment": 0.35}', encoding="utf-8")
    os.utime(older, (2_000_000_000, 2_000_000_000))
    os.utime(newer, (1_000_000_000, 1_000_000_000))

    engine = RegimeEngine(RegimeConfig())
    engine.sentiment_cache_dir = cache_dir

    assert engine._load_market_sentiment() == 0.35
