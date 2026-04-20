from __future__ import annotations

import os
from pathlib import Path

from src.regime.short_term_override import check_short_term_opportunity


def test_check_short_term_opportunity_prefers_filename_timestamp_over_mtime(tmp_path: Path) -> None:
    cache_dir = tmp_path / "data" / "sentiment_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    older = cache_dir / "funding_COMPOSITE_20260419_22.json"
    newer = cache_dir / "funding_COMPOSITE_20260419_23.json"
    older.write_text('{"f6_sentiment": -0.5}', encoding="utf-8")
    newer.write_text('{"f6_sentiment": 0.35}', encoding="utf-8")
    os.utime(older, (2_000_000_000, 2_000_000_000))
    os.utime(newer, (1_000_000_000, 1_000_000_000))

    result = check_short_term_opportunity(
        alpha_scores={
            "BTC/USDT": 1.2,
            "ETH/USDT": 0.9,
            "SOL/USDT": 0.6,
        },
        btc_change_24h=0.0,
        eth_change_24h=0.0,
        funding_sentiment=None,
        cache_dir=cache_dir,
    )

    assert result.should_override is True
    assert "资金费率乐观" in result.reason
