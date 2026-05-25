from __future__ import annotations

import json
from datetime import datetime, timezone

from src.factors import deepseek_sentiment_factor as deepseek_mod
from src.factors import gpt_sentiment_factor as gpt_mod
from src.factors import sentiment_factor as sentiment_mod


def test_sentiment_factor_cache_uses_utc_day_and_accepts_legacy_naive_timestamp(
    monkeypatch,
    tmp_path,
) -> None:
    fixed_now = datetime(2026, 5, 25, 4, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(sentiment_mod, "_utc_now", lambda: fixed_now)
    monkeypatch.setattr(sentiment_mod, "TRANSFORMERS_AVAILABLE", False)

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    (cache_dir / "BTC-USDT_20260525.json").write_text(
        json.dumps(
            {
                "timestamp": "2026-05-25T03:30:00",
                "sentiment": 0.42,
            }
        ),
        encoding="utf-8",
    )

    factor = sentiment_mod.SentimentFactor(cache_dir=str(cache_dir))

    result = factor.calculate("BTC-USDT")
    assert result["f6_sentiment_source"] == "cache"
    assert result["f6_sentiment"] == 0.42


def test_gpt_sentiment_cache_uses_utc_hour(monkeypatch, tmp_path) -> None:
    fixed_now = datetime(2026, 5, 25, 4, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(gpt_mod, "_utc_now", lambda: fixed_now)

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    (cache_dir / "gpt_BTC-USDT_20260525_04.json").write_text(
        json.dumps({"f6_sentiment": 0.2}),
        encoding="utf-8",
    )

    factor = gpt_mod.GPTSentimentFactor(cache_dir=str(cache_dir), api_key="")

    assert factor.calculate("BTC-USDT")["f6_sentiment"] == 0.2


def test_deepseek_sentiment_cache_uses_utc_hour(monkeypatch, tmp_path) -> None:
    fixed_now = datetime(2026, 5, 25, 4, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(deepseek_mod, "_utc_now", lambda: fixed_now)

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    (cache_dir / "deepseek_BTC-USDT_20260525_04.json").write_text(
        json.dumps({"f6_sentiment": 0.3, "f6_sentiment_summary": "cached"}),
        encoding="utf-8",
    )

    factor = deepseek_mod.DeepSeekSentimentFactor(
        cache_dir=str(cache_dir),
        api_key="explicit-key",
        env_path=".env.runtime",
        project_root=tmp_path,
    )

    assert factor.calculate("BTC-USDT")["f6_sentiment"] == 0.3
