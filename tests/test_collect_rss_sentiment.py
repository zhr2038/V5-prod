from __future__ import annotations

import json

import scripts.collect_rss_sentiment as rss_mod


def test_collect_rss_sentiment_passes_runtime_env_to_deepseek(monkeypatch, tmp_path):
    captured = {}

    monkeypatch.setattr(
        rss_mod,
        "parse_rss_feed",
        lambda url, max_items=5: [
            {
                "title": "ETF inflows continue",
                "summary": "Market remains constructive",
                "link": url,
                "published": "",
                "source": "example.com",
            }
        ],
    )

    class _FakeFactor:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def analyze_sentiment(self, texts, symbol="MARKET"):
            return {
                "sentiment_score": 0.4,
                "fear_greed_index": 70,
                "summary": "偏乐观",
                "market_stage": "optimistic",
                "confidence": 0.9,
            }

    monkeypatch.setattr(rss_mod, "DeepSeekSentimentFactor", _FakeFactor)

    rss_mod.collect_rss_sentiment(env_path=".env.runtime", project_root=tmp_path)

    assert captured["env_path"] == str((tmp_path / ".env.runtime").resolve())
    assert captured["project_root"] == tmp_path
    cache_dir = tmp_path / "data" / "sentiment_cache"
    market_files = sorted(cache_dir.glob("rss_MARKET_*.json"))
    assert market_files
    payload = json.loads(market_files[-1].read_text(encoding="utf-8"))
    assert payload["f6_sentiment_source"] == "rss_deepseek"


def test_collect_rss_sentiment_main_passes_env(monkeypatch):
    captured = {}

    def _fake_collect_rss_sentiment(*, env_path=".env", project_root=None):
        captured["env_path"] = env_path
        captured["project_root"] = project_root

    monkeypatch.setattr(rss_mod, "collect_rss_sentiment", _fake_collect_rss_sentiment)
    rss_mod.main(["--env", ".env.runtime"])

    assert captured == {"env_path": ".env.runtime", "project_root": None}
