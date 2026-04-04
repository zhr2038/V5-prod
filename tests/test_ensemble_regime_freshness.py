import json
import os
import time

from configs.schema import RegimeConfig
from src.regime.ensemble_regime_engine import EnsembleRegimeEngine


def _write_cache(path, payload, *, age_sec: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    ts = time.time() - age_sec
    os.utime(path, (ts, ts))


def test_funding_vote_ignores_stale_cache(tmp_path):
    engine = EnsembleRegimeEngine(RegimeConfig(funding_signal_max_age_minutes=1))
    engine.sentiment_cache_dir = tmp_path
    _write_cache(
        tmp_path / "funding_COMPOSITE_20260308.json",
        {"f6_sentiment": 0.8},
        age_sec=180,
    )

    vote = engine._get_funding_vote()

    assert vote["state"] is None
    assert vote["weight"] == 0
    assert vote["error"] == "funding_signal_stale_or_missing"


def test_rss_vote_accepts_fresh_cache(tmp_path):
    engine = EnsembleRegimeEngine(RegimeConfig(rss_signal_max_age_minutes=5))
    engine.sentiment_cache_dir = tmp_path
    _write_cache(
        tmp_path / "rss_MARKET_20260308.json",
        {"f6_sentiment": 0.4, "f6_sentiment_summary": "fresh"},
        age_sec=30,
    )

    vote = engine._get_rss_vote()

    assert vote["state"] == "TRENDING"
    assert vote["weight"] == engine.weights["rss"]
    assert 0 < vote["confidence"] < 0.75


def test_runtime_alerts_include_missing_sentiment_sources():
    engine = EnsembleRegimeEngine(RegimeConfig())

    alerts = engine._collect_runtime_alerts(
        {
            "hmm": {"state": "SIDEWAYS", "probs": {"Sideways": 0.6}},
            "funding": {"state": None, "error": "funding_signal_stale_or_missing"},
            "rss": {"state": None, "error": "rss_signal_stale_or_missing"},
        },
        "SIDEWAYS",
    )

    assert "funding_signal_stale_or_missing" in alerts
    assert "rss_signal_stale_or_missing" in alerts


def test_funding_vote_v2_uses_composite_breadth_metrics(tmp_path):
    engine = EnsembleRegimeEngine(
        RegimeConfig(
            funding_trending_threshold=0.10,
            funding_risk_off_threshold=-0.10,
            funding_breadth_threshold=0.68,
            funding_extreme_sentiment_threshold=0.12,
            funding_extreme_breadth_threshold=0.55,
        )
    )
    engine.sentiment_cache_dir = tmp_path
    _write_cache(
        tmp_path / "funding_COMPOSITE_20260308.json",
        {
            "f6_sentiment": 0.05,
            "positive_weight_share": 0.72,
            "negative_weight_share": 0.28,
            "strongest_sentiment": 0.11,
            "max_abs_sentiment": 0.11,
            "tier_breakdown": {"large": {"avg": 0.05, "count": 2}},
        },
        age_sec=10,
    )

    vote = engine._get_funding_vote_v2()

    assert vote["state"] == "TRENDING"
    assert vote["trigger"] == "breadth"
    assert vote["composite"] is True


def test_funding_vote_v2_respects_zero_extreme_sentiment_threshold(tmp_path):
    engine = EnsembleRegimeEngine(
        RegimeConfig(
            funding_trending_threshold=0.10,
            funding_risk_off_threshold=-0.10,
            funding_breadth_threshold=0.90,
            funding_extreme_sentiment_threshold=0.0,
            funding_extreme_breadth_threshold=0.55,
        )
    )
    engine.sentiment_cache_dir = tmp_path
    _write_cache(
        tmp_path / "funding_COMPOSITE_20260308.json",
        {
            "f6_sentiment": 0.01,
            "positive_weight_share": 0.60,
            "negative_weight_share": 0.40,
            "strongest_sentiment": 0.01,
            "max_abs_sentiment": 0.01,
            "tier_breakdown": {"large": {"avg": 0.01, "count": 2}},
        },
        age_sec=10,
    )

    vote = engine._get_funding_vote_v2()

    assert vote["state"] == "TRENDING"
    assert vote["trigger"] == "extreme_breadth"
    assert vote["composite"] is True
