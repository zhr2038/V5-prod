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
