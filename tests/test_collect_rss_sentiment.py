from __future__ import annotations

import json
import xml.etree.ElementTree as stdlib_et
from datetime import datetime, timezone

import scripts.collect_rss_sentiment as rss_mod


def test_collect_rss_sentiment_passes_runtime_env_to_deepseek(monkeypatch, tmp_path):
    captured = {}
    fixed_now = datetime(2026, 5, 25, 4, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(rss_mod, "_utc_now", lambda: fixed_now)

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
    monkeypatch.setattr(rss_mod, "SAFE_XML_ET", object())

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

    assert rss_mod.collect_rss_sentiment(env_path=".env.runtime", project_root=tmp_path) is True

    assert captured["env_path"] == str((tmp_path / ".env.runtime").resolve())
    assert captured["project_root"] == tmp_path
    cache_dir = tmp_path / "data" / "sentiment_cache"
    market_files = sorted(cache_dir.glob("rss_MARKET_*.json"))
    assert market_files
    payload = json.loads(market_files[-1].read_text(encoding="utf-8"))
    assert payload["f6_sentiment_source"] == "rss_deepseek"
    assert market_files[-1].name == "rss_MARKET_20260525_04.json"
    assert payload["collected_at"] == "2026-05-25T04:00:00Z"


def test_collect_rss_sentiment_skips_disabled_sources(monkeypatch, tmp_path):
    requested_urls = []

    def _fake_parse_rss_feed(url, max_items=5):
        requested_urls.append(url)
        return [
            {
                "title": "ETF inflows continue",
                "summary": "Market remains constructive",
                "link": url,
                "published": "",
                "source": "example.com",
            }
        ]

    class _FakeFactor:
        def __init__(self, **kwargs):
            pass

        def analyze_sentiment(self, texts, symbol="MARKET"):
            return {
                "sentiment_score": 0.1,
                "fear_greed_index": 55,
                "summary": "neutral",
                "market_stage": "neutral",
                "confidence": 0.7,
            }

    monkeypatch.setattr(rss_mod, "parse_rss_feed", _fake_parse_rss_feed)
    monkeypatch.setattr(rss_mod, "DeepSeekSentimentFactor", _FakeFactor)
    monkeypatch.setattr(rss_mod, "SAFE_XML_ET", object())

    assert rss_mod.collect_rss_sentiment(env_path=".env.runtime", project_root=tmp_path) is True

    assert requested_urls
    assert all("theblock.co" not in url for url in requested_urls)


def test_collect_rss_sentiment_main_passes_env(monkeypatch):
    captured = {}

    def _fake_collect_rss_sentiment(*, env_path=".env", project_root=None):
        captured["env_path"] = env_path
        captured["project_root"] = project_root
        return True

    monkeypatch.setattr(rss_mod, "collect_rss_sentiment", _fake_collect_rss_sentiment)
    rss_mod.main(["--env", ".env.runtime"])

    assert captured == {"env_path": ".env.runtime", "project_root": None}


def test_collect_rss_sentiment_missing_safe_parser_fails(monkeypatch, tmp_path):
    monkeypatch.setattr(rss_mod, "SAFE_XML_ET", None)

    assert rss_mod.collect_rss_sentiment(project_root=tmp_path) is False


def test_collect_rss_sentiment_main_exits_nonzero_on_failure(monkeypatch):
    monkeypatch.setattr(rss_mod, "collect_rss_sentiment", lambda **kwargs: False)

    try:
        rss_mod.main(["--env", ".env.runtime"])
    except SystemExit as exc:
        assert exc.code == 1
    else:
        raise AssertionError("main should exit nonzero when RSS collection fails")


def test_parse_rss_feed_uses_safe_xml_parser(monkeypatch):
    class _Response:
        content = b"""
        <rss>
          <channel>
            <item>
              <title>BTC rallies</title>
              <description>Constructive market tone</description>
              <link>https://example.com/btc</link>
              <pubDate>Mon, 25 May 2026 00:00:00 GMT</pubDate>
            </item>
          </channel>
        </rss>
        """

        def raise_for_status(self):
            return None

    class _SafeParser:
        called = False

        @staticmethod
        def fromstring(content):
            _SafeParser.called = True
            return stdlib_et.fromstring(content)

    monkeypatch.setattr(rss_mod.requests, "get", lambda *args, **kwargs: _Response())
    monkeypatch.setattr(rss_mod, "SAFE_XML_ET", _SafeParser)

    articles = rss_mod.parse_rss_feed("https://example.com/rss", max_items=5)

    assert _SafeParser.called is True
    assert articles[0]["title"] == "BTC rallies"


def test_parse_rss_feed_fails_closed_when_safe_parser_missing(monkeypatch):
    class _Response:
        content = b"<rss><channel><item><title>BTC</title></item></channel></rss>"

        def raise_for_status(self):
            return None

    monkeypatch.setattr(rss_mod.requests, "get", lambda *args, **kwargs: _Response())
    monkeypatch.setattr(rss_mod, "SAFE_XML_ET", None)

    assert rss_mod.parse_rss_feed("https://example.com/rss", max_items=5) == []


def test_prepare_analysis_input_deduplicates_and_bounds_payload():
    articles = [
        {
            "title": "ETF inflows continue",
            "summary": "x" * 1000,
            "link": "https://example.com/a",
            "source_name": "CoinDesk",
            "source_weight": 1.0,
        },
        {
            "title": "ETF inflows continue duplicate",
            "summary": "duplicate",
            "link": "https://example.com/a",
            "source_name": "Cointelegraph",
            "source_weight": 1.0,
        },
    ]

    texts, article_ids, fingerprint = rss_mod._prepare_analysis_input(articles)

    assert len(texts) == 1
    assert len(article_ids) == 1
    assert len("\n\n".join(texts)) <= rss_mod.MAX_ANALYSIS_CHARS
    assert len(fingerprint) == 64


def test_collect_rss_sentiment_reuses_unchanged_prediction(monkeypatch, tmp_path):
    current_now = [datetime(2026, 8, 26, 4, 0, tzinfo=timezone.utc)]
    calls = []
    monkeypatch.setattr(rss_mod, "_utc_now", lambda: current_now[0])
    monkeypatch.setattr(rss_mod, "SAFE_XML_ET", object())
    monkeypatch.setattr(
        rss_mod,
        "parse_rss_feed",
        lambda url, max_items=5: [
            {
                "title": "ETF inflows continue",
                "summary": "Market remains constructive",
                "link": "https://example.com/article-1",
                "published": "",
                "source": "example.com",
            }
        ],
    )

    class _FakeFactor:
        model = "deepseek-chat"

        def __init__(self, **_kwargs):
            pass

        def analyze_sentiment(self, texts, symbol="MARKET"):
            calls.append((texts, symbol))
            return {
                "ok": True,
                "direction": "up",
                "horizon_hours": 4,
                "up_probability": 0.6,
                "down_probability": 0.2,
                "flat_probability": 0.2,
                "expected_move_bps": 25,
                "sentiment_score": 0.3,
                "fear_greed_index": 62,
                "summary": "资金流偏多",
                "market_stage": "risk_on",
                "confidence": 0.7,
                "usage": {"total_tokens": 400},
                "request_id": "request-1",
            }

    monkeypatch.setattr(rss_mod, "DeepSeekSentimentFactor", _FakeFactor)

    assert rss_mod.collect_rss_sentiment(project_root=tmp_path) is True
    current_now[0] = datetime(2026, 8, 26, 5, 0, tzinfo=timezone.utc)
    assert rss_mod.collect_rss_sentiment(project_root=tmp_path) is True

    assert len(calls) == 1
    latest = json.loads(
        (tmp_path / "data" / "sentiment_cache" / "rss_MARKET_20260826_05.json").read_text(
            encoding="utf-8"
        )
    )
    assert latest["deepseek_status"] == "reused"
    assert latest["analysis_reused"] is True
    assert latest["deepseek_usage"]["total_tokens"] == 0
    assert latest["analysis_deepseek_usage"]["total_tokens"] == 400
    assert latest["analysis_deepseek_request_id"] == "request-1"
    assert latest["analysis_input_chars"] > 0
    assert latest["analysis_generated_at"] == "2026-08-26T04:00:00Z"


def test_collect_rss_sentiment_does_not_publish_failed_prediction(monkeypatch, tmp_path):
    monkeypatch.setattr(rss_mod, "SAFE_XML_ET", object())
    monkeypatch.setattr(
        rss_mod,
        "parse_rss_feed",
        lambda url, max_items=5: [
            {
                "title": "Market update",
                "summary": "No confirmed catalyst",
                "link": url,
                "published": "",
                "source": "example.com",
            }
        ],
    )

    class _FailedFactor:
        model = "deepseek-chat"

        def __init__(self, **_kwargs):
            pass

        def analyze_sentiment(self, _texts, symbol="MARKET"):
            return {
                "ok": False,
                "error_code": "http_402",
                "error": "Insufficient Balance",
            }

    monkeypatch.setattr(rss_mod, "DeepSeekSentimentFactor", _FailedFactor)

    assert rss_mod.collect_rss_sentiment(project_root=tmp_path) is False
    assert not list((tmp_path / "data" / "sentiment_cache").glob("rss_MARKET_*.json"))
