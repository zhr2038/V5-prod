from __future__ import annotations

import json

from src.factors.deepseek_sentiment_factor import DeepSeekSentimentFactor


def test_deepseek_sentiment_factor_uses_runtime_env_path(monkeypatch, tmp_path):
    (tmp_path / ".env").write_text("DEEPSEEK_API_KEY=root-key\n", encoding="utf-8")
    (tmp_path / ".env.runtime").write_text("DEEPSEEK_API_KEY=runtime-key\n", encoding="utf-8")
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)

    factor = DeepSeekSentimentFactor(
        cache_dir=str(tmp_path / "cache"),
        env_path=".env.runtime",
        project_root=tmp_path,
    )

    assert factor.api_key == "runtime-key"


def test_deepseek_sentiment_factor_explicit_api_key_wins(monkeypatch, tmp_path):
    (tmp_path / ".env.runtime").write_text("DEEPSEEK_API_KEY=runtime-key\n", encoding="utf-8")
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)

    factor = DeepSeekSentimentFactor(
        cache_dir=str(tmp_path / "cache"),
        api_key="explicit-key",
        env_path=".env.runtime",
        project_root=tmp_path,
    )

    assert factor.api_key == "explicit-key"


def test_deepseek_sentiment_factor_resolves_relative_cache_dir_from_project_root(monkeypatch, tmp_path):
    (tmp_path / ".env.runtime").write_text("DEEPSEEK_API_KEY=runtime-key\n", encoding="utf-8")
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)

    factor = DeepSeekSentimentFactor(
        cache_dir="data/custom_sentiment_cache",
        env_path=".env.runtime",
        project_root=tmp_path,
    )

    assert factor.cache_dir == (tmp_path / "data" / "custom_sentiment_cache").resolve()


def test_deepseek_prompt_targets_bounded_four_hour_prediction(tmp_path):
    factor = DeepSeekSentimentFactor(
        cache_dir=str(tmp_path / "cache"),
        api_key="explicit-key",
        project_root=tmp_path,
    )

    prompt = factor._build_prompt("1|CoinDesk|ETF inflows rise", "MARKET")

    assert "未来4小时" in prompt
    assert '"up_probability"' in prompt
    assert "不要给具体价格或交易建议" in prompt
    assert len(prompt) < 1500


def test_deepseek_call_carries_usage_and_uses_compact_output_budget(monkeypatch, tmp_path):
    captured = {}

    class _Response:
        status_code = 200
        headers = {"x-request-id": "request-1"}
        text = ""

        @staticmethod
        def json():
            return {
                "id": "response-1",
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "direction": "up",
                                    "horizon_hours": 4,
                                    "up_probability": 0.6,
                                    "down_probability": 0.2,
                                    "flat_probability": 0.2,
                                    "expected_move_bps": 35,
                                    "sentiment_score": 0.4,
                                    "confidence": 0.7,
                                    "fear_greed_index": 64,
                                    "market_stage": "risk_on",
                                    "summary": "ETF资金流改善",
                                }
                            )
                        }
                    }
                ],
                "usage": {
                    "prompt_tokens": 321,
                    "completion_tokens": 98,
                    "total_tokens": 419,
                },
            }

    def _post(*_args, **kwargs):
        captured.update(kwargs["json"])
        return _Response()

    monkeypatch.setattr("src.factors.deepseek_sentiment_factor.requests.post", _post)
    factor = DeepSeekSentimentFactor(
        cache_dir=str(tmp_path / "cache"),
        api_key="explicit-key",
        project_root=tmp_path,
    )

    result = factor.analyze_sentiment(["1|CoinDesk|ETF inflows rise"], "MARKET")

    assert result["ok"] is True
    assert result["direction"] == "up"
    assert result["usage"]["total_tokens"] == 419
    assert result["request_id"] == "request-1"
    assert captured["max_tokens"] == 420
    assert captured["temperature"] == 0.1


def test_deepseek_parse_normalizes_probabilities_and_bounds_values(tmp_path):
    factor = DeepSeekSentimentFactor(
        cache_dir=str(tmp_path / "cache"),
        api_key="explicit-key",
        project_root=tmp_path,
    )

    result = factor._parse_response(
        json.dumps(
            {
                "direction": "invalid",
                "up_probability": 8,
                "down_probability": 1,
                "flat_probability": 1,
                "expected_move_bps": 9999,
                "sentiment_score": 4,
                "confidence": 2,
                "fear_greed_index": 300,
                "market_stage": "unknown",
                "summary": "x" * 200,
            }
        )
    )

    assert result["direction"] == "up"
    assert result["sentiment_score"] == 1.0
    assert result["confidence"] == 1.0
    assert result["expected_move_bps"] == 500.0
    assert result["fear_greed_index"] == 100
    assert result["market_stage"] == "neutral"
    assert abs(
        result["up_probability"]
        + result["down_probability"]
        + result["flat_probability"]
        - 1.0
    ) < 0.001
