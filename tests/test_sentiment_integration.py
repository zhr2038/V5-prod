from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.factors.deepseek_sentiment_factor import DeepSeekSentimentFactor


def test_sentiment_integration():
    canned = {
        "BTC-USDT": {
            "sentiment_score": 0.65,
            "confidence": 0.91,
            "summary": "Bullish momentum is building.",
            "key_points": ["Breakout", "Volume expansion", "Positive flow"],
            "fear_greed_index": 78,
            "market_stage": "fomo",
            "source": "deepseek",
        },
        "ETH-USDT": {
            "sentiment_score": 0.10,
            "confidence": 0.72,
            "summary": "Sentiment is mildly constructive.",
            "key_points": ["Stable trend", "Mixed positioning", "Low panic"],
            "fear_greed_index": 56,
            "market_stage": "neutral",
            "source": "deepseek",
        },
        "SOL-USDT": {
            "sentiment_score": -0.35,
            "confidence": 0.84,
            "summary": "Traders are nervous after a pullback.",
            "key_points": ["Pullback", "Fast movers", "Fear selling"],
            "fear_greed_index": 32,
            "market_stage": "panic",
            "source": "deepseek",
        },
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        factor = DeepSeekSentimentFactor(cache_dir=tmpdir)
        factor.analyze_sentiment = lambda texts, symbol: canned[symbol]

        results = {}
        for symbol in ["BTC-USDT", "ETH-USDT", "SOL-USDT"]:
            result = factor.calculate(symbol)
            results[symbol] = result

            assert -1.0 <= result["f6_sentiment"] <= 1.0
            assert 0.0 <= result["f6_sentiment_confidence"] <= 1.0
            assert 0.0 <= result["f6_fear_greed_index"] <= 100.0
            assert result["f6_sentiment_source"] == "deepseek"
            assert result["f6_market_stage"] == canned[symbol]["market_stage"]
            assert isinstance(result["f6_sentiment_key_points"], list)

        avg_sentiment = sum(item["f6_sentiment"] for item in results.values()) / len(results)
        avg_fear_greed = sum(item["f6_fear_greed_index"] for item in results.values()) / len(results)

        assert -1.0 <= avg_sentiment <= 1.0
        assert 0.0 <= avg_fear_greed <= 100.0


def main() -> bool:
    try:
        test_sentiment_integration()
    except Exception as exc:
        print(exc)
        return False
    return True


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
