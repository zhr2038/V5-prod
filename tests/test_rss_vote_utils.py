import pytest

from src.regime.rss_vote_utils import build_rss_vote, rss_vote_confidence


def test_rss_vote_confidence_uses_source_confidence():
    assert rss_vote_confidence(-0.3, 0.7) == pytest.approx(0.35)


def test_build_rss_vote_generates_short_summary():
    vote = build_rss_vote(
        {
            "f6_sentiment": -0.3,
            "f6_sentiment_confidence": 0.7,
            "f6_sentiment_summary": "[RSS情报] 加密货币市场情绪偏向谨慎，风险偏好仍然承压。",
        },
        0.25,
    )

    assert vote["state"] == "RISK_OFF"
    assert vote["confidence"] == pytest.approx(0.35)
    assert vote["summary_short"] == "新闻偏空，但未到极端"
