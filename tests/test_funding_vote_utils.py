from src.regime.funding_vote_utils import (
    build_funding_vote,
    classify_funding_state,
    summarize_funding_rows,
)


def test_summarize_funding_rows_uses_weights():
    metrics = summarize_funding_rows(
        [
            {"symbol": "AAA", "sentiment": 0.20, "weight": 0.90},
            {"symbol": "BBB", "sentiment": -0.10, "weight": 0.10},
        ]
    )

    assert metrics["sentiment"] == 0.17
    assert metrics["positive_weight_share"] == 0.9
    assert metrics["negative_weight_share"] == 0.1
    assert metrics["strongest_sentiment"] == 0.2


def test_classify_funding_state_uses_breadth_and_extreme_signal():
    breadth_state = classify_funding_state(
        {
            "sentiment": 0.05,
            "positive_weight_share": 0.72,
            "negative_weight_share": 0.28,
            "strongest_sentiment": 0.09,
        },
        trending_threshold=0.10,
        risk_off_threshold=-0.10,
        breadth_threshold=0.68,
        extreme_sentiment_threshold=0.12,
        extreme_breadth_threshold=0.55,
    )
    assert breadth_state == {"state": "TRENDING", "trigger": "breadth"}

    extreme_state = classify_funding_state(
        {
            "sentiment": -0.04,
            "positive_weight_share": 0.18,
            "negative_weight_share": 0.82,
            "strongest_sentiment": -0.18,
        },
        trending_threshold=0.10,
        risk_off_threshold=-0.10,
        breadth_threshold=0.68,
        extreme_sentiment_threshold=0.12,
        extreme_breadth_threshold=0.55,
    )
    assert extreme_state == {"state": "RISK_OFF", "trigger": "extreme_breadth"}


def test_build_funding_vote_preserves_metrics():
    vote = build_funding_vote(
        sentiment=0.04,
        weight=0.4,
        details={"large": {"avg": 0.02, "count": 2}},
        composite=True,
        positive_weight_share=0.70,
        negative_weight_share=0.30,
        strongest_sentiment=0.11,
        max_abs_sentiment=0.11,
        extreme_positive_weight_share=0.0,
        extreme_negative_weight_share=0.0,
        trending_threshold=0.10,
        risk_off_threshold=-0.10,
        breadth_threshold=0.68,
        extreme_sentiment_threshold=0.12,
        extreme_breadth_threshold=0.55,
    )

    assert vote["state"] == "TRENDING"
    assert vote["trigger"] == "breadth"
    assert vote["positive_weight_share"] == 0.7
    assert vote["negative_weight_share"] == 0.3
    assert vote["composite"] is True
