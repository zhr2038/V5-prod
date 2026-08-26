from src.regime.rss_vote_utils import build_rss_vote


def test_build_rss_vote_keeps_full_summary() -> None:
    summary = (
        "[RSS] Market sentiment is cautious because bitcoin remains range-bound, "
        "ETF outflows persist, options positioning is defensive, and stablecoin "
        "liquidity is improving but not yet strong enough to offset macro risk."
    )

    vote = build_rss_vote(
        {
            "f6_sentiment": -0.24,
            "f6_sentiment_confidence": 0.7,
            "f6_sentiment_summary": summary,
        },
        weight=0.25,
    )

    assert len(summary) > 100
    assert vote["summary"] == summary
    assert vote["summary"].endswith("macro risk.")


def test_build_rss_vote_marks_deepseek_failure_invalid() -> None:
    vote = build_rss_vote(
        {
            "f6_sentiment": 0.0,
            "f6_sentiment_confidence": 0.0,
            "f6_sentiment_summary": "[RSS情报] 无数据",
            "f6_sentiment_source": "rss_deepseek",
            "deepseek_status": "error",
        },
        weight=0.25,
    )

    assert vote["data_valid"] is False
    assert vote["confidence"] == 0.0
    assert vote["source_confidence"] == 0.0
