from __future__ import annotations

import re
from typing import Any, Dict


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def rss_vote_state(sentiment: float) -> str:
    sentiment = float(sentiment or 0.0)
    if sentiment > 0.3:
        return "TRENDING"
    if sentiment < -0.2:
        return "RISK_OFF"
    return "SIDEWAYS"


def rss_vote_confidence(sentiment: float, source_confidence: float = 0.7) -> float:
    sentiment = float(sentiment or 0.0)
    source_confidence = clamp(float(source_confidence or 0.7), 0.3, 1.0)
    magnitude = clamp(abs(sentiment) / 0.6, 0.0, 1.0)
    if abs(sentiment) < 0.15:
        magnitude *= 0.4
    return source_confidence * magnitude


def short_rss_summary(summary: str, sentiment: float, state: str) -> str:
    text = re.sub(r"^\[RSS[^\]]*\]\s*", "", str(summary or "")).strip()
    text = re.sub(r"\s+", " ", text)
    state = str(state or "SIDEWAYS").upper()
    if state == "TRENDING":
        if float(sentiment or 0.0) >= 0.45:
            return "\u65b0\u95fb\u504f\u591a\uff0c\u98ce\u9669\u504f\u597d\u660e\u663e\u56de\u5347"
        return "\u65b0\u95fb\u504f\u591a\uff0c\u4f46\u5f3a\u5ea6\u6709\u9650"
    if state == "RISK_OFF":
        if float(sentiment or 0.0) <= -0.45:
            return "\u65b0\u95fb\u504f\u7a7a\uff0c\u907f\u9669\u60c5\u7eea\u5347\u6e29"
        return "\u65b0\u95fb\u504f\u7a7a\uff0c\u4f46\u672a\u5230\u6781\u7aef"

    if any(
        token in text
        for token in (
            "\u98ce\u9669\u504f\u597d",
            "\u504f\u591a",
            "\u4e50\u89c2",
            "\u56de\u5347",
            "\u53cd\u5f39",
            "\u8d70\u5f3a",
        )
    ):
        return "\u65b0\u95fb\u4e2d\u6027\u504f\u591a\uff0c\u60c5\u7eea\u7565\u6709\u56de\u6696"
    if any(
        token in text
        for token in (
            "\u907f\u9669",
            "\u504f\u7a7a",
            "\u627f\u538b",
            "\u8d70\u5f31",
            "\u56de\u843d",
            "\u8c28\u614e",
        )
    ):
        return "\u65b0\u95fb\u4e2d\u6027\u504f\u7a7a\uff0c\u76d8\u4e2d\u4ecd\u504f\u8c28\u614e"
    return text[:24] if text else "\u65b0\u95fb\u4e2d\u6027\uff0c\u65b9\u5411\u6027\u6682\u4e0d\u5f3a"


def build_rss_vote(payload: Dict[str, Any], weight: float) -> Dict[str, Any]:
    sentiment = float(payload.get("f6_sentiment", 0.0) or 0.0)
    source_confidence = float(payload.get("f6_sentiment_confidence", 0.7) or 0.7)
    state = rss_vote_state(sentiment)
    summary = str(payload.get("f6_sentiment_summary", "") or "")[:100]
    return {
        "state": state,
        "confidence": rss_vote_confidence(sentiment, source_confidence),
        "weight": float(weight),
        "sentiment": sentiment,
        "summary": summary,
        "summary_short": short_rss_summary(summary, sentiment, state),
        "source_confidence": clamp(source_confidence, 0.3, 1.0),
        "raw_state": state,
    }
