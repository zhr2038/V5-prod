from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping


DEFAULT_FUNDING_TRENDING_THRESHOLD = 0.10
DEFAULT_FUNDING_RISK_OFF_THRESHOLD = -0.10
DEFAULT_FUNDING_BREADTH_THRESHOLD = 0.68
DEFAULT_FUNDING_EXTREME_SENTIMENT_THRESHOLD = 0.12
DEFAULT_FUNDING_EXTREME_BREADTH_THRESHOLD = 0.55


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def summarize_funding_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    extreme_sentiment_threshold: float = DEFAULT_FUNDING_EXTREME_SENTIMENT_THRESHOLD,
) -> Dict[str, float]:
    entries = []
    total_weight = 0.0
    for row in rows or []:
        sentiment = max(-1.0, min(1.0, _as_float(row.get("sentiment"), 0.0)))
        weight = max(0.0, _as_float(row.get("weight"), 0.0))
        entries.append({"sentiment": sentiment, "weight": weight})
        total_weight += weight

    if not entries:
        return {
            "sentiment": 0.0,
            "positive_weight_share": 0.0,
            "negative_weight_share": 0.0,
            "breadth_bias": 0.0,
            "strongest_sentiment": 0.0,
            "max_abs_sentiment": 0.0,
            "extreme_positive_weight_share": 0.0,
            "extreme_negative_weight_share": 0.0,
            "symbol_count": 0,
        }

    if total_weight <= 0:
        total_weight = float(len(entries))
        for entry in entries:
            entry["weight"] = 1.0

    weighted_sum = 0.0
    positive_weight = 0.0
    negative_weight = 0.0
    extreme_positive_weight = 0.0
    extreme_negative_weight = 0.0
    strongest_sentiment = 0.0
    max_abs_sentiment = 0.0

    for entry in entries:
        sentiment = float(entry["sentiment"])
        norm_weight = float(entry["weight"]) / float(total_weight)
        weighted_sum += sentiment * norm_weight
        if sentiment > 0:
            positive_weight += norm_weight
        elif sentiment < 0:
            negative_weight += norm_weight

        if sentiment >= float(extreme_sentiment_threshold):
            extreme_positive_weight += norm_weight
        elif sentiment <= -float(extreme_sentiment_threshold):
            extreme_negative_weight += norm_weight

        abs_sentiment = abs(sentiment)
        if abs_sentiment > max_abs_sentiment:
            max_abs_sentiment = abs_sentiment
            strongest_sentiment = sentiment

    return {
        "sentiment": round(weighted_sum, 4),
        "positive_weight_share": round(positive_weight, 4),
        "negative_weight_share": round(negative_weight, 4),
        "breadth_bias": round(positive_weight - negative_weight, 4),
        "strongest_sentiment": round(strongest_sentiment, 4),
        "max_abs_sentiment": round(max_abs_sentiment, 4),
        "extreme_positive_weight_share": round(extreme_positive_weight, 4),
        "extreme_negative_weight_share": round(extreme_negative_weight, 4),
        "symbol_count": int(len(entries)),
    }


def classify_funding_state(
    metrics: Mapping[str, Any],
    *,
    trending_threshold: float = DEFAULT_FUNDING_TRENDING_THRESHOLD,
    risk_off_threshold: float = DEFAULT_FUNDING_RISK_OFF_THRESHOLD,
    breadth_threshold: float = DEFAULT_FUNDING_BREADTH_THRESHOLD,
    extreme_sentiment_threshold: float = DEFAULT_FUNDING_EXTREME_SENTIMENT_THRESHOLD,
    extreme_breadth_threshold: float = DEFAULT_FUNDING_EXTREME_BREADTH_THRESHOLD,
) -> Dict[str, str]:
    sentiment = _as_float(metrics.get("sentiment"), 0.0)
    positive_weight_share = _as_float(metrics.get("positive_weight_share"), 0.0)
    negative_weight_share = _as_float(metrics.get("negative_weight_share"), 0.0)
    strongest_sentiment = _as_float(metrics.get("strongest_sentiment"), 0.0)
    directional_floor = min(abs(float(trending_threshold)), abs(float(risk_off_threshold))) * 0.25

    if sentiment >= float(trending_threshold):
        return {"state": "TRENDING", "trigger": "average"}
    if sentiment <= float(risk_off_threshold):
        return {"state": "RISK_OFF", "trigger": "average"}

    if (
        strongest_sentiment >= float(extreme_sentiment_threshold)
        and positive_weight_share >= float(extreme_breadth_threshold)
    ):
        return {"state": "TRENDING", "trigger": "extreme_breadth"}
    if (
        strongest_sentiment <= -float(extreme_sentiment_threshold)
        and negative_weight_share >= float(extreme_breadth_threshold)
    ):
        return {"state": "RISK_OFF", "trigger": "extreme_breadth"}

    if positive_weight_share >= float(breadth_threshold) and sentiment >= directional_floor:
        return {"state": "TRENDING", "trigger": "breadth"}
    if negative_weight_share >= float(breadth_threshold) and sentiment <= -directional_floor:
        return {"state": "RISK_OFF", "trigger": "breadth"}

    return {"state": "SIDEWAYS", "trigger": "neutral"}


def estimate_funding_confidence(
    metrics: Mapping[str, Any],
    *,
    state: str,
    trending_threshold: float = DEFAULT_FUNDING_TRENDING_THRESHOLD,
    risk_off_threshold: float = DEFAULT_FUNDING_RISK_OFF_THRESHOLD,
    breadth_threshold: float = DEFAULT_FUNDING_BREADTH_THRESHOLD,
    extreme_sentiment_threshold: float = DEFAULT_FUNDING_EXTREME_SENTIMENT_THRESHOLD,
) -> float:
    sentiment = abs(_as_float(metrics.get("sentiment"), 0.0))
    strongest_sentiment = abs(_as_float(metrics.get("strongest_sentiment"), 0.0))
    breadth_bias = abs(_as_float(metrics.get("breadth_bias"), 0.0))

    trend_scale = max(abs(float(trending_threshold)), abs(float(risk_off_threshold)), 1e-6)
    breadth_scale = max(2.0 * max(float(breadth_threshold) - 0.5, 0.05), 1e-6)
    direction_score = min(sentiment / trend_scale, 1.0)
    breadth_score = min(breadth_bias / breadth_scale, 1.0)
    extreme_score = min(strongest_sentiment / max(float(extreme_sentiment_threshold), 1e-6), 1.0)

    if str(state).upper() == "SIDEWAYS":
        return round(min(max(direction_score * 0.35, breadth_score * 0.35, extreme_score * 0.25), 0.49), 4)

    return round(min(max(direction_score, breadth_score * 0.85, extreme_score * 0.75), 1.0), 4)


def build_funding_vote(
    *,
    sentiment: float,
    weight: float,
    details: Mapping[str, Any] | None = None,
    composite: bool,
    positive_weight_share: float = 0.0,
    negative_weight_share: float = 0.0,
    strongest_sentiment: float = 0.0,
    max_abs_sentiment: float = 0.0,
    extreme_positive_weight_share: float = 0.0,
    extreme_negative_weight_share: float = 0.0,
    trending_threshold: float = DEFAULT_FUNDING_TRENDING_THRESHOLD,
    risk_off_threshold: float = DEFAULT_FUNDING_RISK_OFF_THRESHOLD,
    breadth_threshold: float = DEFAULT_FUNDING_BREADTH_THRESHOLD,
    extreme_sentiment_threshold: float = DEFAULT_FUNDING_EXTREME_SENTIMENT_THRESHOLD,
    extreme_breadth_threshold: float = DEFAULT_FUNDING_EXTREME_BREADTH_THRESHOLD,
) -> Dict[str, Any]:
    metrics = {
        "sentiment": max(-1.0, min(1.0, _as_float(sentiment, 0.0))),
        "positive_weight_share": max(0.0, min(1.0, _as_float(positive_weight_share, 0.0))),
        "negative_weight_share": max(0.0, min(1.0, _as_float(negative_weight_share, 0.0))),
        "strongest_sentiment": max(-1.0, min(1.0, _as_float(strongest_sentiment, 0.0))),
        "max_abs_sentiment": max(0.0, min(1.0, _as_float(max_abs_sentiment, 0.0))),
        "extreme_positive_weight_share": max(0.0, min(1.0, _as_float(extreme_positive_weight_share, 0.0))),
        "extreme_negative_weight_share": max(0.0, min(1.0, _as_float(extreme_negative_weight_share, 0.0))),
    }
    metrics["breadth_bias"] = round(metrics["positive_weight_share"] - metrics["negative_weight_share"], 4)

    classification = classify_funding_state(
        metrics,
        trending_threshold=trending_threshold,
        risk_off_threshold=risk_off_threshold,
        breadth_threshold=breadth_threshold,
        extreme_sentiment_threshold=extreme_sentiment_threshold,
        extreme_breadth_threshold=extreme_breadth_threshold,
    )
    state = classification["state"]
    confidence = estimate_funding_confidence(
        metrics,
        state=state,
        trending_threshold=trending_threshold,
        risk_off_threshold=risk_off_threshold,
        breadth_threshold=breadth_threshold,
        extreme_sentiment_threshold=extreme_sentiment_threshold,
    )

    return {
        "state": state,
        "confidence": confidence,
        "weight": float(weight),
        "sentiment": metrics["sentiment"],
        "composite": bool(composite),
        "details": dict(details or {}),
        "raw_state": state,
        "trigger": classification["trigger"],
        "breadth": metrics["breadth_bias"],
        "positive_weight_share": metrics["positive_weight_share"],
        "negative_weight_share": metrics["negative_weight_share"],
        "strongest_sentiment": metrics["strongest_sentiment"],
        "max_abs_sentiment": metrics["max_abs_sentiment"],
        "extreme_positive_weight_share": metrics["extreme_positive_weight_share"],
        "extreme_negative_weight_share": metrics["extreme_negative_weight_share"],
    }
