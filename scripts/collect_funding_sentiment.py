#!/usr/bin/env python3
"""
Collect funding-rate sentiment snapshots from OKX.

The composite output is used by the regime engine and dashboard.
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.regime.funding_vote_utils import (
    DEFAULT_FUNDING_EXTREME_SENTIMENT_THRESHOLD,
    classify_funding_state,
    summarize_funding_rows,
)


def get_cache_dir() -> Path:
    return PROJECT_ROOT / "data" / "sentiment_cache"


SYMBOLS_BY_TIER = {
    "large": {
        "BTC-USDT": 0.25,
        "ETH-USDT": 0.25,
    },
    "mid": {
        "SOL-USDT": 0.10,
        "ADA-USDT": 0.08,
        "AVAX-USDT": 0.07,
        "DOT-USDT": 0.05,
    },
    "small": {
        "DOGE-USDT": 0.08,
        "UNI-USDT": 0.06,
        "PEPE-USDT": 0.04,
        "LTC-USDT": 0.02,
    },
}


def get_all_symbols():
    all_symbols = {}
    for tier, symbols in SYMBOLS_BY_TIER.items():
        tier_weight = {"large": 0.50, "mid": 0.30, "small": 0.20}[tier]
        tier_total = float(sum(symbols.values()) or 1.0)
        for sym, weight_in_tier in symbols.items():
            total_weight = tier_weight * float(weight_in_tier) / tier_total
            all_symbols[sym] = {
                "tier": tier,
                "tier_weight": tier_weight,
                "weight_in_tier": float(weight_in_tier),
                "total_weight": float(total_weight),
            }
    return all_symbols


def get_okx_funding_rate(inst_id: str) -> dict:
    try:
        url = f"https://www.okx.com/api/v5/public/funding-rate?instId={inst_id}"
        response = requests.get(url, timeout=10)
        data = response.json()
        if data.get("code") == "0" and data.get("data"):
            item = data["data"][0]
            return {
                "funding_rate": float(item.get("fundingRate", 0.0)),
                "next_funding_time": item.get("nextFundingTime", ""),
                "method": "okx_api",
            }
    except Exception as exc:
        print(f"[FundingRate] failed to fetch {inst_id}: {exc}")
    return {"funding_rate": 0.0, "method": "fallback"}


def funding_rate_to_sentiment(funding_rate: float) -> dict:
    max_fr = 0.001
    min_fr = -0.001

    if funding_rate >= 0:
        sentiment = min(float(funding_rate) / max_fr, 1.0)
    else:
        sentiment = max(float(funding_rate) / abs(min_fr), -1.0)

    fear_greed = int((sentiment + 1.0) * 50.0)

    if sentiment > 0.7:
        stage = "fomo"
        summary = "Funding is extremely positive; leverage looks crowded on the long side."
    elif sentiment > 0.3:
        stage = "optimistic"
        summary = "Funding is positive; market sentiment is leaning optimistic."
    elif sentiment > -0.3:
        stage = "neutral"
        summary = "Funding is near balance; market sentiment is neutral."
    elif sentiment > -0.7:
        stage = "pessimistic"
        summary = "Funding is negative; market sentiment is leaning defensive."
    else:
        stage = "panic"
        summary = "Funding is extremely negative; market sentiment is highly defensive."

    return {
        "sentiment_score": round(sentiment, 4),
        "fear_greed_index": fear_greed,
        "market_stage": stage,
        "summary": summary,
        "raw_funding_rate": float(funding_rate),
        "confidence": 0.8,
    }


def _market_stage_from_state(state: str) -> str:
    if state == "TRENDING":
        return "optimistic"
    if state == "RISK_OFF":
        return "pessimistic"
    return "neutral"


def collect_funding_sentiment():
    all_symbols = get_all_symbols()
    cache_dir = get_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H")
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] collecting funding sentiment for {len(all_symbols)} symbols...")

    tier_sentiments = {"large": [], "mid": [], "small": []}

    for symbol_name, config in all_symbols.items():
        inst_id = symbol_name.replace("-USDT", "-USDT-SWAP")
        tier = config["tier"]
        try:
            fr_data = get_okx_funding_rate(inst_id)
            funding_rate = float(fr_data["funding_rate"])
            sentiment_data = funding_rate_to_sentiment(funding_rate)

            cache_data = {
                "f6_sentiment": sentiment_data["sentiment_score"],
                "f6_sentiment_magnitude": abs(sentiment_data["sentiment_score"]),
                "f6_fear_greed_index": sentiment_data["fear_greed_index"],
                "f6_sentiment_summary": sentiment_data["summary"],
                "f6_sentiment_confidence": sentiment_data["confidence"],
                "f6_sentiment_source": "funding_rate",
                "f6_market_stage": sentiment_data["market_stage"],
                "raw_funding_rate": sentiment_data["raw_funding_rate"],
                "tier": tier,
                "weight": config["total_weight"],
                "collected_at": datetime.now().isoformat(),
            }
            cache_file = cache_dir / f"funding_{symbol_name}_{timestamp}.json"
            cache_file.write_text(json.dumps(cache_data, ensure_ascii=False, indent=2), encoding="utf-8")

            tier_sentiments[tier].append(
                {
                    "symbol": symbol_name,
                    "sentiment": sentiment_data["sentiment_score"],
                    "funding_rate": funding_rate,
                    "weight": config["total_weight"],
                }
            )
            print(
                f"  [{tier}] {symbol_name}: funding={funding_rate:.6f}, "
                f"sentiment={sentiment_data['sentiment_score']:.2f}"
            )
        except Exception as exc:
            print(f"  {symbol_name}: failed - {exc}")

    print(f"\n[{datetime.now():%Y-%m-%d %H:%M:%S}] aggregating composite sentiment...")

    all_rows = []
    tier_breakdown = {}
    for tier, items in tier_sentiments.items():
        if not items:
            continue
        metrics = summarize_funding_rows(
            items,
            extreme_sentiment_threshold=DEFAULT_FUNDING_EXTREME_SENTIMENT_THRESHOLD,
        )
        all_rows.extend(items)
        tier_breakdown[tier] = {
            "avg": metrics["sentiment"],
            "weighted_avg": metrics["sentiment"],
            "count": len(items),
            "positive_weight_share": metrics["positive_weight_share"],
            "negative_weight_share": metrics["negative_weight_share"],
            "breadth_bias": metrics["breadth_bias"],
            "strongest_sentiment": metrics["strongest_sentiment"],
            "max_abs_sentiment": metrics["max_abs_sentiment"],
            "tier_weight": {"large": 0.50, "mid": 0.30, "small": 0.20}[tier],
        }
        print(
            f"  {tier}: weighted_avg={metrics['sentiment']:.3f}, "
            f"breadth={metrics['breadth_bias']:.3f}"
        )

    overall_metrics = summarize_funding_rows(
        all_rows,
        extreme_sentiment_threshold=DEFAULT_FUNDING_EXTREME_SENTIMENT_THRESHOLD,
    )
    classification = classify_funding_state(
        overall_metrics,
        extreme_sentiment_threshold=DEFAULT_FUNDING_EXTREME_SENTIMENT_THRESHOLD,
    )
    overall_sentiment = float(overall_metrics["sentiment"])
    print(f"\n  composite funding sentiment: {overall_sentiment:.3f} ({classification['state']})")

    overall_data = {
        "f6_sentiment": round(overall_sentiment, 4),
        "f6_sentiment_magnitude": abs(round(overall_sentiment, 4)),
        "f6_fear_greed_index": int((overall_sentiment + 1.0) * 50.0),
        "f6_sentiment_summary": (
            f"Funding composite sentiment: {overall_sentiment:.3f} "
            f"(large {len(tier_sentiments['large'])}, mid {len(tier_sentiments['mid'])}, small {len(tier_sentiments['small'])})"
        ),
        "f6_sentiment_confidence": 0.85,
        "f6_sentiment_source": "funding_rate_composite",
        "f6_market_stage": _market_stage_from_state(classification["state"]),
        "funding_state_hint": classification["state"],
        "funding_state_trigger": classification["trigger"],
        "positive_weight_share": overall_metrics["positive_weight_share"],
        "negative_weight_share": overall_metrics["negative_weight_share"],
        "breadth_bias": overall_metrics["breadth_bias"],
        "strongest_sentiment": overall_metrics["strongest_sentiment"],
        "max_abs_sentiment": overall_metrics["max_abs_sentiment"],
        "extreme_positive_weight_share": overall_metrics["extreme_positive_weight_share"],
        "extreme_negative_weight_share": overall_metrics["extreme_negative_weight_share"],
        "tier_breakdown": tier_breakdown,
        "collected_at": datetime.now().isoformat(),
    }

    overall_file = cache_dir / f"funding_COMPOSITE_{timestamp}.json"
    overall_file.write_text(json.dumps(overall_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] funding sentiment collection complete")


if __name__ == "__main__":
    collect_funding_sentiment()
