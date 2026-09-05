from __future__ import annotations

import json

import pytest

from src.regime.ensemble_regime_engine import EnsembleRegimeEngine
from src.regime.funding_vote_utils import build_funding_vote, classify_funding_state, summarize_funding_rows


def observed_vote(**changes):
    # Production 20260905_22: a 4% extreme constituent, 64% mildly negative
    # breadth, and a slightly positive aggregate previously became RISK_OFF.
    return build_funding_vote(**{
        "sentiment": .0028, "weight": .4, "composite": True,
        "positive_weight_share": .36, "negative_weight_share": .64,
        "strongest_sentiment": -.1253, "max_abs_sentiment": .1253,
        "extreme_positive_weight_share": 0., "extreme_negative_weight_share": .04,
        **changes,
    })


def test_small_extreme_constituent_does_not_zero_trending_portfolio():
    funding = observed_vote()
    assert (funding["state"], funding["trigger"]) == ("SIDEWAYS", "neutral")
    # Only the pure vote reducer is called; constructing the engine writes history.
    state, _, _ = EnsembleRegimeEngine._weighted_vote(None, [
        {"state": "TRENDING", "weight": .35, "confidence": .999384519},
        funding, {"state": "RISK_OFF", "weight": .25, "confidence": .30},
    ])
    assert state == "TRENDING"


@pytest.mark.parametrize("sign,expected", [(1, "TRENDING"), (-1, "RISK_OFF")])
def test_extreme_breadth_requires_actual_extreme_coverage_in_both_directions(sign, expected):
    weak = summarize_funding_rows([
        {"weight": .04, "sentiment": sign * .1253},
        {"weight": .60, "sentiment": sign * .01},
        {"weight": .36, "sentiment": -sign * .02},
    ])
    assert classify_funding_state(weak)["state"] == "SIDEWAYS"
    broad = summarize_funding_rows([
        {"weight": .60, "sentiment": sign * .13},
        # The largest individual outlier has the opposite sign, but not breadth.
        {"weight": .40, "sentiment": -sign * .15},
    ])
    assert classify_funding_state(broad) == {"state": expected, "trigger": "extreme_breadth"}


@pytest.mark.parametrize("share,expected", [(.5499, "SIDEWAYS"), (.55, "RISK_OFF")])
def test_extreme_coverage_threshold_is_inclusive(share, expected):
    assert observed_vote(extreme_negative_weight_share=share)["state"] == expected


def test_changed_threshold_does_not_silently_reinterpret_legacy_composite_shares(caplog):
    vote = observed_vote(extreme_negative_weight_share=.60, extreme_sentiment_threshold=.20)
    assert vote["state"] == "SIDEWAYS"
    assert vote["extreme_breadth_sentiment_threshold"] == .12
    assert "threshold mismatch" in caplog.text
    metrics = summarize_funding_rows([
        {"sentiment": -.21, "weight": .60}, {"sentiment": .25, "weight": .40},
    ], extreme_sentiment_threshold=.20)
    assert classify_funding_state(metrics, extreme_sentiment_threshold=.20) == {
        "state": "RISK_OFF", "trigger": "extreme_breadth",
    }


@pytest.mark.parametrize("value", [None, float("nan"), float("inf"), -1, 1.1, "bad"])
def test_missing_or_invalid_extreme_breadth_warns_and_never_uses_all_negative_breadth(value, caplog):
    vote = observed_vote(extreme_negative_weight_share=value)
    assert vote["state"] == "SIDEWAYS"
    assert vote["extreme_negative_weight_share"] is None
    assert "extreme_negative_weight_share" in caplog.text
    json.dumps(vote, allow_nan=False)


@pytest.mark.parametrize("changes,expected,trigger", [
    ({"sentiment": -.11}, "RISK_OFF", "average"),
    ({"sentiment": .11}, "TRENDING", "average"),
    ({"sentiment": -.03, "negative_weight_share": .70}, "RISK_OFF", "breadth"),
    ({"sentiment": .03, "positive_weight_share": .70}, "TRENDING", "breadth"),
])
def test_average_and_directional_breadth_risk_rules_are_preserved(changes, expected, trigger):
    vote = observed_vote(**changes)
    assert (vote["state"], vote["trigger"]) == (expected, trigger)
