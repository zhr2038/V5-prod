import copy

import pytest

from src.research.participation_comparison import ParticipationComparison
from tests.test_participation_policy import CONFIG, snapshot


def observed(now=360002, price=100):
    value = snapshot(now, price)
    value["bar_ts"] = int(now // 3600) * 3600
    for symbol, row in value["symbols"].items():
        row["instrument"] = {"lot_size": "0.00000001", "minimum_qty": "0.000001", "minimum_notional_usdt": 1,
                             "buy_fee_currency": symbol.split("/")[0], "sell_fee_currency": "USDT",
                             "observed_ts": 1, "source_hash": "a" * 64}
    return value


def test_reference_missing_or_expired_exactly_preserves_candidate_portfolio():
    control = ParticipationComparison(CONFIG)
    treatment = ParticipationComparison(CONFIG, use_reference=True)
    for now, price in [(360002, 100), (360012, 100), (363602, 101), (367202, 97), (367212, 97)]:
        source = observed(now, price)
        original = copy.deepcopy(source)
        a = control.observe(source)
        b = treatment.observe(source, references={"BTC/USDT": {"horizon_hours": 24, "action": "DEFER", "expires_ts": 1}})
        assert a == b
        assert source == original
    assert len(control.book.fills) == len(treatment.book.fills) == 2


def test_reference_only_defers_new_risk_and_never_suppresses_stop_exit():
    control = ParticipationComparison(CONFIG)
    treatment = ParticipationComparison(CONFIG, use_reference=True)
    ref = {"BTC/USDT": {"horizon_hours": 24, "action": "DEFER", "expires_ts": 400000,
                         "published_ts": 360001, "first_received_ts": 360001,
                         "research_evaluable": True, "live_execution_eligible": False, "advice_id": "advice-a"}}
    assert control.observe(observed())["decision"]["action"] == "entry_intent"
    assert treatment.observe(observed(), references=ref)["decision"]["action"] == "reference_deferred_entry"
    control.observe(observed(360012))
    # A later missing reference permits the unchanged strategy; once open, DEFER never blocks a stop.
    treatment.observe(observed(363602))
    treatment.observe(observed(363612))
    assert treatment.book.fills
    assert treatment.observe(observed(367202, 95), references=ref)["decision"]["action"] == "exit_intent"
    treatment.observe(observed(367212, 95), references=ref)
    assert treatment.book.fills[-1]["side"] == "sell"
    assert treatment.state["cooldown_until"]["BTC/USDT"] > 367212


def test_duplicate_or_reversed_observation_never_adds_orders():
    candidate = ParticipationComparison(CONFIG)
    candidate.observe(observed())
    for now in [360002, 360001]:
        with pytest.raises(ValueError, match="chronological"):
            candidate.observe(observed(now))
    assert candidate.book.fills == []
