import copy

import pytest

from src.research.review_acceptance import evaluate_acceptance
from src.research.review_integrity import POLICY, POLICY_HASH
from tests.test_review_comparison import EXPERIMENT


def sufficient_report():
    stats = {"maximum_drawdown_fraction": .01,
             "net_equity_without_largest_profitable_campaign_usdt": 1,
             "daily_equity_increments": {str(i): .1 for i in range(30)},
             "regime_equity_increments": {"Trending": 1, "Risk-Off": -.1}}
    return {"calendar_days": 31, "independent_24h_entry_opportunities": 101,
            "observation_integrity": {"policy_version": POLICY["version"], "policy_sha256": POLICY_HASH,
                "judgment": "PASS", "prospective_days": 31, "quote_slot_coverage": 1, "signal_slot_coverage": 1,
                "maximum_interval_seconds": 60, "missing_duration_fraction": 0,
                "uncovered_common_decision_count": 0, "holding_missing_seconds": {"30:C_hold24_only": 0},
                "holding_signal_unavailable_observations": {"30:C_hold24_only": 0}},
            "distinct_independent_entry_days": 11,
            "reference_funnel": {"30": {"candidates": 40, "matched_candidates": 40, "coverage_rate": .9,
                                             "independent_changed_opportunities": 11}},
            "scenarios": {str(cost): {name: copy.deepcopy(stats) for name in EXPERIMENT["cohorts"]} for cost in (30, 60, 120)},
            "comparisons": {str(cost): {a + "_minus_" + b: {
                "net_equity_delta_usdt": 1, "paired_daily_block_bootstrap_95pct_delta_usdt": [.1, 1.9]}
                for a, b in EXPERIMENT["primary_comparisons"]} for cost in (30, 60, 120)}}


def test_no_opportunities_reports_missing_evidence_not_success():
    result = evaluate_acceptance({}, EXPERIMENT)
    assert result["status"] == "INSUFFICIENT_FORWARD_EVIDENCE"
    assert all(any(r["reason"] == "metric_unavailable" for r in c["requirements"]) for c in result["comparisons"].values())


def test_zero_reference_coverage_is_no_effective_treatment():
    report = sufficient_report()
    report["reference_funnel"]["30"].update(coverage_rate=0, independent_changed_opportunities=0)
    result = evaluate_acceptance(report, EXPERIMENT)
    assert result["status"] == "INSUFFICIENT_REFERENCE_EXPOSURE"
    assert result["comparisons"]["B_participation_v1_minus_A_original_v5"]["status"] == "READY_FOR_MANUAL_RESEARCH_REVIEW"


@pytest.mark.parametrize("field,value", [("net_equity_delta_usdt", -1), ("paired_daily_block_bootstrap_95pct_delta_usdt", [-.1, 1.9])])
def test_sufficient_sample_with_unsupported_result(field, value):
    report = sufficient_report()
    report["comparisons"]["30"]["B_participation_v1_minus_A_original_v5"][field] = value
    assert evaluate_acceptance(report, EXPERIMENT)["status"] == "RESULT_NOT_SUPPORTED"


def test_all_frozen_research_conditions_met_never_authorizes_live_or_mutates_inputs():
    report = sufficient_report()
    original = copy.deepcopy(report)
    result = evaluate_acceptance(report, EXPERIMENT)
    assert result["status"] == "READY_FOR_MANUAL_RESEARCH_REVIEW"
    assert result["live_execution_eligible"] is False
    assert result["automatic_live_scaling"] is False
    assert all(row["result"] == "PASS" for group in result["comparisons"].values() for row in group["requirements"])
    assert report == original


def test_nonfinite_metric_is_missing_instead_of_passing():
    report = sufficient_report()
    report["comparisons"]["30"]["B_participation_v1_minus_A_original_v5"]["net_equity_delta_usdt"] = float("nan")
    assert evaluate_acceptance(report, EXPERIMENT)["status"] == "INSUFFICIENT_FORWARD_EVIDENCE"


@pytest.mark.parametrize("field,value", [("quote_slot_coverage", .5), ("signal_slot_coverage", .5),
    ("maximum_interval_seconds", 4000), ("missing_duration_fraction", .5),
    ("holding_missing_seconds", {"30:C_hold24_only": 60}), ("prospective_days", 29),
    ("uncovered_common_decision_count", 1), ("policy_sha256", "unknown"),
    ("holding_signal_unavailable_observations", {"30:C_hold24_only": 1})])
def test_integrity_measured_values_override_a_claimed_pass(field, value):
    report = sufficient_report()
    report["observation_integrity"][field] = value
    result = evaluate_acceptance(report, EXPERIMENT)
    assert result["status"] == "INSUFFICIENT_OBSERVATION_EVIDENCE"
    assert not result["live_execution_eligible"]


def test_cash_control_limitation_does_not_relax_the_frozen_v2_drawdown_rule():
    report = sufficient_report()
    report["scenarios"]["30"]["A_original_v5"].update(maximum_drawdown_fraction=0, actual_simulated_fills=0)
    result = evaluate_acceptance(report, EXPERIMENT)
    row = next(r for r in result["comparisons"]["B_participation_v1_minus_A_original_v5"]["requirements"]
               if r["criterion"] == "maximum_drawdown_no_greater_than_control")
    assert row["result"] == "FAIL" and row["required"]["maximum"] == 0
