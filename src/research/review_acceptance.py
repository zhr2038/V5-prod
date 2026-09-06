"""Read-only evaluation of frozen research requirements; never grants live access."""
from __future__ import annotations

import math
from src.research.review_integrity import integrity_requirements


def finite(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        value = float(value)
    except (ValueError, TypeError):
        return None
    return value if math.isfinite(value) else None


def evaluate_acceptance(report, experiment):
    evaluations = {}
    primary = str(experiment["roundtrip_cost_scenarios_bps"][0])
    for treatment, control in experiment["primary_comparisons"]:
        key = treatment + "_minus_" + control
        rows = []

        def threshold(name, actual, required, *, strict=False, sample=False):
            value = finite(actual)
            passed = value is not None and (value > required if strict else value >= required)
            rows.append({"criterion": name, "actual": actual, "required": (">" if strict else ">=") + str(required),
                         "result": "PASS" if passed else "INSUFFICIENT" if value is None or sample else "FAIL",
                         "reason": None if passed else "metric_unavailable" if value is None else "sample_threshold_not_reached" if sample else "result_does_not_support_requirement"})

        threshold("forward_calendar_days", report.get("calendar_days"), experiment["minimum_forward_calendar_days"], sample=True)
        threshold("independent_entry_opportunities", report.get("independent_24h_entry_opportunities", 0), experiment["minimum_independent_entry_opportunities"], sample=True)
        threshold("distinct_entry_days", report.get("distinct_independent_entry_days", 0), experiment["minimum_distinct_entry_days"], sample=True)
        sample_ready = all(row["result"] == "PASS" for row in rows)
        integrity = integrity_requirements(report.get("observation_integrity", {}))
        integrity_ready = all(row["result"] == "PASS" for row in integrity)
        rows.append({"criterion": "observation_integrity", "actual": report.get("observation_integrity", {}).get("judgment"),
                     "required": "all_frozen_observation_requirements_pass", "result": "PASS" if integrity_ready else "INSUFFICIENT",
                     "reason": None if integrity_ready else "observation_evidence_incomplete", "details": integrity})
        legacy_count = report.get("observation_integrity", {}).get("legacy_observations", 0)
        if legacy_count:
            legacy = report.get("legacy_observation_integrity") or {}
            legacy_rows = [r for r in integrity_requirements(legacy) if r["criterion"] != "prospective_days"]
            legacy_ready = (legacy.get("valid_quote_observations") == legacy_count
                            and legacy.get("evidence_origin") == "retrospective_recorded_events_not_new_forward_observations"
                            and all(r["result"] == "PASS" for r in legacy_rows))
            integrity_ready = integrity_ready and legacy_ready
            rows.append({"criterion": "inherited_observation_integrity", "actual": legacy.get("judgment"),
                         "required": "all_retained_account_intervals_assessed_without_counting_as_new_forward_days",
                         "result": "PASS" if legacy_ready else "INSUFFICIENT",
                         "reason": None if legacy_ready else "inherited_observation_evidence_incomplete", "details": legacy_rows})
        exposure_ready = True
        funnel = report.get("reference_funnel", {}).get(primary, {})
        if treatment == "D_reference_only":
            minimums = experiment.get("reference_exposure_requirements", {})
            for name, actual, required in (
                ("valid_reference_coverage_rate", funnel.get("coverage_rate"), minimums.get("minimum_valid_coverage_rate", .8)),
                ("matched_reference_candidates", funnel.get("matched_candidates", 0), minimums.get("minimum_matched_candidates", 30)),
                ("independent_reference_changes", funnel.get("independent_changed_opportunities", 0), minimums.get("minimum_independent_changes", 10)),
            ):
                threshold(name, actual, required, sample=True)
            exposure_ready = all(row["result"] == "PASS" for row in rows[-3:])
        values = report.get("comparisons", {}).get(primary, {}).get(key, {})
        treatment_stats = report.get("scenarios", {}).get(primary, {}).get(treatment, {})
        control_stats = report.get("scenarios", {}).get(primary, {}).get(control, {})
        threshold("net_equity_delta_usdt", values.get("net_equity_delta_usdt"), 0, strict=True)
        interval = values.get("paired_daily_block_bootstrap_95pct_delta_usdt")
        threshold("paired_block_bootstrap_95pct_lower_bound_usdt", interval[0] if isinstance(interval, list) and len(interval) == 2 else None, 0, strict=True)
        a, b = finite(treatment_stats.get("maximum_drawdown_fraction")), finite(control_stats.get("maximum_drawdown_fraction"))
        rows.append({"criterion": "maximum_drawdown_no_greater_than_control", "actual": a, "required": {"maximum": b},
                     "result": "INSUFFICIENT" if a is None or b is None else "PASS" if a <= b else "FAIL",
                     "reason": "metric_unavailable" if a is None or b is None else None if a <= b else "treatment_drawdown_exceeds_control"})
        for cost in (60, 120):
            threshold(f"cost_{cost}bps_net_equity_delta_usdt", report.get("comparisons", {}).get(str(cost), {}).get(key, {}).get("net_equity_delta_usdt"), 0, strict=True)
        threshold("remove_largest_profitable_campaign_net_equity_usdt", treatment_stats.get("net_equity_without_largest_profitable_campaign_usdt"), 0, strict=True)
        segments = {name: {field: sorted(report.get("scenarios", {}).get(primary, {}).get(name, {}).get(field, {}))
                           for field in ("daily_equity_increments", "regime_equity_increments")} for name in (treatment, control)}
        available = all(groups for value in segments.values() for groups in value.values())
        rows.append({"criterion": "all_observed_time_and_regime_segments_reported", "actual": segments,
                     "required": "all_observed_segments_without_selection", "result": "PASS" if available else "INSUFFICIENT",
                     "reason": None if available else "no_observed_segments"})
        if sample_ready and not integrity_ready:
            status = "INSUFFICIENT_OBSERVATION_EVIDENCE"
        elif not exposure_ready and funnel.get("candidates", 0):
            status = "INSUFFICIENT_REFERENCE_EXPOSURE"
        elif not sample_ready:
            status = "INSUFFICIENT_FORWARD_EVIDENCE"
        elif not exposure_ready:
            status = "INSUFFICIENT_REFERENCE_EXPOSURE"
        elif any(row["result"] == "FAIL" for row in rows):
            status = "RESULT_NOT_SUPPORTED"
        elif any(row["result"] == "INSUFFICIENT" for row in rows):
            status = "INSUFFICIENT_FORWARD_EVIDENCE"
        else:
            status = "READY_FOR_MANUAL_RESEARCH_REVIEW"
        evaluations[key] = {"status": status, "requirements": rows, "live_execution_eligible": False}
    statuses = {value["status"] for value in evaluations.values()}
    overall = next((s for s in ("INSUFFICIENT_OBSERVATION_EVIDENCE", "INSUFFICIENT_REFERENCE_EXPOSURE", "INSUFFICIENT_FORWARD_EVIDENCE", "RESULT_NOT_SUPPORTED") if s in statuses), "READY_FOR_MANUAL_RESEARCH_REVIEW")
    return {"status": overall, "comparisons": evaluations, "live_execution_eligible": False,
            "automatic_live_scaling": False, "scope": "research_evidence_only_no_order_or_risk_authority"}
