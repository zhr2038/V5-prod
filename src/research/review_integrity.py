"""Streaming observation evidence, separate from financial state and live decisions."""
from __future__ import annotations

import copy
import hashlib
import json
import math
from pathlib import Path

POLICY_PATH = Path(__file__).resolve().parents[2] / "configs/research/review_integrity_v1.json"
POLICY = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
POLICY_HASH = hashlib.sha256(POLICY_PATH.read_bytes()).hexdigest()


def observe_integrity(metrics, event, previous_at, experiment, previous_marks, previous_decision_bar=None):
    now, bar = event["observed_at"], event["bar_ts"]
    interval = 0 if previous_at is None else now - previous_at
    cadence = POLICY["cadence_seconds"]
    slot = math.floor((now - POLICY["schedule_offset_seconds"]) / cadence)
    state = metrics.setdefault("integrity", {
        "policy_version": POLICY["version"], "policy_sha256": POLICY_HASH,
        "start_ts": now, "first_slot": slot, "last_slot": None, "last_signal_slot": None,
        "metric_interval_start_ts": previous_at if previous_at is not None else now,
        "valid_quote_observations": 0, "valid_signal_observations": 0,
        "observed_quote_slots": 0, "observed_signal_slots": 0,
        "maximum_interval_seconds": 0, "missing_duration_seconds": 0,
        "holding_missing_seconds": {}, "holding_signal_unavailable_observations": {},
        "common_decisions": 0, "covered_common_decisions": 0,
        "uncovered_common_decision_count": 0, "uncovered_common_decision_hours": [],
        "next_decision_bar": bar + 3600 if previous_decision_bar == bar else bar, "open_decision_covered": False,
        "legacy_observations": metrics.get("observations", {}).get("count", 1) - 1,
    })
    if state["policy_sha256"] != POLICY_HASH:
        raise ValueError("observation_integrity_policy_changed_requires_explicit_migration")
    state["end_ts"] = now
    state["valid_quote_observations"] += 1
    state["observed_quote_slots"] += int(slot != state["last_slot"])
    state["last_slot"] = slot
    signals = event.get("signal_data", {"status": "valid"})["status"] == "valid"
    if signals:
        state["valid_signal_observations"] += 1
        state["observed_signal_slots"] += int(slot != state["last_signal_slot"])
        state["last_signal_slot"] = slot
    state["maximum_interval_seconds"] = max(state["maximum_interval_seconds"], interval)
    missing = interval - cadence if interval > cadence + POLICY["completion_jitter_seconds"] else 0
    state["missing_duration_seconds"] += missing
    for cost, scenario in event["scenarios"].items():
        for name, entry in scenario["cohorts"].items():
            key = cost + ":" + name
            previous = previous_marks.get(key)
            holding = bool(previous and float(previous["gross_exposure_usdt"]) > 0)
            state["holding_missing_seconds"][key] = state["holding_missing_seconds"].get(key, 0) + (missing if holding else 0)
            # Missing signals leave EMA exits unobserved even when quote stops run.
            current_holding = float(entry["portfolio"]["gross_exposure_usdt"]) > 0
            state["holding_signal_unavailable_observations"][key] = state["holding_signal_unavailable_observations"].get(key, 0) + int(not signals and (holding or current_holding))
    clock = experiment.get("decision_clock", {"offset_seconds": 0, "maximum_lateness_seconds": 3599})
    offset = clock["offset_seconds"] + clock["maximum_lateness_seconds"]
    while state["next_decision_bar"] + offset < now:
        hour = state["next_decision_bar"]
        # A cohort started after this hour's deadline never had that opportunity.
        if hour + offset >= state["start_ts"]:
            state["common_decisions"] += 1
            if state["open_decision_covered"]:
                state["covered_common_decisions"] += 1
            else:
                state["uncovered_common_decision_count"] += 1
                state["uncovered_common_decision_hours"].append(hour)
        state["next_decision_bar"] += 3600
        state["open_decision_covered"] = False
    if event["hourly_decision"]:
        state["open_decision_covered"] = True


def integrity_report(metrics):
    value = copy.deepcopy(metrics.get("integrity", {}))
    if not value:
        return {"judgment": "INSUFFICIENT", "reason": "observation_evidence_unavailable", "policy_version": POLICY["version"]}
    duration = value["end_ts"] - value["start_ts"]
    interval_duration = value["end_ts"] - value["metric_interval_start_ts"]
    planned = value["last_slot"] - value["first_slot"] + 1
    value.update(planned_observations=planned,
                 missed_quote_slots=planned - value["observed_quote_slots"],
                 quote_slot_coverage=value["observed_quote_slots"] / planned,
                 signal_slot_coverage=value["observed_signal_slots"] / planned,
                 prospective_days=duration / 86400,
                 missing_duration_fraction=value["missing_duration_seconds"] / interval_duration if interval_duration else None,
                 exposure_scope="last_observed_holdings_carried_between_quotes_estimate_not_monitoring_proof",
                 drawdown_scope=POLICY["drawdown_scope"])
    value["requirements"] = integrity_requirements(value)
    value["judgment"] = "PASS" if all(r["result"] == "PASS" for r in value["requirements"]) else "INSUFFICIENT"
    for key in ("first_slot", "last_slot", "last_signal_slot", "next_decision_bar", "open_decision_covered"):
        value.pop(key)
    return value


def integrity_requirements(value):
    """Recompute from measurements; never trust a caller-supplied PASS label."""
    requirements = []
    def check(name, actual, limit, minimum=False):
        valid = not isinstance(actual, bool) and isinstance(actual, (int, float)) and math.isfinite(actual)
        passed = valid and (actual >= limit if minimum else 0 <= actual <= limit)
        requirements.append({"criterion": name, "actual": actual, "required": (">=" if minimum else "<=") + str(limit),
                             "result": "PASS" if passed else "INSUFFICIENT",
                             "reason": None if passed else "observation_evidence_incomplete"})
    bound = value.get("policy_sha256") == POLICY_HASH and value.get("policy_version") == POLICY["version"]
    requirements.append({"criterion": "integrity_policy_binding", "actual": value.get("policy_sha256"), "required": POLICY_HASH,
                         "result": "PASS" if bound else "INSUFFICIENT", "reason": None if bound else "observation_policy_missing_or_changed"})
    for name, key, minimum in (
        ("prospective_days", "minimum_prospective_days", True),
        ("quote_slot_coverage", "minimum_quote_slot_coverage", True),
        ("signal_slot_coverage", "minimum_signal_slot_coverage", True),
        ("maximum_interval_seconds", "maximum_interval_seconds", False),
        ("missing_duration_fraction", "maximum_missing_duration_fraction", False),
        ("uncovered_common_decision_count", "maximum_uncovered_common_decisions", False),
    ):
        check(name, value.get(name), POLICY[key], minimum)
    for name, limit in (("holding_missing_seconds", "maximum_holding_missing_seconds"),
                        ("holding_signal_unavailable_observations", "maximum_holding_signal_unavailable_observations")):
        values = value.get(name)
        check(name, max(values.values()) if isinstance(values, dict) and values and all(isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) and v >= 0 for v in values.values()) else None, POLICY[limit])
    return requirements
