"""Frozen downstream research admission, separate from any live permission."""
from __future__ import annotations

import math
import re

FIELDS = ("reference_schema", "experiment_version", "strategy_version", "cost_version",
          "analysis_source_identity", "horizon_hours")


def validate_reference_contract(contract):
    if not isinstance(contract, dict) or set(contract) != set(FIELDS):
        raise ValueError("complete frozen upstream reference bindings required")
    if any(not isinstance(contract[k], str) or not contract[k] for k in FIELDS[:-1]):
        raise ValueError("nonempty semantic reference versions required")
    if not re.fullmatch(r"[a-f0-9]{40}", contract["analysis_source_identity"]):
        raise ValueError("exact upstream worker commit required before experiment start")
    if type(contract["horizon_hours"]) is not int or contract["horizon_hours"] != 24:
        raise ValueError("this experiment studies only 24h references")


def reference_status(reference, now, contract):
    if not reference:
        return "missing"
    if not contract or any(reference.get(key) != contract.get(key) or contract.get(key) is None for key in FIELDS):
        return "version_mismatch"
    times = [reference.get(key) for key in ("published_ts", "first_received_ts", "expires_ts")]
    if any(isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) for value in times):
        return "invalid_time"
    published, received, expiry = times
    if published > now or received > now:
        return "late_or_future"
    if published > received or expiry <= published:
        return "invalid_time"
    if now >= expiry:
        return "expired"
    if reference.get("live_execution_eligible") is not False or reference.get("live_order_effect") != "none":
        return "invalid_research_boundary"
    if reference.get("receipt_reason", "record_only_not_adopted") not in {"record_only_not_adopted", "reference_no_view", "reference_expired"}:
        return "invalid_receipt"
    if reference.get("research_evaluable") is not True or reference.get("action") == "NO_VIEW" or reference.get("receipt_reason") == "reference_no_view":
        return "no_view"
    if reference.get("action") not in {"DEFER", "KEEP_BASELINE", "REVIEW_ENTRY"}:
        return "no_view"
    return "valid"


def reference_use(reference, now, contract):
    status = reference_status(reference, now, contract)
    return {"status": status, "valid": status == "valid",
            "defer": status == "valid" and reference.get("action") == "DEFER",
            "advice_id": (reference or {}).get("advice_id"),
            "actual_versions": {key: (reference or {}).get(key) for key in FIELDS},
            "expected_versions": contract, "decision_ts": now}
