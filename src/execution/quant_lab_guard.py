from __future__ import annotations

# Compatibility wrapper. New guard logic lives in src.quant_lab_client.
from src.quant_lab_client.guard import QuantLabGuard, QuantLabGuardResult
from src.quant_lab_client.permissions import ABORT as DECISION_ABORT
from src.quant_lab_client.permissions import ALLOW as DECISION_ALLOW
from src.quant_lab_client.permissions import SELL_ONLY as DECISION_SELL_ONLY
from src.quant_lab_client.permissions import combine_permissions, fallback_permission, normalize_permission

DECISION_UNKNOWN = "UNKNOWN"


def normalize_quant_lab_decision(value):
    try:
        return normalize_permission(value)
    except Exception:
        return DECISION_UNKNOWN


def decision_from_payload(payload):
    if isinstance(payload, dict):
        for key in ("permission", "decision", "action", "mode", "status"):
            if key in payload:
                decision = normalize_quant_lab_decision(payload.get(key))
                if decision != DECISION_UNKNOWN:
                    return decision
        for key in ("data", "result", "payload"):
            nested = payload.get(key)
            if isinstance(nested, dict):
                decision = decision_from_payload(nested)
                if decision != DECISION_UNKNOWN:
                    return decision
    return DECISION_UNKNOWN


def fallback_decision(fail_policy: str) -> str:
    return fallback_permission(fail_policy)


__all__ = [
    "DECISION_ABORT",
    "DECISION_ALLOW",
    "DECISION_SELL_ONLY",
    "DECISION_UNKNOWN",
    "QuantLabGuard",
    "QuantLabGuardResult",
    "combine_permissions",
    "decision_from_payload",
    "fallback_decision",
    "normalize_quant_lab_decision",
]
