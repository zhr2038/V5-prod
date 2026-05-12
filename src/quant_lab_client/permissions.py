from __future__ import annotations

from typing import Any

from .exceptions import QuantLabPermissionError


ALLOW = "ALLOW"
SELL_ONLY = "SELL_ONLY"
ABORT = "ABORT"
ALLOW_LOCAL = "ALLOW_LOCAL"


def normalize_permission(value: Any, *, allow_local: bool = False) -> str:
    raw = str(value or "").strip().upper().replace("-", "_").replace(" ", "_")
    if allow_local and raw == ALLOW_LOCAL:
        return ALLOW_LOCAL
    if raw in {"ALLOW", "ALLOWED", "OK", "LIVE_ALLOWED", "TRADE_ALLOWED"}:
        return ALLOW
    if raw in {"SELL_ONLY", "SELLONLY", "SELL", "LIQUIDATE_ONLY", "QUARANTINE"}:
        return SELL_ONLY
    if raw in {"ABORT", "DENY", "DENIED", "BLOCK", "BLOCKED", "HALT", "KILL", "STOP"}:
        return ABORT
    raise QuantLabPermissionError(f"unsupported quant-lab permission: {value!r}")


def combine_permissions(local_permission: str, quant_lab_permission: str) -> str:
    local = normalize_permission(local_permission)
    quant = normalize_permission(quant_lab_permission)
    if ABORT in {local, quant}:
        return ABORT
    if SELL_ONLY in {local, quant}:
        return SELL_ONLY
    return ALLOW


def fallback_permission(fail_policy: str) -> str:
    policy = str(fail_policy or "sell_only").strip().lower()
    if policy in {"allow_local_fallback", "allow"}:
        return ALLOW
    if policy == "abort":
        return ABORT
    return SELL_ONLY


def is_order_new_risk(order: Any) -> bool:
    side = str(getattr(order, "side", "") or "").strip().lower()
    intent = str(getattr(order, "intent", "") or "").strip().upper()
    meta = dict(getattr(order, "meta", None) or {})
    if bool(meta.get("reduce_only")):
        return False
    if side == "buy":
        return True
    if intent in {"OPEN_LONG", "OPEN", "ADD_LONG", "REBALANCE"}:
        return True
    try:
        target_weight = float(meta.get("target_weight", meta.get("target_w")))
        current_weight = float(meta.get("current_weight", meta.get("current_w")))
        if target_weight > current_weight:
            return True
    except (TypeError, ValueError):
        pass
    return False
