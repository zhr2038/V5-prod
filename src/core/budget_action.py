from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from configs.schema import AppConfig
from src.reporting.decision_audit import DecisionAudit


def effective_min_trade_notional(cfg: AppConfig, audit: Optional[DecisionAudit]) -> Tuple[float, Dict[str, Any]]:
    """F3.2 second-stage action.

    Returns (min_notional_effective, action_meta_patch).
    action_meta_patch is meant to be merged into audit.budget_action.
    """
    base = float(cfg.budget.min_trade_notional_base)

    patch: Dict[str, Any] = {
        "min_trade_notional_base": base,
        "min_trade_notional_multiplier": 1.0,
        "min_trade_notional_cap": None,
        "min_trade_notional_effective": base,
        "trigger_metrics": {},
    }

    if not cfg.budget.action_enabled:
        return base, patch

    b = (audit.budget or {}) if audit else {}
    if not b or not bool(b.get("exceeded")):
        return base, patch

    fills_count = int(b.get("fills_count_today") or 0)
    median_notional = b.get("median_notional_usdt_today")
    small_ratio = b.get("small_trade_ratio_today")
    equity_est = b.get("avg_equity_est")

    # prerequisites
    if fills_count < int(cfg.budget.min_fills_for_second_stage):
        patch["trigger_metrics"] = {
            "fills_count_today": fills_count,
            "median_notional_usdt_today": median_notional,
            "small_trade_ratio_today": small_ratio,
            "small_trade_notional_cutoff": b.get("small_trade_notional_cutoff"),
        }
        return base, patch

    # compute median threshold
    thr_abs = float(cfg.budget.small_trade_median_threshold_abs)
    thr_eq = None
    try:
        if equity_est is not None:
            thr_eq = float(cfg.budget.small_trade_median_threshold_equity_ratio) * float(equity_est)
    except Exception:
        thr_eq = None
    median_thr = max(thr_abs, float(thr_eq) if thr_eq is not None else thr_abs)

    noisy = False
    try:
        if median_notional is not None and float(median_notional) < float(median_thr):
            noisy = True
    except Exception:
        pass
    try:
        if small_ratio is not None and float(small_ratio) > float(cfg.budget.small_trade_ratio_threshold):
            noisy = True
    except Exception:
        pass

    patch["trigger_metrics"] = {
        "fills_count_today": fills_count,
        "median_notional_usdt_today": median_notional,
        "median_threshold": median_thr,
        "small_trade_ratio_today": small_ratio,
        "small_trade_ratio_threshold": float(cfg.budget.small_trade_ratio_threshold),
        "small_trade_notional_cutoff": b.get("small_trade_notional_cutoff"),
    }

    if not noisy:
        return base, patch

    mult = float(cfg.budget.min_trade_notional_multiplier_exceeded)

    cap_abs = float(cfg.budget.min_trade_notional_cap_abs)
    cap_eq = 0.0
    try:
        if equity_est is not None:
            cap_eq = float(cfg.budget.min_trade_notional_cap_equity_ratio) * float(equity_est)
    except Exception:
        cap_eq = 0.0
    cap = max(cap_abs, cap_eq)

    eff = min(base * mult, cap)
    patch.update(
        {
            "min_trade_notional_multiplier": mult,
            "min_trade_notional_cap": cap,
            "min_trade_notional_effective": eff,
        }
    )
    return float(eff), patch
