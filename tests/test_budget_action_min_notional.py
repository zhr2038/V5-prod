from __future__ import annotations

from configs.schema import AppConfig
from src.core.budget_action import effective_min_trade_notional
from src.reporting.decision_audit import DecisionAudit


def test_second_stage_triggers_on_noisy_small_median():
    cfg = AppConfig()
    cfg.budget.action_enabled = True
    cfg.budget.min_fills_for_second_stage = 5
    cfg.budget.min_trade_notional_base = 25.0
    cfg.budget.min_trade_notional_multiplier_exceeded = 2.0
    cfg.budget.min_trade_notional_cap_abs = 200.0
    cfg.budget.min_trade_notional_cap_equity_ratio = 0.01

    cfg.budget.small_trade_median_threshold_abs = 10.0
    cfg.budget.small_trade_median_threshold_equity_ratio = 0.0025

    audit = DecisionAudit(run_id="r1")
    audit.budget = {
        "exceeded": True,
        "reason": "exceeded_turnover",
        "fills_count_today": 10,
        "median_notional_usdt_today": 5.0,
        "small_trade_ratio_today": 0.1,
        "small_trade_notional_cutoff": 25.0,
        "avg_equity_est": 1000.0,
    }

    eff, patch = effective_min_trade_notional(cfg, audit)
    assert eff == 50.0
    assert patch["min_trade_notional_effective"] == 50.0


def test_second_stage_not_triggered_when_fills_too_few():
    cfg = AppConfig()
    cfg.budget.action_enabled = True
    cfg.budget.min_fills_for_second_stage = 5
    cfg.budget.min_trade_notional_base = 25.0

    audit = DecisionAudit(run_id="r1")
    audit.budget = {
        "exceeded": True,
        "fills_count_today": 3,
        "median_notional_usdt_today": 1.0,
        "small_trade_ratio_today": 0.9,
        "avg_equity_est": 1000.0,
    }

    eff, patch = effective_min_trade_notional(cfg, audit)
    assert eff == 25.0
    assert patch["min_trade_notional_effective"] == 25.0
