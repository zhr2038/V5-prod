from __future__ import annotations

from configs.schema import AppConfig
from src.core.pipeline import _effective_deadband
from src.reporting.decision_audit import DecisionAudit


def test_effective_deadband_widens_when_budget_exceeded():
    cfg = AppConfig()
    cfg.budget.action_enabled = True
    cfg.budget.deadband_multiplier_exceeded = 1.5
    cfg.budget.deadband_cap = 0.15

    audit = DecisionAudit(run_id="r1")
    audit.budget = {"exceeded": True, "reason": "exceeded_turnover"}

    base = 0.05
    eff = _effective_deadband(base, cfg, audit)
    assert eff == min(base * 1.5, 0.15)


def test_effective_deadband_unchanged_when_not_exceeded():
    cfg = AppConfig()
    cfg.budget.action_enabled = True
    audit = DecisionAudit(run_id="r1")
    audit.budget = {"exceeded": False}

    assert _effective_deadband(0.05, cfg, audit) == 0.05
