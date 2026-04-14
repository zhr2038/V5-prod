from __future__ import annotations

from scripts.run_shadow_tuned_xgboost import SHADOW_REPORT_REDIRECTS


def test_shadow_redirects_cover_runtime_state_files() -> None:
    redirects = dict(SHADOW_REPORT_REDIRECTS)

    assert redirects["reports/trend_cache.json"] == "reports/shadow_tuned_xgboost/trend_cache.json"
    assert redirects["reports/fills.sqlite"] == "reports/shadow_tuned_xgboost/fills.sqlite"
    assert redirects["reports/bills.sqlite"] == "reports/shadow_tuned_xgboost/bills.sqlite"
    assert redirects["reports/reconcile_failure_state.json"] == "reports/shadow_tuned_xgboost/reconcile_failure_state.json"
    assert redirects["reports/ledger_state.json"] == "reports/shadow_tuned_xgboost/ledger_state.json"
    assert redirects["reports/ledger_status.json"] == "reports/shadow_tuned_xgboost/ledger_status.json"
    assert redirects["reports/take_profit_cooldown_state.json"] == "reports/shadow_tuned_xgboost/take_profit_cooldown_state.json"
    assert redirects["reports/auto_risk_eval.json"] == "reports/shadow_tuned_xgboost/auto_risk_eval.json"
