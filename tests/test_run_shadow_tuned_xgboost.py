from __future__ import annotations

from pathlib import Path

import scripts.run_shadow_tuned_xgboost as shadow_mod
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


def test_resolve_shadow_base_config_path_uses_runtime_helper(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(shadow_mod, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        shadow_mod,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str((tmp_path / "configs" / "runtime.yaml").resolve()),
    )

    assert shadow_mod._resolve_shadow_base_config_path() == (tmp_path / "configs" / "runtime.yaml").resolve()


def test_resolve_shadow_base_config_path_prefers_explicit_arg(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(shadow_mod, "PROJECT_ROOT", tmp_path)

    def _fake_resolver(raw_config_path=None, project_root=None):
        assert raw_config_path == "configs/prod_alt.yaml"
        return str((tmp_path / "configs" / "prod_alt.yaml").resolve())

    monkeypatch.setattr(shadow_mod, "resolve_runtime_config_path", _fake_resolver)

    assert shadow_mod._resolve_shadow_base_config_path("configs/prod_alt.yaml") == (
        tmp_path / "configs" / "prod_alt.yaml"
    ).resolve()
