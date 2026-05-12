from __future__ import annotations

import pytest

from configs.loader import load_config
from configs.schema import AppConfig, QuantLabConfig
import main as main_module
import scripts.live_preflight_once as live_preflight_once
from src.quant_lab_client.guard import QuantLabGuard


def test_invalid_quant_lab_fail_policy_raises() -> None:
    with pytest.raises(ValueError):
        QuantLabConfig(fail_policy="unsafe_allow")


def test_invalid_quant_lab_cost_quantile_raises() -> None:
    with pytest.raises(ValueError):
        QuantLabConfig(cost_quantile="p99")


def test_app_config_quant_lab_disabled_by_default() -> None:
    assert AppConfig().quant_lab.enabled is False


def test_live_prod_explicitly_enables_quant_lab_shadow() -> None:
    cfg = load_config("configs/live_prod.yaml")

    assert cfg.quant_lab.enabled is True
    assert cfg.quant_lab.mode == "shadow"


def test_guard_from_config_disabled_by_default_does_not_create_client() -> None:
    guard = QuantLabGuard.from_config(AppConfig().quant_lab, run_id="test-disabled")

    assert guard.client is None
    assert guard.called_api is False
    assert guard.permission_result.enabled is False
    assert guard.permission_result.skipped_reason == "quant_lab_disabled"


def test_top_level_quant_lab_ignores_execution_legacy_fail_policy() -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "shadow"
    cfg.quant_lab.fail_policy = "allow_local_fallback"
    cfg.execution.quant_lab_enabled = True
    cfg.execution.quant_lab_fail_policy = "sell_only"

    qcfg = main_module._get_quant_lab_cfg(cfg)
    guard = QuantLabGuard.from_config(qcfg, run_id="config-source-test")

    assert qcfg.fail_policy == "allow_local_fallback"
    assert qcfg.quant_lab_config_source == "top_level"
    assert qcfg.legacy_execution_quant_lab_ignored is True
    assert guard.audit_payload()["quant_lab_config_source"] == "top_level"
    assert guard.audit_payload()["legacy_execution_quant_lab_ignored"] is True


def test_top_level_disabled_legacy_enabled_still_compatible() -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = False
    cfg.execution.quant_lab_enabled = True
    cfg.execution.quant_lab_fail_policy = "abort"

    qcfg = main_module._get_quant_lab_cfg(cfg)

    assert qcfg.enabled is True
    assert qcfg.fail_policy == "abort"
    assert qcfg.quant_lab_config_source == "execution_legacy"
    assert qcfg.legacy_execution_quant_lab_ignored is False


def test_live_preflight_once_prefers_top_level_quant_lab() -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.fail_policy = "allow_local_fallback"
    cfg.execution.quant_lab_enabled = True
    cfg.execution.quant_lab_fail_policy = "sell_only"

    qcfg = live_preflight_once._get_quant_lab_runtime_cfg(cfg)

    assert qcfg.fail_policy == "allow_local_fallback"
    assert qcfg.quant_lab_config_source == "top_level"
    assert qcfg.legacy_execution_quant_lab_ignored is True


def test_live_prod_has_single_authoritative_quant_lab_source() -> None:
    live_text = open("configs/live_prod.yaml", encoding="utf-8").read()
    cfg = load_config("configs/live_prod.yaml")
    qcfg = main_module._get_quant_lab_cfg(cfg)

    assert "\n  quant_lab_enabled:" not in live_text
    assert "\n  quant_lab_fail_policy:" not in live_text
    assert cfg.quant_lab.enabled is True
    assert cfg.quant_lab.mode == "shadow"
    assert qcfg.quant_lab_config_source == "top_level"
    assert qcfg.legacy_execution_quant_lab_ignored is False
