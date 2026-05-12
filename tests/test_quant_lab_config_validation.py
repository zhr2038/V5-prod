from __future__ import annotations

from pathlib import Path

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


def test_quant_lab_shadow_allows_local_fallback() -> None:
    qcfg = QuantLabConfig(mode="shadow", fail_policy="allow_local_fallback")

    assert qcfg.mode == "shadow"
    assert qcfg.fail_policy == "allow_local_fallback"
    assert qcfg.allow_local_fallback_in_enforce is False


def test_quant_lab_enforce_disallows_local_fallback_by_default() -> None:
    with pytest.raises(ValueError, match="allow_local_fallback"):
        QuantLabConfig(mode="enforce", fail_policy="allow_local_fallback")


def test_quant_lab_enforce_allows_local_fallback_with_explicit_override() -> None:
    qcfg = QuantLabConfig(
        mode="enforce",
        fail_policy="allow_local_fallback",
        allow_local_fallback_in_enforce=True,
    )

    assert qcfg.allow_local_fallback_in_enforce is True
    assert qcfg.fail_policy == "allow_local_fallback"


def test_quant_lab_permission_only_disallows_allow_alias_by_default() -> None:
    with pytest.raises(ValueError, match="allow_local_fallback"):
        QuantLabConfig(mode="permission_only", fail_policy="allow")


def test_quant_lab_enforce_sell_only_allowed() -> None:
    qcfg = QuantLabConfig(mode="enforce", fail_policy="sell_only")

    assert qcfg.mode == "enforce"
    assert qcfg.fail_policy == "sell_only"


def test_quant_lab_permission_only_abort_allowed() -> None:
    qcfg = QuantLabConfig(mode="permission_only", fail_policy="abort")

    assert qcfg.mode == "permission_only"
    assert qcfg.fail_policy == "abort"


def test_live_prod_explicitly_enables_quant_lab_shadow() -> None:
    cfg = load_config("configs/live_prod.yaml")

    assert cfg.quant_lab.enabled is True
    assert cfg.quant_lab.mode == "shadow"
    assert cfg.quant_lab.allow_insecure_http_with_token is False


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


def test_main_injects_runtime_override_mode_into_execution(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "shadow"
    cfg.quant_lab.runtime_override_path = str(tmp_path / "quant_lab_mode.json")
    (tmp_path / "quant_lab_mode.json").write_text(
        '{"mode":"enforce","reason":"test","updated_by":"test","updated_at":"2026-05-11T00:00:00Z"}',
        encoding="utf-8",
    )

    main_module._inject_quant_lab_execution_runtime_settings(cfg)

    assert cfg.execution.quant_lab_effective_enabled is True
    assert cfg.execution.quant_lab_effective_mode == "enforce"
    assert cfg.execution.quant_lab_config_source == "top_level"


def test_main_injects_unknown_mode_for_legacy_without_staged_mode() -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = False
    cfg.execution.quant_lab_enabled = True
    cfg.execution.quant_lab_mode = ""

    main_module._inject_quant_lab_execution_runtime_settings(cfg)

    assert cfg.execution.quant_lab_effective_enabled is True
    assert cfg.execution.quant_lab_effective_mode == "unknown"
    assert cfg.execution.quant_lab_config_source == "execution_legacy"
