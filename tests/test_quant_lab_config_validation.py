from __future__ import annotations

import pytest

from configs.loader import load_config
from configs.schema import AppConfig, QuantLabConfig
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
