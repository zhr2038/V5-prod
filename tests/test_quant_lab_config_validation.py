from __future__ import annotations

import pytest

from configs.schema import QuantLabConfig


def test_invalid_quant_lab_fail_policy_raises() -> None:
    with pytest.raises(ValueError):
        QuantLabConfig(fail_policy="unsafe_allow")


def test_invalid_quant_lab_cost_quantile_raises() -> None:
    with pytest.raises(ValueError):
        QuantLabConfig(cost_quantile="p99")
