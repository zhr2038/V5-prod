from __future__ import annotations

from configs.schema import AppConfig
from src.core.models import Order
from src.quant_lab_client.cost_gate import apply_quant_lab_cost_gate
from src.quant_lab_client.models import CostEstimate


def _cfg() -> AppConfig:
    cfg = AppConfig()
    cfg.quant_lab.min_cost_bps_floor = 5.0
    cfg.quant_lab.cost_min_edge_multiplier = 1.5
    cfg.execution.fee_bps = 6.0
    cfg.execution.slippage_bps = 5.0
    return cfg


def test_cost_gate_filters_low_edge() -> None:
    order = Order("BTC/USDT", "buy", "OPEN_LONG", 100.0, 100.0, {"expected_edge_bps": 10.0})
    estimate = CostEstimate(symbol="BTC-USDT", regime="normal", total_cost_bps=1.0, source="public_spread_proxy")
    result = apply_quant_lab_cost_gate(order, estimate, _cfg())

    assert result.effective_total_cost_bps == 11.0
    assert result.min_required_edge_bps == 16.5
    assert result.filtered is True


def test_cost_gate_allows_high_edge_and_missing_edge() -> None:
    cfg = _cfg()
    estimate = CostEstimate(symbol="BTC-USDT", regime="normal", total_cost_bps=5.0, source="public_spread_proxy")
    high_edge = apply_quant_lab_cost_gate(
        Order("BTC/USDT", "buy", "OPEN_LONG", 100.0, 100.0, {"expected_edge_bps": 20.0}),
        estimate,
        cfg,
    )
    missing = apply_quant_lab_cost_gate(Order("BTC/USDT", "buy", "OPEN_LONG", 100.0, 100.0, {}), estimate, cfg)

    assert high_edge.passed is True
    assert missing.passed is True
    assert missing.reason == "expected_edge_missing_no_filter"
