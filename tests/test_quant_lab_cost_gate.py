from __future__ import annotations

from configs.schema import AppConfig
from src.core.models import Order
from src.quant_lab_client.cost_gate import apply_quant_lab_cost_gate, local_cost_bps_for_order
from src.quant_lab_client.models import CostEstimate


def _cfg() -> AppConfig:
    cfg = AppConfig()
    cfg.quant_lab.min_cost_bps_floor = 5.0
    cfg.quant_lab.cost_min_edge_multiplier = 1.5
    cfg.execution.fee_bps = 6.0
    cfg.execution.slippage_bps = 5.0
    return cfg


def test_cost_gate_filters_low_edge() -> None:
    order = Order(
        "BTC/USDT",
        "buy",
        "OPEN_LONG",
        100.0,
        100.0,
        {"expected_edge_bps": 10.0, "expected_edge_source": "final_score_proxy"},
    )
    estimate = CostEstimate(symbol="BTC-USDT", regime="normal", total_cost_bps=1.0, source="public_spread_proxy")
    result = apply_quant_lab_cost_gate(order, estimate, _cfg())

    assert result.total_cost_bps == 1.0
    assert result.local_cost_bps == 22.0
    assert result.local_cost_source == "roundtrip_fee_slippage"
    assert result.effective_total_cost_bps == 22.0
    assert result.min_required_edge_bps == 33.0
    assert result.expected_edge_source == "final_score_proxy"
    assert result.filtered is True


def test_cost_gate_uses_roundtrip_all_in_cost_with_local_floor() -> None:
    cfg = AppConfig()
    cfg.execution.cost_aware_roundtrip_cost_bps = 30
    order = Order("BTC/USDT", "buy", "OPEN_LONG", 100.0, 100.0, {"expected_edge_bps": 60.0})
    estimate = CostEstimate.from_payload(
        {
            "symbol": "BTC-USDT",
            "regime": "normal",
            "one_way_all_in_cost_bps": 12,
            "roundtrip_all_in_cost_bps": 24,
            "source": "public_spread_proxy",
            "cost_quality": "proxy",
            "cost_trusted_for_paper": True,
            "cost_trusted_for_live": False,
            "cost_trusted_for_live_canary": False,
            "cost_trusted_for_live_scale": False,
            "cost_trust_level": "PAPER_ONLY",
            "cost_trust_block_reasons": ["fallback_not_live_safe"],
            "live_cost_sample_count": 12,
            "trusted_live_sample_count": 0,
        }
    )

    result = apply_quant_lab_cost_gate(order, estimate, cfg)

    assert result.one_way_all_in_cost_bps == 12.0
    assert result.roundtrip_all_in_cost_bps == 24.0
    assert result.selected_entry_gate_cost_bps == 30.0
    assert result.effective_total_cost_bps == 30.0
    assert result.min_required_edge_bps == 45.0
    assert result.cost_quality == "proxy"
    assert result.cost_trusted_for_paper is True
    assert result.cost_trusted_for_live is False
    assert result.cost_trusted_for_live_canary is False
    assert result.cost_trusted_for_live_scale is False
    assert result.cost_trust_level == "PAPER_ONLY"
    assert result.live_cost_sample_count == 12
    assert result.trusted_live_sample_count == 0


def test_cost_gate_uses_higher_roundtrip_all_in_cost_over_local_floor() -> None:
    cfg = AppConfig()
    cfg.execution.cost_aware_roundtrip_cost_bps = 30
    order = Order("BTC/USDT", "buy", "OPEN_LONG", 100.0, 100.0, {"expected_edge_bps": 80.0})
    estimate = CostEstimate.from_payload(
        {
            "symbol": "BTC-USDT",
            "regime": "normal",
            "roundtrip_all_in_cost_bps": 45,
            "source": "mixed_actual_proxy",
        }
    )

    result = apply_quant_lab_cost_gate(order, estimate, cfg)

    assert result.roundtrip_all_in_cost_bps == 45.0
    assert result.selected_entry_gate_cost_bps == 45.0
    assert result.effective_total_cost_bps == 45.0
    assert result.min_required_edge_bps == 67.5


def test_cost_gate_allows_high_edge_and_missing_edge() -> None:
    cfg = _cfg()
    estimate = CostEstimate(symbol="BTC-USDT", regime="normal", total_cost_bps=5.0, source="public_spread_proxy")
    high_edge = apply_quant_lab_cost_gate(
        Order("BTC/USDT", "buy", "OPEN_LONG", 100.0, 100.0, {"expected_edge_bps": 40.0}),
        estimate,
        cfg,
    )
    missing = apply_quant_lab_cost_gate(Order("BTC/USDT", "buy", "OPEN_LONG", 100.0, 100.0, {}), estimate, cfg)

    assert high_edge.passed is True
    assert missing.passed is False
    assert missing.filtered is True
    assert missing.reason == "expected_edge_missing_no_filter"


def test_local_cost_defaults_to_roundtrip_fee_slippage() -> None:
    cfg = AppConfig()
    cfg.execution.fee_bps = 10
    cfg.execution.slippage_bps = 5
    cfg.execution.cost_aware_roundtrip_cost_bps = None
    order = Order("BTC/USDT", "buy", "OPEN_LONG", 100.0, 100.0, {})

    result = apply_quant_lab_cost_gate(
        order,
        CostEstimate(symbol="BTC-USDT", regime="normal", total_cost_bps=1.0),
        cfg,
    )

    assert local_cost_bps_for_order(order, cfg) == 30.0
    assert result.local_cost_bps == 30.0
    assert result.local_cost_source == "roundtrip_fee_slippage"


def test_enforce_missing_edge_buy_blocks_and_sell_close_does_not() -> None:
    cfg = AppConfig()
    cfg.quant_lab.mode = "enforce"
    cfg.execution.fee_bps = 10
    cfg.execution.slippage_bps = 5
    cfg.execution.cost_aware_roundtrip_cost_bps = None
    estimate = CostEstimate(symbol="BTC-USDT", regime="normal", total_cost_bps=1.0)

    buy = apply_quant_lab_cost_gate(
        Order("BTC/USDT", "buy", "OPEN_LONG", 100.0, 100.0, {}),
        estimate,
        cfg,
        mode="enforce",
    )
    close = apply_quant_lab_cost_gate(
        Order("BTC/USDT", "sell", "CLOSE_LONG", 100.0, 100.0, {}),
        estimate,
        cfg,
        mode="enforce",
    )

    assert buy.filtered is True
    assert buy.reason == "expected_edge_missing_block"
    assert buy.local_cost_bps == 30.0
    assert close.filtered is False
    assert close.reason == "expected_edge_missing_close_no_filter"


def test_missing_edge_can_use_score_proxy() -> None:
    cfg = AppConfig()
    cfg.quant_lab.mode = "enforce"
    cfg.quant_lab.cost_missing_edge_policy["enforce"] = "use_score_proxy"
    cfg.execution.cost_aware_score_per_bps = 0.0025
    cfg.execution.cost_aware_min_score_floor = 0.08
    cfg.execution.cost_aware_roundtrip_cost_bps = 30
    order = Order("BTC/USDT", "buy", "OPEN_LONG", 100.0, 100.0, {"final_score": 0.2})

    result = apply_quant_lab_cost_gate(
        order,
        CostEstimate(symbol="BTC-USDT", regime="normal", total_cost_bps=1.0),
        cfg,
        mode="enforce",
    )

    assert result.filtered is False
    assert result.reason == "cost_gate_proxy_passed"
    assert result.expected_edge_bps == 48.0
    assert result.expected_edge_source == "order.meta.final_score"
    assert result.proxy_source == "order.meta.final_score"


def test_local_cost_uses_configured_cost_aware_roundtrip() -> None:
    cfg = AppConfig()
    cfg.execution.fee_bps = 10
    cfg.execution.slippage_bps = 5
    cfg.execution.cost_aware_roundtrip_cost_bps = 30
    order = Order("BTC/USDT", "buy", "OPEN_LONG", 100.0, 100.0, {})

    result = apply_quant_lab_cost_gate(
        order,
        CostEstimate(symbol="BTC-USDT", regime="normal", total_cost_bps=1.0),
        cfg,
    )

    assert result.local_cost_bps == 30.0
    assert result.local_cost_source == "execution.cost_aware_roundtrip_cost_bps"


def test_local_cost_uses_order_meta_roundtrip_first() -> None:
    cfg = AppConfig()
    cfg.execution.fee_bps = 10
    cfg.execution.slippage_bps = 5
    cfg.execution.cost_aware_roundtrip_cost_bps = 30
    order = Order("BTC/USDT", "buy", "OPEN_LONG", 100.0, 100.0, {"local_roundtrip_cost_bps": 40})

    result = apply_quant_lab_cost_gate(
        order,
        CostEstimate(symbol="BTC-USDT", regime="normal", total_cost_bps=1.0),
        cfg,
    )

    assert result.local_cost_bps == 40.0
    assert result.local_cost_source == "order_meta.local_roundtrip_cost_bps"
