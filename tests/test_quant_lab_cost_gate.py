from __future__ import annotations

from configs.schema import AppConfig
import pytest

from src.core.models import Order
from src.quant_lab_client.cost_gate import apply_quant_lab_cost_gate, local_cost_bps_for_order
from src.quant_lab_client.models import CostEstimate


def gross(value, **extra):
    return {"expected_gross_return_bps": value, "roundtrip_cost_bps": 30.0,
            "horizon": "24h", "cost_basis": "roundtrip_all_in_quote_bps",
            "forecast_version": "unit-test-v1", **extra}


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
        gross(10.0, expected_edge_source="test_gross"),
    )
    estimate = CostEstimate(symbol="BTC-USDT", regime="normal", total_cost_bps=1.0, source="public_spread_proxy")
    result = apply_quant_lab_cost_gate(order, estimate, _cfg())

    assert result.total_cost_bps == 1.0
    assert result.local_cost_bps == 22.0
    assert result.local_cost_source == "roundtrip_fee_slippage"
    assert result.effective_total_cost_bps == 22.0
    assert result.min_required_edge_bps == 33.0
    assert result.expected_edge_source == "test_gross"
    assert result.filtered is True


def test_cost_gate_uses_roundtrip_all_in_cost_with_local_floor() -> None:
    cfg = AppConfig()
    cfg.execution.cost_aware_roundtrip_cost_bps = 30
    order = Order("BTC/USDT", "buy", "OPEN_LONG", 100.0, 100.0, gross(60.0))
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
    order = Order("BTC/USDT", "buy", "OPEN_LONG", 100.0, 100.0, gross(80.0))
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
        Order("BTC/USDT", "buy", "OPEN_LONG", 100.0, 100.0, gross(40.0)),
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


@pytest.mark.parametrize("value", [None, True, False, "invalid", float("nan"), float("inf"), -float("inf"), 100001, -10001])
@pytest.mark.parametrize("field", ["expected_gross_return_bps", "expected_net_return_bps"])
def test_invalid_forecast_never_passes_entry(value, field):
    meta = gross(50)
    meta.pop("expected_gross_return_bps")
    meta[field] = value
    estimate = CostEstimate(symbol="BTC-USDT", total_cost_bps=30)
    buy = apply_quant_lab_cost_gate(Order("BTC/USDT", "buy", "OPEN_LONG", 100, 100, meta), estimate, _cfg(), mode="enforce")
    assert buy.filtered and not buy.passed and buy.reason.startswith("invalid_expected_return:")
    sell = apply_quant_lab_cost_gate(Order("BTC/USDT", "sell", "CLOSE_LONG", 100, 100, meta), estimate, _cfg(), mode="enforce")
    assert sell.passed and not sell.filtered


def test_gross_and_net_inputs_subtract_cost_once():
    cfg = _cfg()
    estimate = CostEstimate(symbol="BTC-USDT", total_cost_bps=30)
    gross_meta = gross(50)
    net_meta = gross_meta.copy()
    net_meta.pop("expected_gross_return_bps")
    net_meta["expected_net_return_bps"] = 20
    results = [apply_quant_lab_cost_gate(Order("BTC/USDT", "buy", "OPEN_LONG", 100, 100, meta), estimate, cfg) for meta in (gross_meta, net_meta)]
    for result in results:
        assert result.passed
        assert result.expected_gross_return_bps == 50
        assert result.expected_net_return_bps == 20
        assert result.roundtrip_cost_bps == 30
    # Reprice the same forecast against higher current cost, once.
    estimate.total_cost_bps = 40
    result = apply_quant_lab_cost_gate(Order("BTC/USDT", "buy", "OPEN_LONG", 100, 100, net_meta), estimate, cfg)
    assert result.expected_net_return_bps == 10 and result.filtered


@pytest.mark.parametrize("patch", [
    {"cost_basis": "one_way"}, {"return_unit": "percent"}, {"decision_horizon": "4h"},
    {"horizon": None}, {"forecast_version": ""}, {"roundtrip_cost_bps": True},
    {"expected_net_return_bps": 21},
])
def test_forecast_contract_mismatches_block(patch):
    result = apply_quant_lab_cost_gate(Order("BTC/USDT", "buy", "OPEN_LONG", 100, 100, gross(50, **patch)), CostEstimate(symbol="BTC-USDT", total_cost_bps=30), _cfg())
    assert result.filtered and result.reason.startswith("invalid_expected_return:")


@pytest.mark.parametrize("edge,passed", [(-10000, False), (-1, False), (0, False), (44.9999, False), (45, True), (100000, True)])
def test_valid_return_boundaries(edge, passed):
    result = apply_quant_lab_cost_gate(Order("BTC/USDT", "buy", "OPEN_LONG", 100, 100, gross(edge)), CostEstimate(symbol="BTC-USDT", total_cost_bps=30), _cfg())
    assert result.passed is passed


@pytest.mark.parametrize("key", ["expected_edge_bps", "expected_net_edge_bps", "edge_bps"])
@pytest.mark.parametrize("value", [50, float("nan"), float("inf"), True, "invalid", None])
def test_legacy_aliases_cannot_guess_forecast_semantics(key, value):
    result = apply_quant_lab_cost_gate(Order("BTC/USDT", "buy", "OPEN_LONG", 100, 100, {key: value}), CostEstimate(symbol="BTC-USDT", total_cost_bps=30), _cfg())
    assert result.filtered and result.reason.startswith("invalid_expected_return:")
