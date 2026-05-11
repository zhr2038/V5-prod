from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Optional

from .models import CostEstimate


@dataclass
class CostGateResult:
    passed: bool
    filtered: bool
    reason: str
    symbol: str
    regime: str
    notional_usdt: float
    quantile: str
    fee_bps: float
    slippage_bps: float
    spread_bps: float
    total_cost_bps: float
    effective_total_cost_bps: float
    fallback_level: Optional[str]
    source: Optional[str]
    sample_count: Optional[int]
    cost_model_version: Optional[str]
    expected_edge_bps: Optional[float]
    min_required_edge_bps: Optional[float]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _cfg_value(cfg: Any, name: str, default: Any) -> Any:
    quant_lab = getattr(cfg, "quant_lab", cfg)
    return getattr(quant_lab, name, default)


def _order_expected_edge_bps(order: Any) -> Optional[float]:
    meta = dict(getattr(order, "meta", None) or {})
    for key in ("expected_edge_bps", "expected_net_edge_bps", "edge_bps"):
        value = meta.get(key)
        if value is None or value == "":
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def local_cost_bps_for_order(order: Any, cfg: Any) -> float:
    execution = getattr(cfg, "execution", cfg)
    meta = dict(getattr(order, "meta", None) or {})
    fee = meta.get("local_fee_bps", getattr(execution, "fee_bps", 0.0))
    slippage = meta.get("local_slippage_bps", getattr(execution, "slippage_bps", 0.0))
    try:
        return max(0.0, float(fee or 0.0)) + max(0.0, float(slippage or 0.0))
    except (TypeError, ValueError):
        return 0.0


def apply_quant_lab_cost_gate(order: Any, cost_estimate: CostEstimate, cfg: Any) -> CostGateResult:
    min_floor = float(_cfg_value(cfg, "min_cost_bps_floor", 5.0) or 0.0)
    multiplier = float(_cfg_value(cfg, "cost_min_edge_multiplier", 1.5) or 1.5)
    local_cost = local_cost_bps_for_order(order, cfg)
    effective_total_cost_bps = max(
        float(cost_estimate.total_cost_bps or 0.0),
        min_floor,
        local_cost,
    )
    expected_edge = _order_expected_edge_bps(order)
    min_required = effective_total_cost_bps * multiplier
    if expected_edge is None:
        return CostGateResult(
            passed=True,
            filtered=False,
            reason="expected_edge_missing_no_filter",
            symbol=cost_estimate.symbol,
            regime=cost_estimate.regime,
            notional_usdt=float(cost_estimate.notional_usdt or 0.0),
            quantile=cost_estimate.quantile,
            fee_bps=float(cost_estimate.fee_bps or 0.0),
            slippage_bps=float(cost_estimate.slippage_bps or 0.0),
            spread_bps=float(cost_estimate.spread_bps or 0.0),
            total_cost_bps=float(cost_estimate.total_cost_bps or 0.0),
            effective_total_cost_bps=effective_total_cost_bps,
            fallback_level=cost_estimate.fallback_level,
            source=cost_estimate.source,
            sample_count=cost_estimate.sample_count,
            cost_model_version=cost_estimate.cost_model_version,
            expected_edge_bps=None,
            min_required_edge_bps=min_required,
        )
    filtered = expected_edge < min_required
    return CostGateResult(
        passed=not filtered,
        filtered=filtered,
        reason="cost_edge_insufficient" if filtered else "cost_gate_passed",
        symbol=cost_estimate.symbol,
        regime=cost_estimate.regime,
        notional_usdt=float(cost_estimate.notional_usdt or 0.0),
        quantile=cost_estimate.quantile,
        fee_bps=float(cost_estimate.fee_bps or 0.0),
        slippage_bps=float(cost_estimate.slippage_bps or 0.0),
        spread_bps=float(cost_estimate.spread_bps or 0.0),
        total_cost_bps=float(cost_estimate.total_cost_bps or 0.0),
        effective_total_cost_bps=effective_total_cost_bps,
        fallback_level=cost_estimate.fallback_level,
        source=cost_estimate.source,
        sample_count=cost_estimate.sample_count,
        cost_model_version=cost_estimate.cost_model_version,
        expected_edge_bps=expected_edge,
        min_required_edge_bps=min_required,
    )
