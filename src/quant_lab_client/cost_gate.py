from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Optional

from .models import CostEstimate
from .return_contract import ForecastError, finite_number, read_return_forecast


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
    local_cost_bps: float
    local_cost_source: str
    fallback_level: Optional[str]
    source: Optional[str]
    sample_count: Optional[int]
    cost_model_version: Optional[str]
    expected_edge_bps: Optional[float]
    min_required_edge_bps: Optional[float]
    expected_edge_source: Optional[str] = None
    proxy_source: Optional[str] = None
    expected_gross_return_bps: Optional[float] = None
    expected_net_return_bps: Optional[float] = None
    roundtrip_cost_bps: Optional[float] = None
    horizon: Optional[str] = None
    cost_basis: Optional[str] = None
    forecast_version: Optional[str] = None
    one_way_all_in_cost_bps: Optional[float] = None
    roundtrip_all_in_cost_bps: Optional[float] = None
    selected_entry_gate_cost_bps: Optional[float] = None
    cost_quality: Optional[str] = None
    cost_trusted_for_paper: Optional[bool] = None
    cost_trusted_for_live: Optional[bool] = None
    cost_trusted_for_live_canary: Optional[bool] = None
    cost_trusted_for_live_scale: Optional[bool] = None
    cost_trust_level: Optional[str] = None
    live_cost_sample_count: Optional[int] = None
    trusted_live_sample_count: Optional[int] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _cfg_value(cfg: Any, name: str, default: Any) -> Any:
    quant_lab = getattr(cfg, "quant_lab", cfg)
    return getattr(quant_lab, name, default)


def _order_expected_edge_bps(order: Any) -> Optional[float]:
    value, _source = _order_expected_edge(order)
    return value


def _order_expected_edge(order: Any) -> tuple[Optional[float], Optional[str]]:
    meta = dict(getattr(order, "meta", None) or {})
    try:
        forecast = read_return_forecast(meta)
    except ForecastError:
        return None, "invalid_return_contract"
    if forecast is None:
        return None, None
    return forecast.expected_gross_return_bps, forecast.forecast_version


def order_expected_edge_detail(order: Any) -> tuple[Optional[float], Optional[str]]:
    return _order_expected_edge(order)


def _score_proxy_edge_bps(order: Any, cfg: Any) -> tuple[Optional[float], Optional[str]]:
    meta = dict(getattr(order, "meta", None) or {})
    proxy = _safe_non_negative_float(meta.get("expected_edge_bps_proxy"))
    if proxy is not None:
        return proxy, "order.meta.expected_edge_bps_proxy"
    execution = getattr(cfg, "execution", cfg)
    score_per_bps = _safe_non_negative_float(getattr(execution, "cost_aware_score_per_bps", None))
    if score_per_bps is None or score_per_bps <= 0:
        return None, None
    score_floor = _safe_non_negative_float(getattr(execution, "cost_aware_min_score_floor", 0.0)) or 0.0
    for key in ("final_score", "alpha6_score"):
        score = _safe_non_negative_float(meta.get(key))
        if score is not None:
            return max(0.0, score - score_floor) / score_per_bps, f"order.meta.{key}"
    return None, None


def _is_close_or_reduce(order: Any) -> bool:
    meta = dict(getattr(order, "meta", None) or {})
    if meta.get("reduce_only") is True:
        return True
    side = str(getattr(order, "side", "") or "").lower()
    intent = str(getattr(order, "intent", "") or "").upper()
    return side == "sell" or intent in {"CLOSE_LONG", "CLOSE", "REDUCE_ONLY"}


def _is_missing_edge_block_candidate(order: Any) -> bool:
    if _is_close_or_reduce(order):
        return False
    side = str(getattr(order, "side", "") or "").lower()
    intent = str(getattr(order, "intent", "") or "").upper()
    return side == "buy" or intent in {"OPEN_LONG", "REBALANCE"}


def _missing_edge_policy(cfg: Any, mode: str) -> str:
    policy_map = _cfg_value(cfg, "cost_missing_edge_policy", {}) or {}
    mode_key = str(mode or _cfg_value(cfg, "mode", "shadow") or "shadow").strip().lower().replace("-", "_")
    fallback = "record_only"
    if mode_key in {"cost_only", "enforce"}:
        fallback = "block"
    value = dict(policy_map).get(mode_key, fallback)
    policy = str(value or fallback).strip().lower().replace("-", "_")
    return policy if policy in {"record_only", "block", "use_score_proxy"} else fallback


def _safe_non_negative_float(value: Any) -> Optional[float]:
    try:
        return finite_number(value, minimum=0, maximum=100000)
    except ForecastError:
        return None


def local_cost_detail_for_order(order: Any, cfg: Any) -> tuple[float, str]:
    execution = getattr(cfg, "execution", cfg)
    meta = dict(getattr(order, "meta", None) or {})

    meta_roundtrip = _safe_non_negative_float(meta.get("local_roundtrip_cost_bps"))
    if meta_roundtrip is not None:
        return meta_roundtrip, "order_meta.local_roundtrip_cost_bps"

    configured_roundtrip = _safe_non_negative_float(getattr(execution, "cost_aware_roundtrip_cost_bps", None))
    if configured_roundtrip is not None:
        return configured_roundtrip, "execution.cost_aware_roundtrip_cost_bps"

    fee = _safe_non_negative_float(meta.get("local_fee_bps", getattr(execution, "fee_bps", 0.0))) or 0.0
    slippage = _safe_non_negative_float(meta.get("local_slippage_bps", getattr(execution, "slippage_bps", 0.0))) or 0.0
    return 2.0 * (fee + slippage), "roundtrip_fee_slippage"


def local_cost_bps_for_order(order: Any, cfg: Any) -> float:
    local_cost, _source = local_cost_detail_for_order(order, cfg)
    return local_cost


def apply_quant_lab_cost_gate(order: Any, cost_estimate: CostEstimate, cfg: Any, *, mode: Optional[str] = None) -> CostGateResult:
    # This is the sole entry-return validation boundary for every guard mode.
    meta = dict(getattr(order, "meta", None) or {})
    local_cost, local_source = local_cost_detail_for_order(order, cfg)
    def safe(value):
        return _safe_non_negative_float(value) or 0.0
    result = CostGateResult(
        passed=False, filtered=True, reason="expected_edge_missing_no_filter",
        symbol=cost_estimate.symbol, regime=cost_estimate.regime,
        notional_usdt=safe(cost_estimate.notional_usdt), quantile=cost_estimate.quantile,
        fee_bps=safe(cost_estimate.fee_bps), slippage_bps=safe(cost_estimate.slippage_bps),
        spread_bps=safe(cost_estimate.spread_bps), total_cost_bps=safe(cost_estimate.total_cost_bps),
        effective_total_cost_bps=0.0, local_cost_bps=local_cost, local_cost_source=local_source,
        fallback_level=cost_estimate.fallback_level, source=cost_estimate.source,
        sample_count=cost_estimate.sample_count, cost_model_version=cost_estimate.cost_model_version,
        expected_edge_bps=None, min_required_edge_bps=None,
    )
    for name in ("cost_quality", "cost_trusted_for_paper", "cost_trusted_for_live",
                 "cost_trusted_for_live_canary", "cost_trusted_for_live_scale", "cost_trust_level",
                 "live_cost_sample_count", "trusted_live_sample_count"):
        setattr(result, name, getattr(cost_estimate, name, None))
    if _is_close_or_reduce(order):
        result.passed, result.filtered = True, False
        result.reason = "expected_edge_missing_close_no_filter" if not meta else "entry_forecast_not_required_for_reduction"
        return result
    try:
        min_floor = finite_number(_cfg_value(cfg, "min_cost_bps_floor", 5.0), minimum=0, maximum=10000)
        multiplier = finite_number(_cfg_value(cfg, "cost_min_edge_multiplier", 1.5), minimum=1, maximum=100)
        execution = getattr(cfg, "execution", cfg)
        # Reject malformed supplied cost inputs; do not quietly fall through to a cheaper source.
        for key in ("local_roundtrip_cost_bps", "local_fee_bps", "local_slippage_bps"):
            if key in meta:
                finite_number(meta[key], minimum=0, maximum=10000)
        for key in ("cost_aware_roundtrip_cost_bps", "fee_bps", "slippage_bps"):
            value = getattr(execution, key, None)
            if value is not None:
                finite_number(value, minimum=0, maximum=10000)
        for key in ("total_cost_bps", "fee_bps", "slippage_bps", "spread_bps"):
            finite_number(getattr(cost_estimate, key, 0.0), minimum=0, maximum=10000)
        for key in ("one_way_all_in_cost_bps", "roundtrip_all_in_cost_bps"):
            value = getattr(cost_estimate, key, None)
            if value is not None:
                setattr(result, key, finite_number(value, minimum=0, maximum=10000))
        response_cost = result.roundtrip_all_in_cost_bps
        if response_cost is None:
            response_cost = result.total_cost_bps  # Frozen legacy cost API adapter, not a forecast alias.
        result.selected_entry_gate_cost_bps = max(response_cost, local_cost)
        effective_cost = max(result.selected_entry_gate_cost_bps, min_floor)
        result.effective_total_cost_bps = effective_cost
        result.roundtrip_cost_bps = effective_cost
        result.min_required_edge_bps = effective_cost * multiplier
    except ForecastError as exc:
        result.reason = f"invalid_entry_cost:{exc}"
        return result
    try:
        forecast = read_return_forecast(meta)
    except ForecastError as exc:
        result.reason = f"invalid_expected_return:{exc}"
        return result
    if forecast is not None:
        result.expected_gross_return_bps = forecast.expected_gross_return_bps
        result.expected_net_return_bps = forecast.net_at_cost(effective_cost)
        result.expected_edge_bps = forecast.expected_gross_return_bps  # Compatibility output is explicitly gross.
        result.expected_edge_source = str(meta.get("expected_edge_source") or forecast.forecast_version)
        result.horizon, result.cost_basis = forecast.horizon, forecast.cost_basis
        result.forecast_version = forecast.forecast_version
        result.filtered = result.expected_net_return_bps < effective_cost * (multiplier - 1.0)
        result.passed = not result.filtered
        result.reason = "cost_edge_insufficient" if result.filtered else "cost_gate_passed"
        return result
    mode_value = str(mode or _cfg_value(cfg, "mode", "shadow") or "shadow").strip().lower().replace("-", "_")
    policy = _missing_edge_policy(cfg, mode_value)
    if policy == "use_score_proxy":
        try:
            for key in ("expected_edge_bps_proxy", "final_score", "alpha6_score"):
                if key in meta:
                    finite_number(meta[key], minimum=0 if key.endswith("proxy") else -100, maximum=100000)
        except ForecastError as exc:
            result.reason = f"invalid_score_proxy:{exc}"
            return result
        proxy, source = _score_proxy_edge_bps(order, cfg)
        if proxy is not None:
            try:
                proxy = finite_number(proxy, minimum=0, maximum=100000)
            except ForecastError as exc:
                result.reason = f"invalid_score_proxy:{exc}"
                return result
            result.expected_edge_bps, result.expected_edge_source, result.proxy_source = proxy, source, source
            result.filtered = proxy < result.min_required_edge_bps
            result.passed = not result.filtered
            result.reason = "cost_edge_proxy_insufficient" if result.filtered else "cost_gate_proxy_passed"
            return result
        policy = "block" if mode_value in {"cost_only", "enforce"} else "record_only"
    if policy == "block" and _is_missing_edge_block_candidate(order):
        result.reason = "expected_edge_missing_block"
    return result
