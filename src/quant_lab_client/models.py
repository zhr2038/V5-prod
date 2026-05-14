from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Mapping, Optional


def _payload(data: Any) -> Mapping[str, Any]:
    if isinstance(data, Mapping):
        for key in ("data", "result", "payload"):
            nested = data.get(key)
            if isinstance(nested, Mapping):
                return nested
        return data
    return {}


def _float(data: Mapping[str, Any], key: str, default: Optional[float] = None) -> Optional[float]:
    value = data.get(key)
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _first_float(data: Mapping[str, Any], *keys: str, default: Optional[float] = None) -> Optional[float]:
    for key in keys:
        value = _float(data, key)
        if value is not None:
            return value
    return default


def _list(data: Mapping[str, Any], key: str) -> List[Any]:
    value = data.get(key)
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


_QUOTE_SUFFIXES = ("USDT", "USDC", "USD", "BTC", "ETH", "OKB")


def symbol_to_quant_lab_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper().replace("_", "-").replace("/", "-")
    if not raw:
        return ""
    if "-" in raw:
        parts = [part for part in raw.split("-") if part]
        return "-".join(parts) if parts else raw
    for quote in _QUOTE_SUFFIXES:
        if raw.endswith(quote) and len(raw) > len(quote):
            return f"{raw[:-len(quote)]}-{quote}"
    return raw


@dataclass
class QuantLabHealth:
    status: str = ""
    service: str = ""
    mode: str = ""

    @classmethod
    def from_payload(cls, payload: Any) -> "QuantLabHealth":
        data = _payload(payload)
        return cls(
            status=str(data.get("status") or ""),
            service=str(data.get("service") or ""),
            mode=str(data.get("mode") or ""),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class RiskPermission:
    strategy: str = ""
    version: str = ""
    permission: str = "ABORT"
    allowed_modes: List[Any] = field(default_factory=list)
    max_gross_exposure: Optional[float] = None
    max_single_weight: Optional[float] = None
    max_gross_exposure_usdt: Optional[float] = None
    max_single_order_usdt: Optional[float] = None
    enforceable: Optional[bool] = None
    cost_model_version: Optional[str] = None
    gate_version: Optional[str] = None
    reasons: List[Any] = field(default_factory=list)
    risk_reason_codes: List[Any] = field(default_factory=list)
    created_at: Optional[str] = None
    as_of_ts: Optional[str] = None
    expires_at: Optional[str] = None
    status: Optional[str] = None
    permission_status: Optional[str] = None
    source_bundle_ts: Optional[str] = None
    telemetry_latest_ts: Optional[str] = None
    contract_version: Optional[str] = None

    @classmethod
    def from_payload(cls, payload: Any) -> "RiskPermission":
        data = _payload(payload)
        status = str(data.get("permission_status") or data.get("status") or "") or None
        raw_permission = data.get("permission") or data.get("decision")
        if not raw_permission and status:
            upper_status = str(status).upper()
            for suffix in ("ALLOW", "SELL_ONLY", "ABORT"):
                if upper_status.endswith(f"_{suffix}") or upper_status == suffix:
                    raw_permission = suffix
                    break
        return cls(
            strategy=str(data.get("strategy") or ""),
            version=str(data.get("version") or ""),
            permission=str(raw_permission or "ABORT").upper(),
            allowed_modes=_list(data, "allowed_modes"),
            max_gross_exposure=_float(data, "max_gross_exposure"),
            max_single_weight=_float(data, "max_single_weight"),
            max_gross_exposure_usdt=_first_float(data, "max_gross_exposure_usdt", "max_gross_exposure"),
            max_single_order_usdt=_first_float(data, "max_single_order_usdt", "max_single_order", "max_single_notional_usdt"),
            enforceable=bool(data.get("enforceable")) if data.get("enforceable") is not None else None,
            cost_model_version=str(data.get("cost_model_version") or "") or None,
            gate_version=str(data.get("gate_version") or "") or None,
            reasons=_list(data, "reasons") or _list(data, "reason"),
            risk_reason_codes=_list(data, "risk_reason_codes") or _list(data, "reason_codes"),
            created_at=str(data.get("created_at") or data.get("as_of_ts") or data.get("ts") or "") or None,
            as_of_ts=str(data.get("as_of_ts") or data.get("created_at") or data.get("ts") or "") or None,
            expires_at=str(data.get("expires_at") or data.get("permission_expires_at") or "") or None,
            status=status,
            permission_status=status,
            source_bundle_ts=str(data.get("source_bundle_ts") or "") or None,
            telemetry_latest_ts=str(data.get("telemetry_latest_ts") or "") or None,
            contract_version=str(data.get("contract_version") or "") or None,
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CostEstimate:
    symbol: str = ""
    regime: str = ""
    notional_usdt: float = 0.0
    quantile: str = "p75"
    fee_bps: float = 0.0
    slippage_bps: float = 0.0
    spread_bps: float = 0.0
    total_cost_bps: float = 0.0
    cost_bps: float = 0.0
    fallback_level: Optional[str] = None
    source: Optional[str] = None
    sample_count: Optional[int] = None
    cost_model_version: Optional[str] = None
    bucket_id: Optional[str] = None
    matched_regime: Optional[str] = None
    as_of_ts: Optional[str] = None
    total_cost_bps_p50: Optional[float] = None
    total_cost_bps_p75: Optional[float] = None
    total_cost_bps_p90: Optional[float] = None
    required_edge_bps: Optional[float] = None
    fallback_reason: Optional[str] = None
    raw_response: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: Any) -> "CostEstimate":
        data = _payload(payload)
        total = _float(data, "total_cost_bps")
        if total is None:
            total = _float(data, "total_bps")
        if total is None:
            total = _float(data, "cost_bps", 0.0)
        sample_count = data.get("sample_count")
        try:
            sample_count_i = int(sample_count) if sample_count is not None and sample_count != "" else None
        except (TypeError, ValueError):
            sample_count_i = None
        cost_bps = _float(data, "cost_bps")
        return cls(
            symbol=str(data.get("symbol") or ""),
            regime=str(data.get("regime") or data.get("matched_regime") or ""),
            notional_usdt=float(_float(data, "notional_usdt", 0.0) or 0.0),
            quantile=str(data.get("quantile") or "p75"),
            fee_bps=float(_float(data, "fee_bps", 0.0) or 0.0),
            slippage_bps=float(_float(data, "slippage_bps", 0.0) or 0.0),
            spread_bps=float(_float(data, "spread_bps", 0.0) or 0.0),
            total_cost_bps=float(total or 0.0),
            cost_bps=float(cost_bps if cost_bps is not None else (total or 0.0)),
            fallback_level=str(data.get("fallback_level") or "") or None,
            source=str(data.get("source") or data.get("cost_source") or "") or None,
            sample_count=sample_count_i,
            cost_model_version=str(data.get("cost_model_version") or "") or None,
            bucket_id=str(data.get("bucket_id") or "") or None,
            matched_regime=str(data.get("matched_regime") or data.get("regime") or "") or None,
            as_of_ts=str(data.get("as_of_ts") or data.get("created_at") or data.get("ts") or "") or None,
            total_cost_bps_p50=_first_float(data, "total_cost_bps_p50", "p50_total_cost_bps", "cost_bps_p50"),
            total_cost_bps_p75=_first_float(data, "total_cost_bps_p75", "p75_total_cost_bps", "cost_bps_p75"),
            total_cost_bps_p90=_first_float(data, "total_cost_bps_p90", "p90_total_cost_bps", "cost_bps_p90"),
            required_edge_bps=_first_float(data, "required_edge_bps", "min_required_edge_bps"),
            fallback_reason=str(data.get("fallback_reason") or "") or None,
            raw_response=dict(data),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class GateDecision:
    alpha_id: str = ""
    version: Optional[str] = None
    gate_version: Optional[str] = None
    status: str = "QUARANTINE"
    passed: bool = False
    reasons: List[Any] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    next_action: Optional[str] = None
    created_at: Optional[str] = None

    @classmethod
    def from_payload(cls, payload: Any) -> "GateDecision":
        data = _payload(payload)
        return cls(
            alpha_id=str(data.get("alpha_id") or ""),
            version=str(data.get("version") or "") or None,
            gate_version=str(data.get("gate_version") or "") or None,
            status=str(data.get("status") or "QUARANTINE").upper(),
            passed=bool(data.get("passed", False)),
            reasons=_list(data, "reasons") or _list(data, "reason"),
            metrics=dict(data.get("metrics") or {}) if isinstance(data.get("metrics"), Mapping) else {},
            next_action=str(data.get("next_action") or "") or None,
            created_at=str(data.get("created_at") or data.get("ts") or "") or None,
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
