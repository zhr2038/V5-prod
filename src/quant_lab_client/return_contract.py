"""Explicit forecast units. Legacy aliases are diagnostics, never a forecast contract."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping


class ForecastError(ValueError):
    pass


def finite_number(value: Any, *, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or value is None:
        raise ForecastError("non_numeric")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ForecastError("non_numeric") from exc
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise ForecastError("non_finite_or_out_of_range")
    return result


@dataclass(frozen=True)
class ReturnForecast:
    expected_gross_return_bps: float
    roundtrip_cost_bps: float
    horizon: str
    cost_basis: str
    forecast_version: str

    def net_at_cost(self, cost_bps: float) -> float:
        return self.expected_gross_return_bps - finite_number(cost_bps, minimum=0, maximum=10000)


def read_return_forecast(meta: Mapping[str, Any]) -> ReturnForecast | None:
    gross_key, net_key = "expected_gross_return_bps", "expected_net_return_bps"
    if gross_key not in meta and net_key not in meta:
        # An explicitly labelled score proxy is not an expected return.
        if meta.get("expected_edge_source") in {"score_proxy", "final_score_proxy"}:
            for key in ("expected_edge_bps", "expected_edge_bps_proxy"):
                if key in meta:
                    finite_number(meta[key], minimum=0, maximum=100000)
            return None
        for key in ("expected_edge_bps", "expected_net_edge_bps", "edge_bps"):
            if key in meta:
                finite_number(meta[key], minimum=-10000, maximum=100000)
                raise ForecastError("legacy_return_semantics_missing")
        return None
    if meta.get("return_unit", "bps") != "bps":
        raise ForecastError("return_unit_mismatch")
    if meta.get("cost_basis") != "roundtrip_all_in_quote_bps":
        raise ForecastError("cost_basis_missing_or_mismatch")
    horizon = meta.get("horizon")
    if not isinstance(horizon, str) or horizon not in {"1h", "4h", "8h", "24h", "72h"}:
        raise ForecastError("horizon_missing_or_invalid")
    if meta.get("decision_horizon", horizon) != horizon:
        raise ForecastError("horizon_mismatch")
    version = meta.get("forecast_version")
    if not isinstance(version, str) or not version.strip() or len(version) > 128:
        raise ForecastError("forecast_version_missing_or_invalid")
    cost = finite_number(meta.get("roundtrip_cost_bps"), minimum=0, maximum=10000)
    gross = finite_number(meta[gross_key], minimum=-10000, maximum=100000) if gross_key in meta else None
    net = finite_number(meta[net_key], minimum=-10000, maximum=100000) if net_key in meta else None
    if gross is not None and net is not None and not math.isclose(gross - cost, net, abs_tol=1e-8):
        raise ForecastError("gross_net_cost_inconsistent")
    if gross is None:
        gross = finite_number(net + cost, minimum=-10000, maximum=100000)
    return ReturnForecast(gross, cost, horizon, str(meta["cost_basis"]), version)
