from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config  # noqa: E402
from src.core.models import Order  # noqa: E402
from src.quant_lab_client.client import QuantLabClient  # noqa: E402
from src.quant_lab_client.cost_gate import apply_quant_lab_cost_gate  # noqa: E402
from src.quant_lab_client.exceptions import QuantLabValidationError  # noqa: E402
from src.quant_lab_client.mode import QuantLabMode, resolve_quant_lab_mode  # noqa: E402
from src.quant_lab_client.models import QuantLabHealth, RiskPermission  # noqa: E402


def _endpoint_check(response: Any, *, extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
    payload = {
        "ok": bool(getattr(response, "ok", False)),
        "status_code": getattr(response, "status_code", None),
        "cached": bool(getattr(response, "cached", False)),
        "error": getattr(response, "error", None),
    }
    if extra:
        payload.update(dict(extra))
    return payload


def _advisory_item_count(payload: Any) -> int:
    if isinstance(payload, list):
        return len(payload)
    if not isinstance(payload, dict):
        return 0
    for key in ("items", "rows", "data", "advisory", "strategies"):
        value = payload.get(key)
        if isinstance(value, list):
            return len(value)
    return 1 if payload else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only quant-lab API selfcheck for V5")
    parser.add_argument("--config", default="configs/config.yaml")
    parser.add_argument("--symbol", default="BTC/USDT")
    parser.add_argument("--regime", default="normal")
    parser.add_argument("--notional-usdt", type=float, default=200.0)
    parser.add_argument("--out", default="reports/quant_lab_selfcheck.json")
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    qcfg = cfg.quant_lab
    mode_resolution = resolve_quant_lab_mode(cfg)
    if mode_resolution.mode == QuantLabMode.LOCAL_ONLY:
        payload = {
            "mode": mode_resolution.mode.value,
            "mode_source": mode_resolution.mode_source,
            "api_token_loaded": False,
            "api_env_path_present": False,
            "api_env_secure_permissions": None,
            "api_env_token_loaded": False,
            "api_env_warning": None,
            "endpoint_checks": {},
            "health": "skipped",
            "permission": "ALLOW_LOCAL",
            "permission_reasons": ["quant_lab_local_only"],
            "cost": {"source": "local_only"},
            "safe_for_new_risk": True,
            "called_api": False,
        }
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    client = QuantLabClient.from_config(qcfg, run_id="selfcheck", phase="selfcheck", mode=mode_resolution.mode.value)
    if not bool(getattr(client, "api_token", None)):
        raise QuantLabValidationError("quant-lab selfcheck requires QUANT_LAB_API_TOKEN to be loaded")

    health_response = client.get_json("/v1/health")
    health = QuantLabHealth.from_payload(health_response.data)
    client._validate_health(health, endpoint="/v1/health", allow_warning=False)

    deep_health_response = client.get_json("/v1/health/deep")
    deep_health = QuantLabHealth.from_payload(deep_health_response.data)
    client._validate_health(deep_health, endpoint="/v1/health/deep", allow_warning=True)

    permission_response = client.get_json(
        "/v1/risk/live-permission",
        params={"strategy": qcfg.strategy_name, "version": qcfg.strategy_version},
    )
    permission = RiskPermission.from_payload(permission_response.data)

    advisory_response = client.get_json(
        "/v1/strategy-opportunity-advisory/v5-compact",
        params={
            "format": "json",
            "fields": "minimal",
            "latest_only": "true",
            "fresh_only": "true",
        },
    )
    cost = client.estimate_cost(
        symbol=args.symbol,
        regime=args.regime,
        notional_usdt=args.notional_usdt,
        quantile=qcfg.cost_quantile,
    )
    order = Order(args.symbol, "buy", "OPEN_LONG", args.notional_usdt, 1.0, {})
    gate = apply_quant_lab_cost_gate(order, cost, cfg)
    payload = {
        "health": health.status,
        "deep_health": {
            "status": deep_health.status,
            "warnings": deep_health.warnings,
            "data_health": deep_health.data_health,
            "cost_health": deep_health.cost_health,
            "risk_permission_dependency_meta": deep_health.risk_permission_dependency_meta,
        },
        "mode": mode_resolution.mode.value,
        "mode_source": mode_resolution.mode_source,
        "api_token_loaded": True,
        "api_env_path_present": bool(getattr(client, "api_env_path_present", False)),
        "api_env_secure_permissions": getattr(client, "api_env_secure_permissions", None),
        "api_env_token_loaded": bool(getattr(client, "api_env_token_loaded", False)),
        "api_env_warning": getattr(client, "api_env_warning", None),
        "token_auth_disabled_reason": getattr(client, "token_auth_disabled_reason", None),
        "endpoint_checks": {
            "/v1/health": _endpoint_check(health_response, extra={"status": health.status}),
            "/v1/health/deep": _endpoint_check(deep_health_response, extra={"status": deep_health.status}),
            "/v1/risk/live-permission": _endpoint_check(
                permission_response,
                extra={"permission": permission.permission},
            ),
            "/v1/strategy-opportunity-advisory/v5-compact": _endpoint_check(
                advisory_response,
                extra={"item_count": _advisory_item_count(advisory_response.data)},
            ),
        },
        "service": health.service,
        "service_mode": health.mode,
        "permission": permission.permission,
        "permission_reasons": permission.reasons,
        "cost": {
            "symbol": cost.symbol,
            "total_cost_bps": cost.total_cost_bps,
            "effective_total_cost_bps": gate.effective_total_cost_bps,
            "source": cost.source,
            "fallback_level": cost.fallback_level,
            "cost_model_version": cost.cost_model_version,
        },
        "safe_for_new_risk": permission.permission == "ALLOW",
        "called_api": True,
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
