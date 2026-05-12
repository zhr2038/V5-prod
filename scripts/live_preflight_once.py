from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

def _resolve_runtime_json_path(raw_path, *, order_store_path: str, base_name: str, legacy_default: str) -> str:
    from configs.runtime_config import resolve_runtime_path
    from src.execution.fill_store import derive_runtime_named_json_path

    if raw_path is None or str(raw_path).strip() == "" or str(raw_path).strip() == legacy_default:
        return str(derive_runtime_named_json_path(order_store_path, base_name))
    return resolve_runtime_path(raw_path, default=legacy_default)


def _resolve_runtime_jsonl_path(raw_path, *, order_store_path: str, base_name: str, legacy_default: str) -> str:
    from configs.runtime_config import resolve_runtime_path
    from src.execution.fill_store import derive_runtime_named_artifact_path

    if raw_path is None or str(raw_path).strip() == "" or str(raw_path).strip() == legacy_default:
        return str(derive_runtime_named_artifact_path(order_store_path, base_name, ".jsonl"))
    return resolve_runtime_path(raw_path, default=legacy_default)


def _resolve_active_config_path(raw_config_path: str | None = None) -> str:
    from configs.runtime_config import load_runtime_config, resolve_runtime_config_path

    resolved = Path(resolve_runtime_config_path(raw_config_path, project_root=PROJECT_ROOT)).resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"runtime config not found: {resolved}")
    cfg = load_runtime_config(raw_config_path, project_root=PROJECT_ROOT)
    if not isinstance(cfg, dict) or not cfg:
        raise ValueError(f"runtime config is empty or invalid: {resolved}")
    execution_cfg = cfg.get("execution")
    if not isinstance(execution_cfg, dict):
        raise ValueError(f"runtime config missing execution section: {resolved}")
    return str(resolved)


def _quant_lab_runtime_namespace(quant_lab, **overrides):
    from types import SimpleNamespace

    if hasattr(quant_lab, "model_dump"):
        data = quant_lab.model_dump()
    elif hasattr(quant_lab, "__dict__"):
        data = dict(vars(quant_lab))
    else:
        data = {}
    data.update(overrides)
    return SimpleNamespace(**data)


def _legacy_execution_quant_lab_active(execution) -> bool:
    return bool(getattr(execution, "quant_lab_enabled", False))


def _legacy_execution_quant_lab_present(execution) -> bool:
    legacy_keys = {
        "quant_lab_enabled",
        "quant_lab_base_url",
        "quant_lab_timeout_sec",
        "quant_lab_fail_policy",
        "quant_lab_token_env",
        "quant_lab_default_alpha_id",
        "quant_lab_strategy",
        "quant_lab_strategy_version",
        "quant_lab_cost_regime_default",
        "quant_lab_cost_quantile",
        "quant_lab_gate_check_enabled",
        "quant_lab_health_check_enabled",
        "quant_lab_usage_path",
        "quant_lab_requests_path",
    }
    if execution is None:
        return False
    if _legacy_execution_quant_lab_active(execution):
        return True
    return any(key in getattr(execution, "model_fields_set", set()) for key in legacy_keys)


def _get_quant_lab_runtime_cfg(cfg):
    qcfg = getattr(cfg, "quant_lab", None)
    execution = getattr(cfg, "execution", None)
    if qcfg is not None and bool(getattr(qcfg, "enabled", False)):
        return _quant_lab_runtime_namespace(
            qcfg,
            quant_lab_config_source="top_level",
            legacy_execution_quant_lab_ignored=_legacy_execution_quant_lab_present(execution),
        )
    if execution is None or not _legacy_execution_quant_lab_active(execution):
        return qcfg
    from types import SimpleNamespace

    return SimpleNamespace(
        enabled=True,
        mode=str(getattr(execution, "quant_lab_mode", "shadow") or "shadow"),
        base_url=str(getattr(execution, "quant_lab_base_url", "http://qyun2.hrhome.top:8027") or "http://qyun2.hrhome.top:8027"),
        api_token_env=str(getattr(execution, "quant_lab_token_env", "QUANT_LAB_API_TOKEN") or "QUANT_LAB_API_TOKEN"),
        timeout_seconds=float(getattr(execution, "quant_lab_timeout_sec", 2.0) or 2.0),
        max_retries=1,
        cache_ttl_seconds=60,
        fail_policy=str(getattr(execution, "quant_lab_fail_policy", "sell_only") or "sell_only"),
        risk_permission_enabled=True,
        cost_enabled=True,
        gate_enabled=bool(getattr(execution, "quant_lab_gate_check_enabled", False)),
        cost_quantile=str(getattr(execution, "quant_lab_cost_quantile", "p75") or "p75"),
        cost_min_edge_multiplier=1.5,
        cost_fallback_to_local=True,
        min_cost_bps_floor=5.0,
        strategy_name=str(getattr(execution, "quant_lab_strategy", "v5") or "v5"),
        strategy_version=str(getattr(execution, "quant_lab_strategy_version", "5.0.0") or "5.0.0"),
        audit_enabled=True,
        audit_path="reports/quant_lab_usage.jsonl",
        request_log_path="reports/quant_lab_requests.jsonl",
        runtime_override_path="state/quant_lab_mode.json",
        allow_runtime_override=True,
        write_mode_audit=True,
        quant_lab_config_source="execution_legacy",
        legacy_execution_quant_lab_ignored=False,
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--env", default=".env")
    ap.add_argument("--positions-db", default=None)
    ap.add_argument("--bills-db", default=None)
    ap.add_argument("--max-pages", type=int, default=5)
    ap.add_argument("--max-status-age-sec", type=int, default=180)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO)
    from configs.loader import load_config
    from configs.runtime_config import resolve_runtime_env_path, resolve_runtime_path
    from src.execution.account_store import AccountStore
    from src.execution.fill_store import derive_position_store_path, derive_runtime_named_artifact_path, derive_runtime_named_json_path
    from src.execution.live_preflight import LivePreflight
    from src.execution.okx_private_client import OKXPrivateClient
    from src.execution.position_store import PositionStore

    cfg = load_config(
        _resolve_active_config_path(args.config),
        env_path=resolve_runtime_env_path(args.env),
    )
    order_store_path = resolve_runtime_path(
        getattr(cfg.execution, "order_store_path", None),
        default="reports/orders.sqlite",
    )
    if args.positions_db:
        positions_db_path = resolve_runtime_path(args.positions_db, default="reports/positions.sqlite")
    else:
        positions_db_path = derive_position_store_path(order_store_path)
    if args.bills_db:
        bills_db_path = resolve_runtime_path(args.bills_db, default="reports/bills.sqlite")
    else:
        bills_db_path = str(derive_runtime_named_artifact_path(order_store_path, "bills", ".sqlite"))
    reconcile_status_path = _resolve_runtime_json_path(
        getattr(cfg.execution, "reconcile_status_path", None),
        order_store_path=order_store_path,
        base_name="reconcile_status",
        legacy_default="reports/reconcile_status.json",
    )
    reconcile_failure_state_path = _resolve_runtime_json_path(
        getattr(cfg.execution, "reconcile_failure_state_path", None),
        order_store_path=order_store_path,
        base_name="reconcile_failure_state",
        legacy_default="reports/reconcile_failure_state.json",
    )
    kill_switch_path = _resolve_runtime_json_path(
        getattr(cfg.execution, "kill_switch_path", None),
        order_store_path=order_store_path,
        base_name="kill_switch",
        legacy_default="reports/kill_switch.json",
    )
    setattr(cfg.execution, "reconcile_status_path", reconcile_status_path)
    setattr(cfg.execution, "reconcile_failure_state_path", reconcile_failure_state_path)
    setattr(cfg.execution, "kill_switch_path", kill_switch_path)

    ps = PositionStore(path=positions_db_path)
    ac = AccountStore(path=positions_db_path)

    client = OKXPrivateClient(exchange=cfg.exchange)
    try:
        pf = LivePreflight(
            cfg.execution,
            okx=client,
            position_store=ps,
            account_store=ac,
            bills_db_path=bills_db_path,
            ledger_state_path=str(derive_runtime_named_json_path(order_store_path, "ledger_state")),
            ledger_status_path=str(derive_runtime_named_json_path(order_store_path, "ledger_status")),
            reconcile_status_path=reconcile_status_path,
        )
        res = pf.run(max_pages=args.max_pages, max_status_age_sec=args.max_status_age_sec)
        payload = dict(res.__dict__)
        qcfg_runtime = _get_quant_lab_runtime_cfg(cfg)
        if bool(getattr(qcfg_runtime, "enabled", False)):
            from src.quant_lab_client.guard import QuantLabGuard

            qcfg_runtime.request_log_path = _resolve_runtime_jsonl_path(
                getattr(qcfg_runtime, "request_log_path", None),
                order_store_path=order_store_path,
                base_name="quant_lab_requests",
                legacy_default="reports/quant_lab_requests.jsonl",
            )
            qcfg_runtime.audit_path = _resolve_runtime_jsonl_path(
                getattr(qcfg_runtime, "audit_path", None),
                order_store_path=order_store_path,
                base_name="quant_lab_usage",
                legacy_default="reports/quant_lab_usage.jsonl",
            )
            qguard = QuantLabGuard.from_config(qcfg_runtime, phase="live_preflight_once")
            decision = qguard.refresh_permission(include_health=True)
            payload["quant_lab"] = qguard.audit_payload()
            if bool(qguard.permission_result.fallback_used) and decision == "ABORT":
                raise RuntimeError("quant-lab unavailable and quant_lab.fail_policy=abort")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
