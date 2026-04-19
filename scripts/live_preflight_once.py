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


def _resolve_active_config_path(raw_config_path: str | None = None) -> str:
    from configs.runtime_config import load_runtime_config, resolve_runtime_config_path

    resolved = Path(resolve_runtime_config_path(raw_config_path, project_root=PROJECT_ROOT)).resolve()
    cfg = load_runtime_config(raw_config_path, project_root=PROJECT_ROOT)
    if not isinstance(cfg, dict) or not cfg:
        raise ValueError(f"runtime config is empty or invalid: {resolved}")
    execution_cfg = cfg.get("execution")
    if not isinstance(execution_cfg, dict):
        raise ValueError(f"runtime config missing execution section: {resolved}")
    return str(resolved)


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
    from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path, resolve_runtime_path
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
        print(json.dumps(res.__dict__, ensure_ascii=False, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
