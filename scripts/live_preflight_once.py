from __future__ import annotations

import argparse
import json
import logging

from configs.loader import load_config
from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path, resolve_runtime_path
from src.execution.account_store import AccountStore
from src.execution.live_preflight import LivePreflight
from src.execution.okx_private_client import OKXPrivateClient
from src.execution.position_store import PositionStore


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--env", default=".env")
    ap.add_argument("--positions-db", default="reports/positions.sqlite")
    ap.add_argument("--bills-db", default="reports/bills.sqlite")
    ap.add_argument("--max-pages", type=int, default=5)
    ap.add_argument("--max-status-age-sec", type=int, default=180)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO)
    cfg = load_config(
        resolve_runtime_config_path(args.config),
        env_path=resolve_runtime_env_path(args.env),
    )
    positions_db_path = resolve_runtime_path(args.positions_db, default="reports/positions.sqlite")
    bills_db_path = resolve_runtime_path(args.bills_db, default="reports/bills.sqlite")
    reconcile_status_path = resolve_runtime_path(
        getattr(cfg.execution, "reconcile_status_path", None),
        default="reports/reconcile_status.json",
    )
    kill_switch_path = resolve_runtime_path(
        getattr(cfg.execution, "kill_switch_path", None),
        default="reports/kill_switch.json",
    )
    setattr(cfg.execution, "reconcile_status_path", reconcile_status_path)
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
            ledger_state_path=resolve_runtime_path(default="reports/ledger_state.json"),
            ledger_status_path=resolve_runtime_path(default="reports/ledger_status.json"),
            reconcile_status_path=reconcile_status_path,
        )
        res = pf.run(max_pages=args.max_pages, max_status_age_sec=args.max_status_age_sec)
        print(json.dumps(res.__dict__, ensure_ascii=False, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
