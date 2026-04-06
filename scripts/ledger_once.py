from __future__ import annotations

import argparse
import json
import logging

from configs.loader import load_config
from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path, resolve_runtime_path
from src.execution.bills_store import BillsStore
from src.execution.ledger_engine import LedgerEngine, LedgerThresholds
from src.execution.okx_private_client import OKXPrivateClient


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--env", default=".env")
    ap.add_argument("--bills-db", default="reports/bills.sqlite")
    ap.add_argument("--out", default="reports/ledger_status.json")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO)
    cfg = load_config(
        resolve_runtime_config_path(args.config),
        env_path=resolve_runtime_env_path(args.env),
    )
    bills_db_path = resolve_runtime_path(args.bills_db, default="reports/bills.sqlite")
    out_path = resolve_runtime_path(args.out, default="reports/ledger_status.json")
    state_path = resolve_runtime_path(default="reports/ledger_state.json")

    client = OKXPrivateClient(exchange=cfg.exchange)
    try:
        eng = LedgerEngine(
            okx=client,
            bills_store=BillsStore(path=bills_db_path),
            thresholds=LedgerThresholds(),
            state_path=state_path,
        )
        obj = eng.run(out_path=out_path)
        payload = {
            "event": "LEDGER",
            "ok": obj.get("ok"),
            "reason": obj.get("reason"),
            "bill_count": (obj.get("bills_aggregate") or {}).get("count"),
            "last_bill_id": (obj.get("current") or {}).get("last_bill_id"),
        }
        logging.info(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    finally:
        client.close()


if __name__ == "__main__":
    main()
