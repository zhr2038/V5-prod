from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config
from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path, resolve_runtime_path
from src.execution.fill_store import derive_runtime_named_artifact_path, derive_runtime_named_json_path
from src.execution.bills_store import BillsStore
from src.execution.ledger_engine import LedgerEngine, LedgerThresholds
from src.execution.okx_private_client import OKXPrivateClient


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--env", default=".env")
    ap.add_argument("--bills-db", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO)
    cfg = load_config(
        resolve_runtime_config_path(args.config),
        env_path=resolve_runtime_env_path(args.env),
    )
    order_store_path = resolve_runtime_path(
        getattr(getattr(cfg, "execution", None), "order_store_path", None),
        default="reports/orders.sqlite",
    )
    bills_db_path = (
        resolve_runtime_path(args.bills_db, default="reports/bills.sqlite")
        if args.bills_db
        else str(derive_runtime_named_artifact_path(order_store_path, "bills", ".sqlite"))
    )
    out_path = (
        resolve_runtime_path(args.out, default="reports/ledger_status.json")
        if args.out
        else str(derive_runtime_named_json_path(order_store_path, "ledger_status"))
    )
    state_path = str(derive_runtime_named_json_path(order_store_path, "ledger_state"))

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
