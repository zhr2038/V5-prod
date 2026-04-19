from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.runtime_config import load_runtime_config, resolve_runtime_config_path, resolve_runtime_env_path


log = logging.getLogger("bills_sync")


def _resolve_active_config_path(raw_config_path: str | None = None) -> str:
    resolved = Path(resolve_runtime_config_path(raw_config_path, project_root=PROJECT_ROOT)).resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"runtime config not found: {resolved}")
    raw_cfg = load_runtime_config(raw_config_path, project_root=PROJECT_ROOT)
    if not isinstance(raw_cfg, dict) or not raw_cfg:
        raise ValueError(f"runtime config is empty or invalid: {resolved}")
    execution_cfg = raw_cfg.get("execution")
    if not isinstance(execution_cfg, dict):
        raise ValueError(f"runtime config missing execution section: {resolved}")
    return str(resolved)


def _resolve_bills_db_path(raw_db_path: str | None, cfg) -> str:
    from configs.runtime_config import resolve_runtime_path
    from src.execution.fill_store import derive_runtime_named_artifact_path

    if raw_db_path:
        return resolve_runtime_path(raw_db_path, default="reports/bills.sqlite")

    order_store_path = resolve_runtime_path(
        getattr(getattr(cfg, "execution", None), "order_store_path", None),
        default="reports/orders.sqlite",
    )
    return str(derive_runtime_named_artifact_path(order_store_path, "bills", ".sqlite"))


def sync_once(*, store: BillsStore, client: OKXPrivateClient, limit: int = 100, max_pages: int = 50) -> int:
    """Sync latest bills (last 7 days) into BillsStore.

    Strategy: page backward from newest; stop when a page produces 0 new inserts.
    This is idempotent and cheap after warm-up.

    NOTE: OKX bills are ordered newest-first.
    """

    after = None
    total_new = 0
    last_bill_id = None
    last_ts = None
    from src.execution.bills_store import parse_okx_bills

    for _ in range(int(max_pages)):
        r = client.get_bills(after=after, limit=int(limit))
        rows = parse_okx_bills(r.data, source="bills")
        ins, _ = store.upsert_many(rows)
        total_new += int(ins)

        data = (r.data or {}).get("data") or []
        if not isinstance(data, list) or not data:
            break

        # cursor: use the last billId in this page (older side)
        last = data[-1] if isinstance(data[-1], dict) else {}
        after = last.get("billId")

        # record newest item of first page for summary
        if last_bill_id is None:
            first = data[0] if isinstance(data[0], dict) else {}
            last_bill_id = first.get("billId")
            last_ts = first.get("ts")

        if ins == 0:
            break

        time.sleep(0.05)

    store.set_state("last_sync_ts_ms", str(int(time.time() * 1000)))
    if after is not None:
        store.set_state("last_after_cursor", str(after))

    log.info(
        f"BILLS_SYNC new={total_new} total={store.count()} last_bill_id={last_bill_id} last_ts_ms={last_ts} cursor(last_after)={store.get_state('last_after_cursor')}"
    )
    return total_new


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--env", default=".env")
    ap.add_argument("--db", default=None)
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--max-pages", type=int, default=50)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO)
    from configs.loader import load_config
    from src.execution.bills_store import BillsStore
    from src.execution.okx_private_client import OKXPrivateClient

    resolved_config_path = _resolve_active_config_path(args.config)
    cfg = load_config(
        resolved_config_path,
        env_path=resolve_runtime_env_path(args.env, project_root=PROJECT_ROOT),
    )

    store = BillsStore(path=_resolve_bills_db_path(args.db, cfg))
    client = OKXPrivateClient(exchange=cfg.exchange)
    try:
        sync_once(store=store, client=client, limit=args.limit, max_pages=args.max_pages)
    finally:
        client.close()


if __name__ == "__main__":
    main()
