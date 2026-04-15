from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config
from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path
from src.execution.fill_store import FillStore, derive_fill_store_path, parse_okx_fills
from src.execution.okx_private_client import OKXPrivateClient


log = logging.getLogger("fill_sync")


def _resolve_store_path(raw_path: str) -> str:
    path = Path(str(raw_path).strip())
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return str(path)


def sync_once(*, store: FillStore, client: OKXPrivateClient, limit: int = 100, max_pages: int = 20) -> int:
    """Sync latest fills (last 3 days window) into FillStore.

    Strategy: page backward starting from newest; stop when a page produces 0 new inserts.
    This makes the sync idempotent and cheap after warm-up.
    """

    after = None
    total_new = 0
    for _ in range(int(max_pages)):
        r = client.get_fills(after=after, limit=int(limit))
        rows = parse_okx_fills(r.data, source="fills")
        ins, _ = store.upsert_many(rows)
        total_new += int(ins)

        data = (r.data or {}).get("data") or []
        if not isinstance(data, list) or not data:
            break

        # OKX pagination: use billId as cursor if present; else fallback to last tradeId.
        last = data[-1] if isinstance(data[-1], dict) else {}
        after = last.get("billId") or last.get("tradeId")

        if ins == 0:
            break

        time.sleep(0.05)  # be gentle; rate-limit retry is in client anyway

    store.set_state("last_sync_ts_ms", str(int(time.time() * 1000)))
    if after is not None:
        store.set_state("last_after_cursor", str(after))
    return total_new


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--env", default=".env")
    ap.add_argument("--db", default=None)
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--max-pages", type=int, default=20)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO)
    cfg = load_config(
        resolve_runtime_config_path(args.config, project_root=PROJECT_ROOT),
        env_path=resolve_runtime_env_path(args.env, project_root=PROJECT_ROOT),
    )
    if not (cfg.exchange.api_key and cfg.exchange.api_secret and cfg.exchange.passphrase):
        raise RuntimeError("Missing OKX API credentials (exchange.api_key/api_secret/passphrase)")

    raw_db_path = args.db
    if raw_db_path is None:
        order_store_path = getattr(getattr(cfg, "execution", None), "order_store_path", "reports/orders.sqlite")
        raw_db_path = str(derive_fill_store_path(order_store_path))
    db_path = _resolve_store_path(raw_db_path)

    store = FillStore(path=db_path)
    client = OKXPrivateClient(exchange=cfg.exchange)
    try:
        n = sync_once(store=store, client=client, limit=args.limit, max_pages=args.max_pages)
        log.info(f"fill_sync: new_fills={n} total={store.count()} db={db_path}")
        log.info(f"cursor(last_after)={store.get_state('last_after_cursor')} last_sync_ts_ms={store.get_state('last_sync_ts_ms')}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
