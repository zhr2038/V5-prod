from __future__ import annotations

import argparse
import json

from src.execution.order_gc import gc_unknown_orders


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="reports/orders.sqlite")
    ap.add_argument("--ttl-sec", type=int, default=1800)
    ap.add_argument("--limit", type=int, default=500)
    args = ap.parse_args()

    out = gc_unknown_orders(db_path=args.db, ttl_sec=int(args.ttl_sec), limit=int(args.limit))
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
