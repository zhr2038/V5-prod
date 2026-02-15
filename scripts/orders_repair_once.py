from __future__ import annotations

import argparse
import json

from src.execution.order_repair import repair_unknown_orders


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="reports/orders.sqlite")
    ap.add_argument("--limit", type=int, default=500)
    args = ap.parse_args()

    out = repair_unknown_orders(db_path=args.db, limit=int(args.limit))
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
