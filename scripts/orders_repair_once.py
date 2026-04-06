from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.execution.order_repair import repair_unknown_orders


def default_orders_db() -> Path:
    return PROJECT_ROOT / "reports" / "orders.sqlite"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(default_orders_db()))
    ap.add_argument("--limit", type=int, default=500)
    args = ap.parse_args()

    out = repair_unknown_orders(db_path=args.db, limit=int(args.limit))
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
