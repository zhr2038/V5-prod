"""Refresh a read-only cost-evidence report; never import an exchange executor."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from configs.runtime_config import load_runtime_config, resolve_runtime_path
from src.execution.fill_store import derive_fill_store_path, derive_runtime_named_json_path
from src.reporting.live_cost_evidence import SCHEMA, build_live_cost_evidence


def write_atomic(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orders-db")
    parser.add_argument("--fills-db")
    parser.add_argument("--out")
    parser.add_argument("--window-days", type=int, default=30)
    args = parser.parse_args()
    cfg = load_runtime_config(project_root=ROOT)
    orders = Path(resolve_runtime_path(args.orders_db or cfg["execution"]["order_store_path"], default="reports/orders.sqlite", project_root=ROOT))
    fills = Path(args.fills_db).resolve() if args.fills_db else derive_fill_store_path(orders)
    output = Path(args.out).resolve() if args.out else derive_runtime_named_json_path(orders, "live_cost_evidence")
    now = datetime.now(timezone.utc)
    try:
        report = build_live_cost_evidence(fills, orders, now=now, window_days=args.window_days)
    except Exception as exc:
        # Publish explicit failure instead of leaving an apparently current success.
        report = {"schema_version": SCHEMA, "status": "UNAVAILABLE", "generated_at_utc": now.isoformat(),
                  "reason": f"{type(exc).__name__}: {exc}", "live_order_effect": "none", "updates_live_cost_model": False}
        write_atomic(output, report)
        print(json.dumps(report, ensure_ascii=False))
        return 1
    write_atomic(output, report)
    print(json.dumps({k: report[k] for k in ("status", "fill_count", "qualified_fill_count", "qualified_order_count")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
