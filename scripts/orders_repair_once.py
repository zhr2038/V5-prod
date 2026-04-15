from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from configs.runtime_config import load_runtime_config, resolve_runtime_config_path, resolve_runtime_path
from src.execution.order_repair import repair_unknown_orders


def resolve_orders_db(raw_db: str | None = None, *, config_path: str | None = None) -> Path:
    if raw_db:
        return Path(resolve_runtime_path(raw_db, default="reports/orders.sqlite", project_root=PROJECT_ROOT)).resolve()

    cfg = load_runtime_config(config_path, project_root=PROJECT_ROOT)
    execution_cfg = cfg.get("execution", {}) if isinstance(cfg, dict) else {}
    return Path(
        resolve_runtime_path(
            execution_cfg.get("order_store_path") if isinstance(execution_cfg, dict) else None,
            default="reports/orders.sqlite",
            project_root=PROJECT_ROOT,
        )
    ).resolve()


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--db", default=None)
    ap.add_argument("--limit", type=int, default=500)
    args = ap.parse_args(argv)

    resolved_config_path = resolve_runtime_config_path(args.config, project_root=PROJECT_ROOT) if args.config else None
    out = repair_unknown_orders(
        db_path=str(resolve_orders_db(args.db, config_path=resolved_config_path)),
        limit=int(args.limit),
    )
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
