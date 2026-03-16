from __future__ import annotations

import argparse
import json
import logging

from configs.loader import load_config
from src.execution.account_store import AccountStore
from src.execution.okx_private_client import OKXPrivateClient
from src.execution.position_store import PositionStore
from src.execution.reconcile_engine import ReconcileEngine, ReconcileThresholds


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/config.yaml")
    ap.add_argument("--env", default=".env")
    ap.add_argument("--out", default="reports/reconcile_status.json")
    ap.add_argument("--positions-db", default="reports/positions.sqlite")
    ap.add_argument("--abs-usdt-tol", type=float, default=1.0)
    ap.add_argument("--abs-base-tol", type=float, default=1e-5)
    ap.add_argument("--dust-usdt-ignore", type=float, default=2.0, help="Ignore non-USDT diffs whose estimated USDT value is below this (0=strict)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO)
    cfg = load_config(args.config, env_path=args.env)

    client = OKXPrivateClient(exchange=cfg.exchange)
    try:
        eng = ReconcileEngine(
            okx=client,
            position_store=PositionStore(path=args.positions_db),
            account_store=AccountStore(path=args.positions_db),
            thresholds=ReconcileThresholds(
                abs_usdt_tol=float(args.abs_usdt_tol),
                abs_base_tol=float(args.abs_base_tol),
                dust_usdt_ignore=float(args.dust_usdt_ignore),
            ),
        )
        obj = eng.reconcile(out_path=args.out)
        # Single-line structured log for ops / grep (helps G1.2 consecutive-fail analysis)
        payload = {
            "event": "RECONCILE",
            "ok": obj.get("ok"),
            "reason": obj.get("reason"),
            "max_abs_usdt_delta": (obj.get("stats", {}) or {}).get("max_abs_usdt_delta"),
            "out": args.out,
        }
        logging.info(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    finally:
        client.close()


if __name__ == "__main__":
    main()
