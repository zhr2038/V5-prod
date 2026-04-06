from __future__ import annotations

import argparse
import json
import logging

from configs.loader import load_config
from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path, resolve_runtime_path
from src.execution.account_store import AccountStore
from src.execution.okx_private_client import OKXPrivateClient
from src.execution.position_store import PositionStore
from src.execution.reconcile_engine import ReconcileEngine, ReconcileThresholds


def _coalesce(value, default):
    return default if value is None else value


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--env", default=".env")
    ap.add_argument("--out", default=None)
    ap.add_argument("--positions-db", default="reports/positions.sqlite")
    ap.add_argument("--abs-usdt-tol", type=float, default=None)
    ap.add_argument("--abs-base-tol", type=float, default=1e-5)
    ap.add_argument("--dust-usdt-ignore", type=float, default=None, help="Ignore non-USDT diffs whose estimated USDT value is below this (0=strict)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO)
    cfg = load_config(
        resolve_runtime_config_path(args.config),
        env_path=resolve_runtime_env_path(args.env),
    )
    out_path = resolve_runtime_path(
        args.out if args.out is not None else getattr(cfg.execution, "reconcile_status_path", None),
        default="reports/reconcile_status.json",
    )
    positions_db_path = resolve_runtime_path(args.positions_db, default="reports/positions.sqlite")

    client = OKXPrivateClient(exchange=cfg.exchange)
    try:
        eng = ReconcileEngine(
            okx=client,
            position_store=PositionStore(path=positions_db_path),
            account_store=AccountStore(path=positions_db_path),
            thresholds=ReconcileThresholds(
                abs_usdt_tol=float(
                    _coalesce(args.abs_usdt_tol, _coalesce(getattr(cfg.execution, "reconcile_abs_usdt_tol", None), 50.0))
                ),
                abs_base_tol=float(args.abs_base_tol),
                dust_usdt_ignore=float(
                    _coalesce(args.dust_usdt_ignore, _coalesce(getattr(cfg.execution, "reconcile_dust_usdt_ignore", None), 1.0))
                ),
            ),
        )
        obj = eng.reconcile(out_path=out_path)
        # Single-line structured log for ops / grep (helps G1.2 consecutive-fail analysis)
        payload = {
            "event": "RECONCILE",
            "ok": obj.get("ok"),
            "reason": obj.get("reason"),
            "max_abs_usdt_delta": (obj.get("stats", {}) or {}).get("max_abs_usdt_delta"),
            "out": out_path,
        }
        logging.info(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    finally:
        client.close()


if __name__ == "__main__":
    main()
