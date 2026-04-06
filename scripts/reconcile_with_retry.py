#!/usr/bin/env python3
"""
Reconcile with retry and auto-kill-switch-reset.
If reconcile fails, wait and retry up to 3 times.
If succeeds after kill_switch was enabled, auto-disable it.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

from configs.loader import load_config
from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path, resolve_runtime_path
from src.execution.account_store import AccountStore
from src.execution.okx_private_client import OKXPrivateClient
from src.execution.position_store import PositionStore
from src.execution.reconcile_engine import ReconcileEngine, ReconcileThresholds


def _coalesce(value, default):
    return default if value is None else value


def load_kill_switch(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {"enabled": False}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"enabled": False}


def disable_kill_switch(path: str) -> None:
    ks = load_kill_switch(path)
    ks["enabled"] = False
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(ks, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    logging.info(f"Kill switch disabled at {path}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--env", default=".env")
    ap.add_argument("--out", default=None)
    ap.add_argument("--positions-db", default="reports/positions.sqlite")
    ap.add_argument("--abs-usdt-tol", type=float, default=None)
    ap.add_argument("--abs-base-tol", type=float, default=1e-5)
    ap.add_argument("--dust-usdt-ignore", type=float, default=None)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--retry-delay", type=float, default=2.0)
    ap.add_argument("--kill-switch-path", default=None)
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
    kill_switch_path = resolve_runtime_path(
        args.kill_switch_path if args.kill_switch_path is not None else getattr(cfg.execution, "kill_switch_path", None),
        default="reports/kill_switch.json",
    )

    client = OKXPrivateClient(exchange=cfg.exchange)
    
    ks_before = load_kill_switch(kill_switch_path)
    was_enabled = ks_before.get("enabled", False)
    
    last_error = None
    for attempt in range(args.retries):
        if attempt > 0:
            logging.info(f"Retry {attempt}/{args.retries-1} after {args.retry_delay}s delay...")
            time.sleep(args.retry_delay)
        
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
            
            ok = obj.get("ok", False)
            reason = obj.get("reason")
            
            payload = {
                "event": "RECONCILE_WITH_RETRY",
                "attempt": attempt + 1,
                "ok": ok,
                "reason": reason,
                "max_abs_usdt_delta": (obj.get("stats", {}) or {}).get("max_abs_usdt_delta"),
                "was_kill_switch_enabled": was_enabled,
            }
            logging.info(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
            
            if ok:
                # If reconcile succeeded and kill switch was enabled, disable it
                if was_enabled:
                    disable_kill_switch(kill_switch_path)
                return
            else:
                last_error = reason
                
        except Exception as e:
            last_error = str(e)
            logging.error(f"Reconcile attempt {attempt+1} failed with exception: {e}")
    
    # All retries failed
    logging.error(f"All {args.retries} reconcile attempts failed. Last error: {last_error}")
    raise RuntimeError(f"Reconcile failed after {args.retries} attempts: {last_error}")


if __name__ == "__main__":
    main()
