from __future__ import annotations

import argparse
import json
import logging
import time

from configs.loader import load_config
from src.execution.account_store import AccountStore
from src.execution.kill_switch_guard import GuardConfig, KillSwitchGuard
from src.execution.okx_private_client import OKXPrivateClient, OKXPrivateClientError, OKXRateLimitError
from src.execution.position_store import PositionStore
from src.execution.reconcile_engine import ReconcileEngine, ReconcileThresholds


log = logging.getLogger("reconcile_guard")


def _write_status(path: str, obj: dict) -> None:
    from pathlib import Path

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/config.yaml")
    ap.add_argument("--env", default=".env")
    ap.add_argument("--out", default="reports/reconcile_status.json")
    ap.add_argument("--positions-db", default="reports/positions.sqlite")
    ap.add_argument("--abs-usdt-tol", type=float, default=1.0)
    ap.add_argument("--abs-base-tol", type=float, default=1e-8)
    ap.add_argument("--source", default="timer")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO)
    cfg = load_config(args.config, env_path=args.env)

    now = int(time.time() * 1000)

    # Always attempt reconcile; on failure, write an explicit ok=false status to avoid stale ok=true.
    client = OKXPrivateClient(exchange=cfg.exchange)
    status = None
    try:
        eng = ReconcileEngine(
            okx=client,
            position_store=PositionStore(path=args.positions_db),
            account_store=AccountStore(path=args.positions_db),
            thresholds=ReconcileThresholds(abs_usdt_tol=float(args.abs_usdt_tol), abs_base_tol=float(args.abs_base_tol)),
        )
        status = eng.reconcile(out_path=args.out)
        status["generated_ts_ms"] = int(status.get("ts_ms") or now)
        status["source"] = str(args.source)
        _write_status(args.out, status)

    except OKXRateLimitError as e:
        status = {
            "schema_version": 1,
            "generated_ts_ms": now,
            "ts_ms": now,
            "source": str(args.source),
            "ok": False,
            "reason": "rate_limited",
            "error": {"type": "rate_limit", "detail": str(e)},
        }
        _write_status(args.out, status)

    except OKXPrivateClientError as e:
        # best-effort parse for okx_code from message
        detail = str(e)
        status = {
            "schema_version": 1,
            "generated_ts_ms": now,
            "ts_ms": now,
            "source": str(args.source),
            "ok": False,
            "reason": "network_error",
            "error": {"type": "client", "detail": detail},
        }
        _write_status(args.out, status)

    except Exception as e:
        status = {
            "schema_version": 1,
            "generated_ts_ms": now,
            "ts_ms": now,
            "source": str(args.source),
            "ok": False,
            "reason": "parse_error",
            "error": {"type": "exception", "detail": str(e)},
        }
        _write_status(args.out, status)

    finally:
        client.close()

    # Apply guard
    gcfg = GuardConfig(reconcile_status_path=args.out)
    out = KillSwitchGuard(gcfg).apply()

    payload = {
        "event": "RECON_GUARD",
        "ok": out.get("ok"),
        "reason": out.get("reason"),
        "category": out.get("category"),
        "hard": (out.get("failure_state") or {}).get("consecutive_hard"),
        "soft": (out.get("failure_state") or {}).get("consecutive_soft"),
        "kill": ((out.get("kill_switch") or {}).get("enabled")),
    }
    log.info(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
