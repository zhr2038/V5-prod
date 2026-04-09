from __future__ import print_function

import argparse
import json
import logging
import time

from configs.loader import load_config
from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path, resolve_runtime_path
from src.execution.account_store import AccountStore
from src.execution.fill_store import derive_position_store_path, derive_runtime_named_json_path
from src.execution.kill_switch_guard import GuardConfig, KillSwitchGuard
from src.execution.okx_private_client import OKXPrivateClient, OKXPrivateClientError, OKXRateLimitError
from src.execution.position_store import PositionStore
from src.execution.reconcile_engine import ReconcileEngine, ReconcileThresholds


log = logging.getLogger("reconcile_guard")


def _coalesce(value, default):
    return default if value is None else value


def _resolve_runtime_json_path(raw_path, *, order_store_path: str, base_name: str, legacy_default: str) -> str:
    if raw_path is None or str(raw_path).strip() == "" or str(raw_path).strip() == legacy_default:
        return str(derive_runtime_named_json_path(order_store_path, base_name))
    return resolve_runtime_path(raw_path, default=legacy_default)


def _write_status(path: str, obj: dict) -> None:
    from pathlib import Path

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--env", default=".env")
    ap.add_argument("--out", default=None)
    ap.add_argument("--positions-db", default=None)
    ap.add_argument("--abs-usdt-tol", type=float, default=None)
    ap.add_argument("--abs-base-tol", type=float, default=1e-8)
    ap.add_argument("--source", default="timer")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO)
    cfg = load_config(
        resolve_runtime_config_path(args.config),
        env_path=resolve_runtime_env_path(args.env),
    )
    order_store_path = resolve_runtime_path(
        getattr(cfg.execution, "order_store_path", None),
        default="reports/orders.sqlite",
    )
    if args.out is not None:
        out_path = resolve_runtime_path(args.out, default="reports/reconcile_status.json")
    else:
        out_path = _resolve_runtime_json_path(
            getattr(cfg.execution, "reconcile_status_path", None),
            order_store_path=order_store_path,
            base_name="reconcile_status",
            legacy_default="reports/reconcile_status.json",
        )
    if args.positions_db:
        positions_db_path = resolve_runtime_path(args.positions_db, default="reports/positions.sqlite")
    else:
        positions_db_path = derive_position_store_path(order_store_path)
    failure_state_path = _resolve_runtime_json_path(
        getattr(cfg.execution, "reconcile_failure_state_path", None),
        order_store_path=order_store_path,
        base_name="reconcile_failure_state",
        legacy_default="reports/reconcile_failure_state.json",
    )
    kill_switch_path = _resolve_runtime_json_path(
        getattr(cfg.execution, "kill_switch_path", None),
        order_store_path=order_store_path,
        base_name="kill_switch",
        legacy_default="reports/kill_switch.json",
    )

    now = int(time.time() * 1000)

    # Always attempt reconcile; on failure, write an explicit ok=false status to avoid stale ok=true.
    client = OKXPrivateClient(exchange=cfg.exchange)
    status = None
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
                    _coalesce(getattr(cfg.execution, "reconcile_dust_usdt_ignore", None), 1.0)
                ),
            ),
        )
        universe_bases = [str(s).split("/")[0] for s in (cfg.symbols or [])]
        status = eng.reconcile(
            out_path=out_path,
            universe_bases=universe_bases,
            ccy_mode=str(getattr(cfg.execution, "reconcile_ccy_mode", "universe_only")),
        )
        status["generated_ts_ms"] = int(status.get("ts_ms") or now)
        status["source"] = str(args.source)
        _write_status(out_path, status)

    except OKXRateLimitError as e:
        status = {
            "schema_version": 1,
            "generated_ts_ms": now,
            "ts_ms": now,
            "source": str(args.source),
            "ok": False,
            "reason": "rate_limited",
            "error": {"type": "rate_limit", "detail": str(e), "http_status": None, "okx_code": "50011", "okx_msg": str(e)},
        }
        _write_status(out_path, status)

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
        _write_status(out_path, status)

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
        _write_status(out_path, status)

    finally:
        client.close()

    # Apply guard with config
    ks_cfg = getattr(cfg.execution, 'kill_switch', {})
    gcfg = GuardConfig(
        reconcile_status_path=out_path,
        failure_state_path=failure_state_path,
        kill_switch_path=kill_switch_path,
        auto_clear_enabled=getattr(ks_cfg, 'auto_clear_enabled', True),
        auto_clear_after_ok_count=getattr(ks_cfg, 'auto_clear_after_ok_count', 1),
        hard_fail_threshold=getattr(ks_cfg, 'hard_fail_threshold', 5),
        stale_soft_threshold=getattr(ks_cfg, 'stale_soft_threshold', 3),
    )
    out = KillSwitchGuard(gcfg).apply()
    
    # Log auto-clear event
    if out.get('auto_cleared'):
        log.warning(f"Kill switch was AUTO-CLEARED after {out.get('failure_state', {}).get('consecutive_ok')} OK reconciles")

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
