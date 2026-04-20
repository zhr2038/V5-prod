from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

def _coalesce(value, default):
    return default if value is None else value


def _resolve_runtime_json_path(raw_path, *, order_store_path: str, base_name: str, legacy_default: str) -> str:
    from configs.runtime_config import resolve_runtime_path
    from src.execution.fill_store import derive_runtime_named_json_path

    if raw_path is None or str(raw_path).strip() == "" or str(raw_path).strip() == legacy_default:
        return str(derive_runtime_named_json_path(order_store_path, base_name))
    return resolve_runtime_path(raw_path, default=legacy_default)


def _resolve_active_config_path(config_path: str | None = None) -> Path:
    from configs.runtime_config import resolve_runtime_config_path

    resolved = Path(resolve_runtime_config_path(config_path, project_root=PROJECT_ROOT)).resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"runtime config not found: {resolved}")
    try:
        payload = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        raise ValueError(f"runtime config is invalid: {resolved}: {exc}") from exc
    if not isinstance(payload, dict) or not payload:
        raise ValueError(f"runtime config is empty or invalid: {resolved}")
    execution = payload.get("execution")
    if not isinstance(execution, dict):
        raise ValueError(f"runtime config missing execution section: {resolved}")
    return resolved


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--env", default=".env")
    ap.add_argument("--out", default=None)
    ap.add_argument("--positions-db", default=None)
    ap.add_argument("--abs-usdt-tol", type=float, default=None)
    ap.add_argument("--abs-base-tol", type=float, default=1e-5)
    ap.add_argument("--dust-usdt-ignore", type=float, default=None, help="Ignore non-USDT diffs whose estimated USDT value is below this (0=strict)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO)
    from configs.loader import load_config
    from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path, resolve_runtime_path
    from src.execution.account_store import AccountStore
    from src.execution.fill_store import derive_position_store_path
    from src.execution.okx_private_client import OKXPrivateClient
    from src.execution.position_store import PositionStore
    from src.execution.reconcile_engine import ReconcileEngine, ReconcileThresholds

    resolved_config_path = _resolve_active_config_path(args.config)
    cfg = load_config(
        str(resolved_config_path),
        env_path=resolve_runtime_env_path(args.env, project_root=PROJECT_ROOT),
    )
    order_store_path = resolve_runtime_path(
        getattr(cfg.execution, "order_store_path", None),
        default="reports/orders.sqlite",
        project_root=PROJECT_ROOT,
    )
    if args.out is not None:
        out_path = resolve_runtime_path(
            args.out,
            default="reports/reconcile_status.json",
            project_root=PROJECT_ROOT,
        )
    else:
        out_path = _resolve_runtime_json_path(
            getattr(cfg.execution, "reconcile_status_path", None),
            order_store_path=order_store_path,
            base_name="reconcile_status",
            legacy_default="reports/reconcile_status.json",
        )
    if args.positions_db:
        positions_db_path = resolve_runtime_path(
            args.positions_db,
            default="reports/positions.sqlite",
            project_root=PROJECT_ROOT,
        )
    else:
        positions_db_path = derive_position_store_path(order_store_path)

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
