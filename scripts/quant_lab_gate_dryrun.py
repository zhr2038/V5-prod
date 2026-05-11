from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config  # noqa: E402
from src.core.models import Order  # noqa: E402
from src.quant_lab_client.guard import QuantLabGuard, QuantLabGuardResult  # noqa: E402


def _latest_decision_audit(reports_dir: Path) -> Path | None:
    candidates = sorted(reports_dir.glob("runs/**/decision_audit.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def _orders_from_audit(path: Path | None) -> list[Order]:
    if path is None or not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    orders: list[Order] = []
    for row in data.get("router_decisions", []) or []:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "")
        side = str(row.get("side") or row.get("action") or "").lower()
        if side not in {"buy", "sell"}:
            continue
        orders.append(
            Order(
                symbol=symbol,
                side=side,
                intent=str(row.get("intent") or ("OPEN_LONG" if side == "buy" else "CLOSE_LONG")),
                notional_usdt=float(row.get("notional_usdt") or 0.0),
                signal_price=float(row.get("signal_price") or 0.0),
                meta=dict(row.get("meta") or {}),
            )
        )
    return orders


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Dry-run quant-lab gate against recent V5 audit orders")
    parser.add_argument("--config", default="configs/config.yaml")
    parser.add_argument("--reports-dir", default="reports")
    args = parser.parse_args(argv)
    cfg = load_config(args.config)
    latest = _latest_decision_audit(Path(args.reports_dir))
    orders = _orders_from_audit(latest)
    guard = QuantLabGuard.disabled(cfg.quant_lab, run_id="gate-dryrun")
    guard.permission_result = QuantLabGuardResult(
        enabled=bool(cfg.quant_lab.enabled),
        permission="SELL_ONLY",
        reasons=["dryrun_default_sell_only"],
    )
    kept = guard.filter_orders_by_permission(orders, guard.permission_result)
    payload = {
        "decision_audit": str(latest) if latest else None,
        "orders_seen": len(orders),
        "orders_kept": len(kept),
        "orders_filtered": len(orders) - len(kept),
        "permission": guard.permission_result.permission,
        "executed": False,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
