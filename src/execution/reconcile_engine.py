from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.execution.account_store import AccountStore
from src.execution.okx_private_client import OKXPrivateClient
from src.execution.position_store import PositionStore


def _now_ms() -> int:
    return int(time.time() * 1000)


def _atomic_write_json(path: str, obj: Dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _base_ccy_from_symbol(symbol: str) -> str:
    # internal symbols look like BTC/USDT
    return str(symbol).split("/")[0]


@dataclass
class ReconcileThresholds:
    abs_usdt_tol: float = 1.0
    abs_base_tol: float = 1e-8
    dust_usdt_ignore: float = 0.0  # 0 => strict


class ReconcileEngine:
    """Account/position reconcile (G1.0).

    Exchange source: OKX /api/v5/account/balance (cashBal as authority).
    Local source: PositionStore (base qty) + AccountStore (cash_usdt).

    Output: reports/reconcile_status.json (atomic write).
    """

    def __init__(
        self,
        *,
        okx: OKXPrivateClient,
        position_store: PositionStore,
        account_store: AccountStore,
        thresholds: Optional[ReconcileThresholds] = None,
    ):
        self.okx = okx
        self.position_store = position_store
        self.account_store = account_store
        self.thresholds = thresholds or ReconcileThresholds()

    def _fetch_exchange_cash(self) -> Tuple[Dict[str, str], Dict[str, str], int]:
        r = self.okx.get_balance(ccy=None)
        data = (r.data or {}).get("data")
        cash: Dict[str, str] = {}
        frozen: Dict[str, str] = {}
        u_max = 0
        if isinstance(data, list) and data:
            details = (data[0] or {}).get("details") or []
            for d in details:
                if not isinstance(d, dict):
                    continue
                ccy = str(d.get("ccy") or "")
                if not ccy:
                    continue
                cb = d.get("cashBal")
                of = d.get("ordFrozen")
                ut = d.get("uTime")
                if cb is not None:
                    cash[ccy] = str(cb)
                if of is not None:
                    frozen[ccy] = str(of)
                try:
                    u_max = max(u_max, int(ut or 0))
                except Exception:
                    pass
        return cash, frozen, int(u_max)

    def _local_snapshot(self) -> Tuple[str, Dict[str, str]]:
        acc = self.account_store.get()
        cash_usdt = f"{float(acc.cash_usdt):.12g}"
        ccy_qty: Dict[str, str] = {"USDT": cash_usdt}
        for p in self.position_store.list():
            base = _base_ccy_from_symbol(p.symbol)
            ccy_qty[base] = f"{float(p.qty):.12g}"
        return cash_usdt, ccy_qty

    def reconcile(self, *, out_path: str = "reports/reconcile_status.json") -> Dict[str, Any]:
        cash, ord_frozen, u_max = self._fetch_exchange_cash()
        local_cash_usdt, local_ccy_qty = self._local_snapshot()

        ccys = sorted(set(cash.keys()) | set(local_ccy_qty.keys()) | {"USDT"})

        diffs: List[Dict[str, Any]] = []
        ok = True
        reason = None
        max_abs_usdt = 0.0

        for ccy in ccys:
            ex = cash.get(ccy) or "0"
            lo = local_ccy_qty.get(ccy) or "0"
            try:
                ex_f = float(ex)
            except Exception:
                ex_f = 0.0
            try:
                lo_f = float(lo)
            except Exception:
                lo_f = 0.0

            delta = ex_f - lo_f
            delta_usdt = None
            if ccy.upper() == "USDT":
                delta_usdt = float(delta)
                max_abs_usdt = max(max_abs_usdt, abs(float(delta_usdt)))
                if abs(float(delta_usdt)) > float(self.thresholds.abs_usdt_tol):
                    ok = False
                    reason = reason or "usdt_mismatch"
            else:
                if abs(float(delta)) > float(self.thresholds.abs_base_tol):
                    ok = False
                    reason = reason or "base_mismatch"

            diffs.append(
                {
                    "ccy": ccy,
                    "exchange": str(ex),
                    "local": str(lo),
                    "delta": f"{float(delta):.12g}",
                    "delta_usdt": None if delta_usdt is None else f"{float(delta_usdt):.12g}",
                }
            )

        obj: Dict[str, Any] = {
            "schema_version": 1,
            "ts_ms": _now_ms(),
            "ok": bool(ok),
            "reason": reason,
            "exchange_snapshot": {
                "source": "okx_account_balance",
                "uTime_max_ms": int(u_max),
                "ccy_cashBal": dict(cash),
                "ccy_ordFrozen": dict(ord_frozen),
            },
            "local_snapshot": {
                "cash_usdt": str(local_cash_usdt),
                "ccy_qty": dict(local_ccy_qty),
            },
            "diffs": diffs,
            "thresholds": {
                "abs_usdt_tol": float(self.thresholds.abs_usdt_tol),
                "abs_base_tol": float(self.thresholds.abs_base_tol),
                "dust_usdt_ignore": float(self.thresholds.dust_usdt_ignore),
            },
            "stats": {"max_abs_usdt_delta": float(max_abs_usdt)},
            "open_orders": {
                "note": "using cashBal avoids false mismatch from ordFrozen",
            },
        }

        _atomic_write_json(out_path, obj)
        return obj
