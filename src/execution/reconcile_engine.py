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

    def _fetch_exchange_cash(self) -> Tuple[Dict[str, str], Dict[str, str], int, Dict[str, Any]]:
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

        meta = {
            "http_status": int(getattr(r, "http_status", 0) or 0),
            "okx_code": getattr(r, "okx_code", None),
            "okx_msg": getattr(r, "okx_msg", None),
        }
        return cash, frozen, int(u_max), meta

    def _local_snapshot(self) -> Tuple[str, Dict[str, str]]:
        acc = self.account_store.get()
        cash_usdt = f"{float(acc.cash_usdt):.12g}"
        ccy_qty: Dict[str, str] = {"USDT": cash_usdt}
        for p in self.position_store.list():
            base = _base_ccy_from_symbol(p.symbol)
            ccy_qty[base] = f"{float(p.qty):.12g}"
        return cash_usdt, ccy_qty

    def reconcile(
        self,
        *,
        out_path: str = "reports/reconcile_status.json",
        universe_bases: Optional[List[str]] = None,
        ccy_mode: str = "universe_only",
    ) -> Dict[str, Any]:
        cash, ord_frozen, u_max, meta = self._fetch_exchange_cash()
        local_cash_usdt, local_ccy_qty = self._local_snapshot()

        ccys_all = sorted(set(cash.keys()) | set(local_ccy_qty.keys()) | {"USDT"})

        mode = str(ccy_mode or "universe_only").strip().lower()
        bases = set([b.upper() for b in (universe_bases or [])])
        if mode == "all" or not bases:
            ccys = ccys_all
        else:
            # only enforce mismatches for USDT + universe bases; still report other ccys in diffs.
            ccys = ccys_all

        diffs: List[Dict[str, Any]] = []
        ok = True
        reason = None
        max_abs_usdt = 0.0
        max_abs_base = 0.0

        # OKX top-level code/msg
        okx_code = meta.get("okx_code")
        okx_msg = meta.get("okx_msg")
        http_status = meta.get("http_status")
        if okx_code and str(okx_code) != "0":
            ok = False
            # classify common codes
            if str(okx_code).startswith("501") or str(okx_code) == "50041":
                reason = "auth_error"
            elif str(okx_code) == "50011":
                reason = "rate_limited"
            else:
                reason = "api_system_error"

        from src.reporting.spread_snapshot_store import SpreadSnapshotStore

        ss = SpreadSnapshotStore()

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
            
            # Special handling for MERL - was negative, now positive after repayment
            # Allow normal reconciliation for MERL now
            if ccy.upper() == "MERL":
                # Log the current MERL balance for debugging
                import logging
                log = logging.getLogger(__name__)
                if ex_f < -0.1:
                    log.warning(f"RECONCILE: MERL still negative: eq={ex_f}")
                elif ex_f > 0.1:
                    log.info(f"RECONCILE: MERL now positive: eq={ex_f}")
                # Allow normal delta calculation - no special treatment
            
            if ccy.upper() == "USDT":
                delta_usdt = float(delta)
                max_abs_usdt = max(max_abs_usdt, abs(float(delta_usdt)))
                if abs(float(delta_usdt)) > float(self.thresholds.abs_usdt_tol):
                    ok = False
                    reason = reason or "usdt_mismatch"
            else:
                max_abs_base = max(max_abs_base, abs(float(delta)))

                # dust ignore (best-effort using mid from spread snapshots)
                dust_ignore = float(getattr(self.thresholds, "dust_usdt_ignore", 0.0) or 0.0)
                est_usdt = None
                try:
                    snap = ss.get_latest_before(symbol=f"{ccy}/USDT", ts_ms=_now_ms())
                    if snap is not None and snap.mid and float(snap.mid) > 0:
                        est_usdt = abs(float(delta)) * float(snap.mid)
                except Exception:
                    est_usdt = None

                is_universe = (ccy.upper() in bases) if bases else True
                enforce = True if (mode == "all") else is_universe

                if enforce and abs(float(delta)) > float(self.thresholds.abs_base_tol):
                    if est_usdt is not None and est_usdt < dust_ignore:
                        # ignore dust
                        pass
                    else:
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
            "stats": {"max_abs_usdt_delta": float(max_abs_usdt), "max_abs_base_delta": float(max_abs_base)},
            "error": {"http_status": http_status, "okx_code": okx_code, "okx_msg": okx_msg} if okx_code or okx_msg or http_status else None,
            "open_orders": {
                "note": "using cashBal avoids false mismatch from ordFrozen",
            },
        }

        _atomic_write_json(out_path, obj)
        return obj
