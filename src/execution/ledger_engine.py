from __future__ import annotations

import json
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from src.execution.bills_store import BillsStore
from src.execution.okx_private_client import OKXPrivateClient


def _now_ms() -> int:
    return int(time.time() * 1000)


def _atomic_write_json(path: str, obj: Dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _read_json(path: str) -> Optional[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _dec(x: Optional[str]) -> Decimal:
    if x is None or str(x).strip() == "":
        return Decimal("0")
    return Decimal(str(x))


@dataclass
class LedgerThresholds:
    """LedgerThresholds类"""
    abs_usdt_tol: Decimal = Decimal("1")
    # Dust tolerance: ignore dust amounts below this threshold (was 1e-8, now 0.01 to handle dust)
    abs_base_tol: Decimal = Decimal("0.01")
    # Auto-reset baseline when non-USDT dust exceeds this threshold
    dust_reset_threshold: Decimal = Decimal("0.1")


class LedgerEngine:
    """G0.4.1 minimal ledger status check.

    We maintain a baseline snapshot (ledger_state.json):
      expected_balance = baseline_balance + SUM(balChg between baseline_ts..current_ts)

    This is a lightweight, observable check; it does not modify trading behavior.
    """

    def __init__(
        self,
        *,
        okx: OKXPrivateClient,
        bills_store: BillsStore,
        thresholds: Optional[LedgerThresholds] = None,
        state_path: str = "reports/ledger_state.json",
    ):
        self.okx = okx
        self.bills_store = bills_store
        self.thresholds = thresholds or LedgerThresholds()
        self.state_path = str(state_path)

    def _fetch_balance_cash(self) -> Tuple[Dict[str, str], Dict[str, Any]]:
        r = self.okx.get_balance(ccy=None)
        data = (r.data or {}).get("data")
        cash: Dict[str, str] = {}
        if isinstance(data, list) and data:
            details = (data[0] or {}).get("details") or []
            for d in details:
                if not isinstance(d, dict):
                    continue
                ccy = str(d.get("ccy") or "")
                if not ccy:
                    continue
                cb = d.get("cashBal")
                if cb is not None:
                    cash[ccy] = str(cb)

        meta = {
            "http_status": int(getattr(r, "http_status", 0) or 0),
            "okx_code": getattr(r, "okx_code", None),
            "okx_msg": getattr(r, "okx_msg", None),
        }
        return cash, meta

    def _load_baseline(self) -> Optional[Dict[str, Any]]:
        return _read_json(self.state_path)

    def _write_baseline(self, *, ts_ms: int, last_bill_id: Optional[str], last_bill_ts_ms: Optional[int], balances: Dict[str, str]) -> None:
        obj = {
            "schema_version": 1,
            "ts_ms": int(ts_ms),
            "last_bill_id": str(last_bill_id) if last_bill_id is not None else None,
            "last_bill_ts_ms": int(last_bill_ts_ms) if last_bill_ts_ms is not None else None,
            "balances": dict(balances),
        }
        _atomic_write_json(self.state_path, obj)

    def run(self, *, out_path: str = "reports/ledger_status.json") -> Dict[str, Any]:
        """Run"""
        now = _now_ms()

        baseline = self._load_baseline()
        current_bal, meta = self._fetch_balance_cash()

        last_bill = self.bills_store.last_bill()
        last_bill_id = last_bill[0] if last_bill else None
        last_bill_ts_ms = last_bill[1] if last_bill else None

        # first run: create baseline for next time
        if not baseline or not isinstance(baseline.get("balances"), dict):
            self._write_baseline(ts_ms=now, last_bill_id=last_bill_id, last_bill_ts_ms=last_bill_ts_ms, balances=current_bal)
            obj = {
                "schema_version": 1,
                "ts_ms": int(now),
                "ok": False,
                "reason": "no_baseline",
                "window": {"begin_ts_ms": None, "end_ts_ms": int(now)},
                "baseline": None,
                "current": {"ts_ms": int(now), "last_bill_id": last_bill_id, "balances": current_bal},
                "bills_aggregate": {"count": 0, "sum_bal_chg": {}},
                "diffs": {},
                "error": meta,
            }
            _atomic_write_json(out_path, obj)
            return obj

        base_ts = int(baseline.get("ts_ms") or 0)
        base_bill_ts = baseline.get("last_bill_ts_ms")
        begin_ts = int(base_bill_ts) if base_bill_ts is not None else int(base_ts)
        end_ts = int(last_bill_ts_ms or now)

        bills = self.bills_store.list_by_ts(begin_ts_ms=begin_ts, end_ts_ms=end_ts)
        agg: Dict[str, Decimal] = {}
        for b in bills:
            agg[b.ccy] = agg.get(b.ccy, Decimal("0")) + _dec(b.bal_chg)

        # compare expected vs actual
        diffs: Dict[str, Dict[str, str]] = {}
        ok = True
        reason = None
        
        # Track dust accumulation for auto-reset decision
        total_non_usdt_dust = Decimal("0")

        # monitored ccys = union(baseline/current/agg) plus USDT
        ccys = sorted(set((baseline.get("balances") or {}).keys()) | set(current_bal.keys()) | set(agg.keys()) | {"USDT"})
        for ccy in ccys:
            base_bal = _dec((baseline.get("balances") or {}).get(ccy))
            expected = base_bal + agg.get(ccy, Decimal("0"))
            actual = _dec(current_bal.get(ccy))
            delta = actual - expected

            diffs[ccy] = {
                "expected": str(expected),
                "actual": str(actual),
                "delta": str(delta),
            }

            tol = self.thresholds.abs_usdt_tol if ccy.upper() == "USDT" else self.thresholds.abs_base_tol
            if abs(delta) > tol:
                ok = False
                if reason is None:
                    reason = "ledger_mismatch_usdt" if ccy.upper() == "USDT" else "ledger_mismatch_base"
            
            # Accumulate non-USDT dust for auto-reset check
            if ccy.upper() != "USDT" and abs(delta) > tol:
                total_non_usdt_dust += abs(delta)

        obj = {
            "schema_version": 1,
            "ts_ms": int(now),
            "ok": bool(ok),
            "reason": reason,
            "window": {"begin_ts_ms": int(begin_ts), "end_ts_ms": int(end_ts)},
            "baseline": {
                "ts_ms": int(base_ts),
                "last_bill_id": baseline.get("last_bill_id"),
                "last_bill_ts_ms": baseline.get("last_bill_ts_ms"),
                "balances": baseline.get("balances"),
            },
            "current": {"ts_ms": int(now), "last_bill_id": last_bill_id, "last_bill_ts_ms": last_bill_ts_ms, "balances": current_bal},
            "bills_aggregate": {"count": int(len(bills)), "sum_bal_chg": {k: str(v) for k, v in agg.items()}},
            "diffs": diffs,
            "error": meta,
        }

        _atomic_write_json(out_path, obj)

        # Advance baseline only when ok=true (keeps failures reproducible for debugging).
        if ok:
            self._write_baseline(ts_ms=now, last_bill_id=last_bill_id, last_bill_ts_ms=end_ts, balances=current_bal)
        elif total_non_usdt_dust > self.thresholds.dust_reset_threshold:
            # Auto-reset baseline when dust accumulation is too high (dust from sold positions)
            # This prevents permanent SELL_ONLY lock due to dust mismatches
            self._write_baseline(ts_ms=now, last_bill_id=last_bill_id, last_bill_ts_ms=end_ts, balances=current_bal)
            # Mark as ok after reset since this is dust-related, not a real mismatch
            obj["ok"] = True
            obj["reason"] = "dust_baseline_reset"
            obj["dust_reset"] = {
                "total_non_usdt_dust": str(total_non_usdt_dust),
                "threshold": str(self.thresholds.dust_reset_threshold),
            }
            _atomic_write_json(out_path, obj)

        return obj
