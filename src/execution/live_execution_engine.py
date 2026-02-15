from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from configs.schema import ExecutionConfig
from src.core.models import ExecutionReport, Order
from src.execution.clordid import make_cl_ord_id, make_decision_hash
from src.execution.okx_private_client import OKXPrivateClient, OKXPrivateClientError, OKXResponse
from src.execution.order_store import OrderStore
from src.execution.position_store import PositionStore


log = logging.getLogger(__name__)


def symbol_to_inst_id(symbol: str) -> str:
    # V5 internal symbols are like "BTC/USDT"; OKX instId is "BTC-USDT".
    return str(symbol).replace("/", "-")


def _load_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        p = Path(path)
        if not p.exists():
            return None
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_kill_switch_enabled(path: str) -> bool:
    d = _load_json(path) or {}
    return bool(d.get("enabled") or d.get("kill_switch") or d.get("active"))


def load_reconcile_ok(path: str) -> bool:
    # Placeholder before G1.0: assume OK unless file exists and says not ok.
    d = _load_json(path)
    if d is None:
        return True
    if "ok" in d:
        return bool(d.get("ok"))
    if "reconcile_ok" in d:
        return bool(d.get("reconcile_ok"))
    return True


def submit_gate_for_live(cfg: ExecutionConfig) -> Tuple[str, bool, bool]:
    ks = load_kill_switch_enabled(getattr(cfg, "kill_switch_path", "reports/kill_switch.json"))
    rc_ok = load_reconcile_ok(getattr(cfg, "reconcile_status_path", "reports/reconcile_status.json"))
    if ks or not rc_ok:
        return "SELL_ONLY", rc_ok, ks
    return "ALLOW", rc_ok, ks


def map_okx_state(okx_state: Optional[str]) -> str:
    s = str(okx_state or "").lower()
    if s in {"live", "new", "submitted"}:
        return "OPEN"
    if s in {"partially_filled", "partial-filled", "partial"}:
        return "PARTIAL"
    if s in {"filled"}:
        return "FILLED"
    if s in {"canceled", "cancelled", "mmp_canceled"}:
        return "CANCELED"
    if s in {"rejected", "failed"}:
        return "REJECTED"
    return "UNKNOWN"


@dataclass
class LiveExecutionResult:
    cl_ord_id: str
    state: str
    ord_id: Optional[str] = None


class LiveExecutionEngine:
    """Live execution adapter for OKX spot.

    G0.2 scope:
    - place / get / cancel via OKXPrivateClient
    - idempotency anchored on clOrdId + OrderStore
    - strict gate: if kill-switch or reconcile not ok => SELL_ONLY

    Note: This engine does not yet handle partial fills inventory accounting.
    It focuses on idempotent submission & observability.
    """

    def __init__(
        self,
        cfg: ExecutionConfig,
        *,
        okx: OKXPrivateClient,
        order_store: Optional[OrderStore] = None,
        position_store: Optional[PositionStore] = None,
        run_id: str = "",
        exp_time_ms: Optional[int] = None,
    ):
        self.cfg = cfg
        self.okx = okx
        self.order_store = order_store or OrderStore(path=str(getattr(cfg, "order_store_path", "reports/orders.sqlite")))
        self.position_store = position_store or PositionStore(path="reports/positions.sqlite")
        self.run_id = str(run_id or "")
        self.exp_time_ms = exp_time_ms

    def _decision_hash_for_order(self, o: Order) -> str:
        # Prefer upstream decision hash if present
        meta = o.meta or {}
        dh = meta.get("decision_hash") or meta.get("target_hash")
        if dh:
            return str(dh)

        # Fallback: stable hash from semantically relevant order fields.
        payload = {
            "symbol": o.symbol,
            "intent": o.intent,
            "side": o.side,
            "notional_usdt": round(float(o.notional_usdt), 8),
            "signal_price": round(float(o.signal_price or 0.0), 8),
            "target_w": meta.get("target_w"),
            "window_start_ts": meta.get("window_start_ts"),
            "window_end_ts": meta.get("window_end_ts"),
            "regime": meta.get("regime"),
            "deadband_pct": meta.get("deadband_pct"),
        }
        return make_decision_hash(payload)

    def _build_place_payload(self, o: Order, *, inst_id: str, cl_ord_id: str) -> Dict[str, Any]:
        # Minimal market order payload.
        side = str(o.side)
        ord_type = "market"
        td_mode = "cash"

        payload: Dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "ordType": ord_type,
            "clOrdId": cl_ord_id,
        }

        notional = float(o.notional_usdt)

        if side == "buy":
            # Spot market buy: submit quote notional in USDT
            payload["sz"] = str(notional)
            payload["tgtCcy"] = "quote_ccy"
        else:
            # Spot market sell: prefer base qty from local position store
            p = self.position_store.get(o.symbol)
            if not p or float(p.qty) <= 0:
                raise ValueError(f"No position qty available for sell market: {o.symbol}")
            payload["sz"] = str(float(p.qty))
            payload["tgtCcy"] = "base_ccy"

        return payload

    def place(self, o: Order) -> LiveExecutionResult:
        gate, reconcile_ok, kill_switch = submit_gate_for_live(self.cfg)

        # Gate: block all buys (OPEN/REBALANCE) in SELL_ONLY mode.
        if gate == "SELL_ONLY" and o.side == "buy" and o.intent in {"OPEN_LONG", "REBALANCE"}:
            dh = self._decision_hash_for_order(o)
            inst_id = symbol_to_inst_id(o.symbol)
            clid = make_cl_ord_id(self.run_id, inst_id, o.intent, dh, o.side, "market", "cash")
            self.order_store.upsert_new(
                cl_ord_id=clid,
                run_id=self.run_id,
                inst_id=inst_id,
                side=o.side,
                intent=o.intent,
                decision_hash=dh,
                td_mode="cash",
                ord_type="market",
                notional_usdt=float(o.notional_usdt),
                window_start_ts=(o.meta or {}).get("window_start_ts"),
                window_end_ts=(o.meta or {}).get("window_end_ts"),
                req={"blocked_by_gate": True, "gate": gate},
                reconcile_ok_at_submit=reconcile_ok,
                kill_switch_at_submit=kill_switch,
                submit_gate=gate,
            )
            self.order_store.update_state(clid, new_state="REJECTED", last_error_code="GATE", last_error_msg="SELL_ONLY")
            return LiveExecutionResult(cl_ord_id=clid, state="REJECTED")

        inst_id = symbol_to_inst_id(o.symbol)
        dh = self._decision_hash_for_order(o)
        clid = make_cl_ord_id(self.run_id, inst_id, o.intent, dh, o.side, "market", "cash")

        existing = self.order_store.get(clid)
        if existing:
            st0 = str(existing.state).upper()
            if st0 in {"FILLED", "CANCELED", "REJECTED"}:
                return LiveExecutionResult(cl_ord_id=clid, state=st0, ord_id=existing.ord_id)
            # Idempotency: if we already have a record for this clOrdId, do not resubmit.
            st, ord_id = self._query_and_update(inst_id=inst_id, cl_ord_id=clid)
            return LiveExecutionResult(cl_ord_id=clid, state=st, ord_id=ord_id)

        payload = self._build_place_payload(o, inst_id=inst_id, cl_ord_id=clid)

        # 1) persist intent before sending
        self.order_store.upsert_new(
            cl_ord_id=clid,
            run_id=self.run_id,
            inst_id=inst_id,
            side=o.side,
            intent=o.intent,
            decision_hash=dh,
            td_mode="cash",
            ord_type="market",
            notional_usdt=float(o.notional_usdt),
            window_start_ts=(o.meta or {}).get("window_start_ts"),
            window_end_ts=(o.meta or {}).get("window_end_ts"),
            req=payload,
            reconcile_ok_at_submit=reconcile_ok,
            kill_switch_at_submit=kill_switch,
            submit_gate=gate,
        )

        # 2) send
        self.order_store.update_state(clid, new_state="SENT")

        try:
            ack = self.okx.place_order(payload, exp_time_ms=self.exp_time_ms)
            self.order_store.update_state(
                clid,
                new_state="ACK",
                ack=ack.data,
                ord_id=self._extract_ord_id(ack),
                event_type="ACK",
            )
            # follow-up poll to get state if possible
            polled = self._query_and_update(inst_id=inst_id, cl_ord_id=clid)
            return LiveExecutionResult(cl_ord_id=clid, state=polled[0], ord_id=polled[1])

        except OKXPrivateClientError as e:
            # network/timeout/etc. => do NOT resubmit; query by clOrdId
            self.order_store.update_state(
                clid,
                new_state="UNKNOWN",
                last_error_code="CLIENT",
                last_error_msg=str(e),
                event_type="PLACE_ERROR",
            )
            # short delay then query (eventual consistency)
            time.sleep(0.25)
            st, ord_id = self._query_and_update(inst_id=inst_id, cl_ord_id=clid)
            return LiveExecutionResult(cl_ord_id=clid, state=st, ord_id=ord_id)

    def _extract_ord_id(self, resp: OKXResponse) -> Optional[str]:
        d = resp.data if isinstance(resp.data, dict) else {}
        rows = d.get("data")
        if isinstance(rows, list) and rows:
            r0 = rows[0] or {}
            oid = r0.get("ordId") or r0.get("ord_id")
            return str(oid) if oid else None
        return None

    def _query_and_update(self, *, inst_id: str, cl_ord_id: str) -> Tuple[str, Optional[str]]:
        try:
            qr = self.okx.get_order(inst_id=inst_id, cl_ord_id=cl_ord_id)
        except Exception as e:
            self.order_store.update_state(cl_ord_id, new_state="UNKNOWN", last_error_code="QUERY", last_error_msg=str(e), event_type="QUERY_ERROR")
            return "UNKNOWN", None

        d = qr.data if isinstance(qr.data, dict) else {}
        rows = d.get("data")
        if not isinstance(rows, list) or not rows:
            # not found yet
            self.order_store.update_state(cl_ord_id, new_state="UNKNOWN", last_query=d, event_type="QUERY_NOT_FOUND")
            return "UNKNOWN", None

        r0 = rows[0] or {}
        okx_state = r0.get("state")
        st = map_okx_state(okx_state)
        ord_id = r0.get("ordId") or r0.get("ord_id")
        self.order_store.update_state(
            cl_ord_id,
            new_state=st,
            ord_id=str(ord_id) if ord_id else None,
            last_query=d,
            acc_fill_sz=r0.get("accFillSz") or r0.get("acc_fill_sz"),
            avg_px=r0.get("avgPx") or r0.get("avg_px"),
            fee=r0.get("fee"),
            event_type="POLL",
        )
        return st, (str(ord_id) if ord_id else None)

    def cancel(self, *, symbol: str, cl_ord_id: str) -> bool:
        inst_id = symbol_to_inst_id(symbol)
        try:
            ack = self.okx.cancel_order(inst_id=inst_id, cl_ord_id=cl_ord_id)
            self.order_store.update_state(cl_ord_id, new_state="CANCELED", ack=ack.data, event_type="CANCEL")
            return True
        except Exception as e:
            self.order_store.update_state(cl_ord_id, new_state="UNKNOWN", last_error_code="CANCEL", last_error_msg=str(e), event_type="CANCEL_ERROR")
            return False

    def poll_open(self, limit: int = 200) -> List[LiveExecutionResult]:
        # 0) Optional: sync fills into FillStore then reconcile into OrderStore.
        try:
            from src.execution.fill_store import FillStore
            from src.execution.fill_reconciler import FillReconciler

            fs = FillStore(path="reports/fills.sqlite")
            rec = FillReconciler(fill_store=fs, order_store=self.order_store, okx=self.okx)
            rec.reconcile(limit=2000, max_get_order_per_run=20)
        except Exception:
            pass

        out: List[LiveExecutionResult] = []
        rows = self.order_store.list_open(limit=limit)
        for r in rows:
            st, oid = self._query_and_update(inst_id=str(r.inst_id), cl_ord_id=str(r.cl_ord_id))
            out.append(LiveExecutionResult(cl_ord_id=str(r.cl_ord_id), state=st, ord_id=oid))
        return out

    def execute(self, order_batch: List[Order]) -> ExecutionReport:
        # Minimal batch executor; used by main() once wired.
        ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        placed: List[Order] = []
        for o in order_batch or []:
            try:
                self.place(o)
                placed.append(o)
            except Exception as e:
                log.error(f"place failed for {o.symbol} {o.side} {o.intent}: {e}")
        return ExecutionReport(timestamp=ts, dry_run=False, orders=placed)
