from __future__ import annotations

import json
import logging
import time

import requests
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from configs.schema import ExecutionConfig
from src.core.models import ExecutionReport, Order
from src.execution.clordid import make_cl_ord_id, make_decision_hash
from src.execution.okx_private_client import OKXPrivateClient, OKXPrivateClientError, OKXResponse
from src.execution.order_store import OrderStore
from src.execution.position_store import PositionStore
from src.data.okx_instruments import OKXSpotInstrumentsCache, round_down_to_lot


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


def _parse_okx_order_ack(ack_data: Any) -> Tuple[bool, Optional[str], Optional[str], Optional[str]]:
    """Return (ok, ord_id, err_code, err_msg)."""
    d = ack_data if isinstance(ack_data, dict) else {}
    code = d.get("code")
    msg = d.get("msg")
    code_s = str(code) if code is not None else None

    rows = d.get("data")
    r0 = (rows[0] if isinstance(rows, list) and rows else {}) or {}
    s_code = r0.get("sCode")
    s_msg = r0.get("sMsg")

    # OKX semantics: code==0 and sCode==0 means accepted.
    ok = True
    if code_s and code_s != "0":
        ok = False
    if s_code is not None and str(s_code) != "0":
        ok = False

    ord_id = r0.get("ordId") or r0.get("ord_id")

    if ok:
        return True, (str(ord_id) if ord_id else None), None, None

    err_code = str(s_code) if s_code is not None and str(s_code) != "0" else (str(code_s) if code_s else None)
    err_msg = str(s_msg) if s_msg else (str(msg) if msg else None)
    return False, None, err_code, err_msg


class DustOrderSkip(Exception):
    def __init__(self, symbol: str, *, qty: float, qty_rounded: float, min_sz: float, lot_sz: float):
        super().__init__(f"dust_skip {symbol}: qty={qty} rounded={qty_rounded} minSz={min_sz} lotSz={lot_sz}")
        self.symbol = symbol
        self.qty = float(qty)
        self.qty_rounded = float(qty_rounded)
        self.min_sz = float(min_sz)
        self.lot_sz = float(lot_sz)


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


def _public_mid_at_submit(*, inst_id: str, timeout_sec: float = 2.0) -> Optional[Dict[str, Any]]:
    """Best-effort fetch mid/bid/ask from OKX public ticker.

    Returns dict with keys: mid,bid,ask,ts_ms. None on failure.
    """

    try:
        url = "https://www.okx.com/api/v5/market/ticker"
        r = requests.get(url, params={"instId": str(inst_id)}, timeout=float(timeout_sec))
        r.raise_for_status()
        obj = r.json() if r is not None else {}
        rows = obj.get("data") if isinstance(obj, dict) else None
        if not isinstance(rows, list) or not rows:
            return None
        t = rows[0] if isinstance(rows[0], dict) else {}
        bid = float(t.get("bidPx") or 0.0)
        ask = float(t.get("askPx") or 0.0)
        ts = int(t.get("ts") or 0)
        if bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
        else:
            mid = float(t.get("last") or 0.0)
        if mid <= 0:
            return None
        return {"mid": float(mid), "bid": float(bid) if bid > 0 else None, "ask": float(ask) if ask > 0 else None, "ts_ms": ts or None}
    except Exception:
        return None


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
        
        # STRICT NO-BORROW ENFORCEMENT
        # Ensure we're using pure spot mode, no margin/borrow
        if td_mode != "cash":
            raise ValueError(f"Safety violation: tdMode must be 'cash' (no borrow), got '{td_mode}'")
        
        notional = float(o.notional_usdt)
        
        # Log trade intent for audit
        import logging
        log = logging.getLogger(__name__)
        log.info(f"TRADE_SAFETY: {side} {inst_id}, tdMode={td_mode}, intent={o.intent}, notional={notional:.4f}")

        if side == "buy":
            # Spot market buy: submit quote notional in USDT
            # OKX expects plain decimal string
            from decimal import Decimal

            payload["sz"] = format(Decimal(str(notional)), "f")
            payload["tgtCcy"] = "quote_ccy"
        else:
            # Spot market sell: STRICT NO-BORROW ENFORCEMENT
            # 1. Check local position store
            p = self.position_store.get(o.symbol)
            if not p or float(p.qty) <= 0:
                raise ValueError(f"NO_BORROW_SAFETY: No position qty available for sell: {o.symbol}. "
                               f"Would trigger borrow. Aborting.")
            
            # 2. Double-check with OKX balance before selling (best-effort).
            # If OKX explicitly reports negative balance or insufficient base, abort.
            try:
                balance_resp = self.okx.get_balance()
                base_ccy = o.symbol.split('/')[0]
                okx_base_eq = None

                rows = (balance_resp.data or {}).get("data") if isinstance(balance_resp.data, dict) else None
                details = ((rows[0] if isinstance(rows, list) and rows else {}) or {}).get("details")
                if isinstance(details, list):
                    for d in details:
                        if isinstance(d, dict) and str(d.get("ccy")) == str(base_ccy):
                            okx_base_eq = float(d.get("eq") or 0.0)
                            break

                if okx_base_eq is not None:
                    if okx_base_eq < 0:
                        raise ValueError(
                            f"NO_BORROW_SAFETY: OKX shows NEGATIVE balance for {o.symbol}: {okx_base_eq}. "
                            f"Would increase borrow. Aborting."
                        )

                    # Allow 10% tolerance for local store drift, but do not allow obvious oversell.
                    if okx_base_eq < float(p.qty) * 0.9:
                        raise ValueError(
                            f"NO_BORROW_SAFETY: OKX balance ({okx_base_eq}) < local position ({p.qty}) "
                            f"for {o.symbol}. Risk of borrow. Aborting."
                        )

            except ValueError:
                raise
            except Exception as e:
                log.warning(f"Balance pre-check failed (proceeding with caution): {e}")
            
            # 3. Log sell attempt for audit
            log.info(f"SELL_SAFETY_CHECK: Selling {o.symbol}, local_qty={p.qty}, intent={o.intent}")

            qty = float(p.qty)
            # Enforce OKX minSz/lotSz to avoid Parameter sz error.
            specs = OKXSpotInstrumentsCache().get_spec(inst_id)
            if specs is not None and float(specs.lot_sz or 0.0) > 0:
                qty_rounded = round_down_to_lot(qty, float(specs.lot_sz))
            else:
                qty_rounded = qty

            min_sz = float(specs.min_sz) if specs is not None else 0.0
            lot_sz = float(specs.lot_sz) if specs is not None else 0.0
            if min_sz > 0 and qty_rounded < min_sz:
                raise DustOrderSkip(o.symbol, qty=qty, qty_rounded=qty_rounded, min_sz=min_sz, lot_sz=lot_sz)

            # OKX rejects scientific notation (e.g. 5e-05) with 51000 Parameter sz error.
            # Always send plain decimal string and avoid float->str exponent.
            from decimal import Decimal, ROUND_DOWN

            lot = float(specs.lot_sz) if specs is not None and specs.lot_sz is not None else 0.0
            if lot and lot > 0:
                lot_dec = Decimal(str(lot))
                # quantize to lot step
                q = (Decimal(str(qty)) / lot_dec).to_integral_value(rounding=ROUND_DOWN) * lot_dec
                sz_str = format(q, "f")
            else:
                sz_str = format(Decimal(str(qty)), "f")

            payload["sz"] = sz_str
            # For spot market SELL, omit tgtCcy to avoid OKX 51000 on some instruments.

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

        try:
            payload = self._build_place_payload(o, inst_id=inst_id, cl_ord_id=clid)
            # Best-effort mid at submit for slippage attribution (do not send to exchange).
            tob = _public_mid_at_submit(inst_id=inst_id, timeout_sec=2.0)
            req_store = dict(payload)
            if tob:
                req_store["_meta"] = {"mid_px_at_submit": tob.get("mid"), "bid": tob.get("bid"), "ask": tob.get("ask"), "ts_ms": tob.get("ts_ms")}
        except DustOrderSkip as e:
            # Persist and mark as rejected (terminal) without touching the exchange.
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
                req={"dust_skip": True, "symbol": e.symbol, "qty": e.qty, "qty_rounded": e.qty_rounded, "minSz": e.min_sz, "lotSz": e.lot_sz},
                reconcile_ok_at_submit=reconcile_ok,
                kill_switch_at_submit=kill_switch,
                submit_gate=gate,
            )
            self.order_store.update_state(clid, new_state="REJECTED", last_error_code="DUST", last_error_msg=str(e), event_type="DUST_SKIP")
            return LiveExecutionResult(cl_ord_id=clid, state="REJECTED")

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
            req=req_store,
            reconcile_ok_at_submit=reconcile_ok,
            kill_switch_at_submit=kill_switch,
            submit_gate=gate,
        )

        # 2) send
        self.order_store.update_state(clid, new_state="SENT")

        try:
            ack = self.okx.place_order(payload, exp_time_ms=self.exp_time_ms)
            ok_ack, ord_id, err_code, err_msg = _parse_okx_order_ack(ack.data)
            if not ok_ack:
                # Exchange explicitly rejected the order: terminal REJECTED, no polling.
                self.order_store.update_state(
                    clid,
                    new_state="REJECTED",
                    ack=ack.data,
                    last_error_code=err_code,
                    last_error_msg=err_msg,
                    event_type="REJECTED_ACK",
                )
                return LiveExecutionResult(cl_ord_id=clid, state="REJECTED")

            self.order_store.update_state(
                clid,
                new_state="ACK",
                ack=ack.data,
                ord_id=ord_id or self._extract_ord_id(ack),
                event_type="ACK",
            )
            # follow-up poll to get state if possible
            polled_state, polled_ord_id = self._query_and_update(inst_id=inst_id, cl_ord_id=clid)

            # Best-effort position sync on FILLED to keep local store consistent and avoid
            # NO_BORROW false positives on subsequent sells.
            try:
                if polled_state == "FILLED":
                    row = self.order_store.get(clid)
                    # parse last_query to extract accFillSz/avgPx if present
                    last_q = None
                    if row is not None and getattr(row, "last_query_json", None):
                        import json as _json

                        last_q = _json.loads(row.last_query_json)
                    elif row is not None and getattr(row, "ack_json", None):
                        import json as _json

                        last_q = _json.loads(row.ack_json)

                    r0 = None
                    if isinstance(last_q, dict):
                        rows = last_q.get("data")
                        if isinstance(rows, list) and rows:
                            r0 = rows[0] if isinstance(rows[0], dict) else None

                    acc_fill_sz = float((r0 or {}).get("accFillSz") or 0.0) if r0 else 0.0
                    avg_px = float((r0 or {}).get("avgPx") or 0.0) if r0 else 0.0
                    if acc_fill_sz > 0:
                        if str(o.side).lower() == "buy":
                            self.position_store.upsert_buy(o.symbol, qty=float(acc_fill_sz), px=float(avg_px or o.signal_price or 0.0))
                        elif str(o.side).lower() == "sell":
                            # reduce qty; if becomes dust, close
                            p = self.position_store.get(o.symbol)
                            if p is not None:
                                new_qty = max(0.0, float(p.qty) - float(acc_fill_sz))
                                if new_qty <= 0:
                                    self.position_store.close_long(o.symbol)
                                else:
                                    # keep avg_px unchanged on partial close
                                    self.position_store.set_qty(o.symbol, qty=new_qty)
            except Exception:
                pass

            return LiveExecutionResult(cl_ord_id=clid, state=polled_state, ord_id=polled_ord_id)

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
