from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from src.execution.fill_store import FillStore
from src.execution.okx_private_client import OKXPrivateClient
from src.execution.order_store import OrderStore
from src.execution.position_store import PositionStore


log = logging.getLogger(__name__)


def _dec(x: Optional[str]) -> Decimal:
    if x is None or x == "":
        return Decimal("0")
    return Decimal(str(x))


def _opt_dec(x: Optional[str]) -> Optional[Decimal]:
    if x is None or x == "":
        return None
    try:
        return Decimal(str(x))
    except Exception:
        return None


def _fee_map(raw_fee: Optional[str]) -> Dict[str, Decimal]:
    if raw_fee is None or str(raw_fee).strip() == "":
        return {}
    try:
        obj = json.loads(str(raw_fee))
    except Exception:
        return {}
    if not isinstance(obj, dict):
        return {}
    out: Dict[str, Decimal] = {}
    for ccy, value in obj.items():
        try:
            out[str(ccy).upper()] = Decimal(str(value))
        except Exception:
            continue
    return out


def _merge_fee_maps(*fee_maps: Dict[str, Decimal]) -> Dict[str, Decimal]:
    out: Dict[str, Decimal] = {}
    for fee_map in fee_maps:
        for ccy, value in (fee_map or {}).items():
            key = str(ccy).upper()
            out[key] = out.get(key, Decimal("0")) + Decimal(str(value))
    return out


def _merge_fill_stats(
    *,
    prev_acc_fill_sz: Decimal,
    prev_avg_px: Optional[Decimal],
    delta_acc_fill_sz: Decimal,
    delta_vwap_px: Optional[Decimal],
) -> Tuple[Decimal, Optional[Decimal]]:
    total_acc_fill_sz = prev_acc_fill_sz + delta_acc_fill_sz
    if total_acc_fill_sz <= 0:
        return total_acc_fill_sz, None

    total_notional = Decimal("0")
    if prev_acc_fill_sz > 0 and prev_avg_px is not None and prev_avg_px > 0:
        total_notional += prev_acc_fill_sz * prev_avg_px
    if delta_acc_fill_sz > 0 and delta_vwap_px is not None and delta_vwap_px > 0:
        total_notional += delta_acc_fill_sz * delta_vwap_px

    if total_notional > 0:
        return total_acc_fill_sz, (total_notional / total_acc_fill_sz)
    if prev_acc_fill_sz > 0 and prev_avg_px is not None:
        return total_acc_fill_sz, prev_avg_px
    return total_acc_fill_sz, delta_vwap_px


@dataclass
class FillAgg:
    """FillAgg类"""
    inst_id: str
    cl_ord_id: Optional[str]
    ord_id: Optional[str]

    acc_fill_sz: Decimal
    vwap_px: Optional[Decimal]
    fees_by_ccy: Dict[str, Decimal]


class FillReconciler:
    """Reconcile OKX fills into OrderStore.

    Rules:
    - Association priority: clOrdId -> ordId -> ignore (keep only in FillStore)
    - State semantics:
      - If any fill exists => at least PARTIAL
      - Terminal state is authoritative from get_order.state (filled/canceled/mmp_canceled)
    - Fees are aggregated per feeCcy.

    Incremental processing:
    - Uses FillStore.fill_processed (S1) to ensure each (inst_id, trade_id) is processed once.
    """

    def __init__(
        self,
        *,
        fill_store: FillStore,
        order_store: OrderStore,
        okx: Optional[OKXPrivateClient] = None,
        position_store: Optional[PositionStore] = None,
    ):
        self.fill_store = fill_store
        self.order_store = order_store
        self.okx = okx
        self.position_store = position_store

    def _apply_position_delta(self, row, agg: FillAgg) -> None:
        if self.position_store is None:
            return
        if agg.acc_fill_sz <= 0:
            return

        side = str(row.side or "").lower()
        if side not in {"buy", "sell"}:
            return

        symbol = str(row.inst_id).replace("-", "/")
        base_ccy = str(row.inst_id).split("-")[0].upper()
        base_fee = agg.fees_by_ccy.get(base_ccy, Decimal("0"))

        if side == "buy":
            delta_qty = agg.acc_fill_sz + base_fee
            if delta_qty <= 0:
                return
            fill_px = float(agg.vwap_px) if agg.vwap_px is not None else 0.0
            if fill_px <= 0:
                return
            self.position_store.upsert_buy(symbol, qty=float(delta_qty), px=fill_px)
            return

        delta_qty = -agg.acc_fill_sz + base_fee
        if delta_qty >= 0:
            return

        p = self.position_store.get(symbol)
        if p is None:
            log.warning("partial sell fill arrived for missing local position: %s", symbol)
            return

        new_qty = max(0.0, float(p.qty) + float(delta_qty))
        if new_qty <= 0:
            self.position_store.close_long(symbol)
            try:
                from src.execution.live_execution_engine import clear_risk_state_on_full_close

                clear_risk_state_on_full_close(symbol)
            except Exception:
                pass
        else:
            self.position_store.set_qty(symbol, qty=new_qty)

    def _load_unprocessed_fills(self, limit: int = 2000) -> List[Dict[str, Any]]:
        return self.fill_store.list_unprocessed(limit=limit)

    def _aggregate(self, fills: List[Dict[str, Any]]) -> List[FillAgg]:
        # group key: (inst_id, cl_ord_id or '', ord_id or '') but we keep both
        groups: Dict[Tuple[str, Optional[str], Optional[str]], List[Dict[str, Any]]] = {}
        for f in fills:
            k = (str(f.get("inst_id")), f.get("cl_ord_id"), f.get("ord_id"))
            groups.setdefault(k, []).append(f)

        aggs: List[FillAgg] = []
        for (inst_id, clid, oid), xs in groups.items():
            sum_sz = Decimal("0")
            sum_px_sz = Decimal("0")
            fees: Dict[str, Decimal] = {}

            for it in xs:
                sz = _dec(it.get("fill_sz"))
                px = _dec(it.get("fill_px"))
                if sz > 0 and px > 0:
                    sum_sz += sz
                    sum_px_sz += px * sz

                ccy = it.get("fee_ccy")
                fee = it.get("fee")
                if ccy is not None and fee is not None and str(ccy) != "":
                    fees[str(ccy)] = fees.get(str(ccy), Decimal("0")) + _dec(str(fee))

            vwap = (sum_px_sz / sum_sz) if sum_sz > 0 else None
            aggs.append(
                FillAgg(
                    inst_id=inst_id,
                    cl_ord_id=str(clid) if clid else None,
                    ord_id=str(oid) if oid else None,
                    acc_fill_sz=sum_sz,
                    vwap_px=vwap,
                    fees_by_ccy=fees,
                )
            )

        return aggs

    def reconcile(self, *, limit: int = 2000, max_get_order_per_run: int = 20) -> Dict[str, Any]:
        """Reconcile"""
        fills = self._load_unprocessed_fills(limit=limit)
        if not fills:
            return {"new_fills": 0, "updated_orders": 0, "get_order_calls": 0, "fills_exported": 0, "export_errors": 0}

        aggs = self._aggregate(fills)
        updated = 0
        get_calls = 0

        # NOTE: processed marker is written *only after* successful export (trades/cost_events).
        # This prevents data loss if exporter fails mid-run.

        for a in aggs:
            # Associate
            row = None
            if a.cl_ord_id:
                row = self.order_store.get(a.cl_ord_id)
            if row is None and a.ord_id:
                row = self.order_store.get_by_ord_id(a.ord_id)

            if row is None:
                continue

            clid = str(row.cl_ord_id)
            row_state_before = str(row.state or "").upper()
            prev_acc_fill_sz = _dec(getattr(row, "acc_fill_sz", None))
            prev_avg_px = _opt_dec(getattr(row, "avg_px", None))
            prev_fee_map = _fee_map(getattr(row, "fee", None))
            total_acc_fill_sz, total_avg_px = _merge_fill_stats(
                prev_acc_fill_sz=prev_acc_fill_sz,
                prev_avg_px=prev_avg_px,
                delta_acc_fill_sz=a.acc_fill_sz,
                delta_vwap_px=a.vwap_px,
            )
            total_fee_map = _merge_fee_maps(prev_fee_map, a.fees_by_ccy)

            # Always at least PARTIAL if fill exists
            fee_json = json.dumps({k: str(v) for k, v in total_fee_map.items()}, ensure_ascii=False, separators=(",", ":"))
            self.order_store.update_state(
                clid,
                new_state="PARTIAL" if a.acc_fill_sz > 0 else str(row.state),
                acc_fill_sz=str(total_acc_fill_sz),
                avg_px=(str(total_avg_px) if total_avg_px is not None else None),
                fee=fee_json if total_fee_map else None,
                event_type="FILL_AGG",
            )
            if row_state_before != "FILLED":
                self._apply_position_delta(row, a)
            updated += 1

            # Confirm terminal state via get_order when possible
            if self.okx is not None and get_calls < int(max_get_order_per_run):
                try:
                    r = self.okx.get_order(inst_id=str(row.inst_id), ord_id=row.ord_id, cl_ord_id=row.cl_ord_id)
                    get_calls += 1
                    d = (r.data or {}).get("data") or []
                    if isinstance(d, list) and d:
                        st = str((d[0] or {}).get("state") or "")
                        st_l = st.lower()
                        if st_l in {"filled", "canceled", "cancelled", "mmp_canceled"}:
                            mapped = "FILLED" if st_l == "filled" else "CANCELED"
                            self.order_store.update_state(clid, new_state=mapped, last_query=r.data, event_type="ORDER_STATE")
                except Exception as e:
                    log.debug(f"get_order confirm failed for {clid}: {e}")

        # Export per-fill trades/cost events and mark processed only after export.
        exported = 0
        export_errors = 0
        try:
            from src.reporting.fill_trade_exporter import export_fill
        except Exception:
            export_fill = None

        # Cache per-run context for richer cost_events bucketing (regime/deadband/drift)
        run_ctx_cache: Dict[str, Dict[str, Any]] = {}

        def _load_run_ctx(run_id: str) -> Dict[str, Any]:
            if run_id in run_ctx_cache:
                return run_ctx_cache[run_id]
            ctx: Dict[str, Any] = {"regime": None, "by_symbol": {}}
            try:
                import os
                from pathlib import Path

                p = Path(os.path.join("reports", "runs", str(run_id), "decision_audit.json"))
                if p.exists():
                    obj = json.loads(p.read_text(encoding="utf-8"))
                    ctx["regime"] = obj.get("regime")
                    # router_decisions entries often include: symbol, drift, deadband
                    by_sym: Dict[str, Dict[str, Any]] = {}
                    for it in (obj.get("router_decisions") or []):
                        if not isinstance(it, dict):
                            continue
                        sym = it.get("symbol")
                        if not sym:
                            continue
                        by_sym[str(sym)] = it
                    ctx["by_symbol"] = by_sym
            except Exception:
                pass
            run_ctx_cache[run_id] = ctx
            return ctx

        for f in fills:
            inst_id = str(f.get("inst_id") or "")
            trade_id = str(f.get("trade_id") or "")
            if not inst_id or not trade_id:
                continue

            # associate for run_id/intent/window
            row = None
            clid_f = f.get("cl_ord_id")
            oid_f = f.get("ord_id")
            if clid_f:
                row = self.order_store.get(str(clid_f))
            if row is None and oid_f:
                row = self.order_store.get_by_ord_id(str(oid_f))

            if row is None or export_fill is None:
                continue

            try:
                # enrich export with per-run regime and per-symbol deadband/drift when available
                run_id = str(row.run_id)
                symbol = str(inst_id).replace("-", "/")
                ctx = _load_run_ctx(run_id)
                sym_ctx = (ctx.get("by_symbol") or {}).get(symbol) if isinstance(ctx, dict) else None
                export_fill(
                    fill_ts_ms=int(f.get("ts_ms") or 0),
                    inst_id=inst_id,
                    side=str(f.get("side") or row.side or ""),
                    fill_px=str(f.get("fill_px") or "0"),
                    fill_sz=str(f.get("fill_sz") or "0"),
                    fee=f.get("fee"),
                    fee_ccy=f.get("fee_ccy"),
                    run_id=run_id,
                    intent=str(row.intent),
                    window_start_ts=row.window_start_ts,
                    window_end_ts=row.window_end_ts,
                    run_dir=f"reports/runs/{row.run_id}",
                    regime=(ctx.get("regime") if isinstance(ctx, dict) else None),
                    deadband_pct=((sym_ctx or {}).get("deadband") if isinstance(sym_ctx, dict) else None),
                    drift=((sym_ctx or {}).get("drift") if isinstance(sym_ctx, dict) else None),
                    cl_ord_id=str(row.cl_ord_id),
                    order_store_path=str(getattr(self.order_store, 'path', 'reports/orders.sqlite')),
                )
                self.fill_store.mark_processed(inst_id, trade_id)
                exported += 1
            except Exception:
                export_errors += 1

        return {
            "new_fills": len(fills),
            "updated_orders": updated,
            "get_order_calls": int(get_calls),
            "fills_exported": int(exported),
            "export_errors": int(export_errors),
        }
