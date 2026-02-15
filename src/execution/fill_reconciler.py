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


log = logging.getLogger(__name__)


def _dec(x: Optional[str]) -> Decimal:
    if x is None or x == "":
        return Decimal("0")
    return Decimal(str(x))


@dataclass
class FillAgg:
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
    - Uses FillStore sync_state key `last_reconcile_created_ts_ms` and processes new fills by created_ts_ms.
    """

    def __init__(self, *, fill_store: FillStore, order_store: OrderStore, okx: Optional[OKXPrivateClient] = None):
        self.fill_store = fill_store
        self.order_store = order_store
        self.okx = okx

    def _load_new_fills(self, since_created_ms: int, limit: int = 2000) -> List[Dict[str, Any]]:
        # We need created_ts_ms field which is internal; query directly.
        con = sqlite3.connect(str(self.fill_store.path))
        cur = con.cursor()
        cur.execute(
            """
            SELECT inst_id, trade_id, ts_ms, ord_id, cl_ord_id, side, exec_type,
                   fill_px, fill_sz, fee, fee_ccy, raw_json, created_ts_ms
            FROM fills
            WHERE created_ts_ms > ?
            ORDER BY created_ts_ms ASC
            LIMIT ?
            """,
            (int(since_created_ms), int(limit)),
        )
        rows = cur.fetchall()
        con.close()
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "inst_id": r[0],
                    "trade_id": r[1],
                    "ts_ms": int(r[2]),
                    "ord_id": r[3],
                    "cl_ord_id": r[4],
                    "side": r[5],
                    "exec_type": r[6],
                    "fill_px": r[7],
                    "fill_sz": r[8],
                    "fee": r[9],
                    "fee_ccy": r[10],
                    "raw_json": r[11],
                    "created_ts_ms": int(r[12]),
                }
            )
        return out

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

    def reconcile(self, *, limit: int = 2000) -> Dict[str, Any]:
        since = self.fill_store.get_state("last_reconcile_created_ts_ms")
        since_ms = int(since) if since else 0

        fills = self._load_new_fills(since_ms, limit=limit)
        if not fills:
            return {"new_fills": 0, "updated_orders": 0}

        max_created = max(int(f.get("created_ts_ms") or 0) for f in fills)

        aggs = self._aggregate(fills)
        updated = 0

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

            # Always at least PARTIAL if fill exists
            fee_json = json.dumps({k: str(v) for k, v in a.fees_by_ccy.items()}, ensure_ascii=False, separators=(",", ":"))
            self.order_store.update_state(
                clid,
                new_state="PARTIAL" if a.acc_fill_sz > 0 else str(row.state),
                acc_fill_sz=str(a.acc_fill_sz),
                avg_px=(str(a.vwap_px) if a.vwap_px is not None else None),
                fee=fee_json if a.fees_by_ccy else None,
                event_type="FILL_AGG",
            )
            updated += 1

            # Confirm terminal state via get_order when possible
            if self.okx is not None:
                try:
                    r = self.okx.get_order(inst_id=str(row.inst_id), ord_id=row.ord_id, cl_ord_id=row.cl_ord_id)
                    d = (r.data or {}).get("data") or []
                    if isinstance(d, list) and d:
                        st = str((d[0] or {}).get("state") or "")
                        st_l = st.lower()
                        if st_l in {"filled", "canceled", "cancelled", "mmp_canceled"}:
                            mapped = "FILLED" if st_l == "filled" else "CANCELED"
                            self.order_store.update_state(clid, new_state=mapped, last_query=r.data, event_type="ORDER_STATE")
                except Exception as e:
                    log.debug(f"get_order confirm failed for {clid}: {e}")

        self.fill_store.set_state("last_reconcile_created_ts_ms", str(int(max_created)))
        return {"new_fills": len(fills), "updated_orders": updated, "max_created_ts_ms": int(max_created)}
