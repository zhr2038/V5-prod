from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from src.execution.order_store import OrderStore


def _now_ms() -> int:
    return int(time.time() * 1000)


def _j(s: str) -> Dict[str, Any]:
    try:
        obj = json.loads(s or "{}")
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _looks_not_found(last_query_json: str) -> bool:
    d = _j(last_query_json)
    if str(d.get("code") or "") == "51603":
        return True
    msg = str(d.get("msg") or "").lower()
    if "does not exist" in msg:
        return True
    return False


@dataclass
class GCStats:
    """GCStats类"""
    scanned: int = 0
    gc_rejected: int = 0
    skipped: int = 0

    def as_dict(self) -> Dict[str, int]:
        """As dict"""
        return {"scanned": int(self.scanned), "gc_rejected": int(self.gc_rejected), "skipped": int(self.skipped)}


def gc_unknown_orders(*, db_path: str = "reports/orders.sqlite", ttl_sec: int = 1800, limit: int = 500) -> Dict[str, Any]:
    """Garbage-collect stale UNKNOWN orders.

    Pure local operation (no network). Intended to keep OrderStore health signals clean.

    Rules (conservative):
    - candidate: state=UNKNOWN and updated_ts older than ttl_sec
    - if last_query indicates NOT_FOUND (51603), mark REJECTED/NOT_FOUND
    - else if ack_json empty and no ord_id, mark REJECTED/EXPIRED
    """

    st = GCStats()
    store = OrderStore(path=str(db_path))

    con = sqlite3.connect(str(store.path))
    cur = con.cursor()

    cutoff = _now_ms() - int(ttl_sec) * 1000
    cur.execute(
        """
        SELECT cl_ord_id, ord_id, ack_json, last_query_json, updated_ts
        FROM orders
        WHERE state='UNKNOWN' AND updated_ts < ?
        ORDER BY updated_ts ASC
        LIMIT ?
        """,
        (int(cutoff), int(limit)),
    )
    rows = cur.fetchall()
    con.close()

    for clid, ord_id, ack_json, last_query_json, updated_ts in rows:
        st.scanned += 1
        ack = (ack_json or "{}").strip()
        lq = (last_query_json or "{}").strip()

        if _looks_not_found(lq):
            store.update_state(
                str(clid),
                new_state="REJECTED",
                last_error_code="NOT_FOUND",
                last_error_msg=f"UNKNOWN_TTL: last_query=51603 updated_ts={updated_ts}",
                event_type="UNKNOWN_TTL_REJECTED",
            )
            st.gc_rejected += 1
            continue

        if (not ord_id) and (ack in {"", "{}"}):
            store.update_state(
                str(clid),
                new_state="REJECTED",
                last_error_code="EXPIRED",
                last_error_msg=f"UNKNOWN_TTL: no_ack_no_ord_id updated_ts={updated_ts}",
                event_type="UNKNOWN_TTL_REJECTED",
            )
            st.gc_rejected += 1
            continue

        st.skipped += 1

    return {"stats": st.as_dict(), "cutoff_ts_ms": int(cutoff)}
