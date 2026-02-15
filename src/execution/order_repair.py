from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from src.execution.order_store import OrderStore


def _parse_okx_ack(ack_data: Any) -> Tuple[bool, Optional[str], Optional[str]]:
    """Return (accepted, err_code, err_msg).

    accepted=True means OKX accepted the order (code==0 and sCode==0).
    accepted=False means exchange explicitly rejected it.
    """

    d = ack_data if isinstance(ack_data, dict) else {}
    code = d.get("code")
    msg = d.get("msg")
    code_s = str(code) if code is not None else None

    rows = d.get("data")
    r0 = (rows[0] if isinstance(rows, list) and rows else {}) or {}
    s_code = r0.get("sCode")
    s_msg = r0.get("sMsg")

    # OKX semantics: accepted iff both codes are zero.
    if code_s and code_s != "0":
        err_code = str(s_code) if s_code is not None and str(s_code) != "0" else str(code_s)
        err_msg = str(s_msg) if s_msg else (str(msg) if msg else None)
        return False, err_code, err_msg

    if s_code is not None and str(s_code) != "0":
        return False, str(s_code), (str(s_msg) if s_msg else None)

    return True, None, None


@dataclass
class RepairStats:
    scanned: int = 0
    repaired: int = 0
    skipped: int = 0

    def as_dict(self) -> Dict[str, int]:
        return {"scanned": int(self.scanned), "repaired": int(self.repaired), "skipped": int(self.skipped)}


def _has_repair_event(con: sqlite3.Connection, cl_ord_id: str) -> bool:
    cur = con.cursor()
    cur.execute(
        "SELECT 1 FROM order_events WHERE cl_ord_id=? AND event_type='REPAIR_REJECTED' LIMIT 1",
        (str(cl_ord_id),),
    )
    return cur.fetchone() is not None


def repair_unknown_orders(*, db_path: str = "reports/orders.sqlite", limit: int = 500) -> Dict[str, Any]:
    """Repair legacy UNKNOWN orders using persisted ack_json.

    This is a pure-local DB operation. No network calls.
    """

    st = RepairStats()
    by_code: Dict[str, int] = {}

    store = OrderStore(path=str(db_path))
    con = sqlite3.connect(str(store.path))
    cur = con.cursor()

    cur.execute(
        """
        SELECT cl_ord_id, state, ack_json, last_error_code
        FROM orders
        WHERE state='UNKNOWN'
          AND ack_json IS NOT NULL
          AND ack_json != '{}'
        ORDER BY updated_ts DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    rows = cur.fetchall()

    for clid, state, ack_json, last_error_code in rows:
        st.scanned += 1

        # idempotency: already repaired once
        if _has_repair_event(con, clid):
            st.skipped += 1
            continue

        # If already has an error code, we still repair only if ack indicates reject.
        try:
            ack = json.loads(ack_json) if ack_json else {}
        except Exception:
            ack = {}

        accepted, err_code, err_msg = _parse_okx_ack(ack)
        if accepted:
            st.skipped += 1
            continue

        # apply repair
        store.update_state(
            str(clid),
            new_state="REJECTED",
            last_error_code=str(err_code) if err_code else (str(last_error_code) if last_error_code else None),
            last_error_msg=str(err_msg) if err_msg else None,
            event_type="REPAIR_REJECTED",
        )
        st.repaired += 1
        if err_code:
            by_code[str(err_code)] = int(by_code.get(str(err_code), 0)) + 1

    con.close()

    return {"stats": st.as_dict(), "by_error_code": by_code}
