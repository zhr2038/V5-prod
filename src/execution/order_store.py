from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


# Local monotonic state ranking: never move backward.
_STATE_RANK = {
    "NEW": 0,
    "SENT": 1,
    "ACK": 2,
    "OPEN": 3,
    "PARTIAL": 4,
    "FILLED": 5,
    "CANCELED": 6,
    "REJECTED": 6,
    "UNKNOWN": 6,
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _rank(state: str) -> int:
    return int(_STATE_RANK.get(str(state).upper(), 6))


@dataclass
class OrderRow:
    cl_ord_id: str
    run_id: str
    window_start_ts: Optional[int]
    window_end_ts: Optional[int]
    inst_id: str
    side: str
    intent: str
    decision_hash: str
    td_mode: str
    ord_type: str
    px: Optional[str]
    sz: Optional[str]
    notional_usdt: float
    state: str
    ord_id: Optional[str]
    req_json: str
    ack_json: str
    last_query_json: str
    last_error_code: Optional[str]
    last_error_msg: Optional[str]
    created_ts: int
    updated_ts: int
    last_poll_ts: Optional[int]
    acc_fill_sz: Optional[str]
    avg_px: Optional[str]
    fee: Optional[str]
    reconcile_ok_at_submit: Optional[int]
    kill_switch_at_submit: Optional[int]
    submit_gate: Optional[str]


class OrderStore:
    def __init__(self, path: str = "reports/orders.sqlite"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
              cl_ord_id TEXT PRIMARY KEY,
              run_id TEXT,
              window_start_ts INTEGER,
              window_end_ts INTEGER,
              inst_id TEXT,
              side TEXT,
              intent TEXT,
              decision_hash TEXT,
              td_mode TEXT,
              ord_type TEXT,
              px TEXT,
              sz TEXT,
              notional_usdt REAL,
              state TEXT,
              ord_id TEXT,
              req_json TEXT,
              ack_json TEXT,
              last_query_json TEXT,
              last_error_code TEXT,
              last_error_msg TEXT,
              created_ts INTEGER,
              updated_ts INTEGER,
              last_poll_ts INTEGER,
              acc_fill_sz TEXT,
              avg_px TEXT,
              fee TEXT,
              reconcile_ok_at_submit INTEGER,
              kill_switch_at_submit INTEGER,
              submit_gate TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS order_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              cl_ord_id TEXT NOT NULL,
              ts INTEGER NOT NULL,
              event_type TEXT NOT NULL,
              payload_json TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_state ON orders(state)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_run_id ON orders(run_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_ord_id ON orders(ord_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_order_events_clid ON order_events(cl_ord_id)")
        con.commit()
        con.close()

    def _event(self, con: sqlite3.Connection, cl_ord_id: str, event_type: str, payload: Dict[str, Any]) -> None:
        con.execute(
            "INSERT INTO order_events(cl_ord_id, ts, event_type, payload_json) VALUES (?,?,?,?)",
            (str(cl_ord_id), _now_ms(), str(event_type), json.dumps(payload or {}, ensure_ascii=False, separators=(",", ":"))),
        )

    def upsert_new(
        self,
        *,
        cl_ord_id: str,
        run_id: str,
        inst_id: str,
        side: str,
        intent: str,
        decision_hash: str,
        td_mode: str,
        ord_type: str,
        notional_usdt: float,
        window_start_ts: Optional[int] = None,
        window_end_ts: Optional[int] = None,
        px: Optional[str] = None,
        sz: Optional[str] = None,
        req: Optional[Dict[str, Any]] = None,
        reconcile_ok_at_submit: Optional[bool] = None,
        kill_switch_at_submit: Optional[bool] = None,
        submit_gate: Optional[str] = None,
    ) -> None:
        now = _now_ms()
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO orders(
              cl_ord_id, run_id, window_start_ts, window_end_ts,
              inst_id, side, intent, decision_hash, td_mode, ord_type,
              px, sz, notional_usdt,
              state, ord_id,
              req_json, ack_json, last_query_json,
              last_error_code, last_error_msg,
              created_ts, updated_ts, last_poll_ts,
              acc_fill_sz, avg_px, fee,
              reconcile_ok_at_submit, kill_switch_at_submit, submit_gate
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(cl_ord_id) DO UPDATE SET
              updated_ts=excluded.updated_ts
            """,
            (
                str(cl_ord_id),
                str(run_id),
                int(window_start_ts) if window_start_ts is not None else None,
                int(window_end_ts) if window_end_ts is not None else None,
                str(inst_id),
                str(side),
                str(intent),
                str(decision_hash),
                str(td_mode),
                str(ord_type),
                str(px) if px is not None else None,
                str(sz) if sz is not None else None,
                float(notional_usdt),
                "NEW",
                None,
                json.dumps(req or {}, ensure_ascii=False, separators=(",", ":")),
                "{}",
                "{}",
                None,
                None,
                now,
                now,
                None,
                None,
                None,
                None,
                1 if reconcile_ok_at_submit else 0 if reconcile_ok_at_submit is not None else None,
                1 if kill_switch_at_submit else 0 if kill_switch_at_submit is not None else None,
                str(submit_gate) if submit_gate is not None else None,
            ),
        )
        self._event(con, cl_ord_id, "UPSERT_NEW", {"state": "NEW", "req": req or {}})
        con.commit()
        con.close()

    def _row_from_sql(self, row) -> Optional[OrderRow]:
        return OrderRow(*row) if row else None

    def get(self, cl_ord_id: str) -> Optional[OrderRow]:
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            """
            SELECT
              cl_ord_id, run_id, window_start_ts, window_end_ts,
              inst_id, side, intent, decision_hash, td_mode, ord_type,
              px, sz, notional_usdt,
              state, ord_id,
              req_json, ack_json, last_query_json,
              last_error_code, last_error_msg,
              created_ts, updated_ts, last_poll_ts,
              acc_fill_sz, avg_px, fee,
              reconcile_ok_at_submit, kill_switch_at_submit, submit_gate
            FROM orders WHERE cl_ord_id=?
            """,
            (str(cl_ord_id),),
        )
        row = cur.fetchone()
        con.close()
        return self._row_from_sql(row)

    def get_by_ord_id(self, ord_id: str) -> Optional[OrderRow]:
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            """
            SELECT
              cl_ord_id, run_id, window_start_ts, window_end_ts,
              inst_id, side, intent, decision_hash, td_mode, ord_type,
              px, sz, notional_usdt,
              state, ord_id,
              req_json, ack_json, last_query_json,
              last_error_code, last_error_msg,
              created_ts, updated_ts, last_poll_ts,
              acc_fill_sz, avg_px, fee,
              reconcile_ok_at_submit, kill_switch_at_submit, submit_gate
            FROM orders WHERE ord_id=?
            """,
            (str(ord_id),),
        )
        row = cur.fetchone()
        con.close()
        return self._row_from_sql(row)

    def update_state(
        self,
        cl_ord_id: str,
        *,
        new_state: str,
        ord_id: Optional[str] = None,
        ack: Optional[Dict[str, Any]] = None,
        last_query: Optional[Dict[str, Any]] = None,
        last_error_code: Optional[str] = None,
        last_error_msg: Optional[str] = None,
        acc_fill_sz: Optional[str] = None,
        avg_px: Optional[str] = None,
        fee: Optional[str] = None,
        event_type: str = "STATE",
    ) -> None:
        new_state_u = str(new_state).upper()
        now = _now_ms()

        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute("SELECT state FROM orders WHERE cl_ord_id=?", (str(cl_ord_id),))
        r = cur.fetchone()
        if not r:
            con.close()
            raise KeyError(f"order not found: {cl_ord_id}")

        cur_state = str(r[0] or "UNKNOWN").upper()
        if _rank(new_state_u) < _rank(cur_state):
            # monotonic: ignore backward transitions
            self._event(con, cl_ord_id, "STATE_IGNORED", {"from": cur_state, "to": new_state_u})
            con.commit()
            con.close()
            return

        cur.execute(
            """
            UPDATE orders SET
              state=?,
              ord_id=COALESCE(?, ord_id),
              ack_json=COALESCE(?, ack_json),
              last_query_json=COALESCE(?, last_query_json),
              last_error_code=COALESCE(?, last_error_code),
              last_error_msg=COALESCE(?, last_error_msg),
              updated_ts=?,
              last_poll_ts=COALESCE(?, last_poll_ts),
              acc_fill_sz=COALESCE(?, acc_fill_sz),
              sz=COALESCE(?, sz),
              avg_px=COALESCE(?, avg_px),
              fee=COALESCE(?, fee)
            WHERE cl_ord_id=?
            """,
            (
                new_state_u,
                str(ord_id) if ord_id is not None else None,
                json.dumps(ack, ensure_ascii=False, separators=(",", ":")) if ack is not None else None,
                json.dumps(last_query, ensure_ascii=False, separators=(",", ":")) if last_query is not None else None,
                str(last_error_code) if last_error_code is not None else None,
                str(last_error_msg) if last_error_msg is not None else None,
                now,
                now,
                str(acc_fill_sz) if acc_fill_sz is not None else None,
                str(acc_fill_sz) if acc_fill_sz is not None else None,
                str(avg_px) if avg_px is not None else None,
                str(fee) if fee is not None else None,
                str(cl_ord_id),
            ),
        )

        payload = {
            "from": cur_state,
            "to": new_state_u,
            "ord_id": ord_id,
            "err": {"code": last_error_code, "msg": last_error_msg} if (last_error_code or last_error_msg) else None,
        }
        if ack is not None:
            payload["ack"] = ack
        if last_query is not None:
            payload["last_query"] = last_query
        self._event(con, cl_ord_id, event_type, payload)

        con.commit()
        con.close()

    def list_open(self, limit: int = 200) -> List[OrderRow]:
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            """
            SELECT
              cl_ord_id, run_id, window_start_ts, window_end_ts,
              inst_id, side, intent, decision_hash, td_mode, ord_type,
              px, sz, notional_usdt,
              state, ord_id,
              req_json, ack_json, last_query_json,
              last_error_code, last_error_msg,
              created_ts, updated_ts, last_poll_ts,
              acc_fill_sz, avg_px, fee,
              reconcile_ok_at_submit, kill_switch_at_submit, submit_gate
            FROM orders
            WHERE state IN ('NEW','SENT','ACK','OPEN','PARTIAL','UNKNOWN')
            ORDER BY updated_ts DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        rows = cur.fetchall()
        con.close()
        return [OrderRow(*r) for r in rows]
