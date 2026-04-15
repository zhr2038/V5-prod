from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = (PROJECT_ROOT / resolved).resolve()
    return resolved


@dataclass
class BillRow:
    """BillRow类"""
    bill_id: str
    ts_ms: int
    ccy: str

    bal_chg: Optional[str] = None
    bal: Optional[str] = None
    typ: Optional[str] = None
    sub_type: Optional[str] = None

    inst_type: Optional[str] = None
    inst_id: Optional[str] = None
    ord_id: Optional[str] = None
    cl_ord_id: Optional[str] = None

    sz: Optional[str] = None
    px: Optional[str] = None

    source: str = "bills"  # bills|bills_archive
    raw_json: str = "{}"


class BillsStore:
    """SQLite store for OKX account bills.

    Bills are the *ledger source of truth* (all balance-changing events).
    We store numeric strings as TEXT to preserve exact exchange values.
    """

    def __init__(self, path: str = "reports/bills.sqlite"):
        self.path = _resolve_path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bills (
              bill_id TEXT PRIMARY KEY,
              ts_ms INTEGER NOT NULL,
              ccy TEXT NOT NULL,

              bal_chg TEXT,
              bal TEXT,
              type TEXT,
              sub_type TEXT,

              inst_type TEXT,
              inst_id TEXT,
              ord_id TEXT,
              cl_ord_id TEXT,

              sz TEXT,
              px TEXT,

              source TEXT NOT NULL,
              raw_json TEXT NOT NULL,
              created_ts_ms INTEGER NOT NULL
            )
            """
        )

        cur.execute("CREATE INDEX IF NOT EXISTS idx_bills_ts ON bills(ts_ms)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bills_ccy ON bills(ccy)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bills_clid ON bills(cl_ord_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bills_oid ON bills(ord_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bills_inst ON bills(inst_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bills_type ON bills(type, sub_type)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_state (
              k TEXT PRIMARY KEY,
              v TEXT NOT NULL,
              updated_ts_ms INTEGER NOT NULL
            )
            """
        )

        con.commit()
        con.close()

    def get_state(self, key: str) -> Optional[str]:
        """Get state"""
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute("SELECT v FROM sync_state WHERE k=?", (str(key),))
        row = cur.fetchone()
        con.close()
        return str(row[0]) if row else None

    def set_state(self, key: str, value: str) -> None:
        """Set state"""
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            "INSERT INTO sync_state(k, v, updated_ts_ms) VALUES (?,?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_ts_ms=excluded.updated_ts_ms",
            (str(key), str(value), _now_ms()),
        )
        con.commit()
        con.close()

    def upsert_many(self, rows: Iterable[BillRow]) -> Tuple[int, int]:
        """Upsert many"""
        rows_list = list(rows)
        if not rows_list:
            return 0, 0

        now = _now_ms()
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()

        ins = 0
        total = 0
        for r in rows_list:
            total += 1
            cur.execute(
                """
                INSERT OR IGNORE INTO bills(
                  bill_id, ts_ms, ccy,
                  bal_chg, bal, type, sub_type,
                  inst_type, inst_id, ord_id, cl_ord_id,
                  sz, px,
                  source, raw_json, created_ts_ms
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(r.bill_id),
                    int(r.ts_ms),
                    str(r.ccy),
                    str(r.bal_chg) if r.bal_chg is not None else None,
                    str(r.bal) if r.bal is not None else None,
                    str(r.typ) if r.typ is not None else None,
                    str(r.sub_type) if r.sub_type is not None else None,
                    str(r.inst_type) if r.inst_type is not None else None,
                    str(r.inst_id) if r.inst_id is not None else None,
                    str(r.ord_id) if r.ord_id is not None else None,
                    str(r.cl_ord_id) if r.cl_ord_id is not None else None,
                    str(r.sz) if r.sz is not None else None,
                    str(r.px) if r.px is not None else None,
                    str(r.source),
                    str(r.raw_json or "{}"),
                    int(now),
                ),
            )
            ins += int(cur.rowcount)

        con.commit()
        con.close()
        return ins, total

    def count(self) -> int:
        """Count"""
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM bills")
        n = int(cur.fetchone()[0] or 0)
        con.close()
        return n

    def last_bill(self) -> Optional[Tuple[str, int]]:
        """Last bill"""
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute("SELECT bill_id, ts_ms FROM bills ORDER BY ts_ms DESC LIMIT 1")
        row = cur.fetchone()
        con.close()
        if not row:
            return None
        return str(row[0]), int(row[1])

    def list_by_ts(self, *, begin_ts_ms: int, end_ts_ms: int, limit: int = 50000) -> List[BillRow]:
        """List by ts"""
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            """
            SELECT bill_id, ts_ms, ccy, bal_chg, bal, type, sub_type,
                   inst_type, inst_id, ord_id, cl_ord_id, sz, px, source, raw_json
            FROM bills
            WHERE ts_ms > ? AND ts_ms <= ?
            ORDER BY ts_ms ASC
            LIMIT ?
            """,
            (int(begin_ts_ms), int(end_ts_ms), int(limit)),
        )
        rows = cur.fetchall()
        con.close()
        out: List[BillRow] = []
        for r in rows:
            out.append(
                BillRow(
                    bill_id=r[0],
                    ts_ms=int(r[1]),
                    ccy=r[2],
                    bal_chg=r[3],
                    bal=r[4],
                    typ=r[5],
                    sub_type=r[6],
                    inst_type=r[7],
                    inst_id=r[8],
                    ord_id=r[9],
                    cl_ord_id=r[10],
                    sz=r[11],
                    px=r[12],
                    source=r[13],
                    raw_json=r[14],
                )
            )
        return out


def parse_okx_bills(resp_data: Dict[str, Any], *, source: str = "bills") -> List[BillRow]:
    """Parse okx bills"""
    data = (resp_data or {}).get("data")
    if not isinstance(data, list):
        return []

    out: List[BillRow] = []
    for it in data:
        if not isinstance(it, dict):
            continue
        bid = it.get("billId")
        ts = it.get("ts")
        ccy = it.get("ccy")
        if bid is None or ts is None or ccy is None:
            continue
        try:
            ts_ms = int(ts)
        except Exception:
            continue

        out.append(
            BillRow(
                bill_id=str(bid),
                ts_ms=int(ts_ms),
                ccy=str(ccy),
                bal_chg=str(it.get("balChg")) if it.get("balChg") is not None else None,
                bal=str(it.get("bal")) if it.get("bal") is not None else None,
                typ=str(it.get("type")) if it.get("type") is not None else None,
                sub_type=str(it.get("subType")) if it.get("subType") is not None else None,
                inst_type=str(it.get("instType")) if it.get("instType") is not None else None,
                inst_id=str(it.get("instId")) if it.get("instId") is not None else None,
                ord_id=str(it.get("ordId")) if it.get("ordId") is not None else None,
                cl_ord_id=str(it.get("clOrdId")) if it.get("clOrdId") is not None else None,
                sz=str(it.get("sz")) if it.get("sz") is not None else None,
                px=str(it.get("px")) if it.get("px") is not None else None,
                source=str(source),
                raw_json=json.dumps(it, ensure_ascii=False, separators=(",", ":")),
            )
        )

    return out
