from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union


def _now_ms() -> int:
    return int(time.time() * 1000)


def derive_fill_store_path(order_store_path: Union[str, Path]) -> Path:
    """Derive the matching fills DB path from the effective orders DB path.

    Examples:
    - reports/orders.sqlite -> reports/fills.sqlite
    - reports/shadow_orders.sqlite -> reports/shadow_fills.sqlite
    - reports/orders_accelerated.sqlite -> reports/fills_accelerated.sqlite
    - reports/shadow_tuned_xgboost/orders.sqlite -> reports/shadow_tuned_xgboost/fills.sqlite
    """
    path = Path(order_store_path)
    if path.name == "orders.sqlite":
        return path.with_name("fills.sqlite")
    if "orders" in path.stem:
        return path.with_name(path.name.replace("orders", "fills", 1))
    return path.with_name("fills.sqlite")


def derive_runtime_reports_dir(order_store_path: Union[str, Path]) -> Path:
    """Derive the runtime reports directory from the effective orders DB path."""
    return Path(order_store_path).parent


def derive_runtime_runs_dir(order_store_path: Union[str, Path]) -> Path:
    """Derive the matching runs directory from the effective orders DB path."""
    return derive_runtime_reports_dir(order_store_path) / "runs"


def derive_runtime_cost_events_dir(order_store_path: Union[str, Path]) -> Path:
    """Derive the matching cost_events directory from the effective orders DB path."""
    return derive_runtime_reports_dir(order_store_path) / "cost_events"


def derive_runtime_spread_snapshots_dir(order_store_path: Union[str, Path]) -> Path:
    """Derive the matching spread_snapshots directory from the effective orders DB path."""
    return derive_runtime_reports_dir(order_store_path) / "spread_snapshots"


@dataclass
class FillRow:
    """FillRow类"""
    inst_id: str
    trade_id: str
    ts_ms: int

    ord_id: Optional[str] = None
    cl_ord_id: Optional[str] = None
    side: Optional[str] = None
    exec_type: Optional[str] = None

    fill_px: Optional[str] = None
    fill_sz: Optional[str] = None
    fill_notional: Optional[str] = None

    fee: Optional[str] = None
    fee_ccy: Optional[str] = None

    source: str = "fills"  # fills|fills_history
    raw_json: str = "{}"


class FillStore:
    """SQLite store for OKX fills.

    Idempotency / dedup rule:
    - For the same instId, tradeId should only be processed once.

    We store numeric strings as TEXT to preserve exact exchange values.
    """

    def __init__(self, path: str = "reports/fills.sqlite"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fills (
              inst_id TEXT NOT NULL,
              trade_id TEXT NOT NULL,
              ts_ms INTEGER NOT NULL,

              ord_id TEXT,
              cl_ord_id TEXT,
              side TEXT,
              exec_type TEXT,

              fill_px TEXT,
              fill_sz TEXT,
              fill_notional TEXT,

              fee TEXT,
              fee_ccy TEXT,

              source TEXT NOT NULL,
              raw_json TEXT NOT NULL,

              created_ts_ms INTEGER NOT NULL,

              PRIMARY KEY(inst_id, trade_id)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fills_clid ON fills(cl_ord_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fills_oid ON fills(ord_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fills_ts ON fills(ts_ms)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_state (
              k TEXT PRIMARY KEY,
              v TEXT NOT NULL,
              updated_ts_ms INTEGER NOT NULL
            )
            """
        )

        # Reconcile processed marker (S1): allows safe replays/backfills without cursor bugs.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fill_processed (
              inst_id TEXT NOT NULL,
              trade_id TEXT NOT NULL,
              processed_ts_ms INTEGER NOT NULL,
              PRIMARY KEY(inst_id, trade_id)
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

    def upsert_many(self, rows: Iterable[FillRow]) -> Tuple[int, int]:
        """Upsert fills; returns (inserted, total_processed)."""
        rows_list = list(rows)
        if not rows_list:
            return 0, 0

        now = _now_ms()
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()

        inserted = 0
        total = 0
        for r in rows_list:
            total += 1
            cur.execute(
                """
                INSERT OR IGNORE INTO fills(
                  inst_id, trade_id, ts_ms,
                  ord_id, cl_ord_id, side, exec_type,
                  fill_px, fill_sz, fill_notional,
                  fee, fee_ccy,
                  source, raw_json,
                  created_ts_ms
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(r.inst_id),
                    str(r.trade_id),
                    int(r.ts_ms),
                    str(r.ord_id) if r.ord_id is not None else None,
                    str(r.cl_ord_id) if r.cl_ord_id is not None else None,
                    str(r.side) if r.side is not None else None,
                    str(r.exec_type) if r.exec_type is not None else None,
                    str(r.fill_px) if r.fill_px is not None else None,
                    str(r.fill_sz) if r.fill_sz is not None else None,
                    str(r.fill_notional) if r.fill_notional is not None else None,
                    str(r.fee) if r.fee is not None else None,
                    str(r.fee_ccy) if r.fee_ccy is not None else None,
                    str(r.source),
                    str(r.raw_json or "{}"),
                    int(now),
                ),
            )
            inserted += int(cur.rowcount)

        con.commit()
        con.close()
        return inserted, total

    def count(self) -> int:
        """Count"""
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM fills")
        n = int(cur.fetchone()[0] or 0)
        con.close()
        return n

    def mark_processed(self, inst_id: str, trade_id: str) -> None:
        """Mark processed"""
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO fill_processed(inst_id, trade_id, processed_ts_ms) VALUES (?,?,?)",
            (str(inst_id), str(trade_id), _now_ms()),
        )
        con.commit()
        con.close()

    def list_unprocessed(self, limit: int = 2000) -> List[Dict[str, Any]]:
        """Return unprocessed fill records (joined with fill_processed)."""
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            """
            SELECT f.inst_id, f.trade_id, f.ts_ms, f.ord_id, f.cl_ord_id, f.side, f.exec_type,
                   f.fill_px, f.fill_sz, f.fee, f.fee_ccy, f.raw_json
            FROM fills f
            LEFT JOIN fill_processed p
              ON p.inst_id = f.inst_id AND p.trade_id = f.trade_id
            WHERE p.trade_id IS NULL
            ORDER BY f.ts_ms ASC
            LIMIT ?
            """,
            (int(limit),),
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
                }
            )
        return out

    def list_for_order(
        self,
        *,
        inst_id: str,
        cl_ord_id: Optional[str] = None,
        ord_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return all stored fills associated with a specific order."""
        inst_id_u = str(inst_id or "")
        clid = str(cl_ord_id or "")
        oid = str(ord_id or "")
        if not inst_id_u or (not clid and not oid):
            return []

        where = ["inst_id=?"]
        params: List[Any] = [inst_id_u]
        if clid and oid:
            where.append("(cl_ord_id=? OR ord_id=?)")
            params.extend([clid, oid])
        elif clid:
            where.append("cl_ord_id=?")
            params.append(clid)
        else:
            where.append("ord_id=?")
            params.append(oid)

        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            f"""
            SELECT inst_id, trade_id, ts_ms, ord_id, cl_ord_id, side, exec_type,
                   fill_px, fill_sz, fee, fee_ccy, raw_json
            FROM fills
            WHERE {' AND '.join(where)}
            ORDER BY ts_ms ASC
            """,
            tuple(params),
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
                }
            )
        return out

    def list_recent(self, limit: int = 50) -> List[FillRow]:
        """List recent"""
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            """
            SELECT inst_id, trade_id, ts_ms, ord_id, cl_ord_id, side, exec_type,
                   fill_px, fill_sz, fill_notional, fee, fee_ccy, source, raw_json
            FROM fills ORDER BY ts_ms DESC LIMIT ?
            """,
            (int(limit),),
        )
        rows = cur.fetchall()
        con.close()
        out: List[FillRow] = []
        for r in rows:
            out.append(
                FillRow(
                    inst_id=r[0],
                    trade_id=r[1],
                    ts_ms=int(r[2]),
                    ord_id=r[3],
                    cl_ord_id=r[4],
                    side=r[5],
                    exec_type=r[6],
                    fill_px=r[7],
                    fill_sz=r[8],
                    fill_notional=r[9],
                    fee=r[10],
                    fee_ccy=r[11],
                    source=r[12],
                    raw_json=r[13],
                )
            )
        return out


def parse_okx_fills(resp_data: Dict[str, Any], *, source: str = "fills") -> List[FillRow]:
    """Parse okx fills"""
    rows = []
    data = (resp_data or {}).get("data")
    if not isinstance(data, list):
        return []

    for it in data:
        if not isinstance(it, dict):
            continue
        inst_id = str(it.get("instId") or "")
        trade_id = str(it.get("tradeId") or it.get("trade_id") or "")
        if not inst_id or not trade_id:
            continue
        ts = it.get("ts") or it.get("fillTime") or it.get("fill_time")
        try:
            ts_ms = int(ts)
        except Exception:
            ts_ms = 0

        rows.append(
            FillRow(
                inst_id=inst_id,
                trade_id=trade_id,
                ts_ms=int(ts_ms),
                ord_id=str(it.get("ordId")) if it.get("ordId") is not None else None,
                cl_ord_id=str(it.get("clOrdId")) if it.get("clOrdId") is not None else None,
                side=str(it.get("side")) if it.get("side") is not None else None,
                exec_type=str(it.get("execType")) if it.get("execType") is not None else None,
                fill_px=str(it.get("fillPx")) if it.get("fillPx") is not None else None,
                fill_sz=str(it.get("fillSz")) if it.get("fillSz") is not None else None,
                fill_notional=str(it.get("fillNotionalUsd")) if it.get("fillNotionalUsd") is not None else (str(it.get("fillNotional")) if it.get("fillNotional") is not None else None),
                fee=str(it.get("fee")) if it.get("fee") is not None else None,
                fee_ccy=str(it.get("feeCcy")) if it.get("feeCcy") is not None else None,
                source=str(source),
                raw_json=json.dumps(it, ensure_ascii=False, separators=(",", ":")),
            )
        )

    return rows
