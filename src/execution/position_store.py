from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from src.execution.fill_store import derive_runtime_named_json_path
from src.utils.time import utc_now_iso

log = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = (PROJECT_ROOT / resolved).resolve()
    return resolved


@dataclass
class Position:
    """持仓数据类
    
    Attributes:
        symbol: 交易对符号
        qty: 持仓数量
        avg_px: 平均持仓价格
        entry_ts: 入场时间戳
        highest_px: 最高价
        last_update_ts: 最后更新时间
        last_mark_px: 最后标记价格
        unrealized_pnl_pct: 未实现盈亏百分比
        tags_json: 标签JSON字符串
    """
    symbol: str
    qty: float
    avg_px: float
    entry_ts: str
    highest_px: float
    last_update_ts: str
    last_mark_px: float
    unrealized_pnl_pct: float
    tags_json: str = "{}"


class PositionStore:
    """SQLite-backed position store.

    Spot-only, long-only semantics:
      - qty > 0 means holding base asset of symbol (e.g., BTC for BTC/USDT)
      - CLOSE_LONG means reduce qty to 0

    This store is designed to survive restarts.
    """

    def __init__(self, path: str = "reports/positions.sqlite"):
        self.path = _resolve_path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _highest_tracker_state_path(self) -> Path:
        if self.path.name == "positions.sqlite":
            return self.path.with_name("highest_px_state.json")
        if "positions" in self.path.stem:
            return self.path.with_name(
                self.path.name.replace("positions", "highest_px_state", 1)
            ).with_suffix(".json")
        return self.path.with_name("highest_px_state.json")

    def _order_store_path(self) -> Path:
        if self.path.name == "positions.sqlite":
            return self.path.with_name("orders.sqlite")
        if "positions" in self.path.stem:
            return self.path.with_name(self.path.name.replace("positions", "orders", 1))
        return self.path.with_name("orders.sqlite")

    def _runtime_risk_state_paths(self) -> List[Path]:
        order_store_path = self._order_store_path()
        return [
            derive_runtime_named_json_path(order_store_path, "stop_loss_state"),
            derive_runtime_named_json_path(order_store_path, "fixed_stop_loss_state"),
            derive_runtime_named_json_path(order_store_path, "profit_taking_state"),
            self._highest_tracker_state_path(),
        ]

    @staticmethod
    def _remove_symbol_from_state_file(path: Path, symbol: str) -> None:
        try:
            if not path.exists():
                return
            obj = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(obj, dict) or symbol not in obj:
                return
            del obj[symbol]
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(path)
        except Exception:
            pass

    def prune_orphan_risk_state(self) -> Dict[str, int]:
        held_symbols = {str(p.symbol) for p in self.list() if float(getattr(p, "qty", 0.0) or 0.0) > 0}
        removed_counts: Dict[str, int] = {}
        for state_path in self._runtime_risk_state_paths():
            try:
                if not state_path.exists():
                    continue
                obj = json.loads(state_path.read_text(encoding="utf-8"))
                if not isinstance(obj, dict):
                    continue
                stale_symbols = [sym for sym in list(obj.keys()) if str(sym) not in held_symbols]
                if not stale_symbols:
                    continue
                for sym in stale_symbols:
                    obj.pop(sym, None)
                tmp = state_path.with_suffix(state_path.suffix + ".tmp")
                tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
                tmp.replace(state_path)
                removed_counts[state_path.name] = len(stale_symbols)
            except Exception as e:
                log.warning("Failed to prune orphan risk state in %s: %s", state_path, e)
        return removed_counts

    def _init_db(self) -> None:
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS positions (
              symbol TEXT PRIMARY KEY,
              qty REAL NOT NULL,
              avg_px REAL NOT NULL,
              entry_ts TEXT NOT NULL,
              highest_px REAL NOT NULL,
              last_update_ts TEXT NOT NULL DEFAULT '',
              last_mark_px REAL NOT NULL DEFAULT 0,
              unrealized_pnl_pct REAL NOT NULL DEFAULT 0,
              tags_json TEXT NOT NULL
            )
            """
        )
        con.commit()
        con.close()
        self._migrate_add_columns()

    def _migrate_add_columns(self) -> None:
        """Add new columns to existing DBs (safe best-effort)."""
        try:
            con = sqlite3.connect(str(self.path))
            cur = con.cursor()
            cur.execute("PRAGMA table_info(positions)")
            cols = {str(r[1]) for r in cur.fetchall()}
            adds = []
            if "last_update_ts" not in cols:
                adds.append("ALTER TABLE positions ADD COLUMN last_update_ts TEXT NOT NULL DEFAULT ''")
            if "last_mark_px" not in cols:
                adds.append("ALTER TABLE positions ADD COLUMN last_mark_px REAL NOT NULL DEFAULT 0")
            if "unrealized_pnl_pct" not in cols:
                adds.append("ALTER TABLE positions ADD COLUMN unrealized_pnl_pct REAL NOT NULL DEFAULT 0")
            for sql in adds:
                cur.execute(sql)
            con.commit()
            con.close()
        except Exception as e:
            log.warning("Failed to migrate position columns: %s", e)

    def list(self) -> List[Position]:
        """获取有效持仓列表（仅 qty>0）

        Returns:
            持仓列表
        """
        try:
            con = sqlite3.connect(str(self.path))
            cur = con.cursor()
            cur.execute(
                "SELECT symbol, qty, avg_px, entry_ts, highest_px, last_update_ts, last_mark_px, unrealized_pnl_pct, tags_json FROM positions WHERE qty > 0"
            )
            rows = cur.fetchall()
            con.close()
            return [Position(*r) for r in rows]
        except Exception as e:
            log.exception("Failed to list positions: %s", e)
            return []

    def get(self, symbol: str) -> Optional[Position]:
        """获取指定symbol的持仓

        Args:
            symbol: 交易对符号

        Returns:
            持仓对象，如果不存在返回None
        """
        try:
            con = sqlite3.connect(str(self.path))
            cur = con.cursor()
            cur.execute(
                "SELECT symbol, qty, avg_px, entry_ts, highest_px, last_update_ts, last_mark_px, unrealized_pnl_pct, tags_json FROM positions WHERE symbol=?",
                (symbol,),
            )
            row = cur.fetchone()
            con.close()
            return Position(*row) if row else None
        except Exception as e:
            log.exception("Failed to get position for %s: %s", symbol, e)
            return None

    def upsert_buy(self, symbol: str, qty: float, px: float, now_ts: Optional[str] = None) -> Position:
        """买入时更新或创建持仓

        Args:
            symbol: 交易对符号
            qty: 买入数量
            px: 买入价格
            now_ts: 当前时间戳(可选)

        Returns:
            更新后的持仓对象
        """
        qty = float(qty)
        px = float(px)
        now = now_ts or utc_now_iso()

        cur_pos = self.get(symbol)

        # If existing position is only dust (very small notional), treat as flat.
        # Treat tiny residual local positions as flat to reduce dust-churn re-entry noise.
        dust_reset_notional_usdt = 0.5
        if cur_pos and float(cur_pos.qty) > 0 and float(cur_pos.qty) * float(px) < dust_reset_notional_usdt:
            cur_pos = None

        # Import here to avoid circular import
        try:
            from src.execution.highest_px_tracker import get_highest_price_tracker
            tracker = get_highest_price_tracker(self._highest_tracker_state_path())
        except Exception:
            tracker = None

        if not cur_pos or cur_pos.qty <= 0:
            # New position: highest 必须从入场价重置，禁止沿用历史峰值
            highest = px

            pos = Position(
                symbol=symbol,
                qty=qty,
                avg_px=px,
                entry_ts=now,
                highest_px=highest,
                last_update_ts=now,
                last_mark_px=px,
                unrealized_pnl_pct=0.0,
                tags_json="{}",
            )
            # Update tracker with new position (force reset)
            if tracker:
                tracker.update(symbol, highest, px, source="new_position")
        else:
            new_qty = cur_pos.qty + qty
            avg = (cur_pos.avg_px * cur_pos.qty + px * qty) / new_qty if new_qty else px
            
            # Merge with tracker for highest_px
            hi = max(cur_pos.highest_px, px)
            if tracker:
                tracked_high = tracker.get_highest_px(symbol, hi)
                hi = max(hi, tracked_high)
                tracker.update(symbol, hi, avg, source="add_position")
            
            pos = Position(
                symbol=symbol,
                qty=new_qty,
                avg_px=avg,
                entry_ts=cur_pos.entry_ts,
                highest_px=hi,
                last_update_ts=now,
                last_mark_px=px,
                unrealized_pnl_pct=float(cur_pos.unrealized_pnl_pct),
                tags_json=cur_pos.tags_json,
            )

        self.upsert_position(pos)
        return pos

    def mark_position(
        self,
        symbol: str,
        now_ts: str,
        mark_px: float,
        high_px: Optional[float] = None,
    ) -> None:
        """Mark-to-market a position.

        - update last_update_ts
        - update last_mark_px
        - update unrealized_pnl_pct
        - update highest_px = max(existing, high_px or mark_px)
        - sync with HighestPriceTracker
        """
        p = self.get(symbol)
        if not p:
            return
        mp = float(mark_px)
        hp = float(high_px) if high_px is not None else mp
        hi = max(float(p.highest_px), hp)
        
        # Sync with tracker
        try:
            from src.execution.highest_px_tracker import get_highest_price_tracker
            tracker = get_highest_price_tracker(self._highest_tracker_state_path())
            # Check if tracker has higher value
            tracked_high = tracker.get_highest_px(symbol, hi)
            if tracked_high > hi:
                hi = tracked_high
            else:
                # Update tracker with new high
                tracker.update(symbol, hi, p.avg_px, source="mark_to_market")
        except Exception:
            pass
        
        pnl = (mp - float(p.avg_px)) / float(p.avg_px) if float(p.avg_px) > 0 else 0.0
        self.upsert_position(
            Position(
                symbol=p.symbol,
                qty=float(p.qty),
                avg_px=float(p.avg_px),
                entry_ts=str(p.entry_ts),
                highest_px=float(hi),
                last_update_ts=str(now_ts),
                last_mark_px=float(mp),
                unrealized_pnl_pct=float(pnl),
                tags_json=str(p.tags_json),
            )
        )

    def update_highest(self, symbol: str, highest_px: float) -> None:
        """更新持仓最高价

        Args:
            symbol: 交易对符号
            highest_px: 新的最高价
        """
        p = self.get(symbol)
        if not p:
            return
        self.mark_position(symbol=symbol, now_ts=p.last_update_ts or p.entry_ts, mark_px=p.last_mark_px or p.avg_px, high_px=highest_px)

    def upsert_position(self, pos: Position) -> None:
        """Insert/update a full position row (used for migrations/tests).

        Args:
            pos: 持仓对象
        """
        try:
            con = sqlite3.connect(str(self.path))
            c = con.cursor()
            c.execute(
                "INSERT INTO positions(symbol, qty, avg_px, entry_ts, highest_px, last_update_ts, last_mark_px, unrealized_pnl_pct, tags_json) "
                "VALUES (?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(symbol) DO UPDATE SET qty=excluded.qty, avg_px=excluded.avg_px, entry_ts=excluded.entry_ts, highest_px=excluded.highest_px, "
                "last_update_ts=excluded.last_update_ts, last_mark_px=excluded.last_mark_px, unrealized_pnl_pct=excluded.unrealized_pnl_pct, tags_json=excluded.tags_json",
                (
                    pos.symbol,
                    float(pos.qty),
                    float(pos.avg_px),
                    str(pos.entry_ts),
                    float(pos.highest_px),
                    str(pos.last_update_ts),
                    float(pos.last_mark_px),
                    float(pos.unrealized_pnl_pct),
                    str(pos.tags_json),
                ),
            )
            con.commit()
            con.close()
        except Exception as e:
            log.exception("Failed to upsert position: %s", e)
            raise

    def set_qty(self, symbol: str, *, qty: float, now_ts: Optional[str] = None) -> None:
        """Update qty only (avg_px unchanged).

        Args:
            symbol: 交易对符号
            qty: 新的数量
            now_ts: 当前时间戳(可选)
        """
        p = self.get(symbol)
        if not p:
            return

        q = float(qty)
        # 根治：qty<=0 直接视为平仓，避免残留0仓位触发幽灵卖单
        if q <= 0:
            self.close_long(symbol)
            return

        now = now_ts or utc_now_iso()
        self.upsert_position(
            Position(
                symbol=p.symbol,
                qty=q,
                avg_px=float(p.avg_px),
                entry_ts=str(p.entry_ts),
                highest_px=float(p.highest_px),
                last_update_ts=str(now),
                last_mark_px=float(p.last_mark_px or p.avg_px),
                unrealized_pnl_pct=float(p.unrealized_pnl_pct),
                tags_json=str(p.tags_json),
            )
        )

    def close_long(self, symbol: str) -> bool:
        """关闭多头持仓

        Args:
            symbol: 交易对符号

        Returns:
            bool: True表示成功关闭，False表示持仓不存在
        """
        try:
            con = sqlite3.connect(str(self.path))
            c = con.cursor()
            
            # 先检查持仓是否存在
            c.execute("SELECT symbol FROM positions WHERE symbol=?", (symbol,))
            if not c.fetchone():
                con.close()
                log.warning("Attempted to close non-existent position: %s", symbol)
                return False
            
            c.execute("DELETE FROM positions WHERE symbol=?", (symbol,))
            con.commit()
            con.close()

            for state_path in self._runtime_risk_state_paths():
                self._remove_symbol_from_state_file(state_path, symbol)

            log.info("Position closed: %s", symbol)
            return True
        except Exception as e:
            log.exception("Failed to close position for %s: %s", symbol, e)
            raise
