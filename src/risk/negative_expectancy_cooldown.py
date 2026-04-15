
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class NegativeExpectancyConfig:
    enabled: bool = False
    lookback_hours: int = 24
    min_closed_cycles: int = 4
    expectancy_threshold_usdt: float = 0.0
    cooldown_hours: int = 24
    state_path: str = "reports/negative_expectancy_cooldown.json"
    orders_db_path: str = "reports/orders.sqlite"
    fast_fail_max_hold_minutes: int = 120


class NegativeExpectancyCooldown:
    """基于最近成交闭环的负期望标的冷却器（FIFO 近似）。"""

    def __init__(self, cfg: NegativeExpectancyConfig):
        self.cfg = cfg
        self.cfg.state_path = str(self._resolve_path(self.cfg.state_path))
        self.cfg.orders_db_path = str(self._resolve_path(self.cfg.orders_db_path))
        self._last_refresh_ms = 0
        self._cache: Dict[str, Any] = self._load_state()

    @staticmethod
    def _resolve_path(path: str | Path) -> Path:
        resolved = Path(path)
        if not resolved.is_absolute():
            resolved = (PROJECT_ROOT / resolved).resolve()
        return resolved

    @staticmethod
    def _norm_symbol(inst_id: str) -> str:
        s = str(inst_id or "")
        return s.replace("-", "/") if "-" in s else s

    def _load_state(self) -> Dict[str, Any]:
        p = Path(self.cfg.state_path)
        if not p.exists():
            return {"symbols": {}, "updated_ts_ms": 0}
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(obj, dict):
                if "symbols" not in obj:
                    obj["symbols"] = {}
                return obj
        except Exception:
            pass
        return {"symbols": {}, "updated_ts_ms": 0}

    def _save_state(self) -> None:
        p = Path(self.cfg.state_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(self._cache, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(p)

    def _scan_expectancy(self) -> Dict[str, Dict[str, float]]:
        """从 orders.sqlite 计算每个标的最近闭环平均PnL（FIFO近似）。"""
        p = Path(self.cfg.orders_db_path)
        if not p.exists():
            return {}

        lookback_ms = int(self.cfg.lookback_hours) * 3600 * 1000
        since_ms = int(time.time() * 1000) - max(0, lookback_ms)

        by_symbol: Dict[str, Dict[str, float]] = {}
        conn = None
        try:
            conn = sqlite3.connect(str(p))
            conn.row_factory = sqlite3.Row
            col_rows = conn.execute("PRAGMA table_info(orders)").fetchall()
            cols = {str(r["name"]) for r in col_rows}
            if "updated_ts" in cols and "created_ts" in cols:
                event_ts_expr = "COALESCE(NULLIF(updated_ts, 0), created_ts)"
            elif "created_ts" in cols:
                event_ts_expr = "created_ts"
            elif "updated_ts" in cols:
                event_ts_expr = "updated_ts"
            else:
                return {}
            sql = (
                f"SELECT inst_id, side, intent, state, acc_fill_sz, avg_px, {event_ts_expr} AS event_ts "
                "FROM orders "
                f"WHERE state='FILLED' AND {event_ts_expr} >= ? "
                "AND acc_fill_sz IS NOT NULL AND avg_px IS NOT NULL "
                f"ORDER BY inst_id, {event_ts_expr} ASC"
            )
            rows = conn.execute(sql, (since_ms,)).fetchall()
        except Exception:
            return {}
        finally:
            try:
                conn.close()
            except Exception:
                pass

        # FIFO inventory per symbol, preserving entry timestamps so we can detect
        # "fast-fail" round-trips that reverse within a short holding window.
        inv_lots: Dict[str, list[Dict[str, float]]] = {}
        fast_fail_hold_ms = max(0, int(self.cfg.fast_fail_max_hold_minutes)) * 60 * 1000

        for r in rows:
            inst_id = str(r["inst_id"])
            sym = self._norm_symbol(inst_id)
            side = str(r["side"] or "").lower()
            try:
                qty = float(r["acc_fill_sz"] or 0.0)
                px = float(r["avg_px"] or 0.0)
                event_ts = int(r["event_ts"] or 0)
            except Exception:
                continue
            if qty <= 0 or px <= 0 or event_ts <= 0:
                continue

            inv_lots.setdefault(sym, [])
            by_symbol.setdefault(
                sym,
                {
                    "closed_cycles": 0.0,
                    "pnl_sum_usdt": 0.0,
                    "closed_notional_usdt": 0.0,
                    "fast_fail_closed_cycles": 0.0,
                    "fast_fail_pnl_sum_usdt": 0.0,
                    "fast_fail_closed_notional_usdt": 0.0,
                    "fast_fail_hold_minutes_sum": 0.0,
                },
            )

            if side == "buy":
                inv_lots[sym].append(
                    {
                        "qty": qty,
                        "px": px,
                        "ts": float(event_ts),
                    }
                )
            elif side == "sell":
                remaining = qty
                while remaining > 1e-12 and inv_lots[sym]:
                    lot = inv_lots[sym][0]
                    lot_qty = float(lot.get("qty") or 0.0)
                    if lot_qty <= 1e-12:
                        inv_lots[sym].pop(0)
                        continue
                    close_qty = min(lot_qty, remaining)
                    avg_cost = float(lot.get("px") or px)
                    pnl = (px - avg_cost) * close_qty
                    by_symbol[sym]["closed_cycles"] += 1.0
                    by_symbol[sym]["pnl_sum_usdt"] += float(pnl)
                    by_symbol[sym]["closed_notional_usdt"] += float(avg_cost * close_qty)

                    hold_ms = max(0.0, float(event_ts) - float(lot.get("ts") or event_ts))
                    if fast_fail_hold_ms > 0 and hold_ms <= fast_fail_hold_ms:
                        by_symbol[sym]["fast_fail_closed_cycles"] += 1.0
                        by_symbol[sym]["fast_fail_pnl_sum_usdt"] += float(pnl)
                        by_symbol[sym]["fast_fail_closed_notional_usdt"] += float(avg_cost * close_qty)
                        by_symbol[sym]["fast_fail_hold_minutes_sum"] += float(hold_ms / 60000.0)

                    remaining = max(0.0, remaining - close_qty)
                    left_qty = max(0.0, lot_qty - close_qty)
                    if left_qty <= 1e-12:
                        inv_lots[sym].pop(0)
                    else:
                        lot["qty"] = left_qty

        # expectancy
        out = {}
        for sym, st in by_symbol.items():
            n = int(st.get("closed_cycles") or 0)
            pnl_sum = float(st.get("pnl_sum_usdt") or 0.0)
            closed_notional = float(st.get("closed_notional_usdt") or 0.0)
            exp = pnl_sum / n if n > 0 else 0.0
            exp_bps = (pnl_sum / closed_notional * 10000.0) if closed_notional > 1e-12 else 0.0
            ff_n = int(st.get("fast_fail_closed_cycles") or 0)
            ff_pnl_sum = float(st.get("fast_fail_pnl_sum_usdt") or 0.0)
            ff_closed_notional = float(st.get("fast_fail_closed_notional_usdt") or 0.0)
            ff_exp = ff_pnl_sum / ff_n if ff_n > 0 else 0.0
            ff_exp_bps = (ff_pnl_sum / ff_closed_notional * 10000.0) if ff_closed_notional > 1e-12 else 0.0
            ff_hold_minutes_avg = (
                float(st.get("fast_fail_hold_minutes_sum") or 0.0) / ff_n if ff_n > 0 else 0.0
            )
            out[sym] = {
                "closed_cycles": n,
                "pnl_sum_usdt": pnl_sum,
                "closed_notional_usdt": closed_notional,
                "expectancy_usdt": exp,
                "expectancy_bps": exp_bps,
                "fast_fail_closed_cycles": ff_n,
                "fast_fail_pnl_sum_usdt": ff_pnl_sum,
                "fast_fail_closed_notional_usdt": ff_closed_notional,
                "fast_fail_expectancy_usdt": ff_exp,
                "fast_fail_expectancy_bps": ff_exp_bps,
                "fast_fail_avg_hold_minutes": ff_hold_minutes_avg,
            }
        return out

    def refresh(self, force: bool = False) -> Dict[str, Any]:
        if not self.cfg.enabled:
            return self._cache

        now_ms = int(time.time() * 1000)
        # 至多每15分钟刷新一次，避免频繁扫库
        if (not force) and (now_ms - int(self._last_refresh_ms) < 15 * 60 * 1000):
            return self._cache

        stats = self._scan_expectancy()
        symbols = self._cache.get("symbols") or {}

        min_cycles = int(self.cfg.min_closed_cycles)
        exp_th = float(self.cfg.expectancy_threshold_usdt)
        cd_ms = int(self.cfg.cooldown_hours) * 3600 * 1000

        # 清理过期
        for sym in list(symbols.keys()):
            until_ms = int((symbols.get(sym) or {}).get("cooldown_until_ms") or 0)
            if until_ms > 0 and now_ms >= until_ms:
                symbols.pop(sym, None)

        stats_cache: Dict[str, Dict[str, Any]] = {}
        for sym, st in stats.items():
            n = int(st.get("closed_cycles") or 0)
            exp = float(st.get("expectancy_usdt") or 0.0)
            stats_cache[sym] = {
                "closed_cycles": n,
                "pnl_sum_usdt": float(st.get("pnl_sum_usdt") or 0.0),
                "closed_notional_usdt": float(st.get("closed_notional_usdt") or 0.0),
                "expectancy_usdt": exp,
                "expectancy_bps": float(st.get("expectancy_bps") or 0.0),
                "fast_fail_closed_cycles": int(st.get("fast_fail_closed_cycles") or 0),
                "fast_fail_pnl_sum_usdt": float(st.get("fast_fail_pnl_sum_usdt") or 0.0),
                "fast_fail_closed_notional_usdt": float(st.get("fast_fail_closed_notional_usdt") or 0.0),
                "fast_fail_expectancy_usdt": float(st.get("fast_fail_expectancy_usdt") or 0.0),
                "fast_fail_expectancy_bps": float(st.get("fast_fail_expectancy_bps") or 0.0),
                "fast_fail_avg_hold_minutes": float(st.get("fast_fail_avg_hold_minutes") or 0.0),
                "updated_ts_ms": now_ms,
            }
            if n >= min_cycles and exp < exp_th:
                symbols[sym] = {
                    "cooldown_until_ms": now_ms + cd_ms,
                    "expectancy_usdt": exp,
                    "expectancy_bps": float(st.get("expectancy_bps") or 0.0),
                    "closed_cycles": n,
                    "pnl_sum_usdt": float(st.get("pnl_sum_usdt") or 0.0),
                    "closed_notional_usdt": float(st.get("closed_notional_usdt") or 0.0),
                    "updated_ts_ms": now_ms,
                }

        self._cache = {
            "updated_ts_ms": now_ms,
            "lookback_hours": int(self.cfg.lookback_hours),
            "min_closed_cycles": min_cycles,
            "expectancy_threshold_usdt": exp_th,
            "cooldown_hours": int(self.cfg.cooldown_hours),
            "symbols": symbols,
            "stats": stats_cache,
        }
        self._last_refresh_ms = now_ms
        self._save_state()
        return self._cache

    def is_blocked(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.cfg.enabled:
            return None
        now_ms = int(time.time() * 1000)
        st = (self._cache.get("symbols") or {}).get(self._norm_symbol(symbol))
        if not isinstance(st, dict):
            return None
        until_ms = int(st.get("cooldown_until_ms") or 0)
        if until_ms <= now_ms:
            return None
        out = dict(st)
        out["remain_seconds"] = float(max(0, until_ms - now_ms) / 1000.0)
        return out

    def get_symbol_stats(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.cfg.enabled:
            return None
        sym = self._norm_symbol(symbol)
        stats = (self._cache.get("stats") or {}).get(sym)
        if not isinstance(stats, dict):
            return None
        out = dict(stats)
        blocked = self.is_blocked(sym)
        out["cooldown_active"] = blocked is not None
        if blocked:
            out["cooldown_until_ms"] = int(blocked.get("cooldown_until_ms") or 0)
            out["remain_seconds"] = float(blocked.get("remain_seconds") or 0.0)
        return out

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        if not self.cfg.enabled:
            return {}
        stats = self._cache.get("stats") or {}
        return {str(sym): dict(st) for sym, st in stats.items() if isinstance(st, dict)}
