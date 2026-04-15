
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Optional

from src.execution.fill_store import derive_fill_store_path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class NegativeExpectancyConfig:
    enabled: bool = False
    lookback_hours: int = 24
    min_closed_cycles: int = 4
    expectancy_threshold_bps: Optional[float] = None
    expectancy_threshold_usdt: float = 0.0
    cooldown_hours: int = 24
    state_path: str = "reports/negative_expectancy_cooldown.json"
    orders_db_path: str = "reports/orders.sqlite"
    fills_db_path: str = "reports/fills.sqlite"
    prefer_net_from_fills: bool = True
    fast_fail_max_hold_minutes: int = 120


class NegativeExpectancyCooldown:
    """基于最近成交闭环的负期望标的冷却器（FIFO 近似）。"""

    def __init__(self, cfg: NegativeExpectancyConfig):
        self.cfg = cfg
        self.cfg.state_path = str(self._resolve_path(self.cfg.state_path))
        self.cfg.orders_db_path = str(self._resolve_path(self.cfg.orders_db_path))
        raw_fills_path = str(getattr(self.cfg, "fills_db_path", "") or "").strip()
        if not raw_fills_path or raw_fills_path == "reports/fills.sqlite":
            self.cfg.fills_db_path = str(derive_fill_store_path(self.cfg.orders_db_path).resolve())
        else:
            self.cfg.fills_db_path = str(self._resolve_path(raw_fills_path))
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

    @staticmethod
    def _fee_to_usdt_cost(*, fee: Any, fee_ccy: Any, inst_id: str, fill_px: Any) -> float:
        try:
            fee_val = float(fee or 0.0)
        except Exception:
            return 0.0
        fee_ccy_norm = str(fee_ccy or "").strip().upper()
        if not fee_ccy_norm or abs(fee_val) <= 1e-12:
            return 0.0

        base_ccy = str(inst_id).split("-")[0].upper() if "-" in str(inst_id) else ""
        quote_ccy = str(inst_id).split("-")[1].upper() if "-" in str(inst_id) else ""
        if fee_ccy_norm == quote_ccy:
            signed_fee_usdt = fee_val
        elif fee_ccy_norm == base_ccy:
            try:
                signed_fee_usdt = fee_val * float(fill_px or 0.0)
            except Exception:
                return 0.0
        else:
            return 0.0

        return float(0.0 - signed_fee_usdt)

    @staticmethod
    def _orders_fee_to_usdt_best_effort(*, fee: Any) -> float:
        try:
            fee_val = float(fee or 0.0)
        except Exception:
            return 0.0
        return float(0.0 - fee_val)

    @staticmethod
    def _build_expectancy_row(
        *,
        gross_pnl_sum_usdt: float,
        net_pnl_sum_usdt: float,
        closed_cycles: float,
        closed_notional_usdt: float,
        fast_fail_gross_pnl_sum_usdt: float,
        fast_fail_net_pnl_sum_usdt: float,
        fast_fail_closed_cycles: float,
        fast_fail_closed_notional_usdt: float,
        fast_fail_hold_minutes_sum: float,
        source: str,
        degraded_fee_model: bool = False,
        degraded_reason: str = "",
    ) -> Dict[str, Any]:
        n = int(closed_cycles or 0)
        ff_n = int(fast_fail_closed_cycles or 0)
        gross_expectancy_usdt = float(gross_pnl_sum_usdt) / n if n > 0 else 0.0
        net_expectancy_usdt = float(net_pnl_sum_usdt) / n if n > 0 else 0.0
        gross_expectancy_bps = (
            float(gross_pnl_sum_usdt) / float(closed_notional_usdt) * 10000.0
            if float(closed_notional_usdt) > 1e-12
            else 0.0
        )
        net_expectancy_bps = (
            float(net_pnl_sum_usdt) / float(closed_notional_usdt) * 10000.0
            if float(closed_notional_usdt) > 1e-12
            else 0.0
        )
        fast_fail_gross_expectancy_usdt = float(fast_fail_gross_pnl_sum_usdt) / ff_n if ff_n > 0 else 0.0
        fast_fail_net_expectancy_usdt = float(fast_fail_net_pnl_sum_usdt) / ff_n if ff_n > 0 else 0.0
        fast_fail_gross_expectancy_bps = (
            float(fast_fail_gross_pnl_sum_usdt) / float(fast_fail_closed_notional_usdt) * 10000.0
            if float(fast_fail_closed_notional_usdt) > 1e-12
            else 0.0
        )
        fast_fail_net_expectancy_bps = (
            float(fast_fail_net_pnl_sum_usdt) / float(fast_fail_closed_notional_usdt) * 10000.0
            if float(fast_fail_closed_notional_usdt) > 1e-12
            else 0.0
        )
        ff_hold_minutes_avg = float(fast_fail_hold_minutes_sum) / ff_n if ff_n > 0 else 0.0
        return {
            "source": source,
            "degraded_fee_model": bool(degraded_fee_model),
            "degraded_reason": str(degraded_reason or ""),
            "closed_cycles": n,
            "closed_notional_usdt": float(closed_notional_usdt),
            "gross_pnl_sum_usdt": float(gross_pnl_sum_usdt),
            "net_pnl_sum_usdt": float(net_pnl_sum_usdt),
            "gross_expectancy_usdt": float(gross_expectancy_usdt),
            "net_expectancy_usdt": float(net_expectancy_usdt),
            "gross_expectancy_bps": float(gross_expectancy_bps),
            "net_expectancy_bps": float(net_expectancy_bps),
            # legacy compatibility
            "pnl_sum_usdt": float(gross_pnl_sum_usdt),
            "expectancy_usdt": float(gross_expectancy_usdt),
            "expectancy_bps": float(gross_expectancy_bps),
            "fast_fail_closed_cycles": ff_n,
            "fast_fail_closed_notional_usdt": float(fast_fail_closed_notional_usdt),
            "fast_fail_gross_pnl_sum_usdt": float(fast_fail_gross_pnl_sum_usdt),
            "fast_fail_net_pnl_sum_usdt": float(fast_fail_net_pnl_sum_usdt),
            "fast_fail_gross_expectancy_usdt": float(fast_fail_gross_expectancy_usdt),
            "fast_fail_net_expectancy_usdt": float(fast_fail_net_expectancy_usdt),
            "fast_fail_gross_expectancy_bps": float(fast_fail_gross_expectancy_bps),
            "fast_fail_net_expectancy_bps": float(fast_fail_net_expectancy_bps),
            # legacy compatibility
            "fast_fail_pnl_sum_usdt": float(fast_fail_gross_pnl_sum_usdt),
            "fast_fail_expectancy_usdt": float(fast_fail_gross_expectancy_usdt),
            "fast_fail_expectancy_bps": float(fast_fail_gross_expectancy_bps),
            "fast_fail_avg_hold_minutes": float(ff_hold_minutes_avg),
        }

    def _scan_expectancy_from_fills(self) -> Dict[str, Dict[str, Any]]:
        p = Path(self.cfg.fills_db_path)
        if not p.exists():
            return {}

        lookback_ms = int(self.cfg.lookback_hours) * 3600 * 1000
        since_ms = int(time.time() * 1000) - max(0, lookback_ms)

        conn = None
        try:
            conn = sqlite3.connect(str(p))
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT inst_id, side, fill_px, fill_sz, fee, fee_ccy, ts_ms
                FROM fills
                WHERE ts_ms >= ?
                  AND fill_px IS NOT NULL
                  AND fill_sz IS NOT NULL
                ORDER BY inst_id ASC, ts_ms ASC, trade_id ASC
                """,
                (since_ms,),
            ).fetchall()
        except Exception:
            return {}
        finally:
            try:
                conn.close()
            except Exception:
                pass

        inv_lots: Dict[str, list[Dict[str, float]]] = {}
        by_symbol: Dict[str, Dict[str, float]] = {}
        fast_fail_hold_ms = max(0, int(self.cfg.fast_fail_max_hold_minutes)) * 60 * 1000

        for r in rows:
            inst_id = str(r["inst_id"] or "")
            sym = self._norm_symbol(inst_id)
            side = str(r["side"] or "").lower()
            try:
                qty = float(r["fill_sz"] or 0.0)
                px = float(r["fill_px"] or 0.0)
                event_ts = int(r["ts_ms"] or 0)
            except Exception:
                continue
            if qty <= 0 or px <= 0 or event_ts <= 0 or side not in {"buy", "sell"}:
                continue

            fee_cost_usdt = self._fee_to_usdt_cost(
                fee=r["fee"],
                fee_ccy=r["fee_ccy"],
                inst_id=inst_id,
                fill_px=r["fill_px"],
            )

            inv_lots.setdefault(sym, [])
            by_symbol.setdefault(
                sym,
                {
                    "gross_pnl_sum_usdt": 0.0,
                    "net_pnl_sum_usdt": 0.0,
                    "closed_cycles": 0.0,
                    "closed_notional_usdt": 0.0,
                    "fast_fail_gross_pnl_sum_usdt": 0.0,
                    "fast_fail_net_pnl_sum_usdt": 0.0,
                    "fast_fail_closed_cycles": 0.0,
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
                        "fee_cost_usdt_remaining": float(fee_cost_usdt),
                    }
                )
                continue

            remaining = qty
            sell_fee_remaining = float(fee_cost_usdt)
            while remaining > 1e-12 and inv_lots[sym]:
                lot = inv_lots[sym][0]
                lot_qty = float(lot.get("qty") or 0.0)
                if lot_qty <= 1e-12:
                    inv_lots[sym].pop(0)
                    continue
                close_qty = min(lot_qty, remaining)
                buy_px = float(lot.get("px") or px)
                buy_notional = buy_px * close_qty
                sell_notional = px * close_qty
                buy_fee_remaining = float(lot.get("fee_cost_usdt_remaining") or 0.0)
                buy_fee_alloc = buy_fee_remaining * (close_qty / lot_qty) if lot_qty > 0 else 0.0
                sell_fee_alloc = sell_fee_remaining * (close_qty / remaining) if remaining > 0 else 0.0

                gross_pnl = sell_notional - buy_notional
                net_pnl = gross_pnl - buy_fee_alloc - sell_fee_alloc
                by_symbol[sym]["closed_cycles"] += 1.0
                by_symbol[sym]["gross_pnl_sum_usdt"] += float(gross_pnl)
                by_symbol[sym]["net_pnl_sum_usdt"] += float(net_pnl)
                by_symbol[sym]["closed_notional_usdt"] += float(buy_notional)

                hold_ms = max(0.0, float(event_ts) - float(lot.get("ts") or event_ts))
                if fast_fail_hold_ms > 0 and hold_ms <= fast_fail_hold_ms:
                    by_symbol[sym]["fast_fail_closed_cycles"] += 1.0
                    by_symbol[sym]["fast_fail_gross_pnl_sum_usdt"] += float(gross_pnl)
                    by_symbol[sym]["fast_fail_net_pnl_sum_usdt"] += float(net_pnl)
                    by_symbol[sym]["fast_fail_closed_notional_usdt"] += float(buy_notional)
                    by_symbol[sym]["fast_fail_hold_minutes_sum"] += float(hold_ms / 60000.0)

                remaining = max(0.0, remaining - close_qty)
                sell_fee_remaining = float(sell_fee_remaining - sell_fee_alloc)
                left_qty = max(0.0, lot_qty - close_qty)
                left_buy_fee = float(buy_fee_remaining - buy_fee_alloc)
                if left_qty <= 1e-12:
                    inv_lots[sym].pop(0)
                else:
                    lot["qty"] = left_qty
                    lot["fee_cost_usdt_remaining"] = left_buy_fee

        out: Dict[str, Dict[str, Any]] = {}
        for sym, st in by_symbol.items():
            out[sym] = self._build_expectancy_row(
                gross_pnl_sum_usdt=float(st.get("gross_pnl_sum_usdt") or 0.0),
                net_pnl_sum_usdt=float(st.get("net_pnl_sum_usdt") or 0.0),
                closed_cycles=float(st.get("closed_cycles") or 0.0),
                closed_notional_usdt=float(st.get("closed_notional_usdt") or 0.0),
                fast_fail_gross_pnl_sum_usdt=float(st.get("fast_fail_gross_pnl_sum_usdt") or 0.0),
                fast_fail_net_pnl_sum_usdt=float(st.get("fast_fail_net_pnl_sum_usdt") or 0.0),
                fast_fail_closed_cycles=float(st.get("fast_fail_closed_cycles") or 0.0),
                fast_fail_closed_notional_usdt=float(st.get("fast_fail_closed_notional_usdt") or 0.0),
                fast_fail_hold_minutes_sum=float(st.get("fast_fail_hold_minutes_sum") or 0.0),
                source="fills",
            )
        return out

    def _scan_expectancy_from_orders(self) -> Dict[str, Dict[str, Any]]:
        """Fallback path from orders.sqlite.

        This is a degraded approximation because orders.sqlite does not preserve fee_ccy.
        We assume orders.fee is already quote/USDT-denominated signed fee and convert it
        into a cost-oriented USDT number best-effort.
        """
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
                f"SELECT inst_id, side, state, acc_fill_sz, avg_px, fee, {event_ts_expr} AS event_ts "
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

        inv_lots: Dict[str, list[Dict[str, float]]] = {}
        fast_fail_hold_ms = max(0, int(self.cfg.fast_fail_max_hold_minutes)) * 60 * 1000

        for r in rows:
            inst_id = str(r["inst_id"] or "")
            sym = self._norm_symbol(inst_id)
            side = str(r["side"] or "").lower()
            try:
                qty = float(r["acc_fill_sz"] or 0.0)
                px = float(r["avg_px"] or 0.0)
                event_ts = int(r["event_ts"] or 0)
            except Exception:
                continue
            if qty <= 0 or px <= 0 or event_ts <= 0 or side not in {"buy", "sell"}:
                continue

            fee_cost_usdt = self._orders_fee_to_usdt_best_effort(fee=r["fee"])

            inv_lots.setdefault(sym, [])
            by_symbol.setdefault(
                sym,
                {
                    "gross_pnl_sum_usdt": 0.0,
                    "net_pnl_sum_usdt": 0.0,
                    "closed_cycles": 0.0,
                    "closed_notional_usdt": 0.0,
                    "fast_fail_gross_pnl_sum_usdt": 0.0,
                    "fast_fail_net_pnl_sum_usdt": 0.0,
                    "fast_fail_closed_cycles": 0.0,
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
                        "fee_cost_usdt_remaining": float(fee_cost_usdt),
                    }
                )
                continue

            remaining = qty
            sell_fee_remaining = float(fee_cost_usdt)
            while remaining > 1e-12 and inv_lots[sym]:
                lot = inv_lots[sym][0]
                lot_qty = float(lot.get("qty") or 0.0)
                if lot_qty <= 1e-12:
                    inv_lots[sym].pop(0)
                    continue
                close_qty = min(lot_qty, remaining)
                buy_px = float(lot.get("px") or px)
                buy_notional = buy_px * close_qty
                sell_notional = px * close_qty
                buy_fee_remaining = float(lot.get("fee_cost_usdt_remaining") or 0.0)
                buy_fee_alloc = buy_fee_remaining * (close_qty / lot_qty) if lot_qty > 0 else 0.0
                sell_fee_alloc = sell_fee_remaining * (close_qty / remaining) if remaining > 0 else 0.0

                gross_pnl = sell_notional - buy_notional
                net_pnl = gross_pnl - buy_fee_alloc - sell_fee_alloc
                by_symbol[sym]["closed_cycles"] += 1.0
                by_symbol[sym]["gross_pnl_sum_usdt"] += float(gross_pnl)
                by_symbol[sym]["net_pnl_sum_usdt"] += float(net_pnl)
                by_symbol[sym]["closed_notional_usdt"] += float(buy_notional)

                hold_ms = max(0.0, float(event_ts) - float(lot.get("ts") or event_ts))
                if fast_fail_hold_ms > 0 and hold_ms <= fast_fail_hold_ms:
                    by_symbol[sym]["fast_fail_closed_cycles"] += 1.0
                    by_symbol[sym]["fast_fail_gross_pnl_sum_usdt"] += float(gross_pnl)
                    by_symbol[sym]["fast_fail_net_pnl_sum_usdt"] += float(net_pnl)
                    by_symbol[sym]["fast_fail_closed_notional_usdt"] += float(buy_notional)
                    by_symbol[sym]["fast_fail_hold_minutes_sum"] += float(hold_ms / 60000.0)

                remaining = max(0.0, remaining - close_qty)
                sell_fee_remaining = float(sell_fee_remaining - sell_fee_alloc)
                left_qty = max(0.0, lot_qty - close_qty)
                left_buy_fee = float(buy_fee_remaining - buy_fee_alloc)
                if left_qty <= 1e-12:
                    inv_lots[sym].pop(0)
                else:
                    lot["qty"] = left_qty
                    lot["fee_cost_usdt_remaining"] = left_buy_fee

        out: Dict[str, Dict[str, Any]] = {}
        for sym, st in by_symbol.items():
            out[sym] = self._build_expectancy_row(
                gross_pnl_sum_usdt=float(st.get("gross_pnl_sum_usdt") or 0.0),
                net_pnl_sum_usdt=float(st.get("net_pnl_sum_usdt") or 0.0),
                closed_cycles=float(st.get("closed_cycles") or 0.0),
                closed_notional_usdt=float(st.get("closed_notional_usdt") or 0.0),
                fast_fail_gross_pnl_sum_usdt=float(st.get("fast_fail_gross_pnl_sum_usdt") or 0.0),
                fast_fail_net_pnl_sum_usdt=float(st.get("fast_fail_net_pnl_sum_usdt") or 0.0),
                fast_fail_closed_cycles=float(st.get("fast_fail_closed_cycles") or 0.0),
                fast_fail_closed_notional_usdt=float(st.get("fast_fail_closed_notional_usdt") or 0.0),
                fast_fail_hold_minutes_sum=float(st.get("fast_fail_hold_minutes_sum") or 0.0),
                source="orders",
                degraded_fee_model=True,
                degraded_reason="orders.sqlite fallback assumes orders.fee is quote/USDT-denominated signed fee",
            )
        return out

    def _scan_expectancy(self) -> Dict[str, Dict[str, Any]]:
        if bool(getattr(self.cfg, "prefer_net_from_fills", True)):
            fills_stats = self._scan_expectancy_from_fills()
            if fills_stats:
                return fills_stats
            return self._scan_expectancy_from_orders()
        orders_stats = self._scan_expectancy_from_orders()
        if orders_stats:
            return orders_stats
        return self._scan_expectancy_from_fills()

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
        exp_th_usdt = float(self.cfg.expectancy_threshold_usdt)
        exp_th_bps = (
            float(self.cfg.expectancy_threshold_bps)
            if self.cfg.expectancy_threshold_bps is not None
            else None
        )
        cd_ms = int(self.cfg.cooldown_hours) * 3600 * 1000

        # 清理过期
        for sym in list(symbols.keys()):
            until_ms = int((symbols.get(sym) or {}).get("cooldown_until_ms") or 0)
            if until_ms > 0 and now_ms >= until_ms:
                symbols.pop(sym, None)

        stats_cache: Dict[str, Dict[str, Any]] = {}
        for sym, st in stats.items():
            n = int(st.get("closed_cycles") or 0)
            gross_exp_usdt = float(st.get("gross_expectancy_usdt", st.get("expectancy_usdt") or 0.0))
            net_exp_usdt = float(st.get("net_expectancy_usdt", gross_exp_usdt))
            net_exp_bps = float(st.get("net_expectancy_bps", st.get("expectancy_bps") or 0.0))
            stat_row = dict(st)
            stat_row["updated_ts_ms"] = now_ms
            stats_cache[sym] = stat_row

            trigger_negative = False
            metric_used = "net_expectancy_bps" if exp_th_bps is not None else "net_expectancy_usdt"
            trigger_value = net_exp_bps if exp_th_bps is not None else net_exp_usdt
            threshold_value = float(exp_th_bps) if exp_th_bps is not None else float(exp_th_usdt)
            if n >= min_cycles and trigger_value < threshold_value:
                trigger_negative = True
            if n >= min_cycles and trigger_negative:
                symbols[sym] = {
                    "cooldown_until_ms": now_ms + cd_ms,
                    "metric_used": metric_used,
                    "threshold_value": threshold_value,
                    "gross_expectancy_usdt": gross_exp_usdt,
                    "net_expectancy_usdt": net_exp_usdt,
                    "gross_expectancy_bps": float(st.get("gross_expectancy_bps", st.get("expectancy_bps") or 0.0)),
                    "net_expectancy_bps": net_exp_bps,
                    # legacy compatibility
                    "expectancy_usdt": float(st.get("expectancy_usdt") or gross_exp_usdt),
                    "expectancy_bps": float(st.get("expectancy_bps") or st.get("gross_expectancy_bps") or 0.0),
                    "closed_cycles": n,
                    "gross_pnl_sum_usdt": float(st.get("gross_pnl_sum_usdt", st.get("pnl_sum_usdt") or 0.0)),
                    "net_pnl_sum_usdt": float(st.get("net_pnl_sum_usdt", st.get("pnl_sum_usdt") or 0.0)),
                    "pnl_sum_usdt": float(st.get("pnl_sum_usdt") or st.get("gross_pnl_sum_usdt") or 0.0),
                    "closed_notional_usdt": float(st.get("closed_notional_usdt") or 0.0),
                    "source": str(st.get("source") or "orders"),
                    "degraded_fee_model": bool(st.get("degraded_fee_model", False)),
                    "degraded_reason": str(st.get("degraded_reason") or ""),
                    "updated_ts_ms": now_ms,
                }

        self._cache = {
            "updated_ts_ms": now_ms,
            "lookback_hours": int(self.cfg.lookback_hours),
            "min_closed_cycles": min_cycles,
            "expectancy_threshold_bps": exp_th_bps,
            "expectancy_threshold_usdt": exp_th_usdt,
            "cooldown_hours": int(self.cfg.cooldown_hours),
            "prefer_net_from_fills": bool(getattr(self.cfg, "prefer_net_from_fills", True)),
            "fills_db_path": str(self.cfg.fills_db_path),
            "orders_db_path": str(self.cfg.orders_db_path),
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
