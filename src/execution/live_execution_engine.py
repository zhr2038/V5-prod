from __future__ import annotations

import json
import logging
import os
import time

import requests
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from configs.schema import ExecutionConfig
from src.core.models import ExecutionReport, Order
from src.execution.clordid import make_cl_ord_id, make_decision_hash
from src.execution.fill_store import derive_position_store_path, derive_runtime_named_json_path
from src.monitoring.api_telemetry import classify_api_status, is_rate_limited, record_api_request
from src.execution.okx_private_client import OKXPrivateClient, OKXPrivateClientError, OKXResponse
from src.execution.order_store import OrderStore
from src.execution.position_store import PositionStore
from src.execution.probe_metadata import probe_tags_from_order_meta
from src.data.okx_instruments import OKXSpotInstrumentsCache, round_down_to_lot


log = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Treat sub-threshold notional as dust (local-state hygiene, not exchange accounting).
DUST_NOTIONAL_USDT = 0.5


def symbol_to_inst_id(symbol: str) -> str:
    """将V5内部symbol转换为OKX instId格式
    
    V5 internal symbols are like "BTC/USDT"; OKX instId is "BTC-USDT".
    """
    return str(symbol).replace("/", "-")


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = (PROJECT_ROOT / resolved).resolve()
    return resolved


def _load_json(path: str) -> Optional[Dict[str, Any]]:
    """加载JSON文件"""
    try:
        p = _resolve_path(path)
        if not p.exists():
            return None
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _coalesce(value: Any, default: Any) -> Any:
    return default if value is None else value


def _order_meta_from_row(row) -> Dict[str, Any]:
    try:
        obj = json.loads(getattr(row, "req_json", "") or "{}")
        meta = obj.get("_v5_order_meta") if isinstance(obj, dict) else None
        return dict(meta or {}) if isinstance(meta, dict) else {}
    except Exception:
        return {}


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _get_spot_spec_best_effort(inst_id: str):
    try:
        return OKXSpotInstrumentsCache().get_spec(inst_id)
    except Exception as exc:
        log.warning("OKX instrument spec unavailable for %s: %s", inst_id, exc)
        return None


def _derive_highest_tracker_state_path(position_store_path: str | Path) -> str:
    path = _resolve_path(position_store_path)
    if path.name == "positions.sqlite":
        return str(path.with_name("highest_px_state.json"))
    if "positions" in path.stem:
        return str(path.with_name(path.name.replace("positions", "highest_px_state", 1)).with_suffix(".json"))
    return str(path.with_name("highest_px_state.json"))


def _resolve_runtime_json_path(cfg: ExecutionConfig, *, attr_name: str, base_name: str, legacy_default: str) -> str:
    raw_path = getattr(cfg, attr_name, None)
    if raw_path is None or str(raw_path).strip() == "" or str(raw_path).strip() == legacy_default:
        order_store_path = str(getattr(cfg, "order_store_path", "reports/orders.sqlite") or "reports/orders.sqlite")
        return str(_resolve_path(derive_runtime_named_json_path(order_store_path, base_name)))
    return str(_resolve_path(raw_path))


def load_kill_switch_enabled(path: str) -> bool:
    """检查是否启用kill switch"""
    d = _load_json(path) or {}
    if "enabled" in d:
        return _to_bool(d.get("enabled"))
    if "active" in d:
        return _to_bool(d.get("active"))
    nested = d.get("kill_switch")
    if isinstance(nested, dict):
        if "enabled" in nested:
            return _to_bool(nested.get("enabled"))
        if "active" in nested:
            return _to_bool(nested.get("active"))
        return False
    return _to_bool(nested)


def load_reconcile_ok(path: str) -> bool:
    """检查对账状态是否正常
    
    Placeholder before G1.0: assume OK unless file exists and says not ok.
    """
    d = _load_json(path)
    if d is None:
        return True
    if "ok" in d:
        return _to_bool(d.get("ok"))
    if "reconcile_ok" in d:
        return _to_bool(d.get("reconcile_ok"))
    return True


def load_ledger_ok(path: str) -> bool:
    """检查 ledger 状态是否正常。

    Keep the execution fallback gate aligned with live preflight: missing status
    is treated as OK, but an explicit falsey ledger status must hold the engine
    in SELL_ONLY mode.
    """
    d = _load_json(path)
    if d is None:
        return True
    if "ok" in d:
        return _to_bool(d.get("ok"))
    if "ledger_ok" in d:
        return _to_bool(d.get("ledger_ok"))
    return True


def _remove_symbol_from_state_file(path: str, symbol: str) -> bool:
    """Best-effort remove symbol state from a JSON dict file.

    Returns True if file was modified.
    """
    try:
        p = _resolve_path(path)
        if not p.exists():
            return False
        obj = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(obj, dict):
            return False
        if symbol not in obj:
            return False
        del obj[symbol]
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(p)
        return True
    except Exception:
        return False


def clear_risk_state_on_full_close(
    symbol: str,
    *,
    state_files: Optional[List[str]] = None,
    order_store_path: str = "reports/orders.sqlite",
    position_store_path: str = "reports/positions.sqlite",
) -> None:
    """Clear symbol state in stop/profit trackers after full close.

    This prevents stale stop-loss/profit state from contaminating a later re-entry.
    """
    files = state_files or [
        str(derive_runtime_named_json_path(order_store_path, "stop_loss_state")),
        str(derive_runtime_named_json_path(order_store_path, "fixed_stop_loss_state")),
        str(derive_runtime_named_json_path(order_store_path, "profit_taking_state")),
        _derive_highest_tracker_state_path(position_store_path),
    ]
    removed = 0
    for f in files:
        if _remove_symbol_from_state_file(f, symbol):
            removed += 1
    if removed > 0:
        log.info("RISK_STATE_CLEARED: %s removed_from=%d", symbol, removed)


def _read_rank_exit_cooldown_state(path: str = "reports/rank_exit_cooldown_state.json") -> Dict[str, Any]:
    obj = _load_json(path)
    return obj if isinstance(obj, dict) else {}


def _write_rank_exit_cooldown_state(data: Dict[str, Any], path: str = "reports/rank_exit_cooldown_state.json") -> None:
    p = _resolve_path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data or {}, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _record_rank_exit_fill(
    symbol: str,
    reason: str,
    path: str = "reports/rank_exit_cooldown_state.json",
    *,
    ts_ms: Optional[int] = None,
) -> None:
    try:
        st = _read_rank_exit_cooldown_state(path)
        event_ts_ms = int(ts_ms or _now_ms())
        prev = st.get(str(symbol)) if isinstance(st, dict) else None
        prev_ts_ms = int((prev or {}).get("last_rank_exit_ts_ms") or 0) if isinstance(prev, dict) else 0
        st[str(symbol)] = {
            "last_rank_exit_ts_ms": max(prev_ts_ms, event_ts_ms),
            "reason": str(reason or "rank_exit"),
        }
        _write_rank_exit_cooldown_state(st, path)
        log.info("RANK_EXIT_COOLDOWN_SET: %s reason=%s", symbol, reason)
    except Exception as e:
        log.warning("RANK_EXIT_COOLDOWN_SET failed for %s: %s", symbol, e)


def _rank_exit_cooldown_remaining_ms(symbol: str, cooldown_minutes: int, path: str = "reports/rank_exit_cooldown_state.json") -> int:
    try:
        if int(cooldown_minutes or 0) <= 0:
            return 0
        st = _read_rank_exit_cooldown_state(path)
        rec = st.get(str(symbol)) if isinstance(st, dict) else None
        if not isinstance(rec, dict):
            return 0
        ts_ms = int(rec.get("last_rank_exit_ts_ms") or 0)
        if ts_ms <= 0:
            return 0
        cooldown_ms = int(cooldown_minutes) * 60 * 1000
        elapsed = max(0, _now_ms() - ts_ms)
        return max(0, cooldown_ms - elapsed)
    except Exception:
        return 0


def _read_take_profit_cooldown_state(path: str = "reports/take_profit_cooldown_state.json") -> Dict[str, Any]:
    obj = _load_json(path)
    return obj if isinstance(obj, dict) else {}


def _write_take_profit_cooldown_state(data: Dict[str, Any], path: str = "reports/take_profit_cooldown_state.json") -> None:
    p = _resolve_path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data or {}, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _record_take_profit_fill(
    symbol: str,
    reason: str,
    path: str = "reports/take_profit_cooldown_state.json",
    *,
    ts_ms: Optional[int] = None,
) -> None:
    try:
        st = _read_take_profit_cooldown_state(path)
        event_ts_ms = int(ts_ms or _now_ms())
        prev = st.get(str(symbol)) if isinstance(st, dict) else None
        prev_ts_ms = int((prev or {}).get("last_take_profit_ts_ms") or 0) if isinstance(prev, dict) else 0
        st[str(symbol)] = {
            "last_take_profit_ts_ms": max(prev_ts_ms, event_ts_ms),
            "reason": str(reason or "take_profit"),
        }
        _write_take_profit_cooldown_state(st, path)
        log.info("TAKE_PROFIT_COOLDOWN_SET: %s reason=%s", symbol, reason)
    except Exception as e:
        log.warning("TAKE_PROFIT_COOLDOWN_SET failed for %s: %s", symbol, e)


def _take_profit_cooldown_remaining_ms(symbol: str, cooldown_minutes: int, path: str = "reports/take_profit_cooldown_state.json") -> int:
    try:
        if int(cooldown_minutes or 0) <= 0:
            return 0
        st = _read_take_profit_cooldown_state(path)
        rec = st.get(str(symbol)) if isinstance(st, dict) else None
        if not isinstance(rec, dict):
            return 0
        ts_ms = int(rec.get("last_take_profit_ts_ms") or 0)
        if ts_ms <= 0:
            return 0
        cooldown_ms = int(cooldown_minutes) * 60 * 1000
        elapsed = max(0, _now_ms() - ts_ms)
        return max(0, cooldown_ms - elapsed)
    except Exception:
        return 0


def submit_gate_for_live(cfg: ExecutionConfig) -> Tuple[str, bool, bool]:
    """Submit gate for live.

    Keep execution gate aligned with preflight policy:
    - kill-switch => SELL_ONLY
    - reconcile ok + ledger ok => ALLOW
    - reconcile not ok + ledger ok + allow_trade_on_small_reconcile_drift => ALLOW (forced)
    - otherwise reconcile/ledger not ok => SELL_ONLY
    """
    ks = load_kill_switch_enabled(
        _resolve_runtime_json_path(
            cfg,
            attr_name="kill_switch_path",
            base_name="kill_switch",
            legacy_default="reports/kill_switch.json",
        )
    )
    rc_ok = load_reconcile_ok(
        _resolve_runtime_json_path(
            cfg,
            attr_name="reconcile_status_path",
            base_name="reconcile_status",
            legacy_default="reports/reconcile_status.json",
        )
    )
    ledger_ok = load_ledger_ok(
        _resolve_runtime_json_path(
            cfg,
            attr_name="ledger_status_path",
            base_name="ledger_status",
            legacy_default="reports/ledger_status.json",
        )
    )

    if ks:
        return "SELL_ONLY", rc_ok, ks

    if rc_ok and ledger_ok:
        return "ALLOW", rc_ok, ks

    if ledger_ok and not rc_ok:
        force_allow = bool(getattr(cfg, "allow_trade_on_small_reconcile_drift", False))
        if force_allow:
            return "ALLOW", rc_ok, ks

    return "SELL_ONLY", rc_ok, ks


def _parse_okx_order_ack(ack_data: Any) -> Tuple[bool, Optional[str], Optional[str], Optional[str]]:
    """Return (ok, ord_id, err_code, err_msg)."""
    d = ack_data if isinstance(ack_data, dict) else {}
    code = d.get("code")
    msg = d.get("msg")
    code_s = str(code) if code is not None else None

    rows = d.get("data")
    r0 = (rows[0] if isinstance(rows, list) and rows else {}) or {}
    s_code = r0.get("sCode")
    s_msg = r0.get("sMsg")

    # OKX semantics: code==0 and sCode==0 means accepted.
    ok = True
    if code_s and code_s != "0":
        ok = False
    if s_code is not None and str(s_code) != "0":
        ok = False

    ord_id = r0.get("ordId") or r0.get("ord_id")

    if ok:
        return True, (str(ord_id) if ord_id else None), None, None

    err_code = str(s_code) if s_code is not None and str(s_code) != "0" else (str(code_s) if code_s else None)
    err_msg = str(s_msg) if s_msg else (str(msg) if msg else None)
    return False, None, err_code, err_msg


class DustOrderSkip(Exception):
    """DustOrderSkip类"""
    def __init__(self, symbol: str, *, qty: float, qty_rounded: float, min_sz: float, lot_sz: float):
        super().__init__(f"dust_skip {symbol}: qty={qty} rounded={qty_rounded} minSz={min_sz} lotSz={lot_sz}")
        self.symbol = symbol
        self.qty = float(qty)
        self.qty_rounded = float(qty_rounded)
        self.min_sz = float(min_sz)
        self.lot_sz = float(lot_sz)


def map_okx_state(okx_state: Optional[str]) -> str:
    """Map okx state"""
    s = str(okx_state or "").lower()
    if s in {"live", "new", "submitted"}:
        return "OPEN"
    if s in {"partially_filled", "partial-filled", "partial"}:
        return "PARTIAL"
    if s in {"filled"}:
        return "FILLED"
    if s in {"canceled", "cancelled", "mmp_canceled"}:
        return "CANCELED"
    if s in {"rejected", "failed"}:
        return "REJECTED"
    return "UNKNOWN"


@dataclass
class LiveExecutionResult:
    """LiveExecutionResult类"""
    cl_ord_id: str
    state: str
    ord_id: Optional[str] = None


def _public_mid_at_submit(*, inst_id: str, timeout_sec: float = 2.0) -> Optional[Dict[str, Any]]:
    """Best-effort fetch mid/bid/ask from OKX public ticker.

    Returns dict with keys: mid,bid,ask,ts_ms. None on failure.
    """

    try:
        url = "https://www.okx.com/api/v5/market/ticker"
        started_at = time.perf_counter()
        try:
            r = requests.get(url, params={"instId": str(inst_id)}, timeout=float(timeout_sec))
            http_status = int(getattr(r, "status_code", 0) or 0) or None
            r.raise_for_status()
            obj = r.json() if r is not None else {}
        except Exception as exc:
            response = getattr(exc, "response", None)
            http_status = int(getattr(response, "status_code", 0) or 0) or None
            record_api_request(
                exchange="okx",
                method="GET",
                endpoint="/api/v5/market/ticker",
                duration_ms=(time.perf_counter() - started_at) * 1000.0,
                status_class=classify_api_status(http_status=http_status, okx_code=None),
                http_status=http_status,
                okx_msg=str(exc),
                rate_limited=is_rate_limited(http_status=http_status, okx_code=None, error_text=str(exc)),
                error_type=exc.__class__.__name__,
            )
            raise
        code = str(obj.get("code")) if isinstance(obj, dict) and obj.get("code") is not None else None
        msg = str(obj.get("msg")) if isinstance(obj, dict) and obj.get("msg") is not None else None
        record_api_request(
            exchange="okx",
            method="GET",
            endpoint="/api/v5/market/ticker",
            duration_ms=(time.perf_counter() - started_at) * 1000.0,
            status_class=classify_api_status(http_status=http_status, okx_code=code),
            http_status=http_status,
            okx_code=code,
            okx_msg=msg,
            rate_limited=is_rate_limited(http_status=http_status, okx_code=code, error_text=msg),
        )
        rows = obj.get("data") if isinstance(obj, dict) else None
        if not isinstance(rows, list) or not rows:
            return None
        t = rows[0] if isinstance(rows[0], dict) else {}
        bid = float(t.get("bidPx") or 0.0)
        ask = float(t.get("askPx") or 0.0)
        ts = int(t.get("ts") or 0)
        if bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
        else:
            mid = float(t.get("last") or 0.0)
        if mid <= 0:
            return None
        return {"mid": float(mid), "bid": float(bid) if bid > 0 else None, "ask": float(ask) if ask > 0 else None, "ts_ms": ts or None}
    except Exception:
        return None


class LiveExecutionEngine:
    """Live execution adapter for OKX spot.

    G0.2 scope:
    - place / get / cancel via OKXPrivateClient
    - idempotency anchored on clOrdId + OrderStore
    - strict gate: if kill-switch or reconcile not ok => SELL_ONLY

    Note: partial-fill inventory sync relies on FillReconciler in poll_open/main.
    This engine focuses on idempotent submission & observability at submit time.
    """

    def __init__(
        self,
        cfg: ExecutionConfig,
        *,
        okx: OKXPrivateClient,
        order_store: Optional[OrderStore] = None,
        position_store: Optional[PositionStore] = None,
        run_id: str = "",
        exp_time_ms: Optional[int] = None,
    ):
        self.cfg = cfg
        self.okx = okx
        order_store_path = str(getattr(cfg, "order_store_path", "reports/orders.sqlite"))
        self.order_store = order_store or OrderStore(path=order_store_path)
        effective_order_store_path = str(getattr(self.order_store, "path", order_store_path))
        self.position_store = position_store or PositionStore(
            path=str(derive_position_store_path(effective_order_store_path))
        )
        self.risk_state_files = [
            str(derive_runtime_named_json_path(effective_order_store_path, "stop_loss_state")),
            str(derive_runtime_named_json_path(effective_order_store_path, "fixed_stop_loss_state")),
            str(derive_runtime_named_json_path(effective_order_store_path, "profit_taking_state")),
            _derive_highest_tracker_state_path(getattr(self.position_store, "path", derive_position_store_path(effective_order_store_path))),
        ]
        self.rank_exit_cooldown_state_path = str(
            derive_runtime_named_json_path(effective_order_store_path, "rank_exit_cooldown_state")
        )
        self.take_profit_cooldown_state_path = str(
            derive_runtime_named_json_path(effective_order_store_path, "take_profit_cooldown_state")
        )
        self.run_id = str(run_id or "")
        self.exp_time_ms = exp_time_ms
        self._closed = False  # 跟踪资源状态
        # In-run quote budget cache for buy-side no-borrow protection.
        self._buy_quote_budget_remaining: Optional[float] = None
        self._sell_base_budget_remaining: Dict[str, float] = {}

    @staticmethod
    def _base_ccy(symbol: str) -> str:
        return str(symbol).split("/")[0].upper()

    def _compute_base_delta_from_fills(
        self,
        *,
        inst_id: str,
        ord_id: Optional[str],
        side: str,
    ) -> Optional[float]:
        if not ord_id or not hasattr(self.okx, "get_fills"):
            return None
        try:
            r = self.okx.get_fills(inst_id=str(inst_id), ord_id=str(ord_id), limit=100)
            data = (r.data or {}).get("data") or []
            if not isinstance(data, list) or not data:
                return None

            base_ccy = str(inst_id).split("-")[0].upper()
            delta = Decimal("0")
            side_l = str(side).lower()
            for it in data:
                if not isinstance(it, dict):
                    continue
                fill_sz = Decimal(str(it.get("fillSz") or "0"))
                fee = Decimal(str(it.get("fee") or "0"))
                fee_ccy = str(it.get("feeCcy") or "").upper()
                base_fee = fee if fee_ccy == base_ccy else Decimal("0")
                if side_l == "buy":
                    delta += fill_sz + base_fee
                elif side_l == "sell":
                    delta += (-fill_sz) + base_fee
            return float(delta)
        except Exception as e:
            log.warning("fill-based position delta fallback failed for %s/%s: %s", inst_id, ord_id, e)
            return None

    def close(self):
        """关闭执行引擎，释放资源"""
        if self._closed:
            return
        self._closed = True
        
        # 关闭OKX客户端（如果支持）
        try:
            if hasattr(self.okx, 'close'):
                self.okx.close()
        except Exception as e:
            log.warning(f"Error closing OKX client: {e}")
        
        # 关闭数据存储
        try:
            if hasattr(self.order_store, 'close'):
                self.order_store.close()
        except Exception as e:
            log.warning(f"Error closing order store: {e}")
        
        try:
            if hasattr(self.position_store, 'close'):
                self.position_store.close()
        except Exception as e:
            log.warning(f"Error closing position store: {e}")

    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口，确保关闭资源"""
        self.close()
        return False  # 不吞掉异常

    def _decision_hash_for_order(self, o: Order) -> str:
        # Prefer upstream decision hash if present
        meta = o.meta or {}
        dh = meta.get("decision_hash") or meta.get("target_hash")
        if dh:
            return str(dh)

        # Fallback: stable hash from semantically relevant order fields.
        payload = {
            "symbol": o.symbol,
            "intent": o.intent,
            "side": o.side,
            "notional_usdt": round(float(o.notional_usdt), 8),
            "signal_price": round(float(o.signal_price or 0.0), 8),
            "target_w": meta.get("target_w"),
            "window_start_ts": meta.get("window_start_ts"),
            "window_end_ts": meta.get("window_end_ts"),
            "regime": meta.get("regime"),
            "deadband_pct": meta.get("deadband_pct"),
        }
        return make_decision_hash(payload)

    def _snapshot_in_run_budgets(self) -> Tuple[Optional[float], Dict[str, float]]:
        buy_budget = self._buy_quote_budget_remaining
        sell_budget = {str(k): float(v) for k, v in self._sell_base_budget_remaining.items()}
        return buy_budget, sell_budget

    def _restore_in_run_budgets(self, snapshot: Tuple[Optional[float], Dict[str, float]]) -> None:
        buy_budget, sell_budget = snapshot
        self._buy_quote_budget_remaining = float(buy_budget) if buy_budget is not None else None
        self._sell_base_budget_remaining = {str(k): float(v) for k, v in (sell_budget or {}).items()}

    @staticmethod
    def _budget_releasable_terminal_state(state: Optional[str]) -> bool:
        return str(state or "").upper() in {"REJECTED", "CANCELED"}

    @staticmethod
    def _fee_map_from_order_row_fee(raw_fee: Optional[str]) -> Dict[str, Decimal]:
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

    def _known_base_delta_from_order_row(self, row) -> float:
        if row is None:
            return 0.0
        side = str(getattr(row, "side", "") or "").lower()
        inst_id = str(getattr(row, "inst_id", "") or "")
        if side not in {"buy", "sell"} or not inst_id:
            return 0.0

        acc_fill_sz = Decimal(str(getattr(row, "acc_fill_sz", None) or "0"))
        base_ccy = inst_id.split("-")[0].upper()
        fee_map = self._fee_map_from_order_row_fee(getattr(row, "fee", None))
        base_fee = fee_map.get(base_ccy, Decimal("0"))
        if side == "buy":
            return float(acc_fill_sz + base_fee)
        return float((-acc_fill_sz) + base_fee)

    def _check_open_long_entry_guard(self, o: Order, *, inst_id: str, tob: Optional[Dict[str, Any]]) -> None:
        """Guard NEW long entries against chasing and excessive microstructure cost."""
        if not bool(getattr(self.cfg, "open_long_entry_guard_enabled", False)):
            return
        if str(getattr(o, "side", "")).lower() != "buy":
            return
        if str(getattr(o, "intent", "")).upper() != "OPEN_LONG":
            return

        signal_px = float(getattr(o, "signal_price", 0.0) or 0.0)
        if signal_px <= 0:
            return

        d = tob or {}
        ask = float(d.get("ask") or 0.0)
        bid = float(d.get("bid") or 0.0)
        mid = float(d.get("mid") or 0.0)
        ref_px = ask if ask > 0 else (mid if mid > 0 else 0.0)

        if ref_px <= 0:
            fail_open = bool(getattr(self.cfg, "open_long_entry_guard_fail_open", True))
            if fail_open:
                log.warning("ENTRY_GUARD skip (no top-of-book): %s", inst_id)
                return
            raise ValueError(f"ENTRY_GUARD_NO_TOB: {inst_id}")

        max_premium = float(_coalesce(getattr(self.cfg, "open_long_max_signal_premium_pct", None), 0.006))
        premium = float(ref_px / signal_px - 1.0)
        if premium > max_premium:
            raise ValueError(
                f"ENTRY_GUARD_PREMIUM: {inst_id} ask_or_mid={ref_px:.8f} signal={signal_px:.8f} "
                f"premium={premium:.4%} > max={max_premium:.4%}"
            )

        if bid > 0 and ask > 0 and mid > 0:
            spread_bps = float((ask - bid) / mid * 10000.0)
            max_spread_bps = float(_coalesce(getattr(self.cfg, "open_long_max_spread_bps", None), 35.0))
            if spread_bps > max_spread_bps:
                raise ValueError(
                    f"ENTRY_GUARD_SPREAD: {inst_id} spread_bps={spread_bps:.2f} > max={max_spread_bps:.2f}"
                )

    def _build_place_payload(self, o: Order, *, inst_id: str, cl_ord_id: str) -> Dict[str, Any]:
        # Minimal market order payload.
        side = str(o.side)
        ord_type = "market"
        td_mode = "cash"

        payload: Dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "ordType": ord_type,
            "clOrdId": cl_ord_id,
            "banAmend": True,
        }
        
        # STRICT NO-BORROW ENFORCEMENT
        # Ensure we're using pure spot mode, no margin/borrow
        if td_mode != "cash":
            raise ValueError(f"Safety violation: tdMode must be 'cash' (no borrow), got '{td_mode}'")
        
        notional = float(o.notional_usdt)
        
        # Log trade intent for audit
        import logging
        log = logging.getLogger(__name__)
        meta = o.meta or {}
        reason = None
        try:
            reason = meta.get("reason")
        except Exception:
            reason = None

        extra = ""
        if reason == "atr_trailing":
            try:
                extra = (
                    f" last={meta.get('last')} stop={meta.get('stop')} highest={meta.get('highest')}"
                    f" atr={meta.get('atr')} mult={meta.get('atr_mult')} n={meta.get('atr_n')}"
                )
            except Exception:
                extra = ""

        log.info(
            f"TRADE_SAFETY: {side} {inst_id}, tdMode={td_mode}, intent={o.intent}, reason={reason}, notional={notional:.4f}{extra}"
        )

        if side == "buy":
            # Spot market buy: submit quote notional in USDT
            # OKX expects plain decimal string
            from decimal import Decimal
            buy_budget_allowed: Optional[float] = None
            buy_budget_avail_quote: Optional[float] = None
            buy_budget_reserve = 0.0

            # Rank-exit re-entry cooldown: after FILLED rank_exit sell, delay OPEN_LONG re-entry.
            buy_intent = str(o.intent or "").upper()
            if buy_intent == "OPEN_LONG":
                cooldown_min = int(getattr(self.cfg, "rank_exit_reentry_cooldown_minutes", 0) or 0)
                if cooldown_min > 0:
                    remain_ms = _rank_exit_cooldown_remaining_ms(
                        o.symbol,
                        cooldown_min,
                        path=self.rank_exit_cooldown_state_path,
                    )
                    if remain_ms > 0:
                        remain_sec = remain_ms / 1000.0
                        raise ValueError(
                            f"RANK_EXIT_REENTRY_COOLDOWN: {o.symbol} remain={remain_sec:.1f}s (<{cooldown_min}m)"
                        )
            # Profit-taking cooldown should block any subsequent buy, including
            # REBALANCE top-ups after a partial trim.
            if buy_intent in {"OPEN_LONG", "REBALANCE"}:
                take_profit_cooldown_min = int(getattr(self.cfg, "take_profit_reentry_cooldown_minutes", 0) or 0)
                if take_profit_cooldown_min > 0:
                    remain_ms = _take_profit_cooldown_remaining_ms(
                        o.symbol,
                        take_profit_cooldown_min,
                        path=self.take_profit_cooldown_state_path,
                    )
                    if remain_ms > 0:
                        remain_sec = remain_ms / 1000.0
                        raise ValueError(
                            f"TAKE_PROFIT_REENTRY_COOLDOWN: {o.symbol} remain={remain_sec:.1f}s (<{take_profit_cooldown_min}m)"
                        )

            # Hard no-borrow guard (buy-side):
            # never allow buy notional to exceed available quote balance budget.
            if bool(getattr(self.cfg, "buy_quote_balance_safety_check", True)):
                if not hasattr(self.okx, "get_balance"):
                    log.warning("BUY_QUOTE_GUARD skipped: client missing get_balance()")
                else:
                    quote_ccy = "USDT"
                    avail_quote: Optional[float] = None
                    liab_quote = 0.0

                    try:
                        b = self.okx.get_balance(ccy=quote_ccy)
                        rows = (b.data or {}).get("data") if isinstance(b.data, dict) else None
                        details = ((rows[0] if isinstance(rows, list) and rows else {}) or {}).get("details")
                        if isinstance(details, list):
                            for d in details:
                                if isinstance(d, dict) and str(d.get("ccy")) == quote_ccy:
                                    avail_quote = float(d.get("availBal") or d.get("eq") or 0.0)
                                    liab_quote = float(d.get("liab") or 0.0)
                                    break
                    except Exception as e:
                        raise ValueError(f"NO_BORROW_BUY_BLOCK: quote balance query failed: {e}")

                    liab_eps = float(_coalesce(getattr(self.cfg, "borrow_liab_eps", None), 1e-6))
                    if liab_quote > liab_eps:
                        raise ValueError(
                            f"NO_BORROW_BUY_BLOCK: existing {quote_ccy} liability={liab_quote:.8f}"
                        )

                    if avail_quote is None:
                        raise ValueError("NO_BORROW_BUY_BLOCK: unavailable quote balance")

                    reserve = float(getattr(self.cfg, "buy_quote_reserve_usdt", 0.5) or 0.0)
                    slack = float(getattr(self.cfg, "buy_quote_slack_ratio", 0.001) or 0.0)

                    # Initialize per-run budget on first buy.
                    if self._buy_quote_budget_remaining is None:
                        self._buy_quote_budget_remaining = max(0.0, float(avail_quote) - float(reserve))

                    buy_budget_allowed = max(0.0, float(self._buy_quote_budget_remaining))
                    buy_budget_avail_quote = float(avail_quote)
                    buy_budget_reserve = float(reserve)
                    if float(notional) > buy_budget_allowed * (1.0 + max(0.0, slack)):
                        raise ValueError(
                            f"NO_BORROW_BUY_BLOCK: notional={float(notional):.6f} exceeds "
                            f"quote_budget={buy_budget_allowed:.6f} {quote_ccy}"
                        )

            # Pre-check against minSz using signal price estimate to avoid predictable rejects.
            specs = _get_spot_spec_best_effort(inst_id)
            px_ref = float(getattr(o, "signal_price", 0.0) or 0.0)
            if specs is not None and float(specs.min_sz or 0.0) > 0 and px_ref > 0:
                est_base_qty = float(notional) / float(px_ref)
                if est_base_qty < float(specs.min_sz):
                    raise DustOrderSkip(
                        o.symbol,
                        qty=est_base_qty,
                        qty_rounded=est_base_qty,
                        min_sz=float(specs.min_sz),
                        lot_sz=float(specs.lot_sz or 0.0),
                    )

            if buy_budget_allowed is not None:
                self._buy_quote_budget_remaining = max(0.0, float(buy_budget_allowed) - float(notional))
                log.info(
                    "BUY_QUOTE_GUARD pass: notional=%.6f avail=%.6f reserve=%.6f remain=%.6f",
                    float(notional),
                    float(buy_budget_avail_quote or 0.0),
                    float(buy_budget_reserve),
                    float(self._buy_quote_budget_remaining),
                )

            payload["sz"] = format(Decimal(str(notional)), "f")
            payload["tgtCcy"] = "quote_ccy"
        else:
            # Spot market sell: STRICT NO-BORROW ENFORCEMENT
            # 1. Check local position store
            p = self.position_store.get(o.symbol)
            if not p or float(p.qty) <= 0:
                raise ValueError(f"NO_BORROW_SAFETY: No position qty available for sell: {o.symbol}. "
                               f"Would trigger borrow. Aborting.")
            
            # 2. Double-check with OKX balance before selling (best-effort).
            # If OKX explicitly reports negative balance or insufficient base, abort.
            try:
                balance_resp = self.okx.get_balance()
                base_ccy = o.symbol.split('/')[0]
                okx_base_eq = None
                okx_base_avail = None

                rows = (balance_resp.data or {}).get("data") if isinstance(balance_resp.data, dict) else None
                details = ((rows[0] if isinstance(rows, list) and rows else {}) or {}).get("details")
                if isinstance(details, list):
                    for d in details:
                        if isinstance(d, dict) and str(d.get("ccy")) == str(base_ccy):
                            okx_base_eq = float(d.get("eq") or 0.0)
                            okx_base_avail = float(d.get("availBal") or d.get("cashBal") or d.get("eq") or 0.0)
                            break

                if okx_base_eq is not None:
                    if okx_base_eq < 0:
                        raise ValueError(
                            f"NO_BORROW_SAFETY: OKX shows NEGATIVE balance for {o.symbol}: {okx_base_eq}. "
                            f"Would increase borrow. Aborting."
                        )

                    if okx_base_avail is not None and okx_base_avail <= 0:
                        raise ValueError(
                            f"NO_BORROW_SAFETY: OKX available balance ({okx_base_avail}) <= 0 "
                            f"for {o.symbol}. Risk of borrow. Aborting."
                        )

                    if okx_base_eq <= 0 and (okx_base_avail is None or okx_base_avail <= 0):
                        raise ValueError(
                            f"NO_BORROW_SAFETY: OKX balance ({okx_base_eq}) <= 0 "
                            f"for {o.symbol}. Risk of borrow. Aborting."
                        )

            except ValueError:
                raise
            except Exception as e:
                log.warning(f"Balance pre-check failed (proceeding with caution): {e}")
            
            # 3. Log sell attempt for audit
            log.info(f"SELL_SAFETY_CHECK: Selling {o.symbol}, local_qty={p.qty}, intent={o.intent}")

            # Determine sell quantity.
            # - CLOSE_LONG: sell full local position
            # - REBALANCE: sell partial by requested notional (capped by local qty)
            # CRITICAL: 全程使用Decimal避免精度丢失
            from decimal import Decimal
            
            sellable_qty = float(p.qty)
            if okx_base_avail is not None:
                sellable_qty = min(sellable_qty, max(0.0, float(okx_base_avail)))
            elif okx_base_eq is not None:
                sellable_qty = min(sellable_qty, max(0.0, float(okx_base_eq)))

            budget_key = str(o.symbol)
            cached_budget = self._sell_base_budget_remaining.get(budget_key)
            if cached_budget is None:
                self._sell_base_budget_remaining[budget_key] = max(0.0, float(sellable_qty))
            else:
                sellable_qty = min(sellable_qty, max(0.0, float(cached_budget)))

            if sellable_qty <= 0:
                raise ValueError(
                    f"NO_BORROW_SAFETY: no sellable balance on OKX for {o.symbol}. "
                    f"local={p.qty} okx_eq={okx_base_eq} okx_avail={okx_base_avail}"
                )

            if sellable_qty + 1e-12 < float(p.qty):
                log.warning(
                    "SELL_QTY_CAPPED_BY_OKX: %s local_qty=%.12g okx_eq=%s okx_avail=%s capped_qty=%.12g",
                    o.symbol,
                    float(p.qty),
                    okx_base_eq,
                    okx_base_avail,
                    sellable_qty,
                )

            qty_full_dec = Decimal(str(max(0.0, sellable_qty)))
            if str(o.intent).upper() == "CLOSE_LONG":
                qty_dec = qty_full_dec
            else:
                px_ref = float(getattr(o, "signal_price", 0.0) or 0.0)
                if px_ref <= 0:
                    # Safety: for REBALANCE sells, missing signal price must NOT degrade to full liquidation.
                    raise ValueError(
                        f"REBALANCE_SAFETY: missing signal_price for {o.symbol}, refusing fallback full-qty sell"
                    )
                # 使用Decimal计算: min(qty_full, notional / px_ref)
                notional_dec = Decimal(str(o.notional_usdt))
                px_dec = Decimal(str(px_ref))
                qty_from_notional = notional_dec / px_dec
                qty_dec = min(qty_full_dec, qty_from_notional)
            
            qty = float(qty_dec)  # 转换回float用于后续检查

            # Enforce OKX minSz/lotSz to avoid Parameter sz error.
            specs = OKXSpotInstrumentsCache().get_spec(inst_id)
            if specs is not None and float(specs.lot_sz or 0.0) > 0:
                qty_rounded = round_down_to_lot(qty, float(specs.lot_sz))
            else:
                qty_rounded = qty

            min_sz = float(specs.min_sz) if specs is not None else 0.0
            lot_sz = float(specs.lot_sz) if specs is not None else 0.0
            if min_sz > 0 and qty_rounded < min_sz:
                # Optional: if partial REBALANCE sell is below minSz, auto-upgrade to full close.
                # This avoids repeated DUST rejects when residual position itself is tradable.
                auto_upgrade = bool(getattr(self.cfg, "auto_upgrade_dust_sell_to_close", True))
                if auto_upgrade and str(o.intent).upper() == "REBALANCE":
                    qty_full = float(qty_full_dec)
                    qty_full_rounded = round_down_to_lot(qty_full, lot_sz) if lot_sz > 0 else qty_full
                    if qty_full_rounded >= min_sz:
                        pre_upgrade_qty = float(qty_rounded)
                        log.warning(
                            "DUST_AUTO_UPGRADE_TO_FULL_CLOSE: %s partial_qty=%.12g < minSz=%.12g, upgrade_to_full_qty=%.12g",
                            o.symbol,
                            pre_upgrade_qty,
                            float(min_sz),
                            float(qty_full_rounded),
                        )
                        qty = float(qty_full_rounded)
                        qty_rounded = float(qty_full_rounded)
                        try:
                            if isinstance(o.meta, dict):
                                o.meta["dust_auto_upgrade"] = True
                                o.meta["dust_upgrade_from_qty"] = pre_upgrade_qty
                                o.meta["dust_upgrade_to_qty"] = float(qty_full_rounded)
                                o.meta["dust_upgrade_min_sz"] = float(min_sz)
                        except Exception:
                            pass
                    else:
                        raise DustOrderSkip(o.symbol, qty=qty, qty_rounded=qty_rounded, min_sz=min_sz, lot_sz=lot_sz)
                else:
                    raise DustOrderSkip(o.symbol, qty=qty, qty_rounded=qty_rounded, min_sz=min_sz, lot_sz=lot_sz)

            reserved_qty = min(float(sellable_qty), max(0.0, float(qty_rounded)))
            self._sell_base_budget_remaining[budget_key] = max(0.0, float(sellable_qty) - reserved_qty)

            # OKX rejects scientific notation (e.g. 5e-05) with 51000 Parameter sz error.
            # Always send plain decimal string and avoid float->str exponent.
            from decimal import Decimal, ROUND_DOWN

            lot = float(specs.lot_sz) if specs is not None and specs.lot_sz is not None else 0.0
            if lot and lot > 0:
                lot_dec = Decimal(str(lot))
                # quantize to lot step
                q = (Decimal(str(qty)) / lot_dec).to_integral_value(rounding=ROUND_DOWN) * lot_dec
                sz_str = format(q, "f")
            else:
                sz_str = format(Decimal(str(qty)), "f")

            payload["sz"] = sz_str
            # For spot market SELL, omit tgtCcy to avoid OKX 51000 on some instruments.

        return payload

    def place(self, o: Order) -> LiveExecutionResult:
        """Place"""
        gate, reconcile_ok, kill_switch = submit_gate_for_live(self.cfg)
        inst_id = symbol_to_inst_id(o.symbol)
        dh = self._decision_hash_for_order(o)
        clid = make_cl_ord_id(self.run_id, inst_id, o.intent, dh, o.side, "market", "cash")

        # Manual approval required for liability-repair intents.
        repair_intents = {
            "REPAY_LIABILITY",
            "REPAY_SOL_LIABILITY",
            "EMERGENCY_MERL_REPAYMENT",
            "IMMEDIATE_LIABILITY_REPAYMENT",
        }
        if str(o.intent or "").upper() in repair_intents and os.getenv("V5_REPAIR_ARM") != "YES":
            self.order_store.upsert_new(
                cl_ord_id=clid,
                run_id=self.run_id,
                inst_id=inst_id,
                side=o.side,
                intent=o.intent,
                decision_hash=dh,
                td_mode="cash",
                ord_type="market",
                notional_usdt=float(o.notional_usdt),
                window_start_ts=(o.meta or {}).get("window_start_ts"),
                window_end_ts=(o.meta or {}).get("window_end_ts"),
                req={"blocked_by_policy": True, "reason": "repair_intent_requires_manual_arm", "env": "V5_REPAIR_ARM"},
                reconcile_ok_at_submit=reconcile_ok,
                kill_switch_at_submit=kill_switch,
                submit_gate=gate,
            )
            self.order_store.update_state(clid, new_state="REJECTED", last_error_code="POLICY", last_error_msg="repair_intent_requires_manual_arm")
            return LiveExecutionResult(cl_ord_id=clid, state="REJECTED")

        # Gate: block all buys (OPEN/REBALANCE) in SELL_ONLY mode.
        if gate == "SELL_ONLY" and o.side == "buy" and o.intent in {"OPEN_LONG", "REBALANCE"}:
            self.order_store.upsert_new(
                cl_ord_id=clid,
                run_id=self.run_id,
                inst_id=inst_id,
                side=o.side,
                intent=o.intent,
                decision_hash=dh,
                td_mode="cash",
                ord_type="market",
                notional_usdt=float(o.notional_usdt),
                window_start_ts=(o.meta or {}).get("window_start_ts"),
                window_end_ts=(o.meta or {}).get("window_end_ts"),
                req={"blocked_by_gate": True, "gate": gate},
                reconcile_ok_at_submit=reconcile_ok,
                kill_switch_at_submit=kill_switch,
                submit_gate=gate,
            )
            self.order_store.update_state(clid, new_state="REJECTED", last_error_code="GATE", last_error_msg="SELL_ONLY")
            return LiveExecutionResult(cl_ord_id=clid, state="REJECTED")

        existing = self.order_store.get(clid)
        if existing:
            st0 = str(existing.state).upper()
            if st0 in {"FILLED", "CANCELED", "REJECTED"}:
                return LiveExecutionResult(cl_ord_id=clid, state=st0, ord_id=existing.ord_id)
            # Idempotency: if we already have a record for this clOrdId, do not resubmit.
            st, ord_id = self._query_and_update(inst_id=inst_id, cl_ord_id=clid)
            return LiveExecutionResult(cl_ord_id=clid, state=st, ord_id=ord_id)

        # Hard safety: if same symbol has FILLED OPEN_LONG within cooldown window,
        # reject new OPEN_LONG to avoid repeated entries from overlapping triggers.
        if str(o.side).lower() == "buy" and str(o.intent or "").upper() == "OPEN_LONG":
            cooldown_min = int(getattr(self.cfg, "open_long_cooldown_minutes", 10) or 0)
            if cooldown_min > 0:
                now_ms = int(time.time() * 1000)
                since_ts = now_ms - cooldown_min * 60 * 1000
                latest = self.order_store.get_latest_filled(
                    inst_id=inst_id,
                    side="buy",
                    intent="OPEN_LONG",
                    since_ts=since_ts,
                )
                if latest is not None:
                    latest_event_ts = int(latest.updated_ts) if int(latest.updated_ts) > 0 else int(latest.created_ts)
                    elapsed_sec = max(0.0, (now_ms - latest_event_ts) / 1000.0)
                    remain_sec = max(0.0, cooldown_min * 60.0 - elapsed_sec)
                    self.order_store.upsert_new(
                        cl_ord_id=clid,
                        run_id=self.run_id,
                        inst_id=inst_id,
                        side=o.side,
                        intent=o.intent,
                        decision_hash=dh,
                        td_mode="cash",
                        ord_type="market",
                        notional_usdt=float(o.notional_usdt),
                        window_start_ts=(o.meta or {}).get("window_start_ts"),
                        window_end_ts=(o.meta or {}).get("window_end_ts"),
                        req={
                            "blocked_by_cooldown": True,
                            "cooldown_minutes": cooldown_min,
                            "latest_filled_cl_ord_id": str(latest.cl_ord_id),
                            "latest_filled_run_id": str(latest.run_id),
                            "latest_filled_updated_ts": int(latest.updated_ts),
                            "latest_filled_event_ts": latest_event_ts,
                            "elapsed_sec": float(round(elapsed_sec, 3)),
                            "remain_sec": float(round(remain_sec, 3)),
                        },
                        reconcile_ok_at_submit=reconcile_ok,
                        kill_switch_at_submit=kill_switch,
                        submit_gate=gate,
                    )
                    msg = (
                        f"OPEN_LONG cooldown active for {o.symbol}: "
                        f"latest fill {elapsed_sec:.1f}s ago (< {cooldown_min * 60:.0f}s)"
                    )
                    self.order_store.update_state(
                        clid,
                        new_state="REJECTED",
                        last_error_code="OPEN_LONG_COOLDOWN",
                        last_error_msg=msg,
                        event_type="COOLDOWN_BLOCK",
                    )
                    log.warning(msg)
                    return LiveExecutionResult(cl_ord_id=clid, state="REJECTED")

        try:
            # Best-effort top-of-book at submit time for entry guard + slippage attribution.
            tob = _public_mid_at_submit(inst_id=inst_id, timeout_sec=2.0)
            self._check_open_long_entry_guard(o, inst_id=inst_id, tob=tob)

            budget_snapshot = self._snapshot_in_run_budgets()
            payload = self._build_place_payload(o, inst_id=inst_id, cl_ord_id=clid)
            req_store = dict(payload)
            req_store["_v5_reason"] = str(((o.meta or {}).get("reason")) or "")
            req_store["_v5_order_meta"] = probe_tags_from_order_meta(
                o.meta or {},
                entry_px=float(o.signal_price or 0.0),
            ) or {}
            if tob:
                req_store["_meta"] = {"mid_px_at_submit": tob.get("mid"), "bid": tob.get("bid"), "ask": tob.get("ask"), "ts_ms": tob.get("ts_ms")}
        except ValueError as e:
            # Policy/safety reject without touching the exchange.
            self.order_store.upsert_new(
                cl_ord_id=clid,
                run_id=self.run_id,
                inst_id=inst_id,
                side=o.side,
                intent=o.intent,
                decision_hash=dh,
                td_mode="cash",
                ord_type="market",
                notional_usdt=float(o.notional_usdt),
                window_start_ts=(o.meta or {}).get("window_start_ts"),
                window_end_ts=(o.meta or {}).get("window_end_ts"),
                req={"safety_reject": True, "error": str(e)},
                reconcile_ok_at_submit=reconcile_ok,
                kill_switch_at_submit=kill_switch,
                submit_gate=gate,
            )
            self.order_store.update_state(
                clid,
                new_state="REJECTED",
                last_error_code="SAFETY",
                last_error_msg=str(e),
                event_type="SAFETY_REJECT",
            )
            return LiveExecutionResult(cl_ord_id=clid, state="REJECTED")
        except DustOrderSkip as e:
            # Persist and mark as rejected (terminal) without touching the exchange.
            self.order_store.upsert_new(
                cl_ord_id=clid,
                run_id=self.run_id,
                inst_id=inst_id,
                side=o.side,
                intent=o.intent,
                decision_hash=dh,
                td_mode="cash",
                ord_type="market",
                notional_usdt=float(o.notional_usdt),
                window_start_ts=(o.meta or {}).get("window_start_ts"),
                window_end_ts=(o.meta or {}).get("window_end_ts"),
                req={"dust_skip": True, "symbol": e.symbol, "qty": e.qty, "qty_rounded": e.qty_rounded, "minSz": e.min_sz, "lotSz": e.lot_sz},
                reconcile_ok_at_submit=reconcile_ok,
                kill_switch_at_submit=kill_switch,
                submit_gate=gate,
            )
            self.order_store.update_state(clid, new_state="REJECTED", last_error_code="DUST", last_error_msg=str(e), event_type="DUST_SKIP")

            # For tiny SELL dust (< threshold), clear local position to prevent repeated close retries.
            try:
                if str(o.side).lower() == "sell":
                    px = float(getattr(o, "signal_price", 0.0) or 0.0)
                    est_qty = float(e.qty_rounded or e.qty or 0.0)
                    est_notional = est_qty * px if px > 0 else 0.0
                    if est_notional > 0 and est_notional < DUST_NOTIONAL_USDT:
                        self.position_store.close_long(o.symbol)
                        clear_risk_state_on_full_close(o.symbol, state_files=self.risk_state_files)
                        log.info(
                            f"DUST_LOCAL_CLOSE: {o.symbol} est_notional={est_notional:.6f} < {DUST_NOTIONAL_USDT}"
                        )
            except Exception as _e:
                log.warning(f"DUST_LOCAL_CLOSE failed for {o.symbol}: {_e}")

            return LiveExecutionResult(cl_ord_id=clid, state="REJECTED")

        # 1) persist intent before sending
        self.order_store.upsert_new(
            cl_ord_id=clid,
            run_id=self.run_id,
            inst_id=inst_id,
            side=o.side,
            intent=o.intent,
            decision_hash=dh,
            td_mode="cash",
            ord_type="market",
            notional_usdt=float(o.notional_usdt),
            window_start_ts=(o.meta or {}).get("window_start_ts"),
            window_end_ts=(o.meta or {}).get("window_end_ts"),
            req=req_store,
            reconcile_ok_at_submit=reconcile_ok,
            kill_switch_at_submit=kill_switch,
            submit_gate=gate,
        )

        # 2) send
        self.order_store.update_state(clid, new_state="SENT")

        try:
            ack = self.okx.place_order(payload, exp_time_ms=self.exp_time_ms)
            ok_ack, ord_id, err_code, err_msg = _parse_okx_order_ack(ack.data)
            if not ok_ack:
                # Exchange explicitly rejected the order: terminal REJECTED, no polling.
                self._restore_in_run_budgets(budget_snapshot)
                self.order_store.update_state(
                    clid,
                    new_state="REJECTED",
                    ack=ack.data,
                    last_error_code=err_code,
                    last_error_msg=err_msg,
                    event_type="REJECTED_ACK",
                )
                return LiveExecutionResult(cl_ord_id=clid, state="REJECTED")

            self.order_store.update_state(
                clid,
                new_state="ACK",
                ack=ack.data,
                ord_id=ord_id or self._extract_ord_id(ack),
                event_type="ACK",
            )
            # follow-up poll to get state if possible
            polled_state, polled_ord_id = self._query_and_update(inst_id=inst_id, cl_ord_id=clid)
            if self._budget_releasable_terminal_state(polled_state):
                self._restore_in_run_budgets(budget_snapshot)

            # Keep filled-order position sync centralized in _query_and_update().
            # place() already calls that path once after ACK; repeating the sync here
            # double-counts immediate fills.
            try:
                if polled_state == "FILLED":
                    # Record rank-exit fill for re-entry cooldown gate.
                    try:
                        reason = str((o.meta or {}).get("reason", "") or "")
                        if str(o.side).lower() == "sell" and reason.startswith("rank_exit_"):
                            _record_rank_exit_fill(o.symbol, reason, path=self.rank_exit_cooldown_state_path)
                        if str(o.side).lower() == "sell" and (
                            reason.startswith("profit_taking_")
                            or reason.startswith("profit_partial_")
                        ):
                            _record_take_profit_fill(o.symbol, reason, path=self.take_profit_cooldown_state_path)
                    except Exception:
                        pass
            except Exception:
                pass

            return LiveExecutionResult(cl_ord_id=clid, state=polled_state, ord_id=polled_ord_id)

        except OKXPrivateClientError as e:
            # network/timeout/etc. => do NOT resubmit; query by clOrdId
            self.order_store.update_state(
                clid,
                new_state="UNKNOWN",
                last_error_code="CLIENT",
                last_error_msg=str(e),
                event_type="PLACE_ERROR",
            )
            # short delay then query (eventual consistency)
            time.sleep(0.25)
            st, ord_id = self._query_and_update(inst_id=inst_id, cl_ord_id=clid)
            if self._budget_releasable_terminal_state(st):
                self._restore_in_run_budgets(budget_snapshot)
            return LiveExecutionResult(cl_ord_id=clid, state=st, ord_id=ord_id)

    def _extract_ord_id(self, resp: OKXResponse) -> Optional[str]:
        d = resp.data if isinstance(resp.data, dict) else {}
        rows = d.get("data")
        if isinstance(rows, list) and rows:
            r0 = rows[0] or {}
            oid = r0.get("ordId") or r0.get("ord_id")
            return str(oid) if oid else None
        return None

    def _query_and_update(self, *, inst_id: str, cl_ord_id: str) -> Tuple[str, Optional[str]]:
        row_before = self.order_store.get(cl_ord_id)
        try:
            qr = self.okx.get_order(inst_id=inst_id, cl_ord_id=cl_ord_id)
        except Exception as e:
            self.order_store.update_state(cl_ord_id, new_state="UNKNOWN", last_error_code="QUERY", last_error_msg=str(e), event_type="QUERY_ERROR")
            return "UNKNOWN", None

        d = qr.data if isinstance(qr.data, dict) else {}
        rows = d.get("data")
        if not isinstance(rows, list) or not rows:
            # not found yet
            self.order_store.update_state(cl_ord_id, new_state="UNKNOWN", last_query=d, event_type="QUERY_NOT_FOUND")
            return "UNKNOWN", None

        r0 = rows[0] or {}
        okx_state = r0.get("state")
        st = map_okx_state(okx_state)
        ord_id = r0.get("ordId") or r0.get("ord_id")
        self.order_store.update_state(
            cl_ord_id,
            new_state=st,
            ord_id=str(ord_id) if ord_id else None,
            last_query=d,
            acc_fill_sz=r0.get("accFillSz") or r0.get("acc_fill_sz"),
            avg_px=r0.get("avgPx") or r0.get("avg_px"),
            fee=r0.get("fee"),
            event_type="POLL",
        )
        if st == "FILLED":
            try:
                row = self.order_store.get(cl_ord_id)
                if row is not None:
                    acc_fill_sz_total = float(r0.get("accFillSz") or r0.get("acc_fill_sz") or 0.0)
                    avg_px = float(r0.get("avgPx") or r0.get("avg_px") or 0.0)
                    total_base_delta = self._compute_base_delta_from_fills(
                        inst_id=str(inst_id),
                        ord_id=str(ord_id or ""),
                        side=str(row.side or ""),
                    )
                    prev_acc_fill_sz = float(getattr(row_before, "acc_fill_sz", None) or 0.0)
                    prev_known_base_delta = self._known_base_delta_from_order_row(row_before)
                    if str(row.side).lower() == "buy":
                        if total_base_delta is not None:
                            delta_buy_qty = float(total_base_delta) - float(prev_known_base_delta)
                        else:
                            delta_buy_qty = float(acc_fill_sz_total) - float(prev_acc_fill_sz)
                        if delta_buy_qty > 0:
                            self.position_store.upsert_buy(
                                str(row.inst_id).replace("-", "/"),
                                qty=float(delta_buy_qty),
                                px=float(avg_px or 0.0),
                                tags=probe_tags_from_order_meta(
                                    _order_meta_from_row(row),
                                    entry_px=float(avg_px or 0.0),
                                ),
                            )
                    elif str(row.side).lower() == "sell":
                        p = self.position_store.get(str(row.inst_id).replace("-", "/"))
                        if p is not None:
                            if total_base_delta is not None:
                                reduce_qty = max(0.0, -(float(total_base_delta) - float(prev_known_base_delta)))
                            else:
                                reduce_qty = max(0.0, float(acc_fill_sz_total) - float(prev_acc_fill_sz))
                            new_qty = max(0.0, float(p.qty) - float(reduce_qty))
                            if new_qty <= 0:
                                self.position_store.close_long(str(row.inst_id).replace("-", "/"))
                                clear_risk_state_on_full_close(
                                    str(row.inst_id).replace("-", "/"),
                                    state_files=self.risk_state_files,
                                )
                            else:
                                self.position_store.set_qty(str(row.inst_id).replace("-", "/"), qty=new_qty)
            except Exception as e:
                log.warning("position sync on query fill failed for %s: %s", cl_ord_id, e)
        return st, (str(ord_id) if ord_id else None)

    def cancel(self, *, symbol: str, cl_ord_id: str) -> bool:
        """Cancel"""
        inst_id = symbol_to_inst_id(symbol)
        try:
            ack = self.okx.cancel_order(inst_id=inst_id, cl_ord_id=cl_ord_id)
            self.order_store.update_state(cl_ord_id, new_state="CANCELED", ack=ack.data, event_type="CANCEL")
            return True
        except Exception as e:
            self.order_store.update_state(cl_ord_id, new_state="UNKNOWN", last_error_code="CANCEL", last_error_msg=str(e), event_type="CANCEL_ERROR")
            return False

    def poll_open(self, limit: int = 200) -> List[LiveExecutionResult]:
        """Poll open"""
        # 0) Optional: sync fills into FillStore then reconcile into OrderStore.
        try:
            from src.execution.fill_store import FillStore, derive_fill_store_path
            from src.execution.fill_reconciler import FillReconciler

            fs = FillStore(path=str(derive_fill_store_path(self.order_store.path)))
            rec = FillReconciler(fill_store=fs, order_store=self.order_store, okx=self.okx, position_store=self.position_store)
            rec.reconcile(limit=2000, max_get_order_per_run=20)
        except Exception:
            pass

        out: List[LiveExecutionResult] = []
        rows = self.order_store.list_open(limit=limit)
        for r in rows:
            st, oid = self._query_and_update(inst_id=str(r.inst_id), cl_ord_id=str(r.cl_ord_id))
            out.append(LiveExecutionResult(cl_ord_id=str(r.cl_ord_id), state=st, ord_id=oid))
        return out

    def execute(self, order_batch: List[Order]) -> ExecutionReport:
        """Execute"""
        # Minimal batch executor; used by main() once wired.
        ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        # Reset per-run quote budget cache.
        self._buy_quote_budget_remaining = None
        self._sell_base_budget_remaining = {}
        placed: List[Order] = []
        for o in order_batch or []:
            try:
                self.place(o)
                placed.append(o)
            except Exception as e:
                log.error(f"place failed for {o.symbol} {o.side} {o.intent}: {e}")
        return ExecutionReport(timestamp=ts, dry_run=False, orders=placed)
