from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

from src.reporting.trade_log import Fill, TradeLogWriter
from src.reporting.cost_events import append_cost_event
from src.reporting.spread_snapshot_store import SpreadSnapshotStore
from src.execution.order_store import OrderStore


def _dec(x: Optional[str]) -> Decimal:
    if x is None or x == "":
        return Decimal("0")
    return Decimal(str(x))


def inst_id_to_symbol(inst_id: str) -> str:
    """Inst id to symbol"""
    return str(inst_id).replace("-", "/")


def _iso_utc_from_ts_ms(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    # keep milliseconds
    return dt.isoformat().replace("+00:00", "Z")


def fee_to_usdt_signed(*, fee: str, fee_ccy: str, inst_id: str, fill_px: str) -> Optional[Decimal]:
    """Convert OKX fee to signed USDT value.

    OKX semantics (commonly observed):
    - fee < 0 => cost
    - fee > 0 => rebate

    Returns signed USDT: negative means cost, positive means rebate.
    """
    if fee is None or fee_ccy is None:
        return None

    fee_d = _dec(fee)
    ccy = str(fee_ccy)
    if ccy.upper() == "USDT":
        return fee_d

    base = str(inst_id).split("-")[0] if "-" in str(inst_id) else None
    if base and ccy.upper() == base.upper():
        return fee_d * _dec(fill_px)

    return None


def fee_cost_usdt(*, fee: str, fee_ccy: str, inst_id: str, fill_px: str) -> Optional[Decimal]:
    """Return cost-oriented fee in USDT.

    - cost is positive
    - rebate is negative
    """
    s = fee_to_usdt_signed(fee=fee, fee_ccy=fee_ccy, inst_id=inst_id, fill_px=fill_px)
    if s is None:
        return None
    return Decimal("0") - s


@dataclass
class ExportResult:
    """ExportResult类"""
    trade_written: bool
    cost_event_written: bool


def compute_slippage(
    *,
    side: str,
    fill_px: float,
    qty: float,
    bid: Optional[float],
    ask: Optional[float],
    mid: Optional[float],
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Compute (slippage_bps, slippage_usdt, bid, ask) with direction-normalized sign.

    - buy: worse if fill_px > mid
    - sell: worse if fill_px < mid

    Returns:
      slippage_bps, slippage_usdt, bid, ask
    """
    if mid is None or mid <= 0 or fill_px <= 0 or qty <= 0:
        return None, None, bid, ask

    s = str(side).lower()
    if s == "buy":
        slip_bps = (fill_px - mid) / mid * 10_000.0
        slip_usdt = abs(fill_px - mid) * qty
        return float(slip_bps), float(slip_usdt), bid, ask
    if s == "sell":
        slip_bps = (mid - fill_px) / mid * 10_000.0
        slip_usdt = abs(fill_px - mid) * qty
        return float(slip_bps), float(slip_usdt), bid, ask

    return None, None, bid, ask


def _read_submit_meta(*, symbol: str, cl_ord_id: Optional[str], order_store_path: str = "reports/orders.sqlite") -> Tuple[Optional[float], Optional[float], Optional[float], Optional[int]]:
    """Return (mid,bid,ask,ts_ms) from OrderStore.req_json._meta if available."""
    if not cl_ord_id:
        return None, None, None, None
    try:
        os = OrderStore(path=order_store_path)
        row = os.get(str(cl_ord_id))
        if row is None:
            return None, None, None, None
        try:
            req = json.loads(row.req_json or "{}")
        except Exception:
            req = {}
        meta = (req.get("_meta") or {}) if isinstance(req, dict) else {}
        if not isinstance(meta, dict):
            return None, None, None, None
        mid = meta.get("mid_px_at_submit")
        bid = meta.get("bid")
        ask = meta.get("ask")
        ts = meta.get("ts_ms")
        return (float(mid) if mid is not None else None,
                float(bid) if bid is not None else None,
                float(ask) if ask is not None else None,
                int(ts) if ts is not None else None)
    except Exception:
        return None, None, None, None


def export_fill(
    *,
    fill_ts_ms: int,
    inst_id: str,
    side: str,
    fill_px: str,
    fill_sz: str,
    fee: Optional[str],
    fee_ccy: Optional[str],
    run_id: str,
    intent: str,
    window_start_ts: Optional[int],
    window_end_ts: Optional[int],
    run_dir: str,
    regime: Optional[str] = None,
    deadband_pct: Optional[float] = None,
    drift: Optional[float] = None,
    spread_store: Optional[SpreadSnapshotStore] = None,
    cl_ord_id: Optional[str] = None,
    order_store_path: str = "reports/orders.sqlite",
) -> ExportResult:
    """Export a single fill into trades.csv (per run) and cost_events NDJSON (daily).

    Slippage is left as 0.0 for now (will be improved using spread snapshots).
    """

    symbol = inst_id_to_symbol(inst_id)
    qty = float(_dec(fill_sz))
    px = float(_dec(fill_px))
    notional = float((_dec(fill_sz) * _dec(fill_px)))

    fee_cost = fee_cost_usdt(fee=str(fee) if fee is not None else "0", fee_ccy=str(fee_ccy) if fee_ccy is not None else "", inst_id=inst_id, fill_px=fill_px)
    fee_usdt = float(fee_cost) if fee_cost is not None else 0.0

    # Slippage reference (priority): mid_at_submit -> spread snapshot -> None
    mid, bid, ask, meta_ts_ms = _read_submit_meta(symbol=symbol, cl_ord_id=cl_ord_id, order_store_path=order_store_path)

    spread_bps = None
    if mid is None:
        ss = spread_store or SpreadSnapshotStore()
        snap = None
        try:
            snap = ss.get_latest_before(symbol=symbol, ts_ms=int(fill_ts_ms))
        except Exception:
            snap = None

        bid = float(snap.bid) if snap is not None else None
        ask = float(snap.ask) if snap is not None else None
        mid = float(snap.mid) if snap is not None else None
        spread_bps = float(snap.spread_bps) if (snap is not None and snap.spread_bps is not None) else None
    else:
        # derive spread_bps if we have bid/ask
        if bid is not None and ask is not None and mid is not None and mid > 0:
            spread_bps = float((ask - bid) / mid * 10_000.0)

    slip_bps, slip_usdt, _, _ = compute_slippage(side=str(side), fill_px=float(px), qty=float(qty), bid=bid, ask=ask, mid=mid)

    # Trades CSV
    tl = TradeLogWriter(run_dir=run_dir)
    tl.append_fill(
        Fill(
            ts=_iso_utc_from_ts_ms(int(fill_ts_ms)),
            run_id=str(run_id),
            symbol=str(symbol),
            intent=str(intent),
            side=str(side),
            qty=float(qty),
            price=float(px),
            notional_usdt=float(notional),
            fee_usdt=float(fee_usdt),
            slippage_usdt=(None if slip_usdt is None else float(slip_usdt)),
            realized_pnl_usdt=None,
            realized_pnl_pct=None,
        )
    )

    trade_written = True

    # Cost event (requires window_start_ts)
    cost_event_written = False
    if window_start_ts is not None and window_end_ts is not None:
        fee_usdt_val = (float(fee_usdt) if fee_cost is not None else None)
        slip_usdt_val = (float(slip_usdt) if slip_usdt is not None else None)

        fee_bps_val = None
        if fee_usdt_val is not None and float(notional) > 0:
            fee_bps_val = float(fee_usdt_val) / float(notional) * 10_000.0

        cost_usdt_total_val = None
        if fee_usdt_val is not None:
            cost_usdt_total_val = float(fee_usdt_val) + (float(slip_usdt_val) if slip_usdt_val is not None else 0.0)

        cost_bps_total_val = None
        if cost_usdt_total_val is not None and float(notional) > 0:
            cost_bps_total_val = float(cost_usdt_total_val) / float(notional) * 10_000.0

        event: Dict[str, Any] = {
            "schema_version": 1,
            "source": "okx_fill",
            "event_type": "fill",
            "ts": int(int(fill_ts_ms) / 1000),
            "run_id": str(run_id),
            "window_start_ts": int(window_start_ts),
            "window_end_ts": int(window_end_ts),
            "symbol": str(symbol),
            "side": str(side),
            "intent": str(intent),
            "regime": regime,
            "router_action": "fill",
            "notional_usdt": float(notional),
            "mid_px": (float(mid) if mid is not None else None),
            "bid": (float(bid) if bid is not None else None),
            "ask": (float(ask) if ask is not None else None),
            "spread_bps": (float(spread_bps) if spread_bps is not None else None),
            "mid_source": ("submit" if meta_ts_ms is not None and mid is not None else ("snapshot" if mid is not None else None)),
            "fill_px": float(px),
            "mid_px_at_submit": (float(mid) if (meta_ts_ms is not None and mid is not None) else None),
            "mid_ts_ms": (int(meta_ts_ms) if meta_ts_ms is not None else None),
            "slippage_bps": (float(slip_bps) if slip_bps is not None else None),
            "fee_usdt": fee_usdt_val,
            "fee_bps": fee_bps_val,
            "cost_usdt_total": cost_usdt_total_val,
            "cost_bps_total": cost_bps_total_val,
            "deadband_pct": deadband_pct,
            "drift": drift,
        }
        append_cost_event(event)
        cost_event_written = True

    return ExportResult(trade_written=trade_written, cost_event_written=cost_event_written)
