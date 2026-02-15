from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

from src.reporting.trade_log import Fill, TradeLogWriter
from src.reporting.cost_events import append_cost_event


def _dec(x: Optional[str]) -> Decimal:
    if x is None or x == "":
        return Decimal("0")
    return Decimal(str(x))


def inst_id_to_symbol(inst_id: str) -> str:
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
    trade_written: bool
    cost_event_written: bool


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
            slippage_usdt=0.0,
            realized_pnl_usdt=None,
            realized_pnl_pct=None,
        )
    )

    trade_written = True

    # Cost event (requires window_start_ts)
    cost_event_written = False
    if window_start_ts is not None and window_end_ts is not None:
        event: Dict[str, Any] = {
            "schema_version": 1,
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
            "mid_px": None,
            "bid": None,
            "ask": None,
            "spread_bps": None,
            "fill_px": float(px),
            "slippage_bps": None,
            "fee_usdt": float(fee_usdt) if fee_cost is not None else None,
            "fee_bps": None,
            "cost_usdt_total": float(fee_usdt) if fee_cost is not None else None,
            "cost_bps_total": None,
            "deadband_pct": deadband_pct,
            "drift": drift,
        }
        append_cost_event(event)
        cost_event_written = True

    return ExportResult(trade_written=trade_written, cost_event_written=cost_event_written)
