#!/usr/bin/env python3
"""Profitability report for V5 runs.

Goal: Answer "did we make money?" using the most reliable artifacts available.

Data sources (in priority order):
- reports/runs/*/summary.json: window boundaries + equity_start/equity_end when present.
- reports/runs/*/trades.csv: per-fill fee/slippage + (optional) realized pnl.

Important notes:
- Some legacy summaries use the field name total_return_pct but store a *ratio* (e.g. 0.0123)
  or even a *multiple* (e.g. 4.46 means +446%). This script detects and normalizes.
- If num_trades==0, summary returns can be misleading (equity_start may be a sizing cap).

Usage examples:
  python3 scripts/profitability_report.py --hours 24
  python3 scripts/profitability_report.py --days 7
  python3 scripts/profitability_report.py --since-run 20260222_00
  python3 scripts/profitability_report.py --list-runs --limit 20
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


RUNS_DIR = Path("reports/runs")


def _dt_utc_from_epoch(ts: int) -> datetime:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc)


def _fmt_ts(ts: Optional[int]) -> str:
    if ts is None:
        return "-"
    try:
        return _dt_utc_from_epoch(int(ts)).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(ts)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or x == "":
            return float(default)
        return float(x)
    except Exception:
        return float(default)


@dataclass
class RunRow:
    run_id: str
    path: Path
    window_start_ts: Optional[int]
    window_end_ts: Optional[int]
    num_trades: int
    equity_start: Optional[float]
    equity_end: Optional[float]
    total_return_raw: Optional[float]
    max_drawdown_pct: Optional[float]
    sharpe: Optional[float]
    fees_usdt_total: Optional[float]
    slippage_usdt_total: Optional[float]
    cost_usdt_total: Optional[float]


def load_summary(path: Path) -> Optional[RunRow]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

    run_id = str(obj.get("run_id") or path.parent.name)
    ws = obj.get("window_start_ts")
    we = obj.get("window_end_ts")

    row = RunRow(
        run_id=run_id,
        path=path.parent,
        window_start_ts=(int(ws) if ws is not None else None),
        window_end_ts=(int(we) if we is not None else None),
        num_trades=int(obj.get("num_trades") or 0),
        equity_start=(float(obj.get("equity_start")) if obj.get("equity_start") is not None else None),
        equity_end=(float(obj.get("equity_end")) if obj.get("equity_end") is not None else None),
        total_return_raw=(float(obj.get("total_return_pct")) if obj.get("total_return_pct") is not None else None),
        max_drawdown_pct=(float(obj.get("max_drawdown_pct")) if obj.get("max_drawdown_pct") is not None else None),
        sharpe=(float(obj.get("sharpe")) if obj.get("sharpe") is not None else None),
        fees_usdt_total=(float(obj.get("fees_usdt_total")) if obj.get("fees_usdt_total") is not None else None),
        slippage_usdt_total=(float(obj.get("slippage_usdt_total")) if obj.get("slippage_usdt_total") is not None else None),
        cost_usdt_total=(float(obj.get("cost_usdt_total")) if obj.get("cost_usdt_total") is not None else None),
    )
    return row


def list_runs() -> List[RunRow]:
    if not RUNS_DIR.exists():
        return []

    rows: List[RunRow] = []
    for p in RUNS_DIR.glob("*/summary.json"):
        r = load_summary(p)
        if r is not None:
            rows.append(r)

    # sort by window_end_ts or mtime
    def key(r: RunRow):
        if r.window_end_ts is not None:
            return int(r.window_end_ts)
        try:
            return int((r.path / "summary.json").stat().st_mtime)
        except Exception:
            return 0

    rows.sort(key=key)
    return rows


def normalize_total_return(row: RunRow) -> Tuple[Optional[float], str]:
    """Return (return_ratio, note).

    return_ratio is expressed as a *ratio* (e.g. 0.01 means +1%).
    """
    if row.total_return_raw is None:
        return None, "missing"

    x = float(row.total_return_raw)

    # If equity_start/end available, prefer them.
    if row.equity_start is not None and row.equity_end is not None and row.equity_start > 0:
        r = float(row.equity_end) / float(row.equity_start) - 1.0
        # sanity check against x; but always use equity-based.
        return r, "equity_based"

    # Heuristic for legacy:
    # - tiny values like 1e-5 are already ratios (0.001%)
    # - values between -1 and 1 likely ratios
    # - values like 4.46 likely multiples (+446%)
    if -1.0 <= x <= 1.0:
        return x, "raw_ratio"

    # If x looks like a percent already (e.g. 2.5 meaning 2.5%), we can't know.
    # But in our observed data, 4.46 came from equity_end/equity_start-1 (multiple).
    # Treat >1 as multiple ratio.
    return x, "raw_multiple"


def read_trades_csv(run_dir: Path) -> List[Dict[str, Any]]:
    p = run_dir / "trades.csv"
    if not p.exists():
        return []
    out: List[Dict[str, Any]] = []
    with p.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            out.append(row)
    return out


def summarize_trades(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    fee = 0.0
    slip = 0.0
    notional = 0.0
    realized = 0.0
    realized_present = 0

    by_symbol: Dict[str, Dict[str, float]] = {}

    for t in trades:
        sym = str(t.get("symbol") or "")
        n = _safe_float(t.get("notional_usdt"), 0.0)
        f = _safe_float(t.get("fee_usdt"), 0.0)
        s = _safe_float(t.get("slippage_usdt"), 0.0)
        r = t.get("realized_pnl_usdt")
        rp = _safe_float(r, 0.0) if (r is not None and r != "") else 0.0

        notional += abs(n)
        fee += abs(f)
        slip += abs(s)

        if r is not None and r != "":
            realized += float(rp)
            realized_present += 1

        if sym:
            d = by_symbol.setdefault(sym, {"notional": 0.0, "fee": 0.0, "slip": 0.0, "realized": 0.0, "fills": 0.0})
            d["notional"] += abs(n)
            d["fee"] += abs(f)
            d["slip"] += abs(s)
            if r is not None and r != "":
                d["realized"] += float(rp)
            d["fills"] += 1.0

    return {
        "fills": len(trades),
        "notional_usdt": notional,
        "fees_usdt": fee,
        "slippage_usdt": slip,
        "cost_usdt": fee + slip,
        "realized_pnl_usdt": (realized if realized_present else None),
        "by_symbol": by_symbol,
    }


def pick_rows(rows: List[RunRow], *, since_ts: Optional[int], hours: Optional[float], days: Optional[float], since_run: Optional[str]) -> List[RunRow]:
    if not rows:
        return []

    if since_run:
        # include from that run_id (inclusive)
        for i, r in enumerate(rows):
            if r.run_id == since_run:
                return rows[i:]
        return rows

    end_ts = rows[-1].window_end_ts or int(time.time())
    if since_ts is not None:
        start_ts = int(since_ts)
    elif hours is not None:
        start_ts = int(end_ts - float(hours) * 3600)
    elif days is not None:
        start_ts = int(end_ts - float(days) * 86400)
    else:
        # default last 24h
        start_ts = int(end_ts - 24 * 3600)

    out: List[RunRow] = []
    for r in rows:
        we = r.window_end_ts
        if we is None:
            continue
        if int(we) >= int(start_ts):
            out.append(r)
    return out


def print_report(rows: List[RunRow], *, limit_symbols: int = 8) -> int:
    if not rows:
        print("No runs found under reports/runs")
        return 1

    ws0 = rows[0].window_start_ts
    we1 = rows[-1].window_end_ts
    print(f"Runs: {len(rows)}")
    print(f"Window: {ws0} → {we1}  ({_fmt_ts(ws0)} → {_fmt_ts(we1)})")

    # Summaries
    total_trades = sum(int(r.num_trades or 0) for r in rows)

    # Return estimate: sum of ratios is NOT correct; but we can present:
    # - equity-based compounding if equity points are consistent across runs (often not)
    # - per-run ratio list, flag suspicious ones
    ratios: List[float] = []
    suspicious: List[Tuple[str, float, str]] = []

    for r in rows:
        rr, note = normalize_total_return(r)
        if rr is None:
            continue
        ratios.append(float(rr))
        if (r.num_trades or 0) == 0 and abs(float(rr)) > 0.02:
            suspicious.append((r.run_id, float(rr), "num_trades=0 but return_large"))
        if abs(float(rr)) > 1.0:
            suspicious.append((r.run_id, float(rr), "return_ratio_abs_gt_100%"))

    # Trades aggregation
    agg = {"fills": 0, "notional_usdt": 0.0, "fees_usdt": 0.0, "slippage_usdt": 0.0, "cost_usdt": 0.0, "realized_pnl_usdt": None, "by_symbol": {}}
    for r in rows:
        trades = read_trades_csv(r.path)
        s = summarize_trades(trades)
        agg["fills"] += int(s["fills"])
        agg["notional_usdt"] += float(s["notional_usdt"])
        agg["fees_usdt"] += float(s["fees_usdt"])
        agg["slippage_usdt"] += float(s["slippage_usdt"])
        agg["cost_usdt"] += float(s["cost_usdt"])

        if s.get("realized_pnl_usdt") is not None:
            if agg["realized_pnl_usdt"] is None:
                agg["realized_pnl_usdt"] = 0.0
            agg["realized_pnl_usdt"] += float(s["realized_pnl_usdt"])

        for sym, d in (s.get("by_symbol") or {}).items():
            bd = agg["by_symbol"].setdefault(sym, {"fills": 0.0, "notional": 0.0, "fee": 0.0, "slip": 0.0, "realized": 0.0})
            bd["fills"] += float(d.get("fills") or 0.0)
            bd["notional"] += float(d.get("notional") or 0.0)
            bd["fee"] += float(d.get("fee") or 0.0)
            bd["slip"] += float(d.get("slip") or 0.0)
            bd["realized"] += float(d.get("realized") or 0.0)

    print("\nTrade-derived totals (from trades.csv):")
    print(f"  fills: {agg['fills']}")
    print(f"  notional_usdt: {agg['notional_usdt']:.4f}")
    print(f"  fees_usdt: {agg['fees_usdt']:.6f}")
    print(f"  slippage_usdt: {agg['slippage_usdt']:.6f}")
    print(f"  cost_usdt_total: {agg['cost_usdt']:.6f}")
    if agg["realized_pnl_usdt"] is not None:
        print(f"  realized_pnl_usdt: {float(agg['realized_pnl_usdt']):.6f}")
    else:
        print("  realized_pnl_usdt: N/A (not exported)")

    if agg["notional_usdt"] > 0:
        print(f"  cost_bps: {agg['cost_usdt'] / agg['notional_usdt'] * 1e4:.3f} bps")

    # Top symbols by notional
    bs = list(agg["by_symbol"].items())
    bs.sort(key=lambda kv: float(kv[1].get("notional") or 0.0), reverse=True)
    if bs:
        print("\nTop symbols (by notional):")
        for sym, d in bs[: int(limit_symbols)]:
            n = float(d.get("notional") or 0.0)
            fee = float(d.get("fee") or 0.0)
            slip = float(d.get("slip") or 0.0)
            fills = int(d.get("fills") or 0)
            line = f"  {sym}: fills={fills} notional={n:.2f} fee={fee:.4f} slip={slip:.4f}"
            if n > 0:
                line += f" cost_bps={(fee+slip)/n*1e4:.2f}"
            print(line)

    print("\nSummary return sanity:")
    if ratios:
        # show distribution (not compounded)
        avg = sum(ratios) / len(ratios)
        print(f"  return_ratio avg={avg:.6f} min={min(ratios):.6f} max={max(ratios):.6f} (NOTE: not compounded)")
    else:
        print("  no usable total_return in summaries")

    if suspicious:
        print("\nSuspicious summary rows (likely metric/denominator mismatch):")
        for rid, rr, why in suspicious[:20]:
            print(f"  {rid}: return_ratio={rr:.6f} ({why})")

    # Simple profitability verdict:
    # - If realized pnl exists, use it.
    # - Otherwise, cannot prove profitability; we can only show costs and activity.
    print("\nVerdict:")
    if agg["realized_pnl_usdt"] is not None:
        pnl = float(agg["realized_pnl_usdt"])
        net = pnl - float(agg["cost_usdt"])
        print(f"  realized_pnl_usdt={pnl:.6f}, costs={agg['cost_usdt']:.6f}, net_after_costs={net:.6f}")
        print("  (This is realized PnL only; does not include unrealized PnL of open positions.)")
    else:
        print("  Cannot conclude profitability from reports alone: realized_pnl is not exported in live fills.")
        print("  What we *can* say: execution costs are being recorded; IC/alpha history can be evaluated.")

    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=None)
    ap.add_argument("--days", type=float, default=None)
    ap.add_argument("--since-ts", type=int, default=None)
    ap.add_argument("--since-run", type=str, default=None)
    ap.add_argument("--list-runs", action="store_true")
    ap.add_argument("--limit", type=int, default=40)
    ap.add_argument("--limit-symbols", type=int, default=8)
    args = ap.parse_args()

    rows = list_runs()
    if args.list_runs:
        rows2 = rows[-int(args.limit) :] if args.limit and len(rows) > args.limit else rows
        for r in rows2:
            rr, note = normalize_total_return(r)
            print(
                f"{r.run_id}  win={r.window_start_ts}->{r.window_end_ts}  trades={r.num_trades}  return={('%.6f'%rr) if rr is not None else '-'} ({note})  path={r.path}"
            )
        return 0

    picked = pick_rows(rows, since_ts=args.since_ts, hours=args.hours, days=args.days, since_run=args.since_run)
    return print_report(picked, limit_symbols=int(args.limit_symbols))


if __name__ == "__main__":
    raise SystemExit(main())
