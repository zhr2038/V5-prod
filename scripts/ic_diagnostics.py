#!/usr/bin/env python3
"""IC diagnostics (research) to debug whether alpha/IC is meaningful.

Outputs a JSON report with:
- sample coverage (rows, non-null forward returns)
- overall RankIC by factor (mean + quantiles) for raw and tradable subsets
- RankIC by regime
- per-symbol RankIC summary (top/bottom)

Tradable subset is approximated by:
- instrument minSz (base) * close_px <= target_notional_usdt / slack

This script only reads alpha_history.db + OKX instrument specs cache.
It does NOT affect live trading.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

# allow running from repo root
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.data.okx_instruments import OKXSpotInstrumentsCache
from src.execution.live_execution_engine import symbol_to_inst_id

DB_PATH = Path("reports/alpha_history.db")

FACTORS = [
    "f1_mom_5d",
    "f2_mom_20d",
    "f3_vol_adj_ret_20d",
    "f4_volume_expansion",
    "f5_rsi_trend_confirm",
]


def _rank(x: np.ndarray) -> np.ndarray:
    temp = x.argsort(kind="mergesort")
    ranks = np.empty_like(temp, dtype=float)
    ranks[temp] = np.arange(len(x), dtype=float)

    xs = x[temp]
    i = 0
    while i < len(xs):
        j = i
        while j + 1 < len(xs) and xs[j + 1] == xs[i]:
            j += 1
        if j > i:
            avg = (i + j) / 2.0
            ranks[temp[i : j + 1]] = avg
        i = j + 1
    return ranks


def _spearman(a: np.ndarray, b: np.ndarray) -> Optional[float]:
    if len(a) < 5 or len(b) < 5:
        return None
    ra = _rank(a)
    rb = _rank(b)
    ra = ra - ra.mean()
    rb = rb - rb.mean()
    denom = float(np.sqrt((ra * ra).sum()) * np.sqrt((rb * rb).sum()))
    if denom <= 0:
        return None
    return float((ra * rb).sum() / denom)


def _quantiles(xs: List[float]) -> Dict[str, Optional[float]]:
    if not xs:
        return {"count": 0, "mean": None, "p25": None, "p50": None, "p75": None, "p90": None}
    ys = sorted(float(x) for x in xs)
    n = len(ys)

    def q(p: float) -> float:
        k = max(0, min(n - 1, int(round(p * (n - 1)))))
        return float(ys[k])

    return {
        "count": n,
        "mean": float(sum(ys) / n),
        "p25": q(0.25),
        "p50": q(0.50),
        "p75": q(0.75),
        "p90": q(0.90),
    }


@dataclass
class Row:
    ts: int
    symbol: str
    regime: str
    close_px: float
    fwd_ret: Optional[float]
    factors: Dict[str, float]


def load_rows(con: sqlite3.Connection, *, since_ts: int, horizon_col: str) -> List[Row]:
    """Load alpha rows.

    NOTE: forward-return columns in alpha_snapshots may be missing/zero; we load them only
    for diagnostics, but we will compute forward returns from market_data_1h closes.
    """
    cols = ", ".join(FACTORS)
    sql = (
        f"SELECT ts, symbol, COALESCE(regime,'Unknown') as regime, "
        f"(SELECT close FROM market_data_1h m WHERE m.symbol=alpha_snapshots.symbol AND m.timestamp=alpha_snapshots.ts) as close_px, "
        f"{horizon_col} as fwd_ret_col, {cols} "
        f"FROM alpha_snapshots WHERE ts >= ?"
    )
    cur = con.cursor()
    cur.execute(sql, (int(since_ts),))
    out: List[Row] = []
    for r in cur.fetchall():
        ts = int(r[0])
        sym = str(r[1])
        regime = str(r[2] or "Unknown")
        try:
            close_px = float(r[3] or 0.0)
        except Exception:
            close_px = 0.0

        fwd = r[4]
        try:
            fwd_ret = (float(fwd) if fwd is not None else None)
        except Exception:
            fwd_ret = None

        facs: Dict[str, float] = {}
        for i, f in enumerate(FACTORS):
            try:
                facs[f] = float(r[5 + i])
            except Exception:
                facs[f] = float("nan")
        out.append(Row(ts=ts, symbol=sym, regime=regime, close_px=close_px, fwd_ret=fwd_ret, factors=facs))
    return out


def is_tradable(sym: str, *, close_px: float, target_notional_usdt: float, slack: float) -> bool:
    if close_px <= 0:
        return False
    try:
        inst = symbol_to_inst_id(sym)
        spec = OKXSpotInstrumentsCache().get_spec(inst)
        if spec is None:
            return False
        min_sz = float(spec.min_sz or 0.0)
        if min_sz <= 0:
            return False
        min_notional = float(min_sz) * float(close_px)
        return float(target_notional_usdt) >= float(min_notional) * float(slack)
    except Exception:
        return False


def compute_ic_panel(rows: List[Row], *, closes: Dict[Tuple[str, int], float], horizon_sec: int) -> Dict[str, Any]:
    # group by ts cross-section
    by_ts: Dict[int, List[Row]] = defaultdict(list)
    for r in rows:
        by_ts[int(r.ts)].append(r)

    per_factor_ics: Dict[str, List[float]] = {f: [] for f in FACTORS}
    used_points = 0
    used_timestamps = 0
    missing_close = 0

    for ts, rs in by_ts.items():
        ys: List[float] = []
        rs2: List[Row] = []
        for x in rs:
            c0 = closes.get((x.symbol, int(ts)))
            c1 = closes.get((x.symbol, int(ts) + int(horizon_sec)))
            if c0 is None or c1 is None or float(c0) <= 0:
                missing_close += 1
                continue
            ys.append(float(float(c1) / float(c0) - 1.0))
            rs2.append(x)

        if len(ys) < 5:
            continue

        y_arr = np.array(ys, dtype=float)
        used_points += int(len(ys))
        used_timestamps += 1

        for f in FACTORS:
            xs = np.array([float(x.factors.get(f)) for x in rs2], dtype=float)
            ok = np.isfinite(xs) & np.isfinite(y_arr)
            if ok.sum() < 5:
                continue
            v = _spearman(xs[ok], y_arr[ok])
            if v is None or (v != v):
                continue
            per_factor_ics[f].append(float(v))

    out = {
        "used_points": used_points,
        "used_timestamps": used_timestamps,
        "missing_close_pairs": int(missing_close),
        "ic": {f: _quantiles(xs) for f, xs in per_factor_ics.items()},
    }
    return out


def compute_ic_by_regime(rows: List[Row], *, closes: Dict[Tuple[str, int], float], horizon_sec: int) -> Dict[str, Any]:
    by_reg: Dict[str, List[Row]] = defaultdict(list)
    for r in rows:
        by_reg[str(r.regime or "Unknown")].append(r)
    out: Dict[str, Any] = {}
    for reg, rs in by_reg.items():
        out[reg] = compute_ic_panel(rs, closes=closes, horizon_sec=horizon_sec)
    return out


def compute_ic_by_symbol(rows: List[Row]) -> Dict[str, Any]:
    # per-symbol correlation over time (not cross-sectional)
    # Use factor score vs fwd_ret time series.
    by_sym: Dict[str, List[Row]] = defaultdict(list)
    for r in rows:
        if r.fwd_ret is None:
            continue
        by_sym[r.symbol].append(r)

    stats: Dict[str, Dict[str, float]] = {}
    for sym, rs in by_sym.items():
        ys = np.array([float(x.fwd_ret) for x in rs if x.fwd_ret is not None], dtype=float)
        if len(ys) < 20:
            continue
        for f in FACTORS:
            xs = np.array([float(x.factors.get(f)) for x in rs], dtype=float)
            ok = np.isfinite(xs) & np.isfinite(ys)
            if ok.sum() < 20:
                continue
            v = _spearman(xs[ok], ys[ok])
            if v is None:
                continue
            stats.setdefault(sym, {})[f] = float(v)

    # rank by mom20d
    keyf = "f2_mom_20d"
    items = [(sym, d.get(keyf)) for sym, d in stats.items() if d.get(keyf) is not None]
    items.sort(key=lambda x: float(x[1]), reverse=True)
    return {
        "symbols": len(stats),
        "key_factor": keyf,
        "top10": items[:10],
        "bottom10": items[-10:],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DB_PATH))
    ap.add_argument("--lookback-days", type=int, default=30)
    ap.add_argument("--horizon", default="1h", choices=["1h", "4h", "12h", "24h", "72h"])
    ap.add_argument("--target-notional-usdt", type=float, default=20.0)
    ap.add_argument("--slack", type=float, default=1.05)
    ap.add_argument("--out", default="reports/ic_diagnostics_30d.json")
    args = ap.parse_args()

    db = Path(args.db)
    if not db.exists():
        print(f"missing db: {db}")
        return 2

    now = int(time.time())
    since_ts = int(now - int(args.lookback_days) * 86400)
    horizon_map = {
        "1h": ("fwd_ret_1h", 3600),
        "4h": ("fwd_ret_4h", 4 * 3600),
        "12h": ("fwd_ret_12h", 12 * 3600),
        "24h": ("fwd_ret_24h", 24 * 3600),
        "72h": ("fwd_ret_72h", 72 * 3600),
    }
    horizon_col, horizon_sec = horizon_map[str(args.horizon)]

    con = sqlite3.connect(str(db))
    rows0 = load_rows(con, since_ts=since_ts, horizon_col=horizon_col)
    con.close()

    # Load closes for forward-return computation (same approach as compute_dynamic_alpha_weights.py)
    until_ts = int(now + horizon_sec + 3600)
    con = sqlite3.connect(str(db))
    cur = con.cursor()
    cur.execute(
        "SELECT symbol, timestamp, close FROM market_data_1h WHERE timestamp >= ? AND timestamp <= ?",
        (int(since_ts), int(until_ts)),
    )
    closes: Dict[Tuple[str, int], float] = {}
    for sym, ts, close in cur.fetchall():
        try:
            closes[(str(sym), int(ts))] = float(close)
        except Exception:
            pass
    con.close()

    # basic coverage (forward-return column health)
    total = len(rows0)
    fwd_nonnull = [r.fwd_ret for r in rows0 if r.fwd_ret is not None]
    has_fwd_col = int(len(fwd_nonnull))
    fwd_col_distinct = int(len(set(float(x) for x in fwd_nonnull))) if fwd_nonnull else 0
    has_close = sum(1 for r in rows0 if float(r.close_px) > 0)

    # tradable subset (by minSz*close)
    tradable_rows = [
        r
        for r in rows0
        if is_tradable(r.symbol, close_px=float(r.close_px), target_notional_usdt=float(args.target_notional_usdt), slack=float(args.slack))
    ]

    raw_rows = list(rows0)

    report: Dict[str, Any] = {
        "schema_version": 2,
        "generated_ts": int(now),
        "lookback_days": int(args.lookback_days),
        "horizon": str(args.horizon),
        "target_notional_usdt": float(args.target_notional_usdt),
        "slack": float(args.slack),
        "coverage": {
            "rows_total": int(total),
            "rows_with_fwd_ret_col": int(has_fwd_col),
            "fwd_ret_col_distinct": int(fwd_col_distinct),
            "rows_with_close": int(has_close),
            "rows_tradable": int(len(tradable_rows)),
            "closes_points": int(len(closes)),
        },
        "overall_raw": compute_ic_panel(raw_rows, closes=closes, horizon_sec=horizon_sec),
        "overall_tradable": compute_ic_panel(tradable_rows, closes=closes, horizon_sec=horizon_sec),
        "by_regime_raw": compute_ic_by_regime(raw_rows, closes=closes, horizon_sec=horizon_sec),
        "by_regime_tradable": compute_ic_by_regime(tradable_rows, closes=closes, horizon_sec=horizon_sec),
        "by_symbol": compute_ic_by_symbol(raw_rows),
    }

    outp = Path(str(args.out))
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {outp}")
    print(json.dumps({"coverage": report["coverage"], "overall_tradable": report["overall_tradable"]["ic"]["f2_mom_20d"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
