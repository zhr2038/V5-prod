#!/usr/bin/env python3
"""Compute dynamic alpha factor weights by regime from alpha_history.db.

Outputs reports/alpha_dynamic_weights_by_regime.json

- Uses Spearman RankIC between each factor and forward returns computed from market_data_1h
- Splits by alpha_snapshots.regime (string, e.g. Sideways/Risk-Off/Trending)
- Also produces an overall weight (all regimes pooled)

Usage:
  python3 scripts/compute_dynamic_alpha_weights_by_regime.py --lookback-days 30 --horizon 1h
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

DB_PATH = Path("reports/alpha_history.db")
OUT_PATH = Path("reports/alpha_dynamic_weights_by_regime.json")

FACTORS = [
    ("f1_mom_5d", "f1_mom_5d"),
    ("f2_mom_20d", "f2_mom_20d"),
    ("f3_vol_adj_ret_20d", "f3_vol_adj_ret_20d"),
    ("f4_volume_expansion", "f4_volume_expansion"),
    ("f5_rsi_trend_confirm", "f5_rsi_trend_confirm"),
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


def load_rows(con: sqlite3.Connection, *, since_ts: int) -> List[Tuple]:
    cols = [f for f, _ in FACTORS]
    sql = f"SELECT ts, symbol, COALESCE(regime,'Unknown') as regime, {', '.join(cols)} FROM alpha_snapshots WHERE ts >= ?"
    cur = con.cursor()
    cur.execute(sql, (int(since_ts),))
    return list(cur.fetchall())


def load_closes(con: sqlite3.Connection, *, since_ts: int, until_ts: int) -> Dict[Tuple[str, int], float]:
    cur = con.cursor()
    cur.execute(
        "SELECT symbol, timestamp, close FROM market_data_1h WHERE timestamp >= ? AND timestamp <= ?",
        (int(since_ts), int(until_ts)),
    )
    out: Dict[Tuple[str, int], float] = {}
    for sym, ts, close in cur.fetchall():
        try:
            out[(str(sym), int(ts))] = float(close)
        except Exception:
            pass
    return out


def compute_ic(rows: List[Tuple], closes: Dict[Tuple[str, int], float], horizon_sec: int) -> Tuple[Dict[str, float], int, int]:
    # group by ts
    by_ts: Dict[int, List[Tuple]] = defaultdict(list)
    for r in rows:
        by_ts[int(r[0])].append(r)

    ics: Dict[str, List[float]] = {f: [] for f, _ in FACTORS}
    used_points = 0
    used_timestamps = 0

    for ts, rs in by_ts.items():
        ys: List[float] = []
        xs_by_factor: Dict[str, List[float]] = {f: [] for f, _ in FACTORS}

        for r in rs:
            sym = str(r[1])
            c0 = closes.get((sym, ts))
            c1 = closes.get((sym, ts + int(horizon_sec)))
            if c0 is None or c1 is None or float(c0) <= 0:
                continue
            y = float(float(c1) / float(c0) - 1.0)
            ys.append(y)
            for i, (f, _) in enumerate(FACTORS):
                xs_by_factor[f].append(float(r[3 + i]))

        if len(ys) < 5:
            continue

        used_points += len(ys)
        used_timestamps += 1
        y_arr = np.array(ys, dtype=float)

        for f, _ in FACTORS:
            x_arr = np.array(xs_by_factor[f], dtype=float)
            ok = np.isfinite(x_arr) & np.isfinite(y_arr)
            if ok.sum() < 5:
                continue
            val = _spearman(x_arr[ok], y_arr[ok])
            if val is None or (val != val):
                continue
            ics[f].append(float(val))

    out = {f: (float(np.mean(xs)) if xs else 0.0) for f, xs in ics.items()}
    return out, int(used_points), int(used_timestamps)


def ic_to_weights(ic: Dict[str, float], *, floor: float = 0.05, cap: float = 0.60) -> Dict[str, float]:
    pos = {k: max(0.0, float(v)) for k, v in ic.items()}
    s = sum(pos.values())
    if s <= 0:
        n = len(FACTORS)
        return {k: 1.0 / n for k, _ in FACTORS}

    w = {k: v / s for k, v in pos.items()}
    w2 = {k: max(float(floor), float(v)) for k, v in w.items()}
    w2 = {k: min(float(cap), float(v)) for k, v in w2.items()}
    s2 = sum(w2.values())
    return {k: float(v) / float(s2) for k, v in w2.items()}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DB_PATH))
    ap.add_argument("--out", default=str(OUT_PATH))
    ap.add_argument("--lookback-days", type=int, default=30)
    ap.add_argument("--horizon", default="1h", choices=["1h", "4h", "12h", "24h"])
    ap.add_argument("--min-ts", type=int, default=None)
    args = ap.parse_args()

    db = Path(args.db)
    if not db.exists():
        print(f"missing db: {db}")
        return 2

    now = int(time.time())
    since_ts = int(args.min_ts) if args.min_ts is not None else int(now - int(args.lookback_days) * 86400)
    horizon_sec = {"1h": 3600, "4h": 4 * 3600, "12h": 12 * 3600, "24h": 24 * 3600}[str(args.horizon)]

    con = sqlite3.connect(str(db))
    rows = load_rows(con, since_ts=since_ts)
    closes = load_closes(con, since_ts=since_ts, until_ts=now + horizon_sec + 3600)
    con.close()

    # split by regime label (normalize legacy stringified objects to stable labels)
    def _norm_reg(x: str) -> str:
        s = str(x or "Unknown")
        if "Risk-Off" in s or "RISK_OFF" in s or "RISK_OFF" in s:
            return "Risk-Off"
        if "Sideways" in s or "SIDEWAYS" in s:
            return "Sideways"
        if "Trending" in s or "TRENDING" in s:
            return "Trending"
        return s

    by_reg: Dict[str, List[Tuple]] = defaultdict(list)
    for r in rows:
        by_reg[_norm_reg(str(r[2] or "Unknown"))].append(r)

    out: Dict[str, Any] = {
        "schema_version": 1,
        "generated_ts": int(now),
        "lookback_days": int(args.lookback_days),
        "horizon": str(args.horizon),
        "alpha_rows": int(len(rows)),
        "regimes": {},
    }

    # overall pooled
    ic_all, used_points_all, used_ts_all = compute_ic(rows, closes, horizon_sec)
    out["overall"] = {
        "ic_spearman_mean": ic_all,
        "weights": ic_to_weights(ic_all),
        "used_points": used_points_all,
        "used_timestamps": used_ts_all,
    }

    for reg, rs in sorted(by_reg.items(), key=lambda kv: kv[0]):
        ic, used_points, used_ts = compute_ic(rs, closes, horizon_sec)
        out["regimes"][reg] = {
            "alpha_rows": int(len(rs)),
            "ic_spearman_mean": ic,
            "weights": ic_to_weights(ic),
            "used_points": used_points,
            "used_timestamps": used_ts,
        }

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {outp}")
    print(json.dumps({"overall": out["overall"], "regimes": {k: v["weights"] for k, v in out["regimes"].items()}}, ensure_ascii=False)[:2000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
