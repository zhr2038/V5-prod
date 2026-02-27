from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np


def read_equity_jsonl(path: str) -> List[Dict[str, Any]]:
    """Read equity jsonl"""
    p = Path(path)
    if not p.exists():
        return []
    out = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def compute_equity_metrics(equity_rows: List[Dict[str, Any]], ann_factor: float = math.sqrt(24 * 365)) -> Dict[str, Any]:
    """Compute equity metrics"""
    if not equity_rows:
        return {
            "equity_start": None,
            "equity_end": None,
            "total_return_ratio": None,
            "total_return_pct": None,
            "max_drawdown_pct": None,
            "sharpe": None,
        }

    eq = np.array([float(r.get("equity") or 0.0) for r in equity_rows], dtype=float)
    eq_start = float(eq[0])
    eq_end = float(eq[-1])
    total_ret_ratio = (eq_end / eq_start - 1.0) if eq_start else 0.0

    peak = np.maximum.accumulate(eq)
    dd = np.where(peak > 0, 1.0 - eq / peak, 0.0)
    max_dd = float(np.max(dd))

    if len(eq) >= 3:
        rets = eq[1:] / eq[:-1] - 1.0
        sharpe = float(np.mean(rets) / (np.std(rets) + 1e-12) * ann_factor)
    else:
        sharpe = None

    return {
        "equity_start": eq_start,
        "equity_end": eq_end,
        # Keep both for clarity.
        "total_return_ratio": float(total_ret_ratio),
        "total_return_pct": float(total_ret_ratio) * 100.0,
        "max_drawdown_pct": float(max_dd) * 100.0,
        "sharpe": sharpe,
    }


def read_trades_csv(path: str) -> List[Dict[str, Any]]:
    """Read trades csv"""
    p = Path(path)
    if not p.exists():
        return []
    with p.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return [dict(row) for row in r]


def _to_f(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        s = str(x).strip()
        if s == "":
            return 0.0
        return float(s)
    except Exception:
        return 0.0


def _to_opt_f(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def compute_trade_metrics(trades: List[Dict[str, Any]], avg_equity: Optional[float] = None) -> Dict[str, Any]:
    """Compute trade metrics"""
    if not trades:
        return {
            "num_trades": 0,
            "turnover_ratio": 0.0,
            "fees_usdt_total": 0.0,
            "slippage_usdt_total": 0.0,
            "cost_usdt_total": 0.0,
            "cost_ratio": 0.0,
            "win_rate": None,
            "profit_factor": None,
        }

    notionals = [abs(_to_f(t.get("notional_usdt"))) for t in trades]
    fees = [_to_f(t.get("fee_usdt")) for t in trades]
    slp_opt = [_to_opt_f(t.get("slippage_usdt")) for t in trades]
    slp_vals = [x for x in slp_opt if x is not None]

    realized = [_to_f(t.get("realized_pnl_usdt")) for t in trades if str(t.get("realized_pnl_usdt") or "").strip() != ""]

    wins = [x for x in realized if x > 0]
    losses = [-x for x in realized if x < 0]

    win_rate = (len(wins) / len(realized)) if realized else None
    pf = (sum(wins) / (sum(losses) + 1e-12)) if realized else None

    turnover = float(sum(notionals)) / float(avg_equity or 1.0) if avg_equity else None

    fee_total = float(sum(fees))
    slp_total = float(sum(slp_vals))
    cost_total = fee_total + slp_total

    cost_ratio = float(cost_total) / float(avg_equity or 1.0) if avg_equity else None

    return {
        "num_trades": int(len(trades)),
        "num_round_trips": int(len(realized)),
        "turnover_ratio": turnover,
        "fees_usdt_total": fee_total,
        "slippage_usdt_total": slp_total,
        "slippage_coverage": (float(len(slp_vals)) / float(len(trades))) if trades else None,
        "cost_usdt_total": cost_total,
        "cost_ratio": cost_ratio,
        "win_rate": win_rate,
        "profit_factor": pf,
    }
