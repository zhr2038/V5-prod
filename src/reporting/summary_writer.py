from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from src.reporting.metrics import (
    compute_equity_metrics,
    compute_trade_metrics,
    read_equity_jsonl,
    read_trades_csv,
)


def write_summary(run_dir: str) -> Dict[str, Any]:
    rd = Path(run_dir)
    eq_rows = read_equity_jsonl(str(rd / "equity.jsonl"))
    trades = read_trades_csv(str(rd / "trades.csv"))

    avg_equity = None
    if eq_rows:
        xs = [float(r.get("equity") or 0.0) for r in eq_rows]
        avg_equity = sum(xs) / len(xs) if xs else None

    eqm = compute_equity_metrics(eq_rows)
    tm = compute_trade_metrics(trades, avg_equity=avg_equity)

    summ: Dict[str, Any] = {
        "run_id": rd.name,
        "start_ts": (eq_rows[0].get("ts") if eq_rows else None),
        "end_ts": (eq_rows[-1].get("ts") if eq_rows else None),
        "avg_equity": avg_equity,
        **eqm,
        **tm,
    }

    (rd / "summary.json").write_text(json.dumps(summ, ensure_ascii=False, indent=2), encoding="utf-8")
    return summ
