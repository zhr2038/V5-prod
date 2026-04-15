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

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_run_dir(run_dir: str | Path) -> Path:
    resolved = Path(run_dir)
    if not resolved.is_absolute():
        resolved = (PROJECT_ROOT / resolved).resolve()
    return resolved


def write_summary(
    run_dir: str,
    window_start_ts: int | None = None,
    window_end_ts: int | None = None,
) -> Dict[str, Any]:
    rd = _resolve_run_dir(run_dir)
    eq_rows = read_equity_jsonl(str(rd / "equity.jsonl"))
    trades = read_trades_csv(str(rd / "trades.csv"))

    avg_equity = None
    if eq_rows:
        xs = [float(r.get("equity") or 0.0) for r in eq_rows]
        avg_equity = sum(xs) / len(xs) if xs else None

    eqm = compute_equity_metrics(eq_rows)
    tm = compute_trade_metrics(trades, avg_equity=avg_equity)
    
    # 确定窗口时间：优先使用传入的窗口，否则使用equity.jsonl范围
    equity_first_ts = eq_rows[0].get("ts") if eq_rows else None
    equity_last_ts = eq_rows[-1].get("ts") if eq_rows else None
    
    start_ts = equity_first_ts
    end_ts = equity_last_ts
    
    # 若main传了窗口，则覆盖（窗口语义优先）
    if window_start_ts is not None and window_end_ts is not None:
        start_ts = window_start_ts
        end_ts = window_end_ts

    summ: Dict[str, Any] = {
        "run_id": rd.name,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "window_start_ts": window_start_ts,
        "window_end_ts": window_end_ts,
        "avg_equity": avg_equity,
        **eqm,
        **tm,
    }

    (rd / "summary.json").write_text(json.dumps(summ, ensure_ascii=False, indent=2), encoding="utf-8")
    return summ


def refresh_summary_metrics(run_dir: str) -> Dict[str, Any]:
    """Recompute trade/equity metrics from current trades.csv + equity.jsonl.

    Used for live finalize: fills/trades may arrive after the initial summary was written.
    This function patches summary.json in-place while preserving unrelated fields (e.g. budget).
    """

    rd = _resolve_run_dir(run_dir)
    p = rd / "summary.json"
    if not p.exists():
        # create from scratch
        return write_summary(run_dir)

    summ = json.loads(p.read_text(encoding="utf-8"))

    eq_rows = read_equity_jsonl(str(rd / "equity.jsonl"))
    trades = read_trades_csv(str(rd / "trades.csv"))

    avg_equity = None
    if eq_rows:
        xs = [float(r.get("equity") or 0.0) for r in eq_rows]
        avg_equity = sum(xs) / len(xs) if xs else None

    eqm = compute_equity_metrics(eq_rows)
    tm = compute_trade_metrics(trades, avg_equity=avg_equity)

    # patch
    summ["avg_equity"] = avg_equity
    for k, v in {**eqm, **tm}.items():
        summ[k] = v

    p.write_text(json.dumps(summ, ensure_ascii=False, indent=2), encoding="utf-8")
    return summ


def attach_budget(run_dir: str, budget: Dict[str, Any]) -> Dict[str, Any]:
    """Patch run_dir/summary.json with a top-level 'budget' field."""
    rd = _resolve_run_dir(run_dir)
    p = rd / "summary.json"
    if not p.exists():
        raise FileNotFoundError(str(p))
    summ = json.loads(p.read_text(encoding="utf-8"))
    summ["budget"] = budget
    p.write_text(json.dumps(summ, ensure_ascii=False, indent=2), encoding="utf-8")
    return summ


def attach_exit_signals(run_dir: str, exit_signals: list[dict[str, Any]]) -> Dict[str, Any]:
    """Patch run_dir/summary.json with a top-level 'exit_signals' field."""
    rd = _resolve_run_dir(run_dir)
    p = rd / "summary.json"
    if not p.exists():
        raise FileNotFoundError(str(p))
    summ = json.loads(p.read_text(encoding="utf-8"))
    summ["exit_signals"] = exit_signals
    p.write_text(json.dumps(summ, ensure_ascii=False, indent=2), encoding="utf-8")
    return summ

