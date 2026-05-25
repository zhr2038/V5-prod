from __future__ import annotations

import csv
import json
import math
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]

TRADE_EXPORT_SCHEMA_VERSION = "v5.trade_export.v1"
SUMMARY_METRICS_VERSION = "v5.summary_metrics.v1"


@dataclass
class TradeCsvReadResult:
    rows: List[Dict[str, Any]]
    file_exists: bool
    file_rows: int
    counted_rows: int
    warnings: List[str]
    parse_error: Optional[str] = None


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = (PROJECT_ROOT / resolved).resolve()
    return resolved


def read_equity_jsonl(path: str) -> List[Dict[str, Any]]:
    """Read equity jsonl"""
    p = _resolve_path(path)
    if not p.exists():
        return []
    out = []
    for idx, raw_line in enumerate(p.read_text(encoding="utf-8").splitlines()):
        line = raw_line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
            if isinstance(row, dict):
                row["_read_idx"] = idx
            out.append(row)
        except Exception:
            continue
    def _sort_key(item: Dict[str, Any]) -> tuple[int, str]:
        raw_ts = str(item.get("ts") or "").strip()
        if not raw_ts:
            return (1, "")
        normalized = raw_ts[:-1] + "+00:00" if raw_ts.endswith("Z") else raw_ts
        try:
            return (0, datetime.fromisoformat(normalized).isoformat())
        except Exception:
            return (1, raw_ts)
    out.sort(key=lambda item: (_sort_key(item), int(item.get("_read_idx", 0))))
    dedup: Dict[str, Dict[str, Any]] = {}
    for item in out:
        key = str(item.get("ts") or "").strip()
        if not key:
            continue
        cleaned = dict(item)
        cleaned.pop("_read_idx", None)
        dedup[key] = cleaned
    ordered_keys = sorted(dedup.keys(), key=lambda raw_ts: _sort_key({"ts": raw_ts}))
    return [dedup[key] for key in ordered_keys]


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
    p = _resolve_path(path)
    if not p.exists():
        return []
    with p.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return [dict(row) for row in r]


def read_trades_csv_detailed(path: str) -> TradeCsvReadResult:
    """Read trades.csv with validation metadata for run summaries.

    The legacy read_trades_csv helper intentionally returns raw DictReader rows.
    Summary/budget accounting needs stricter behavior: count only effective fill
    rows, surface parse problems, and derive notional from qty*price when needed.
    """

    p = _resolve_path(path)
    if not p.exists():
        return TradeCsvReadResult(
            rows=[],
            file_exists=False,
            file_rows=0,
            counted_rows=0,
            warnings=[],
        )

    rows: List[Dict[str, Any]] = []
    warnings: List[str] = []
    raw_rows = 0
    try:
        with p.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, strict=True)
            if reader.fieldnames is None:
                warnings.append("trades.csv has no header")
                return TradeCsvReadResult(
                    rows=[],
                    file_exists=True,
                    file_rows=0,
                    counted_rows=0,
                    warnings=warnings,
                    parse_error="missing_header",
                )

            fieldnames = {str(name or "").strip() for name in (reader.fieldnames or [])}
            if "notional_usdt" not in fieldnames and not {"qty", "price"}.issubset(fieldnames):
                warnings.append("trades.csv missing notional_usdt and qty/price columns")

            for line_no, row in enumerate(reader, start=2):
                raw_rows += 1
                if row is None:
                    warnings.append(f"trades.csv row {line_no} is empty or malformed")
                    continue
                if None in row:
                    warnings.append(f"trades.csv row {line_no} has extra columns")
                    continue

                cleaned = {str(k).strip(): v for k, v in row.items() if k is not None}
                if not any(str(value or "").strip() for value in cleaned.values()):
                    continue

                notional = _to_opt_f(cleaned.get("notional_usdt"))
                if notional is None or abs(float(notional)) <= 0.0:
                    qty = _to_opt_f(cleaned.get("qty"))
                    price = _to_opt_f(cleaned.get("price"))
                    if qty is not None and price is not None and abs(float(qty) * float(price)) > 0.0:
                        cleaned["notional_usdt"] = str(abs(float(qty) * float(price)))
                        notional = abs(float(qty) * float(price))

                if notional is None or abs(float(notional)) <= 0.0:
                    warnings.append(f"trades.csv row {line_no} not counted: missing positive notional")
                    continue

                if _to_opt_f(cleaned.get("fee_usdt")) is None:
                    warnings.append(f"trades.csv row {line_no} missing fee_usdt")
                if _to_opt_f(cleaned.get("slippage_usdt")) is None:
                    warnings.append(f"trades.csv row {line_no} missing slippage_usdt")

                rows.append(cleaned)
    except Exception as exc:
        msg = f"trades.csv parse failed: {exc!r}"
        warnings.append(msg)
        return TradeCsvReadResult(
            rows=[],
            file_exists=True,
            file_rows=raw_rows,
            counted_rows=0,
            warnings=warnings,
            parse_error=msg,
        )

    return TradeCsvReadResult(
        rows=rows,
        file_exists=True,
        file_rows=raw_rows,
        counted_rows=len(rows),
        warnings=warnings,
    )


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
            "trade_export_schema_version": TRADE_EXPORT_SCHEMA_VERSION,
            "summary_metrics_version": SUMMARY_METRICS_VERSION,
            "num_trades": 0,
            "fills_count_today": 0,
            "notional_usdt_total": 0.0,
            "turnover_usdt": 0.0,
            "turnover_ratio": 0.0,
            "fee_usdt_total": 0.0,
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

    notional_total = float(sum(notionals))
    fee_total = float(sum(fees))
    slp_total = float(sum(slp_vals))
    cost_total = fee_total + slp_total

    cost_ratio = float(cost_total) / float(avg_equity or 1.0) if avg_equity else None

    return {
        "trade_export_schema_version": TRADE_EXPORT_SCHEMA_VERSION,
        "summary_metrics_version": SUMMARY_METRICS_VERSION,
        "num_trades": int(len(trades)),
        "fills_count_today": int(len(trades)),
        "num_round_trips": int(len(realized)),
        "notional_usdt_total": notional_total,
        "turnover_usdt": notional_total,
        "turnover_ratio": turnover,
        "fee_usdt_total": fee_total,
        "fees_usdt_total": fee_total,
        "slippage_usdt_total": slp_total,
        "slippage_coverage": (float(len(slp_vals)) / float(len(trades))) if trades else None,
        "cost_usdt_total": cost_total,
        "cost_ratio": cost_ratio,
        "win_rate": win_rate,
        "profit_factor": pf,
    }
