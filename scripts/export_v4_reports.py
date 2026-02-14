from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, List

import numpy as np

# allow running as a script from repo root
sys.path.append(str(Path(__file__).resolve().parents[1]))


def _read_jsonl(p: Path) -> List[Dict[str, Any]]:
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
    if not equity_rows:
        return {
            "equity_start": None,
            "equity_end": None,
            "total_return_pct": None,
            "max_drawdown_pct": None,
            "sharpe": None,
        }

    eq = np.array([float(r.get("equity") or 0.0) for r in equity_rows], dtype=float)
    eq_start = float(eq[0])
    eq_end = float(eq[-1])
    total_ret = (eq_end / eq_start - 1.0) if eq_start else 0.0

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
        "total_return_pct": float(total_ret),
        "max_drawdown_pct": float(max_dd),
        "sharpe": sharpe,
    }


def export_v4(v4_reports_dir: str, out_dir: str) -> None:
    src = Path(v4_reports_dir)
    dst = Path(out_dir)
    dst.mkdir(parents=True, exist_ok=True)

    # -----------------
    # equity snapshots
    # -----------------
    eq_path = src / "equity_snapshots.jsonl"
    equity_rows: List[Dict[str, Any]] = []
    if eq_path.exists():
        for r in _read_jsonl(eq_path):
            # best-effort normalize
            ts = r.get("ts") or r.get("timestamp") or r.get("time")
            equity = r.get("equity")
            if equity is None:
                equity = r.get("total_equity")
            if equity is None:
                equity = r.get("equity_usdt")
            if equity is None:
                equity = r.get("totalEq")
            if equity is None:
                continue
            equity_rows.append({"ts": ts, "equity": equity})

        # write v5-compatible equity.jsonl
        if equity_rows:
            (dst / "equity.jsonl").write_text(
                "\n".join([json.dumps(x, ensure_ascii=False) for x in equity_rows]) + "\n",
                encoding="utf-8",
            )

    # -----------------
    # trades
    # -----------------
    # Prefer trade_reflections (has realized_pnl/pnl_pct), fallback to trades_*.jsonl
    candidates = sorted(src.glob("trade_reflections_*.jsonl"), key=lambda x: x.stat().st_mtime, reverse=True)
    trades: List[Dict[str, Any]] = []
    if candidates:
        rows = _read_jsonl(candidates[0])
        for r in rows:
            sym = r.get("symbol") or ""
            side = r.get("side") or ""
            qty = r.get("qty") or ""
            price = r.get("price") or ""
            notional = ""
            try:
                if qty and price:
                    notional = float(qty) * float(price)
            except Exception:
                notional = ""

            realized = r.get("realized_pnl")
            pnl_pct = r.get("pnl_pct")
            # entry_recorded buy events may have no realized
            trades.append(
                {
                    "ts": r.get("ts") or r.get("timestamp") or "",
                    "run_id": "v4",
                    "symbol": sym,
                    "intent": ("OPEN_LONG" if side == "buy" else "CLOSE_LONG"),
                    "side": side,
                    "qty": qty,
                    "price": price,
                    "notional_usdt": notional,
                    "fee_usdt": 0,
                    "slippage_usdt": 0,
                    "realized_pnl_usdt": "" if realized is None else realized,
                    "realized_pnl_pct": "" if pnl_pct is None else pnl_pct,
                }
            )
    else:
        # fallback trades_*.jsonl
        cand2 = sorted(src.glob("trades_*.jsonl"), key=lambda x: x.stat().st_mtime, reverse=True)
        if cand2:
            rows = _read_jsonl(cand2[0])
            for r in rows:
                sym = r.get("symbol") or ""
                side = r.get("side") or ""
                qty = r.get("qty") or ""
                price = r.get("price") or ""
                notional = r.get("notional_usdt") or ""
                if not notional:
                    try:
                        notional = float(qty) * float(price)
                    except Exception:
                        notional = ""
                trades.append(
                    {
                        "ts": r.get("ts") or r.get("timestamp") or "",
                        "run_id": "v4",
                        "symbol": sym,
                        "intent": ("OPEN_LONG" if side == "buy" else "CLOSE_LONG"),
                        "side": side,
                        "qty": qty,
                        "price": price,
                        "notional_usdt": notional,
                        "fee_usdt": 0,
                        "slippage_usdt": 0,
                        "realized_pnl_usdt": "",
                        "realized_pnl_pct": "",
                    }
                )

    # Write trades.csv
    cols = [
        "ts",
        "run_id",
        "symbol",
        "intent",
        "side",
        "qty",
        "price",
        "notional_usdt",
        "fee_usdt",
        "slippage_usdt",
        "realized_pnl_usdt",
        "realized_pnl_pct",
    ]
    with (dst / "trades.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for t in trades:
            w.writerow({c: t.get(c, "") for c in cols})

    # summary
    realized = []
    for t in trades:
        try:
            s = str(t.get("realized_pnl_usdt") or "").strip()
            if s:
                realized.append(float(s))
        except Exception:
            pass
    wins = [x for x in realized if x > 0]
    losses = [-x for x in realized if x < 0]
    win_rate = (len(wins) / len(realized)) if realized else None
    pf = (sum(wins) / (sum(losses) + 1e-12)) if realized else None

    eqm = compute_equity_metrics(equity_rows) if equity_rows else {
        "equity_start": None,
        "equity_end": None,
        "total_return_pct": None,
        "max_drawdown_pct": None,
        "sharpe": None,
    }

    summ = {
        "run_id": "v4",
        "start_ts": (equity_rows[0].get("ts") if equity_rows else None),
        "end_ts": (equity_rows[-1].get("ts") if equity_rows else None),
        **eqm,
        "num_trades": len(trades),
        "num_round_trips": len(realized),
        "win_rate": win_rate,
        "profit_factor": pf,
    }
    (dst / "summary.json").write_text(json.dumps(summ, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--v4_reports_dir", required=True)
    ap.add_argument("--out_dir", default="v4_export")
    args = ap.parse_args()
    export_v4(args.v4_reports_dir, args.out_dir)
    print(f"wrote {args.out_dir}/trades.csv and summary.json")


if __name__ == "__main__":
    main()
