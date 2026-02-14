from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List


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


def export_v4(v4_reports_dir: str, out_dir: str) -> None:
    src = Path(v4_reports_dir)
    dst = Path(out_dir)
    dst.mkdir(parents=True, exist_ok=True)

    # Try trade_reflections first
    candidates = sorted(src.glob("trade_reflections_*.jsonl"), key=lambda x: x.stat().st_mtime, reverse=True)
    trades: List[Dict[str, Any]] = []
    if candidates:
        rows = _read_jsonl(candidates[0])
        for r in rows:
            # best-effort mapping
            trades.append(
                {
                    "ts": r.get("ts") or r.get("timestamp") or "",
                    "run_id": "v4",
                    "symbol": r.get("symbol") or "",
                    "intent": r.get("intent") or r.get("side") or "",
                    "side": r.get("side") or "",
                    "qty": r.get("qty") or "",
                    "price": r.get("price") or "",
                    "notional_usdt": r.get("notional_usdt") or r.get("notional") or "",
                    "fee_usdt": r.get("fee_usdt") or 0,
                    "slippage_usdt": r.get("slippage_usdt") or 0,
                    "realized_pnl_usdt": r.get("realized_pnl_usdt") or r.get("pnl_usdt") or r.get("pnl") or "",
                    "realized_pnl_pct": r.get("realized_pnl_pct") or r.get("pnl_pct") or "",
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

    # summary minimal (trade-based only)
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

    summ = {
        "run_id": "v4",
        "start_ts": None,
        "end_ts": None,
        "equity_start": None,
        "equity_end": None,
        "total_return_pct": None,
        "max_drawdown_pct": None,
        "sharpe": None,
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
