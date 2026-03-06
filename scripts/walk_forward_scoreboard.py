#!/usr/bin/env python3
"""Build a lightweight walk-forward scoreboard from recent run summaries.

Output: reports/walk_forward_scoreboard.json
"""

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime

RUNS_DIR = Path("reports/runs")
OUT_PATH = Path("reports/walk_forward_scoreboard.json")


def _safe_float(v, default=0.0):
    try:
        x = float(v)
        if x != x:
            return default
        return x
    except Exception:
        return default


def _iter_summary_files(limit=120):
    if not RUNS_DIR.exists():
        return []
    dirs = [d for d in RUNS_DIR.iterdir() if d.is_dir()]
    dirs = sorted(dirs, key=lambda p: p.name, reverse=True)
    out = []
    for d in dirs[:limit]:
        s = d / "summary.json"
        if s.exists():
            out.append(s)
    return out


def main() -> int:
    files = _iter_summary_files()
    pnl_list = []
    cost_list = []
    wins = 0
    total = 0

    for f in files:
        try:
            obj = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue

        # tolerant parsing
        pnl = _safe_float(obj.get("net_pnl_usdt", obj.get("pnl_usdt", 0.0)))
        cost = _safe_float(obj.get("total_cost_usdt", obj.get("cost_usdt", 0.0)))
        pnl_list.append(pnl)
        cost_list.append(cost)
        total += 1
        if pnl > 0:
            wins += 1

    if total == 0:
        out = {
            "ts": datetime.now().isoformat(),
            "runs_count": 0,
            "note": "no_summary_files",
        }
    else:
        net = sum(pnl_list)
        avg = net / total
        win_rate = wins / total
        cost = sum(cost_list)
        out = {
            "ts": datetime.now().isoformat(),
            "runs_count": total,
            "net_pnl_usdt": net,
            "avg_pnl_usdt": avg,
            "win_rate": win_rate,
            "total_cost_usdt": cost,
            "cost_to_gross_abs_ratio": (cost / max(1e-9, sum(abs(x) for x in pnl_list))),
            "last_20": {
                "runs": min(20, total),
                "net_pnl_usdt": sum(pnl_list[:20]),
                "win_rate": (sum(1 for x in pnl_list[:20] if x > 0) / max(1, min(20, total))),
            },
        }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
