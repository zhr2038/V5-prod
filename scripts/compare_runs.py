from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple


KEYS = [
    "num_trades",
    "num_round_trips",
    "total_return_pct",
    "max_drawdown_pct",
    "sharpe",
    "turnover_ratio",
    "cost_usdt_total",
    "cost_ratio",
    "win_rate",
    "profit_factor",
]


def _load(p: str) -> Dict[str, Any]:
    return json.loads(Path(p).read_text(encoding="utf-8"))


def _fmt(x: Any) -> str:
    if x is None:
        return "-"
    try:
        return f"{float(x):.4g}"
    except Exception:
        return str(x)


def compare(v4: Dict[str, Any], v5: Dict[str, Any]) -> str:
    lines = []
    lines.append("# v4 vs v5\n")
    lines.append(f"- v4: `{v4.get('run_id')}`")
    lines.append(f"- v5: `{v5.get('run_id')}`\n")

    lines.append("| metric | v4 | v5 | delta |")
    lines.append("|---|---:|---:|---:|")
    for k in KEYS:
        a = v4.get(k)
        b = v5.get(k)
        d = None
        try:
            if a is not None and b is not None:
                d = float(b) - float(a)
        except Exception:
            d = None
        lines.append(f"| {k} | {_fmt(a)} | {_fmt(b)} | {_fmt(d)} |")

    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--v4_summary", required=True)
    ap.add_argument("--v5_summary", required=True)
    ap.add_argument("--out", default="reports/compare/v4_vs_v5.md")
    args = ap.parse_args()

    v4 = _load(args.v4_summary)
    v5 = _load(args.v5_summary)
    md = compare(v4, v5)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md, encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
