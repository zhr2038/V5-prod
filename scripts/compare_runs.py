from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict

# allow running as a script from repo root
sys.path.append(str(Path(__file__).resolve().parents[1]))


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


def compare(v4: Dict[str, Any], v5: Dict[str, Any], window: str = "") -> str:
    lines = []
    lines.append("# v4 vs v5\n")
    if window:
        lines.append(f"- window: {window}")
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
    ap.add_argument("--v4_summary", required=False, help="path to v4 summary.json")
    ap.add_argument("--v5_summary", required=True, help="path to v5 summary.json")
    ap.add_argument("--out", default="reports/compare/v4_vs_v5.md")

    # optional automation: export v4 on the fly using v5 window
    ap.add_argument("--v4_reports_dir", default=None)
    ap.add_argument("--v4_out_dir", default="v4_export")
    args = ap.parse_args()

    v5 = _load(args.v5_summary)
    start_ts = v5.get("start_ts")
    end_ts = v5.get("end_ts")

    window = f"[{start_ts}, {end_ts}]"

    v4_summary_path = args.v4_summary
    if args.v4_reports_dir:
        # export v4 with aligned window
        cmd = [
            sys.executable,
            str(Path(__file__).resolve().parents[0] / "export_v4_reports.py"),
            "--v4_reports_dir",
            args.v4_reports_dir,
            "--out_dir",
            args.v4_out_dir,
        ]
        if start_ts is not None and end_ts is not None:
            cmd += ["--start_ts", str(start_ts), "--end_ts", str(end_ts)]
        subprocess.check_call(cmd)
        v4_summary_path = str(Path(args.v4_out_dir) / "summary.json")

    if not v4_summary_path:
        raise SystemExit("need --v4_summary or --v4_reports_dir")

    v4 = _load(v4_summary_path)

    # strong window check (best-effort)
    if (v4.get("start_ts") is not None and start_ts is not None and str(v4.get("start_ts")) != str(start_ts)) or (
        v4.get("end_ts") is not None and end_ts is not None and str(v4.get("end_ts")) != str(end_ts)
    ):
        raise SystemExit(f"window mismatch: v4=[{v4.get('start_ts')}, {v4.get('end_ts')}], v5={window}")

    md = compare(v4, v5, window=window)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md, encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
