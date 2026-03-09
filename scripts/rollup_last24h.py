from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

# allow running as a script from repo root
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.reporting.summary_writer import write_summary


def _utc_hour_floor(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)


def window_ids(end_hour_exclusive: datetime, hours: int = 24) -> List[str]:
    # end_hour_exclusive is aligned to hour
    out = []
    for i in range(hours, 0, -1):
        h = end_hour_exclusive - timedelta(hours=i)
        out.append(h.strftime("%Y%m%d_%H"))
    return out


def _read_equity_jsonl(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            pass
    return rows


def _read_trades_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return [dict(x) for x in r]


def rollup(end_hour_exclusive: datetime, runs_dir: Path, out_dir: Path, hours: int = 24) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)

    eq_all: List[Dict] = []
    tr_all: List[Dict[str, str]] = []

    for wid in window_ids(end_hour_exclusive, hours=hours):
        rd = runs_dir / wid
        eq_all.extend(_read_equity_jsonl(rd / "equity.jsonl"))
        tr_all.extend(_read_trades_csv(rd / "trades.csv"))

    # write merged artifacts
    if eq_all:
        (out_dir / "equity.jsonl").write_text(
            "\n".join([json.dumps(x, ensure_ascii=False) for x in eq_all]) + "\n",
            encoding="utf-8",
        )

    if tr_all:
        cols = list(tr_all[0].keys())
        with (out_dir / "trades.csv").open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for row in tr_all:
                w.writerow(row)

    # rollup窗口语义：覆盖过去N小时 [start, end)
    window_end_ts = int(end_hour_exclusive.timestamp())
    window_start_ts = int((end_hour_exclusive - timedelta(hours=hours)).timestamp())

    write_summary(
        str(out_dir),
        window_start_ts=window_start_ts,
        window_end_ts=window_end_ts,
    )
    return out_dir / "summary.json"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=24)
    ap.add_argument("--runs_dir", default="reports/runs")
    ap.add_argument("--out_dir", default=None, help="default: reports/rollups/last24h_YYYYMMDD_HH")
    ap.add_argument("--v4_reports_dir", default="/home/admin/clawd/v4-trading-bot/reports")
    ap.add_argument("--compare_out", default=None, help="default: reports/compare/daily/v4_vs_v5_last24h_YYYYMMDD_HH.md")
    args = ap.parse_args()

    end = _utc_hour_floor(datetime.now(timezone.utc))
    tag = end.strftime("%Y%m%d_%H")

    runs_dir = Path(args.runs_dir)
    out_dir = Path(args.out_dir) if args.out_dir else Path(f"reports/rollups/last24h_{tag}")

    v5_summary = rollup(end, runs_dir=runs_dir, out_dir=out_dir, hours=int(args.hours))

    compare_out = Path(args.compare_out) if args.compare_out else Path(f"reports/compare/daily/v4_vs_v5_last24h_{tag}.md")
    compare_out.parent.mkdir(parents=True, exist_ok=True)

    # window alignment + auto v4 export happens here
    import subprocess

    subprocess.check_call(
        [
            sys.executable,
            str(Path(__file__).resolve().parents[0] / "compare_runs.py"),
            "--v4_reports_dir",
            str(args.v4_reports_dir),
            "--v5_summary",
            str(v5_summary),
            "--out",
            str(compare_out),
        ]
    )

    print(f"wrote {v5_summary}")
    print(f"wrote {compare_out}")


if __name__ == "__main__":
    main()
