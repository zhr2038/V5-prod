#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any, Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _resolve_repo_path(value: str | Path) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def _ts_to_epoch_sec(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, (int, float)) or (isinstance(v, str) and v.strip().isdigit()):
        x = int(float(v))
        if x > 10_000_000_000:
            x //= 1000
        return x
    return None


def _no_data_summary(start_ts: int, end_ts: int) -> Dict[str, Any]:
    return {
        "run_id": "v4",
        "start_ts": int(start_ts),
        "end_ts": int(end_ts),
        "window_start_ts": int(start_ts),
        "window_end_ts": int(end_ts),
        "data_quality": "no_data",
        "equity_points": 0,
        "trade_events": 0,
        "equity_start": None,
        "equity_end": None,
        "total_return_pct": None,
        "max_drawdown_pct": None,
        "sharpe": None,
        "num_trades": None,
        "num_round_trips": None,
        "win_rate": None,
        "profit_factor": None,
    }


def export_v4_reports(*, v4_reports_dir: Path, out_dir: Path, start_ts: int, end_ts: int) -> Path:
    v4_reports_dir = v4_reports_dir.resolve()
    out_dir = out_dir.resolve()
    if v4_reports_dir == out_dir:
        raise ValueError("out_dir must differ from v4_reports_dir")
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_path = out_dir / "summary.json"
    src_summary = v4_reports_dir / "summary.json"

    summary: Dict[str, Any] | None = None
    if src_summary.exists():
        try:
            loaded = json.loads(src_summary.read_text(encoding="utf-8"))
        except Exception:
            loaded = None
        if isinstance(loaded, dict):
            src_start = _ts_to_epoch_sec(loaded.get("window_start_ts", loaded.get("start_ts")))
            src_end = _ts_to_epoch_sec(loaded.get("window_end_ts", loaded.get("end_ts")))
            if src_start == int(start_ts) and src_end == int(end_ts):
                summary = loaded
                if not summary.get("run_id"):
                    summary["run_id"] = "v4"
                for name in ("equity.jsonl", "trades.csv"):
                    src = v4_reports_dir / name
                    if src.exists():
                        shutil.copyfile(src, out_dir / name)

    if summary is None:
        summary = _no_data_summary(int(start_ts), int(end_ts))
        (out_dir / "equity.jsonl").write_text("", encoding="utf-8")
        (out_dir / "trades.csv").write_text("", encoding="utf-8")

    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary_path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--v4_reports_dir", required=True)
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--start_ts", required=True, type=int)
    ap.add_argument("--end_ts", required=True, type=int)
    args = ap.parse_args()

    summary_path = export_v4_reports(
        v4_reports_dir=_resolve_repo_path(args.v4_reports_dir),
        out_dir=_resolve_repo_path(args.out_dir),
        start_ts=int(args.start_ts),
        end_ts=int(args.end_ts),
    )
    print(f"wrote {summary_path}")


if __name__ == "__main__":
    main()
