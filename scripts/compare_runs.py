from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# allow running as a script from repo root
sys.path.append(str(Path(__file__).resolve().parents[1]))


def _ts_to_epoch_sec(v: Any) -> Optional[int]:
    """
    Accepts:
    - epoch seconds (int/str)
    - epoch milliseconds (int/str)  
    - ISO8601 string (with Z or offset)
    Returns epoch seconds (int) or None.
    """
    if v is None:
        return None
    
    # numbers or digit-strings
    if isinstance(v, (int, float)) or (isinstance(v, str) and v.strip().isdigit()):
        x = int(float(v))
        # ms -> sec
        if x > 10_000_000_000:  # ~2286-11-20 in seconds
            x //= 1000
        return x
    
    if isinstance(v, str):
        s = v.strip()
        # tolerate "Z"
        s = s.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            # last resort: try common formats
            for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
                try:
                    dt = datetime.strptime(s, fmt)
                    break
                except ValueError:
                    dt = None
            if dt is None:
                raise ValueError(f"Unrecognized timestamp: {v}")
        
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    
    raise ValueError(f"Unsupported timestamp type: {type(v)}")


def _sec_to_iso(s: int) -> str:
    """Convert epoch seconds to ISO8601 string with Z suffix."""
    return datetime.fromtimestamp(s, tz=timezone.utc).isoformat().replace("+00:00", "Z")


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
        return "N/A"
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
    v4_q = v4.get("data_quality")
    if v4_q and v4_q != "ok":
        lines.append(
            f"- v4 data_quality: {v4_q} (equity_points={v4.get('equity_points')}, trade_events={v4.get('trade_events')})"
        )
    lines.append(f"- v5: `{v5.get('run_id')}`")
    v5_q = v5.get("data_quality")
    if v5_q and v5_q != "ok":
        lines.append(
            f"- v5 data_quality: {v5_q} (equity_points={v5.get('equity_points')}, trade_events={v5.get('trade_events')})"
        )
    lines.append("")

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
    
    # 读取时间戳（优先使用window_*字段）
    v5_start_raw = v5.get("window_start_ts", v5.get("start_ts"))
    v5_end_raw = v5.get("window_end_ts", v5.get("end_ts"))
    
    # 归一化为epoch秒
    v5_start = _ts_to_epoch_sec(v5_start_raw)
    v5_end = _ts_to_epoch_sec(v5_end_raw)
    
    if v5_start is None or v5_end is None:
        raise SystemExit("v5 summary missing start/end window timestamps")
    
    # 创建窗口显示（epoch秒 + ISO格式）
    window_epoch = f"[{v5_start}, {v5_end})"
    window_iso = f"({_sec_to_iso(v5_start)} → {_sec_to_iso(v5_end)})"
    window = f"{window_epoch} UTC {window_iso}"

    v4_summary_path = args.v4_summary
    if args.v4_reports_dir:
        # export v4 with aligned window (使用归一化的epoch秒)
        cmd = [
            sys.executable,
            str(Path(__file__).resolve().parents[0] / "export_v4_reports.py"),
            "--v4_reports_dir",
            args.v4_reports_dir,
            "--out_dir",
            args.v4_out_dir,
        ]
        cmd += ["--start_ts", str(v5_start), "--end_ts", str(v5_end)]
        subprocess.check_call(cmd)
        v4_summary_path = str(Path(args.v4_out_dir) / "summary.json")

    if not v4_summary_path:
        raise SystemExit("need --v4_summary or --v4_reports_dir")

    v4 = _load(v4_summary_path)
    
    # 读取v4时间戳
    v4_start_raw = v4.get("window_start_ts", v4.get("start_ts"))
    v4_end_raw = v4.get("window_end_ts", v4.get("end_ts"))
    v4_start = _ts_to_epoch_sec(v4_start_raw)
    v4_end = _ts_to_epoch_sec(v4_end_raw)
    
    if v4_start is None or v4_end is None:
        raise SystemExit("v4 summary missing start/end window timestamps")

    # 强校验：归一化后比较
    if v4_start != v5_start or v4_end != v5_end:
        raise SystemExit(
            f"Window mismatch after normalization: "
            f"v4=[{v4_start},{v4_end}) v5=[{v5_start},{v5_end}) "
            f"(raw v4={v4_start_raw}->{v4_end_raw}, v5={v5_start_raw}->{v5_end_raw})"
        )

    md = compare(v4, v5, window=window)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md, encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
