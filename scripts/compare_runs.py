from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))


def _resolve_repo_path(value: str | os.PathLike[str] | None, *, default: Path | None = None) -> Path:
    path = Path(value) if value is not None else Path(default)  # type: ignore[arg-type]
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def _default_v4_export_out_dir() -> Path:
    return PROJECT_ROOT / "reports" / "compare" / "v4_export"


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

    if isinstance(v, (int, float)) or (isinstance(v, str) and v.strip().isdigit()):
        x = int(float(v))
        if x > 10_000_000_000:
            x //= 1000
        return x

    if isinstance(v, str):
        s = v.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
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


def _safe_load_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception:
        return None


def _fmt(x: Any) -> str:
    if x is None:
        return "N/A"
    try:
        return f"{float(x):.4g}"
    except Exception:
        return str(x)


def _fmt_bool(x: Any) -> str:
    if x is None:
        return "N/A"
    if isinstance(x, bool):
        return "true" if x else "false"
    s = str(x).strip().lower()
    if s in ("true", "false"):
        return s
    return s


def _to_bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if x is None:
        return False
    return str(x).strip().lower() in {"1", "true", "yes", "on"}


def _budget_reason_norm(reason: Any) -> str:
    if not reason:
        return "N/A"
    s = str(reason)
    if "+" in s:
        parts = s.split("+")
        out = []
        if any("turnover" in p for p in parts):
            out.append("turnover")
        if any("cost" in p for p in parts):
            out.append("cost")
        if len(out) == 2:
            return "both"
        return out[0] if out else s
    if "turnover" in s:
        return "turnover"
    if "cost" in s:
        return "cost"
    return s


def _budget_header_lines(v5: Dict[str, Any], v5_audit: Optional[Dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    b = (v5.get("budget") or {}) if isinstance(v5, dict) else {}

    exceeded = b.get("exceeded")
    reason = b.get("reason")

    if b:
        lines.append(f"- v5 budget_exceeded: {_fmt_bool(exceeded)}")
        lines.append(f"- v5 budget_reason: {_budget_reason_norm(reason)}")

        t_used = b.get("turnover_used")
        t_budget = b.get("turnover_budget_per_day")
        c_bps = b.get("cost_used_bps")
        c_budget = b.get("cost_budget_bps_per_day")
        if t_budget is not None or c_budget is not None:
            tu = _fmt(t_used)
            tb = _fmt(t_budget)
            cu = _fmt(c_bps)
            cb = _fmt(c_budget)
            lines.append(f"- v5 budget_used: turnover={tu}/{tb} cost_bps={cu}/{cb}")

    ba = (v5_audit.get("budget_action") or {}) if v5_audit else {}
    if ba and _to_bool(ba.get("enabled")):
        if ba.get("deadband_effective") is not None:
            lines.append(
                f"- v5 deadband_effective: {_fmt(ba.get('deadband_effective'))} "
                f"(base={_fmt(ba.get('deadband_base'))} mult={_fmt(ba.get('deadband_multiplier'))} cap={_fmt(ba.get('deadband_cap'))})"
            )
        if ba.get("min_trade_notional_effective") is not None:
            cap = ba.get("min_trade_notional_cap")
            lines.append(
                f"- v5 min_trade_notional_effective: {_fmt(ba.get('min_trade_notional_effective'))} "
                f"(base={_fmt(ba.get('min_trade_notional_base'))} mult={_fmt(ba.get('min_trade_notional_multiplier'))} cap={_fmt(cap)})"
            )

    return lines


def compare(
    v4: Dict[str, Any],
    v5: Dict[str, Any],
    window: str = "",
    v5_audit: Optional[Dict[str, Any]] = None,
) -> str:
    lines: list[str] = []
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

    if v5_audit:
        lines.append(f"- v5 deadband_pct: {_fmt(v5_audit.get('rebalance_deadband_pct'))}")
        lines.append(f"- v5 deadband_skipped_count: {_fmt(v5_audit.get('rebalance_skipped_deadband_count'))}")
        deadband_rej = (v5_audit.get("rejects") or {}).get("deadband_skip")
        lines.append(f"- v5 rejects.deadband_skip: {_fmt(deadband_rej)}")

    lines.extend(_budget_header_lines(v5, v5_audit))
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--v4_summary", required=False, help="path to v4 summary.json")
    ap.add_argument("--v5_summary", required=True, help="path to v5 summary.json")
    ap.add_argument("--out", default=None)
    ap.add_argument("--v4_reports_dir", default=None)
    ap.add_argument("--v4_out_dir", default=None)
    args = ap.parse_args()

    v5_summary_path = _resolve_repo_path(args.v5_summary)
    out_path = _resolve_repo_path(args.out, default=PROJECT_ROOT / "reports" / "compare" / "v4_vs_v5.md")
    v4_out_dir = _resolve_repo_path(args.v4_out_dir, default=_default_v4_export_out_dir())

    v5 = _load(str(v5_summary_path))

    v5_start_raw = v5.get("window_start_ts", v5.get("start_ts"))
    v5_end_raw = v5.get("window_end_ts", v5.get("end_ts"))
    v5_start = _ts_to_epoch_sec(v5_start_raw)
    v5_end = _ts_to_epoch_sec(v5_end_raw)

    if v5_start is None or v5_end is None:
        raise SystemExit("v5 summary missing start/end window timestamps")

    window_epoch = f"[{v5_start}, {v5_end})"
    window_iso = f"({_sec_to_iso(v5_start)} -> {_sec_to_iso(v5_end)})"
    window = f"{window_epoch} UTC {window_iso}"

    v4_summary_path = str(_resolve_repo_path(args.v4_summary)) if args.v4_summary else None
    if args.v4_reports_dir:
        v4_reports_dir = _resolve_repo_path(args.v4_reports_dir)
        if v4_reports_dir == v4_out_dir:
            raise SystemExit("--v4_out_dir must differ from --v4_reports_dir")
        cmd = [
            sys.executable,
            str(Path(__file__).resolve().parents[0] / "export_v4_reports.py"),
            "--v4_reports_dir",
            str(v4_reports_dir),
            "--out_dir",
            str(v4_out_dir),
            "--start_ts",
            str(v5_start),
            "--end_ts",
            str(v5_end),
        ]
        subprocess.check_call(cmd)
        v4_summary_path = str(v4_out_dir / "summary.json")

    if not v4_summary_path:
        raise SystemExit("need --v4_summary or --v4_reports_dir")

    v4 = _load(v4_summary_path)

    v4_start_raw = v4.get("window_start_ts", v4.get("start_ts"))
    v4_end_raw = v4.get("window_end_ts", v4.get("end_ts"))
    v4_start = _ts_to_epoch_sec(v4_start_raw)
    v4_end = _ts_to_epoch_sec(v4_end_raw)

    if v4_start is None or v4_end is None:
        raise SystemExit("v4 summary missing start/end window timestamps")

    if v4_start != v5_start or v4_end != v5_end:
        raise SystemExit(
            f"Window mismatch after normalization: "
            f"v4=[{v4_start},{v4_end}) v5=[{v5_start},{v5_end}) "
            f"(raw v4={v4_start_raw}->{v4_end_raw}, v5={v5_start_raw}->{v5_end_raw})"
        )

    v5_run_dir = os.path.dirname(str(v5_summary_path))
    v5_audit = _safe_load_json(os.path.join(v5_run_dir, "decision_audit.json"))

    md = compare(v4, v5, window=window, v5_audit=v5_audit)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md, encoding="utf-8")
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
