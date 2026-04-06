from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_spread_snapshots_dir(base_dir: str | Path = "reports/spread_snapshots") -> Path:
    path = Path(base_dir)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def _utc_yyyymmdd_from_epoch_sec(ts: int) -> str:
    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    return dt.strftime("%Y%m%d")


def append_spread_snapshot(event: Dict[str, Any], base_dir: str | Path = "reports/spread_snapshots") -> Path:
    """Append one NDJSON line of a spread snapshot.

    File name is based on window_end_ts's UTC date.

    Required fields:
    - window_end_ts: epoch seconds
    - symbols: list[ {symbol,bid,ask,mid,spread_bps,selected?} ]
    """
    we = event.get("window_end_ts")
    if we is None:
        raise ValueError("spread_snapshot.window_end_ts is required")

    ymd = _utc_yyyymmdd_from_epoch_sec(int(we))
    out_dir = _resolve_spread_snapshots_dir(base_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{ymd}.jsonl"

    line = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

    return path


def compute_spread_stats(symbol_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute basic quantiles for a list of rows with spread_bps."""
    xs = []
    for r in symbol_rows:
        try:
            v = float(r.get("spread_bps"))
        except Exception:
            continue
        if v == v and v >= 0:
            xs.append(v)

    xs.sort()
    if not xs:
        return {"count": 0}

    def q(p: float) -> float:
        """Q"""
        if len(xs) == 1:
            return float(xs[0])
        idx = int(round(p * (len(xs) - 1)))
        idx = max(0, min(len(xs) - 1, idx))
        return float(xs[idx])

    return {
        "count": int(len(xs)),
        "p50": q(0.50),
        "p75": q(0.75),
        "p90": q(0.90),
        "p95": q(0.95),
        "max": float(xs[-1]),
    }
