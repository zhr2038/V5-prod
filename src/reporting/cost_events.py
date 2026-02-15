from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _utc_yyyymmdd_from_epoch_sec(ts: int) -> str:
    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    return dt.strftime("%Y%m%d")


def append_cost_event(event: Dict[str, Any], base_dir: str = "reports/cost_events") -> Path:
    """Append one NDJSON line. File name is based on window_start_ts's UTC date.

    Requirements:
    - one line per JSON object (no pretty print)
    - UTF-8
    """
    ws = event.get("window_start_ts")
    if ws is None:
        raise ValueError("cost_event.window_start_ts is required")

    ymd = _utc_yyyymmdd_from_epoch_sec(int(ws))
    out_dir = Path(base_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{ymd}.jsonl"

    # strict NDJSON: one object per line
    line = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

    return path
