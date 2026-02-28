#!/usr/bin/env python3
"""Remove polluted placeholder cost events.

We observed invalid okx_fill cost events like:
- ts=0
- run_id='r'
- window_start_ts=1
These break F2 calibration rollups.

This script rewrites reports/cost_events/*.jsonl in-place, keeping a .bak copy.
"""

from __future__ import annotations

import json
from pathlib import Path


def is_invalid(evt: dict) -> bool:
    try:
        src = evt.get("source")
        ts = int(evt.get("ts") or 0)
        run_id = str(evt.get("run_id") or "")
        w0 = int(evt.get("window_start_ts") or 0)
    except Exception:
        return True

    if src == "okx_fill":
        if ts <= 0:
            return True
        if run_id in {"r", "", "none", "null"}:
            return True
        if w0 > 0 and w0 < 1_600_000_000:
            return True
    return False


def main() -> int:
    base = Path("reports/cost_events")
    if not base.exists():
        print("no reports/cost_events")
        return 0

    files = sorted(base.glob("*.jsonl"))
    total_drop = 0
    total_keep = 0

    for p in files:
        lines = p.read_text(encoding="utf-8").splitlines()
        keep = []
        drop = 0
        for ln in lines:
            ln = ln.strip()
            if not ln:
                continue
            try:
                evt = json.loads(ln)
            except Exception:
                continue
            if is_invalid(evt):
                drop += 1
                continue
            keep.append(json.dumps(evt, ensure_ascii=False))

        if drop > 0:
            bak = p.with_suffix(p.suffix + ".bak")
            bak.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
            p.write_text("\n".join(keep) + ("\n" if keep else ""), encoding="utf-8")

        print(f"{p.name}: keep={len(keep)} drop={drop}")
        total_drop += drop
        total_keep += len(keep)

    print(f"TOTAL keep={total_keep} drop={total_drop}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
