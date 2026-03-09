from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

# allow running as a script from repo root
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.reporting.spread_snapshots import compute_spread_stats


def _utc_yyyymmdd_from_epoch_sec(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y%m%d")


def rollup_day(day_ymd: str, snapshots_dir: Path, out_dir: Path) -> Path:
    path = snapshots_dir / f"{day_ymd}.jsonl"
    out_dir.mkdir(parents=True, exist_ok=True)

    by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                evt = json.loads(line)
            except Exception:
                continue
            for row in (evt.get("symbols") or []):
                sym = str(row.get("symbol") or "")
                if not sym:
                    continue
                by_symbol.setdefault(sym, []).append(dict(row))

    stats = {
        "day": day_ymd,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "symbols": {},
    }

    for sym, rows in sorted(by_symbol.items()):
        stats["symbols"][sym] = compute_spread_stats(rows)

    out_path = out_dir / f"daily_spread_stats_{day_ymd}.json"
    out_path.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", default=None, help="UTC day YYYYMMDD (default: today UTC)")
    ap.add_argument("--snapshots_dir", default="reports/spread_snapshots")
    ap.add_argument("--out_dir", default="reports/spread_stats")
    args = ap.parse_args()

    day = args.day
    if not day:
        day = datetime.now(timezone.utc).strftime("%Y%m%d")

    out = rollup_day(day, snapshots_dir=Path(args.snapshots_dir), out_dir=Path(args.out_dir))
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
